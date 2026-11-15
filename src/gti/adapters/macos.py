"""macOS Bluetooth Classic RFCOMM transport via IOBluetooth/pyobjc.

macOS has no kernel BT socket API, so this drives ``IOBluetoothRFCOMMChannel``
through pyobjc.  A daemon thread runs an NSRunLoop for IOBluetooth callbacks;
received bytes are fed into an ``asyncio.StreamReader`` via
``call_soon_threadsafe``; ``send()`` calls ``writeSync:length:`` in an executor.

Requires: ``pyobjc-framework-Cocoa`` (``pip install pyobjc-framework-Cocoa``).
"""

from __future__ import annotations

import asyncio
import logging
import re
import struct
import sys
import threading
from typing import Any, Optional

from .channel import RFCOMMStreamTransport
from ..transport import TransportClosed

__all__ = ["MacOSRFCOMMHostTransport", "MacOSRFCOMMConsumerTransport"]

logger = logging.getLogger(__name__)

_FRAME_LEN = struct.Struct(">I")
_INCOMING = 1  # kIOBluetoothUserNotificationChannelDirectionIncoming

# Raw SDP data-element encodings.  gti advertises its own service UUID so a
# client can find the gti record even when the peer also exposes other SPP
# services (macOS often has a built-in "COM5" SPP record).
_SPP_UUID_BYTES = bytes.fromhex("0000110100001000800000805f9b34fb")
_GTI_UUID_BYTES = bytes.fromhex("e6031152a17a4f769aa5cba1b4321dcf")
_L2CAP_UUID_BYTES = bytes.fromhex("0100")
_RFCOMM_UUID_BYTES = bytes.fromhex("0003")
_PUBLIC_BROWSE_GROUP_BYTES = bytes.fromhex("1002")


def _require_macos() -> None:
    if sys.platform != "darwin":
        raise RuntimeError(
            "MacOSRFCOMMTransport is only supported on macOS; "
            "use LinuxRFCOMMTransport on Linux or WindowsRFCOMMTransport on Windows"
        )


def _require_bluetooth_on(ns: dict) -> None:
    controller = ns["IOBluetoothHostController"].defaultController()
    if controller is None or controller.powerState() == 0:
        raise OSError(
            "Bluetooth is off; turn it on in System Settings > Bluetooth"
        )


def _load_iobluetooth() -> dict:
    try:
        import objc  # type: ignore
    except ImportError as exc:
        raise ImportError(
            "pyobjc-framework-Cocoa is required for macOS RFCOMM support "
            "(pip install pyobjc-framework-Cocoa)"
        ) from exc
    ns: dict = {}
    objc.loadBundle(
        "IOBluetooth",
        bundle_path="/System/Library/Frameworks/IOBluetooth.framework",
        module_globals=ns,
    )
    return ns


_DelegateClass: Any = None


def _get_delegate_class() -> Any:
    global _DelegateClass
    if _DelegateClass is not None:
        return _DelegateClass

    import objc
    from Foundation import NSObject  # type: ignore

    class _RFCOMMDelegate(NSObject):
        @objc.python_method
        def _setup(
            self,
            loop: asyncio.AbstractEventLoop,
            reader: asyncio.StreamReader,
            on_open: "asyncio.Event | None" = None,
        ) -> None:
            self._loop = loop
            self._reader = reader
            self._on_open = on_open

        def rfcommChannelData_data_length_(
            self, channel: Any, data: Any, length: int
        ) -> None:
            raw = bytes(data[:length])
            self._loop.call_soon_threadsafe(self._reader.feed_data, raw)

        def rfcommChannelClosed_(self, channel: Any) -> None:
            self._loop.call_soon_threadsafe(self._reader.feed_eof)

        def rfcommChannelOpened_channel_(
            self, notification: Any, channel: Any
        ) -> None:
            channel.setDelegate_(self)
            self._channel_ref = channel
            if self._on_open is not None:
                self._loop.call_soon_threadsafe(self._on_open.set)

    _DelegateClass = _RFCOMMDelegate
    return _DelegateClass


class MacOSRFCOMMHostTransport(RFCOMMStreamTransport):
    """Host-side RFCOMM server via IOBluetooth.

    Tries to publish an SPP SDP record so clients can discover the channel.
    Some macOS releases (Ventura and later) fail to publish SDP records; in
    that case the transport falls back to listening on *channel*, and the
    dialler must pass the same channel explicitly.
    """

    def __init__(self, channel: int = 1) -> None:
        super().__init__()
        _require_macos()
        self._requested_channel = channel
        self._channel_id: int = channel
        self._rfcomm_channel: Any = None
        self._sdp_record: Any = None
        self._open_notification: Any = None
        self._closed_event = asyncio.Event()

    @property
    def channel(self) -> int:
        return self._channel_id

    @property
    def channel_id(self) -> int:
        """Alias for :attr:`channel` (IOBluetooth calls it a channel id)."""
        return self._channel_id

    def _sdp_dictionary(self) -> dict:
        # Values use the IOBluetoothSDPDataElement dictionary/bytes format:
        # UUIDs and protocol identifiers are raw SDP data-element bytes.
        return {
            "0001 - ServiceClassIDList": [_SPP_UUID_BYTES, _GTI_UUID_BYTES],
            "0004 - ProtocolDescriptorList": [
                [_L2CAP_UUID_BYTES],
                [
                    _RFCOMM_UUID_BYTES,
                    {
                        "DataElementSize": 1,
                        "DataElementType": 1,
                        "DataElementValue": self._requested_channel,
                    },
                ],
            ],
            "0005 - BrowseGroupList*": [_PUBLIC_BROWSE_GROUP_BYTES],
            "0100 - ServiceName*": "gti",
        }

    def _publish_sdp(self, IOBluetoothSDPServiceRecord: Any) -> None:
        record = IOBluetoothSDPServiceRecord.publishedServiceRecordWithDictionary_(
            self._sdp_dictionary()
        )
        if record is None:
            return

        # pyobjc does not expose the out-parameter of getRFCOMMChannelID: as a
        # tuple on all macOS versions, so read the assigned channel from the
        # record description (and fall back to the protocol descriptor).
        channel = self._requested_channel
        match = re.search(r"RFCOMM ChannelID:\s*(\d+)", str(record))
        if match is not None:
            channel = int(match.group(1))
        else:
            try:
                descriptor = str(record.getAttributeDataElement_(4))
                numbers = re.findall(r"uint32\((\d+)\)", descriptor)
                if numbers:
                    channel = int(numbers[-1])
            except Exception:
                pass

        self._sdp_record = record
        self._channel_id = channel
        if self._channel_id != self._requested_channel:
            logger.warning(
                "macOS assigned RFCOMM channel %d (requested %d); "
                "pass --channel %d on the dialler",
                self._channel_id,
                self._requested_channel,
                self._channel_id,
            )

    async def start(self) -> None:
        if self._closed:
            raise TransportClosed("macOS RFCOMM host is closed")
        ns = _load_iobluetooth()
        _require_bluetooth_on(ns)
        IOBluetoothSDPServiceRecord = ns["IOBluetoothSDPServiceRecord"]
        IOBluetoothRFCOMMChannel = ns["IOBluetoothRFCOMMChannel"]

        loop = asyncio.get_running_loop()
        rx_reader: asyncio.StreamReader = asyncio.StreamReader()
        connected: asyncio.Event = asyncio.Event()

        Delegate = _get_delegate_class()
        delegate = Delegate.alloc().init()
        delegate._setup(loop, rx_reader, on_open=connected)

        bt_ready = threading.Event()
        bt_error: list[str] = []

        def _bt_thread() -> None:
            from Foundation import NSRunLoop, NSDate

            try:
                # Register for incoming channels first: the daemon needs an
                # interested client before the SDP record can route a
                # connection to us.
                self._open_notification = (
                    IOBluetoothRFCOMMChannel
                    .registerForChannelOpenNotifications_selector_withChannelID_direction_(
                        delegate,
                        b"rfcommChannelOpened:channel:",
                        self._requested_channel,
                        _INCOMING,
                    )
                )
                try:
                    self._publish_sdp(IOBluetoothSDPServiceRecord)
                except Exception as exc:
                    logger.debug("SDP publishing failed: %s", exc)
                if self._sdp_record is None:
                    self._channel_id = self._requested_channel
                    logger.warning(
                        "macOS SDP publishing unavailable; listening on fixed "
                        "RFCOMM channel %d (pass --channel %d on the dialler)",
                        self._channel_id,
                        self._channel_id,
                    )
                elif self._channel_id != self._requested_channel:
                    # The system assigned a different channel; move the
                    # incoming-channel registration to it.
                    try:
                        self._open_notification.unregister()
                    except Exception:
                        pass
                    self._open_notification = (
                        IOBluetoothRFCOMMChannel
                        .registerForChannelOpenNotifications_selector_withChannelID_direction_(
                            delegate,
                            b"rfcommChannelOpened:channel:",
                            self._channel_id,
                            _INCOMING,
                        )
                    )
            except Exception as exc:
                bt_error.append(f"IOBluetooth host setup failed: {exc}")
            finally:
                bt_ready.set()

            rl = NSRunLoop.currentRunLoop()
            while not self._closed:
                rl.runUntilDate_(NSDate.dateWithTimeIntervalSinceNow_(0.25))

        t = threading.Thread(target=_bt_thread, daemon=True, name="gti-rfcomm-host")
        t.start()
        try:
            await loop.run_in_executor(None, lambda: bt_ready.wait(10))
        except asyncio.CancelledError:
            self._closed_event.set()
            raise
        if not bt_ready.is_set():
            bt_error.append("IOBluetooth host setup timed out")
        if bt_error:
            raise OSError(bt_error[0])
        if self._open_notification is None:
            raise OSError("IOBluetooth failed to register for incoming channels")

        connected_task = asyncio.create_task(connected.wait())
        closed_task = asyncio.create_task(self._closed_event.wait())
        try:
            await asyncio.wait(
                {connected_task, closed_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
        finally:
            for task in (connected_task, closed_task):
                task.cancel()
        if self._closed:
            raise TransportClosed("macOS RFCOMM host closed while waiting")
        self._rfcomm_channel = getattr(delegate, "_channel_ref", None)
        if self._rfcomm_channel is None:
            raise OSError("IOBluetooth did not provide an RFCOMM channel")
        self._reader = rx_reader
        self._start_recv()

    async def send(self, frame: bytes) -> None:
        if self._closed or self._rfcomm_channel is None:
            raise TransportClosed("macOS RFCOMM channel is not open")
        loop = asyncio.get_running_loop()
        data = _FRAME_LEN.pack(len(frame)) + frame
        await loop.run_in_executor(
            None,
            lambda: self._rfcomm_channel.writeSync_length_(data, len(data)),
        )

    async def close(self) -> None:
        self._closed_event.set()
        notification = self._open_notification
        self._open_notification = None
        if notification is not None:
            try:
                notification.unregister()
            except Exception:
                pass
        record = self._sdp_record
        self._sdp_record = None
        if record is not None:
            try:
                record.removeServiceRecord()
            except Exception:
                pass
        channel = self._rfcomm_channel
        self._rfcomm_channel = None
        if channel is not None:
            try:
                channel.closeChannel()
            except Exception:
                pass
        await super().close()


class MacOSRFCOMMConsumerTransport(RFCOMMStreamTransport):
    """Consumer-side RFCOMM connection on macOS."""

    def __init__(
        self, address: str, channel: int = 1, require_sdp: bool = False
    ) -> None:
        super().__init__()
        _require_macos()
        self._address = address
        self._channel_id = channel
        self._require_sdp = require_sdp
        self._rfcomm_channel: Any = None

    @property
    def channel(self) -> int:
        return self._channel_id

    @property
    def channel_id(self) -> int:
        """Alias for :attr:`channel` (IOBluetooth calls it a channel id)."""
        return self._channel_id

    async def start(self) -> None:
        ns = _load_iobluetooth()
        _require_bluetooth_on(ns)
        IOBluetoothDevice = ns["IOBluetoothDevice"]
        loop = asyncio.get_running_loop()
        rx_reader: asyncio.StreamReader = asyncio.StreamReader()
        connected: asyncio.Event = asyncio.Event()

        Delegate = _get_delegate_class()
        delegate = Delegate.alloc().init()
        delegate._setup(loop, rx_reader, on_open=connected)

        device = IOBluetoothDevice.deviceWithAddressString_(self._address)
        if device is None:
            raise OSError(
                f"Bluetooth device not found: {self._address!r} "
                "(is it paired and in range?)"
            )

        # Prefer the channel advertised by the peer's SDP record.  This lets
        # us connect to a macOS listener that published an SPP record without
        # the user having to pass --channel.
        channel_id = self._channel_id
        found_sdp = False
        try:
            uuid = ns["IOBluetoothSDPUUID"].alloc().initWithBytes_length_(
                _GTI_UUID_BYTES, len(_GTI_UUID_BYTES)
            )
            record = device.getServiceRecordForUUID_(uuid)
            if record is not None:
                match = re.search(r"RFCOMM ChannelID:\s*(\d+)", str(record))
                if match is not None:
                    channel_id = int(match.group(1))
                    found_sdp = True
        except Exception:
            pass

        if self._require_sdp and not found_sdp:
            raise OSError(
                f"no gti SPP service found on {self._address}; is the listener "
                "running?  (or pass --channel N to force a channel)"
            )

        # openRFCOMMChannelSync:withChannelID:delegate: crashes on recent
        # macOS releases (the out-parameter is not bridged correctly).  Use
        # the channel object's initializer instead; it returns the channel
        # directly and reports open failures as an IOReturn.
        IOBluetoothRFCOMMChannel = ns["IOBluetoothRFCOMMChannel"]
        classic_peer = device.classicPeer()
        if classic_peer is None:
            raise OSError(
                f"Bluetooth device {self._address} is not connected; "
                "pair/connect it first"
            )

        def _open_channel() -> tuple[Any, int]:
            channel_obj = (
                IOBluetoothRFCOMMChannel.alloc()
                .initWithDevice_andClassicPeer_channelID_delegate_(
                    device, classic_peer, channel_id, delegate
                )
            )
            if channel_obj is None:
                return None, -1
            channel_obj.setupRFCOMMChannelForDevice()
            return channel_obj, int(channel_obj.waitforChanneOpen())

        channel_obj, ret = await loop.run_in_executor(None, _open_channel)
        if channel_obj is None or ret != 0 or not channel_obj.isOpen():
            if channel_obj is not None:
                try:
                    channel_obj.closeChannel()
                except Exception:
                    pass
            raise OSError(
                f"failed to open RFCOMM channel {channel_id} on "
                f"{self._address}: IOBluetooth error 0x{ret & 0xFFFFFFFF:08x}"
            )
        self._channel_id = channel_id
        self._rfcomm_channel = channel_obj
        self._reader = rx_reader
        self._start_recv()


    async def send(self, frame: bytes) -> None:
        if self._closed or self._rfcomm_channel is None:
            raise TransportClosed("macOS RFCOMM channel is not open")
        loop = asyncio.get_running_loop()
        data = _FRAME_LEN.pack(len(frame)) + frame
        await loop.run_in_executor(
            None,
            lambda: self._rfcomm_channel.writeSync_length_(data, len(data)),
        )

    async def close(self) -> None:
        channel = self._rfcomm_channel
        self._rfcomm_channel = None
        if channel is not None:
            try:
                channel.closeChannel()
            except Exception:
                pass
        await super().close()
