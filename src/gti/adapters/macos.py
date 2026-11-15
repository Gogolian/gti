"""macOS Bluetooth Classic RFCOMM transport via IOBluetooth/pyobjc.

macOS has no kernel BT socket API, so this drives ``IOBluetoothRFCOMMChannel``
through pyobjc.  A daemon thread runs an NSRunLoop for IOBluetooth callbacks;
received bytes are fed into an ``asyncio.StreamReader`` via
``call_soon_threadsafe``; ``send()`` calls ``writeSync:length:`` in an executor.

Requires: ``pyobjc-framework-Cocoa`` (``pip install pyobjc-framework-Cocoa``).
"""

from __future__ import annotations

import asyncio
import json
import logging
import multiprocessing
import re
import struct
import sys
import threading
from multiprocessing.connection import Connection
from typing import Any, Optional

from .channel import RFCOMMStreamTransport
from ..transport import Transport, TransportClosed

__all__ = ["MacOSRFCOMMHostTransport", "MacOSRFCOMMConsumerTransport"]

logger = logging.getLogger(__name__)

_FRAME_LEN = struct.Struct(">I")
_ANY_DIRECTION = 0  # kIOBluetoothUserNotificationChannelDirectionAny

# Raw SDP data-element encodings.  gti advertises its own service UUID so a
# client can find the gti record even when the peer also exposes other SPP
# services (macOS often has a built-in "COM5" SPP record).
_SPP_UUID_BYTES = bytes.fromhex("0000110100001000800000805f9b34fb")
_GTI_UUID_BYTES = bytes.fromhex("e6031152a17a4f769aa5cba1b4321dcf")
_L2CAP_UUID_BYTES = bytes.fromhex("0100")
_RFCOMM_UUID_BYTES = bytes.fromhex("0003")
_PUBLIC_BROWSE_GROUP_BYTES = bytes.fromhex("1002")


def _sdp_dictionary(requested_channel: int) -> dict:
    """SDP record for the gti RFCOMM service."""
    return {
        "0001 - ServiceClassIDList": [_SPP_UUID_BYTES, _GTI_UUID_BYTES],
        "0004 - ProtocolDescriptorList": [
            [_L2CAP_UUID_BYTES],
            [
                _RFCOMM_UUID_BYTES,
                {
                    "DataElementSize": 1,
                    "DataElementType": 1,
                    "DataElementValue": requested_channel,
                },
            ],
        ],
        "0005 - BrowseGroupList*": [_PUBLIC_BROWSE_GROUP_BYTES],
        "0009 - BluetoothProfileDescriptorList": [
            [
                _SPP_UUID_BYTES,
                {
                    "DataElementSize": 2,
                    "DataElementType": 1,
                    "DataElementValue": 0x0102,
                },
            ]
        ],
        "0100 - ServiceName*": "gti",
    }


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
            logger.warning("incoming RFCOMM channel opened: %s", channel)
            channel.setDelegate_(self)
            self._channel_ref = channel
            if self._on_open is not None:
                self._loop.call_soon_threadsafe(self._on_open.set)

    _DelegateClass = _RFCOMMDelegate
    return _DelegateClass


def _host_process(conn: Connection, requested_channel: int) -> None:
    """Child process: publish SDP, run the NSRunLoop on the main thread, proxy bytes."""
    import objc
    from Foundation import NSDate, NSObject, NSRunLoop

    def _send_control(**payload: Any) -> None:
        conn.send_bytes(b"C" + json.dumps(payload).encode())

    try:
        ns: dict = {}
        objc.loadBundle(
            "IOBluetooth",
            bundle_path="/System/Library/Frameworks/IOBluetooth.framework",
            module_globals=ns,
        )
        _require_bluetooth_on(ns)
        IOBluetoothSDPServiceRecord = ns["IOBluetoothSDPServiceRecord"]
        IOBluetoothRFCOMMChannel = ns["IOBluetoothRFCOMMChannel"]

        record = None
        try:
            record = IOBluetoothSDPServiceRecord.publishedServiceRecordWithDictionary_(
                _sdp_dictionary(requested_channel)
            )
        except Exception as exc:
            logger.debug("SDP publishing failed: %s", exc)

        channel_id = requested_channel
        if record is not None:
            match = re.search(r"RFCOMM ChannelID:\s*(\d+)", str(record))
            if match is not None:
                channel_id = int(match.group(1))

        class _HostDelegate(NSObject):
            def rfcommChannelOpened_channel_(self, notification: Any, channel: Any) -> None:
                self.channel = channel
                channel.setDelegate_(self)
                try:
                    opened_channel = int(channel.getChannelID())
                except Exception:
                    opened_channel = channel_id
                _send_control(type="opened", channel=opened_channel)

            def rfcommChannelData_data_length_(
                self, channel: Any, data: Any, length: int
            ) -> None:
                conn.send_bytes(b"D" + bytes(data[:length]))

            def rfcommChannelClosed_(self, channel: Any) -> None:
                self.closed = True
                _send_control(type="closed")

        delegate = _HostDelegate.alloc().init()
        delegate.channel = None
        delegate.closed = False

        notification = (
            IOBluetoothRFCOMMChannel
            .registerForChannelOpenNotifications_selector_withChannelID_direction_(
                delegate,
                b"rfcommChannelOpened:channel:",
                0,  # any channel; SDP advertises the actual one
                0,  # kIOBluetoothUserNotificationChannelDirectionAny
            )
        )
        if notification is None:
            _send_control(type="error", message="failed to register for incoming channels")
            return

        _send_control(type="ready", channel=channel_id)

        def _reader() -> None:
            while True:
                try:
                    frame = conn.recv_bytes()
                except (EOFError, OSError):
                    break
                channel = delegate.channel
                if channel is not None:
                    try:
                        channel.writeSync_length_(frame, len(frame))
                    except Exception:
                        break
            channel = delegate.channel
            if channel is not None:
                try:
                    channel.closeChannel()
                except Exception:
                    pass
            delegate.closed = True

        threading.Thread(target=_reader, daemon=True, name="gti-rfcomm-writer").start()

        run_loop = NSRunLoop.currentRunLoop()
        while not delegate.closed:
            run_loop.runUntilDate_(NSDate.dateWithTimeIntervalSinceNow_(0.1))

        try:
            notification.unregister()
        except Exception:
            pass
        if record is not None:
            try:
                record.removeServiceRecord()
            except Exception:
                pass
    except Exception as exc:
        try:
            _send_control(type="error", message=str(exc))
        except Exception:
            pass
    finally:
        try:
            conn.close()
        except Exception:
            pass


class MacOSRFCOMMHostTransport(Transport):
    """Host-side RFCOMM server.

    The IOBluetooth host runs in a child process whose main thread owns the
    NSRunLoop.  This is required for the Bluetooth daemon to deliver incoming
    RFCOMM channel notifications on recent macOS releases, and it also keeps
    pyobjc crashes out of the main process.
    """

    def __init__(self, channel: int = 1) -> None:
        super().__init__()
        _require_macos()
        self._requested_channel = channel
        self._channel_id = channel
        self._conn: Optional[Connection] = None
        self._proc: Optional[Any] = None
        self._recv_task: Optional["asyncio.Task[None]"] = None
        self._opened_future: Optional["asyncio.Future[None]"] = None
        self._closed = False

    @property
    def channel(self) -> int:
        return self._channel_id

    @property
    def channel_id(self) -> int:
        """Alias for :attr:`channel` (IOBluetooth calls it a channel id)."""
        return self._channel_id

    @property
    def is_closed(self) -> bool:
        return self._closed

    async def start(self) -> None:
        if self._closed:
            raise TransportClosed("macOS RFCOMM host is closed")
        loop = asyncio.get_running_loop()
        ctx = multiprocessing.get_context("spawn")
        parent_conn, child_conn = ctx.Pipe(duplex=True)
        proc = ctx.Process(
            target=_host_process,
            args=(child_conn, self._requested_channel),
            daemon=True,
        )
        proc.start()
        child_conn.close()
        self._conn = parent_conn
        self._proc = proc

        def _wait_ready() -> Optional[bytes]:
            if not parent_conn.poll(15):
                return None
            return parent_conn.recv_bytes()

        try:
            data = await loop.run_in_executor(None, _wait_ready)
        except asyncio.CancelledError:
            await self.close()
            raise
        if data is None:
            await self.close()
            raise OSError("macOS RFCOMM host setup timed out")
        if not data.startswith(b"C"):
            await self.close()
            raise OSError("macOS RFCOMM host sent an invalid handshake")
        message = json.loads(data[1:].decode())
        if message.get("type") == "error":
            await self.close()
            raise OSError(message.get("message", "macOS RFCOMM host failed"))
        if message.get("type") != "ready":
            await self.close()
            raise OSError("macOS RFCOMM host sent an unexpected handshake")
        self._channel_id = int(message.get("channel", self._requested_channel))
        self._opened_future = loop.create_future()
        self._recv_task = asyncio.create_task(self._recv_loop())
        try:
            await self._opened_future
        except asyncio.CancelledError:
            await self.close()
            raise
        except Exception:
            await self.close()
            raise

    async def _recv_loop(self) -> None:
        loop = asyncio.get_running_loop()
        conn = self._conn
        if conn is None:
            return
        while not self._closed:
            try:
                data = await loop.run_in_executor(None, conn.recv_bytes)
            except (EOFError, OSError):
                break
            if not data:
                continue
            if data.startswith(b"D"):
                await self._dispatch(data[1:])
            elif data.startswith(b"C"):
                try:
                    message = json.loads(data[1:].decode())
                except ValueError:
                    continue
                if message.get("type") == "opened":
                    self._channel_id = int(
                        message.get("channel", self._channel_id)
                    )
                    future = self._opened_future
                    if future is not None and not future.done():
                        future.set_result(None)
                elif message.get("type") == "closed":
                    future = self._opened_future
                    if future is not None and not future.done():
                        future.set_exception(
                            ConnectionError("macOS RFCOMM channel closed")
                        )
                    break
                elif message.get("type") == "error":
                    future = self._opened_future
                    if future is not None and not future.done():
                        future.set_exception(
                            ConnectionError(
                                str(message.get("message", "macOS RFCOMM host failed"))
                            )
                        )
                    logger.error("macOS RFCOMM host: %s", message.get("message"))
                    break
        if not self._closed:
            self._closed = True
            await self._notify_close()

    async def send(self, frame: bytes) -> None:
        if self._closed or self._conn is None:
            raise TransportClosed("macOS RFCOMM host is not connected")
        loop = asyncio.get_running_loop()
        try:
            await loop.run_in_executor(None, self._conn.send_bytes, b"D" + frame)
        except (EOFError, OSError) as exc:
            raise TransportClosed(f"macOS RFCOMM host is not connected: {exc}") from exc

    async def close(self) -> None:
        if self._closed and self._conn is None and self._proc is None:
            return
        self._closed = True
        conn = self._conn
        self._conn = None
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
        future = self._opened_future
        self._opened_future = None
        if future is not None and not future.done():
            future.cancel()
        task = self._recv_task
        self._recv_task = None
        if task is not None:
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass
        proc = self._proc
        self._proc = None
        if proc is not None:
            proc.join(timeout=2)
            if proc.is_alive():
                proc.terminate()
                proc.join(timeout=2)
            if proc.is_alive():
                proc.kill()
        await self._notify_close()


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
        self._delegate: Any = None

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
        self._delegate = delegate

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
        self._delegate = None
        await super().close()
