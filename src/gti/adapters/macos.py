"""macOS Bluetooth Classic RFCOMM transport via IOBluetooth/pyobjc.

macOS has no kernel BT socket API, so this drives ``IOBluetoothRFCOMMChannel``
through pyobjc.  A daemon thread runs an NSRunLoop for IOBluetooth callbacks;
received bytes are fed into an ``asyncio.StreamReader`` via
``call_soon_threadsafe``; ``send()`` calls ``writeSync:length:`` in an executor.

Requires: ``pyobjc-core`` (``pip install pyobjc-core``).
"""

from __future__ import annotations

import asyncio
import struct
import sys
import threading
from typing import Any, Optional

from .channel import RFCOMMStreamTransport
from ..transport import TransportClosed

__all__ = ["MacOSRFCOMMHostTransport", "MacOSRFCOMMConsumerTransport"]

_FRAME_LEN = struct.Struct(">I")
_INCOMING = 1  # kIOBluetoothUserNotificationChannelDirectionIncoming


def _require_macos() -> None:
    if sys.platform != "darwin":
        raise RuntimeError(
            "MacOSRFCOMMTransport is only supported on macOS; "
            "use LinuxRFCOMMTransport on Linux or WindowsRFCOMMTransport on Windows"
        )


def _load_iobluetooth() -> dict:
    try:
        import objc
    except ImportError as exc:
        raise ImportError(
            "pyobjc-core is required for macOS RFCOMM support (pip install pyobjc-core)"
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
    from Foundation import NSObject

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
    """Host-side RFCOMM server via IOBluetoothSDPServiceRecord."""

    def __init__(self) -> None:
        super().__init__()
        _require_macos()
        self._channel_id: int = 0
        self._rfcomm_channel: Any = None
        self._open_notification: Any = None

    @property
    def channel_id(self) -> int:
        return self._channel_id

    async def start(self) -> None:
        ns = _load_iobluetooth()
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

            sdp = {
                "0001 - ServiceClassIDList": ["00001101-0000-1000-8000-00805F9B34FB"],
                "0004 - ProtocolDescriptorList": [["L2CAP"], ["RFCOMM", 0]],
                "0100 - ServiceName*": "gti",
            }
            record = IOBluetoothSDPServiceRecord.publishedServiceRecord_(sdp)
            if record is None:
                bt_error.append("IOBluetooth: failed to publish SPP service record")
                bt_ready.set()
                return
            ret, ch_id = record.getRFCOMMChannelID_(None)
            if ret != 0 or not ch_id:
                bt_error.append(
                    f"IOBluetooth: getRFCOMMChannelID failed (ret=0x{ret:08x})"
                )
                bt_ready.set()
                return
            self._channel_id = int(ch_id)
            self._open_notification = (
                IOBluetoothRFCOMMChannel
                .registerForChannelOpenNotifications_selector_withChannelID_direction_(
                    delegate,
                    b"rfcommChannelOpened:channel:",
                    self._channel_id,
                    _INCOMING,
                )
            )
            bt_ready.set()
            rl = NSRunLoop.currentRunLoop()
            while not self._closed:
                rl.runUntilDate_(NSDate.dateWithTimeIntervalSinceNow_(0.25))

        t = threading.Thread(target=_bt_thread, daemon=True, name="gti-rfcomm-host")
        t.start()
        await loop.run_in_executor(None, bt_ready.wait)
        if bt_error:
            raise OSError(bt_error[0])
        await connected.wait()
        self._rfcomm_channel = delegate._channel_ref
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


class MacOSRFCOMMConsumerTransport(RFCOMMStreamTransport):
    """Consumer-side RFCOMM connection on macOS."""

    def __init__(self, address: str, channel_id: int = 1) -> None:
        super().__init__()
        _require_macos()
        self._address = address
        self._channel_id = channel_id
        self._rfcomm_channel: Any = None

    async def start(self) -> None:
        ns = _load_iobluetooth()
        IOBluetoothDevice = ns["IOBluetoothDevice"]
        loop = asyncio.get_running_loop()
        rx_reader: asyncio.StreamReader = asyncio.StreamReader()
        connected: asyncio.Event = asyncio.Event()

        Delegate = _get_delegate_class()
        delegate = Delegate.alloc().init()
        delegate._setup(loop, rx_reader, on_open=connected)

        device = IOBluetoothDevice.deviceWithAddressString_(self._address)
        if device is None:
            raise OSError(f"Bluetooth device not found: {self._address!r}")

        ret, channel = device.openRFCOMMChannelSync_withChannelID_delegate_(
            None, self._channel_id, delegate
        )
        if ret != 0 or channel is None:
            raise OSError(f"openRFCOMMChannelSync failed (ret=0x{ret:08x})")

        self._rfcomm_channel = channel
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
