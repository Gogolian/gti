"""Windows Bluetooth Classic RFCOMM transport (AF_BTH/BTHPROTO_RFCOMM)."""

from __future__ import annotations

import asyncio
import socket
import sys

from .channel import RFCOMMStreamTransport

__all__ = ["WindowsRFCOMMHostTransport", "WindowsRFCOMMConsumerTransport"]

_DEFAULT_CHANNEL = 1
_AF_BTH: int = getattr(socket, "AF_BTH", 32)
_BTHPROTO_RFCOMM: int = getattr(socket, "BTHPROTO_RFCOMM", 3)
_BDADDR_ANY = "00:00:00:00:00:00"


def _require_windows() -> None:
    if sys.platform != "win32":
        raise RuntimeError(
            "WindowsRFCOMMTransport is only supported on Windows; "
            "use LinuxRFCOMMTransport on Linux or MacOSRFCOMMTransport on macOS"
        )


class WindowsRFCOMMHostTransport(RFCOMMStreamTransport):
    """Listening RFCOMM host on Windows."""

    def __init__(self, channel: int = _DEFAULT_CHANNEL) -> None:
        super().__init__()
        _require_windows()
        self._channel = channel
        self._srv_sock: socket.socket | None = None

    @property
    def channel(self) -> int:
        return self._channel

    @property
    def port(self) -> int:
        """Alias for :attr:`channel` (Windows calls it an RFCOMM port)."""
        return self._channel

    async def start(self) -> None:
        loop = asyncio.get_running_loop()
        srv = socket.socket(_AF_BTH, socket.SOCK_STREAM, _BTHPROTO_RFCOMM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.setblocking(False)
        try:
            srv.bind((_BDADDR_ANY, self._channel))
            srv.listen(1)
        except OSError as exc:
            srv.close()
            raise OSError(
                f"failed to listen on RFCOMM channel {self._channel}: {exc}"
            ) from exc
        self._srv_sock = srv
        try:
            conn, _addr = await loop.sock_accept(srv)
        finally:
            srv.close()
            self._srv_sock = None
        conn.setblocking(False)
        reader, writer = await asyncio.open_connection(sock=conn)
        self._attach(reader, writer)
        self._start_recv()

    async def close(self) -> None:
        if self._srv_sock is not None:
            try:
                self._srv_sock.close()
            except OSError:
                pass
            self._srv_sock = None
        await super().close()


class WindowsRFCOMMConsumerTransport(RFCOMMStreamTransport):
    """Connecting RFCOMM consumer on Windows."""

    def __init__(self, address: str, channel: int = _DEFAULT_CHANNEL) -> None:
        super().__init__()
        _require_windows()
        self._address = address
        self._channel = channel

    @property
    def channel(self) -> int:
        return self._channel

    @property
    def port(self) -> int:
        """Alias for :attr:`channel` (Windows calls it an RFCOMM port)."""
        return self._channel

    async def start(self) -> None:
        loop = asyncio.get_running_loop()
        sock = socket.socket(_AF_BTH, socket.SOCK_STREAM, _BTHPROTO_RFCOMM)
        sock.setblocking(False)
        try:
            await loop.sock_connect(sock, (self._address, self._channel))
            reader, writer = await asyncio.open_connection(sock=sock)
        except asyncio.CancelledError:
            sock.close()
            raise
        except Exception as exc:
            sock.close()
            raise OSError(
                f"failed to connect to {self._address} on RFCOMM channel "
                f"{self._channel}: {exc}"
            ) from exc
        self._attach(reader, writer)
        self._start_recv()
