"""Transport abstraction, 4-byte framing base, and in-memory loopback.

A :class:`Transport` is a bidirectional, frame-oriented byte pipe — one
``send()`` equals one receive on the other side.  :class:`StreamTransport`
adds 4-byte big-endian length-prefix framing over any asyncio stream
(RFCOMM socket, socketpair, etc.).  :class:`InMemoryTransport` is an
in-process loopback used by the test suite.
"""

from __future__ import annotations

import abc
import asyncio
import struct
from typing import Awaitable, Callable, List, Optional, Tuple

__all__ = [
    "Transport",
    "TransportClosed",
    "StreamTransport",
    "InMemoryTransport",
    "create_loopback",
]

ReceiveCallback = Callable[[bytes], Awaitable[None]]


class TransportClosed(Exception):
    """Raised when sending on a closed transport."""


class Transport(abc.ABC):
    """Bidirectional, frame-oriented byte pipe."""

    def __init__(self) -> None:
        self._receive_cb: Optional[ReceiveCallback] = None

    def on_receive(self, callback: ReceiveCallback) -> None:
        """Register the coroutine invoked for every received frame."""
        self._receive_cb = callback

    async def _dispatch(self, frame: bytes) -> None:
        if self._receive_cb is not None:
            await self._receive_cb(frame)

    @abc.abstractmethod
    async def start(self) -> None:
        """Bring the transport up (connect / listen / no-op for loopback)."""

    @abc.abstractmethod
    async def send(self, frame: bytes) -> None:
        """Send a single frame to the peer."""

    @abc.abstractmethod
    async def close(self) -> None:
        """Tear the transport down."""

    async def __aenter__(self) -> "Transport":
        await self.start()
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.close()


class StreamTransport(Transport):
    """4-byte big-endian length-prefix framing over an asyncio stream.

    Subclasses call :meth:`_attach` once the underlying stream is established,
    then :meth:`_start_recv` to begin the read loop.
    """

    _LEN_STRUCT = struct.Struct(">I")
    _LEN_SIZE = _LEN_STRUCT.size  # 4 bytes

    def __init__(self) -> None:
        super().__init__()
        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None
        self._recv_task: Optional["asyncio.Task[None]"] = None
        self._closed = False

    def _attach(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        self._reader = reader
        self._writer = writer

    def _start_recv(self) -> None:
        self._recv_task = asyncio.ensure_future(self._recv_loop())

    async def _recv_loop(self) -> None:
        assert self._reader is not None
        try:
            while not self._closed:
                len_bytes = await self._reader.readexactly(self._LEN_SIZE)
                (frame_len,) = self._LEN_STRUCT.unpack(len_bytes)
                frame_bytes = await self._reader.readexactly(frame_len)
                await self._dispatch(frame_bytes)
        except (asyncio.IncompleteReadError, ConnectionError, OSError):
            pass

    async def send(self, frame: bytes) -> None:
        if self._writer is None or self._closed:
            raise TransportClosed("stream transport is not connected")
        self._writer.write(self._LEN_STRUCT.pack(len(frame)) + frame)
        await self._writer.drain()

    async def close(self) -> None:
        self._closed = True
        if self._recv_task is not None:
            self._recv_task.cancel()
            try:
                await self._recv_task
            except (asyncio.CancelledError, Exception):
                pass
            self._recv_task = None
        if self._writer is not None:
            try:
                self._writer.close()
                await self._writer.wait_closed()
            except Exception:
                pass
            self._writer = None
        self._reader = None

    async def start(self) -> None:  # pragma: no cover
        raise NotImplementedError("subclasses implement start()")


class InMemoryTransport(Transport):
    """In-process loopback transport linked to a peer in the same process.

    Frames sent here are delivered to the peer's receive callback via
    ``asyncio.create_task`` so delivery is asynchronous, like a real link.
    """

    def __init__(self, name: str = "") -> None:
        super().__init__()
        self.name = name
        self._peer: Optional["InMemoryTransport"] = None
        self._closed = False
        #: Frames sent by this endpoint, in order (handy in assertions).
        self.sent: List[bytes] = []
        self._inflight: "set[asyncio.Task[None]]" = set()

    def link(self, peer: "InMemoryTransport") -> None:
        self._peer = peer

    async def start(self) -> None:
        self._closed = False

    async def send(self, frame: bytes) -> None:
        if self._closed:
            raise TransportClosed(f"transport {self.name!r} is closed")
        if self._peer is None:
            raise RuntimeError("transport is not linked to a peer")
        data = bytes(frame)
        self.sent.append(data)
        await self._peer._deliver(data)

    async def _deliver(self, frame: bytes) -> None:
        if self._closed:
            return
        task = asyncio.create_task(self._dispatch(frame))
        self._inflight.add(task)
        task.add_done_callback(self._inflight.discard)

    async def close(self) -> None:
        self._closed = True
        if self._inflight:
            await asyncio.gather(*self._inflight, return_exceptions=True)


def create_loopback(
    names: Tuple[str, str] = ("a", "b"),
) -> Tuple[InMemoryTransport, InMemoryTransport]:
    """Return a linked ``(a, b)`` in-memory transport pair."""
    a = InMemoryTransport(names[0])
    b = InMemoryTransport(names[1])
    a.link(b)
    b.link(a)
    return a, b
