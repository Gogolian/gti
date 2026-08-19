"""Symmetric multiplexing peer.

One :class:`Peer` wraps a :class:`~gti.transport.Transport` and provides
bidirectional, multiplexed request/response over it.  Either side can initiate
requests; a single writer task drains the outbound queue so frames are never
interleaved at the byte level.

Request IDs are split by *role* to prevent collisions when both peers initiate
concurrently:

* ``role="client"`` uses IDs  1 … 2³¹−1
* ``role="server"`` uses IDs  2³¹ … 2³²−1

This ensures that an EVENT arriving at a peer is unambiguously routed: IDs from
the remote side (upload data) land in ``_server_inflight``; IDs from our own
allocator (response data) land in ``_client_pending``.
"""

from __future__ import annotations

import asyncio
import itertools
from typing import AsyncIterator, Awaitable, Callable, Dict, Optional

from .errors import NotSupportedError, RemoteError
from .protocol import Envelope, Flags, Kind, Status, PROTOCOL_VERSION
from .transport import Transport

__all__ = ["Peer", "RequestContext"]

ServiceHandler = Callable[["bytes", "RequestContext"], Awaitable[None]]

_DONE = object()
_SERVER_ID_BASE = 1 << 31


class _ClientEntry:
    """State for one of our outgoing requests."""

    __slots__ = ("ack_future", "events")

    def __init__(self) -> None:
        loop = asyncio.get_running_loop()
        self.ack_future: "asyncio.Future[Envelope]" = loop.create_future()
        self.events: "asyncio.Queue[object]" = asyncio.Queue()


class _ServerEntry:
    """State for one incoming request from the peer."""

    __slots__ = ("incoming", "task")

    def __init__(self) -> None:
        self.incoming: "asyncio.Queue[object]" = asyncio.Queue()
        self.task: "Optional[asyncio.Task[None]]" = None


class _Call:
    """Handle for a single outgoing request, returned by :meth:`Peer.open_call`.

    Supports uploading data chunks to the server (``send_chunk``) and iterating
    response EVENTs from the server (``async for payload in call``).
    """

    def __init__(self, peer: "Peer", request_id: int, entry: _ClientEntry) -> None:
        self._peer = peer
        self.request_id = request_id
        self._entry = entry
        self._upload_seq = 0

    async def wait_ack(self, timeout: float = 30.0) -> Status:
        """Await the peer's ACK; raises on NOT_SUPPORTED or ERROR."""
        try:
            env = await asyncio.wait_for(self._entry.ack_future, timeout)
        except asyncio.TimeoutError:
            raise asyncio.TimeoutError(
                f"timeout waiting for ACK on request {self.request_id}"
            )
        if env.status == Status.NOT_SUPPORTED:
            raise NotSupportedError(
                f"service {env.service_id} is not supported by peer"
            )
        if env.status == Status.ERROR:
            raise RemoteError(env.payload.decode(errors="replace"))
        return env.status

    async def send_chunk(self, payload: bytes, *, end: bool = False) -> None:
        """Upload a data chunk (client→server EVENT) for this request."""
        flags = Flags.END if end else Flags.NONE
        env = Envelope(
            version=PROTOCOL_VERSION,
            kind=Kind.EVENT,
            status=Status.OK,
            flags=flags,
            service_id=0,
            request_id=self.request_id,
            seq=self._upload_seq,
            payload=payload,
        )
        self._upload_seq += 1
        await self._peer._enqueue(env)

    def __aiter__(self) -> "_Call":
        return self

    async def __anext__(self) -> bytes:
        item = await self._entry.events.get()
        if item is _DONE:
            raise StopAsyncIteration
        if isinstance(item, Exception):
            raise item  # type: ignore[misc]
        assert isinstance(item, Envelope)
        return item.payload


class RequestContext:
    """Lets a service handler send ACK, emit response events, or signal errors."""

    def __init__(
        self,
        peer: "Peer",
        request_id: int,
        service_id: int,
        entry: _ServerEntry,
    ) -> None:
        self._peer = peer
        self._request_id = request_id
        self._service_id = service_id
        self._entry = entry
        self._seq = 0

    async def ack(self, status: Status = Status.IN_PROGRESS) -> None:
        """Send an ACK frame back to the requester."""
        env = Envelope(
            version=PROTOCOL_VERSION,
            kind=Kind.ACK,
            status=status,
            flags=Flags.NONE,
            service_id=self._service_id,
            request_id=self._request_id,
            seq=0,
            payload=b"",
        )
        await self._peer._enqueue(env)

    async def emit(self, payload: bytes, *, end: bool = False) -> None:
        """Send a response EVENT; pass ``end=True`` on the final frame."""
        flags = Flags.END if end else Flags.NONE
        env = Envelope(
            version=PROTOCOL_VERSION,
            kind=Kind.EVENT,
            status=Status.OK,
            flags=flags,
            service_id=self._service_id,
            request_id=self._request_id,
            seq=self._seq,
            payload=payload,
        )
        self._seq += 1
        await self._peer._enqueue(env)

    async def error(self, message: str) -> None:
        """Send an ERROR frame, aborting this request on the caller's side."""
        env = Envelope(
            version=PROTOCOL_VERSION,
            kind=Kind.ERROR,
            status=Status.ERROR,
            flags=Flags.END,
            service_id=self._service_id,
            request_id=self._request_id,
            seq=0,
            payload=message.encode(),
        )
        await self._peer._enqueue(env)

    async def incoming(self) -> AsyncIterator[bytes]:
        """Async-iterate upload EVENTs sent by the requester (for file upload)."""
        while True:
            item = await self._entry.incoming.get()
            if item is _DONE:
                return
            if isinstance(item, Exception):
                raise item  # type: ignore[misc]
            yield item  # type: ignore[misc]


class Peer:
    """Bidirectional symmetric multiplexing peer.

    Usage::

        async with Peer(transport, role="client") as peer:
            peer.register(SERVICE_ID, handler)
            async for chunk in peer.call(SERVICE_ID, payload):
                ...
    """

    def __init__(
        self,
        transport: Transport,
        *,
        role: str = "client",
    ) -> None:
        self._transport = transport
        self._send_queue: "asyncio.Queue[Envelope]" = asyncio.Queue()
        self._client_pending: Dict[int, _ClientEntry] = {}
        self._server_inflight: Dict[int, _ServerEntry] = {}
        self._handlers: Dict[int, ServiceHandler] = {}
        id_base = _SERVER_ID_BASE if role == "server" else 1
        self._id_counter = itertools.count(id_base)
        self._writer_task: "Optional[asyncio.Task[None]]" = None

    def _alloc_id(self) -> int:
        return next(self._id_counter)

    async def _enqueue(self, env: Envelope) -> None:
        await self._send_queue.put(env)

    async def _drain_queue(self) -> None:
        try:
            while True:
                env = await self._send_queue.get()
                await self._transport.send(env.encode())
        except asyncio.CancelledError:
            pass

    async def _route(self, frame: bytes) -> None:
        try:
            env = Envelope.decode(frame)
        except Exception:
            return  # drop malformed frames silently

        if env.kind == Kind.REQUEST:
            await self._handle_request(env)

        elif env.kind == Kind.ACK:
            entry = self._client_pending.get(env.request_id)
            if entry is not None and not entry.ack_future.done():
                entry.ack_future.set_result(env)

        elif env.kind == Kind.EVENT:
            # High-bit role split: our IDs are in _client_pending;
            # the peer's IDs (opposite half) are in _server_inflight.
            s_entry = self._server_inflight.get(env.request_id)
            if s_entry is not None:
                # Upload event from requester to handler.
                await s_entry.incoming.put(env.payload)
                if env.flags & Flags.END:
                    await s_entry.incoming.put(_DONE)
            else:
                c_entry = self._client_pending.get(env.request_id)
                if c_entry is not None:
                    await c_entry.events.put(env)
                    if env.flags & Flags.END:
                        await c_entry.events.put(_DONE)

        elif env.kind == Kind.ERROR:
            entry = self._client_pending.get(env.request_id)
            if entry is not None:
                exc = RemoteError(env.payload.decode(errors="replace"))
                if not entry.ack_future.done():
                    entry.ack_future.set_exception(exc)
                else:
                    await entry.events.put(exc)
                    await entry.events.put(_DONE)

        elif env.kind == Kind.CANCEL:
            s_entry = self._server_inflight.get(env.request_id)
            if s_entry is not None and s_entry.task is not None:
                s_entry.task.cancel()

    async def _handle_request(self, env: Envelope) -> None:
        handler = self._handlers.get(env.service_id)
        s_entry = _ServerEntry()
        self._server_inflight[env.request_id] = s_entry

        if handler is None:
            ack = Envelope(
                version=PROTOCOL_VERSION,
                kind=Kind.ACK,
                status=Status.NOT_SUPPORTED,
                flags=Flags.NONE,
                service_id=env.service_id,
                request_id=env.request_id,
                seq=0,
                payload=b"",
            )
            await self._enqueue(ack)
            del self._server_inflight[env.request_id]
            return

        ctx = RequestContext(self, env.request_id, env.service_id, s_entry)

        async def _run() -> None:
            try:
                await handler(env.payload, ctx)
            except Exception as exc:
                try:
                    await ctx.error(str(exc))
                except Exception:
                    pass
            finally:
                self._server_inflight.pop(env.request_id, None)

        task = asyncio.create_task(_run())
        s_entry.task = task

    def register(self, service_id: int, handler: ServiceHandler) -> None:
        """Register a service handler for *service_id*."""
        self._handlers[service_id] = handler

    async def open_call(
        self,
        service_id: int,
        payload: bytes,
    ) -> _Call:
        """Send a REQUEST and return a :class:`_Call` for streaming/uploading."""
        rid = self._alloc_id()
        entry = _ClientEntry()
        self._client_pending[rid] = entry
        env = Envelope(
            version=PROTOCOL_VERSION,
            kind=Kind.REQUEST,
            status=Status.OK,
            flags=Flags.NONE,
            service_id=service_id,
            request_id=rid,
            seq=0,
            payload=payload,
        )
        await self._enqueue(env)
        return _Call(self, rid, entry)

    async def call(
        self,
        service_id: int,
        payload: bytes,
        *,
        timeout: float = 30.0,
    ) -> AsyncIterator[bytes]:
        """Send a REQUEST, await ACK, then yield response EVENT payloads.

        Raises :exc:`~gti.errors.NotSupportedError` or
        :exc:`~gti.errors.RemoteError` before the first yield if the peer
        rejects the request.
        """
        call_handle = await self.open_call(service_id, payload)
        try:
            await call_handle.wait_ack(timeout)
            async for chunk in call_handle:
                yield chunk
        finally:
            self._client_pending.pop(call_handle.request_id, None)

    async def start(self) -> None:
        """Start the transport and writer task."""
        await self._transport.start()
        self._transport.on_receive(self._route)
        self._writer_task = asyncio.create_task(self._drain_queue())

    async def stop(self) -> None:
        """Stop the writer task and close the transport."""
        if self._writer_task is not None:
            self._writer_task.cancel()
            try:
                await self._writer_task
            except asyncio.CancelledError:
                pass
            self._writer_task = None
        for s_entry in list(self._server_inflight.values()):
            if s_entry.task is not None:
                s_entry.task.cancel()
        await self._transport.close()

    async def __aenter__(self) -> "Peer":
        await self.start()
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.stop()
