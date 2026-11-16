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

When the transport closes (or a send fails), every in-flight request is failed
with :class:`ConnectionError` instead of hanging forever.
"""

from __future__ import annotations

import asyncio
import itertools
import logging
from typing import Any, AsyncIterator, Awaitable, Callable, Dict, Optional

from .errors import NotSupportedError, RemoteError
from .protocol import Envelope, Flags, Kind, Status, PROTOCOL_VERSION
from .transport import Transport, TransportClosed

__all__ = ["Peer", "RequestContext"]

logger = logging.getLogger(__name__)

ServiceHandler = Callable[["bytes", "RequestContext"], Awaitable[None]]

_DONE = object()
_SERVER_ID_BASE = 1 << 31


def _consume_future_exception(future: "asyncio.Future[Any]") -> None:
    """Mark a future's exception as retrieved so abandoned calls stay quiet."""
    if not future.cancelled():
        future.exception()


class _ClientEntry:
    """State for one of our outgoing requests."""

    __slots__ = ("ack_future", "events")

    def __init__(self) -> None:
        loop = asyncio.get_running_loop()
        self.ack_future: "asyncio.Future[Envelope]" = loop.create_future()
        self.ack_future.add_done_callback(_consume_future_exception)
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

    def close(self) -> None:
        """Release local state for this call (does not notify the peer)."""
        self._peer._client_pending.pop(self.request_id, None)

    async def cancel(self) -> None:
        """Ask the peer to stop this request and release local state."""
        if not self._peer.is_closed:
            env = Envelope(
                version=PROTOCOL_VERSION,
                kind=Kind.CANCEL,
                status=Status.OK,
                flags=Flags.NONE,
                service_id=0,
                request_id=self.request_id,
                seq=0,
                payload=b"",
            )
            try:
                await self._peer._enqueue(env)
            except ConnectionError:
                pass
        self.close()
        if not self._entry.ack_future.done():
            self._entry.ack_future.cancel()
        await self._entry.events.put(_DONE)

    def __aiter__(self) -> "_Call":
        return self

    async def __anext__(self) -> bytes:
        item = await self._entry.events.get()
        if item is _DONE:
            raise StopAsyncIteration
        if isinstance(item, Exception):
            raise item
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
                raise item
            assert isinstance(item, bytes)
            yield item


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
        if role not in ("client", "server"):
            raise ValueError(f"role must be 'client' or 'server', got {role!r}")
        self._transport = transport
        self._send_queue: "asyncio.Queue[Envelope]" = asyncio.Queue()
        self._client_pending: Dict[int, _ClientEntry] = {}
        self._server_inflight: Dict[int, _ServerEntry] = {}
        self._handlers: Dict[int, ServiceHandler] = {}
        id_base = _SERVER_ID_BASE if role == "server" else 1
        self._id_counter = itertools.count(id_base)
        self._writer_task: "Optional[asyncio.Task[None]]" = None
        self._closed = False
        self._stopped = False
        self._closed_event = asyncio.Event()
        self._failure: Optional[BaseException] = None

    @property
    def is_closed(self) -> bool:
        return self._closed

    @property
    def failure(self) -> Optional[BaseException]:
        return self._failure

    def _alloc_id(self) -> int:
        return next(self._id_counter)

    async def _enqueue(self, env: Envelope) -> None:
        if self._closed:
            raise ConnectionError("peer is closed")
        await self._send_queue.put(env)

    def _fail_all(self, exc: BaseException) -> None:
        """Fail every in-flight request with *exc* and clear the maps."""
        for entry in list(self._client_pending.values()):
            if not entry.ack_future.done():
                entry.ack_future.set_exception(exc)
            entry.events.put_nowait(exc)
            entry.events.put_nowait(_DONE)
        self._client_pending.clear()

        for s_entry in list(self._server_inflight.values()):
            if s_entry.task is not None:
                s_entry.task.cancel()
            s_entry.incoming.put_nowait(exc)
            s_entry.incoming.put_nowait(_DONE)
        self._server_inflight.clear()

    async def _on_transport_close(self, exc: Optional[BaseException] = None) -> None:
        """Handle link loss: fail in-flight work and wake ``wait_closed``."""
        if self._closed:
            return
        self._closed = True
        self._failure = exc or ConnectionError("connection closed")
        self._fail_all(self._failure)
        self._closed_event.set()
        writer = self._writer_task
        if writer is not None and writer is not asyncio.current_task():
            writer.cancel()

    async def _drain_queue(self) -> None:
        try:
            while True:
                env = await self._send_queue.get()
                try:
                    await self._transport.send(env.encode())
                except asyncio.CancelledError:
                    raise
                except (TransportClosed, ConnectionError) as exc:
                    logger.debug("transport send failed: %s", exc)
                    await self._on_transport_close(exc)
                    return
                except Exception as exc:
                    logger.exception("transport send failed")
                    await self._on_transport_close(exc)
                    return
        except asyncio.CancelledError:
            pass

    async def _route(self, frame: bytes) -> None:
        if self._closed:
            return
        try:
            env = Envelope.decode(frame)
        except Exception:
            logger.debug("dropping malformed frame", exc_info=True)
            return

        try:
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
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("failed to route frame")

    async def _handle_request(self, env: Envelope) -> None:
        if self._closed:
            return
        handler = self._handlers.get(env.service_id)

        # A duplicate request id means the peer reused an id; cancel the old
        # handler so its state cannot leak.
        existing = self._server_inflight.get(env.request_id)
        if existing is not None and existing.task is not None:
            existing.task.cancel()

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
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                try:
                    await ctx.error(str(exc))
                except Exception:
                    pass
            finally:
                if self._server_inflight.get(env.request_id) is s_entry:
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
        if self._closed:
            raise ConnectionError("peer is closed")
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
        try:
            await self._enqueue(env)
        except BaseException:
            self._client_pending.pop(rid, None)
            entry.ack_future.cancel()
            raise
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
        complete = False
        try:
            await call_handle.wait_ack(timeout)
            async for chunk in call_handle:
                yield chunk
            complete = True
        finally:
            if complete:
                call_handle.close()
            else:
                await call_handle.cancel()

    async def start(self) -> None:
        """Start the transport and writer task."""
        if self._closed:
            raise ConnectionError("peer is closed")
        if self._writer_task is not None:
            return
        # Register callbacks *before* starting the transport: some adapters
        # begin reading (and dispatching) inside start(), and a frame arriving
        # before the callback is set would be silently dropped.
        self._transport.on_receive(self._route)
        self._transport.on_close(self._on_transport_close)
        try:
            await self._transport.start()
        except BaseException:
            await self._transport.close()
            raise
        self._writer_task = asyncio.create_task(self._drain_queue())

    async def stop(self) -> None:
        """Stop the writer task, fail in-flight work, and close the transport."""
        if self._stopped:
            return
        self._stopped = True
        if self._writer_task is not None:
            self._writer_task.cancel()
            try:
                await self._writer_task
            except asyncio.CancelledError:
                pass
            self._writer_task = None
        await self._on_transport_close(ConnectionError("peer stopped"))
        try:
            await self._transport.close()
        except Exception:
            logger.exception("transport close failed")

    async def wait_closed(self) -> None:
        """Wait until the link closes (or the peer is stopped)."""
        await self._closed_event.wait()

    async def __aenter__(self) -> "Peer":
        await self.start()
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.stop()
