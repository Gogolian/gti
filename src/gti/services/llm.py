"""OpenAI Chat Completions relay over a gti peer.

Version 2 uses bounded flow control in both directions. Uploads are sent in
large chunks; response data is accumulated into short batches, compressed when
that helps, and sent with a small sliding window. This keeps BLE round trips
and JSON/SSE overhead from dominating the link while preserving streaming
latency. HTTP bodies, including SSE and tool-call deltas, are never interpreted
or rewritten. The host alone chooses the upstream URL and credentials.

The same service also answers a small ``models`` operation used by the proxy to
mirror the inference server's model catalog. That response is passed through
verbatim, so a llama.cpp router or another multi-model backend stays usable.
"""
from __future__ import annotations

import asyncio
import json
import zlib
from typing import AsyncGenerator, AsyncIterator, Optional, Tuple
from urllib.parse import urlsplit

from aiohttp import ClientError, ClientResponse, ClientSession, ClientTimeout

from ..peer import Peer, RequestContext
from ..telemetry import TrafficMeter
from .base import LLM_SERVICE_ID

CHUNK_SIZE = 2048
UPLOAD_CHUNK_SIZE = 16 * 1024
RESPONSE_BATCH_SIZE = 4 * 1024
RESPONSE_BATCH_INTERVAL = 0.10
RESPONSE_WINDOW = 2
MAX_REQUEST_SIZE = 8 * 1024 * 1024
MAX_RESPONSE_DATA = RESPONSE_BATCH_SIZE
FLOW_TIMEOUT = 120.0
MODELS_TIMEOUT = 30.0
COMPRESSION_LEVEL = 6
COMPRESSION_MIN = 512

class RelayError(Exception):
    """The inference relay encountered an invalid or incomplete stream."""


async def _next(iterator: AsyncIterator[bytes], timeout: float = FLOW_TIMEOUT) -> bytes:
    try:
        return await asyncio.wait_for(iterator.__anext__(), timeout)
    except StopAsyncIteration:
        raise RelayError("unexpected end of relay stream") from None


def _relay_payload(op: str = "chat") -> bytes:
    if op == "chat":
        return b'{"version":2}'
    return json.dumps({"version": 2, "op": op}, separators=(",", ":")).encode()


def _valid_content_type(value: object) -> bool:
    return (
        isinstance(value, str)
        and "\r" not in value
        and "\n" not in value
    )


def _parse_header(record: bytes) -> Tuple[int, str]:
    if not record.startswith(b"H"):
        raise RelayError("missing inference response headers")
    try:
        meta = json.loads(record[1:])
        status = meta["status"]
        content_type = meta["content_type"]
        if not isinstance(status, int) or not 200 <= status <= 599:
            raise ValueError("invalid status")
        if not _valid_content_type(content_type):
            raise ValueError("invalid content type")
    except (ValueError, TypeError, KeyError) as exc:
        raise RelayError("invalid inference response headers") from exc
    return status, content_type


def _encode_record(raw_prefix: bytes, compressed_prefix: bytes, data: bytes) -> bytes:
    """Compress a record when it is large enough to be worth the CPU."""
    if len(data) >= COMPRESSION_MIN:
        compressed = zlib.compress(data, COMPRESSION_LEVEL)
        if len(compressed) < len(data):
            return compressed_prefix + compressed
    return raw_prefix + data


def _decode_record(
    record: bytes,
    *,
    raw_prefix: bytes,
    compressed_prefix: Optional[bytes],
    max_size: int,
    what: str,
) -> bytes:
    prefix = record[:1]
    if prefix == raw_prefix:
        data = record[1:]
        if not data or len(data) > max_size:
            raise RelayError(f"invalid {what} chunk")
        return data
    if prefix == compressed_prefix:
        try:
            decompressor = zlib.decompressobj()
            data = decompressor.decompress(record[1:], max_size + 1)
            if (decompressor.unconsumed_tail or decompressor.unused_data
                    or not decompressor.eof or not data or len(data) > max_size):
                raise RelayError(f"invalid compressed {what} chunk")
            return data
        except zlib.error:
            raise RelayError(f"invalid compressed {what} chunk") from None
    raise RelayError(f"invalid {what} chunk")


async def _batches(
    iterator: AsyncIterator[bytes],
    *,
    size: int,
    interval: float,
) -> AsyncGenerator[bytes, None]:
    """Coalesce upstream chunks by size or elapsed time.

    Flush the first bytes immediately, then bound batches by size and by time
    since their first byte. Keep at most one read in flight; a timed flush must
    not cancel that read (which could close the upstream iterator). Backpressure
    is left to aiohttp rather than an extra reader with its own queue.
    """
    loop = asyncio.get_running_loop()
    pending: Optional[asyncio.Future[bytes]] = None
    batch = bytearray()
    deadline = 0.0
    first = True
    try:
        while True:
            if pending is None:
                pending = asyncio.ensure_future(iterator.__anext__())
            if batch:
                ready, _ = await asyncio.wait(
                    [pending], timeout=max(0.0, deadline - loop.time())
                )
                if not ready:
                    data = bytes(batch)
                    batch.clear()
                    yield data
                    continue
            try:
                chunk = await pending
            except StopAsyncIteration:
                if batch:
                    yield bytes(batch)
                return
            finally:
                pending = None
            if not chunk:
                continue
            if first:
                first = False
                yield chunk[:size]
                chunk = chunk[size:]
            while chunk:
                if not batch:
                    deadline = loop.time() + interval
                take = min(size - len(batch), len(chunk))
                batch.extend(chunk[:take])
                chunk = chunk[take:]
                if len(batch) == size:
                    data = bytes(batch)
                    batch.clear()
                    yield data
    finally:
        if pending is not None:
            pending.cancel()
            await asyncio.gather(pending, return_exceptions=True)


class LLMService:
    """Serve requests using a configured OpenAI-compatible inference server."""

    def __init__(
        self,
        base_url: str,
        *,
        api_key: Optional[str] = None,
        meter: Optional[TrafficMeter] = None,
    ) -> None:
        parsed = urlsplit(base_url)
        if (parsed.scheme not in ("http", "https") or not parsed.hostname
                or parsed.username is not None or parsed.password is not None
                or parsed.query or parsed.fragment):
            raise ValueError("base URL must be an HTTP(S) URL without credentials, query or fragment")
        base = base_url.rstrip("/")
        self._url = base + "/chat/completions"
        self._models_url = base + "/models"
        self._api_key = api_key
        self._meter = meter

    def _headers(self, *, accept: str) -> dict:
        headers = {"Accept": accept, "Accept-Encoding": "identity"}
        if self._api_key:
            headers["Authorization"] = "Bearer " + self._api_key
        return headers

    async def handle(self, payload: bytes, ctx: RequestContext) -> None:
        try:
            request = json.loads(payload)
        except (ValueError, UnicodeDecodeError):
            raise RelayError("invalid LLM relay protocol") from None
        if not isinstance(request, dict) or request.get("version") not in (1, 2):
            raise RelayError("unsupported LLM relay protocol (upgrade both GTI peers)")
        version = request["version"]
        operation = request.get("op", "chat")

        meter = self._meter if operation == "chat" else None
        if meter is not None:
            meter.begin()
        try:
            if operation == "chat":
                await self._handle_chat(ctx, version=version)
            elif operation == "models":
                await self._handle_models(ctx, version=version)
            else:
                raise RelayError("unsupported LLM relay operation")
        finally:
            if meter is not None:
                meter.end()

    async def _handle_chat(self, ctx: RequestContext, *, version: int) -> None:
        await ctx.ack()
        incoming = ctx.incoming().__aiter__()
        body = bytearray()
        while True:
            part = await _next(incoming)
            if part == b"E":
                break
            data = _decode_record(
                part,
                raw_prefix=b"U",
                compressed_prefix=b"u" if version == 2 else None,
                max_size=UPLOAD_CHUNK_SIZE if version == 2 else CHUNK_SIZE,
                what="upload",
            )
            body.extend(data)
            if len(body) > MAX_REQUEST_SIZE:
                raise RelayError("request body exceeds relay limit")
            if self._meter is not None:
                self._meter.add(up=len(part))
            await ctx.emit(b"A")

        try:
            obj = json.loads(body)
            if not isinstance(obj, dict):
                raise ValueError("expected an object")
        except (ValueError, UnicodeDecodeError):
            raise RelayError("invalid JSON request") from None

        headers = self._headers(accept="application/json")
        headers["Content-Type"] = "application/json"
        timeout = ClientTimeout(total=None, sock_connect=30, sock_read=300)
        try:
            async with ClientSession(timeout=timeout) as session:
                async with session.post(self._url, data=bytes(body), headers=headers,
                                        allow_redirects=False) as response:
                    await self._stream_response(response, incoming, ctx, version=version)
        except (ClientError, asyncio.TimeoutError):
            # Avoid forwarding URLs, credentials or raw backend exception text.
            raise RelayError("inference backend connection failed or timed out") from None

    async def _handle_models(self, ctx: RequestContext, *, version: int) -> None:
        await ctx.ack()
        incoming = ctx.incoming().__aiter__()
        timeout = ClientTimeout(total=MODELS_TIMEOUT, sock_connect=MODELS_TIMEOUT,
                                sock_read=MODELS_TIMEOUT)
        try:
            async with ClientSession(timeout=timeout) as session:
                async with session.get(self._models_url, headers=self._headers(accept="application/json"),
                                       allow_redirects=False) as response:
                    await self._stream_response(response, incoming, ctx, version=version)
        except (ClientError, asyncio.TimeoutError):
            raise RelayError("inference backend connection failed or timed out") from None

    async def _stream_response(self, response: ClientResponse, incoming: AsyncIterator[bytes],
                               ctx: RequestContext, *, version: int) -> None:
        meta = json.dumps(
            {
                "status": response.status,
                "content_type": response.headers.get("Content-Type", "application/json"),
            },
            separators=(",", ":"),
        ).encode()
        await ctx.emit(b"H" + meta)
        if await _next(incoming) != b"A":
            raise RelayError("invalid response acknowledgement")

        chunks = response.content.iter_chunked(CHUNK_SIZE)
        batches = _batches(chunks, size=RESPONSE_BATCH_SIZE, interval=RESPONSE_BATCH_INTERVAL)
        source = batches if version == 2 else chunks
        window = RESPONSE_WINDOW if version == 2 else 1
        pending = 0
        try:
            async for data in source:
                if pending >= window:
                    await self._wait_acks(incoming, 1)
                    pending -= 1
                record = _encode_record(b"D", b"d", data) if version == 2 else b"D" + data
                if self._meter is not None:
                    self._meter.add(down=len(record))
                await ctx.emit(record)
                pending += 1
            if pending:
                await self._wait_acks(incoming, pending)
            await ctx.emit(b"Z", end=True)
        finally:
            await batches.aclose()

    @staticmethod
    async def _wait_acks(incoming: AsyncIterator[bytes], count: int) -> None:
        for _ in range(count):
            if await _next(incoming) != b"A":
                raise RelayError("invalid response acknowledgement")


async def relay(
    peer: Peer,
    body: bytes,
    *,
    meter: Optional[TrafficMeter] = None,
) -> AsyncGenerator[Tuple[str, bytes], None]:
    """Yield (header|data, bytes) records; cancel remote work on early exit."""
    if not body or len(body) > MAX_REQUEST_SIZE:
        raise RelayError("request body is empty or exceeds relay limit")
    call = await peer.open_call(LLM_SERVICE_ID, _relay_payload("chat"))
    complete = False
    if meter is not None:
        meter.begin()
    try:
        await call.wait_ack(FLOW_TIMEOUT)
        for offset in range(0, len(body), UPLOAD_CHUNK_SIZE):
            chunk = body[offset:offset + UPLOAD_CHUNK_SIZE]
            record = _encode_record(b"U", b"u", chunk)
            if meter is not None:
                meter.add(up=len(record))
            await call.send_chunk(record)
            if await _next(call) != b"A":
                raise RelayError("invalid upload acknowledgement")
        await call.send_chunk(b"E")
        header = await _next(call, timeout=600.0)
        _parse_header(header)
        yield "header", header[1:]
        await call.send_chunk(b"A")
        while True:
            record = await _next(call, timeout=600.0)
            if record == b"Z":
                complete = True
                break
            data = _decode_record(
                record,
                raw_prefix=b"D",
                compressed_prefix=b"d",
                max_size=MAX_RESPONSE_DATA,
                what="inference response",
            )
            if meter is not None:
                meter.add(down=len(record))
            # Acknowledge before yielding so the host can keep its window full
            # while the local HTTP client consumes the previous batch.
            await call.send_chunk(b"A")
            yield "data", data
    finally:
        if meter is not None:
            meter.end()
        if complete:
            call.close()
        else:
            await call.cancel()


async def relay_models(peer: Peer, *, timeout: float = MODELS_TIMEOUT) -> Tuple[int, str, bytes]:
    """Fetch the host's upstream model catalog without rebuilding it locally."""
    call = await peer.open_call(LLM_SERVICE_ID, _relay_payload("models"))
    complete = False
    try:
        await call.wait_ack(FLOW_TIMEOUT)
        status, content_type = _parse_header(await _next(call, timeout=timeout))
        await call.send_chunk(b"A")
        body = bytearray()
        while True:
            record = await _next(call, timeout=timeout)
            if record == b"Z":
                complete = True
                break
            data = _decode_record(
                record,
                raw_prefix=b"D",
                compressed_prefix=b"d",
                max_size=MAX_RESPONSE_DATA,
                what="model list",
            )
            await call.send_chunk(b"A")
            body.extend(data)
            if len(body) > MAX_REQUEST_SIZE:
                raise RelayError("model list exceeds relay limit")
        return status, content_type, bytes(body)
    finally:
        if complete:
            call.close()
        else:
            await call.cancel()
