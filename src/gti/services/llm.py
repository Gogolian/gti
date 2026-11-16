"""OpenAI Chat Completions relay over a gti peer.

Version 1 uses stop-and-wait flow control in both directions. Every bounded
upload chunk and every response record is acknowledged by the other peer before
another is sent. This matters for BLE adapters whose send() only queues packets.
HTTP bodies, including SSE and tool-call deltas, are never interpreted or
rewritten. The host alone chooses the upstream URL and credentials.
"""
from __future__ import annotations

import asyncio
import json
from typing import AsyncGenerator, AsyncIterator, Optional, Tuple
from urllib.parse import urlsplit

from aiohttp import ClientError, ClientSession, ClientTimeout

from ..peer import Peer, RequestContext
from .base import LLM_SERVICE_ID

CHUNK_SIZE = 2048
MAX_REQUEST_SIZE = 8 * 1024 * 1024
FLOW_TIMEOUT = 120.0


class RelayError(Exception):
    """The inference relay encountered an invalid or incomplete stream."""


async def _next(iterator: AsyncIterator[bytes], timeout: float = FLOW_TIMEOUT) -> bytes:
    try:
        return await asyncio.wait_for(iterator.__anext__(), timeout)
    except StopAsyncIteration:
        raise RelayError("unexpected end of relay stream") from None


class LLMService:
    """Serve requests using a configured OpenAI-compatible inference server."""

    def __init__(self, base_url: str, *, api_key: Optional[str] = None) -> None:
        parsed = urlsplit(base_url)
        if (parsed.scheme not in ("http", "https") or not parsed.hostname
                or parsed.username is not None or parsed.password is not None
                or parsed.query or parsed.fragment):
            raise ValueError("base URL must be an HTTP(S) URL without credentials, query or fragment")
        self._url = base_url.rstrip("/") + "/chat/completions"
        self._api_key = api_key

    async def handle(self, payload: bytes, ctx: RequestContext) -> None:
        if payload != b'{"version":1}':
            raise RelayError("unsupported LLM relay protocol")
        await ctx.ack()
        incoming = ctx.incoming().__aiter__()
        body = bytearray()
        while True:
            part = await _next(incoming)
            if part == b"E":
                break
            if not part.startswith(b"U") or len(part) == 1 or len(part) > CHUNK_SIZE + 1:
                raise RelayError("invalid upload chunk")
            body.extend(part[1:])
            if len(body) > MAX_REQUEST_SIZE:
                raise RelayError("request body exceeds relay limit")
            await ctx.emit(b"A")

        try:
            obj = json.loads(body)
            if not isinstance(obj, dict):
                raise ValueError("expected an object")
        except (ValueError, UnicodeDecodeError):
            raise RelayError("invalid JSON request") from None

        headers = {"Content-Type": "application/json", "Accept-Encoding": "identity"}
        if self._api_key:
            headers["Authorization"] = "Bearer " + self._api_key
        timeout = ClientTimeout(total=None, sock_connect=30, sock_read=300)
        try:
            async with ClientSession(timeout=timeout) as session:
                async with session.post(self._url, data=bytes(body), headers=headers,
                                        allow_redirects=False) as response:
                    meta = json.dumps({"status": response.status,
                                       "content_type": response.headers.get("Content-Type", "application/json")},
                                      separators=(",", ":")).encode()
                    await ctx.emit(b"H" + meta)
                    if await _next(incoming) != b"A":
                        raise RelayError("invalid response acknowledgement")
                    async for chunk in response.content.iter_chunked(CHUNK_SIZE):
                        await ctx.emit(b"D" + chunk)
                        if await _next(incoming) != b"A":
                            raise RelayError("invalid response acknowledgement")
                    await ctx.emit(b"Z", end=True)
        except (ClientError, asyncio.TimeoutError):
            # Avoid forwarding URLs, credentials or raw backend exception text.
            raise RelayError("inference backend connection failed or timed out") from None


async def relay(peer: Peer, body: bytes) -> AsyncGenerator[Tuple[str, bytes], None]:
    """Yield (header|data, bytes) records; cancel remote work on early exit."""
    if not body or len(body) > MAX_REQUEST_SIZE:
        raise RelayError("request body is empty or exceeds relay limit")
    call = await peer.open_call(LLM_SERVICE_ID, b'{"version":1}')
    complete = False
    try:
        await call.wait_ack(FLOW_TIMEOUT)
        for offset in range(0, len(body), CHUNK_SIZE):
            await call.send_chunk(b"U" + body[offset:offset + CHUNK_SIZE])
            if await _next(call) != b"A":
                raise RelayError("invalid upload acknowledgement")
        await call.send_chunk(b"E")
        header = await _next(call, timeout=600.0)
        if not header.startswith(b"H"):
            raise RelayError("missing inference response headers")
        yield "header", header[1:]
        await call.send_chunk(b"A")
        while True:
            record = await _next(call, timeout=600.0)
            if record == b"Z":
                complete = True
                break
            if not record.startswith(b"D") or len(record) <= 1 or len(record) > CHUNK_SIZE + 1:
                raise RelayError("invalid inference response chunk")
            yield "data", record[1:]
            await call.send_chunk(b"A")
    finally:
        if complete:
            call.close()
        else:
            await call.cancel()
