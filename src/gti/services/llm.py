"""OpenAI Chat Completions relay over a gti peer.

Version 1 uses stop-and-wait flow control in both directions. Every bounded
upload chunk and every response record is acknowledged by the other peer before
another is sent. This matters for BLE adapters whose send() only queues packets.
HTTP bodies, including SSE and tool-call deltas, are never interpreted or
rewritten. The host alone chooses the upstream URL and credentials.

The same service also answers a small ``models`` operation used by the proxy to
mirror the inference server's model catalog. That response is passed through
verbatim, so a llama.cpp router or another multi-model backend stays usable.
"""
from __future__ import annotations

import asyncio
import json
from typing import AsyncGenerator, AsyncIterator, Optional, Tuple
from urllib.parse import urlsplit

from aiohttp import ClientError, ClientResponse, ClientSession, ClientTimeout

from ..peer import Peer, RequestContext
from ..telemetry import TrafficMeter
from .base import LLM_SERVICE_ID

CHUNK_SIZE = 2048
MAX_REQUEST_SIZE = 8 * 1024 * 1024
FLOW_TIMEOUT = 120.0
MODELS_TIMEOUT = 30.0


class RelayError(Exception):
    """The inference relay encountered an invalid or incomplete stream."""


async def _next(iterator: AsyncIterator[bytes], timeout: float = FLOW_TIMEOUT) -> bytes:
    try:
        return await asyncio.wait_for(iterator.__anext__(), timeout)
    except StopAsyncIteration:
        raise RelayError("unexpected end of relay stream") from None


def _relay_payload(op: str = "chat") -> bytes:
    if op == "chat":
        return b'{"version":1}'
    return json.dumps({"version": 1, "op": op}, separators=(",", ":")).encode()


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
        if not isinstance(request, dict) or request.get("version") != 1:
            raise RelayError("unsupported LLM relay protocol")
        operation = request.get("op", "chat")

        meter = self._meter if operation == "chat" else None
        if meter is not None:
            meter.begin()
        try:
            if operation == "chat":
                await self._handle_chat(ctx)
            elif operation == "models":
                await self._handle_models(ctx)
            else:
                raise RelayError("unsupported LLM relay operation")
        finally:
            if meter is not None:
                meter.end()

    async def _handle_chat(self, ctx: RequestContext) -> None:
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
            if self._meter is not None:
                self._meter.add(up=len(part) - 1)
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
                    await self._stream_response(response, incoming, ctx)
        except (ClientError, asyncio.TimeoutError):
            # Avoid forwarding URLs, credentials or raw backend exception text.
            raise RelayError("inference backend connection failed or timed out") from None

    async def _handle_models(self, ctx: RequestContext) -> None:
        await ctx.ack()
        incoming = ctx.incoming().__aiter__()
        timeout = ClientTimeout(total=MODELS_TIMEOUT, sock_connect=MODELS_TIMEOUT,
                                sock_read=MODELS_TIMEOUT)
        try:
            async with ClientSession(timeout=timeout) as session:
                async with session.get(self._models_url, headers=self._headers(accept="application/json"),
                                       allow_redirects=False) as response:
                    await self._stream_response(response, incoming, ctx)
        except (ClientError, asyncio.TimeoutError):
            raise RelayError("inference backend connection failed or timed out") from None

    async def _stream_response(self, response: ClientResponse, incoming: AsyncIterator[bytes],
                               ctx: RequestContext) -> None:
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
        async for chunk in response.content.iter_chunked(CHUNK_SIZE):
            if self._meter is not None:
                self._meter.add(down=len(chunk))
            await ctx.emit(b"D" + chunk)
            if await _next(incoming) != b"A":
                raise RelayError("invalid response acknowledgement")
        await ctx.emit(b"Z", end=True)


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
        for offset in range(0, len(body), CHUNK_SIZE):
            chunk = body[offset:offset + CHUNK_SIZE]
            if meter is not None:
                meter.add(up=len(chunk))
            await call.send_chunk(b"U" + chunk)
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
            if not record.startswith(b"D") or len(record) <= 1 or len(record) > CHUNK_SIZE + 1:
                raise RelayError("invalid inference response chunk")
            data = record[1:]
            if meter is not None:
                meter.add(down=len(data))
            yield "data", data
            await call.send_chunk(b"A")
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
            if not record.startswith(b"D") or len(record) <= 1 or len(record) > CHUNK_SIZE + 1:
                raise RelayError("invalid model list chunk")
            body.extend(record[1:])
            if len(body) > MAX_REQUEST_SIZE:
                raise RelayError("model list exceeds relay limit")
            await call.send_chunk(b"A")
        return status, content_type, bytes(body)
    finally:
        if complete:
            call.close()
        else:
            await call.cancel()
