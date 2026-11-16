"""Loopback-only OpenAI-compatible HTTP facade for the LLM relay."""
from __future__ import annotations

import asyncio
import json
from typing import List, Optional, Tuple

from aiohttp import web

from .errors import NotSupportedError, RemoteError
from .peer import Peer
from .services.llm import MAX_REQUEST_SIZE, RelayError, relay, relay_models
from .telemetry import TrafficMeter


def create_app(
    peer: Peer,
    models: Optional[List[str]] = None,
    *,
    meter: Optional[TrafficMeter] = None,
) -> web.Application:
    """Create a proxy app. The caller must bind it to loopback only.

    When *models* is non-empty it is advertised verbatim. Otherwise the proxy
    asks the host for the upstream model catalog on each request, caching the
    last successful response so a transient BLE or backend failure does not
    make ``/v1/models`` disappear.
    """
    app = web.Application(client_max_size=MAX_REQUEST_SIZE)
    model_ids = list(models or [])
    cached_models: Optional[Tuple[str, bytes]] = None

    async def list_models(request: web.Request) -> web.Response:
        nonlocal cached_models
        if "Origin" in request.headers:
            raise web.HTTPForbidden(text="browser origins are not supported")
        if model_ids:
            return web.json_response({"object": "list", "data": [
                {"id": model, "object": "model", "owned_by": "gti"} for model in model_ids
            ]})
        try:
            status, content_type, body = await relay_models(peer)
            if not 200 <= status < 300:
                raise RelayError(f"upstream model list returned HTTP {status}")
            if not content_type.lower().startswith("application/json"):
                raise RelayError("upstream model list is not JSON")
            json.loads(body)
            cached_models = (content_type, body)
            return web.Response(body=body, status=status,
                                headers={"Content-Type": content_type})
        except (RelayError, RemoteError, NotSupportedError, ConnectionError,
                asyncio.TimeoutError, ValueError) as exc:
            if cached_models is not None:
                content_type, body = cached_models
                return web.Response(body=body, headers={"Content-Type": content_type})
            return web.json_response(
                {"error": {"message": str(exc), "type": "gti_relay_error"}},
                status=502,
            )

    async def completions(request: web.Request) -> web.StreamResponse:
        if "Origin" in request.headers:
            raise web.HTTPForbidden(text="browser origins are not supported")
        if request.content_type != "application/json":
            raise web.HTTPUnsupportedMediaType(text="expected application/json")
        body = await request.read()
        try:
            if not isinstance(json.loads(body), dict):
                raise ValueError("expected an object")
        except (ValueError, UnicodeDecodeError):
            raise web.HTTPBadRequest(text="expected a JSON object") from None

        response: Optional[web.StreamResponse] = None
        records = relay(peer, body, meter=meter)
        task = asyncio.current_task()

        async def watch_disconnect() -> None:
            # Detect disconnect even while waiting for the first token or BLE ACK.
            while True:
                await asyncio.sleep(0.2)
                transport = request.transport
                if transport is None or transport.is_closing():
                    if task is not None:
                        task.cancel()
                    return

        watcher = asyncio.create_task(watch_disconnect())
        try:
            async for kind, data in records:
                if kind == "header":
                    try:
                        meta = json.loads(data)
                        status = meta["status"]
                        content_type = meta["content_type"]
                        if (not isinstance(status, int) or not 200 <= status <= 599
                                or not isinstance(content_type, str)
                                or "\r" in content_type or "\n" in content_type):
                            raise ValueError("invalid response metadata")
                    except (ValueError, TypeError, KeyError):
                        raise RelayError("invalid inference response headers") from None
                    response = web.StreamResponse(status=status, headers={"Content-Type": content_type})
                    await response.prepare(request)
                else:
                    if response is None:
                        raise RelayError("missing inference response headers")
                    await response.write(data)
            if response is None:
                raise RelayError("missing inference response headers")
            return response
        except (RelayError, RemoteError, NotSupportedError, ConnectionError,
                asyncio.TimeoutError) as exc:
            if response is not None and response.prepared:
                # An unfinished chunked HTTP response must not look successful.
                if request.transport is not None:
                    request.transport.close()
                return response
            return web.json_response({"error": {"message": str(exc), "type": "gti_relay_error"}}, status=502)
        finally:
            watcher.cancel()
            await asyncio.gather(watcher, return_exceptions=True)
            await records.aclose()

    app.router.add_get("/v1/models", list_models)
    app.router.add_post("/v1/chat/completions", completions)
    return app
