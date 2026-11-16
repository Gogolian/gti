"""End-to-end inference relay tests, using HTTP and loopback peers (no radio)."""
from __future__ import annotations

import asyncio
import json
import random
import unittest

from aiohttp import ClientPayloadError, ClientSession, web

from gti.adapters.ble_framing import FrameAssembler, fragment_frame
from gti.errors import RemoteError
from gti.llm_proxy import create_app
from gti.peer import Peer
from gti.protocol import Envelope, Kind
from gti.services.base import CHAT_SERVICE_ID, LLM_SERVICE_ID
from gti.services.chat import ChatService
from gti.services.llm import UPLOAD_CHUNK_SIZE, LLMService, relay
from gti.transport import InMemoryTransport, TransportClosed, create_loopback


class TestLLMRelay(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.client_transport, host_transport = create_loopback()
        self.host_transport = host_transport
        self.client = Peer(self.client_transport, role="client")
        self.host = Peer(host_transport, role="server")
        await self.client.start()
        await self.host.start()
        self.runners = []
        self.session = ClientSession()

    async def asyncTearDown(self) -> None:
        await self.session.close()
        for runner in reversed(self.runners):
            await runner.cleanup()
        await self.client.stop()
        await self.host.stop()

    async def serve(self, app: web.Application) -> str:
        runner = web.AppRunner(app, handler_cancellation=True)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        self.runners.append(runner)
        port = site._server.sockets[0].getsockname()[1]
        return f"http://127.0.0.1:{port}"

    async def setup_relay(self, handler) -> str:
        upstream = web.Application()
        upstream.router.add_post("/v1/chat/completions", handler)
        base = await self.serve(upstream)
        svc = LLMService(base + "/v1", api_key="secret")
        self.host.register(LLM_SERVICE_ID, svc.handle)
        return await self.serve(create_app(self.client, ["model-x"]))

    async def test_stream_tool_calls_and_backpressure(self) -> None:
        received = []
        first_sent = asyncio.Event()
        finish = asyncio.Event()
        first = ('data: {"choices":[{"delta":{"content":"héllo"}}]}\n\n').encode()
        second = (b'data: {"choices":[{"delta":{"tool_calls":[{"index":0,'
                  b'"function":{"name":"read","arguments":"{}"}}]},'
                  b'"finish_reason":"tool_calls"}]}\n\ndata: [DONE]\n\n')

        async def upstream(request):
            received.append((await request.json(), request.headers.get("Authorization")))
            response = web.StreamResponse(headers={"Content-Type": "text/event-stream"})
            await response.prepare(request)
            await response.write(first)
            first_sent.set()
            await finish.wait()
            await response.write(second)
            return response

        base = await self.setup_relay(upstream)
        payload = {"model": "model-x", "stream": True, "messages": [{"role": "user", "content": "x" * 40000}],
                   "tools": [{"type": "function", "function": {"name": "read", "parameters": {"type": "object"}}}]}
        async with self.session.post(base + "/v1/chat/completions", json=payload) as response:
            self.assertEqual(response.status, 200)
            self.assertEqual(response.headers["Content-Type"], "text/event-stream")
            await asyncio.wait_for(first_sent.wait(), 2)
            self.assertEqual(await asyncio.wait_for(response.content.readexactly(len(first)), 2), first)
            finish.set()
            self.assertEqual(await response.read(), second)
        self.assertEqual(received, [(payload, "Bearer secret")])
        request_events = [Envelope.decode(f) for f in self.client_transport.sent
                          if Envelope.decode(f).kind == Kind.EVENT]
        uploads = [e.payload for e in request_events if e.payload[:1] in (b"U", b"u")]
        self.assertGreater(len(uploads), 1)
        self.assertTrue(all(len(x) <= UPLOAD_CHUNK_SIZE + 1 for x in uploads))
        self.assertTrue(any(x.startswith(b"u") for x in uploads))
        async with self.session.get(base + "/v1/models") as response:
            self.assertEqual((await response.json())["data"][0]["id"], "model-x")

    async def test_nonstream_and_http_error_passthrough(self) -> None:
        async def upstream(request):
            data = await request.json()
            if data.get("model") == "bad":
                return web.json_response({"error": {"message": "model not found"}}, status=404)
            return web.json_response({"choices": [{"message": {"content": "ok"}}]})
        base = await self.setup_relay(upstream)
        for model, status in [("good", 200), ("bad", 404)]:
            async with self.session.post(base + "/v1/chat/completions", json={"model": model}) as response:
                self.assertEqual(response.status, status)
                data = await response.json()
                self.assertIn("choices" if status == 200 else "error", data)

    async def test_disconnect_cancels_inference_before_headers(self) -> None:
        started = asyncio.Event()
        cancelled = asyncio.Event()
        async def upstream(request):
            started.set()
            try:
                await asyncio.sleep(60)
            except asyncio.CancelledError:
                cancelled.set()
                raise
            return web.Response()
        base = await self.setup_relay(upstream)
        task = asyncio.create_task(self.session.post(base + "/v1/chat/completions", json={"model": "x"}))
        await asyncio.wait_for(started.wait(), 2)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        await asyncio.wait_for(cancelled.wait(), 3)
        self.assertFalse(self.client._client_pending)
        self.assertFalse(self.host._server_inflight)

    async def test_disconnect_after_first_token_cancels_inference(self) -> None:
        cancelled = asyncio.Event()
        async def upstream(request):
            response = web.StreamResponse(headers={"Content-Type": "text/event-stream"})
            await response.prepare(request)
            await response.write(b"data: first\n\n")
            try:
                await asyncio.sleep(60)
            except asyncio.CancelledError:
                cancelled.set()
                raise
            return response
        base = await self.setup_relay(upstream)
        response = await self.session.post(base + "/v1/chat/completions", json={"stream": True})
        self.assertEqual(await asyncio.wait_for(response.content.readexactly(13), 2), b"data: first\n\n")
        response.close()
        await asyncio.wait_for(cancelled.wait(), 3)
        self.assertFalse(self.client._client_pending)
        self.assertFalse(self.host._server_inflight)

    async def test_peer_drop_before_headers_returns_error(self) -> None:
        started = asyncio.Event()
        async def upstream(request):
            started.set()
            await asyncio.sleep(60)
            return web.Response()
        base = await self.setup_relay(upstream)
        task = asyncio.create_task(self.session.post(base + "/v1/chat/completions", json={"model": "x"}))
        await asyncio.wait_for(started.wait(), 2)
        await self.host.stop()
        response = await asyncio.wait_for(task, 3)
        self.assertEqual(response.status, 502)
        response.close()

    async def test_aborted_backend_stream_is_not_successful(self) -> None:
        async def upstream(request):
            response = web.StreamResponse(headers={"Content-Type": "text/event-stream"})
            await response.prepare(request)
            await response.write(b'data: partial\n\n')
            request.transport.close()
            return response
        base = await self.setup_relay(upstream)
        async with self.session.post(base + "/v1/chat/completions", json={"stream": True}) as response:
            with self.assertRaises(ClientPayloadError):
                await asyncio.wait_for(response.read(), 3)

    async def test_input_validation_and_unsupported_peer(self) -> None:
        base = await self.serve(create_app(self.client))
        async with self.session.post(base + "/v1/chat/completions", data="no", headers={"Content-Type": "application/json"}) as response:
            self.assertEqual(response.status, 400)
        async with self.session.post(base + "/v1/chat/completions", json={}, headers={"Origin": "https://example.org"}) as response:
            self.assertEqual(response.status, 403)
        async with self.session.post(base + "/v1/chat/completions", json={}) as response:
            self.assertEqual(response.status, 502)
            self.assertIn("not supported", (await response.json())["error"]["message"])

    async def test_upload_waits_for_ack_and_cancels(self) -> None:
        got_chunk = asyncio.Event()
        cancelled = asyncio.Event()
        async def stalled(payload, ctx):
            await ctx.ack()
            try:
                async for chunk in ctx.incoming():
                    self.assertIn(chunk[:1], (b"U", b"u"))
                    got_chunk.set()
                    await asyncio.sleep(60)
            finally:
                cancelled.set()
        self.host.register(LLM_SERVICE_ID, stalled)
        stream = relay(self.client, b"x" * (UPLOAD_CHUNK_SIZE * 3))
        task = asyncio.create_task(stream.__anext__())
        await asyncio.wait_for(got_chunk.wait(), 2)
        await asyncio.sleep(0.05)
        frames = [Envelope.decode(f) for f in self.client_transport.sent]
        uploads = [f for f in frames if f.kind == Kind.EVENT and f.payload[:1] in (b"U", b"u")]
        self.assertEqual(len(uploads), 1)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        await asyncio.wait_for(cancelled.wait(), 2)
        self.assertFalse(self.client._client_pending)

    async def test_concurrent_requests_and_chat_interleave(self) -> None:
        started = asyncio.Event()
        release = asyncio.Event()
        messages = []

        async def upstream(request):
            data = await request.json()
            if data["model"] == "slow":
                started.set()
                await release.wait()
            return web.json_response({"model": data["model"]})

        base = await self.setup_relay(upstream)
        self.host.register(CHAT_SERVICE_ID, ChatService(on_message=messages.append).handle)
        slow = asyncio.create_task(self.session.post(base + "/v1/chat/completions", json={"model": "slow"}))
        await asyncio.wait_for(started.wait(), 2)
        await asyncio.wait_for(ChatService().send("hello", self.client), 2)
        async with self.session.post(base + "/v1/chat/completions", json={"model": "fast"}) as response:
            self.assertEqual((await response.json())["model"], "fast")
        self.assertEqual(messages, ["hello"])
        release.set()
        response = await asyncio.wait_for(slow, 2)
        self.assertEqual((await response.json())["model"], "slow")
        response.close()

    async def test_close_relay_generator_cancels_host(self) -> None:
        async def upstream(request):
            return web.Response(body=b"x" * 5000)
        await self.setup_relay(upstream)
        stream = relay(self.client, json.dumps({"model": "x"}).encode())
        self.assertEqual((await stream.__anext__())[0], "header")
        await stream.aclose()
        for _ in range(50):
            if not self.host._server_inflight:
                break
            await asyncio.sleep(0.01)
        self.assertFalse(self.host._server_inflight)
        self.assertFalse(self.client._client_pending)

    async def test_legacy_client_can_upload_and_receive_raw_response(self) -> None:
        expected = b"x" * 9000

        async def upstream(request):
            self.assertEqual(await request.json(), {"model": "legacy"})
            return web.Response(body=expected)

        await self.setup_relay(upstream)
        call = await self.client.open_call(LLM_SERVICE_ID, b'{"version":1}')
        try:
            await call.wait_ack()
            await call.send_chunk(b'U{"model":"legacy"}')
            self.assertEqual(await call.__anext__(), b"A")
            await call.send_chunk(b"E")
            self.assertTrue((await call.__anext__()).startswith(b"H"))
            await call.send_chunk(b"A")
            received = bytearray()
            async for record in call:
                if record == b"Z":
                    break
                self.assertEqual(record[:1], b"D")
                self.assertLessEqual(len(record), 2049)
                received.extend(record[1:])
                await call.send_chunk(b"A")
            self.assertEqual(received, expected)
        finally:
            await call.cancel()

    async def test_incompressible_response_is_preserved(self) -> None:
        rng = random.Random(42)
        expected = bytes(rng.randrange(256) for _ in range(30000))

        async def upstream(request):
            return web.Response(body=expected, content_type="application/octet-stream")

        base = await self.setup_relay(upstream)
        async with self.session.post(base + "/v1/chat/completions", json={}) as response:
            self.assertEqual(await response.read(), expected)
        records = [Envelope.decode(frame).payload for frame in self.host_transport.sent]
        self.assertTrue(any(record.startswith(b"D") for record in records))
        self.assertFalse(any(record.startswith(b"d") for record in records))

    async def test_unsupported_version_fails_before_upload(self) -> None:
        async def upstream(request):
            self.fail("unsupported version must not reach backend")

        await self.setup_relay(upstream)
        call = await self.client.open_call(LLM_SERVICE_ID, b'{"version":99}')
        try:
            with self.assertRaisesRegex(RemoteError, "unsupported LLM relay protocol"):
                await call.wait_ack()
        finally:
            await call.cancel()

    async def test_response_is_batched_and_compressed(self) -> None:
        async def upstream(request):
            response = web.StreamResponse(headers={"Content-Type": "text/event-stream"})
            await response.prepare(request)
            for _ in range(200):
                await response.write(b'data: {"choices":[{"delta":{"content":"hello"}}]}\n\n')
            return response

        base = await self.setup_relay(upstream)
        async with self.session.post(base + "/v1/chat/completions",
                                     json={"stream": True, "messages": []}) as response:
            body = await response.read()
        self.assertEqual(body.count(b"hello"), 200)

        frames = [Envelope.decode(frame) for frame in self.host_transport.sent]
        data_records = [
            frame.payload for frame in frames
            if frame.kind == Kind.EVENT and frame.payload[:1] in (b"D", b"d")
        ]
        self.assertGreater(len(data_records), 0)
        self.assertLess(len(data_records), 200)
        self.assertTrue(any(record.startswith(b"d") for record in data_records))

    async def test_models_discovered_from_host_and_cached(self) -> None:
        fail = False

        async def upstream_models(request):
            if fail:
                return web.json_response({"error": "backend down"}, status=503)
            return web.json_response({
                "object": "list",
                "data": [
                    {"id": "model-a", "context_length": 32768},
                    {"id": "model-b", "context_length": 8192},
                ],
            })

        async def upstream_chat(request):
            return web.json_response({"choices": []})

        upstream = web.Application()
        upstream.router.add_get("/v1/models", upstream_models)
        upstream.router.add_post("/v1/chat/completions", upstream_chat)
        base = await self.serve(upstream)
        self.host.register(LLM_SERVICE_ID, LLMService(base + "/v1").handle)
        proxy = await self.serve(create_app(self.client))

        async with self.session.get(proxy + "/v1/models") as response:
            self.assertEqual(response.status, 200)
            data = await response.json()
        self.assertEqual([model["id"] for model in data["data"]], ["model-a", "model-b"])
        self.assertEqual(data["data"][0]["context_length"], 32768)

        # A later backend failure should not make the catalog disappear.
        fail = True
        async with self.session.get(proxy + "/v1/models") as response:
            self.assertEqual(response.status, 200)
            self.assertEqual([model["id"] for model in (await response.json())["data"]],
                             ["model-a", "model-b"])


class FragmentedLoopback(InMemoryTransport):
    """Simulate small BLE packets, reassembling frames at the other end."""
    def __init__(self) -> None:
        super().__init__()
        self.assembler = FrameAssembler()

    async def send(self, frame: bytes) -> None:
        if self._closed or self._peer is None or self._peer.is_closed:
            raise TransportClosed("link closed")
        assert isinstance(self._peer, FragmentedLoopback)
        for packet in fragment_frame(frame, 20):
            await asyncio.sleep(0)
            for decoded in self._peer.assembler.feed(packet):
                await self._peer._dispatch(decoded)


class TestFragmentedRelay(unittest.IsolatedAsyncioTestCase):
    async def test_reverse_roles_with_fragmented_frames(self) -> None:
        async def upstream(request):
            payload = await request.read()
            return web.Response(body=payload, content_type="application/json")

        app = web.Application()
        app.router.add_post("/v1/chat/completions", upstream)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]
        a, b = FragmentedLoopback(), FragmentedLoopback()
        a.link(b)
        b.link(a)
        host, client = Peer(a, role="client"), Peer(b, role="server")
        host.register(LLM_SERVICE_ID, LLMService(f"http://127.0.0.1:{port}/v1").handle)
        body = json.dumps({"content": "héllo" * 1000}, ensure_ascii=False).encode()
        try:
            async with host, client:
                parts = [item async for item in relay(client, body)]
                self.assertEqual(b"".join(data for kind, data in parts if kind == "data"), body)
        finally:
            await runner.cleanup()


class TestLLMConfiguration(unittest.TestCase):
    def test_reject_unsafe_urls(self) -> None:
        for url in ("file:///etc/passwd", "http://user:pass@localhost/v1", "http://localhost/v1?key=x"):
            with self.assertRaises(ValueError):
                LLMService(url)
