"""Connection supervisor tests using a two-device simulated radio."""
from __future__ import annotations

import asyncio
import unittest
from typing import Dict, Optional, Tuple

from aiohttp import ClientSession, web

from gti.config import Config
from gti.connection import ConnectionManager
from gti.llm_proxy import create_app
from gti.transport import InMemoryTransport, Transport, create_loopback


class Radio:
    def __init__(self) -> None:
        self.waiting: Dict[Tuple[str, str], RadioTransport] = {}
        self.blocked: "set[Tuple[str, str]]" = set()

    def factory(self, name: str):
        return lambda role: RadioTransport(self, name, role)

    async def join(self, transport: "RadioTransport") -> InMemoryTransport:
        name, role = transport.name, transport.role
        if (name, role) in self.blocked:
            return await transport.connected
        other = "b" if name == "a" else "a"
        other_role = "listen" if role == "connect" else "connect"
        remote = self.waiting.pop((other, other_role), None)
        if remote is None:
            self.waiting[(name, role)] = transport
            try:
                return await transport.connected
            finally:
                if self.waiting.get((name, role)) is transport:
                    del self.waiting[(name, role)]
        local_link, remote_link = create_loopback()
        remote.connected.set_result(remote_link)
        return local_link


class RadioTransport(Transport):
    def __init__(self, radio: Radio, name: str, role: str) -> None:
        super().__init__()
        self.radio, self.name, self.role = radio, name, role
        self.connected: "asyncio.Future[InMemoryTransport]" = asyncio.get_running_loop().create_future()
        self.link: Optional[InMemoryTransport] = None

    async def start(self) -> None:
        self.link = await self.radio.join(self)
        self.link.on_receive(self._dispatch)
        self.link.on_close(self._notify_close)
        await self.link.start()

    async def send(self, frame: bytes) -> None:
        assert self.link is not None
        await self.link.send(frame)

    async def close(self) -> None:
        if self.link is not None:
            await self.link.close()
        if not self.connected.done():
            self.connected.cancel()
        await self._notify_close()


async def wait_for(predicate, timeout=4.0):
    async def poll():
        while not predicate():
            await asyncio.sleep(0.01)
    await asyncio.wait_for(poll(), timeout)


class TestConnectionManager(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.radio = Radio()
        self.messages = []
        self.a = ConnectionManager(
            Config("Alice", "0" * 32), factory=self.radio.factory("a"),
            on_message=lambda name, text: self.messages.append(("a", name, text)),
            retry_delay=0.02, heartbeat_interval=0.05, heartbeat_timeout=1, handshake_timeout=1,
            fallback_delay=0.1,
        )
        self.b = ConnectionManager(
            Config("Bob", "f" * 32), factory=self.radio.factory("b"),
            on_message=lambda name, text: self.messages.append(("b", name, text)),
            retry_delay=0.02, heartbeat_interval=0.05, heartbeat_timeout=1, handshake_timeout=1,
            fallback_delay=0.1,
        )
        self.a.start()
        self.b.start()

    async def asyncTearDown(self) -> None:
        await asyncio.gather(self.a.stop(), self.b.stop())
        self.assertFalse(self.radio.waiting)

    async def test_crossed_connect_chat_capability_and_reconnect(self) -> None:
        await wait_for(lambda: self.a.peer is not None and self.b.peer is not None)
        self.assertEqual(self.a.peer_info.name, "Bob")
        self.assertEqual(self.b.peer_info.name, "Alice")
        self.assertEqual(self.a.peer._transport.role, "connect")
        self.assertEqual(self.b.peer._transport.role, "listen")
        await self.a.send_chat("hello")
        await self.b.send_chat("hi")
        self.assertEqual(self.messages, [("b", "Alice", "hello"), ("a", "Bob", "hi")])
        self.a.set_host_inference(True)
        await wait_for(lambda: self.b.peer_info is not None and self.b.peer_info.host_inference)
        old_a, old_b = self.a.peer, self.b.peer
        await old_a._transport.close()
        await wait_for(lambda: self.a.peer is not None and self.b.peer is not None
                       and self.a.peer is not old_a and self.b.peer is not old_b)
        await self.b.send_chat("back")
        self.assertIn(("a", "Bob", "back"), self.messages)

    async def test_inference_proxy_survives_reconnect(self) -> None:
        async def completion(request):
            body = await request.json()
            return web.json_response({"echo": body["model"]})

        upstream = web.Application()
        upstream.router.add_post("/v1/chat/completions", completion)
        upstream_runner = web.AppRunner(upstream)
        await upstream_runner.setup()
        upstream_site = web.TCPSite(upstream_runner, "127.0.0.1", 0)
        await upstream_site.start()
        upstream_port = upstream_site._server.sockets[0].getsockname()[1]
        self.b.config = Config("Bob", "f" * 32, inference_url=f"http://127.0.0.1:{upstream_port}/v1")
        self.b.set_host_inference(True)
        runner = web.AppRunner(create_app(lambda: self.a.peer))
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]
        try:
            await wait_for(lambda: self.a.peer is not None and self.b.peer is not None)
            async with ClientSession() as session:
                async def send():
                    async with session.post(f"http://127.0.0.1:{port}/v1/chat/completions",
                                            json={"model": "test"}) as response:
                        self.assertEqual(response.status, 200)
                        self.assertEqual(await response.json(), {"echo": "test"})
                await send()
                old_a, old_b = self.a.peer, self.b.peer
                await old_a._transport.close()
                await wait_for(lambda: self.a.peer is not None and self.b.peer is not None
                               and self.a.peer is not old_a and self.b.peer is not old_b)
                await send()
        finally:
            await runner.cleanup()
            await upstream_runner.cleanup()

    async def test_reverse_direction_fallback(self) -> None:
        await asyncio.gather(self.a.stop(), self.b.stop())
        self.radio.blocked = {("a", "connect"), ("b", "listen")}
        self.a.start()
        self.b.start()
        await wait_for(lambda: self.a.peer is not None and self.b.peer is not None)
        self.assertEqual(self.a.peer._transport.role, "listen")
        self.assertEqual(self.b.peer._transport.role, "connect")
        await self.a.send_chat("fallback")
        self.assertIn(("b", "Alice", "fallback"), self.messages)

    async def test_offline_send_fails(self) -> None:
        await self.b.stop()
        await wait_for(lambda: self.a.peer is None)
        with self.assertRaises(ConnectionError):
            await self.a.send_chat("offline")
