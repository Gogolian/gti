"""Tests for ChatService end-to-end over in-memory loopback."""

from __future__ import annotations

import asyncio
import unittest

from gti.peer import Peer
from gti.services.chat import ChatService
from gti.transport import create_loopback


def _make_chat_peers():
    t_a, t_b = create_loopback()
    peer_a = Peer(t_a, role="client")
    peer_b = Peer(t_b, role="server")
    return peer_a, peer_b


class TestChatService(unittest.IsolatedAsyncioTestCase):
    async def test_send_receive(self) -> None:
        peer_a, peer_b = _make_chat_peers()
        received: list[str] = []
        chat_a = ChatService()
        chat_b = ChatService(on_message=received.append)
        peer_b.register(1, chat_b.handle)

        async with peer_a, peer_b:
            await chat_a.send("hello", peer_a)

        self.assertEqual(received, ["hello"])

    async def test_bidirectional(self) -> None:
        peer_a, peer_b = _make_chat_peers()
        recv_a: list[str] = []
        recv_b: list[str] = []
        chat_a = ChatService(on_message=recv_a.append)
        chat_b = ChatService(on_message=recv_b.append)
        peer_a.register(1, chat_a.handle)
        peer_b.register(1, chat_b.handle)

        async with peer_a, peer_b:
            await chat_a.send("Hello from A", peer_a)
            await chat_b.send("Hello from B", peer_b)

        self.assertEqual(recv_b, ["Hello from A"])
        self.assertEqual(recv_a, ["Hello from B"])

    async def test_multiple_messages(self) -> None:
        peer_a, peer_b = _make_chat_peers()
        received: list[str] = []
        chat_a = ChatService()
        chat_b = ChatService(on_message=received.append)
        peer_b.register(1, chat_b.handle)

        async with peer_a, peer_b:
            for i in range(5):
                await chat_a.send(f"msg{i}", peer_a)

        self.assertEqual(received, [f"msg{i}" for i in range(5)])

    async def test_unicode(self) -> None:
        peer_a, peer_b = _make_chat_peers()
        received: list[str] = []
        chat_a = ChatService()
        chat_b = ChatService(on_message=received.append)
        peer_b.register(1, chat_b.handle)

        async with peer_a, peer_b:
            await chat_a.send("こんにちは 🎉", peer_a)

        self.assertEqual(received, ["こんにちは 🎉"])

    async def test_concurrent_messages(self) -> None:
        peer_a, peer_b = _make_chat_peers()
        received: list[str] = []
        chat_a = ChatService()
        chat_b = ChatService(on_message=received.append)
        peer_b.register(1, chat_b.handle)

        async with peer_a, peer_b:
            tasks = [asyncio.create_task(chat_a.send(f"m{i}", peer_a)) for i in range(4)]
            await asyncio.gather(*tasks)

        self.assertEqual(sorted(received), sorted([f"m{i}" for i in range(4)]))


if __name__ == "__main__":
    unittest.main()
