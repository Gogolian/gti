"""Tests for transport.py and adapters/loopback.py."""

from __future__ import annotations

import asyncio
import unittest

from gti.transport import (
    InMemoryTransport,
    StreamTransport,
    TransportClosed,
    create_loopback,
)
from gti.adapters.loopback import create_rfcomm_loopback
from gti.adapters import open_connection


class TestInMemoryTransport(unittest.IsolatedAsyncioTestCase):
    async def test_round_trip(self) -> None:
        a, b = create_loopback()
        received: list[bytes] = []

        async def cb(frame: bytes) -> None:
            received.append(frame)

        b.on_receive(cb)
        await a.start()
        await b.start()
        await a.send(b"hello")
        await a.send(b"world")
        # Give delivery tasks a chance to run.
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        self.assertEqual(received, [b"hello", b"world"])

    async def test_sent_log(self) -> None:
        a, b = create_loopback()
        await a.start()
        await b.start()
        await a.send(b"x")
        self.assertEqual(a.sent, [b"x"])

    async def test_closed_raises(self) -> None:
        a, b = create_loopback()
        await a.start()
        await a.close()
        with self.assertRaises(TransportClosed):
            await a.send(b"after close")

    async def test_unlinked_raises(self) -> None:
        t = InMemoryTransport("solo")
        await t.start()
        with self.assertRaises(RuntimeError):
            await t.send(b"no peer")

    async def test_context_manager(self) -> None:
        a, b = create_loopback()
        received: list[bytes] = []

        async def cb(frame: bytes) -> None:
            received.append(frame)

        b.on_receive(cb)
        async with a, b:
            await a.send(b"ctx")
        await asyncio.sleep(0)
        self.assertEqual(received, [b"ctx"])


class TestRFCOMMLoopback(unittest.IsolatedAsyncioTestCase):
    async def test_round_trip(self) -> None:
        t_a, t_b = await create_rfcomm_loopback()
        received: list[bytes] = []

        async def cb(frame: bytes) -> None:
            received.append(frame)

        t_b.on_receive(cb)
        await t_a.start()
        await t_b.start()
        await t_a.send(b"rfcomm-hello")
        # Allow the recv loop to run.
        await asyncio.sleep(0.05)
        await t_a.close()
        await t_b.close()
        self.assertEqual(received, [b"rfcomm-hello"])

    async def test_multiple_frames(self) -> None:
        t_a, t_b = await create_rfcomm_loopback()
        received: list[bytes] = []

        async def cb(frame: bytes) -> None:
            received.append(frame)

        t_b.on_receive(cb)
        await t_a.start()
        await t_b.start()
        for i in range(5):
            await t_a.send(f"frame{i}".encode())
        await asyncio.sleep(0.05)
        await t_a.close()
        await t_b.close()
        self.assertEqual(received, [f"frame{i}".encode() for i in range(5)])

    async def test_large_frame(self) -> None:
        t_a, t_b = await create_rfcomm_loopback()
        received: list[bytes] = []

        async def cb(frame: bytes) -> None:
            received.append(frame)

        t_b.on_receive(cb)
        await t_a.start()
        await t_b.start()
        big = b"X" * (64 * 1024)
        await t_a.send(big)
        await asyncio.sleep(0.1)
        await t_a.close()
        await t_b.close()
        self.assertEqual(received, [big])


class TestOpenConnectionFactory(unittest.TestCase):
    def test_loopback_kind(self) -> None:
        t = open_connection("loopback", name="test")
        self.assertIsInstance(t, InMemoryTransport)
        self.assertEqual(t.name, "test")

    def test_unknown_kind_raises(self) -> None:
        with self.assertRaises(ValueError):
            open_connection("nonexistent")


if __name__ == "__main__":
    unittest.main()
