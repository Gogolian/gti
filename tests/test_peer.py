"""Tests for peer.py — multiplexing, bidirectional calls, error propagation."""

from __future__ import annotations

import asyncio
import unittest

from gti.errors import NotSupportedError, RemoteError
from gti.peer import Peer, RequestContext
from gti.protocol import Status
from gti.transport import create_loopback


def _make_peers():
    """Return (peer_a, peer_b) over an in-memory loopback pair."""
    t_a, t_b = create_loopback()
    return Peer(t_a, role="client"), Peer(t_b, role="server")


class TestNotSupported(unittest.IsolatedAsyncioTestCase):
    async def test_unregistered_service_raises(self) -> None:
        peer_a, peer_b = _make_peers()
        async with peer_a, peer_b:
            with self.assertRaises(NotSupportedError):
                async for _ in peer_a.call(99, b"payload"):
                    pass

    async def test_not_supported_both_directions(self) -> None:
        peer_a, peer_b = _make_peers()
        async with peer_a, peer_b:
            with self.assertRaises(NotSupportedError):
                async for _ in peer_b.call(99, b"payload"):
                    pass


class TestSimpleCall(unittest.IsolatedAsyncioTestCase):
    async def test_request_response(self) -> None:
        peer_a, peer_b = _make_peers()
        results: list[bytes] = []

        async def echo(payload: bytes, ctx: RequestContext) -> None:
            await ctx.ack()
            await ctx.emit(payload, end=True)

        peer_b.register(1, echo)

        async with peer_a, peer_b:
            async for chunk in peer_a.call(1, b"ping"):
                results.append(chunk)

        self.assertEqual(results, [b"ping"])

    async def test_multi_event_response(self) -> None:
        peer_a, peer_b = _make_peers()
        results: list[bytes] = []

        async def handler(payload: bytes, ctx: RequestContext) -> None:
            await ctx.ack()
            for i in range(3):
                await ctx.emit(f"chunk{i}".encode(), end=(i == 2))

        peer_b.register(2, handler)

        async with peer_a, peer_b:
            async for chunk in peer_a.call(2, b""):
                results.append(chunk)

        self.assertEqual(results, [b"chunk0", b"chunk1", b"chunk2"])


class TestBidirectional(unittest.IsolatedAsyncioTestCase):
    async def test_a_and_b_call_each_other(self) -> None:
        peer_a, peer_b = _make_peers()
        recv_a: list[bytes] = []
        recv_b: list[bytes] = []

        async def handler_a(payload: bytes, ctx: RequestContext) -> None:
            await ctx.ack()
            recv_a.append(payload)
            await ctx.emit(b"ack-from-a", end=True)

        async def handler_b(payload: bytes, ctx: RequestContext) -> None:
            await ctx.ack()
            recv_b.append(payload)
            await ctx.emit(b"ack-from-b", end=True)

        peer_a.register(1, handler_a)
        peer_b.register(1, handler_b)

        async with peer_a, peer_b:
            resp_a: list[bytes] = []
            resp_b: list[bytes] = []

            async def call_a() -> None:
                async for c in peer_a.call(1, b"from-a"):
                    resp_a.append(c)

            async def call_b() -> None:
                async for c in peer_b.call(1, b"from-b"):
                    resp_b.append(c)

            await asyncio.gather(call_a(), call_b())

        self.assertEqual(recv_b, [b"from-a"])
        self.assertEqual(recv_a, [b"from-b"])
        self.assertEqual(resp_a, [b"ack-from-b"])
        self.assertEqual(resp_b, [b"ack-from-a"])

    async def test_concurrent_calls_from_same_peer(self) -> None:
        peer_a, peer_b = _make_peers()

        async def slow(payload: bytes, ctx: RequestContext) -> None:
            await ctx.ack()
            delay = float(payload)
            await asyncio.sleep(delay)
            await ctx.emit(payload, end=True)

        peer_b.register(1, slow)

        async with peer_a, peer_b:
            order: list[bytes] = []

            async def call(delay: float) -> None:
                async for c in peer_a.call(1, str(delay).encode()):
                    order.append(c)

            # call with delay=0.05 and delay=0.01 — shorter should finish first
            await asyncio.gather(call(0.05), call(0.01))

        self.assertEqual(order[0], b"0.01")
        self.assertEqual(order[1], b"0.05")


class TestErrorPropagation(unittest.IsolatedAsyncioTestCase):
    async def test_handler_error_raises_remote_error(self) -> None:
        peer_a, peer_b = _make_peers()

        async def bad(payload: bytes, ctx: RequestContext) -> None:
            await ctx.ack()
            await ctx.error("something went wrong")

        peer_b.register(1, bad)

        async with peer_a, peer_b:
            with self.assertRaises(RemoteError) as cm:
                async for _ in peer_a.call(1, b""):
                    pass
            self.assertIn("something went wrong", str(cm.exception))

    async def test_unhandled_exception_sends_error(self) -> None:
        peer_a, peer_b = _make_peers()

        async def raises(payload: bytes, ctx: RequestContext) -> None:
            await ctx.ack()
            raise ValueError("boom")

        peer_b.register(1, raises)

        async with peer_a, peer_b:
            with self.assertRaises(RemoteError):
                async for _ in peer_a.call(1, b""):
                    pass

    async def test_error_before_ack(self) -> None:
        """ACK future gets the exception if handler errors before acking."""
        peer_a, peer_b = _make_peers()

        async def raises_immediately(payload: bytes, ctx: RequestContext) -> None:
            raise RuntimeError("pre-ack failure")

        peer_b.register(1, raises_immediately)

        async with peer_a, peer_b:
            with self.assertRaises(RemoteError):
                async for _ in peer_a.call(1, b""):
                    pass


class TestTimeout(unittest.IsolatedAsyncioTestCase):
    async def test_wait_ack_timeout(self) -> None:
        peer_a, peer_b = _make_peers()

        async def never_acks(payload: bytes, ctx: RequestContext) -> None:
            await asyncio.sleep(10)  # never responds

        peer_b.register(1, never_acks)

        async with peer_a, peer_b:
            call = await peer_a.open_call(1, b"")
            with self.assertRaises(asyncio.TimeoutError):
                await call.wait_ack(timeout=0.05)


if __name__ == "__main__":
    unittest.main()
