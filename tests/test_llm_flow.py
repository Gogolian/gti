"""Flow-control, batching and untrusted compression regression tests."""
from __future__ import annotations

import asyncio
import unittest
import zlib
from types import SimpleNamespace

from gti.services.llm import (
    CHUNK_SIZE, RESPONSE_WINDOW, LLMService, RelayError,
    _batches, _decode_record, _encode_record,
)


class TestRecords(unittest.TestCase):
    def decode(self, record):
        return _decode_record(record, raw_prefix=b"D", compressed_prefix=b"d",
                              max_size=4096, what="test")

    def test_roundtrip(self):
        for data in (b"small", b"hello" * 800, bytes(range(256)) * 16):
            self.assertEqual(self.decode(_encode_record(b"D", b"d", data)), data)

    def test_invalid_compressed_records(self):
        valid = zlib.compress(b"hello")
        for record in (b"d", b"dgarbage", b"d" + valid[:-1], b"d" + valid + b"tail",
                       b"d" + valid + valid, b"d" + zlib.compress(b""),
                       b"d" + zlib.compress(b"x" * 1000000), b"D", b"D" + b"x" * 4097):
            with self.subTest(record=record[:20]), self.assertRaises(RelayError):
                self.decode(record)


class TestBatches(unittest.IsolatedAsyncioTestCase):
    async def test_first_bytes_do_not_wait_for_batch_interval(self):
        async def source():
            yield b"first"
            await asyncio.Event().wait()
        iterator = source()
        batches = _batches(iterator, size=4096, interval=60)
        try:
            self.assertEqual(await asyncio.wait_for(batches.__anext__(), 0.2), b"first")
        finally:
            await batches.aclose()
            await iterator.aclose()

    async def test_exact_size_bound_and_byte_preservation(self):
        async def source():
            for chunk in (b"", b"a" * 37, b"b", b"c" * 15):
                yield chunk
        batches = [chunk async for chunk in _batches(source(), size=8, interval=0.01)]
        self.assertEqual(b"".join(batches), b"a" * 37 + b"b" + b"c" * 15)
        self.assertTrue(all(0 < len(chunk) <= 8 for chunk in batches))

    async def test_first_byte_immediate_and_timed_flush_keeps_read_alive(self):
        release = asyncio.Event()
        closed = asyncio.Event()

        async def source():
            try:
                yield b"first"
                yield b"second"
                await release.wait()
                yield b"third"
            finally:
                closed.set()

        source_iter = source()
        batches = _batches(source_iter, size=100, interval=0.02)
        try:
            # A large batching interval must not delay first bytes.
            self.assertEqual(await asyncio.wait_for(batches.__anext__(), 0.2), b"first")
            self.assertEqual(await asyncio.wait_for(batches.__anext__(), 0.2), b"second")
            self.assertFalse(closed.is_set())
            release.set()
            self.assertEqual(await batches.__anext__(), b"third")
            with self.assertRaises(StopAsyncIteration):
                await batches.__anext__()
        finally:
            await batches.aclose()
            await source_iter.aclose()

    async def test_close_cancels_pending_read(self):
        closed = asyncio.Event()

        async def source():
            try:
                yield b"first"
                yield b"second"
                await asyncio.Event().wait()
            finally:
                closed.set()

        batches = _batches(source(), size=100, interval=0.01)
        self.assertEqual(await batches.__anext__(), b"first")
        self.assertEqual(await batches.__anext__(), b"second")
        await asyncio.wait_for(batches.aclose(), 0.2)
        self.assertTrue(closed.is_set())

    async def test_upstream_failure_propagates(self):
        async def source():
            yield b"first"
            raise RuntimeError("backend failed")
        batches = _batches(source(), size=100, interval=0.01)
        self.assertEqual(await batches.__anext__(), b"first")
        with self.assertRaisesRegex(RuntimeError, "backend failed"):
            await batches.__anext__()


class TestResponseWindow(unittest.IsolatedAsyncioTestCase):
    async def test_window_rolls_after_one_ack_and_remains_bounded(self):
        records = asyncio.Queue()
        acks = asyncio.Queue()

        async def incoming():
            while True:
                yield await acks.get()

        async def emit(data, *, end=False):
            await records.put(data)

        async def chunks(size):
            for _ in range(20):
                yield b"x" * size

        response = SimpleNamespace(status=200, headers={}, content=SimpleNamespace(iter_chunked=chunks))
        ctx = SimpleNamespace(emit=emit)
        service = LLMService("http://localhost/v1")
        task = asyncio.create_task(service._stream_response(response, incoming(), ctx, version=2))
        try:
            self.assertTrue((await records.get()).startswith(b"H"))
            await acks.put(b"A")
            for _ in range(RESPONSE_WINDOW):
                self.assertIn((await asyncio.wait_for(records.get(), 1))[:1], (b"D", b"d"))
            await asyncio.sleep(0.02)
            self.assertTrue(records.empty())
            # A batch-and-drain implementation hangs here waiting for a second ACK.
            await acks.put(b"A")
            self.assertIn((await asyncio.wait_for(records.get(), 1))[:1], (b"D", b"d"))
            await asyncio.sleep(0.02)
            self.assertTrue(records.empty())
        finally:
            task.cancel()
            await asyncio.wait_for(asyncio.gather(task, return_exceptions=True), 1)

    async def test_v1_host_response_remains_raw_and_stop_and_wait(self):
        records = asyncio.Queue()
        acks = asyncio.Queue()

        async def incoming():
            while True:
                yield await acks.get()

        async def emit(data, *, end=False):
            await records.put(data)

        async def chunks(size):
            for _ in range(2):
                yield b"x" * size

        response = SimpleNamespace(status=200, headers={}, content=SimpleNamespace(iter_chunked=chunks))
        task = asyncio.create_task(LLMService("http://localhost/v1")._stream_response(
            response, incoming(), SimpleNamespace(emit=emit), version=1))
        try:
            await records.get()  # Headers
            await acks.put(b"A")
            self.assertEqual(await records.get(), b"D" + b"x" * CHUNK_SIZE)
            await asyncio.sleep(0.02)
            self.assertTrue(records.empty())
            await acks.put(b"A")
            self.assertEqual(await records.get(), b"D" + b"x" * CHUNK_SIZE)
            await acks.put(b"A")
            self.assertEqual(await records.get(), b"Z")
            await task
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
