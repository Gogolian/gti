"""Tests for FileTransferService — upload, checksum, interleave with chat."""

from __future__ import annotations

import asyncio
import hashlib
import os
import tempfile
import unittest

from gti.errors import RemoteError
from gti.peer import Peer
from gti.services.chat import ChatService
from gti.services.file_transfer import DEFAULT_CHUNK_SIZE, FileTransferService
from gti.transport import create_loopback


def _make_peers():
    t_a, t_b = create_loopback()
    return Peer(t_a, role="client"), Peer(t_b, role="server")


class TestFileTransfer(unittest.IsolatedAsyncioTestCase):
    async def test_small_file(self) -> None:
        peer_a, peer_b = _make_peers()
        received: list[tuple] = []
        file_a = FileTransferService()
        file_b = FileTransferService(on_file=lambda n, d: received.append((n, d)))
        peer_b.register(2, file_b.handle)

        with tempfile.NamedTemporaryFile(delete=False, suffix=".txt") as f:
            f.write(b"hello file")
            path = f.name
        try:
            async with peer_a, peer_b:
                await file_a.send(path, peer_a)
        finally:
            os.unlink(path)

        self.assertEqual(len(received), 1)
        name, data = received[0]
        self.assertEqual(name, os.path.basename(path))
        self.assertEqual(data, b"hello file")

    async def test_multi_chunk_file(self) -> None:
        peer_a, peer_b = _make_peers()
        received: list[bytes] = []
        file_a = FileTransferService()
        file_b = FileTransferService(on_file=lambda _n, d: received.append(d))
        peer_b.register(2, file_b.handle)

        # 3.5 chunks worth of data
        content = os.urandom(int(3.5 * DEFAULT_CHUNK_SIZE))
        sha256 = hashlib.sha256(content).hexdigest()

        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(content)
            path = f.name
        try:
            async with peer_a, peer_b:
                await file_a.send(path, peer_a)
        finally:
            os.unlink(path)

        self.assertEqual(len(received), 1)
        self.assertEqual(hashlib.sha256(received[0]).hexdigest(), sha256)

    async def test_checksum_mismatch_raises(self) -> None:
        """Receiver that reports a wrong sha256 causes RemoteError on the sender."""
        import json
        from gti.peer import RequestContext
        from gti.protocol import Status

        peer_a, peer_b = _make_peers()
        file_a = FileTransferService()

        received_errors: list[str] = []

        async def bad_receiver(payload: bytes, ctx: RequestContext) -> None:
            meta = json.loads(payload)
            meta["sha256"] = "00" * 32  # wrong hash — will never match
            await ctx.ack(Status.IN_PROGRESS)
            chunks = []
            async for chunk in ctx.incoming():
                chunks.append(chunk)
            data = b"".join(chunks)
            actual = hashlib.sha256(data).hexdigest()
            if actual != meta["sha256"]:
                received_errors.append("mismatch")
                await ctx.error(f"sha256 mismatch: expected {meta['sha256']}, got {actual}")
                return
            await ctx.emit(json.dumps({"ok": True}).encode(), end=True)

        peer_b.register(2, bad_receiver)

        content = b"data to corrupt"
        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(content)
            path = f.name
        try:
            async with peer_a, peer_b:
                with self.assertRaises(RemoteError):
                    await file_a.send(path, peer_a)
        finally:
            os.unlink(path)

        self.assertEqual(received_errors, ["mismatch"])

    async def test_interleave_with_chat(self) -> None:
        """Chat message is received by B before the file transfer completes."""
        peer_a, peer_b = _make_peers()
        events: list[str] = []

        chat_a = ChatService()
        chat_b = ChatService(on_message=lambda m: events.append(f"chat:{m}"))
        file_a = FileTransferService()
        file_b = FileTransferService(
            on_file=lambda n, _d: events.append(f"file:{n}")
        )

        peer_b.register(1, chat_b.handle)
        peer_b.register(2, file_b.handle)

        # File large enough to require several chunks.
        content = os.urandom(5 * DEFAULT_CHUNK_SIZE)
        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(content)
            path = f.name
        try:
            async with peer_a, peer_b:
                # Start file transfer first, then inject a chat message mid-transfer.
                file_task = asyncio.create_task(file_a.send(path, peer_a))
                await asyncio.sleep(0)  # let file transfer begin
                chat_task = asyncio.create_task(
                    chat_a.send("mid-transfer", peer_a)
                )
                await asyncio.gather(file_task, chat_task)
        finally:
            os.unlink(path)

        self.assertIn(f"chat:mid-transfer", events)
        self.assertIn(f"file:{os.path.basename(path)}", events)
        # Chat callback fires before file-done callback.
        chat_idx = events.index("chat:mid-transfer")
        file_idx = events.index(f"file:{os.path.basename(path)}")
        self.assertLess(chat_idx, file_idx)


if __name__ == "__main__":
    unittest.main()
