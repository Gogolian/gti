"""Tests for FileTransferService — streaming upload, checksum, interleave."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import tempfile
import unittest
from pathlib import Path

from gti.errors import RemoteError
from gti.peer import Peer, RequestContext
from gti.protocol import Status
from gti.services.chat import ChatService
from gti.services.file_transfer import (
    DEFAULT_CHUNK_SIZE,
    FileTransferService,
    unique_path,
)
from gti.transport import create_loopback


def _make_peers():
    t_a, t_b = create_loopback()
    return Peer(t_a, role="client"), Peer(t_b, role="server")


class TestUniquePath(unittest.TestCase):
    def test_never_overwrites(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            directory = Path(d)
            (directory / "notes.txt").write_bytes(b"one")
            (directory / "notes.1.txt").write_bytes(b"two")
            self.assertEqual(unique_path(directory, "notes.txt").name, "notes.2.txt")

    def test_strips_path_components(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            directory = Path(d)
            p = unique_path(directory, "../../etc/passwd")
            self.assertEqual(p.parent, directory)
            self.assertEqual(p.name, "passwd")

    def test_rejects_dot_names(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            directory = Path(d)
            for name in ("", ".", ".."):
                p = unique_path(directory, name)
                self.assertEqual(p.parent, directory)
                self.assertEqual(p.name, "received.bin")


class TestFileTransfer(unittest.IsolatedAsyncioTestCase):
    async def test_small_file(self) -> None:
        peer_a, peer_b = _make_peers()
        received: list[tuple[str, Path]] = []
        with tempfile.TemporaryDirectory() as d:
            file_a = FileTransferService()
            file_b = FileTransferService(
                on_file=lambda n, p: received.append((n, p)),
                receive_dir=d,
            )
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
            name, saved = received[0]
            self.assertEqual(name, os.path.basename(path))
            self.assertEqual(saved.read_bytes(), b"hello file")
            self.assertEqual(saved.parent, Path(d))

    async def test_multi_chunk_file(self) -> None:
        peer_a, peer_b = _make_peers()
        received: list[Path] = []
        with tempfile.TemporaryDirectory() as d:
            file_a = FileTransferService()
            file_b = FileTransferService(
                on_file=lambda _n, p: received.append(p), receive_dir=d
            )
            peer_b.register(2, file_b.handle)

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
            self.assertEqual(hashlib.sha256(received[0].read_bytes()).hexdigest(), sha256)

    async def test_empty_file(self) -> None:
        peer_a, peer_b = _make_peers()
        received: list[Path] = []
        with tempfile.TemporaryDirectory() as d:
            file_a = FileTransferService()
            file_b = FileTransferService(
                on_file=lambda _n, p: received.append(p), receive_dir=d
            )
            peer_b.register(2, file_b.handle)

            with tempfile.NamedTemporaryFile(delete=False) as f:
                path = f.name
            try:
                async with peer_a, peer_b:
                    await file_a.send(path, peer_a)
            finally:
                os.unlink(path)

            self.assertEqual(len(received), 1)
            self.assertEqual(received[0].read_bytes(), b"")

    async def test_checksum_mismatch_raises(self) -> None:
        """A receiver that reports a wrong sha256 causes RemoteError on the sender."""
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

    async def test_receiver_cleans_up_on_bad_hash(self) -> None:
        """A bad hash leaves no .part file and no final file behind."""
        peer_a, peer_b = _make_peers()
        with tempfile.TemporaryDirectory() as d:
            file_b = FileTransferService(receive_dir=d)
            peer_b.register(2, file_b.handle)

            content = b"corrupt me"
            meta = json.dumps(
                {
                    "name": "bad.bin",
                    "size": len(content),
                    "sha256": "00" * 32,
                    "chunk_size": 4,
                }
            )

            async with peer_a, peer_b:
                call = await peer_a.open_call(2, meta.encode())
                try:
                    await call.wait_ack()
                    for i in range(0, len(content), 4):
                        chunk = content[i : i + 4]
                        await call.send_chunk(chunk, end=(i + 4 >= len(content)))
                    with self.assertRaises(RemoteError):
                        async for _ in call:
                            pass
                finally:
                    peer_a._client_pending.pop(call.request_id, None)

            self.assertEqual(list(Path(d).iterdir()), [])

    async def test_max_size_rejects(self) -> None:
        peer_a, peer_b = _make_peers()
        with tempfile.TemporaryDirectory() as d:
            file_a = FileTransferService()
            file_b = FileTransferService(receive_dir=d, max_size=4)
            peer_b.register(2, file_b.handle)

            with tempfile.NamedTemporaryFile(delete=False) as f:
                f.write(b"too large")
                path = f.name
            try:
                async with peer_a, peer_b:
                    with self.assertRaises(RemoteError):
                        await file_a.send(path, peer_a)
            finally:
                os.unlink(path)

            self.assertEqual(list(Path(d).iterdir()), [])

    async def test_progress_callbacks(self) -> None:
        peer_a, peer_b = _make_peers()
        sent_progress: list[tuple[int, int]] = []
        recv_progress: list[tuple[int, int]] = []
        with tempfile.TemporaryDirectory() as d:
            file_a = FileTransferService()
            file_b = FileTransferService(
                receive_dir=d,
                on_progress=lambda done, total: recv_progress.append((done, total)),
            )
            peer_b.register(2, file_b.handle)

            content = os.urandom(3 * DEFAULT_CHUNK_SIZE)
            with tempfile.NamedTemporaryFile(delete=False) as f:
                f.write(content)
                path = f.name
            try:
                async with peer_a, peer_b:
                    await file_a.send(
                        path,
                        peer_a,
                        on_progress=lambda done, total: sent_progress.append(
                            (done, total)
                        ),
                    )
            finally:
                os.unlink(path)

            self.assertEqual(sent_progress[-1], (len(content), len(content)))
            self.assertEqual(recv_progress[-1], (len(content), len(content)))

    async def test_sender_error_cancels_receiver(self) -> None:
        """A local sender failure sends CANCEL and the receiver cleans up."""
        peer_a, peer_b = _make_peers()
        with tempfile.TemporaryDirectory() as d:
            file_a = FileTransferService()
            file_b = FileTransferService(receive_dir=d)
            peer_b.register(2, file_b.handle)

            content = os.urandom(4 * DEFAULT_CHUNK_SIZE)
            with tempfile.NamedTemporaryFile(delete=False) as f:
                f.write(content)
                path = f.name

            def boom(_done: int, _total: int) -> None:
                raise RuntimeError("progress callback failed")

            try:
                async with peer_a, peer_b:
                    with self.assertRaises(RuntimeError):
                        await file_a.send(path, peer_a, on_progress=boom)
                    await asyncio.sleep(0.05)
            finally:
                os.unlink(path)

            self.assertEqual(list(Path(d).iterdir()), [])

    async def test_interleave_with_chat(self) -> None:
        """Chat message is received by B before the file transfer completes."""
        peer_a, peer_b = _make_peers()
        events: list[str] = []
        with tempfile.TemporaryDirectory() as d:
            chat_a = ChatService()
            chat_b = ChatService(on_message=lambda m: events.append(f"chat:{m}"))
            file_a = FileTransferService()
            file_b = FileTransferService(
                on_file=lambda n, _p: events.append(f"file:{n}"),
                receive_dir=d,
            )

            peer_b.register(1, chat_b.handle)
            peer_b.register(2, file_b.handle)

            content = os.urandom(5 * DEFAULT_CHUNK_SIZE)
            with tempfile.NamedTemporaryFile(delete=False) as f:
                f.write(content)
                path = f.name
            try:
                async with peer_a, peer_b:
                    file_task = asyncio.create_task(file_a.send(path, peer_a))
                    await asyncio.sleep(0)  # let file transfer begin
                    chat_task = asyncio.create_task(
                        chat_a.send("mid-transfer", peer_a)
                    )
                    await asyncio.gather(file_task, chat_task)
            finally:
                os.unlink(path)

            self.assertIn("chat:mid-transfer", events)
            self.assertIn(f"file:{os.path.basename(path)}", events)
            chat_idx = events.index("chat:mid-transfer")
            file_idx = events.index(f"file:{os.path.basename(path)}")
            self.assertLess(chat_idx, file_idx)


if __name__ == "__main__":
    unittest.main()
