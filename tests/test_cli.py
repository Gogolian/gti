"""Tests for the unified CLI session (_run_session) over loopback."""

from __future__ import annotations

import asyncio
import tempfile
import unittest
from pathlib import Path

from gti.adapters.loopback import create_rfcomm_loopback
from gti.cli import PrintFn, _run_session, _save_received_file
from gti.peer import Peer
from gti.services.chat import ChatService
from gti.services.file_transfer import FileTransferService


def _scripted_input(*lines: str):
    """Return an input_fn serving *lines*, then EOF."""
    it = iter(lines)

    def input_fn(_prompt: str) -> str:
        try:
            return next(it)
        except StopIteration:
            raise EOFError

    return input_fn


def _capture(out: list) -> "PrintFn":
    def print_fn(*args, **_kwargs) -> None:
        out.append(" ".join(map(str, args)))

    return print_fn


class TestSaveReceivedFile(unittest.TestCase):
    def test_never_overwrites(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            directory = Path(d)
            p1 = _save_received_file("notes.txt", b"one", directory)
            p2 = _save_received_file("notes.txt", b"two", directory)
            p3 = _save_received_file("notes.txt", b"three", directory)
            self.assertEqual([p1.name, p2.name, p3.name],
                             ["notes.txt", "notes.1.txt", "notes.2.txt"])
            self.assertEqual(p1.read_bytes(), b"one")
            self.assertEqual(p2.read_bytes(), b"two")
            self.assertEqual(p3.read_bytes(), b"three")

    def test_strips_path_components(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            directory = Path(d)
            p = _save_received_file("../../etc/passwd", b"x", directory)
            self.assertEqual(p.parent, directory)
            self.assertEqual(p.name, "passwd")


class TestSession(unittest.IsolatedAsyncioTestCase):
    async def test_chat_file_and_quit(self) -> None:
        t_client, t_server = await create_rfcomm_loopback()

        peer_msgs: list[str] = []
        peer_dir = Path(tempfile.mkdtemp())

        hello = Path(tempfile.mkdtemp()) / "hello.txt"
        hello.write_bytes(b"file payload " * 100)

        out: list[str] = []

        chat = ChatService(on_message=peer_msgs.append)
        files = FileTransferService(
            on_file=lambda name, data: (peer_dir / name).write_bytes(data)
        )
        peer = Peer(t_server, role="server")
        peer.register(1, chat.handle)
        peer.register(2, files.handle)

        async def client_side() -> None:
            await _run_session(
                t_client,
                role="client",
                input_fn=_scripted_input(
                    "hi there", f"/file {hello}", "/quit"
                ),
                print_fn=_capture(out),
                receive_dir=peer_dir,
            )

        async def peer_side() -> None:
            """Headless peer: greets, then stays up until the client leaves."""
            client = asyncio.create_task(client_side())
            async with peer:
                await chat.send("hello from the peer", peer)
                await asyncio.wait_for(client, timeout=10)

        await asyncio.gather(peer_side())

        self.assertEqual(peer_msgs, ["hi there"])
        received = list(peer_dir.glob("hello*.txt"))
        self.assertEqual(len(received), 1)
        self.assertEqual(received[0].read_bytes(), hello.read_bytes())
        transcript = "\n".join(out)
        self.assertIn("hello from the peer", transcript)
        self.assertIn(f"sent file {hello}", transcript)

    async def test_unknown_command(self) -> None:
        t_client, _t_server = await create_rfcomm_loopback()
        out: list[str] = []
        await _run_session(
            t_client,
            role="client",
            input_fn=_scripted_input("/nope"),
            print_fn=_capture(out),
        )
        self.assertTrue(any("unknown command" in line for line in out))

    async def test_missing_file(self) -> None:
        t_client, _t_server = await create_rfcomm_loopback()
        out: list[str] = []
        await _run_session(
            t_client,
            role="client",
            input_fn=_scripted_input("/file /no/such/file.bin"),
            print_fn=_capture(out),
        )
        self.assertTrue(any("no such file" in line for line in out))


if __name__ == "__main__":
    unittest.main()
