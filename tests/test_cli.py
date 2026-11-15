"""Tests for the unified CLI session (_run_session) over loopback."""

from __future__ import annotations

import asyncio
import tempfile
import unittest
from pathlib import Path

from gti.adapters.loopback import create_rfcomm_loopback
from gti.cli import PrintFn, _run_session, main
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


class TestParser(unittest.TestCase):
    def test_version(self) -> None:
        with self.assertRaises(SystemExit) as cm:
            main(["--version"])
        self.assertEqual(cm.exception.code, 0)


class TestSession(unittest.IsolatedAsyncioTestCase):
    async def test_chat_file_and_quit(self) -> None:
        t_client, t_server = await create_rfcomm_loopback()

        peer_msgs: list[str] = []
        received_files: list[Path] = []
        peer_tmp = tempfile.TemporaryDirectory()
        self.addCleanup(peer_tmp.cleanup)
        peer_dir = Path(peer_tmp.name)

        hello_tmp = tempfile.TemporaryDirectory()
        self.addCleanup(hello_tmp.cleanup)
        hello = Path(hello_tmp.name) / "hello.txt"
        hello.write_bytes(b"file payload " * 100)

        out: list[str] = []

        chat = ChatService(on_message=peer_msgs.append)
        files = FileTransferService(
            on_file=lambda _n, p: received_files.append(p),
            receive_dir=peer_dir,
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
        self.assertEqual(len(received_files), 1)
        self.assertEqual(received_files[0].read_bytes(), hello.read_bytes())
        transcript = "\n".join(out)
        self.assertIn("hello from the peer", transcript)
        self.assertIn(f"sent file {hello}", transcript)

    async def test_unknown_command(self) -> None:
        t_client, t_server = await create_rfcomm_loopback()
        out: list[str] = []
        await _run_session(
            t_client,
            role="client",
            input_fn=_scripted_input("/nope"),
            print_fn=_capture(out),
        )
        await t_server.close()
        self.assertTrue(any("unknown command" in line for line in out))

    async def test_missing_file(self) -> None:
        t_client, t_server = await create_rfcomm_loopback()
        out: list[str] = []
        await _run_session(
            t_client,
            role="client",
            input_fn=_scripted_input("/file /no/such/file.bin"),
            print_fn=_capture(out),
        )
        await t_server.close()
        self.assertTrue(any("no such file" in line for line in out))

    async def test_peer_disconnect_is_reported(self) -> None:
        import threading

        t_client, t_server = await create_rfcomm_loopback()
        out: list[str] = []
        closed = threading.Event()

        def blocking_input(_prompt: str) -> str:
            closed.wait(timeout=2)
            return ""

        async def close_peer() -> None:
            await asyncio.sleep(0.05)
            await t_server.close()
            closed.set()

        closer = asyncio.create_task(close_peer())
        await _run_session(
            t_client,
            role="client",
            input_fn=blocking_input,
            print_fn=_capture(out),
        )
        await closer
        self.assertTrue(any("peer disconnected" in line for line in out))


if __name__ == "__main__":
    unittest.main()
