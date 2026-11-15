"""End-to-end loopback demo: two in-process peers exchange chat + file."""

from __future__ import annotations

import asyncio
import os
import sys
import tempfile
from typing import List

# Allow running as ``python examples/demo.py`` from the repo root.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from gti.adapters.loopback import create_rfcomm_loopback
from gti.peer import Peer
from gti.services.chat import ChatService
from gti.services.file_transfer import FileTransferService, DEFAULT_CHUNK_SIZE


async def main() -> None:
    t_a, t_b = await create_rfcomm_loopback()
    peer_a = Peer(t_a, role="client")
    peer_b = Peer(t_b, role="server")

    msgs_a: List[str] = []
    msgs_b: List[str] = []
    files_b: List[tuple] = []

    chat_a = ChatService(on_message=lambda m: msgs_a.append(m))
    chat_b = ChatService(on_message=lambda m: msgs_b.append(m))
    file_a = FileTransferService()

    peer_a.register(1, chat_a.handle)
    peer_a.register(2, file_a.handle)
    peer_b.register(1, chat_b.handle)

    # Create a test file (~3 chunks worth of data).
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".bin")
    tmp.write(os.urandom(3 * DEFAULT_CHUNK_SIZE + 512))
    tmp.close()

    with tempfile.TemporaryDirectory() as receive_dir:
        file_b = FileTransferService(
            on_file=lambda n, p: files_b.append((n, p.stat().st_size)),
            receive_dir=receive_dir,
        )
        peer_b.register(2, file_b.handle)
        try:
            async with peer_a, peer_b:
                # Chat in both directions.
                await chat_a.send("Hello from A!", peer_a)
                await chat_b.send("Hello from B!", peer_b)

                # File transfer and a concurrent chat message.
                file_task = asyncio.create_task(file_a.send(tmp.name, peer_a))
                chat_task = asyncio.create_task(
                    chat_a.send("Mid-transfer message", peer_a)
                )
                await asyncio.gather(file_task, chat_task)
        finally:
            os.unlink(tmp.name)

    print("=== Demo results ===")
    print(f"A received messages : {msgs_a}")
    print(f"B received messages : {msgs_b}")
    print(f"B received files    : {files_b}")
    assert msgs_b == ["Hello from A!", "Mid-transfer message"], f"unexpected: {msgs_b}"
    assert msgs_a == ["Hello from B!"], f"unexpected: {msgs_a}"
    assert len(files_b) == 1, f"expected 1 file, got {files_b}"
    print("All assertions passed.")


if __name__ == "__main__":
    asyncio.run(main())
