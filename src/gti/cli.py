"""Command-line entry point for gti.

Usage::

    gti demo                # loopback chat + file demo (no hardware needed)
    gti chat                # listen for an incoming RFCOMM connection
    gti chat --address ADDR # connect to a peer over RFCOMM

``chat`` is a unified session: both peers can send chat messages and files
(`/file PATH`) in either direction over the single connection.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from pathlib import Path
from typing import Callable, List, Optional

InputFn = Callable[[str], str]
PrintFn = Callable[..., object]


# ── demo ──────────────────────────────────────────────────────────────────────

async def _run_demo() -> None:
    """Two in-process peers exchange a chat message and a small file."""
    import tempfile

    from .adapters.loopback import create_rfcomm_loopback
    from .peer import Peer
    from .services.chat import ChatService
    from .services.file_transfer import FileTransferService

    t_a, t_b = await create_rfcomm_loopback()
    peer_a = Peer(t_a, role="client")
    peer_b = Peer(t_b, role="server")

    received_msgs: List[str] = []
    received_files: List[tuple] = []

    chat_a = ChatService(on_message=lambda m: print(f"[A] received message: {m!r}"))
    chat_b = ChatService(
        on_message=lambda m: received_msgs.append(m) or print(f"[B] received message: {m!r}")
    )
    file_a = FileTransferService()
    file_b = FileTransferService(
        on_file=lambda n, d: received_files.append((n, len(d)))
        or print(f"[B] received file {n!r} ({len(d)} bytes)")
    )

    peer_a.register(1, chat_a.handle)
    peer_a.register(2, file_a.handle)
    peer_b.register(1, chat_b.handle)
    peer_b.register(2, file_b.handle)

    # Write a small temp file to send.
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".txt")
    tmp.write(b"Hello from gti demo!\n" * 100)
    tmp.close()

    try:
        async with peer_a, peer_b:
            await chat_a.send("Hello from A!", peer_a)
            await chat_b.send("Hello from B!", peer_b)
            await file_a.send(tmp.name, peer_a)
        print("\nDemo complete.")
        print(f"  messages received by B : {received_msgs}")
        print(f"  files received by B    : {received_files}")
    finally:
        os.unlink(tmp.name)


# ── unified session ───────────────────────────────────────────────────────────

def _save_received_file(name: str, data: bytes, directory: Path) -> Path:
    """Write *data* to *directory* under *name*, never overwriting."""
    safe = os.path.basename(name) or "received.bin"
    target = directory / safe
    stem, suffix = target.stem, target.suffix
    n = 1
    while target.exists():
        target = directory / f"{stem}.{n}{suffix}"
        n += 1
    target.write_bytes(data)
    return target


async def _run_session(
    transport,
    *,
    role: str,
    input_fn: InputFn = input,
    print_fn: PrintFn = print,
    receive_dir: Optional[Path] = None,
) -> None:
    """Unified interactive session over *transport*: chat + files, both ways.

    *role* must be ``"client"`` (we dial) or ``"server"`` (we listen) — the
    peer multiplexer splits request-id space by role, so the two ends of a
    connection must use opposite roles.

    Commands: ``/file PATH`` send a file, ``/quit`` leave the session;
    anything else is sent as a chat message.
    """
    from .peer import Peer
    from .services.chat import ChatService
    from .services.file_transfer import FileTransferService

    def _print_peer(text: str) -> None:
        print_fn(f"\r[peer] {text}\n> ", end="", flush=True)

    received_files: List[Path] = []

    def _on_file(name: str, data: bytes) -> None:
        path = _save_received_file(name, data, receive_dir or Path.cwd())
        received_files.append(path)
        _print_peer(f"file {name!r} saved to {path}")

    chat = ChatService(on_message=_print_peer)
    files = FileTransferService(on_file=_on_file)
    peer = Peer(transport, role=role)
    peer.register(1, chat.handle)
    peer.register(2, files.handle)

    async with peer:
        print_fn(
            "Connected.  Type a message, '/file PATH' to send a file, "
            "'/quit' to leave."
        )
        loop = asyncio.get_running_loop()
        while True:
            try:
                line = (await loop.run_in_executor(None, input_fn, "> ")).strip()
            except (EOFError, KeyboardInterrupt):
                break
            if not line:
                continue
            if line in ("/quit", "/q"):
                break
            if line.startswith("/file "):
                path = line[len("/file "):].strip().strip('"')
                if not os.path.isfile(path):
                    print_fn(f"gti: no such file: {path}")
                    continue
                await files.send(path, peer)
                print_fn(f"sent file {path}")
            elif line.startswith("/"):
                print_fn("gti: unknown command; use /file PATH or /quit")
            else:
                await chat.send(line, peer)


async def _run_chat(address: Optional[str]) -> None:
    """Open an RFCOMM connection (dial or listen) and run the unified session."""
    from .adapters import open_connection

    if address:
        transport = open_connection("rfcomm_connect", address=address)
        print(f"Connecting to {address} …", flush=True)
        role = "client"
    else:
        transport = open_connection("rfcomm_listen")
        print("Waiting for incoming connection …", flush=True)
        role = "server"
    await _run_session(transport, role=role)


# ── parser ────────────────────────────────────────────────────────────────────

def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="gti",
        description="Multiplexed chat and file transfer over one Bluetooth Classic RFCOMM link.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("demo", help="run a loopback chat + file demo (no hardware needed)")

    chat = sub.add_parser("chat", help="interactive chat + file session over RFCOMM")
    chat.add_argument(
        "--address",
        help="peer Bluetooth address (omit to wait for an incoming connection)",
    )

    return parser


def main(argv: Optional[List[str]] = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.command == "demo":
        asyncio.run(_run_demo())
        return 0

    if args.command == "chat":
        asyncio.run(_run_chat(args.address))
        return 0

    parser.error(f"unknown command {args.command!r}")  # pragma: no cover
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
