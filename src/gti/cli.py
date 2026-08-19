"""Command-line entry point for gti.

Usage::

    gti demo                         # loopback chat + file demo (no hardware needed)
    gti listen                       # accept an incoming RFCOMM connection
    gti connect --address <addr>     # connect to a peer over RFCOMM
    gti chat [--address <addr>]      # interactive chat session
    gti send-file [--address <addr>] PATH   # send one file to a peer
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from typing import List, Optional


# ── demo ──────────────────────────────────────────────────────────────────────

async def _run_demo() -> None:
    """Two in-process peers exchange a chat message and a small file."""
    import tempfile, os
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


# ── interactive chat helper ────────────────────────────────────────────────────

async def _run_chat(address: Optional[str]) -> None:
    """Interactive readline chat loop; Ctrl-C or EOF to quit."""
    from .adapters import open_connection
    from .peer import Peer
    from .services.chat import ChatService

    role: str
    if address:
        transport = open_connection("rfcomm_connect", address=address)
        role = "client"
        print(f"Connecting to {address} …", flush=True)
    else:
        transport = open_connection("rfcomm_listen")
        role = "server"
        print("Waiting for incoming connection …", flush=True)

    def _on_message(text: str) -> None:
        print(f"\r[peer] {text}\n> ", end="", flush=True)

    svc = ChatService(on_message=_on_message)
    peer = Peer(transport, role=role)
    peer.register(1, svc.handle)

    async with peer:
        print("Connected.  Type a message and press Enter; Ctrl-C to quit.")
        loop = asyncio.get_running_loop()
        while True:
            try:
                line = await loop.run_in_executor(None, input, "> ")
            except (EOFError, KeyboardInterrupt):
                break
            if line.strip():
                await svc.send(line, peer)


# ── send-file helper ───────────────────────────────────────────────────────────

async def _run_send_file(path: str, address: Optional[str]) -> None:
    from .adapters import open_connection
    from .peer import Peer
    from .services.file_transfer import FileTransferService

    role: str
    if address:
        transport = open_connection("rfcomm_connect", address=address)
        role = "client"
        print(f"Connecting to {address} …", flush=True)
    else:
        transport = open_connection("rfcomm_listen")
        role = "server"
        print("Waiting for incoming connection …", flush=True)

    svc = FileTransferService()
    peer = Peer(transport, role=role)
    peer.register(2, svc.handle)

    async with peer:
        print(f"Sending {path!r} …", flush=True)
        await svc.send(path, peer)
        print("Done.")


# ── parser ─────────────────────────────────────────────────────────────────────

def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="gti",
        description="Multiplexed chat and file transfer over one Bluetooth Classic RFCOMM link.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("demo", help="run a loopback chat + file demo (no hardware needed)")
    sub.add_parser("listen", help="accept an incoming RFCOMM connection (placeholder)")

    connect = sub.add_parser("connect", help="connect to a peer over RFCOMM (placeholder)")
    connect.add_argument("--address", required=True, help="peer Bluetooth address")

    chat = sub.add_parser("chat", help="interactive chat session")
    chat.add_argument("--address", help="peer Bluetooth address (omit to listen)")

    send = sub.add_parser("send-file", help="send one file to a peer")
    send.add_argument("--address", help="peer Bluetooth address (omit to listen)")
    send.add_argument("path", help="path to the file to send")

    return parser


def main(argv: Optional[List[str]] = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.command == "demo":
        asyncio.run(_run_demo())
        return 0

    if args.command == "chat":
        asyncio.run(_run_chat(getattr(args, "address", None)))
        return 0

    if args.command == "send-file":
        asyncio.run(_run_send_file(args.path, getattr(args, "address", None)))
        return 0

    # listen / connect are stubs (require real BT hardware to test)
    print(f"gti: command {args.command!r} is not yet implemented", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
