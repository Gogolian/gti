"""Command-line entry point for gti.

Usage::

    gti demo                # loopback chat + file demo (no hardware needed)
    gti scan                # list paired/nearby Bluetooth devices
    gti chat                # listen for an incoming RFCOMM connection
    gti chat --scan         # scan, pick a peer, and connect
    gti chat --address ADDR # connect to a peer over RFCOMM

``chat`` is a unified session: both peers can send chat messages and files
(`/file PATH`) in either direction over the single connection.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
from pathlib import Path
from typing import Callable, List, Optional

from . import __version__
from .transport import Transport

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

    def _on_message_a(message: str) -> None:
        print(f"[A] received message: {message!r}")

    def _on_message_b(message: str) -> None:
        received_msgs.append(message)
        print(f"[B] received message: {message!r}")

    def _on_file_b(name: str, path: Path) -> None:
        size = path.stat().st_size
        received_files.append((name, size))
        print(f"[B] received file {name!r} ({size} bytes)")

    chat_a = ChatService(on_message=_on_message_a)
    chat_b = ChatService(on_message=_on_message_b)
    file_a = FileTransferService()

    peer_a.register(1, chat_a.handle)
    peer_a.register(2, file_a.handle)
    peer_b.register(1, chat_b.handle)

    # Write a small temp file to send, and receive into a temp directory.
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".txt")
    tmp.write(b"Hello from gti demo!\n" * 100)
    tmp.close()

    with tempfile.TemporaryDirectory() as receive_dir:
        file_b = FileTransferService(on_file=_on_file_b, receive_dir=receive_dir)
        peer_b.register(2, file_b.handle)
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

async def _run_session(
    transport: Transport,
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
    from .errors import RemoteError
    from .peer import Peer
    from .services.chat import ChatService
    from .services.file_transfer import FileTransferService

    def _print_peer(text: str) -> None:
        print_fn(f"\r[peer] {text}\n> ", end="", flush=True)

    received_files: List[Path] = []

    def _on_file(name: str, path: Path) -> None:
        received_files.append(path)
        _print_peer(f"file {name!r} saved to {path}")

    chat = ChatService(on_message=_print_peer)
    files = FileTransferService(on_file=_on_file, receive_dir=receive_dir)
    peer = Peer(transport, role=role)
    peer.register(1, chat.handle)
    peer.register(2, files.handle)

    async with peer:
        channel = getattr(transport, "channel", None)
        if channel is not None:
            print_fn(f"Using RFCOMM channel {channel}.")
        print_fn(
            "Connected.  Type a message, '/file PATH' to send a file, "
            "'/quit' to leave."
        )
        loop = asyncio.get_running_loop()
        while True:
            if peer.is_closed:
                print_fn("gti: peer disconnected")
                break
            try:
                line = (await loop.run_in_executor(None, input_fn, "> ")).strip()
            except (EOFError, KeyboardInterrupt):
                break
            if not line:
                continue
            if line in ("/quit", "/q"):
                break
            try:
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
            except (ConnectionError, RemoteError, asyncio.TimeoutError, OSError) as exc:
                print_fn(f"gti: {exc}")
                if peer.is_closed:
                    break


async def _choose_transport(
    transport: Optional[str],
    *,
    input_fn: InputFn = input,
    print_fn: PrintFn = print,
) -> str:
    """Return ``"rfcomm"`` or ``"ble"``, prompting when not specified."""
    if transport in ("rfcomm", "ble"):
        return transport
    print_fn("Select transport:")
    print_fn("  1. Bluetooth Classic (RFCOMM)")
    print_fn("  2. Bluetooth Low Energy (BLE)")
    loop = asyncio.get_running_loop()
    while True:
        try:
            raw = (
                await loop.run_in_executor(None, input_fn, "Transport [1]: ")
            ).strip().lower()
        except (EOFError, KeyboardInterrupt):
            return "rfcomm"
        if raw in ("", "1", "rfcomm", "classic"):
            return "rfcomm"
        if raw in ("2", "ble", "low-energy", "lowenergy"):
            return "ble"
        print_fn("gti: choose 1 (RFCOMM) or 2 (BLE)")


async def _run_scan(timeout: float, transport: Optional[str] = None) -> None:
    """List paired/nearby devices for the selected transport."""
    from .discovery import format_devices

    kind = await _choose_transport(transport)
    if kind == "ble":
        from .adapters.ble import discover_ble

        devices = await discover_ble(timeout)
    else:
        from .discovery import discover

        devices = await discover(timeout)
    if not devices:
        print(
            "No Bluetooth devices found.  Check that Bluetooth is on, the "
            "devices are paired, and the listener is discoverable."
        )
        return
    print("Bluetooth devices:")
    for line in format_devices(devices):
        print(line)


async def _run_chat(
    address: Optional[str],
    *,
    channel: Optional[int] = None,
    receive_dir: Optional[Path] = None,
    scan: bool = False,
    scan_timeout: float = 15.0,
    transport: Optional[str] = None,
    connect_timeout: float = 180.0,
) -> None:
    """Open a connection (dial or listen) and run the unified session."""
    from .address import normalize_address
    from .discovery import pick_device

    kind = await _choose_transport(transport)
    loop = asyncio.get_running_loop()
    transport_obj: Transport

    if kind == "ble":
        from .adapters.ble import BLETransport, discover_ble

        if scan:
            print(f"Scanning for BLE devices ({scan_timeout:.0f}s) …", flush=True)
            devices = await discover_ble(scan_timeout)
            chosen = await loop.run_in_executor(
                None, lambda: pick_device(devices)
            )
            if chosen is None:
                print("gti: no device selected", file=sys.stderr)
                return
            address = chosen.address
            print(f"Selected {chosen.name} ({chosen.address})", flush=True)
        if address:
            transport_obj = BLETransport(
                role="connect", target=address, timeout=connect_timeout
            )
            print(f"Connecting to BLE device {address} …", flush=True)
            role = "client"
        else:
            transport_obj = BLETransport(role="listen", timeout=connect_timeout)
            print("Waiting for BLE connection …", flush=True)
            role = "server"
        await _run_session(transport_obj, role=role, receive_dir=receive_dir)
        return

    from .adapters import open_connection

    if scan:
        from .discovery import discover

        print(f"Scanning for Bluetooth devices ({scan_timeout:.0f}s) …", flush=True)
        devices = await discover(scan_timeout)
        chosen = await loop.run_in_executor(
            None, lambda: pick_device(devices)
        )
        if chosen is None:
            print("gti: no device selected", file=sys.stderr)
            return
        address = chosen.address
        print(f"Selected {chosen.name} ({chosen.address})", flush=True)
    elif address:
        try:
            address = normalize_address(address)
        except ValueError as exc:
            print(f"gti: {exc}", file=sys.stderr)
            return

    kwargs = {}
    if channel is not None:
        kwargs["channel"] = channel

    if address:
        # On macOS, require an SDP record unless the user forced a channel;
        # this avoids opening a non-existent channel (which can crash
        # IOBluetooth on recent macOS releases).
        kwargs["require_sdp"] = channel is None
        transport_obj = open_connection("rfcomm_connect", address=address, **kwargs)
        print(f"Connecting to {address} …", flush=True)
        role = "client"
    else:
        transport_obj = open_connection("rfcomm_listen", **kwargs)
        print("Waiting for incoming connection …", flush=True)
        role = "server"
    await _run_session(transport_obj, role=role, receive_dir=receive_dir)


# ── inference relay ──────────────────────────────────────────────────────────

async def _run_llm(args: argparse.Namespace) -> None:
    from .adapters.ble import BLETransport, discover_ble
    from .discovery import pick_device
    from .peer import Peer
    # Fail before starting a potentially long BLE scan/listen if the extra is missing.
    try:
        import aiohttp  # noqa: F401
    except ImportError:
        raise RuntimeError('LLM relay requires: pip install "gti[llm]"') from None

    address = args.address
    if args.scan:
        print(f"Scanning for BLE devices ({args.timeout:.0f}s) …", flush=True)
        devices = await discover_ble(args.timeout)
        address_device = await asyncio.get_running_loop().run_in_executor(
            None, lambda: pick_device(devices)
        )
        if address_device is None:
            raise RuntimeError("no device selected")
        address = address_device.address
    role = "client" if address else "server"
    transport = BLETransport(role="connect" if address else "listen", target=address,
                             timeout=args.connect_timeout)
    peer = Peer(transport, role=role)
    if args.command == "llm-host":
        from .services.base import LLM_SERVICE_ID
        from .services.llm import LLMService

        service = LLMService(args.base_url, api_key=os.environ.get(args.api_key_env))
        peer.register(LLM_SERVICE_ID, service.handle)
    print("Connecting over BLE …" if address else "Waiting for BLE connection …", flush=True)
    async with peer:
        if args.command == "llm-host":
            print(f"Connected. Relaying inference to {args.base_url}", flush=True)
            await peer.wait_closed()
        else:
            from aiohttp import web
            from .llm_proxy import create_app

            runner = web.AppRunner(create_app(peer, args.model), handler_cancellation=True)
            await runner.setup()
            try:
                site = web.TCPSite(runner, "127.0.0.1", args.port)
                await site.start()
                print(f"Connected. OpenAI endpoint: http://127.0.0.1:{args.port}/v1", flush=True)
                await peer.wait_closed()
            finally:
                await runner.cleanup()
        print("gti: peer disconnected", file=sys.stderr)


# ── parser ────────────────────────────────────────────────────────────────────

def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="gti",
        description="Multiplexed chat, file transfer, and LLM relay over Bluetooth.",
    )
    parser.add_argument(
        "--version", action="version", version=f"%(prog)s {__version__}"
    )
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("demo", help="run a loopback chat + file demo (no hardware needed)")

    scan = sub.add_parser("scan", help="list paired/nearby Bluetooth devices")
    scan.add_argument(
        "--timeout", type=float, default=15.0, help="scan duration in seconds"
    )
    scan.add_argument(
        "--transport",
        choices=["rfcomm", "ble"],
        default=None,
        help="transport to scan (prompted when omitted)",
    )

    chat = sub.add_parser("chat", help="interactive chat + file session over RFCOMM")
    group = chat.add_mutually_exclusive_group()
    group.add_argument(
        "--address",
        help="peer Bluetooth address (omit to wait for an incoming connection)",
    )
    group.add_argument(
        "--scan",
        action="store_true",
        help="scan for devices, pick one interactively, and connect",
    )
    chat.add_argument(
        "--timeout",
        type=float,
        default=15.0,
        help="scan duration in seconds when --scan is used",
    )
    chat.add_argument(
        "--connect-timeout",
        type=float,
        default=180.0,
        help="seconds to wait for a BLE connection (default: 180)",
    )
    chat.add_argument(
        "--transport",
        choices=["rfcomm", "ble"],
        default=None,
        help="transport to use (prompted when omitted)",
    )
    chat.add_argument(
        "--channel",
        type=int,
        default=None,
        help="RFCOMM channel/port (default: 1; macOS host may fall back to this)",
    )
    chat.add_argument(
        "--receive-dir",
        type=Path,
        default=None,
        help="directory for received files (default: current directory)",
    )

    host = sub.add_parser("llm-host", help="serve a local OpenAI-compatible model over BLE")
    proxy = sub.add_parser("llm-proxy", help="expose a BLE model as a loopback OpenAI endpoint")
    for command in (host, proxy):
        group = command.add_mutually_exclusive_group()
        group.add_argument("--address", help="BLE peer address or identifier (omit to listen)")
        group.add_argument("--scan", action="store_true", help="scan and select a BLE peer")
        command.add_argument("--timeout", type=float, default=15.0, help="scan duration")
        command.add_argument("--connect-timeout", type=float, default=180.0,
                             help="BLE connection timeout in seconds (default: 180)")
    host.add_argument("--base-url", default="http://127.0.0.1:11434/v1",
                      help="upstream OpenAI API base URL (default: Ollama)")
    host.add_argument("--api-key-env", default="GTI_LLM_API_KEY",
                      help="environment variable containing upstream API key")
    proxy.add_argument("--port", type=int, default=8765, help="loopback HTTP port")
    proxy.add_argument("--model", action="append", default=[],
                       help="model ID to advertise in /v1/models (repeatable)")

    return parser


def main(argv: Optional[List[str]] = None) -> int:
    logging.basicConfig(level=logging.WARNING, format="gti: %(message)s")
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.command == "demo":
        asyncio.run(_run_demo())
        return 0

    if args.command == "scan":
        try:
            asyncio.run(_run_scan(args.timeout, args.transport))
        except (ImportError, OSError, RuntimeError) as exc:
            print(f"gti: {exc}", file=sys.stderr)
            return 1
        return 0

    if args.command == "chat":
        try:
            asyncio.run(
                _run_chat(
                    args.address,
                    channel=args.channel,
                    receive_dir=args.receive_dir,
                    scan=args.scan,
                    scan_timeout=args.timeout,
                    transport=args.transport,
                    connect_timeout=args.connect_timeout,
                )
            )
        except (ImportError, OSError, RuntimeError) as exc:
            print(f"gti: {exc}", file=sys.stderr)
            return 1
        return 0

    if args.command in ("llm-host", "llm-proxy"):
        try:
            asyncio.run(_run_llm(args))
        except KeyboardInterrupt:
            return 0
        except (ImportError, OSError, RuntimeError, ValueError) as exc:
            print(f"gti: {exc}", file=sys.stderr)
            return 1
        return 0

    parser.error(f"unknown command {args.command!r}")  # pragma: no cover
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
