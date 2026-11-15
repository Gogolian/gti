# gti

**GTI (General Terms Interface)** — an open-source developer helper.

This repository bootstraps gti's foundation: a **symmetric, multiplexed messaging
layer** that runs several independent services over a **single Bluetooth Classic
(RFCOMM) connection**. Either machine can initiate work; every request is answered
first with a small acknowledgement (in-progress / not-supported / error) and then
with one or more content events, correlated by request id so replies may arrive
out of order.

The first two services are **chat** (send text messages) and **file transfer**
(one file at a time). Screen sharing and other services can be added over the
same connection later.

## Architecture

| Layer | Module | Responsibility |
| --- | --- | --- |
| Connection | `gti.transport`, `gti.adapters` | Frame-oriented byte pipe. Vendored from keytalk: a `Transport` interface, a length-prefixed stream base, RFCOMM radios (macOS / Linux / Windows), and an in-memory loopback. A factory selects the adapter so future links (BLE, TCP) drop in. |
| Wire protocol | `gti.protocol` | gti's `Envelope`: version, kind, status, flags, service id, request id, sequence, payload. |
| Peer | `gti.peer` | One symmetric peer per connection: multiplexes requests, routes acks/events by request id, dispatches inbound requests to services, and fails in-flight work when the link drops. |
| Services | `gti.services` | `chat`, `file_transfer`, and the `Service` base they share. |
| CLI | `gti.cli` | `gti demo` (loopback demo) and `gti chat` — a unified interactive session: chat messages and files (`/file PATH`) flow in either direction over the one connection. |

The radio layer is the only part that needs Bluetooth. Everything above it is
pure Python and is exercised end-to-end over the in-memory loopback, so the
protocol and services can be tested without hardware.

## Install

```bash
python3 -m pip install -e .            # core library + in-memory loopback (no radio deps)
python3 -m pip install -e ".[macos]"   # + pyobjc-framework-Cocoa, for RFCOMM on macOS
```

Linux and Windows use the standard-library socket API and need no extra
packages. Bluetooth Classic RFCOMM requires the two devices to be paired first.

## Usage

```bash
gti demo                 # loopback chat + file demo, no hardware needed
gti chat                 # wait for an incoming RFCOMM connection
gti chat --address XX:XX:XX:XX:XX:XX   # dial a peer
```

Inside a `chat` session, both ends are symmetric:

- anything you type is sent as a chat message;
- `/file PATH` sends a file (it streams in chunks, so chat keeps flowing
  while a transfer is in progress);
- received files are saved to the current directory (or `--receive-dir DIR`),
  never overwriting — duplicates get a `.1`, `.2`, … suffix;
- `/quit` leaves the session.

Both peers must run the same command: one side with `--address`, the other
without. Use `--channel N` to override the RFCOMM channel/port (default 1).
Bluetooth Classic RFCOMM requires the two devices to be paired first.

## Library use

```python
import asyncio
from pathlib import Path

from gti.adapters import open_connection
from gti.peer import Peer
from gti.services.chat import ChatService
from gti.services.file_transfer import FileTransferService


async def main() -> None:
    transport = open_connection("rfcomm_connect", address="AA:BB:CC:DD:EE:FF")
    chat = ChatService(on_message=lambda text: print("peer:", text))
    files = FileTransferService(
        on_file=lambda name, path: print("saved", name, "to", path),
        receive_dir=Path("downloads"),
    )
    async with Peer(transport, role="client") as peer:
        peer.register(1, chat.handle)
        peer.register(2, files.handle)
        await chat.send("hello", peer)
        await files.send("photo.jpg", peer)


asyncio.run(main())
```

File transfer streams through disk on both sides: the sender reads the file in
chunks, and the receiver writes to a temporary `.part` file, verifies the
SHA-256, then atomically renames it into place. `FileTransferService` also
accepts `max_size=` (reject oversized files) and `on_progress=` (called with
`(done, total)` bytes).

## Tests

```bash
python3 -m unittest discover -s tests -v
python3 -m mypy src/gti
```

CI runs the suite and mypy on Python 3.9–3.13.

## Status

Early bootstrap, v0.2.0. The connection layer, wire protocol, peer, services,
and CLI are implemented and exercised over the in-memory loopback. The RFCOMM
radio adapters are implemented but **not yet verified against real hardware**;
that is the next milestone.

## License

MIT. Fully open source — contributions and donations welcome.
