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
| Peer | `gti.peer` | One symmetric peer per connection: multiplexes requests, routes acks/events by request id, dispatches inbound requests to services. |
| Services | `gti.services` | `chat`, `file_transfer`, and the `Service` base they share. |
| CLI | `gti.cli` | `gti demo` (loopback demo) and `gti chat` — a unified interactive session: chat messages and files (`/file PATH`) flow in either direction over the one connection. |

The radio layer is the only part that needs Bluetooth. Everything above it is
pure Python and is exercised end-to-end over the in-memory loopback, so the
protocol and services can be tested without hardware.

## Install

```bash
python3 -m pip install -e .            # core library + in-memory loopback (no radio deps)
python3 -m pip install -e ".[macos]"   # + pyobjc-core, for RFCOMM on macOS
python3 -m pip install -e ".[linux]"   # + PyBluez, for device discovery on Linux
```

Bluetooth Classic RFCOMM requires the two devices to be paired first. Windows
uses the standard-library socket API and needs no extra package.

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
- received files are saved to the current directory (never overwriting —
  duplicates get a `.1`, `.2`, … suffix);
- `/quit` leaves the session.

Both peers must run the same command: one side with `--address`, the other
without. Bluetooth Classic RFCOMM requires the two devices to be paired
first.

## Tests

```bash
python3 -m unittest discover -s tests -v
```

## Status

Early bootstrap. The connection layer, wire protocol, peer, services, and CLI
are implemented and exercised over the in-memory loopback; the RFCOMM radio
adapters are not yet verified against real hardware.

## License

MIT. Fully open source — contributions and donations welcome.
