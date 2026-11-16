# gti

**GTI (General Terms Interface)** — an open-source developer helper.

This repository bootstraps gti's foundation: a **symmetric, multiplexed messaging
layer** that runs several independent services over a **single Bluetooth
connection** (BLE or Classic RFCOMM). Either machine can initiate work; every request is answered
first with a small acknowledgement (in-progress / not-supported / error) and then
with one or more content events, correlated by request id so replies may arrive
out of order.

Services include **chat**, **file transfer**, and an **LLM relay** that exposes
a model running on one machine to an agent harness on the other.

## Architecture

| Layer | Module | Responsibility |
| --- | --- | --- |
| Connection | `gti.transport`, `gti.adapters` | Frame-oriented byte pipe: RFCOMM, BLE, and an in-memory loopback. |
| Wire protocol | `gti.protocol` | gti's `Envelope`: version, kind, status, flags, service id, request id, sequence, payload. |
| Peer | `gti.peer` | One symmetric peer per connection: multiplexes requests, routes acks/events by request id, dispatches inbound requests to services, and fails in-flight work when the link drops. |
| Services | `gti.services` | Chat, file transfer, and a flow-controlled LLM HTTP relay. |
| CLI | `gti.cli` | Loopback demo, interactive chat, and LLM host/proxy commands. |

The radio layer is the only part that needs Bluetooth. Everything above it is
pure Python and is exercised end-to-end over the in-memory loopback, so the
protocol and services can be tested without hardware.

## Install

```bash
python3 -m pip install -e .            # core library + in-memory loopback (no radio deps)
python3 -m pip install -e ".[macos]"   # + pyobjc, for RFCOMM/BLE on macOS
python3 -m pip install -e ".[windows]" # + WinRT, for BLE on Windows
python3 -m pip install -e ".[llm,macos]"   # LLM relay + macOS BLE
python3 -m pip install -e ".[llm,windows]" # LLM relay + Windows BLE
```

RFCOMM on Linux and Windows uses the standard-library socket API. Bluetooth
Classic RFCOMM requires the two devices to be paired first.

## Usage

```bash
gti demo                 # loopback chat + file demo, no hardware needed
gti scan                 # list paired/nearby Bluetooth devices
gti chat                 # pick a transport, then wait for a connection
gti chat --scan          # scan, pick a peer, and connect
gti chat --transport ble # use BLE instead of RFCOMM
gti chat --address XX:XX:XX:XX:XX:XX   # dial a peer
```

When `--transport` is omitted, `gti chat` and `gti scan` prompt for
**RFCOMM** or **BLE**.

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

On macOS, the RFCOMM listener publishes a gti-specific SDP record so
`gti chat --scan` can find it automatically. If SDP publishing is unavailable,
the listener falls back to a fixed RFCOMM channel (default 1) and prints the
channel it is using; pass the same `--channel N` on the dialler.

The BLE transport is implemented on macOS and Windows. It uses a GATT service
with a write characteristic (central → peripheral) and a notify characteristic
(peripheral → central), and fragments frames into BLE-sized packets. BLE is
slower than RFCOMM for file transfer; chat is unaffected. Use
`--connect-timeout N` to wait longer for a BLE peer (default 180s, so an
advertising host is not dropped while the other side starts up).

## Run an LLM over BLE

Run an OpenAI **Chat Completions** compatible inference server on the host,
for example Ollama at `http://127.0.0.1:11434/v1` with a model already loaded.
The model and server must support tool calls for autonomous agent work.
Install the `llm` and appropriate platform extras on **both** machines.

On the inference machine, advertise and wait for the other machine:

```bash
gti llm-host --base-url http://127.0.0.1:11434/v1
```

On the harness machine, scan for the host and expose the model locally:

```bash
gti llm-proxy --scan --model qwen2.5-coder:7b
# OpenAI-compatible base URL: http://127.0.0.1:8765/v1
```

Either side can listen or connect: use `--address ADDRESS` instead of `--scan`
to connect to a known BLE address/identifier, or omit both to listen. The two
ends must use opposite roles. `--port` changes the proxy's loopback port;
repeat `--model` to advertise IDs via `/v1/models` (this list is configured
locally, not discovered from the host). If the upstream requires a bearer key,
set `GTI_LLM_API_KEY` on the **host**, or use `--api-key-env NAME`. Proxy
credentials are not forwarded to the host.

For Pi, add a provider to `~/.pi/agent/models.json` on the harness machine
(merge it with any existing providers, and replace the model ID and limits):

```json
{
  "providers": {
    "gti": {
      "baseUrl": "http://127.0.0.1:8765/v1",
      "api": "openai-completions",
      "apiKey": "gti",
      "models": [{ "id": "qwen2.5-coder:7b", "contextWindow": 32768, "maxTokens": 4096 }]
    }
  }
}
```

Select the model with `/model`. The dummy key makes the model selectable; it
is not authentication. Configure Pi compatibility settings if your particular
backend requires them. Other harnesses can use the same base URL. For a quick
check:

```bash
curl -N http://127.0.0.1:8765/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -d '{"model":"qwen2.5-coder:7b","messages":[{"role":"user","content":"Hello"}],"stream":true}'
```

The relay passes request JSON and response bytes through unchanged, including
streamed SSE, tool calls, usage and non-streaming responses. It allows only
`POST /v1/chat/completions` and `GET /v1/models`, not arbitrary HTTP routes.
The proxy listens only on `127.0.0.1` and rejects browser Origin requests, but
other local processes can use it. **There is no GTI peer authentication or
application-layer encryption**: a nearby BLE peer may be able to use your
inference server and see prompts and responses. Use only in a trusted setting;
do not rely on pairing as a security boundary. Requests are limited to 8 MiB;
BLE is slow and large agent histories can take a long time to upload. The
connection serves one peer and exits on disconnect; restart both commands to
reconnect. Streaming has been tested over loopback, not yet on physical BLE.

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
python3 -m pip install -e ".[dev]"
python3 -m unittest discover -s tests -v
python3 -m mypy src/gti
```

CI runs the suite and mypy on Python 3.9–3.13.

## Status

Early bootstrap, v0.2.0. The connection layer, wire protocol, peer, services,
and CLI are implemented and exercised over the in-memory loopback. LLM relay
streaming and tool-call passthrough still need verification on physical BLE with
an actual inference server and agent harness.

## Known limitations

- RFCOMM adapters are not yet verified on real paired devices.
- macOS SDP publishing may be unavailable; the listener falls back to a fixed
  RFCOMM channel, so the dialler may need an explicit `--channel`.
- RFCOMM scanning lists paired devices on macOS and known devices via
  `bluetoothctl` on Linux; Windows RFCOMM discovery is not implemented.
- BLE transport is implemented on macOS and Windows; Linux BLE is future work.
- Windows BLE peripheral mode requires Windows 10 1803+ and an adapter that
  supports the LE peripheral role.
- BLE file transfer is slower than RFCOMM (typically 2–10×, depending on PHY,
  MTU, and connection interval).
- Pairing is not managed by GTI.
- One file at a time; concurrent multi-file transfers are not implemented.
- No TCP adapter for the gti peer protocol.
- No GTI authentication or application-layer encryption; BLE GATT uses plain permissions.

## License

MIT. Fully open source — contributions and donations welcome.
