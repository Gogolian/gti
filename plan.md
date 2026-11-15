# Plan: gti — symmetric multiplexed messaging over vendored RFCOMM

> **Status (v0.2.0):** P0–P6 are implemented and green over the in-memory
> loopback. Remaining work is real-hardware verification of the RFCOMM
> adapters, plus the hardening items tracked in the README/CI.

Bootstrap `gti` by **vendoring** keytalk's `Transport` + RFCOMM adapters (no runtime dependency), then building a **symmetric peer** that multiplexes independent services (chat, file, future screen-share) over **one connection**. Each request follows your flow: **REQUEST → ACK (in-progress / not-supported / error) → EVENT(s) with content**, correlated by `request_id` so replies can arrive out of order. v1 ships **simple chat** + **one-file-at-a-time transfer**, plus a CLI and loopback demo.

Why this shape: keytalk's transport already gives us a frame-oriented byte pipe (one `send()` = one receive, big frames OK), but its protocol/host/consumer are LLM-specific and single-purpose — so gti keeps the transport and builds its own tiny service-multiplexing envelope on top.

## Steps (phased)

### P0 — Scaffolding & packaging
1. Create `pyproject.toml` mirroring keytalk (setuptools, `requires-python>=3.9`, src layout, `py.typed`, MIT, `dependencies=[]`, platform extras, `gti = gti.cli:main`), plus `src/gti/` skeleton, `LICENSE`, expanded README, `tests/`, CLI stub. *Verify: `pip install -e .`, `gti --help`, empty suite green.*

### P1 — Vendor connection adapters *(depends on P0)*
2. Copy keytalk's `Transport` ABC + the length-prefix `StreamTransport` framing base + `InMemoryTransport`/`create_loopback` into `src/gti/transport.py`; copy RFCOMM (`channel` + per-platform macOS/Linux/Windows + loopback) into `src/gti/adapters/`, fixing imports. Add the **abstraction layer**: `open_connection(kind, **kw)` factory + registry that lazily selects the platform RFCOMM transport (keytalk lacks this) so future BLE/TCP adapters drop in. *Verify: `test_transport` loopback round-trip; factory returns loopback.*

### P2 — gti wire protocol (Envelope) *(parallel with P1)*
3. `src/gti/protocol.py`: `Envelope` dataclass + `encode`/`decode` with a 14-byte big-endian header `>BBBBHII` = `version, kind, status, flags, service_id(u16), request_id(u32), seq(u32)` + payload; `Kind` (REQUEST/ACK/EVENT/ERROR/CANCEL), `Status` (OK/IN_PROGRESS/NOT_SUPPORTED/ERROR), `Flags` (START/END/MORE). Mirrors keytalk's `Frame` struct approach. *Verify: `test_protocol` round-trip + validation.*

### P3 — Peer multiplexing core *(depends on P1, P2)*
4. `src/gti/peer.py` + `errors.py`: an outbound `asyncio.Queue` drained by one writer task (your "pseudo queue"); `request_id` allocator; **client** `_pending` map (ack-future + events-queue + done) and a **separate server** `_inflight` map (echoes the peer's `request_id`) so symmetric ids never collide; inbound router branching REQUEST → spawn service handler vs ACK/EVENT/ERROR/END → correlate; `call()`, `register()`/`serve()`, timeout, `RemoteError`. Mirrors `consumer.py` `_pending`/`_alloc_id` + `host.py` dispatch. *Verify: `test_peer` — bidirectional concurrent calls, out-of-order completion, timeout, NOT_SUPPORTED, error propagation.*

### P4 — Chat service *(depends on P3)*
5. `services/base.py` (Service ABC + Status + handler ack/emit ctx) and `services/chat.py`: `send(text)` → REQUEST → await ACK{OK}; receiver acks and surfaces the message via an `on_message` callback; replies are new requests (symmetric). *Verify: `test_chat` end-to-end.*

### P5 — File transfer service *(depends on P3; P4 for interleave test)*
6. `services/file_transfer.py`: REQUEST = JSON metadata `{name,size,sha256,chunk_size}`; responder ACKs accept/not-supported; sender streams the file as EVENT frames (`seq`=chunk index, raw bytes, last flagged END, bounded ~16 KiB so it **interleaves with chat**); receiver writes a temp file, verifies sha256 on END, returns completion EVENT{OK} or ERROR. *Verify: `test_file_transfer` — ack, chunk stream, checksum OK, wrong-checksum → ERROR, and chat-not-blocked-by-file.*

### P6 — CLI + demo *(depends on P4, P5)*
7. `cli.py` (`gti demo` / `gti chat`, a unified interactive session with `/file PATH` and `/quit`) and `examples/demo.py` (two loopback peers exchanging a chat message + a file). *Verify: run demo; `gti --help`; interactive loopback chat.*

## Relevant files
- `/Users/mac1/Desktop/code/gti/pyproject.toml` — mirror keytalk `pyproject.toml` (src layout, `py.typed`, MIT, no core deps, extras `macos=[pyobjc-framework-Cocoa]`; no Linux extra because Linux uses raw sockets).
- `src/gti/transport.py` — vendor keytalk `transport.py` `Transport` ABC + `ble/l2cap/channel.py` framing base (renamed `StreamTransport`).
- `src/gti/adapters/` — vendor `classic/channel.py`, `classic/loopback.py`, and `linux/macos/windows`; add `open_connection()` factory.
- `src/gti/protocol.py` — new `Envelope`, referencing `Frame` in keytalk `protocol.py`.
- `src/gti/peer.py` — new mux, referencing `_pending`/`_alloc_id` in `consumer.py`.
- `src/gti/services/{base,chat,file_transfer}.py`, `src/gti/cli.py`, `examples/demo.py`, `tests/test_*.py`.

## Verification
1. Per phase: `python -m unittest discover -s tests` stays green; `pip install -e .` works after P0.
2. P3 core test: two loopback peers issue A→B and B→A calls concurrently; assert out-of-order completion, timeout, and error propagation.
3. P5: large file transfer in progress does **not** delay a chat message (both complete; chat returns first).
4. P6: `examples/demo.py` runs a chat + file exchange end-to-end over loopback.

## Decisions
- Vendor keytalk transport+RFCOMM (no runtime dep); symmetric `Peer`; service multiplexing via `service_id`+`request_id` in gti's own envelope.
- Skip a reliability/retransmission layer in v1 (RFCOMM + loopback are reliable & ordered; keytalk's Go-Back-N only exists for lossy BLE notifications).
- **Excluded (seams left open):** screen share, BLE/TCP adapters (via the factory), compression, concurrent multi-file, cross-service fairness scheduler, pairing/auth UX.

## Further considerations
1. **Control/metadata encoding** — JSON (stdlib, dependency-free, matches keytalk) for metadata + raw bytes for file chunks. *Recommend JSON+bytes.* Alt: binary struct / msgpack.
2. **`request_id` width** — u32 with separate client/server maps to avoid symmetric collisions. *Recommend u32.* Alt: u16 (matches keytalk).
3. **File chunk size** — configurable default ~16 KiB, bounded so transfers interleave with chat. *Recommend 16 KiB.* Alt: larger for throughput, smaller for responsiveness.
