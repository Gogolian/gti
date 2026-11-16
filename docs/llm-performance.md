# LLM relay throughput

## What is optimized

BLE bandwidth and turnaround time are often more limiting than model generation.
The relay keeps the HTTP body byte-for-byte intact, including SSE, tool calls,
UTF-8 split across chunks, and backend error responses. It reduces transport work
rather than parsing or shortening model output:

- Uploads use at most 16 KiB of uncompressed data per record, with one record
  outstanding. This amortizes application ACKs over large agent histories.
- Each data record uses independent zlib compression (level 6) when its body is
  at least 512 bytes and compression reduces its size. Incompressible data is raw.
- The first response bytes are forwarded immediately. Subsequent bytes are
  coalesced into at most 4 KiB, flushing after 100 ms from the first buffered byte
  or at EOF. The timer never cancels the underlying HTTP read.
- A rolling response window permits two records outstanding. One ACK frees one
  slot; the host does not wait for the entire window to drain before refilling.
- The proxy acknowledges a validated response record before yielding it to the
  local HTTP writer. A slow consumer still bounds sender progress to the window
  plus the current record; there is no unbounded relay read-ahead queue.

The window bounds **application-level outstanding records**, not all buffering
inside aiohttp, the OS, or the Bluetooth controller. HTTP/BLE transport queues
still have their own behavior. A batch can also wait behind transmission and flow
control; 100 ms is a coalescing budget, not an end-to-end latency guarantee.

Compression saves link bandwidth; it does not speed up the inference server.
When generation is slower than the link, expect similar completion time with
fewer packets, not a large tokens/second improvement.

## Reproduce without hardware

Install the `llm` extra, then run from the repository root:

```bash
python examples/benchmark_llm.py
python examples/benchmark_llm.py --token-ms 10 --tokens 100 --prompt-kib 1
python examples/benchmark_llm.py --payload 20 --prompt-kib 4 --tokens 50
```

The benchmark starts a local deterministic HTTP backend and relays its SSE over
an in-process, packet-fragmenting link. It verifies every response byte. Each
frame costs 10 ms plus 3 ms per 180-byte packet by default. Directions have
independent serialized writers. This is a controlled bandwidth/latency model,
**not a Bluetooth emulator**: it does not model interference, radio scheduling,
controller queues, packet loss, or platform API costs.

The default workload is a synthetic, compressible 64 KiB word-based history and
500 Chat Completions SSE events. It measures saturation, not model compute.
For a comparison with the pre-optimization implementation:

```bash
baseline=$(mktemp -d)
git archive dd804bc src | tar -x -C "$baseline"
PYTHONPATH="$baseline/src" python examples/benchmark_llm.py
python examples/benchmark_llm.py
```

Example local results (timings vary; wire sizes are deterministic):

| Default workload | Baseline `dd804bc` | Optimized |
| --- | ---: | ---: |
| End-to-end | 4.83 s | 0.79 s |
| Upload | 2.04 s | 0.36 s |
| Response phase | 2.80 s | 0.43 s |
| Upload wire bytes, including control/framing | 67,172 | 11,414 |
| Response wire bytes, including control/framing | 93,537 | 6,223 |
| Upload / response packets | 433 / 575 | 89 / 54 |

This is approximately 6.1x end-to-end for this particular workload and model of
the link. With `--token-ms 10 --tokens 100 --prompt-kib 1`, both versions took
about 1.23 s, while response wire bytes fell from 19,059 to 2,425. That workload
is generation-bound, as expected. Logical response KiB/s in the benchmark counts
**decompressed HTTP bytes**, so it can exceed simulated raw link bandwidth.

## Physical-device validation still required

Compare the same warmed-up model, prompt, output limit, and generation settings
using direct HTTP and the GTI proxy. Record time to first token, full completion
time, prompt size, and output token count, not just the live byte meter. Also
record which OS is host/proxy and which runs `--scan` (BLE central), since
central writes and peripheral notifications use different platform paths.

The live inference meter counts encoded upload/response data-record bytes,
including their one-byte record prefix but excluding application ACKs, envelope
headers and BLE framing. It is neither a radio bandwidth meter nor a token-rate
meter. Compression may make this number **smaller while useful throughput rises**.
The benchmark's wire counts include envelopes, control records, and the four-byte
BLE length prefixes, but not ATT/radio headers.

## Relay protocol versions

These are **LLM service versions**, not changes to the outer GTI envelope.
New proxies request `{"version":2}` (and `"op":"models"` for model discovery).
Update both machines for the optimized path. An old host rejects version 2
before receiving an upload or invoking inference; the proxy does not retry or
risk duplicate inference. A new host also accepts version 1 for old proxies,
using the old 2 KiB raw records and stop-and-wait response flow control.

Version 2 records:

| Direction | Record | Meaning |
| --- | --- | --- |
| proxy → host | `U` + bytes / `u` + zlib bytes | Upload, at most 16 KiB after decoding |
| host → proxy | `A` | Upload record received; send the next record |
| proxy → host | `E` | End of upload |
| host → proxy | `H` + JSON | HTTP status and content type |
| proxy → host | `A` | Headers received |
| host → proxy | `D` + bytes / `d` + zlib bytes | Response, at most 4 KiB after decoding |
| proxy → host | `A` | One response record received; release one window slot |
| host → proxy | `Z`, with envelope END flag | All response records acknowledged; complete |

Only the host chooses the upstream URL/credentials. The 8 MiB total request
limit applies after decompression. Decoding limits each compressed record's
output and rejects truncated streams, concatenated streams, trailing bytes,
empty output, and invalid zlib data. Cancellation closes the batching generator
and cancels any pending upstream read; early proxy exit cancels the remote call.
