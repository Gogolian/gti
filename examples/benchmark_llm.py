"""Repeatable LLM relay benchmark; no Bluetooth hardware or model required.

Run: python examples/benchmark_llm.py
To compare a checkout: PYTHONPATH=/path/to/checkout/src python examples/benchmark_llm.py

Each direction has its own serialized writer. A frame costs a configurable
latency plus its BLE packet count times packet airtime. This models bandwidth
and turnaround costs, NOT radio scheduling, OS queues, interference, or losses.
The upstream emits deterministic Chat Completions SSE; all bytes are verified.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import random
import time

from aiohttp import web

from gti.adapters.ble_framing import FrameAssembler, fragment_frame
from gti.peer import Peer
from gti.services.base import LLM_SERVICE_ID
from gti.services.llm import LLMService, relay
from gti.transport import InMemoryTransport, TransportClosed


class SimulatedBLE(InMemoryTransport):
    def __init__(self, args):
        super().__init__()
        self.args = args
        self.assembler = FrameAssembler()
        self.packets = 0
        self.wire_bytes = 0

    async def send(self, frame):
        if self.is_closed or self._peer is None or self._peer.is_closed:
            raise TransportClosed("link closed")
        packets = fragment_frame(frame, self.args.payload)
        self.packets += len(packets)
        self.wire_bytes += sum(map(len, packets))
        await asyncio.sleep(self.args.latency_ms / 1000 +
                            len(packets) * self.args.packet_ms / 1000)
        for packet in packets:
            for decoded in self._peer.assembler.feed(packet):
                await self._peer._dispatch(decoded)


async def benchmark(args):
    rng = random.Random(42)
    words = "function return async await request response error content model tool arguments context file test value string import class".split()
    prompt = " ".join(rng.choices(words, k=args.prompt_kib * 160))[:args.prompt_kib * 1024]
    body = json.dumps({"model": "benchmark", "stream": True,
                       "messages": [{"role": "user", "content": prompt}]}).encode()
    events = []
    for index in range(args.tokens):
        delta = {"id": "chatcmpl-benchmark", "object": "chat.completion.chunk",
                 "created": 1700000000, "model": "benchmark",
                 "choices": [{"index": 0, "delta": {"content": rng.choice(words) + " "},
                              "finish_reason": None}]}
        events.append(b"data: " + json.dumps(delta, separators=(",", ":")).encode() + b"\n\n")
    events.append(b"data: [DONE]\n\n")
    backend_started = 0.0

    async def upstream(request):
        nonlocal backend_started
        assert await request.read() == body
        backend_started = time.perf_counter()
        response = web.StreamResponse(headers={"Content-Type": "text/event-stream"})
        await response.prepare(request)
        for event in events:
            await response.write(event)
            await asyncio.sleep(args.token_ms / 1000)
        return response

    app = web.Application()
    app.router.add_post("/v1/chat/completions", upstream)
    runner = web.AppRunner(app, handler_cancellation=True)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = site._server.sockets[0].getsockname()[1]
    a, b = SimulatedBLE(args), SimulatedBLE(args)
    a.link(b)
    b.link(a)
    received = bytearray()
    first_data = 0.0
    try:
        async with Peer(a, role="client") as client, Peer(b, role="server") as host:
            host.register(LLM_SERVICE_ID, LLMService(f"http://127.0.0.1:{port}/v1").handle)
            start = time.perf_counter()
            async for kind, data in relay(client, body):
                if kind == "data":
                    if not first_data:
                        first_data = time.perf_counter()
                    received.extend(data)
            end = time.perf_counter()
        assert received == b"".join(events), "response corruption"
        print(json.dumps({
            "elapsed_s": round(end - start, 3),
            "upload_s": round(backend_started - start, 3),
            "first_data_s": round(first_data - start, 3),
            "response_s": round(end - backend_started, 3),
            "response_kib_s": round(len(received) / 1024 / (end - backend_started), 1),
            "request_bytes": len(body), "response_bytes": len(received),
            "up_wire_bytes": a.wire_bytes, "down_wire_bytes": b.wire_bytes,
            "up_packets": a.packets, "down_packets": b.packets,
        }, indent=2))
    finally:
        await runner.cleanup()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--payload", type=int, default=180)
    parser.add_argument("--packet-ms", type=float, default=3)
    parser.add_argument("--latency-ms", type=float, default=10)
    parser.add_argument("--prompt-kib", type=int, default=64)
    parser.add_argument("--tokens", type=int, default=500)
    parser.add_argument("--token-ms", type=float, default=0,
                        help="Upstream generation delay per SSE event; zero tests link saturation")
    args = parser.parse_args()
    if (args.payload < 4 or min(args.packet_ms, args.latency_ms, args.token_ms) < 0
            or args.prompt_kib < 1 or args.tokens < 1):
        parser.error("payload must be >=4, sizes positive and delays nonnegative")
    asyncio.run(benchmark(args))
