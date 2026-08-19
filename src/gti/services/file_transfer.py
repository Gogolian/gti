"""Single-file transfer service with chunked EVENT upload streaming.

Protocol flow::

    Sender  →  REQUEST(JSON metadata: name/size/sha256/chunk_size)
    Receiver→  ACK{IN_PROGRESS}
    Sender  →  EVENT(chunk₀, seq=0)
    Sender  →  EVENT(chunk₁, seq=1)
    …
    Sender  →  EVENT(chunkₙ, seq=n, END)
    Receiver→  EVENT(JSON{"ok":true}, END)   # after sha256 verification

Chunks are bounded at *chunk_size* (default 16 KiB) so an in-progress
transfer interleaves with other services on the shared send queue.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
from pathlib import Path
from typing import Callable, Optional, Union

from ..peer import Peer, RequestContext
from ..protocol import Status
from .base import FILE_TRANSFER_SERVICE_ID

__all__ = ["FileTransferService", "DEFAULT_CHUNK_SIZE"]

DEFAULT_CHUNK_SIZE = 16 * 1024  # 16 KiB

TransferCallback = Callable[[str, bytes], None]


class FileTransferService:
    """Bidirectional file transfer over gti.

    Register ``svc.handle`` on a :class:`~gti.peer.Peer`, then call
    ``await svc.send(path, peer)`` to upload a file.  Received files are
    delivered to *on_file(name, data)*.
    """

    def __init__(self, on_file: Optional[TransferCallback] = None) -> None:
        self._on_file: TransferCallback = on_file or (lambda _n, _d: None)

    async def handle(self, payload: bytes, ctx: RequestContext) -> None:
        """Receive metadata, collect upload chunks, verify sha256, call on_file."""
        meta = json.loads(payload.decode())
        expected_sha256 = meta["sha256"]
        await ctx.ack(Status.IN_PROGRESS)

        chunks = []
        async for chunk in ctx.incoming():
            chunks.append(chunk)

        data = b"".join(chunks)
        actual = hashlib.sha256(data).hexdigest()
        if actual != expected_sha256:
            await ctx.error(
                f"sha256 mismatch: expected {expected_sha256}, got {actual}"
            )
            return

        self._on_file(meta["name"], data)
        await ctx.emit(json.dumps({"ok": True}).encode(), end=True)

    async def send(
        self,
        path: Union[str, Path],
        peer: Peer,
        *,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
    ) -> None:
        """Send *path* in chunks and await receipt confirmation."""
        path = Path(path)
        data = path.read_bytes()
        sha256 = hashlib.sha256(data).hexdigest()
        meta = json.dumps(
            {
                "name": path.name,
                "size": len(data),
                "sha256": sha256,
                "chunk_size": chunk_size,
            }
        )
        call = await peer.open_call(FILE_TRANSFER_SERVICE_ID, meta.encode())
        try:
            await call.wait_ack()
            offset = 0
            while offset < len(data):
                chunk = data[offset : offset + chunk_size]
                offset += chunk_size
                is_last = offset >= len(data)
                await call.send_chunk(chunk, end=is_last)
                await asyncio.sleep(0)  # yield between chunks for interleaving
            async for result_bytes in call:
                result = json.loads(result_bytes)
                if not result.get("ok"):
                    raise RuntimeError(f"file transfer failed: {result}")
        finally:
            peer._client_pending.pop(call.request_id, None)
