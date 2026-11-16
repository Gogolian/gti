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

Both sides stream through disk: the sender never loads the whole file into
memory, and the receiver writes chunks to a temporary ``.part`` file, verifies
the checksum, then atomically renames it into place.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import tempfile
from pathlib import Path
from typing import Callable, Optional, Union

from ..errors import RemoteError
from ..peer import Peer, RequestContext
from ..protocol import Status
from .base import FILE_TRANSFER_SERVICE_ID

__all__ = [
    "FileTransferService",
    "DEFAULT_CHUNK_SIZE",
    "unique_path",
]

DEFAULT_CHUNK_SIZE = 16 * 1024  # 16 KiB

FileCallback = Callable[[str, Path], None]
ProgressCallback = Callable[[int, int], None]


def unique_path(directory: Union[str, Path], name: str) -> Path:
    """Return a non-existing path in *directory* for *name*.

    Path components are stripped from *name*; duplicates get ``.1``, ``.2``, …
    inserted before the suffix (``notes.txt`` → ``notes.1.txt``).
    """
    directory = Path(directory)
    safe = os.path.basename(name).replace("\x00", "")
    if safe in ("", ".", ".."):
        safe = "received.bin"
    target = directory / safe
    stem, suffix = target.stem, target.suffix
    n = 1
    while target.exists():
        target = directory / f"{stem}.{n}{suffix}"
        n += 1
    return target


def _sha256_file(path: Path, chunk_size: int) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as fh:
        while True:
            chunk = fh.read(chunk_size)
            if not chunk:
                break
            hasher.update(chunk)
    return hasher.hexdigest()


class FileTransferService:
    """Bidirectional file transfer over gti.

    Register ``svc.handle`` on a :class:`~gti.peer.Peer`, then call
    ``await svc.send(path, peer)`` to upload a file.  Received files are
    written to *receive_dir* (default: current directory) and delivered to
    *on_file(name, path)* after checksum verification.
    """

    def __init__(
        self,
        on_file: Optional[FileCallback] = None,
        *,
        receive_dir: Optional[Union[str, Path]] = None,
        max_size: Optional[int] = None,
        on_progress: Optional[ProgressCallback] = None,
    ) -> None:
        self._on_file: FileCallback = on_file or (lambda _n, _p: None)
        self._receive_dir = Path(receive_dir) if receive_dir is not None else None
        self._max_size = max_size
        self._on_progress = on_progress

    async def handle(self, payload: bytes, ctx: RequestContext) -> None:
        """Receive metadata, stream chunks to disk, verify sha256, call on_file."""
        try:
            meta = json.loads(payload.decode())
            name = str(meta["name"])
            expected_size = int(meta["size"])
            expected_sha256 = str(meta["sha256"])
        except (ValueError, KeyError, TypeError) as exc:
            await ctx.error(f"invalid file metadata: {exc}")
            return

        if expected_size < 0:
            await ctx.error(f"invalid file size: {expected_size}")
            return
        if self._max_size is not None and expected_size > self._max_size:
            await ctx.error(
                f"file too large: {expected_size} > {self._max_size} bytes"
            )
            return

        await ctx.ack(Status.IN_PROGRESS)

        directory = self._receive_dir or Path.cwd()
        directory.mkdir(parents=True, exist_ok=True)
        fd, tmp_name = tempfile.mkstemp(
            prefix=".gti-", suffix=".part", dir=directory
        )
        tmp_path = Path(tmp_name)
        hasher = hashlib.sha256()
        total = 0

        try:
            with os.fdopen(fd, "wb") as fh:
                async for chunk in ctx.incoming():
                    total += len(chunk)
                    if self._max_size is not None and total > self._max_size:
                        raise ValueError(
                            f"file too large: more than {self._max_size} bytes"
                        )
                    hasher.update(chunk)
                    fh.write(chunk)
                    if self._on_progress is not None:
                        self._on_progress(total, expected_size)

            if total != expected_size:
                raise ValueError(
                    f"size mismatch: expected {expected_size}, got {total}"
                )
            actual = hasher.hexdigest()
            if actual != expected_sha256:
                raise ValueError(
                    f"sha256 mismatch: expected {expected_sha256}, got {actual}"
                )

            final_path = unique_path(directory, name)
            os.replace(tmp_path, final_path)
            self._on_file(name, final_path)
            await ctx.emit(
                json.dumps(
                    {"ok": True, "name": final_path.name, "size": total}
                ).encode(),
                end=True,
            )
        except asyncio.CancelledError:
            try:
                tmp_path.unlink()
            except FileNotFoundError:
                pass
            raise
        except Exception as exc:
            try:
                tmp_path.unlink()
            except FileNotFoundError:
                pass
            await ctx.error(str(exc))

    async def send(
        self,
        path: Union[str, Path],
        peer: Peer,
        *,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        on_progress: Optional[ProgressCallback] = None,
    ) -> None:
        """Stream *path* in chunks and await receipt confirmation."""
        path = Path(path)
        if chunk_size <= 0:
            raise ValueError("chunk_size must be positive")
        size = path.stat().st_size
        sha256 = _sha256_file(path, chunk_size)
        meta = json.dumps(
            {
                "name": path.name,
                "size": size,
                "sha256": sha256,
                "chunk_size": chunk_size,
            }
        )
        call = await peer.open_call(FILE_TRANSFER_SERVICE_ID, meta.encode())
        complete = False
        try:
            await call.wait_ack()
            remaining = size
            sent = 0
            with path.open("rb") as fh:
                while remaining > 0:
                    chunk = fh.read(min(chunk_size, remaining))
                    if not chunk:
                        raise OSError("file shrank while it was being sent")
                    remaining -= len(chunk)
                    sent += len(chunk)
                    await call.send_chunk(chunk, end=(remaining == 0))
                    if on_progress is not None:
                        on_progress(sent, size)
                    await asyncio.sleep(0)  # yield between chunks
            if size == 0:
                # An empty file still needs a terminating EVENT.
                await call.send_chunk(b"", end=True)
                if on_progress is not None:
                    on_progress(0, 0)

            async for result_bytes in call:
                result = json.loads(result_bytes)
                if not result.get("ok"):
                    raise RemoteError(f"file transfer failed: {result}")
            complete = True
        finally:
            if complete:
                call.close()
            else:
                # Also cancel if ACK times out or the caller stops the upload.
                await call.cancel()
