"""Length-prefixed framing for BLE GATT packets.

BLE notifications/writes are small, so a gti frame is split into packets of
at most ``max_payload`` bytes.  The receiver concatenates packet payloads and
parses the same 4-byte big-endian length prefix used by the stream transport.
"""

from __future__ import annotations

import struct
from typing import List

__all__ = ["fragment_frame", "FrameAssembler"]

_LEN = struct.Struct(">I")


def fragment_frame(frame: bytes, max_payload: int) -> List[bytes]:
    """Split *frame* into BLE-sized packets, each carrying a length prefix."""
    if max_payload < _LEN.size:
        raise ValueError("max_payload must be at least 4 bytes")
    data = _LEN.pack(len(frame)) + frame
    return [data[i : i + max_payload] for i in range(0, len(data), max_payload)]


class FrameAssembler:
    """Reassemble length-prefixed frames from BLE packet payloads."""

    def __init__(self) -> None:
        self._buffer = bytearray()

    def feed(self, data: bytes) -> List[bytes]:
        self._buffer.extend(data)
        frames: List[bytes] = []
        while len(self._buffer) >= _LEN.size:
            (length,) = _LEN.unpack(self._buffer[: _LEN.size])
            end = _LEN.size + length
            if len(self._buffer) < end:
                break
            frames.append(bytes(self._buffer[_LEN.size : end]))
            del self._buffer[:end]
        return frames
