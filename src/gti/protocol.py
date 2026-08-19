"""gti wire protocol — Envelope encode/decode.

Header layout (big-endian, 14 bytes)::

    offset  size  field
    0       1     version
    1       1     kind       (REQUEST / ACK / EVENT / ERROR / CANCEL)
    2       1     status     (OK / IN_PROGRESS / NOT_SUPPORTED / ERROR)
    3       1     flags      (START / END / MORE bitmask)
    4       2     service_id (u16)
    6       4     request_id (u32)
    10      4     seq        (u32)
    14      N     payload

``kind`` steers routing; ``service_id`` selects the handler on the receiver;
``request_id`` correlates replies across concurrent requests.
"""

from __future__ import annotations

import struct
from dataclasses import dataclass
from enum import IntEnum, IntFlag

__all__ = [
    "PROTOCOL_VERSION",
    "HEADER_SIZE",
    "Kind",
    "Status",
    "Flags",
    "Envelope",
    "ProtocolError",
]

PROTOCOL_VERSION = 1

# >BBBBHII: version(1)+kind(1)+status(1)+flags(1)+service_id(2)+request_id(4)+seq(4)
_HEADER = struct.Struct(">BBBBHII")
HEADER_SIZE = _HEADER.size  # 14 bytes


class Kind(IntEnum):
    REQUEST = 1
    ACK = 2
    EVENT = 3
    ERROR = 4
    CANCEL = 5


class Status(IntEnum):
    OK = 0
    IN_PROGRESS = 1
    NOT_SUPPORTED = 2
    ERROR = 3


class Flags(IntFlag):
    NONE = 0
    START = 1
    END = 2
    MORE = 4


class ProtocolError(Exception):
    """Raised on malformed or version-mismatch frames."""


@dataclass
class Envelope:
    version: int
    kind: Kind
    status: Status
    flags: Flags
    service_id: int
    request_id: int
    seq: int
    payload: bytes

    def encode(self) -> bytes:
        return (
            _HEADER.pack(
                self.version,
                int(self.kind),
                int(self.status),
                int(self.flags),
                self.service_id,
                self.request_id,
                self.seq,
            )
            + self.payload
        )

    @classmethod
    def decode(cls, data: bytes) -> "Envelope":
        if len(data) < HEADER_SIZE:
            raise ProtocolError(f"frame too short: {len(data)} < {HEADER_SIZE}")
        version, kind, status, flags, service_id, request_id, seq = _HEADER.unpack(
            data[:HEADER_SIZE]
        )
        if version != PROTOCOL_VERSION:
            raise ProtocolError(f"unsupported protocol version: {version}")
        try:
            kind_e = Kind(kind)
        except ValueError:
            raise ProtocolError(f"unknown kind: {kind!r}")
        try:
            status_e = Status(status)
        except ValueError:
            raise ProtocolError(f"unknown status: {status!r}")
        return cls(
            version=version,
            kind=kind_e,
            status=status_e,
            flags=Flags(flags),
            service_id=service_id,
            request_id=request_id,
            seq=seq,
            payload=data[HEADER_SIZE:],
        )
