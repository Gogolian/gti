"""Tests for protocol.py — Envelope encode/decode."""

from __future__ import annotations

import unittest

from gti.protocol import (
    HEADER_SIZE,
    PROTOCOL_VERSION,
    Envelope,
    Flags,
    Kind,
    ProtocolError,
    Status,
)


class TestEnvelopeRoundTrip(unittest.TestCase):
    def _make(self, kind: Kind, payload: bytes = b"") -> Envelope:
        return Envelope(
            version=PROTOCOL_VERSION,
            kind=kind,
            status=Status.OK,
            flags=Flags.NONE,
            service_id=1,
            request_id=42,
            seq=0,
            payload=payload,
        )

    def test_header_size(self) -> None:
        self.assertEqual(HEADER_SIZE, 14)

    def test_encode_decode_request(self) -> None:
        env = self._make(Kind.REQUEST, b"hello world")
        rt = Envelope.decode(env.encode())
        self.assertEqual(rt.kind, Kind.REQUEST)
        self.assertEqual(rt.service_id, 1)
        self.assertEqual(rt.request_id, 42)
        self.assertEqual(rt.payload, b"hello world")

    def test_all_kinds(self) -> None:
        for kind in Kind:
            env = self._make(kind)
            rt = Envelope.decode(env.encode())
            self.assertEqual(rt.kind, kind)

    def test_all_statuses(self) -> None:
        for status in Status:
            env = Envelope(
                version=PROTOCOL_VERSION,
                kind=Kind.ACK,
                status=status,
                flags=Flags.NONE,
                service_id=0,
                request_id=1,
                seq=0,
                payload=b"",
            )
            rt = Envelope.decode(env.encode())
            self.assertEqual(rt.status, status)

    def test_flags_end(self) -> None:
        env = Envelope(
            version=PROTOCOL_VERSION,
            kind=Kind.EVENT,
            status=Status.OK,
            flags=Flags.END,
            service_id=2,
            request_id=99,
            seq=7,
            payload=b"chunk",
        )
        rt = Envelope.decode(env.encode())
        self.assertTrue(rt.flags & Flags.END)
        self.assertEqual(rt.seq, 7)
        self.assertEqual(rt.payload, b"chunk")

    def test_empty_payload(self) -> None:
        env = self._make(Kind.ACK)
        rt = Envelope.decode(env.encode())
        self.assertEqual(rt.payload, b"")

    def test_large_payload(self) -> None:
        big = bytes(range(256)) * 256  # 64 KiB
        env = self._make(Kind.EVENT, big)
        rt = Envelope.decode(env.encode())
        self.assertEqual(rt.payload, big)

    def test_service_id_max(self) -> None:
        env = Envelope(
            version=PROTOCOL_VERSION,
            kind=Kind.REQUEST,
            status=Status.OK,
            flags=Flags.NONE,
            service_id=0xFFFF,
            request_id=0xFFFFFFFF,
            seq=0xFFFFFFFF,
            payload=b"",
        )
        rt = Envelope.decode(env.encode())
        self.assertEqual(rt.service_id, 0xFFFF)
        self.assertEqual(rt.request_id, 0xFFFFFFFF)
        self.assertEqual(rt.seq, 0xFFFFFFFF)


class TestEnvelopeDecodeErrors(unittest.TestCase):
    def test_too_short(self) -> None:
        with self.assertRaises(ProtocolError):
            Envelope.decode(b"\x01\x02\x03")

    def test_wrong_version(self) -> None:
        env = Envelope(
            version=99,
            kind=Kind.REQUEST,
            status=Status.OK,
            flags=Flags.NONE,
            service_id=1,
            request_id=1,
            seq=0,
            payload=b"",
        )
        with self.assertRaises(ProtocolError):
            Envelope.decode(env.encode())

    def test_unknown_kind(self) -> None:
        import struct
        _HEADER = struct.Struct(">BBBBHII")
        raw = _HEADER.pack(PROTOCOL_VERSION, 99, 0, 0, 1, 1, 0)
        with self.assertRaises(ProtocolError):
            Envelope.decode(raw)

    def test_unknown_status(self) -> None:
        import struct
        _HEADER = struct.Struct(">BBBBHII")
        raw = _HEADER.pack(PROTOCOL_VERSION, int(Kind.REQUEST), 99, 0, 1, 1, 0)
        with self.assertRaises(ProtocolError):
            Envelope.decode(raw)


if __name__ == "__main__":
    unittest.main()
