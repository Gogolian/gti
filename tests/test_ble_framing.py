"""Tests for BLE packet fragmentation/reassembly."""

from __future__ import annotations

import unittest

from gti.adapters.ble_framing import FrameAssembler, fragment_frame


class TestBLEFraming(unittest.TestCase):
    def test_round_trip(self) -> None:
        frame = b"hello ble"
        packets = fragment_frame(frame, 8)
        self.assertGreater(len(packets), 1)
        assembler = FrameAssembler()
        frames = []
        for packet in packets:
            frames.extend(assembler.feed(packet))
        self.assertEqual(frames, [frame])

    def test_multiple_frames(self) -> None:
        frames = [b"one", b"two", b"three"]
        data = b"".join(
            packet for frame in frames for packet in fragment_frame(frame, 4)
        )
        assembler = FrameAssembler()
        self.assertEqual(assembler.feed(data), frames)

    def test_partial_feed(self) -> None:
        frame = b"x" * 100
        packets = fragment_frame(frame, 10)
        assembler = FrameAssembler()
        out = []
        for packet in packets:
            out.extend(assembler.feed(packet))
        self.assertEqual(out, [frame])

    def test_empty_frame(self) -> None:
        packets = fragment_frame(b"", 20)
        assembler = FrameAssembler()
        out = []
        for packet in packets:
            out.extend(assembler.feed(packet))
        self.assertEqual(out, [b""])

    def test_invalid_payload(self) -> None:
        with self.assertRaises(ValueError):
            fragment_frame(b"x", 3)


if __name__ == "__main__":
    unittest.main()
