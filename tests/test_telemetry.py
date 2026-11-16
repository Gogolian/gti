"""Tests for the terminal throughput meter."""

from __future__ import annotations

import asyncio
import io
import unittest

from gti.telemetry import TrafficMeter, format_bytes, format_rate


class TestFormatting(unittest.TestCase):
    def test_bytes(self) -> None:
        self.assertEqual(format_bytes(0), "0 B")
        self.assertEqual(format_bytes(999), "999 B")
        self.assertEqual(format_bytes(1000), "1.0 kB")
        self.assertEqual(format_bytes(1_500_000), "1.5 MB")

    def test_rate(self) -> None:
        self.assertEqual(format_rate(1000), "1.0 kB/s")


class TestTrafficMeter(unittest.IsolatedAsyncioTestCase):
    async def test_renders_live_line_and_summary(self) -> None:
        out = io.StringIO()
        meter = TrafficMeter(stream=out, interval=0.01, enabled=True)
        meter.begin()
        meter.add(down=2048, up=128)
        await asyncio.sleep(0.04)
        meter.end()
        await asyncio.sleep(0.04)
        text = out.getvalue()
        self.assertIn("[inference]", text)
        self.assertIn("down", text)
        self.assertIn("up", text)
        self.assertIn(" kB", text)
