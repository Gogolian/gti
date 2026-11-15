"""Tests for device discovery helpers (no hardware needed)."""

from __future__ import annotations

import unittest

from gti.discovery import (
    Device,
    _parse_bluetoothctl_devices,
    format_devices,
    pick_device,
)


class TestParseBluetoothctl(unittest.TestCase):
    def test_parses_devices(self) -> None:
        text = (
            "Device 5C:52:30:9B:24:9B MacBook Pro\n"
            "Device 60-3E-5F-33-C4-DF m\n"
            "not a device line\n"
        )
        devices = _parse_bluetoothctl_devices(text)
        self.assertEqual(
            [(d.name, d.address) for d in devices],
            [("m", "60:3E:5F:33:C4:DF"), ("MacBook Pro", "5C:52:30:9B:24:9B")],
        )

    def test_empty(self) -> None:
        self.assertEqual(_parse_bluetoothctl_devices(""), [])


class TestFormatAndPick(unittest.TestCase):
    def setUp(self) -> None:
        self.devices = [
            Device("MacBook Pro", "5C:52:30:9B:24:9B", paired=True),
            Device("m", "60:3E:5F:33:C4:DF", paired=True),
        ]

    def test_format(self) -> None:
        lines = format_devices(self.devices)
        self.assertIn("1. MacBook Pro", lines[0])
        self.assertIn("[5C:52:30:9B:24:9B]", lines[0])
        self.assertIn("(paired)", lines[0])

    def _pick(self, *lines: str):
        it = iter(lines)
        out: list[str] = []

        def input_fn(_prompt: str) -> str:
            return next(it)

        def print_fn(*args, **_kwargs) -> None:
            out.append(" ".join(map(str, args)))

        return pick_device(self.devices, input_fn=input_fn, print_fn=print_fn), out

    def test_pick_by_number(self) -> None:
        chosen, _out = self._pick("2")
        self.assertIsNotNone(chosen)
        self.assertEqual(chosen.address, "60:3E:5F:33:C4:DF")

    def test_pick_by_address(self) -> None:
        chosen, _out = self._pick("5c-52-30-9b-24-9b")
        self.assertIsNotNone(chosen)
        self.assertEqual(chosen.name, "MacBook Pro")

    def test_pick_unknown_address(self) -> None:
        chosen, _out = self._pick("AA:BB:CC:DD:EE:FF")
        self.assertIsNotNone(chosen)
        self.assertEqual(chosen.address, "AA:BB:CC:DD:EE:FF")

    def test_pick_invalid_then_cancel(self) -> None:
        chosen, out = self._pick("nope", "")
        self.assertIsNone(chosen)
        self.assertTrue(any("invalid Bluetooth address" in line for line in out))

    def test_pick_empty_list(self) -> None:
        out: list[str] = []
        chosen = pick_device(
            [], input_fn=lambda _p: "", print_fn=lambda *a, **k: out.append(" ".join(map(str, a)))
        )
        self.assertIsNone(chosen)
        self.assertTrue(any("No Bluetooth devices" in line for line in out))


if __name__ == "__main__":
    unittest.main()
