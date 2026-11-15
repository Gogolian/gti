"""Tests for Bluetooth address normalization."""

from __future__ import annotations

import unittest

from gti.address import normalize_address


class TestNormalizeAddress(unittest.TestCase):
    def test_colon(self) -> None:
        self.assertEqual(
            normalize_address("5c:52:30:9b:24:9b"), "5C:52:30:9B:24:9B"
        )

    def test_hyphen(self) -> None:
        self.assertEqual(
            normalize_address("5C-52-30-9B-24-9B"), "5C:52:30:9B:24:9B"
        )

    def test_bare_hex(self) -> None:
        self.assertEqual(normalize_address("5C52309B249B"), "5C:52:30:9B:24:9B")

    def test_spaces(self) -> None:
        self.assertEqual(
            normalize_address(" 5C 52 30 9B 24 9B "), "5C:52:30:9B:24:9B"
        )

    def test_invalid_length(self) -> None:
        with self.assertRaises(ValueError):
            normalize_address("5C:52:30")

    def test_invalid_hex(self) -> None:
        with self.assertRaises(ValueError):
            normalize_address("ZZ:52:30:9B:24:9B")


if __name__ == "__main__":
    unittest.main()
