"""Smoke test: the package imports and exposes its version."""

from __future__ import annotations

import unittest

import gti


class SmokeTest(unittest.TestCase):
    def test_version(self) -> None:
        self.assertEqual(gti.__version__, "0.1.0")


if __name__ == "__main__":
    unittest.main()
