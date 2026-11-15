"""Tests for the connection factory and platform guards (no hardware needed)."""

from __future__ import annotations

import sys
import unittest
from unittest import mock

from gti.adapters import open_connection
from gti.transport import InMemoryTransport


class TestFactory(unittest.TestCase):
    def test_loopback(self) -> None:
        t = open_connection("loopback", name="x")
        self.assertIsInstance(t, InMemoryTransport)
        self.assertEqual(t.name, "x")

    def test_unknown_kind(self) -> None:
        with self.assertRaises(ValueError):
            open_connection("carrier-pigeon")

    def test_unsupported_platform(self) -> None:
        with mock.patch("gti.adapters.sys.platform", "sunos5"):
            with self.assertRaises(RuntimeError):
                open_connection("rfcomm_listen")


class TestPlatformGuards(unittest.TestCase):
    @unittest.skipIf(sys.platform == "linux", "running on Linux")
    def test_linux_transport_rejects_other_platforms(self) -> None:
        from gti.adapters.linux import LinuxRFCOMMHostTransport

        with self.assertRaises(RuntimeError):
            LinuxRFCOMMHostTransport()

    @unittest.skipIf(sys.platform == "win32", "running on Windows")
    def test_windows_transport_rejects_other_platforms(self) -> None:
        from gti.adapters.windows import WindowsRFCOMMHostTransport

        with self.assertRaises(RuntimeError):
            WindowsRFCOMMHostTransport()

    @unittest.skipIf(sys.platform == "darwin", "running on macOS")
    def test_macos_transport_rejects_other_platforms(self) -> None:
        from gti.adapters.macos import MacOSRFCOMMHostTransport

        with self.assertRaises(RuntimeError):
            MacOSRFCOMMHostTransport()


if __name__ == "__main__":
    unittest.main()
