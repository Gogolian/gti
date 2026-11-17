"""BLE helper startup with redirected terminal streams and spawn failures."""
from __future__ import annotations

import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from gti.adapters.ble import BLETransport, _start_process, discover_ble


class TestSpawn(unittest.TestCase):
    def test_redirected_stderr_is_restored_on_failure(self) -> None:
        stream = MagicMock()
        stream.fileno.return_value = -1
        process = MagicMock()

        def fail() -> None:
            self.assertIsNot(sys.stderr, stream)
            with self.assertRaises(OSError):
                sys.stderr.fileno()
            sys.stderr.write("diagnostic")
            raise RuntimeError("spawn failed")

        process.start.side_effect = fail
        with patch.object(sys, "stderr", stream):
            with self.assertRaisesRegex(RuntimeError, "spawn failed"):
                _start_process(process)
            self.assertIs(sys.stderr, stream)
        stream.write.assert_called_once_with("diagnostic")

    def test_real_spawn_with_invalid_stderr_fd(self) -> None:
        # A fresh interpreter is important: an already-running resource tracker
        # would hide the Textual regression on macOS and Linux.
        code = '''\
import io
import multiprocessing
import sys
from gti.adapters.ble import _start_process

class Redirected(io.StringIO):
    def fileno(self):
        return -1

def worker():
    pass

if __name__ == "__main__":
    original = sys.stderr
    redirected = Redirected()
    sys.stderr = redirected
    try:
        proc = multiprocessing.get_context("spawn").Process(target=worker)
        _start_process(proc)
        assert sys.stderr is redirected
        proc.join(10)
        assert proc.exitcode == 0, proc.exitcode
    finally:
        sys.stderr = original
'''
        with tempfile.TemporaryDirectory() as directory:
            script = Path(directory) / "spawn_test.py"
            script.write_text(code, encoding="utf-8")
            result = subprocess.run([sys.executable, str(script)], capture_output=True,
                                    text=True, timeout=20)
        self.assertEqual(result.returncode, 0, result.stderr)


class TestSpawnFailureCleanup(unittest.IsolatedAsyncioTestCase):
    async def test_transport_and_scan_close_pipes_if_spawn_fails(self) -> None:
        for scan in (False, True):
            parent, child = MagicMock(), MagicMock()
            process = MagicMock()
            process.start.side_effect = RuntimeError("spawn failed")
            ctx = MagicMock()
            ctx.Pipe.return_value = (parent, child)
            ctx.Process.return_value = process
            with patch("gti.adapters.ble._require_ble_supported"), \
                 patch("gti.adapters.ble.multiprocessing.get_context", return_value=ctx):
                with self.assertRaisesRegex(RuntimeError, "spawn failed"):
                    if scan:
                        await discover_ble()
                    else:
                        await BLETransport().start()
            parent.close.assert_called_once_with()
            child.close.assert_called_once_with()
