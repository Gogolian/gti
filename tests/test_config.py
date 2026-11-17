from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from gti.config import load_config


class TestConfig(unittest.TestCase):
    def test_create_and_reload(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "gti" / "config.json"
            original = load_config(path)
            data = json.loads(path.read_text())
            data["name"] = "Kamil"
            path.write_text(json.dumps(data))
            updated = load_config(path)
            self.assertEqual(updated.name, "Kamil")
            self.assertEqual(updated.device_id, original.device_id)
            self.assertEqual(updated.inference_port, 8765)

    def test_bad_config_is_not_overwritten(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "config.json"
            path.write_text('{"name": "bad"}')
            with self.assertRaisesRegex(ValueError, "invalid GTI configuration"):
                load_config(path)
            self.assertEqual(path.read_text(), '{"name": "bad"}')

    def test_invalid_port(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "config.json"
            load_config(path)
            data = json.loads(path.read_text())
            data["inference_port"] = 0
            path.write_text(json.dumps(data))
            with self.assertRaisesRegex(ValueError, "inference_port"):
                load_config(path)
