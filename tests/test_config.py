from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from gti.config import load_config, save_inference_settings


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

    def test_save_switches_preserves_other_settings(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "config.json"
            config = load_config(path)
            data = json.loads(path.read_text())
            data["name"] = "Edited after launch"
            data["custom"] = {"keep": True}
            path.write_text(json.dumps(data))
            save_inference_settings(config, host_inference=True)
            save_inference_settings(config, use_inference=True)
            result = load_config(path)
            self.assertTrue(result.host_inference)
            self.assertTrue(result.use_inference)
            self.assertEqual(result.name, "Edited after launch")
            self.assertEqual(json.loads(path.read_text())["custom"], {"keep": True})
            self.assertFalse(list(path.parent.glob(".gti-config-*.tmp")))

    def test_old_configs_default_to_disabled_and_validate_flags(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "config.json"
            load_config(path)
            data = json.loads(path.read_text())
            data.pop("host_inference")
            data.pop("use_inference")
            path.write_text(json.dumps(data))
            result = load_config(path)
            self.assertFalse(result.host_inference)
            self.assertFalse(result.use_inference)
            data["host_inference"] = "false"
            path.write_text(json.dumps(data))
            with self.assertRaisesRegex(ValueError, "must be booleans"):
                load_config(path)

    def test_invalid_port(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "config.json"
            load_config(path)
            data = json.loads(path.read_text())
            data["inference_port"] = 0
            path.write_text(json.dumps(data))
            with self.assertRaisesRegex(ValueError, "inference_port"):
                load_config(path)
