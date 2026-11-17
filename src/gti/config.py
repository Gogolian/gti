"""Persistent settings for the interactive GTI application."""
from __future__ import annotations

import json
import os
import socket
import stat
import tempfile
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional


@dataclass(frozen=True)
class Config:
    name: str
    device_id: str
    inference_url: str = "http://127.0.0.1:11434/v1"
    inference_port: int = 8765
    api_key_env: str = "GTI_LLM_API_KEY"
    host_inference: bool = False
    use_inference: bool = False
    config_path: Optional[Path] = field(default=None, repr=False, compare=False)


def default_config_path() -> Path:
    base = os.environ.get("XDG_CONFIG_HOME")
    return (Path(base) if base else Path.home() / ".config") / "gti" / "config.json"


def load_config(path: Optional[Path] = None) -> Config:
    """Load settings, creating a name and installation ID on first launch.

    The ID is for connection arbitration, not authentication. Never silently
    replace a malformed configuration: doing so would change the device ID.
    """
    path = (path or default_config_path()).resolve()
    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        values = {"name": socket.gethostname() or "gti", "device_id": uuid.uuid4().hex,
                  "inference_url": "http://127.0.0.1:11434/v1", "inference_port": 8765,
                  "api_key_env": "GTI_LLM_API_KEY", "host_inference": False,
                  "use_inference": False}
        try:
            with path.open("x", encoding="utf-8") as f:
                json.dump(values, f, indent=2)
                f.write("\n")
        except FileExistsError:
            pass
    try:
        values = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(values, dict):
            raise ValueError("expected a JSON object")
        name = values["name"]
        device_id = values["device_id"]
        url = values.get("inference_url", "http://127.0.0.1:11434/v1")
        port = values.get("inference_port", 8765)
        key_env = values.get("api_key_env", "GTI_LLM_API_KEY")
        host_inference = values.get("host_inference", False)
        use_inference = values.get("use_inference", False)
        if not isinstance(name, str) or not name.strip() or len(name) > 128:
            raise ValueError("name must be a non-empty string (max 128 characters)")
        if not isinstance(device_id, str) or len(device_id) != 32:
            raise ValueError("device_id must be a 32-character UUID hex string")
        device_id = uuid.UUID(hex=device_id).hex
        if not isinstance(url, str) or not url:
            raise ValueError("inference_url must be a non-empty string")
        if not isinstance(port, int) or isinstance(port, bool) or not 1 <= port <= 65535:
            raise ValueError("inference_port must be between 1 and 65535")
        if not isinstance(key_env, str) or not key_env:
            raise ValueError("api_key_env must be a non-empty string")
        if not isinstance(host_inference, bool) or not isinstance(use_inference, bool):
            raise ValueError("host_inference and use_inference must be booleans")
        return Config(name.strip(), device_id, url, port, key_env,
                      host_inference, use_inference, path)
    except (OSError, ValueError, KeyError, TypeError) as exc:
        raise ValueError(f"invalid GTI configuration {path}: {exc}") from exc


def save_inference_settings(config: Config, *, host_inference: Optional[bool] = None,
                            use_inference: Optional[bool] = None) -> None:
    """Atomically update switch preferences without discarding other settings.

    Configs constructed in memory have no backing file and are not persisted.
    """
    path = config.config_path
    if path is None or (host_inference is None and use_inference is None):
        return
    values = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(values, dict):
        raise ValueError("configuration must be a JSON object")
    if host_inference is not None:
        values["host_inference"] = host_inference
    if use_inference is not None:
        values["use_inference"] = use_inference
    mode = stat.S_IMODE(path.stat().st_mode)
    fd, temp_name = tempfile.mkstemp(prefix=".gti-config-", suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(values, stream, indent=2)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.chmod(temp_name, mode)
        os.replace(temp_name, path)
    finally:
        try:
            os.unlink(temp_name)
        except FileNotFoundError:
            pass
