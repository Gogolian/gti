"""Persistent settings for the interactive GTI application."""
from __future__ import annotations

import json
import os
import socket
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


@dataclass(frozen=True)
class Config:
    name: str
    device_id: str
    inference_url: str = "http://127.0.0.1:11434/v1"
    inference_port: int = 8765
    api_key_env: str = "GTI_LLM_API_KEY"


def default_config_path() -> Path:
    base = os.environ.get("XDG_CONFIG_HOME")
    return (Path(base) if base else Path.home() / ".config") / "gti" / "config.json"


def load_config(path: Optional[Path] = None) -> Config:
    """Load settings, creating a name and installation ID on first launch.

    The ID is for connection arbitration, not authentication. Never silently
    replace a malformed configuration: doing so would change the device ID.
    """
    path = path or default_config_path()
    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        values = {"name": socket.gethostname() or "gti", "device_id": uuid.uuid4().hex,
                  "inference_url": "http://127.0.0.1:11434/v1", "inference_port": 8765,
                  "api_key_env": "GTI_LLM_API_KEY"}
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
        return Config(name.strip(), device_id, url, port, key_env)
    except (OSError, ValueError, KeyError, TypeError) as exc:
        raise ValueError(f"invalid GTI configuration {path}: {exc}") from exc
