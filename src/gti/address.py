"""Bluetooth address helpers."""

from __future__ import annotations

import re

__all__ = ["normalize_address"]

_NON_HEX = re.compile(r"[^0-9A-Fa-f]")


def normalize_address(value: str) -> str:
    """Return *value* as upper-case ``AA:BB:CC:DD:EE:FF``.

    Accepts colon-, hyphen-, or space-separated addresses as well as a bare
    12-digit hex string. Raises :class:`ValueError` for anything else.
    """
    compact = _NON_HEX.sub("", value.strip())
    if len(compact) != 12:
        raise ValueError(f"invalid Bluetooth address: {value!r}")
    return ":".join(compact[i : i + 2] for i in range(0, 12, 2)).upper()
