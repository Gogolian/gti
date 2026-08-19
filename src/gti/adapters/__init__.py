"""Connection factory — ``open_connection(kind, **kw)`` selects the platform adapter."""

from __future__ import annotations

import sys
from typing import Any

from ..transport import InMemoryTransport, Transport

__all__ = ["open_connection"]

_KINDS = frozenset({"rfcomm_listen", "rfcomm_connect", "loopback"})


def open_connection(kind: str = "rfcomm_listen", **kw: Any) -> Transport:
    """Return a transport endpoint for the given *kind* on the current platform.

    Supported kinds:

    * ``"rfcomm_listen"``  — host side; blocks in ``start()`` until a client connects.
    * ``"rfcomm_connect"`` — client side; ``start()`` dials the peer (pass ``address=``).
    * ``"loopback"``       — :class:`~gti.transport.InMemoryTransport` (tests / demo).

    RFCOMM kinds lazily import the correct platform adapter so non-Bluetooth
    platforms never import pyobjc or platform-socket constants.
    """
    if kind == "loopback":
        return InMemoryTransport(**kw)

    if kind not in _KINDS:
        raise ValueError(
            f"unknown connection kind {kind!r}; valid kinds: {sorted(_KINDS)}"
        )

    if sys.platform == "linux":
        from .linux import LinuxRFCOMMHostTransport, LinuxRFCOMMConsumerTransport
        if kind == "rfcomm_listen":
            return LinuxRFCOMMHostTransport(**kw)
        return LinuxRFCOMMConsumerTransport(**kw)

    if sys.platform == "darwin":
        from .macos import MacOSRFCOMMHostTransport, MacOSRFCOMMConsumerTransport
        if kind == "rfcomm_listen":
            return MacOSRFCOMMHostTransport(**kw)
        return MacOSRFCOMMConsumerTransport(**kw)

    if sys.platform == "win32":
        from .windows import WindowsRFCOMMHostTransport, WindowsRFCOMMConsumerTransport
        if kind == "rfcomm_listen":
            return WindowsRFCOMMHostTransport(**kw)
        return WindowsRFCOMMConsumerTransport(**kw)

    raise RuntimeError(
        f"unsupported platform for RFCOMM: {sys.platform!r}; use kind='loopback'"
    )
