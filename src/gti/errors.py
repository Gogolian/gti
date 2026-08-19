"""gti exceptions."""

from __future__ import annotations

__all__ = ["RemoteError", "NotSupportedError"]


class RemoteError(Exception):
    """Raised when the peer returns an ERROR response."""


class NotSupportedError(Exception):
    """Raised when the peer has no handler for the requested service."""
