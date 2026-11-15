"""gti - symmetric, multiplexed messaging over a single Bluetooth Classic link.

gti runs several independent services (chat, file transfer, ...) over one RFCOMM
connection.  Either peer can initiate a request; every request is answered first
with a small acknowledgement (in-progress / not-supported / error) and then with
one or more content events, correlated by request id so replies may arrive out
of order.

The connection layer is a vendored copy of keytalk's frame ``Transport`` and its
Bluetooth Classic RFCOMM adapters; everything above it is gti's own.
"""

from __future__ import annotations

__version__ = "0.2.0"

__all__ = ["__version__"]
