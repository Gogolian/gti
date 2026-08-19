"""RFCOMM stream transport base — inherits StreamTransport 4-byte framing."""

from __future__ import annotations

from ..transport import StreamTransport

__all__ = ["RFCOMMStreamTransport", "SPP_UUID"]

# Serial Port Profile UUID registered with the Bluetooth SIG.
SPP_UUID = "00001101-0000-1000-8000-00805F9B34FB"


class RFCOMMStreamTransport(StreamTransport):
    """Transport backed by a Bluetooth Classic RFCOMM stream.

    Inherits 4-byte length-prefix framing from :class:`~gti.transport.StreamTransport`.
    Subclasses open the RFCOMM socket/channel and call ``_attach`` + ``_start_recv``.
    """
