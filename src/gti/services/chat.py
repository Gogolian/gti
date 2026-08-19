"""Simple text chat service.

Protocol flow::

    Sender  →  REQUEST(text_bytes)
    Receiver→  ACK{IN_PROGRESS}
    Receiver→  EVENT(b"", END)       # delivery confirmation

Replies are new requests (symmetric); either peer can initiate.
"""

from __future__ import annotations

from typing import Callable, Optional

from ..peer import Peer, RequestContext
from ..protocol import Status
from .base import CHAT_SERVICE_ID

__all__ = ["ChatService"]

MessageCallback = Callable[[str], None]


class ChatService:
    """Symmetric chat over gti.

    Register ``svc.handle`` on a :class:`~gti.peer.Peer`, then call
    ``await svc.send(text, peer)`` to deliver a message.  Received messages
    are passed to *on_message*.
    """

    def __init__(self, on_message: Optional[MessageCallback] = None) -> None:
        self._on_message: MessageCallback = on_message or (lambda _: None)

    async def handle(self, payload: bytes, ctx: RequestContext) -> None:
        text = payload.decode("utf-8", errors="replace")
        await ctx.ack(Status.IN_PROGRESS)
        self._on_message(text)
        await ctx.emit(b"", end=True)

    async def send(self, text: str, peer: Peer) -> None:
        """Send *text* and await delivery confirmation from the peer."""
        async for _ in peer.call(CHAT_SERVICE_ID, text.encode("utf-8")):
            pass
