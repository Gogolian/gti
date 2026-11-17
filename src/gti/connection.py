"""Single-peer connection supervisor, independent of the user interface.

Both BLE roles run concurrently. A symmetric handshake prefers the link on
which the lower installation ID is central; the reverse direction can win
after a short grace period if the preferred link isn't available. Bluetooth
addresses are not used as installation identities.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING, Callable, Dict, Optional, Tuple

from .config import Config
from .peer import Peer, RequestContext
from .protocol import Status
from .services.base import CHAT_SERVICE_ID, LLM_SERVICE_ID
from .services.chat import ChatService
from .transport import Transport

if TYPE_CHECKING:
    from .services.llm import LLMService

logger = logging.getLogger(__name__)
CONTROL_SERVICE_ID = 4
CONTROL_VERSION = 1


@dataclass(frozen=True)
class PeerInfo:
    device_id: str
    name: str
    host_inference: bool


def _parse_info(payload: bytes) -> PeerInfo:
    try:
        value = json.loads(payload)
        if not isinstance(value, dict) or value.get("version") != CONTROL_VERSION:
            raise ValueError("unsupported GTI control protocol")
        identity, name, host = value["id"], value["name"], value["host_inference"]
        if not isinstance(identity, str) or len(identity) != 32:
            raise ValueError("invalid device ID")
        identity = uuid.UUID(hex=identity).hex
        if not isinstance(name, str) or not name.strip() or len(name) > 128:
            raise ValueError("invalid peer name")
        if not isinstance(host, bool):
            raise ValueError("invalid inference capability")
        return PeerInfo(identity, name.strip(), host)
    except (ValueError, KeyError, TypeError, UnicodeDecodeError) as exc:
        raise ValueError(f"invalid peer handshake: {exc}") from exc


TransportFactory = Callable[[str], Transport]
StatusCallback = Callable[[str], None]
MessageCallback = Callable[[str, str], None]
InfoCallback = Callable[[Optional[PeerInfo]], None]


class ConnectionManager:
    """Own discovery, one connection, service registration and reconnection.

    The factory receives ``listen`` or ``connect`` and is injectable so the
    state machine can be tested with simulated links instead of BLE hardware.
    """

    def __init__(self, config: Config, *, factory: Optional[TransportFactory] = None,
                 on_status: Optional[StatusCallback] = None,
                 on_message: Optional[MessageCallback] = None,
                 on_peer: Optional[InfoCallback] = None,
                 retry_delay: float = 2.0, heartbeat_interval: float = 15.0,
                 heartbeat_timeout: float = 120.0, handshake_timeout: float = 30.0,
                 fallback_delay: float = 3.0) -> None:
        self.config = config
        self._self_addresses: "set[str]" = set()
        self._factory = factory or self._ble_factory
        self._on_status = on_status or (lambda _: None)
        self._on_message = on_message or (lambda _name, _message: None)
        self._on_peer = on_peer or (lambda _: None)
        self.retry_delay = retry_delay
        self.heartbeat_interval = heartbeat_interval
        self.heartbeat_timeout = heartbeat_timeout
        self.handshake_timeout = handshake_timeout
        self.fallback_delay = fallback_delay
        self.peer: Optional[Peer] = None
        self.peer_info: Optional[PeerInfo] = None
        self.host_inference = False
        self._llm_service: Optional["LLMService"] = None
        self._chat = ChatService()
        self._runner: Optional["asyncio.Task[None]"] = None
        self._announcements: "set[asyncio.Task[None]]" = set()

    def _ble_factory(self, role: str) -> Transport:
        from .adapters.ble import BLETransport
        return BLETransport(role=role, timeout=180.0, exclude=list(self._self_addresses))

    def _info_payload(self) -> bytes:
        return json.dumps({"version": CONTROL_VERSION, "id": self.config.device_id,
                           "name": self.config.name, "host_inference": self.host_inference},
                          separators=(",", ":")).encode()

    def set_host_inference(self, enabled: bool) -> None:
        if enabled and self._llm_service is None:
            from .services.llm import LLMService
            self._llm_service = LLMService(self.config.inference_url,
                                           api_key=os.environ.get(self.config.api_key_env))
        self.host_inference = enabled
        peer = self.peer
        if peer is not None and not peer.is_closed:
            task = asyncio.create_task(self._announce(peer))
            self._announcements.add(task)
            task.add_done_callback(self._announcements.discard)

    async def _announce(self, peer: Peer) -> None:
        try:
            info = await self._exchange(peer, self.heartbeat_timeout)
            if self.peer is peer:
                self._set_peer_info(info)
        except asyncio.CancelledError:
            raise
        except Exception:
            # Heartbeat or link-close detection owns connection recovery.
            logger.debug("capability announcement failed", exc_info=True)

    async def _handle_llm(self, payload: bytes, ctx: RequestContext) -> None:
        if not self.host_inference or self._llm_service is None:
            await ctx.ack(Status.NOT_SUPPORTED)
            return
        await self._llm_service.handle(payload, ctx)

    async def send_chat(self, text: str) -> None:
        peer = self.peer
        if peer is None or peer.is_closed:
            raise ConnectionError("no connected peer")
        await self._chat.send(text, peer)

    def _set_peer_info(self, info: Optional[PeerInfo]) -> None:
        if info != self.peer_info:
            self.peer_info = info
            self._on_peer(info)

    async def _exchange(self, peer: Peer, timeout: float) -> PeerInfo:
        async def receive() -> PeerInfo:
            records = [item async for item in peer.call(CONTROL_SERVICE_ID, self._info_payload(),
                                                         timeout=timeout)]
            if len(records) != 1:
                raise ValueError("invalid control response")
            return _parse_info(records[0])
        return await asyncio.wait_for(receive(), timeout=timeout)

    async def _candidate(self, role: str) -> Optional[Tuple[Peer, PeerInfo]]:
        peer: Optional[Peer] = None
        keep = False
        try:
            transport = self._factory(role)
            peer = Peer(transport, role="server" if role == "listen" else "client")
            candidate_info: Optional[PeerInfo] = None

            async def control(payload: bytes, ctx: RequestContext) -> None:
                nonlocal candidate_info
                info = _parse_info(payload)
                candidate_info = info
                if self.peer is peer:
                    self._set_peer_info(info)
                await ctx.ack()
                await ctx.emit(self._info_payload(), end=True)

            def received(message: str) -> None:
                info = candidate_info or self.peer_info
                self._on_message(info.name if info else "Peer", message)

            chat = ChatService(received)
            peer.register(CONTROL_SERVICE_ID, control)
            peer.register(CHAT_SERVICE_ID, chat.handle)
            peer.register(LLM_SERVICE_ID, self._handle_llm)
            await peer.start()
            info = await self._exchange(peer, self.handshake_timeout)
            candidate_info = info
            if info.device_id == self.config.device_id:
                address = getattr(transport, "remote_address", None)
                if role == "connect" and address:
                    self._self_addresses.add(address.lower())
                raise ValueError("peer has the same device ID; use a separate config on each device")
            # Prefer the same direction on both ends. Keep the reverse link
            # briefly as a fallback: some adapters only work reliably in one
            # GATT role. If the preferred link wins, it closes this candidate.
            should_connect = self.config.device_id < info.device_id
            if (role == "connect") != should_connect:
                try:
                    await asyncio.wait_for(peer.wait_closed(), timeout=self.fallback_delay)
                    return None
                except asyncio.TimeoutError:
                    if peer.is_closed:
                        return None
            keep = True
            return peer, info
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.debug("%s candidate failed", role, exc_info=True)
            self._on_status(f"{role}: {exc}; retrying")
            return None
        finally:
            if peer is not None and not keep:
                await peer.stop()

    async def _race(self) -> Tuple[Peer, PeerInfo]:
        async def attempt(role: str) -> Optional[Tuple[Peer, PeerInfo]]:
            result = await self._candidate(role)
            if result is None:
                await asyncio.sleep(self.retry_delay * random.uniform(1.0, 1.5))
            return result

        tasks: Dict["asyncio.Task[Optional[Tuple[Peer, PeerInfo]]]", str] = {
            asyncio.create_task(attempt(role)): role for role in ("listen", "connect")
        }
        winner: Optional[Tuple[Peer, PeerInfo]] = None
        try:
            while winner is None:
                done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                for task in done:
                    role = tasks.pop(task)
                    result = task.result()
                    if result is not None:
                        if winner is None:
                            winner = result
                        else:
                            await result[0].stop()
                    else:
                        tasks[asyncio.create_task(attempt(role))] = role
            return winner
        finally:
            for task in tasks:
                task.cancel()
            try:
                results = await asyncio.gather(*tasks, return_exceptions=True)
                # A second candidate may finish while cancellation is delivered.
                for pending_result in results:
                    if isinstance(pending_result, tuple) and pending_result is not winner:
                        await pending_result[0].stop()
            except BaseException:
                # A shutdown can arrive after selecting a winner but before it
                # has been transferred to run(); don't leak that transport.
                results = await asyncio.gather(*tasks, return_exceptions=True)
                for pending_result in results:
                    if isinstance(pending_result, tuple) and pending_result is not winner:
                        await pending_result[0].stop()
                if winner is not None:
                    await winner[0].stop()
                raise

    async def _monitor(self, peer: Peer) -> None:
        while not peer.is_closed:
            try:
                await asyncio.wait_for(peer.wait_closed(), timeout=self.heartbeat_interval)
                return
            except asyncio.TimeoutError:
                pass
            try:
                info = await self._exchange(peer, self.heartbeat_timeout)
                if self.peer_info is not None and info.device_id != self.peer_info.device_id:
                    raise ConnectionError("peer identity changed")
                self._set_peer_info(info)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._on_status(f"connection lost: {exc}")
                return

    async def run(self) -> None:
        """Run until cancelled; each lost link is cleaned up before retrying."""
        try:
            while True:
                self._on_status("Discovering and advertising…")
                peer, info = await self._race()
                self.peer = peer
                self._set_peer_info(info)
                self._on_status(f"Connected to {info.name}")
                try:
                    await self._monitor(peer)
                finally:
                    self.peer = None
                    self._set_peer_info(None)
                    await peer.stop()
                self._on_status("Disconnected; retrying…")
                await asyncio.sleep(self.retry_delay)
        finally:
            self.peer = None
            self._set_peer_info(None)
            self._on_status("Stopped")

    def start(self) -> None:
        if self._runner is None or self._runner.done():
            self._runner = asyncio.create_task(self.run())

    async def reconnect(self) -> None:
        """Close the current link or discovery attempts and start fresh.

        Service preferences stay unchanged; in-flight requests fail when the
        old peer closes and are not replayed.
        """
        await self.stop()
        self._on_status("Reconnecting…")
        self.start()

    async def stop(self) -> None:
        if self._runner is not None:
            self._runner.cancel()
            await asyncio.gather(self._runner, return_exceptions=True)
            self._runner = None
        for task in self._announcements:
            task.cancel()
        await asyncio.gather(*self._announcements, return_exceptions=True)
        self._announcements.clear()
