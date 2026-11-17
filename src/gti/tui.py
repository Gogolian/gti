"""Interactive single-peer application. Radio work belongs to ConnectionManager."""
from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Optional

from aiohttp import web
from rich.text import Text
from textual.app import App, ComposeResult
from textual.containers import Horizontal
from textual.widgets import Footer, Header, Input, RichLog, Static, Switch

from .config import Config, load_config
from .connection import ConnectionManager, PeerInfo
from .llm_proxy import create_app


class GTIApp(App[None]):
    TITLE = "GTI"
    CSS = """
    Screen { layout: vertical; }
    #status { height: 2; padding: 0 1; }
    #peer { height: 2; padding: 0 1; }
    #warning { height: 2; padding: 0 1; color: $warning; }
    #services { height: 3; padding: 0 1; }
    #services Static { width: auto; padding: 1 1 0 0; }
    #services Switch { margin-right: 2; }
    #endpoint { height: 2; padding: 0 1; }
    #messages { height: 1fr; border: round $primary; }
    #chat { dock: bottom; }
    """
    BINDINGS = [("ctrl+q", "quit", "Quit")]

    def __init__(self, config: Config, *, manager: Optional[ConnectionManager] = None) -> None:
        super().__init__()
        self.config = config
        self.manager = manager or ConnectionManager(
            config, on_status=self._status_changed, on_message=self._message_received,
            on_peer=self._peer_changed,
        )
        self._proxy_runner: Optional[web.AppRunner] = None
        self._proxy_lock = asyncio.Lock()
        self._use_inference = False
        self._ui_ready = False

    def compose(self) -> ComposeResult:
        yield Header()
        yield Static("Starting…", id="status")
        yield Static("Peer: not connected", id="peer")
        yield Static("Private networks only: GTI peers are not authenticated or encrypted.", id="warning")
        with Horizontal(id="services"):
            yield Static("Host inference")
            yield Switch(value=False, id="host")
            yield Static("Use peer inference")
            yield Switch(value=False, id="use")
        yield Static("Inference proxy: off", id="endpoint")
        yield RichLog(id="messages", wrap=True, markup=False, highlight=False)
        yield Input(placeholder="Message peer and press Enter", id="chat")
        yield Footer()

    def on_mount(self) -> None:
        self._ui_ready = True
        self.query_one("#chat", Input).focus()
        self.manager.start()

    async def on_unmount(self) -> None:
        self._ui_ready = False
        self._use_inference = False
        await self.manager.stop()
        async with self._proxy_lock:
            if self._proxy_runner is not None:
                await self._proxy_runner.cleanup()
                self._proxy_runner = None

    def _status_changed(self, status: str) -> None:
        if self._ui_ready:
            self.query_one("#status", Static).update(Text(f"{self.config.name}: {status}"))

    def _peer_changed(self, info: Optional[PeerInfo]) -> None:
        if not self._ui_ready:
            return
        label = "Peer: not connected" if info is None else (
            f"Peer: {info.name}  |  inference: {'available' if info.host_inference else 'off'}"
        )
        self.query_one("#peer", Static).update(Text(label))

    def _message_received(self, name: str, message: str) -> None:
        if self._ui_ready:
            self.query_one("#messages", RichLog).write(Text(f"{name}: {message}"))

    async def on_input_submitted(self, event: Input.Submitted) -> None:
        message = event.value.strip()
        if not message:
            return
        try:
            await self.manager.send_chat(message)
        except Exception as exc:
            self.query_one("#messages", RichLog).write(Text(f"Message not sent: {exc}"))
            return
        event.input.value = ""
        self.query_one("#messages", RichLog).write(Text(f"{self.config.name}: {message}"))

    async def on_switch_changed(self, event: Switch.Changed) -> None:
        if event.switch.id == "host":
            try:
                self.manager.set_host_inference(event.value)
            except (ValueError, ImportError) as exc:
                self.query_one("#messages", RichLog).write(Text(f"Cannot host inference: {exc}"))
                event.switch.value = False
        elif event.switch.id == "use":
            self._use_inference = event.value
            async with self._proxy_lock:
                if self._use_inference and self._proxy_runner is None:
                    runner = web.AppRunner(
                        create_app(lambda: self.manager.peer if self._use_inference else None),
                        handler_cancellation=True,
                    )
                    try:
                        await runner.setup()
                        await web.TCPSite(runner, "127.0.0.1", self.config.inference_port).start()
                    except (OSError, ValueError) as exc:
                        await runner.cleanup()
                        self._use_inference = False
                        event.switch.value = False
                        self.query_one("#messages", RichLog).write(Text(f"Cannot start proxy: {exc}"))
                        return
                    self._proxy_runner = runner
                elif not self._use_inference and self._proxy_runner is not None:
                    await self._proxy_runner.cleanup()
                    self._proxy_runner = None
                endpoint = (f"Inference proxy: http://127.0.0.1:{self.config.inference_port}/v1"
                            if self._proxy_runner is not None else "Inference proxy: off")
                self.query_one("#endpoint", Static).update(Text(endpoint))


def run_tui(config_path: Optional[Path] = None) -> None:
    from .adapters.ble import _require_ble_supported
    _require_ble_supported()
    GTIApp(load_config(config_path)).run()
