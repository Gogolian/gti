"""Interactive single-peer application. Radio work belongs to ConnectionManager."""
from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Optional

from aiohttp import web
from rich.text import Text
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Horizontal
from textual.widgets import Footer, Header, Input, OptionList, RichLog, Static, Switch

from .config import Config, load_config, save_inference_settings
from .connection import ConnectionManager, PeerInfo
from .llm_proxy import create_app


_RECONNECT = "/reconnect"


def _is_command_prefix(value: str) -> bool:
    return value.startswith("/") and _RECONNECT.startswith(value.lower())


class ChatInput(Input):
    """Complete a visible slash command without stealing normal Tab navigation."""

    BINDINGS = [Binding("tab", "complete_command", show=False, priority=True)]

    def check_action(self, action: str, parameters: tuple) -> Optional[bool]:
        if action == "complete_command":
            return _is_command_prefix(self.value)
        return super().check_action(action, parameters)

    def action_complete_command(self) -> None:
        self.value = _RECONNECT
        self.cursor_position = len(self.value)


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
    #commands { display: none; height: 3; border: round $primary; }
    #chat { height: 3; }
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
            yield Switch(value=self.config.host_inference, id="host")
            yield Static("Use peer inference")
            yield Switch(value=self.config.use_inference, id="use")
        yield Static("Inference proxy: off", id="endpoint")
        yield RichLog(id="messages", wrap=True, markup=False, highlight=False)
        yield OptionList("/reconnect    Restart BLE connection  [Tab to complete]", id="commands")
        yield ChatInput(placeholder="Message peer or type / for commands", id="chat")
        yield Footer()

    async def on_mount(self) -> None:
        self._ui_ready = True
        self.query_one("#chat", Input).focus()
        if self.config.host_inference and not self._set_host(True):
            self._reset_switch("host")
        if self.config.use_inference and not await self._set_proxy(True):
            self._reset_switch("use")
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

    def on_input_changed(self, event: Input.Changed) -> None:
        if event.input.id == "chat":
            self.query_one("#commands", OptionList).display = _is_command_prefix(event.value)

    def on_option_list_option_selected(self, event: OptionList.OptionSelected) -> None:
        if event.option_list.id == "commands":
            chat = self.query_one("#chat", ChatInput)
            chat.value = _RECONNECT
            chat.cursor_position = len(chat.value)
            chat.focus()

    async def on_input_submitted(self, event: Input.Submitted) -> None:
        message = event.value.strip()
        if not message:
            return
        if message.startswith("/"):
            if len(message) > 1 and _is_command_prefix(message):
                event.input.value = ""
                self.query_one("#messages", RichLog).write(Text("Reconnecting…"))
                await self.manager.reconnect()
            else:
                self.query_one("#messages", RichLog).write(Text(f"Unknown command: {message}"))
            return
        try:
            await self.manager.send_chat(message)
        except Exception as exc:
            self.query_one("#messages", RichLog).write(Text(f"Message not sent: {exc}"))
            return
        event.input.value = ""
        self.query_one("#messages", RichLog).write(Text(f"{self.config.name}: {message}"))

    def _reset_switch(self, name: str) -> None:
        switch = self.query_one(f"#{name}", Switch)
        with switch.prevent(Switch.Changed):
            switch.value = False

    def _set_host(self, enabled: bool) -> bool:
        try:
            self.manager.set_host_inference(enabled)
            return True
        except (ValueError, ImportError) as exc:
            self.query_one("#messages", RichLog).write(Text(f"Cannot host inference: {exc}"))
            return False

    async def _set_proxy(self, enabled: bool) -> bool:
        self._use_inference = enabled
        async with self._proxy_lock:
            if enabled and self._proxy_runner is None:
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
                    self.query_one("#messages", RichLog).write(Text(f"Cannot start proxy: {exc}"))
                    return False
                self._proxy_runner = runner
            elif not enabled and self._proxy_runner is not None:
                await self._proxy_runner.cleanup()
                self._proxy_runner = None
            endpoint = (f"Inference proxy: http://127.0.0.1:{self.config.inference_port}/v1"
                        if self._proxy_runner is not None else "Inference proxy: off")
            self.query_one("#endpoint", Static).update(Text(endpoint))
        return True

    async def on_switch_changed(self, event: Switch.Changed) -> None:
        if event.switch.id == "host":
            if not self._set_host(event.value):
                self._reset_switch("host")
                return
            settings = {"host_inference": event.value}
        elif event.switch.id == "use":
            if not await self._set_proxy(event.value):
                self._reset_switch("use")
                return
            settings = {"use_inference": event.value}
        else:
            return
        try:
            save_inference_settings(self.config, **settings)
        except (OSError, ValueError) as exc:
            self.query_one("#messages", RichLog).write(
                Text(f"Could not save inference setting; change will not persist: {exc}")
            )


def run_tui(config_path: Optional[Path] = None) -> None:
    from .adapters.ble import _require_ble_supported
    _require_ble_supported()
    GTIApp(load_config(config_path)).run()
