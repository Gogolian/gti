from __future__ import annotations

import json
import socket
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from aiohttp import ClientSession
from textual.widgets import Input, OptionList, RichLog, Switch

from gti.config import Config, load_config
from gti.connection import ConnectionManager
from gti.tui import GTIApp
from tests.test_connection import Radio


class TestTUI(unittest.IsolatedAsyncioTestCase):
    async def test_slash_reconnect_suggestions_and_shortcut(self) -> None:
        config = Config("Alice", "0" * 32)
        manager = ConnectionManager(config, factory=Radio().factory("a"), retry_delay=0.02)
        app = GTIApp(config, manager=manager)
        with patch.object(manager, "reconnect", new_callable=AsyncMock) as reconnect, \
             patch.object(manager, "send_chat", new_callable=AsyncMock) as send_chat:
            async with app.run_test(size=(110, 35)) as pilot:
                chat = app.query_one("#chat", Input)
                popup = app.query_one("#commands", OptionList)
                await pilot.press("slash", "r")
                await pilot.pause()
                self.assertEqual(chat.value, "/r")
                self.assertTrue(popup.display)
                await pilot.press("tab")
                await pilot.pause()
                self.assertEqual(chat.value, "/reconnect")
                self.assertTrue(chat.has_focus)
                await pilot.press("enter")
                await pilot.pause()
                reconnect.assert_awaited_once()
                self.assertEqual(chat.value, "")
                self.assertFalse(popup.display)
                await pilot.press("slash", "r", "enter")
                await pilot.pause()
                self.assertEqual(reconnect.await_count, 2)
                send_chat.assert_not_awaited()
                await pilot.press("slash", "r")
                await pilot.pause()
                await pilot.click("#commands", offset=(3, 1))
                await pilot.pause()
                self.assertEqual(chat.value, "/reconnect")
                self.assertTrue(chat.has_focus)
                chat.value = ""
                await pilot.pause()
                await pilot.press("tab")
                self.assertFalse(chat.has_focus)
                chat.focus()
                await pilot.press("slash", "z", "enter")
                await pilot.pause()
                self.assertFalse(popup.display)
                send_chat.assert_not_awaited()
                self.assertTrue(any("Unknown command" in line.text
                                    for line in app.query_one("#messages", RichLog).lines))

    async def test_switches_are_restored_and_can_be_disabled(self) -> None:
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "config.json"
            load_config(path)
            data = json.loads(path.read_text())
            data["inference_port"] = port
            path.write_text(json.dumps(data))
            config = load_config(path)
            manager = ConnectionManager(config, factory=Radio().factory("a"))
            async with GTIApp(config, manager=manager).run_test(size=(110, 35)) as pilot:
                await pilot.click("#host")
                await pilot.click("#use")
                await pilot.pause()
                self.assertTrue(manager.host_inference)
            restored = load_config(path)
            self.assertTrue(restored.host_inference)
            self.assertTrue(restored.use_inference)
            manager2 = ConnectionManager(restored, factory=Radio().factory("a"))
            app2 = GTIApp(restored, manager=manager2)
            async with app2.run_test(size=(110, 35)) as pilot:
                await pilot.pause()
                self.assertTrue(manager2.host_inference)
                self.assertTrue(app2.query_one("#host", Switch).value)
                self.assertTrue(app2.query_one("#use", Switch).value)
                async with ClientSession() as session:
                    async with session.get(f"http://127.0.0.1:{port}/v1/models") as response:
                        self.assertEqual(response.status, 503)
                await pilot.click("#host")
                await pilot.click("#use")
                await pilot.pause()
            final = load_config(path)
            self.assertFalse(final.host_inference)
            self.assertFalse(final.use_inference)

    async def test_proxy_start_failure_does_not_save_enabled(self) -> None:
        with socket.socket() as sock, tempfile.TemporaryDirectory() as directory:
            sock.bind(("127.0.0.1", 0))
            sock.listen()
            port = sock.getsockname()[1]
            path = Path(directory) / "config.json"
            load_config(path)
            data = json.loads(path.read_text())
            data["inference_port"] = port
            path.write_text(json.dumps(data))
            config = load_config(path)
            manager = ConnectionManager(config, factory=Radio().factory("a"))
            app = GTIApp(config, manager=manager)
            async with app.run_test(size=(110, 35)) as pilot:
                await pilot.click("#use")
                await pilot.pause()
                self.assertFalse(app.query_one("#use", Switch).value)
                self.assertFalse(load_config(path).use_inference)
                self.assertTrue(any("Cannot start proxy" in line.text
                                    for line in app.query_one("#messages", RichLog).lines))

    async def test_unavailable_saved_proxy_is_retried_on_next_start(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "config.json"
            load_config(path)
            with socket.socket() as sock:
                sock.bind(("127.0.0.1", 0))
                sock.listen()
                port = sock.getsockname()[1]
                data = json.loads(path.read_text())
                data.update(inference_port=port, use_inference=True)
                path.write_text(json.dumps(data))
                config = load_config(path)
                manager = ConnectionManager(config, factory=Radio().factory("a"))
                app = GTIApp(config, manager=manager)
                async with app.run_test(size=(110, 35)) as pilot:
                    await pilot.pause()
                    self.assertFalse(app.query_one("#use", Switch).value)
                    self.assertTrue(load_config(path).use_inference)
                    self.assertTrue(any("Cannot start proxy" in line.text
                                        for line in app.query_one("#messages", RichLog).lines))
            config = load_config(path)
            app = GTIApp(config, manager=ConnectionManager(config, factory=Radio().factory("a")))
            async with app.run_test(size=(110, 35)) as pilot:
                await pilot.pause()
                self.assertTrue(app.query_one("#use", Switch).value)
                self.assertIsNotNone(app._proxy_runner)

    async def test_failed_save_is_visible(self) -> None:
        config = Config("Alice", "0" * 32)
        manager = ConnectionManager(config, factory=Radio().factory("a"))
        app = GTIApp(config, manager=manager)
        with patch("gti.tui.save_inference_settings", side_effect=OSError("disk error")):
            async with app.run_test(size=(110, 35)) as pilot:
                await pilot.click("#host")
                await pilot.pause()
                self.assertTrue(manager.host_inference)
                self.assertTrue(any("change will not persist" in line.text
                                    for line in app.query_one("#messages", RichLog).lines))

    async def test_switches_and_offline_proxy(self) -> None:
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
        config = Config("Alice", "0" * 32, inference_port=port)
        manager = ConnectionManager(config, factory=Radio().factory("a"), retry_delay=0.02)
        app = GTIApp(config, manager=manager)
        async with app.run_test(size=(110, 35)) as pilot:
            await pilot.click("#host")
            await pilot.pause()
            self.assertTrue(manager.host_inference)
            await pilot.click("#use")
            await pilot.pause()
            self.assertTrue(app.query_one("#use", Switch).value)
            async with ClientSession() as session:
                async with session.get(f"http://127.0.0.1:{port}/v1/models") as response:
                    self.assertEqual(response.status, 503)
            await pilot.click("#use")
            await pilot.pause()
            self.assertIsNone(app._proxy_runner)
            await pilot.click("#host")
            await pilot.pause()
            self.assertFalse(manager.host_inference)
