from __future__ import annotations

import socket
import unittest

from aiohttp import ClientSession
from textual.widgets import Switch

from gti.config import Config
from gti.connection import ConnectionManager
from gti.tui import GTIApp
from tests.test_connection import Radio


class TestTUI(unittest.IsolatedAsyncioTestCase):
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
