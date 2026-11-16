"""Live throughput reporting for long-running gti transfers.

The meter aggregates traffic across concurrent requests and renders a terminal
line about once per second. It is deliberately transport-agnostic: callers only
report bytes that moved, so the same meter works for the LLM relay, file
transfer, or future services.
"""

from __future__ import annotations

import asyncio
import sys
import time
from typing import Optional, TextIO

__all__ = ["TrafficMeter", "format_bytes", "format_rate"]

_UNITS = ("B", "kB", "MB", "GB", "TB")


def format_bytes(value: float) -> str:
    """Format a byte count with a compact binary-style suffix."""
    amount = max(0.0, value)
    for unit in _UNITS:
        if amount < 1000.0 or unit == _UNITS[-1]:
            if unit == "B":
                return f"{amount:.0f} B"
            return f"{amount:.1f} {unit}"
        amount /= 1000.0
    return f"{amount:.1f} TB"  # pragma: no cover - the loop always returns


def format_rate(value: float) -> str:
    """Format a bytes-per-second rate like a terminal network meter."""
    return format_bytes(value) + "/s"


class TrafficMeter:
    """Aggregate byte counts and print a 1-second average on a terminal.

    ``stream`` defaults to stderr. Rendering is enabled only when that stream
    is a TTY unless *enabled* is passed explicitly, so background logs do not
    fill with carriage-return updates. Byte accounting is cheap and always
    active; only rendering is conditional.
    """

    def __init__(
        self,
        *,
        stream: Optional[TextIO] = None,
        interval: float = 1.0,
        enabled: Optional[bool] = None,
    ) -> None:
        self._stream = stream if stream is not None else sys.stderr
        self._interval = interval
        if enabled is None:
            isatty = getattr(self._stream, "isatty", None)
            enabled = bool(isatty()) if callable(isatty) else False
        self._enabled = enabled
        self._active = 0
        self._started = time.monotonic()
        self._last_tick = self._started
        self._tick_down = 0
        self._tick_up = 0
        self._total_down = 0
        self._total_up = 0
        self._line_length = 0
        self._task: Optional["asyncio.Task[None]"] = None

    @property
    def enabled(self) -> bool:
        return self._enabled

    def begin(self) -> None:
        """Mark the start of one request; concurrent requests are aggregated."""
        if self._active == 0:
            self._started = time.monotonic()
            self._last_tick = self._started
            self._tick_down = self._tick_up = 0
            self._total_down = self._total_up = 0
        self._active += 1
        self._ensure_task()

    def add(self, *, down: int = 0, up: int = 0) -> None:
        """Report bytes received (down) and sent (up) by a request."""
        if down:
            self._tick_down += down
            self._total_down += down
        if up:
            self._tick_up += up
            self._total_up += up

    def end(self) -> None:
        """Mark one request as finished."""
        if self._active > 0:
            self._active -= 1
        self._ensure_task()

    def _ensure_task(self) -> None:
        if not self._enabled or self._interval <= 0:
            return
        task = self._task
        if task is not None and not task.done():
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        self._task = loop.create_task(self._run())

    async def _run(self) -> None:
        while True:
            await asyncio.sleep(self._interval)
            idle = self._active == 0
            elapsed = max(0.001, time.monotonic() - self._last_tick)
            down_rate = self._tick_down / elapsed
            up_rate = self._tick_up / elapsed
            self._tick_down = self._tick_up = 0
            self._last_tick = time.monotonic()
            if not idle:
                text = (
                    f"[inference] down {format_rate(down_rate)}"
                    f"  up {format_rate(up_rate)}"
                    f"  ({self._active} active)"
                )
                self._render(text)
            # A request may have started while the sleep was in progress.
            if idle and self._active == 0:
                break
        self._task = None
        self._finish()

    def _render(self, text: str) -> None:
        padding = " " * max(0, self._line_length - len(text))
        self._stream.write("\r" + text + padding)
        self._stream.flush()
        self._line_length = len(text)

    def _clear(self) -> None:
        if self._line_length:
            self._stream.write("\r" + " " * self._line_length + "\r")
            self._stream.flush()
            self._line_length = 0

    def _finish(self) -> None:
        if not (self._total_down or self._total_up):
            self._clear()
            return
        elapsed = max(0.001, time.monotonic() - self._started)
        rate = self._total_down / elapsed
        self._clear()
        self._stream.write(
            f"[inference] {format_bytes(self._total_down)} down, "
            f"{format_bytes(self._total_up)} up in {elapsed:.1f}s "
            f"({format_rate(rate)})\n"
        )
        self._stream.flush()
