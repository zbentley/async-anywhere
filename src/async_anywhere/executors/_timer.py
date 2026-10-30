import asyncio
from typing import Any, Callable

from attrs import define, field, validators


@define(kw_only=True)
class Timer:
    _loop: asyncio.AbstractEventLoop = field()
    _callback: Callable[[], Any] = field(validator=validators.is_callable())
    _handle: asyncio.TimerHandle | None = field(default=None)

    def reset(self, seconds: float | None):
        if self._handle is not None:
            self._handle.cancel()
        if seconds is None or seconds == float("inf"):
            self._handle = None
        else:
            self._handle = self._loop.call_later(seconds, self._callback)
