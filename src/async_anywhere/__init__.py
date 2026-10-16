"""This is the docstring for async_anywhere."""

__version__ = "0.0.0"

from typing import Awaitable
import asyncio

async def gather[T](*coros: Awaitable[T], return_exceptions: bool = False) -> tuple[T]:
    """
    Convenience wrapper for asyncio.gather which behaves exactly as it does. Exists for two reasons:

    1. asyncio.gather without any awaitables is somewhat costly to call just to get an empty tuple back. This method
       is cheaper to call.
    2. asyncio.gather isn't a true async method; it's a synchronous method that returns a Future. That's usually
       desirable, but occasionally which makes some wrapping tools (e.g. asgiref.async_to_sync) not work, and also
       can confuse some type checkers. This is a regular async method, which helps with those situations.
    """
    if not len(coros):
        return ()
    return await asyncio.gather(*coros, return_exceptions=return_exceptions)
