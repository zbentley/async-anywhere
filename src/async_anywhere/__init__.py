"""This is the docstring for async_anywhere."""

__version__ = "0.0.0"

from typing import Awaitable, Literal, overload
import asyncio

@overload
async def gather(*, return_exceptions: bool = False) -> tuple[()]: ...
@overload
async def gather[T](*coros: Awaitable[T], return_exceptions: Literal[False] = False) -> tuple[T, ...]: ...
@overload
async def gather[T](*coros: Awaitable[T], return_exceptions: bool) -> tuple[T | BaseException, ...]: ...
async def gather[T](*coros: Awaitable[T], return_exceptions: bool = False) -> tuple[T | BaseException, ...]:
    """
    Convenience wrapper for asyncio.gather which behaves exactly as it does. Exists for two reasons:

    1. asyncio.gather without any awaitables is somewhat costly to call just to get an empty tuple back. This method
       is cheaper to call.
    2. asyncio.gather isn't a true async method; it's a synchronous method that returns a Future. That's usually
       desirable, but occasionally which makes some wrapping tools (e.g. asgiref.async_to_sync) not work, and also
       can confuse some type checkers. This is a regular async method, which helps with those situations.
    """
    if len(coros) == 0:
        return ()
    return tuple(await asyncio.gather(*coros, return_exceptions=return_exceptions))
