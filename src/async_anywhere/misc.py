"""
Miscellaneous utility functions.
"""
import asyncio
import sys
from typing import Awaitable, Literal, overload, Never

from async_anywhere._util.trio import TrioRuntime


@overload
async def gather(*, return_exceptions: bool = False) -> tuple[()]: ...
@overload
async def gather[T](*coros: Awaitable[T], return_exceptions: Literal[False] = False) -> tuple[T, ...]: ...
@overload
async def gather[T](*coros: Awaitable[T], return_exceptions: bool) -> tuple[T | BaseException, ...]: ...
async def gather[T](*coros: Awaitable[T], return_exceptions: bool = False) -> tuple[T | BaseException, ...]:
    """
    Convenience wrapper for :py:func:`asyncio.gather` which behaves exactly as it does. Exists for two reasons:

    1. :py:func:`asyncio.gather` without any awaitables is somewhat costly to call just to get an empty tuple back. This
       method is cheaper to call.
    2. :py:func:`asyncio.gather` isn't a true async method; it's a synchronous method that returns a ``Future``. That's
       usually desirable, but occasionally it makes some wrapping tools (e.g. ``asgiref.async_to_sync``) not work, and
       also can confuse some type checkers. This is a regular async method, which helps with those situations.
    """
    if len(coros) == 0:
        return ()
    return tuple(await asyncio.gather(*coros, return_exceptions=return_exceptions))


async def wait_forever(*unused) -> Never:
    """
    Waits forever until cancelled or the interpreter shuts down. Works when called from any async runtime
    (e.g. Trio/AnyIO/``asyncio``).

    Args:
        *unused: ignored.

    Returns:
        Never returns.
    """
    del unused  # Remove references to allow GC

    if TrioRuntime.in_task():
        await sys.modules["trio"].sleep_forever()
    elif sys.modules.get("anyio") is not None:  # None is how an import is blocked
        await sys.modules["anyio"].sleep_forever()
    else:
        await asyncio.Future()
    raise RuntimeError("Invariant violation: wait_forever returned somehow!")
