"""Tests for gather(), which stands in for asyncio.gather()."""
from __future__ import annotations

import asyncio

import pytest
from asgiref.sync import async_to_sync

from async_anywhere import gather


def test_gathering_nothing_needs_no_loop():
    with pytest.raises(StopIteration) as stopped:
        gather().send(None)  # Outside any loop: asyncio.gather() makes a future on one, even for nothing
    assert stopped.value.value == ()


async def test_results_are_in_the_order_given():
    assert await gather(asyncio.sleep(0.01, "slow"), asyncio.sleep(0, "fast")) == ("slow", "fast")  # Not a list


async def test_errors_are_raised_or_returned():
    async def fail():
        raise KeyError("error")

    with pytest.raises(KeyError):
        await gather(fail())
    error, done = await gather(fail(), asyncio.sleep(0, "done"), return_exceptions=True)
    assert type(error) is KeyError and done == "done"


def test_async_to_sync_can_wrap_it():
    # Unlike asyncio.gather(), which async_to_sync() would call on this thread, outside any loop, and warn about
    assert async_to_sync(gather)(asyncio.sleep(0, "done")) == ("done",)
