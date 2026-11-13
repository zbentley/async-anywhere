.. py:currentmodule:: async_anywhere.futures

.. _future_compatibility:

Compatibility
-------------

How :py:class:`AwaitableFuture` works with the standard library's functions for waiting on futures.

.. list-table::
    :header-rows: 1
    :widths: 30 12 58

    * - Operation
      - Supported
      - Notes
    * - :py:func:`concurrent.futures.as_completed`
      - Yes
      - Ignores ``prevent_blocking_wait_within``, so can wait forever.
    * - :py:func:`concurrent.futures.wait`
      - Yes
      - Ignores ``prevent_blocking_wait_within``, so can wait forever.
    * - :py:func:`asyncio.wait`
      - No (Planned)
      - Can hang when a future finishes on a thread other than the waiting loop's.
        Use :py:meth:`as_async_future` instead.
    * - :py:func:`asyncio.wait_for`
      - Yes
      -
    * - :py:func:`asyncio.gather`
      - Yes
      -
    * - :py:func:`asyncio.shield`
      - No
      - Wraps ``AwaitableFuture``\ s in :py:class:`asyncio.Task`\ s which can be left running in the background.
        Use ``ignore_cancel=True`` without ``shield()``, or :py:meth:`as_async_future` with ``shield()``, instead.
    * - :py:func:`asyncio.timeout`
      - Yes
      -
    * - :py:func:`asyncio.as_completed`
      - Partially
      - Wraps ``AwaitableFuture``\ s in :py:class:`asyncio.Task`\ s which can be left running in the background. Use :py:meth:`as_async_future` instead.
    * - :py:func:`asyncio.ensure_future`
      - No (Planned)
      - Wraps ``AwaitableFuture``\ s in :py:class:`asyncio.Task`\ s. Use :py:meth:`as_async_future` instead.
    * - :py:func:`asyncio.wrap_future`
      - Yes
      -
    * - :py:func:`asyncio.create_task`
      - No
      -
    * - :py:func:`asyncio.isfuture`
      - No
      - Returns False. Calling ``isfuture()`` on :py:meth:`as_async_future` returns True.
    * - :py:func:`asyncio.run`
      - Yes
      -
    * - :py:meth:`asyncio.loop.run_until_complete`
      - Yes
      -
