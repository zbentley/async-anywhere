.. py:currentmodule:: async_anywhere.futures.awaitable

.. _future_fork_safety:

``AwaitableFuture`` Fork Safety
-------------------------------

Stdlib Future-like primitives have several problems related to ``fork(2)``:

- In child processes created by :py:func:`os.fork`, :py:obj:`concurrent.futures.Future`\ s can easily become
  deadlocked and/or corrupt later calls to :py:func:`concurrent.futures.wait` or
  :py:func:`concurrent.futures.as_completed`.
- Neither :py:obj:`asyncio.Future` nor any other awaitable primitive allows waits to occur on the far side of a
  forked process boundary.
- Done callbacks added via ``add_done_callback`` can run twice (once in the parent and once in the child) when
  added to futures that are interacted with on both sides of a ``fork(2)``.

:py:class:`AwaitableFuture` addresses all of those issues:

- ``AwaitableFuture``\ s will never deadlock or break ``concurrent.futures`` code after fork.
- ``AwaitableFuture``\ s can be blocked on or ``await``\ ed from any process. However, results aren't
  transmitted across process boundaries, so, depending on what you're using futures for, results may never
  arrive. Still, a cancellable ``await`` or time-outable blocking wait are much preferable to an uninterruptible
  deadlock after ``fork(2)`` caused by ``concurrent.futures.Future``\ 's internal condition lock corruption.
- ``AwaitableFuture``\ s can be configured to cancel themselves immediately when forked into a child process,
  to prevent easy-to-make mistakes.
- :py:meth:`AwaitableFuture.add_done_callback` can configure callbacks to always (or never) run in forked child
  processes.
