.. py:currentmodule:: async_anywhere.executors

.. _fork_safety:

``EventLoopExecutor`` Fork Safety
---------------------------------

.. warning::

    It is **never** safe to fork *within* an ``EventLoopExecutor``\ 's thread, for the same reasons that it is never
    safe to fork within a running :py:class:`asyncio.Task` (which describes all :term:`code in an executor`).

    The only guaranteed-safe things you can do after forking inside an executor are:

    #. ``exec``\ ing a subprocess, and
    #.  :py:func:`os._exit`. Even `sys.exit` and `raise SystemExit` are not guaranteed to work.

Other than that, EventLoopExecutor is "fork-safe", in that it preserves the following invariants:

- Using an EventLoopExecutor in the parent process of a process that :py:func:`os.fork`\ s (from a thread other than the
  executor's own, per the above warning) will work normally.
- Interacting with the API of an EventLoopExecutor that was created before ``fork(2)`` in a child process of ``fork(2)``
  will **never** indefinitely block, whether or not ``fork(2)`` was invoked by CPython's :py:func:`os.fork`.
- An EventLoopExecutor created before ``fork(2)`` will refuse to be used in a child process. Attempts to interact with it
  will fail; it will register as having been :ref:`shut down <stopping>` at the moment
  :py:func:`os.fork` was called.
- Constructing and using new EventLoopExecutors after a fork, in the parent or the child process, will work normally.

However, it is important to note that forking in the presence of threads and async code may still cause deadlocks or
other bad behavior in forked child processes, because many of Python's internals are themselves not fork-safe.
This includes internal destructors of asyncio resources (which may run in child processes if a
user triggers garbage collection of an EventLoopExecutor), loggers used for asyncio internals like "Task exception
was never retrieved" warnings, and much more. For example, the following situations may still cause undesirable behavior
in child processes during or after ``fork(2)``:

- Discarding an incomplete :py:class:`asyncio.Task` (or a future which wraps a Task, like those returned by
  :py:func:`EventLoopExecutor.submit`) may trigger asyncio logging like "Task exception was never retrieved". Python's
  logging functionality `is not fork-safe and may deadlock on IO stream locks <https://github.com/python/cpython/issues/84579>`_.
- Calling destructors for async-runtime-related resources, like event loops or :py:class:`concurrent.futures.Future`\ s
  can rely on threading.Locks that are corrupted in ``fork(2)``\ ed child processes, leading to deadlocks.
- During interpreter shutdown, async-runtime-related resources initialized before a fork may be cleaned up in unsafe
  or potentially-deadlocking ways.

Because of this, it is not advisable to interact with shared asyncio resources, including EventLoopExecutors, across
fork(2) boundaries.
