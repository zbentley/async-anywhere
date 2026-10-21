"""
CaptiveWorker is fork-safe, in that it preserves the following invariants:

#. Using a CaptiveWorker in the parent process of a process that `fork(2)`s will continue to work.
#. Interacting with a CaptiveWorker that was created before `fork(2)` in a child process of `fork(2)` will **never**
indefinitely block, whether or not `fork(2)` was invoked by CPython's `os.fork`.
#. A CaptiveWorker created before fork(2) will refuse to be used in a child process. Attempts to interact with it will
fail.

However, it is important to note that forking in the presence of threads and async code may still cause deadlocks or
other bad behavior in forked child processes, because many of Python's internals are themselves not fork-safe.
This includes internal destructors of asyncio resources (which may run in child processes if a
user triggers garbage collection of a CaptiveWorker) and loggers used for asyncio internals like "Task exception was
never retrieved" warnings. For example, the following situations may still cause undesirable behavior in child processes
during or after fork(2):

#. Discarding an incomplete asyncio.Task (or a future which wraps a Task, like those returned by calls to CaptiveWorker's
bridge methods) may trigger asyncio logging like "Task exception was never retrieved". Python's logging functionality
is not fork-safe and may deadlock on IO stream locks (https://github.com/python/cpython/issues/84579).
#. Calling destructors for async-runtime-related resources, like event loops or concurrent.futures.Futures (both of
which may be present within a CaptiveWorker's internals) can rely on threading.Locks that are corrupted in fork(2)ed
child processes, leading to deadlocks.
#. During interpreter shutdown, async-runtime-related resources initialized before a fork may be cleaned up in unsafe
or potentially-deadlocking ways.

Because of this, it is not advisable to interact with shared asyncio resources, including CaptiveWorkers, across fork(2)
boundaries.

Note that it is **never** safe to fork within a CaptiveWorker's thread (inside a bridge method or parent_task), for the
same reasons that it is never safe to fork within a running `asyncio.Task` (which bridge methods and parent_task are).
The only safe things you can do after forking inside a worker are 1) `exec`ing a subprocess and 2) `os._exit`. Even
`sys.exit` and `raise SystemExit` may fail.
"""

import os
from typing import TYPE_CHECKING
from weakref import WeakSet

if TYPE_CHECKING:
    from async_anywhere.captive import CaptiveWorker

workers: WeakSet[CaptiveWorker] = WeakSet()  # Every worker, once constructed: see CaptiveWorker.__attrs_post_init__()

if hasattr(os, "register_at_fork"):
    def _after_fork_in_child():
        while len(workers) > 0:
            workers.pop()._after_fork_in_child(os.getpid())

    os.register_at_fork(after_in_child=_after_fork_in_child)
