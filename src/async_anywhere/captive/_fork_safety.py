"""
CaptiveWorker is fork-safe, in that it preserves the following invariants:

#. Using a CaptiveWorker in the parent process of a process that `fork(2)`s will continue to work.
#. Using a CaptiveWorker that was created before `fork(2)` in a child process of `fork(2)` will **never** indefinitely
block, whether or not `fork(2)` was invoked by CPython's `os.fork`, provided the code that forked calls
`PyOS_AfterFork_Child()`, as CPython requires of anything forking a process that will run Python
(https://docs.python.org/3/c-api/sys.html#c.PyOS_AfterFork_Child).
#. In a child process of `fork(2)`, a CaptiveWorker created before it can't be used. For one running at the fork,
calling its bridged functions or `stop()` raises `WorkerForked`, and its calls in flight fail with one. One draining at
the fork fails likewise, but keeps why it stopped. One that had finished refuses calls, and `stop()` returns, with why
it stopped, as in the parent.
"""

import os
import threading
from typing import TYPE_CHECKING
from weakref import WeakSet

from async_anywhere.captive._exceptions import WorkerForked

if TYPE_CHECKING:
    from async_anywhere.captive import CaptiveWorker

workers: WeakSet[CaptiveWorker] = WeakSet()  # Every worker, once constructed: see CaptiveWorker.__attrs_post_init__()

if hasattr(os, "register_at_fork"):
    def _after_fork_in_child():
        """
        In the child process of a fork, CaptiveWorker instances are invalidated. This is true even if code running on a
        CaptiveWorker's thread did the fork; asyncio's at-fork semantics are cloudy enough that it's not worth trying to
        preserve running workers' states after fork--and that's without getting into what happens when the parent and
        child both try to interact with a worker!

        When fork occurs, this method ensures the following is true in the child process for every CaptiveWorker that
        existed when fork(2) occurred:
        - worker._stopped is unlocked (in case a @bridge call was being dispatched at the moment of fork(2)).
        - The worker's stop bit is set with a reason indicating that fork(2) occurred, unless it had stopped already.
        - The worker is marked as shut down.
        """
        for worker in workers:
            # Its lock may be held, by a thread that isn't in this one, which an RLock won't let this one release
            worker._stopped._condition = threading.Condition()
            worker._stopped.unclean_stop(f"forked into process {os.getpid()}", worker._calls)  # Unless it had stopped
            if worker._fail(worker._finished, WorkerForked()):  # Unless it had finished before the fork
                worker._finalize()  # Failing calls in flight with WorkerForked, as _finished now has
                worker._pending.clear()
        workers.clear()  # None of them can be used here, nor in what forks from here, which needn't revisit them

    os.register_at_fork(after_in_child=_after_fork_in_child)
