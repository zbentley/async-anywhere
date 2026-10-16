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
from typing import TYPE_CHECKING
from weakref import WeakSet

if TYPE_CHECKING:
    from async_anywhere.captive import CaptiveWorker

workers: WeakSet[CaptiveWorker] = WeakSet()  # Every worker, once constructed: see CaptiveWorker.__attrs_post_init__()

if hasattr(os, "register_at_fork"):
    def _after_fork_in_child():
        while len(workers):
            workers.pop()._after_fork_in_child(os.getpid())

    os.register_at_fork(after_in_child=_after_fork_in_child)
