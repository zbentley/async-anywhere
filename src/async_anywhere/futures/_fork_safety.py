"""
AwaitableFuture keeps concurrent.futures.Future's documented promise that a callback "will always be called by a thread
in the same process in which it was added", across fork(2) too (which CPython's own futures don't). In the child process
of a fork, for every AwaitableFuture that hadn't finished when fork(2) occurred:

#. Its condition is replaced, and concurrent.futures.wait()'s and as_completed()'s waiters dropped: threads that held
   the one, or are the others, aren't in the child.
#. Its done callbacks are dropped, bar those added with run_in_forked_child=True, which may so run once in each process.
#. If it was made with cancel_pending_in_forked_child=True, it's cancelled (ignore_cancel or not), running the callbacks
   it kept: as nothing in the child may finish it otherwise. Only once every future has had the above done, so those
   callbacks find any other one usable. Otherwise, it's left pending, as a concurrent.futures.Future would be.

Futures that had finished are left as they are: one whose condition another thread held as the process forked (in
concurrent.futures.wait(), say) is held forever in the child, as for any concurrent.futures.Future. Futures leave the
registry below only once they've finished and released their condition, so that never strands a pending one. In the
child, those left pending stay in it, for processes forked from that one; those that had finished, but not yet left it,
as another thread was finishing them as the process forked, are readied too, then dropped, as they'll never run
callbacks again.

Modules whose own fork handlers finish futures in the child (see captive._fork_safety) must import this one first, so
that its handler runs first.
"""

from weakref import WeakSet

from async_anywhere._util.fork_safety import ForkSafeInstanceTracker


class AwaitableFutureForkSafety(ForkSafeInstanceTracker):
    def after_fork_in_child(self):
        to_cancel = WeakSet()
        # Reset each future's internal state in one pass, then run any callbacks in another pass. Otherwise, callbacks
        # on one AwaitableFuture that mutate another AwaitableFuture's state might hang/deadlock due to misaligned
        # threading.Conditions after fork.
        # Iterated, not drained: those left pending stay registered, so that processes forked from this one ready them too.
        # A WeakSet iterates a copy of itself, so unregistering as it goes (or futures made meanwhile) is safe.
        for future in self.instances:
            future._after_fork_in_child()
            if future.done():  # Finished on a thread that isn't here, which was yet to unregister it
                self.ensure_unregistered(future)
            elif future._cancel_pending_in_forked_child:
                to_cancel.add(future)
        while len(to_cancel) > 0:
            to_cancel.pop()._cancel(even_running=True)


# Every AwaitableFuture until it has finished: see AwaitableFuture.__init__() and _invoke_callbacks()
futures = AwaitableFutureForkSafety()
