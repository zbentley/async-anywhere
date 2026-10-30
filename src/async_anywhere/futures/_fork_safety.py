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
