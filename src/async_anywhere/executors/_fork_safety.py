# Register its fork handler first, so that it runs first (os.register_at_fork() runs them in order):
import async_anywhere.futures  # noqa: F401
from async_anywhere._util.fork_safety import ForkSafeInstanceTracker


class EventLoopExecutorForkSafety(ForkSafeInstanceTracker):
    def after_fork_in_child(self):
        for instance in list(self):  # Not those made here meanwhile, by callbacks
            self.discard(instance)
            instance._after_fork_in_child()


executors = EventLoopExecutorForkSafety()  # Every executor, once constructed: see EventLoopExecutor.__attrs_post_init__()
