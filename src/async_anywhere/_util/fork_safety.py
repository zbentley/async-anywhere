import os
from abc import ABC, abstractmethod
from weakref import WeakSet

class ForkSafeInstanceTracker(WeakSet, ABC):
    __slots__ = ("ensure_registered", "ensure_unregistered")
    def __init__(self):
        super().__init__()

        if hasattr(os, "register_at_fork"):
            self.ensure_registered = self.add
            self.ensure_unregistered = self.discard
            os.register_at_fork(after_in_child=self.after_fork_in_child)
        else:
            self.ensure_registered = self.ensure_unregistered = lambda _: None

    @abstractmethod
    def after_fork_in_child(self):
        raise NotImplementedError
