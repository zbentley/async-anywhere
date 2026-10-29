import os
from abc import ABC, abstractmethod
from weakref import WeakSet

_HAS_ATFORK = hasattr(os, "register_at_fork")

class ForkSafeInstanceTracker(ABC):
    def __init__(self):
        self.instances = WeakSet()
        if _HAS_ATFORK:
            os.register_at_fork(after_in_child=self.after_fork_in_child)

    @abstractmethod
    def after_fork_in_child(self):
        raise NotImplementedError

    def ensure_registered(self, instance):
        if _HAS_ATFORK:
            self.instances.add(instance)

    def ensure_unregistered(self, instance):
        self.instances.discard(instance)