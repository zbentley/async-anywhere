.. py:currentmodule:: async_anywhere.state_tracker

.. _state_tracker_fork_safety:

``StateTracker`` Fork Safety
----------------------------

``StateTracker`` itself is safe to use in the presence of :py:func:`os.fork` in that it *will not crash or
deadlock* in either the parent or child process. However, there are two important caveats when using this class
with forking code.

1. If the ``lock`` constructor parameter cannot be safely reset/re-constructed in a child process, behavior is
   undefined.
2. If forking happens from another thread while a ``StateTracker`` is calling :py:meth:`StateTracker._transition`, some,
   all, or none of the following may be true when the tracker is used in a forked child process:

   - It might be as if the call to ``_transition`` never occurred. ``_transition``\ s side effects are only
     guaranteed if it has *returned* by the time ``os.fork`` is called.
   - ``StateTracker.state`` might point to the new State, but ``_after_state_change`` may be skipped in the forked child.
   - *Some* code in ``_after_state_change`` may have run, but that method may have never completed/returned,
     leaving any data it mutated in an undefined/indeterminate state.

Forking and continuing to use a StateTracker in the parent process requires no special consideration.
