from __future__ import annotations

import asyncio
import concurrent.futures
import copyreg
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from async_anywhere.state_tracker import StateTracker
    from async_anywhere.state_tracker._state import State


# TODO test that superclass ctors are empty
class StateTrackerError(asyncio.InvalidStateError, concurrent.futures.InvalidStateError):
    def __init__(
        self,
        source: StateTracker,
        msg: str,
        *states: State,
    ):
        desc = self.state_names(*states)
        errprefix = f"{source.__class__.__qualname__}[state={source.state.name}]"
        super().__init__(f"{errprefix}: {msg.strip(" :")}: {desc}")

    def __reduce__(self):
        # For copy and pickle: recreate from args (the formatted message) without calling __init__, which can't take it
        return copyreg.__newobj__, (type(self), *self.args), vars(self)

    @classmethod
    def state_names(cls, *states: State) -> str:
        assert len(states) > 0
        if len(states) == 1:
            return states[0].name
        return f'one of {", ".join(sorted(s.name for s in states))}'
