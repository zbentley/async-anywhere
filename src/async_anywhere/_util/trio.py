from importlib.util import find_spec
from typing import Callable, TYPE_CHECKING

import sniffio

if TYPE_CHECKING:
    import trio as _trio


class TrioRuntime:
    current_token: Callable[[], _trio.lowlevel.TrioToken]  # trio's own, bound by _in_trio_task() on first use

    if find_spec("trio") is None:  # Then nothing can be awaiting in trio
        in_task = bool
    else:
        @staticmethod
        def in_task() -> bool:
            # trio.run() and guest ticks set it: cheaper than in_trio_task(), which raises internally outside trio
            return sniffio.thread_local.name == "trio" and TrioRuntime._in_trio_task()

    @staticmethod
    def _in_trio_task() -> bool:
        # Only reached in a trio run, which has imported trio: so unused or absent, trio costs nothing
        import trio as _trio
        TrioRuntime._in_trio_task = _trio.lowlevel.in_trio_task  # In place of this one, from now on
        TrioRuntime.current_token = staticmethod(_trio.lowlevel.current_trio_token)
        return _trio.lowlevel.in_trio_task()
