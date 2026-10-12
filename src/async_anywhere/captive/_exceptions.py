import concurrent.futures

class WorkerStopped(Exception):
    def __init__(self, summary: str, cause: BaseException | None = None):
        super().__init__(summary)
        self.__cause__ = cause


class CleanStop(WorkerStopped): ...


class WorkerInterrupted(concurrent.futures.CancelledError):
    """What a call's waiters get when an interrupt cancels it (see CaptiveWorker._job()): a subclass, as wrap_future()
    remakes concurrent.futures.CancelledError itself as asyncio's for those awaiting it, losing its __cause__."""
