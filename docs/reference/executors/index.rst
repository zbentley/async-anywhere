``async_anywhere.executors``
----------------------------

``async_anywhere.executors`` contains classes which provide :py:obj:`concurrent.futures.Executor` implementations
which accept asynchronous Python jobs, and run those jobs on an isolated, captive thread and event loop.

.. toctree::
    :maxdepth: 4

    EventLoopExecutor
    SingletonEventLoopExecutor
    cancellation
    stopping
    fork_safety

