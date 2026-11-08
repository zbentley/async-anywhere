``async_anywhere.executors``
----------------------------

``async_anywhere.executors`` contains classes which provide :py:class:`concurrent.futures.Executor` implementations
which accept asynchronous Python jobs, and run those jobs on an isolated, captive thread and event loop.

.. toctree::
    :maxdepth: 2

    glossary
    EventLoopExecutor
    SingletonEventLoopExecutor
    interruption
    stopping
    fork_safety

