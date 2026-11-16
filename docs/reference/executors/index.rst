``async_anywhere.executors``
----------------------------

``async_anywhere.executors`` contains classes which provide :py:obj:`concurrent.futures.Executor` implementations
which accept asynchronous Python jobs, and run those jobs on an isolated, captive thread and event loop.

.. list-table::
    :header-rows: 1

    * - API Reference
      - Additional Resources
    * - .. toctree::
            :maxdepth: 4

            EventLoopExecutor
            SingletonEventLoopExecutor

      - .. toctree::
            :maxdepth: 4

            Cancellation <cancellation>
            Stopping <stopping>
            Fork Safety <fork_safety>
