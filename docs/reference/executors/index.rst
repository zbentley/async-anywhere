``async_anywhere.executors``
----------------------------

``async_anywhere.executors`` contains classes which provide :py:obj:`concurrent.futures.Executor` implementations
which accept asynchronous Python jobs, and run those jobs on an isolated, captive thread and event loop.

The executor documentation uses some terms (e.g. :term:`job` and :term:`main task`) with meanings specific to
``async_anywhere``; see the :doc:`glossary <../Glossary>` for their definitions.

.. list-table::
    :header-rows: 1
    :class: valign-top

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
