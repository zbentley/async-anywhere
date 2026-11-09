Glossary
--------

.. glossary::

    executor
    worker
        An instance of EventLoopExecutor.

    main task
        The async function passed to the ``main=`` keyword argument to ``EventLoopExecutor``'s constructor, which is
        wrapped in a :py:class:`asyncio.Task` and executed as the first job on the executor's event loop. An executor
        will shut itself down as soon as its main task finishes.

    job
        User-supplied Python code that executes in the captive thread of an EventLoopExecutor. Any of:
            #. Code in the main task.
            #. Code passed to the executor via ``executor.submit()``.
            #. Code decorated with ``@executor.bridge``.

    code in an executor
    code in a worker
        `Any` Python code (besides implementation details of ``async_anywhere``) which executes in the captive thread of an ``EventLoopExecutor``, including but not limited to:
            #. Any code in a :term:`job` running on the executor.
            #. Code in callbacks added by the user to futures returned by the executor via :py:func:`AwaitableFuture.add_done_callback()`. Note that code in done callbacks `may or may not` execute in the executor's main thread; users should not rely on which thread a done callback executes on.
            #. Destructors of objects freed by user code on the executor's captive event loop/thread.
            #. Any construction or shutdown logic of the event loop used on the executor's thread (see the ``loop_factory=`` keyword argument to ``EventLoopExecutor`` for more details).
            #. Code in any :py:class:`asyncio.Task`\ s created by a :term:`job`. It is `strongly recommended` that jobs wait for all ``asyncio.Task``\ s they create.

        .. note::

            Code in an executor that itself runs code on a different thread, even a thread managed by the executor's
            event loop via :py:func:`asyncio.to_thread`, does `not` count as running inside the executor, though it may
            inherit context from the executor's captive thread, and the presence of such background-threaded work may
            block the :ref:`executor's shutdown process <stopping>`.

    interrupt
    cancel
        (as pertains to :term:`code in an executor`)

        Call ``.cancel()`` on an asyncio.Task running in an executor. See :ref:`interruption` for details.

    complete
    completed
    completing
        (as pertains to :py:class:`AwaitableFuture`, :py:class:`asyncio.Future`, :py:class:`asyncio.task`, or :py:class:`concurrent.futures.Future`)

        Mark a future or task as done by doing exactly one of:
        - Cancelling it.
        - Setting its result.
        - Setting its exception.

        A task/future is completed if ``future.done() == True``.