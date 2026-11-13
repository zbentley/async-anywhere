Glossary
--------

.. glossary::

    executor
        An instance of EventLoopExecutor.

    main task
        The async function passed to the ``main=`` keyword argument to ``EventLoopExecutor``'s constructor, which is
        wrapped in a :py:obj:`asyncio.Task` and executed as the first task on the executor's event loop. An executor
        will shut itself down as soon as its main task finishes.

    job
    jobs
        User-supplied Python code that executes in the captive thread of an EventLoopExecutor, besides the :term:`main task`. Either of:
            #. Code passed to the executor via ``executor.submit()``.
            #. Code decorated with ``@executor.bridge``.

    code in an executor
    code in a worker
        `Any` Python code (besides implementation details of ``async_anywhere``) which executes in the captive thread of an ``EventLoopExecutor``, including but not limited to:
            #. Any code in a :term:`job` or the :term:`main task` running on the executor.
            #. Code in callbacks added by the user to futures returned by the executor via :py:meth:`.AwaitableFuture.add_done_callback()`. Note that code in done callbacks `may or may not` execute in the executor's main thread; users should not rely on which thread a done callback executes on.
            #. Destructors of objects freed by user code on the executor's captive event loop/thread.
            #. Any construction or shutdown logic of the event loop used on the executor's thread (see the ``loop_factory=`` keyword argument to ``EventLoopExecutor`` for more details).
            #. Code in any :py:obj:`asyncio.Task`\ s created by a :term:`job` or the :term:`main task`. It is `strongly recommended` that jobs and the main task wait for all ``asyncio.Task``\ s they create.

        .. note::

            Code in an executor that itself runs code on a different thread, even a thread managed by the executor's
            event loop via :py:func:`asyncio.to_thread`, does `not` count as running inside the executor, though it may
            inherit context from the executor's captive thread, and the presence of such background-threaded work may
            block the :ref:`executor's shutdown process <stopping>`.

    cancel
        (as pertains to :term:`code in an executor`)

        Call ``.cancel()`` on an asyncio.Task running in an executor. See :ref:`cancellation` for details.

    complete
    completed
    completing
        (as pertains to :py:class:`.AwaitableFuture`, :py:obj:`asyncio.Future`, :py:obj:`asyncio.Task`, or :py:obj:`concurrent.futures.Future`)

        Mark a future or task as done by doing exactly one of:
        - Cancelling it.
        - Setting its result.
        - Setting its exception.

        A task/future is completed if ``future.done() == True``.