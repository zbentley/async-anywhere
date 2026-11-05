Glossary
--------

.. glossary::

    executor
        An instance of EventLoopExecutor.

    worker
        See "executor"

    job
        User-supplied Python code that executes in the captive thread of an EventLoopExecutor. Any of:
            #. Code in the user-supplied ``main=`` constructor keyword argument.
            #. Code passed to the worker via ``executor.submit()``.
            #. Code decorated with ``@executor.bridge``.

    "code in an executor"
        `Any` Python code that executes in the captive thread of an ``EventLoopExecutor``, including but not limited to:
            #. Any code in a job (see "job").
            #. Code in callbacks added by the user to futures returned by the executor via :py:func:`AwaitableFuture.add_done_callback()`. Note that code in done callbacks `may or may not` execute in the worker's main thread; users should not rely on which thread a done callback executs on.
            #. Destructors of objects freed by user code on the executor's captive event loop/thread.
            #. Any construction or shutdown logic of the event loop used on the executor's thread (see the ``loop_factory=`` keyword argument to ``EventLoopExecutor`` for more details).
            #. Code in any ``asyncio.Task``s created by a job (see "job"). It is `strongly recommended` that jobs wait for all ``asyncio.Task``\ s they create.

        Code in an executor that itself runs code on a different thread, even a thread managed by the executor's event loop via :py:func:`asyncio.to_thread`, does `not` count as running inside the executor, though it may inherit context from the executor's captive thread, and the presence of such background-threaded work may block the executor's shutdown process (see "Stopping" for more information).

    interrupt [an EventLoopExecutor job]
        Call `.cancel()` on the asyncio.Task running a given job in an executor. Calling `AwaitableFuture.cancel()` on the futures returned from `EventLoopExecutor.submit()` cancels their jobs. Certain executor stop conditions also cancel running jobs. Note that an asyncio.Task can be cancelled (and thus raise CancelledError from certain await points) more than once.

    cancel [an EventLoopExecutor job]
        See "interrupt"