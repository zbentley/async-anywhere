.. py:currentmodule:: async_anywhere.executors

.. _stopping:

Stopping :py:class:`EventLoopExecutor`\ s
-----------------------------------------

An :py:class:`EventLoopExecutor` can be **stopped**, meaning that it's :py:class:`.WaitableStateTracker` API is in the
``Stopping`` or ``Shutdown`` :py:class:`.State`.

Once an executor is stopped, attempts to submit new jobs will fail with an error. Waiting for jobs submitted just before
it was stopped (that have not yet started running) will also fail with an error.

When is an executor stopped?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Executors are stopped in these situations:

- :py:meth:`EventLoopExecutor.shutdown` is called by the user.
- The executor's :term:`main task` returns or raises an exception.
- :py:func:`os.fork` is called: in this case, the executor will register as stopped in the child process, and will continue running as usual in the parent.
- When the captive event loop used by the executor crashes (e.g. if user code inside the executor calls ``asyncio.get_event_loop().close()``, or if any unhandled loop-breaking error occurs).
- Any code running on the executor's event loop (job or not) raises  `SystemExit` or `KeyboardInterrupt`. Raising other ``BaseException``\ s besides ``CancelledError`` is unsupported and may also trigger executor crash.

What happens when an executor is stopped?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

When an executor is stopped, the following behavior happens:

#. Immediately upon being stopped, new jobs can no longer be submitted to the executor.
#. The executor is put into the ``Stopping`` State.
#. The executor's captive event loop then waits for all :term:`job`\ s to complete. Jobs and the :term:`main task` can receive :term:`interrupt`\ s during this phase: see :ref:`interruption`.
     - This step is skipped when an executor is stopped due to ``os.fork()``, in the child process.
#. Once those jobs have completed, if the :term:`main task` is still running, and has not been cancelled, ``asyncio.Task.cancel()`` is called on it.
    - This step is skipped when an executor is stopped due to ``os.fork()``, in the child process.
#. Once all jobs have completed (or once waiting for them fails due to e.g. a broken event loop), ``asyncio.Task.cancel()`` is called on `all` remaining tasks on the executor's event loop (including any still-running jobs, the main task, and tasks created/backgrounded by them).
    - This step is skipped when an executor is stopped due to ``os.fork()``, in the child process.
#. Unfinished futures corresponding to all jobs in-flight on the executor when it was stopped are `failed` with an exception describing the conditions of the stop. In other words: if a job ignored cancellation, never started, or was stuck, attempts to wait on that job's completion will not wait for it, and will fail fast with an error after this point.
#. The executor's captive event loop is shut down.
    - In the process that created the executor, this is done by shutting down all active tasks, async generators, and signalling the loop's ``ThreadPoolExecutor`` to shut down before calling ``.close()`` on the event loop.
    - When an executor is stopped due to ``os.fork()``, in the child process, this is done by calling ``.close()`` on the executor's event loop.

    .. note::
        Any ``asyncio.Task``\ s in the executor that have not completed by this point are left in an indeterminate
        partially-executed state. Most event loop implementations remove all resources associated with such unfinished
        tasks when ``.close()``\ d.

#. The executor is transitioned to the ``Shutdown`` State.
#. The executor's captive daemon thread is joined.
    - This step is skipped when an executor is stopped due to ``os.fork()``, in the child process.
