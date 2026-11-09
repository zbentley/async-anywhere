.. py:currentmodule:: async_anywhere.executors

.. _interruption:

Interruption/Cancellation with :py:class:`EventLoopExecutor`
------------------------------------------------------------

User :term:`code in an executor` is :term:`interrupt`\ ed (cancelled via :py:obj:`asyncio.CancelledError`) in a
variety of situations, listed below.

.. note::
    Interruptions can be sent to the same :py:obj:`asyncio.Task` more than once, both due to the user requesting
    interruption repeatedly or the executor harness cancelling code while stopping due to a failure.

    For example a cancellation is sent to the main task and running jobs `each` time
    ``shutdown(cancel_futures=True)`` is called. This can lead to code receiving multiple cancellation events.

.. note::
    Code that chooses to ignore :py:obj:`asyncio.CancelledError` may misbehave when interrupts occur or the executor
    is stopped.

    Code which swallows or deduplicates cancellations (e.g. `TaskGroup`) may behave unexpectedly when this occurs.
    Consult the documentation for any such code you are using to determine how this affects you.

Job Cancellation
^^^^^^^^^^^^^^^^

:term:`Job`\ s are interrupted in these situations:

- ``EventLoopExecutor.shutdown(cancel_futures=True)`` is called.
- The :term:`main task` completes unexpectedly (returns or raises an exception without
  ``EventLoopExecutor.shutdown()`` having been called or expiration having occurred). This has the same effect as
  calling ``EventLoopExecutor.shutdown(cancel_futures=True)``.
- A job's caller cancels it (e.g. via ``EventLoopExecutor().submit(...).cancel()`` or by triggering cancellation
  during ``await EventLoopExecutor().submit(...)``).
- The executor finishes :ref:`shutting down <stopping>`, if tasks have not completed in response to prior interruptions.

Main task Cancellation
^^^^^^^^^^^^^^^^^^^^^^

An executor's :term:`main task` is interrupted in these situations:

- The idle timeout expires (which means the executor is running no jobs and has been idle for that amount of time).
- ``EventLoopExecutor.shutdown(cancel_futures=True)`` is called.
- ``EventLoopExecutor.shutdown(cancel_futures=False)`` is called, once no in-progress :term:`job`\ s are running. This
  interruption will only be sent to the main task once, and will only be sent if that task has not been interrupted
  for any other reason.
- The executor finishes :ref:`shutting down <stopping>`, if tasks have not completed in response to prior interruptions.

Non-job/non-main-task Cancellation
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Non-:term:`job`, non-:term:`main task` ``asyncio.Task``\ s created inside the executor (e.g. via executor code using :py:func:`asyncio.shield` or
un-awaited/backgrounded :py:obj:`asyncio.Task`\ s) are interrupted in these situations:

- User code supervising a Task triggers an ordinary cancellation, as with normal async Python programming.
- The executor finishes :ref:`shutting down <stopping>`.
