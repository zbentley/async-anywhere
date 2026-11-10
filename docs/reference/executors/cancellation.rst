.. py:currentmodule:: async_anywhere.executors

.. _cancellation:

Cancellation with :py:class:`EventLoopExecutor`
-----------------------------------------------

User :term:`code in an executor` is cancelled (via :py:obj:`asyncio.CancelledError`) in a
variety of situations, listed below.

.. admonition:: Repeated Cancellation

    Cancellations can be sent to the same :py:obj:`asyncio.Task` more than once, both due to the user requesting
    cancellation repeatedly or the executor harness cancelling code while stopping due to a failure.

    For example a cancellation is sent to the main task and running jobs `each` time
    ``shutdown(cancel_futures=True)`` is called. This can lead to code receiving multiple cancellation events.

.. admonition:: Ignoring Cancellation

    Code that chooses to ignore :py:obj:`asyncio.CancelledError` may misbehave when cancellations occur or the executor
    is stopped.

    Code which swallows or deduplicates cancellations (e.g. `TaskGroup`) may behave unexpectedly when this occurs.
    Consult the documentation for any such code you are using to determine how this affects you.

Job Cancellation
^^^^^^^^^^^^^^^^

:term:`Job`\ s are cancelled in these situations:

- ``EventLoopExecutor.shutdown(cancel_futures=True)`` is called.
- The :term:`main task` completes unexpectedly (returns or raises an exception without
  ``EventLoopExecutor.shutdown()`` having been called or expiration having occurred). This has the same effect as
  calling ``EventLoopExecutor.shutdown(cancel_futures=True)``.
- A job's caller cancels it (e.g. via ``EventLoopExecutor().submit(...).cancel()`` or by triggering cancellation
  during ``await EventLoopExecutor().submit(...)``).
- Code inside the job cancels it via ``executor.current_job().cancel()``, with the same effect as its caller cancelling
  it.
- Code cancels the job's :py:obj:`asyncio.Task` directly (e.g. via ``asyncio.current_task().cancel()`` in the job). The
  job's future is cancelled once the job finishes.
- The executor finishes :ref:`shutting down <stopping>`, if tasks have not completed in response to prior cancellations.

Main task Cancellation
^^^^^^^^^^^^^^^^^^^^^^

An executor's :term:`main task` is cancelled in these situations:

- The idle timeout expires (which means the executor is running no jobs and has been idle for that amount of time).
- ``EventLoopExecutor.shutdown(cancel_futures=True)`` is called.
- ``EventLoopExecutor.shutdown(cancel_futures=False)`` is called, once no in-progress :term:`job`\ s are running. This
  cancellation will only be sent to the main task once, and will only be sent if that task has not been cancelled
  for any other reason.
- The executor finishes :ref:`shutting down <stopping>`, if tasks have not completed in response to prior cancellations.

Non-job/non-main-task Cancellation
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Non-:term:`job`, non-:term:`main task` ``asyncio.Task``\ s created inside the executor (e.g. via executor code using :py:func:`asyncio.shield` or
un-awaited/backgrounded :py:obj:`asyncio.Task`\ s) are cancelled in these situations:

- User code supervising a Task triggers an ordinary cancellation, as with normal async Python programming.
- The executor finishes :ref:`shutting down <stopping>`.

.. _trio_cancellation:

Cancellation and Trio
^^^^^^^^^^^^^^^^^^^^^

Awaiting from Trio
~~~~~~~~~~~~~~~~~~

An :py:class:`.AwaitableFuture`, such as one returned by :py:meth:`EventLoopExecutor.submit`, can be awaited from a
:doc:`Trio <trio:index>` task. Which exception the ``await`` raises depends on what was cancelled:

- **The awaiting Trio code is cancelled**: if a cancel scope enclosing the ``await`` is cancelled (e.g. its nursery is
  cancelled, or a :py:func:`trio.move_on_after` deadline passes), the ``await`` raises :py:obj:`trio.Cancelled`, as any
  other Trio checkpoint would, and the ``AwaitableFuture`` is cancelled too. If the future is a :term:`job`'s, this
  cancels the job as usual. If the future refuses cancellation (e.g. it was created with
  ``ignore_cancel=True``), the Trio code still receives :py:obj:`trio.Cancelled` and stops waiting, but the future is
  left as it was.
- **The AwaitableFuture is cancelled elsewhere**: if the future is cancelled by anything other than the awaiting Trio
  code's own cancel scopes (e.g. by a call to its ``cancel()`` method, or by the job's caller), the ``await`` raises
  :py:obj:`concurrent.futures.CancelledError`, an ordinary :py:obj:`Exception`. It does *not* raise
  :py:obj:`trio.Cancelled`, which Trio reserves for cancellations of the waiting code's own cancel scopes. See
  :py:meth:`.AwaitableFuture.as_async_future` for details.

.. note::

    Awaiting an ``AwaitableFuture`` that has already :term:`completed` is not a Trio checkpoint: if an enclosing cancel
    scope is already cancelled, the ``await`` still produces the future's result or exception, and
    :py:obj:`trio.Cancelled` is raised at the next checkpoint instead.

Running Trio code in an executor
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Trio code can run inside an executor as a guest on the executor's event loop (see :ref:`trio:guest-mode` and
:py:func:`trio.lowlevel.start_guest_run`). The executor cancels a job that hands work to Trio like any other job: it
cancels the job's :py:obj:`asyncio.Task`, and the job's own code decides how that cancellation reaches the Trio code, and
how cancellations in Trio are handled.

When a Trio-using :term:`job`'s ``AwaitableFuture`` is cancelled (e.g. by its caller, or by
``EventLoopExecutor.shutdown(cancel_futures=True)``):

1. The future is cancelled immediately: code waiting on it gets a ``CancelledError`` at once, without waiting for the
   Trio code to finish.
2. The job's :py:obj:`asyncio.Task` inside the executor is cancelled, as for any job. TODO...
.. 3. Trio processes the cancellation normally: the Trio code receives :py:obj:`trio.Cancelled` at its next checkpoint,
..   can perform cleanup (e.g. in a shielded cancel scope), and the cancel scope catches the :py:obj:`trio.Cancelled` on
..   the way out.
.. 4. The job ends cancelled. The guest run is unaffected, and keeps running other Trio code. If the Trio code ignores the
..   cancellation and returns a value anyway, the value is discarded, as with any job that ignores cancellation.

When Trio code in a :term:`job` cancels itself, Trio's semantics dictate that the cancellation be handled inside the
job.

- **A cancel scope opened by the Trio code**: Trio handles this as usual, and the executor is not involved. The job's
  outcome is whatever the Trio code returns or raises afterwards; for example, if a :py:func:`trio.fail_after` deadline
  passes and its :py:obj:`trio.TooSlowError` escapes the Trio code, the job's future fails with that error.
- **The cancel scope the recipe wraps around the Trio code**: if this scope is cancelled from the Trio side, rather than
  by cancelling the job's future, the cancellation is reported back to the job as an :py:obj:`asyncio.CancelledError`.
  Since the job's future was not cancelled, the executor :term:`completes <complete>` it as failed with that
  :py:obj:`asyncio.CancelledError`, rather than as cancelled: the future's ``cancelled()`` method returns False, and
  ``result()``, or awaiting the future from asyncio or Trio, raises :py:obj:`asyncio.CancelledError`.

If your Trio code was conceptually cancelled, report this by cancelling the job's future, which
:py:meth:`EventLoopExecutor.current_job` returns, as the job's caller would. Trio code running as a guest is not inside
the job, so ``current_job()`` returns None there: call it from the job's asyncio code, and pass the returned future to
the Trio code.
