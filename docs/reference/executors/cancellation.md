(cancellation)=

# Cancellation with `EventLoopExecutor`

User [code in an executor](https://async-anywhere.readthedocs.io/en/latest/reference/Glossary.html#term-code-in-an-executor) is cancelled (via
[`asyncio.CancelledError`](https://docs.python.org/3/library/asyncio-exceptions.html#asyncio.CancelledError)) in a
variety of situations, listed below.

> [!NOTE]
> **Repeated Cancellation**
>
> Cancellations can be sent to the same [`asyncio.Task`](https://docs.python.org/3/library/asyncio-task.html#asyncio.Task)
> more than once, both due to the user requesting cancellation repeatedly or the executor harness cancelling code while
> stopping due to a failure.
>
> For example a cancellation is sent to the main task and running jobs *each* time
> `shutdown(cancel_futures=True)` is called. This can lead to code receiving multiple cancellation events.

> [!NOTE]
> **Ignoring Cancellation**
>
> Code that chooses to ignore
> [`asyncio.CancelledError`](https://docs.python.org/3/library/asyncio-exceptions.html#asyncio.CancelledError) may
> misbehave when cancellations occur or the executor is stopped.
>
> Code which swallows or deduplicates cancellations (e.g.
> [`TaskGroup`](https://docs.python.org/3/library/asyncio-task.html#asyncio.TaskGroup)) may behave unexpectedly when
> this occurs. Consult the documentation for any such code you are using to determine how this affects you.

## Job Cancellation

[Job](https://async-anywhere.readthedocs.io/en/latest/reference/Glossary.html#term-job)s are cancelled in these situations:

1. A job's caller cancels it (e.g. via `EventLoopExecutor().submit(...).cancel()` or by triggering cancellation
   during `await EventLoopExecutor().submit(...)`).
2. `EventLoopExecutor.shutdown(cancel_futures=True)` is called.
3. The [main task](https://async-anywhere.readthedocs.io/en/latest/reference/Glossary.html#term-main-task) completes unexpectedly (returns or raises an exception without
   `EventLoopExecutor.shutdown()` having been called or expiration having occurred). This has the same effect as
   calling `EventLoopExecutor.shutdown(cancel_futures=True)`.
4. The executor finishes [shutting down](https://async-anywhere.readthedocs.io/en/latest/reference/executors/stopping.html), if tasks have not completed in response to prior cancellations.
5. Code inside the job cancels it via `executor.current_job().cancel()`, with the same effect as its caller cancelling
   it.
6. Code cancels the job's [`asyncio.Task`](https://docs.python.org/3/library/asyncio-task.html#asyncio.Task) directly
   (e.g. via `asyncio.current_task().cancel()` in the job). The job's future is cancelled once the job finishes.
7. An [`asyncio.CancelledError`](https://docs.python.org/3/library/asyncio-exceptions.html#asyncio.CancelledError)
   escapes the job without anything having cancelled it (e.g. the job awaits something that was cancelled). As with an
   [`asyncio.Task`](https://docs.python.org/3/library/asyncio-task.html#asyncio.Task), the job's future is cancelled.

> [!TIP]
> 5, 6, and 7 provide a useful property in some contexts. If a job was "conceptually cancelled" by something external
> to the event loop (e.g. a Trio guest thread cancelled a Trio task, or a network service said "I have cancelled
> your session and closed your connection"), and you want to report that condition as an asyncio cancellation rather
> than an ordinary result/error, you can `raise asyncio.CancelledError` from the job, or, if you are deep within the
> job's call stack and don't trust the error to make it out, you can call `executor.current_job().cancel()` from
> any code on the executor's thread.

## Main task Cancellation

An executor's [main task](https://async-anywhere.readthedocs.io/en/latest/reference/Glossary.html#term-main-task) is cancelled in these situations:

- The idle timeout expires (which means the executor is running no jobs and has been idle for that amount of time).
- `EventLoopExecutor.shutdown(cancel_futures=True)` is called.
- `EventLoopExecutor.shutdown(cancel_futures=False)` is called, once no in-progress [job](https://async-anywhere.readthedocs.io/en/latest/reference/Glossary.html#term-job)s are running. This
  cancellation will only be sent to the main task once, and will only be sent if that task has not been cancelled
  for any other reason.
- The executor finishes [shutting down](https://async-anywhere.readthedocs.io/en/latest/reference/executors/stopping.html), if tasks have not completed in response to prior cancellations.

## Non-job/non-main-task Cancellation

Non-[job](https://async-anywhere.readthedocs.io/en/latest/reference/Glossary.html#term-job), non-[main task](https://async-anywhere.readthedocs.io/en/latest/reference/Glossary.html#term-main-task) `asyncio.Task`s created inside the executor (e.g. via executor code using
[`asyncio.shield()`](https://docs.python.org/3/library/asyncio-task.html#asyncio.shield) or un-awaited/backgrounded
[`asyncio.Task`](https://docs.python.org/3/library/asyncio-task.html#asyncio.Task)s) are cancelled in these situations:

- User code supervising a Task triggers an ordinary cancellation, as with normal async Python programming.
- The executor finishes [shutting down](https://async-anywhere.readthedocs.io/en/latest/reference/executors/stopping.html).

## Cancellation and Trio

### Cancelling executor jobs while awaiting from Trio

An [`AwaitableFuture`](https://async-anywhere.readthedocs.io/en/latest/reference/futures/awaitable.html#async_anywhere.futures.AwaitableFuture), such as one returned by [`EventLoopExecutor.submit()`](https://async-anywhere.readthedocs.io/en/latest/reference/executors/EventLoopExecutor.html#async_anywhere.executors.EventLoopExecutor.submit), can be awaited from a
[Trio](https://trio.readthedocs.io/en/stable/) task. Which exception the `await` raises depends on what was cancelled:

- **The awaiting Trio code is cancelled**: if a cancel scope enclosing the `await` is cancelled (e.g. its nursery is
  cancelled, or a [`trio.move_on_after()`](https://trio.readthedocs.io/en/stable/reference-core.html#trio.move_on_after)
  deadline passes), the `await` raises
  [`trio.Cancelled`](https://trio.readthedocs.io/en/stable/reference-core.html#trio.Cancelled), as any other Trio
  checkpoint would, and the `AwaitableFuture` is cancelled too. If the future is a [job](https://async-anywhere.readthedocs.io/en/latest/reference/Glossary.html#term-job)'s, this cancels the job
  as usual. If the future refuses cancellation (e.g. it is an [`UncancellableAwaitableFuture`](https://async-anywhere.readthedocs.io/en/latest/reference/futures/uncancellable.html#async_anywhere.futures.UncancellableAwaitableFuture)), the Trio code
  still receives [`trio.Cancelled`](https://trio.readthedocs.io/en/stable/reference-core.html#trio.Cancelled) and stops
  waiting, but the future is left as it was.
- **The AwaitableFuture is cancelled elsewhere**: if the future is cancelled by anything other than the awaiting Trio
  code's own cancel scopes (e.g. by a call to its `cancel()` method, or by the job's caller), the `await` raises
  [`concurrent.futures.CancelledError`](https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.CancelledError),
  an ordinary [`Exception`](https://docs.python.org/3/builtins/exceptions.html#Exception). It does *not* raise
  [`trio.Cancelled`](https://trio.readthedocs.io/en/stable/reference-core.html#trio.Cancelled), which Trio reserves for
  cancellations of the waiting code's own cancel scopes. See [`AwaitableFuture.as_async_future()`](https://async-anywhere.readthedocs.io/en/latest/reference/futures/awaitable.html#async_anywhere.futures.AwaitableFuture.as_async_future) for details.

> [!NOTE]
> Awaiting an `AwaitableFuture` that has already [completed](https://async-anywhere.readthedocs.io/en/latest/reference/Glossary.html#term-completed) is not a Trio checkpoint: if an enclosing cancel
> scope is already cancelled, the `await` still produces the future's result or exception, and
> [`trio.Cancelled`](https://trio.readthedocs.io/en/stable/reference-core.html#trio.Cancelled) is raised at the next
> checkpoint instead.

### Cancelling Trio code running in an executor

Trio code can run inside an executor as a guest on the executor's event loop (see
[Using “guest mode” to run Trio on top of other event loops](https://trio.readthedocs.io/en/stable/reference-lowlevel.html#guest-mode)
and [`trio.lowlevel.start_guest_run()`](https://trio.readthedocs.io/en/stable/reference-lowlevel.html#trio.lowlevel.start_guest_run)).
The executor cancels a job that hands work to Trio like any other job: it cancels the job's
[`asyncio.Task`](https://docs.python.org/3/library/asyncio-task.html#asyncio.Task), and the job's own code decides how
that cancellation reaches the Trio code, and how cancellations in Trio are handled.

When a Trio-using [job](https://async-anywhere.readthedocs.io/en/latest/reference/Glossary.html#term-job)'s `AwaitableFuture` is cancelled (e.g. by its caller, or by
`EventLoopExecutor.shutdown(cancel_futures=True)`):

1. The job's `AwaitableFuture` is cancelled immediately: code waiting on it gets a `CancelledError` at once, without
   waiting for the Trio code to finish.
2. The job's [`asyncio.Task`](https://docs.python.org/3/library/asyncio-task.html#asyncio.Task) inside the executor is
   cancelled, as for any job. While it can be waited on to completion, the end result of the wait/`await` will be a
   `CancelledError`.
3. Trio code running inside the job *will not notice the cancellation by default*. If the job's code is e.g. waiting
   on a Trio guest run, the outer, non-Trio wait must handle `asyncio.CancelledError` appropriately (e.g. by
   calling `.cancel()` on a Trio cancel scope that is active).

When Trio code in a [job](https://async-anywhere.readthedocs.io/en/latest/reference/Glossary.html#term-job) cancels itself, Trio's semantics dictate that the cancellation be handled inside the
job. The job is completed with whatever the Trio code returns or raises, and is unaware of any internal cancellation
events unless told about them. You can report a Trio (or otherwise "external" to the event loop) cancellation by
cancelling a job's future (which can be obtained by [`EventLoopExecutor.current_job()`](https://async-anywhere.readthedocs.io/en/latest/reference/executors/EventLoopExecutor.html#async_anywhere.executors.EventLoopExecutor.current_job)).

> [!NOTE]
> Trio code running as a [guest](https://trio.readthedocs.io/en/stable/reference-lowlevel.html#trio.lowlevel.start_guest_run) is not inside a job, so `current_job()` returns None there: call it from the job's
> asyncio code, and pass the returned future to the Trio code.
