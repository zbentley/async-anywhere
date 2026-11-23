(stopping)=

# Stopping `EventLoopExecutor`s

An [`EventLoopExecutor`](https://async-anywhere.readthedocs.io/en/latest/reference/executors/EventLoopExecutor.html#async_anywhere.executors.EventLoopExecutor) can be **stopped**, meaning that its [`WaitableStateTracker`](https://async-anywhere.readthedocs.io/en/latest/reference/state_tracker/WaitableStateTracker.html#async_anywhere.state_tracker.waitable.WaitableStateTracker) API is in the
`Stopping` or `Shutdown` [`State`](https://async-anywhere.readthedocs.io/en/latest/reference/state_tracker/State.html#async_anywhere.state_tracker.State).

Once an executor is stopped, attempts to submit new jobs will fail with an error.

## When is an executor stopped?

Executors are stopped in these situations:

- [`EventLoopExecutor.shutdown()`](https://async-anywhere.readthedocs.io/en/latest/reference/executors/EventLoopExecutor.html#async_anywhere.executors.EventLoopExecutor.shutdown) is called by the user.
- The executor's [main task](https://async-anywhere.readthedocs.io/en/latest/reference/Glossary.html#term-main-task) returns or raises an exception.
- [`os.fork()`](https://docs.python.org/3/library/os.html#os.fork) is called: in this case, the executor will register as stopped in the child process, and will continue running as usual in the parent.
- When the captive event loop used by the executor crashes (e.g. if user code inside the executor calls `asyncio.get_event_loop().close()`, or if any unhandled loop-breaking error occurs).
- Any code running on the executor's event loop (job or not) raises [`SystemExit`](https://docs.python.org/3/builtins/exceptions.html#SystemExit) or [`KeyboardInterrupt`](https://docs.python.org/3/builtins/exceptions.html#KeyboardInterrupt). Raising other `BaseException`s besides `CancelledError` is unsupported and may also trigger executor crash.

## What happens when an executor is stopped?

When an executor is stopped, the following behavior happens:

1. Immediately upon being stopped, new jobs can no longer be submitted to the executor.
2. The executor is put into the `Stopping` State.
3. The executor's captive event loop then waits for all [job](https://async-anywhere.readthedocs.io/en/latest/reference/Glossary.html#term-job)s to complete. Jobs and the [main task](https://async-anywhere.readthedocs.io/en/latest/reference/Glossary.html#term-main-task) can receive cancellations during this phase: see [Cancellation with `EventLoopExecutor`](https://async-anywhere.readthedocs.io/en/latest/reference/executors/cancellation.html).
   - This step is skipped when an executor is stopped due to `os.fork()`, in the child process.
4. Once those jobs have completed, if the [main task](https://async-anywhere.readthedocs.io/en/latest/reference/Glossary.html#term-main-task) is still running, and has not been cancelled, `asyncio.Task.cancel()` is called on it.
   - This step is skipped when an executor is stopped due to `os.fork()`, in the child process.
5. Once all jobs have completed (or once waiting for them fails due to e.g. a broken event loop), `asyncio.Task.cancel()` is called on *all* remaining tasks on the executor's event loop (including any still-running jobs, the main task, and tasks created/backgrounded by them).
   - This step is skipped when an executor is stopped due to `os.fork()`, in the child process.
6. Unfinished futures corresponding to all jobs in-flight on the executor when it was stopped are *failed* with an exception describing the conditions of the stop. In other words: if a job ignored cancellation, never started, or was stuck, attempts to wait on that job's completion will not wait for it, and will fail fast with an error after this point.
7. The executor's captive event loop is shut down.
   - In the process that created the executor, this is done by shutting down all active tasks, async generators, and signalling the loop's `ThreadPoolExecutor` to shut down before calling `.close()` on the event loop.
   - When an executor is stopped due to `os.fork()`, in the child process, this is done by calling `.close()` on the executor's event loop.

   > [!NOTE]
   > Any `asyncio.Task`s in the executor that have not completed by this point are left in an indeterminate
   > partially-executed state. Most event loop implementations remove all resources associated with such unfinished
   > tasks when `.close()`d.

8. The executor is transitioned to the `Shutdown` State.
9. The executor's captive daemon thread is joined.
   - This step is skipped when an executor is stopped due to `os.fork()`, in the child process.
