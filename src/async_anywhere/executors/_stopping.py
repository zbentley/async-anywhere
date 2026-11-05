# `EventLoopExecutor` Stopping

An `EventLoopExecutor` can be **stopped**, meaning that it's `WaitableStateTracker` is in the `Stopping` or `Shutdown` 
States.

Once an executor is stopped, attempts to submit new jobs will fail with an error. Waiting for jobs submitted just before 
it was stopped (that have not yet started running) will also fail with an error.

## When is an executor stopped?

Executors are stopped in these situations:

1. `EventLoopExecutor.shutdown()` is called by the user.
1. The parent task provided via the `main=` keyword argument to the executor constructor returns or raises an exception.
1. After a fork, executor inherited by a child process register as being stopped in that process.
1. When the captive event loop used by the executor crashes (e.g. if user code inside the executor calls 
   `asyncio.get_event_loop().close()`, or if an unhandled event loop runtime error occurs).

## What happens when an executor is stopped?

When an executor is stopped in the process that created it, the following behavior happens:

1. Immediately upon being stopped, new jobs can no longer be submitted to the executor.
2. If the parent task provided via the `main=` keyword argument to the constructor is still running, and has not been
   cancelled, `asyncio.Task.cancel()` is called on it.
3. The worker's captive event loop then waits for all requested [jobs](TODO link to glossary) to complete. Jobs can receive [interruptions](TODO) during this phase.
4. The worker's captive event loop is shut down. This is done by shutting down all active tasks, async generators, and signalling the loop's executor to shut down before calling `.close()` on the event loop.
5. TODO wait for executor and bg tasks.
6. The executor's captive daemon thread is joined and the executor is transitioned to the `Shutdown` State.

A worker is shut down (Shutdown) once it has terminated its event loop. The internal order of shutdown is roughly:

1. Ensure the worker is stopped, or stop it.
2. Ensure the parent task and all bridge jobs in flight have completed (see "Interruption" for cancellation semantics).
3. Abort all other active work on the captive event loop.
4. Stop the captive event loop and remove references to all active work.

After a worker has shut down, attempts to retrieve the result of any incomplete bridge calls initiated before the shutdown will fail with an error.

Shutdown occurs in the following situations:


- If the parent task or any bridge job raises  `SystemExit` or `KeyboardInterrupt`, a worker will be shut down.
- After the parent task finishes (via ordinary completion, expiration, or interruption),the worker is shut down.
- After a fork, workers inherited by a child process register as being shut down in that process.

## Interruption

User code is interrupted (sent a cancellation) in these situations:

The parent task:
    - The idle timeout expires.
    - `shutdown(cancel_futures=True)` is called.
    - `shutdown(cancel_futures=False)` is called, once no in-progress bridge jobs are running. This interruption will only be sent to the parent task once, and will only be sent if it has not been interrupted for any other reason.

Bridge jobs:
    - `shutdown(cancel_futures=True)` is called.
    - The parent task completes unexpectedly (returns or raises an exception without `shutdown()` having been called or expiration having
      occurred). This has the same effect as calling `shutdown(cancel_futures=True)`.
    - A bridge job's caller cancels it (e.g. via `worker.bridge_call().cancel()` or by triggering cancellation during
      `await worker.bridge_call(...)`).

Other tasks created inside the worker (e.g. via worker code using `asyncio.shield` or un-awaited `asyncio.Task`s):
    - User code triggers an ordinary cancellation.
    - The worker finishes shutting down.

**Note:** a cancellation is sent to the parent task and running bridge jobs *each* time `shutdown(cancel_futures=True)` is called. This can
lead to them receiving multiple cancellation events from the worker. Code which swallows or deduplicates cancellations (e.g. `TaskGroup`) may behave unexpectedly when this occurs.

**Note:** code that chooses to ignore cancellations may miss worker interrupts or expirations.

The first stop moves the worker to Stopping, with why as the data of that transition, and of those after it: a
WorkerStopped (a CleanStop for expiry, a shutdown() that interrupts nothing, or the parent task returning), which it logs.
Later stops don't change that, and every call from then on is refused with it. Whatever the worker raises it for, or chains it to, gets a copy, keeping its cause,
so callers' tracebacks don't pile up on one shared exception.

Once the parent task and every call have finished, the loop cancels any tasks left on it (work that the parent task or
calls spawned and didn't wait for), gives them 0.05s, shuts down async generators and the default executor (abandoning, with a RuntimeWarning,
threads that haven't finished by then), and closes. Only then does the worker move to Shutdown, waking shutdown()'s waiters.
"""