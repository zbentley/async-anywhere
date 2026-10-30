# Status

**Unfinished:** This module is feature complete but not yet finalized. I plan to finish finalizing the API, docs, tests, *et cetera* in late 2026 and publish it to PyPi, at which time this section will be removed.

# async-anywhere

`async-anywhere` is a set of tools which address common Python concurrency needs. It contains tools for bridging between any number of blocking threads and `async` event loops, tools for modelling state machines and state transitions, `fork(2)`-safety semantics for common concurrency primitives, and more.

If you've run up against the limits of what `asgiref` supports, are struggling with code that needs to coordinate between multiple threads and [event loops], dealing with deadlocks caused by interacting with `Future` objects after a `fork(2)`, or you need to maintain stateful/continuously-running async components of a larger program, this module may help!

It includes:

- `AwaitableFuture`, a `concurrent.futures.Future` which can be blocked on or `await`ed on from any number of event loops or threads. It supports configurable cancellation behavior, `fork(2)` safety, lock-free operation on many paths that the stdlib serializes, and more.
- `EventLoopExecutor`, a `concurrent.futures.Executor` which runs **`async`** code on a thread, and whose results can be waited on from anywhere. It supports idle expiration, calling into itself from within its captive thread, `fork(2)` safety, task interruption, controlled shutdown, "parent" functions which own the event loop, and more.
- `WaitableStateTracker`, a "state machine lite" which allows a class to transition between one of several declared states, and allows other code to wait for the tracker to enter one or more states in an `async`- and thread-agnostic way.
- Several other tools and utilities related to Python concurrency management.

See "Examples" below, or [the API documentation]() for more information.

`async-anywhere` supports:

- Code written for `asyncio`.
- Code written for `trio`.
- Code written for `anyio`.
- Code using `AbstractEventLoop`, including `uvloop`.
- Correct, minimally locking operations with or without the Global Interpreter Lock (GIL).

## Installation and Requirements

[//]: # (You can install _async-anywhere_ from [PyPI]&#40;https://pypi.org/project/async-anywhere/&#41;:)

[//]: # ()
[//]: # (```console)

[//]: # ($ pip install async-anywhere)

[//]: # (```)

[//]: # ()
[//]: # (Or with [`uv`]&#40;https://docs.astral.sh/uv/&#41;:)

[//]: # ()
[//]: # (```console)

[//]: # ($ uv add async-anywhere)

[//]: # (```)

`async-anywhere` is a pure-Python module with no compiled components, and aims to have as few pure-python transitive dependencies as possible.

## Examples

### `AwaitableFuture`

```python
from async_anywhere.futures import AwaitableFuture
from threading import Thread
import asyncio

fut = AwaitableFuture(
    # Don't allow calls to .result() if this loop is running; that'll deadlock.
    prevent_blocking_wait_in_loops=[asyncio.get_event_loop()],
)
fut.add_done_callback(lambda f: print(f"Future {f} is done!"), run_in_forked_child=True)

async def foo():
    print(await fut)

Thread(target=asyncio.run, args=(foo(),)).start() # Wait from one event loop
Thread(target=asyncio.run, args=(foo(),)).start() # ... two event loops
Thread(target=fut.set_result, args=("All done!",)).start()
asyncio.run(foo()) # ... three event loops
print(fut.result(timeout=1)) # ... or synchronously.
```

### `EventLoopExecutor`

```python
from async_anywhere.executors import EventLoopExecutor
import asyncio

executor = EventLoopExecutor(idle_timeout=10)  # Dies if not given work to do for 10 seconds

fut = executor.submit(asyncio.sleep, 1, result="Hello from a captive loop after 1 second!")


@executor.bridge
async def runs_in_captive_loop_thread():
    print("Hello from a captive event loop on a dedicated thread!")
    print(await executor.submit(asyncio.sleep, 1, result="Called from inside the executor!"))
    print(await fut)  # Await futures from anywhere


async def wait_for_executor():
    await runs_in_captive_loop_thread()  # Wait for results from async code
    print(await fut)  # Wait from futures from any loop/thread


asyncio.run(wait_for_executor())
print(fut.result(timeout=2))  # ...or from blocking code.


# Wait for executors to shut down from async code
async def finish():
    await executor.shutdown(wait=True, cancel_futures=True)


# ...or synchronous code
executor.shutdown(wait=True, cancel_futures=True).result()

```

`EventLoopExecutor` is ideal for integrating third-party `async` code that wants to own the event loop/entry point (e.g. [`discordpy`](https://discordpy.readthedocs.io/en/stable/quickstart.html)) into a larger program that may not be `async`, may block the event loop, or may use multiple event loop runtimes. Useful when you need more than `asgiref.async_to_sync` can provide.

## When should you use this module?

Consider using this module if:
- You frequently struggle with `RuntimeError: This event loop is already running` errors, or `bound to a different event loop` errors.
- You've ever hit a problem where you think "I'll run an event loop in a thread and send calls to it", and then struggled with implementing `await`, or blocking waits, on the results of that thread.
- You've fought with errors related to the shutdown of event loops, or the outside-of-`async`-code cancellation of jobs running on an event loop elsewhere in your program.
- You've ever wanted to wait on the same result to arrive from multiple threads and `async` routines simultaneously, and are tired of chaining futures/callbacks by hand.
- You write code in a framework that combines multiple threads and `async` code, and need to share or coordinate state between multiple threads and event loops that you don't directly control. A common example of this is when you need a single shared connection pool with an `async` API for use in a threaded/partially-`async` Python context with multiple loops and threads.
- You've experienced deadlocks interacting with `ThreadPoolExecutors` or `asyncio.Future`s across `fork(2)` boundaries.
- You have code that uses `asgiref.async_to_sync`, but have encountered issues with the overhead

That said, this module is an advanced tool! You may **not** need this module if:
- `asgiref.async_to_sync` and `asgiref.sync_to_async` serve your needs. `asgiref` is an excellent, battle-tested module that serves a specific niche: if you fit in that niche, use `asgiref`. 
- Your programs typically only have one main application thread and/or one event loop (even then, some of the general data modelling utilities in this module like `WaitableStateTracker` may still prove useful).
- You need to coordinate and wait on _blocking_ work from `async` contexts. In that case, `asyncio.to_thread`, `asgiref`, or other tools are likely better fits.

## Development and Contributing

See [CONTRIBUTING.md](./CONTRIBUTING.md)

### LLM usage

Code in this module was written by hand. LLMs are used by maintainers to identify issues and sometimes to prototype changes or write tests, but all non-test code and documentation is reviewed and re-written by hand. No exceptions.

## License

`async-anywhere` is free and open source software. See `LICENSE` for more information.

