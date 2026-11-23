# Status

**Unfinished:** This module is feature complete but not yet finalized. I plan to finish finalizing the API, docs, tests, *et cetera* in late 2026 and publish it to PyPi, at which time this section will be removed.

# async-anywhere

`async-anywhere` is a set of tools which address common Python concurrency needs. It contains tools for bridging between any number of blocking threads and `async` event loops, tools for modelling state machines and state transitions, `fork(2)`-safety semantics for common concurrency primitives, and more.

If you've run up against the limits of what `asgiref` supports, are struggling with code that needs to coordinate between multiple threads and event loops, dealing with deadlocks caused by interacting with `Future` objects after a `fork(2)`, or you need to maintain stateful/continuously-running async components of a larger program, this module may help!

It includes:

- `AwaitableFuture`, a `concurrent.futures.Future` which can be blocked on or `await`ed on from any number of event loops or threads. It supports configurable cancellation behavior, `fork(2)` safety, lock-free operation on many paths that the stdlib serializes, and more.
- `EventLoopExecutor`, a `concurrent.futures.Executor` which runs **`async`** code on a thread, and whose results can be waited on from anywhere. It supports idle expiration, calling into itself from within its captive thread, `fork(2)` safety, task cancellation, controlled shutdown, "parent" functions which own the event loop, and more.
- `StateTracker`, a "state machine lite" which allows a class to transition between one of several declared states, and allows other code to wait for the tracker to enter one or more states in an `async`- and thread-agnostic way.
- Several other tools and utilities related to Python concurrency management.

See "Examples" below, or [the API documentation]() for more information.

## Installation and Requirements

PyPi packages are not yet available. **PyPi packaging is [being actively worked on](https://github.com/zbentley/async-anywhere/issues/11)**.

Until PyPi packaging support lands, this module can be installed from source, with `pip`:

```console
pip install git+https://github.com/zbentley/async-anywhere.git@main
```

Or via `uv`:

```console
uv add git+https://github.com/zbentley/async-anywhere.git@main
```

`async-anywhere` is a pure-Python module with no compiled components, and aims to have as few pure-python transitive dependencies as possible.

### Compatibility

This module is tested and used in production on Linux and MacOS systems. BSDs are likely supported, but tests on those systems have not yet been performed. Windows support is planned.

`async-anywhere` supports:

- Code written for `asyncio`.
- Code written for `trio`.
- Code written for `anyio`.
- Code using `AbstractEventLoop`, including `uvloop`.
- Correct, minimally locking operations with or without the Global Interpreter Lock (GIL).

## Examples

### `AwaitableFuture`

```python
from async_anywhere.futures import AwaitableFuture
from threading import Thread
import asyncio

fut = AwaitableFuture(
    # Don't allow calls to .result() if this loop is running; that'll deadlock.
    prevent_blocking_wait_within=[asyncio.get_event_loop()],
)
# 
fut.add_done_callback(lambda f: print(f"Future {f} is done!"), run_in_forked_child=True)

async def foo():
    print(await fut)

# Wait from one event loop:
Thread(target=asyncio.run, args=(foo(),)).start() 
# ... two event loops:
Thread(target=asyncio.run, args=(foo(),)).start() 
# Set the result from anywhere:
Thread(target=fut.set_result, args=("All done!",)).start()
# Wait from event loops without a wrapper:
asyncio.run(fut) 
# ... or synchronously:
print(fut.result(timeout=1))
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

This module can be used in a variety of situations, including uses where `async` Python code isn't in use at all. Each of the tools in this module has affinity for particular use cases, and may not be appropriate for others.

- `EventLoopExecutor` is the most powerful tool in this module, and is useful when you need to invoke `async` code from a combination of synchronous code, multiple threads, and multiple different event loops, and `asgiref.async_to_sync` isn't sufficient ([when to use `EventLoopExecutor`](docs/reference/executors/when_to_use.md)).
- `AwaitableFuture` is useful if you have code which heavily uses `asyncio.Future` or `concurrent.futures.Future` directly, and you want a friendlier (and more performant in many cases) API ([when to use `AwaitableFuture`](docs/reference/futures/when_to_use.md)).
- `StateTracker` is useful for code that wants to model "state machines without hooks": code that transitions between states in a graph, but doesn't want to model all of its code as hooks on particular state transitions ([when to use `StateTracker`](docs/reference/state_tracker/when_to_use.md)).

## Development and Contributing

See [CONTRIBUTING.md](./CONTRIBUTING.md)

### LLM usage

Code in this module was written by hand. LLMs are sometimes used by maintainers to identify issues, prototype changes, or write tests. 

However, all non-test generated code and documentation is reviewed and re-written by hand before being released (even if that means re-typing it verbatim). No exceptions. We believe that the mechanical act of *code authorship* itself benefits comprehension and maintainability, and thus will hand-write/transcribe all non-test code even when LLM outputs are correct. 

Code supplied by contributors will be reviewed regardless of provenance, but we request that contributors participate in the contribution process as humans and not rely exclusively on agents, regardless of who wrote the code.

## License

`async-anywhere` is free and open source software. See `LICENSE` for more information.

