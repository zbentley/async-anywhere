# async-anywhere

| [API Documentation](https://async-anywhere.readthedocs.io/en/latest/) [![Docs](https://img.shields.io/readthedocs/async-anywhere/latest)](https://async-anywhere.readthedocs.io/en/latest/) | [Source code](https://github.com/zbentley/async-anywhere) [![Last commit](https://img.shields.io/github/last-commit/zbentley/async-anywhere)](https://github.com/zbentley/async-anywhere) | [PyPI Package](https://pypi.org/project/async-anywhere/) [![PyPI](https://img.shields.io/pypi/v/async-anywhere)](https://pypi.org/project/async-anywhere/) |
|:---:|:---:|:---:|

[![Tests](https://img.shields.io/github/actions/workflow/status/zbentley/async-anywhere/test.yml?branch=main&label=tests)](https://github.com/zbentley/async-anywhere/actions/workflows/test.yml?query=branch%3Amain)
[![Black](https://img.shields.io/github/actions/workflow/status/zbentley/async-anywhere/black.yml?branch=main&label=black)](https://github.com/zbentley/async-anywhere/actions/workflows/black.yml?query=branch%3Amain)
![Python 3.13.3+](https://img.shields.io/badge/python-3.13.3%2B-blue)
![Python 3.14](https://img.shields.io/badge/python-3.14-blue)
[![Free-threaded: tested](https://img.shields.io/badge/free--threaded-3.14t%20tested-blue)](https://github.com/zbentley/async-anywhere/blob/main/.github/workflows/test.yml)
![Platform: Linux | macOS](https://img.shields.io/badge/platform-Linux%20%7C%20macOS-lightgrey)

A library of tools which address common Python concurrency needs. It helps with bridging between blocking threads and `async` event loops, modelling state machines and state transitions, adding fork-safety semantics for common concurrency primitives, and more.

If you are struggling with code that needs to coordinate between multiple threads and event loops (even if you're not using `async`/`await` anywhere), dealing with deadlocks caused by interacting with `Future` objects after a `fork(2)`, or you need to maintain stateful/continuously-running `async` components of a larger program, this library may help!

It includes:

- [`AwaitableFuture`](https://async-anywhere.readthedocs.io/en/latest/reference/futures/awaitable.html#async_anywhere.futures.AwaitableFuture), a `concurrent.futures.Future` which can be blocked on or `await`ed on from any number of event loops or threads. It supports configurable cancellation behavior, `fork(2)` safety, lock-free operation on many paths that the stdlib serializes, and more.
- [`EventLoopExecutor`](https://async-anywhere.readthedocs.io/en/latest/reference/executors/EventLoopExecutor.html#async_anywhere.executors.EventLoopExecutor), a `concurrent.futures.Executor` which runs **`async`** code on a thread, and whose results can be waited on from anywhere. It supports idle expiration, calling into itself from within its captive thread, `fork(2)` safety, task cancellation, controlled shutdown, "parent" functions which own the event loop, and more.
- [`StateTracker`](https://async-anywhere.readthedocs.io/en/latest/reference/state_tracker/StateTracker.html#async_anywhere.state_tracker.StateTracker), a "state machine lite" which allows a class to transition between one of several declared states, and allows other code to wait for the tracker to enter one or more states in an `async`- and thread-agnostic way.
- Several other tools and utilities related to Python concurrency management.

See "Examples" below, or [the API documentation](https://async-anywhere.readthedocs.io/en/latest/reference/index.html) for more information.

## Installation and Requirements

This library requires Python 3.13.3+, and can be installed from [PyPI](https://pypi.org/project/async-anywhere/), with `pip`:

```console
pip install async-anywhere
```

Or via `uv`:

```console
uv add async-anywhere
```

`async-anywhere` is a pure-Python library with no required compiled components, and aims to have as few pure-python transitive dependencies as possible.

## Examples

### `AwaitableFuture`

```python
from async_anywhere.futures import AwaitableFuture
from threading import Thread
import asyncio

loop = asyncio.new_event_loop()
fut = AwaitableFuture(
    # Don't allow calls to .result() if this loop is running; that'll deadlock.
    prevent_blocking_wait_within=[loop],
)
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
print(loop.run_until_complete(fut))
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
    await executor.shutdown(wait=False, cancel_futures=True)


# ...or synchronous code
executor.shutdown(wait=True, cancel_futures=True).result()

```

`EventLoopExecutor` is ideal for integrating third-party `async` code that wants to own the event loop/entry point (e.g. [`discordpy`](https://discordpy.readthedocs.io/en/stable/quickstart.html)) into a larger program that may not be `async`, may block the event loop, or may use multiple event loop runtimes. Useful when you need more than `asgiref.async_to_sync` can provide.

### Compatibility

This library is tested and used in production on Linux and MacOS systems. BSDs are likely supported, but tests on those systems have not yet been performed. Windows support is planned.

`async-anywhere` supports:

- Code written for `asyncio`.
- Code written for `trio`, though Trio support is a bit fussy to use and will be improved in a future release.
- Code written for `anyio`.
- Code using `AbstractEventLoop`, including `uvloop`.
- Correct, minimally locking operations with or without the Global Interpreter Lock (GIL).

Code using [nest-asyncio](https://pypi.org/project/nest-asyncio/), or other tools which monkeypatch or mock out core `asyncio` primitives, may not work well with this library.

## When should you use this library?

This library can be used in a variety of situations, including uses where `async` Python code isn't in use at all. Each of the tools in this library has affinity for particular use cases, and may not be appropriate for others.

- `EventLoopExecutor` is the most powerful tool in this library, and is useful when you need to invoke `async` code from a combination of synchronous code, multiple threads, and multiple different event loops, and `asgiref.async_to_sync` isn't sufficient ([when to use `EventLoopExecutor`](https://async-anywhere.readthedocs.io/en/latest/reference/executors/when_to_use.html)).
- `AwaitableFuture` is useful if you have code which heavily uses `asyncio.Future` or `concurrent.futures.Future` directly, and you want a friendlier (and more performant in many cases) API ([when to use `AwaitableFuture`](https://async-anywhere.readthedocs.io/en/latest/reference/futures/when_to_use.html)).
- `StateTracker` is useful for code that wants to model "state machines without hooks": code that transitions between states in a graph, but doesn't want to model all of its code as hooks on particular state transitions ([when to use `StateTracker`](https://async-anywhere.readthedocs.io/en/latest/reference/state_tracker/when_to_use.html)).

For how `EventLoopExecutor` compares to other tools that bridge synchronous and `async` code, see [Prior Art](https://async-anywhere.readthedocs.io/en/latest/reference/executors/prior_art.html).

## Fork Safety

The different subcomponents of this library are fork-safe, in that they have documented semantics and avoid unexpected blocking or state corruption in the presence of code that uses `os.fork()`:

- [`EventLoopExecutor` fork safety](https://async-anywhere.readthedocs.io/en/latest/reference/executors/fork_safety.html).
- [`AwaitableFuture` fork safety](https://async-anywhere.readthedocs.io/en/latest/reference/futures/fork_safety.html).
- [`StateTracker` fork safety](https://async-anywhere.readthedocs.io/en/latest/reference/state_tracker/fork_safety.html).

**Note** that care is still needed when forking in the presence of this library or concurrent Python code generally; read
the above carefully in order to understand tradeoffs related to `fork(2)`.

## Development and Contributing

See [CONTRIBUTING.md](https://github.com/zbentley/async-anywhere/blob/main/CONTRIBUTING.md)

### LLM usage

Code in this library was written by hand. LLMs are sometimes used by maintainers to identify issues, prototype changes, or write tests. 

However, all non-test generated code and documentation is reviewed and re-written by hand before being released (even if that means re-typing it verbatim). No exceptions. We believe that the mechanical act of *code authorship* itself benefits comprehension and maintainability, and thus will hand-write/transcribe all non-test code even when LLM outputs are correct. 

Code supplied by contributors will be reviewed regardless of provenance, but we request that contributors participate in the contribution process as humans and not rely exclusively on agents, regardless of who wrote the code.

## License

`async-anywhere` is free and open source software. See [`LICENSE`](https://github.com/zbentley/async-anywhere/blob/main/LICENSE) for more information.
