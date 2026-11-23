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

### `EventLoopExecutor`

`EventLoopExecutor` is the most powerful tool in this module. It may be appropriate for you if:

- You frequently need to invoke `async` code from a combination of synchronous code, multiple threads, and multiple different event loops/async contexts.
- You need to integrate multiple libraries that want to own and drive the main `async` entry point of your program, and you need to use them from different loops and threads (e.g. worker threads or event loops started by a webserver framework). 
  - The prototypical example here is a [DiscordPy bot client](https://discordpy.readthedocs.io/en/stable/quickstart.html): a constantly-running async routine that wants to control the entry point of your program so that it can remain connected to remote services. If a program contains a mix of threads and async code, or if your program already uses an entry-point-controlling framework like Django, integrating an always-running async client like DiscordPy's may prove challenging.
- You have a some stateful, long-running `async` code that needs to continually perform certain actions with low latency, but you're using it from a large asynchronous codebase which may occasionally block its event loop. `EventLoopExecutor` allows such code to run in a thread unobstructed by event loop hangs in the rest of the program, without requiring the complexity and overhead of isolating such connections into an external process.
  - The prototypical example here is an `async` database connection driver in a large `async` application codebase with many contributors. If the main event loop is ever blocked, that blocking can cause the database connection to fail application-level heartbeats and disconnect. While big codebases would ideally only use one event loop and avoid blocking it, it is our experience that this ideal rarely survives contact with reality, and that blocking, when it *does* occur, can result in unnecessarily costly issues: failures using the blocking-sensitive connection that are distant (in time and code location) from the source of the loop-blocking hang; the need for complicated and error-prone extraction performance/application-critical code into external processes or networked services; the continual proliferation or replacement of connections that may be expensive to establish or maintain, and so on.
  - This is a situation where `asgiref.async_to_sync` can cause issues, since the event loop it uses may not be running once a given `async_to_sync` call finishes. To address that, users often wrap `asyncio.run` or similar in `asgiref.sync_to_async`, and then struggle with ensuring the futures returned from `sync_to_async` can be waited on from other event loops, or have issues starting/stopping/cancelling the captive event loop running inside `asgiref`'s thread. 
- You've had issues with `asgiref.async_to_sync` (e.g. high overhead, a desire to `await` on its results rather than blocking-wait for them, or repeated uses of `async_to_sync` resulting in errors related to closed or mismatched event loops).
- You frequently struggle with `RuntimeError: This event loop is already running` errors, or `bound to a different event loop` errors.
- You've ever hit a problem where you think "I'll run an event loop in a thread and send calls to it", and then struggled with implementing `await`, or blocking waits, on the results of that thread.
- You've struggled to appropriately cancel jobs running on an event loop that is not the currently active one.
- You've fought with errors related to cleaning up jobs when event loops shut down.

That said, `EventLoopExecutor` may **not** be appropriate for some situations:

- If `asgiref.async_to_sync` and `asgiref.sync_to_async` serve your needs, you may not need `EventLoopExecutor`. `asgiref` is an excellent, battle-tested module that serves a specific niche: if you fit in that niche, use `asgiref`. `EventLoopExecutor` is presented as something that *picks up where `asgiref` leaves off*, not as an alternative to its functionality.
- If your programs typically only have one main application thread and/or one event loop which is rarely blocked, the overhead resulting from `EventLoopExecutor` is likely not worth the functionality it provides.
- You need to coordinate and wait on _blocking_ work from `async` contexts. In that case, `asyncio.to_thread`, `asgiref.sync_to_async`, `multiprocessing`, or other tools are likely better fits.

### `AwaitableFuture`

Use `AwaitableFuture` if you have code which heavily uses `asyncio.Future` or `concurrent.futures.Future` directly, and you want a friendlier (and more performant in many cases) API. 

`concurrent.futures.Future` objects in the stdlib are recommended only for internal use; if you've ever wanted to create and pass those around as part of your regular business logic, then `AwaitableFuture` may meet your needs. 

`AwaitableFuture` is well-suited for modelling "coalescing" waits: when multiple async or threaded routines want to wait on a the result of a single computation, or when code wants to provide all callers with cached results of a computation that may or may be complete.

In general:
- If you've ever wanted to wait on the same result to arrive from multiple threads and `async`/`await` routines simultaneously, and are tired of chaining futures/callbacks by hand, `AwaitableFuture` may be appropriate for you.
- Exclusively synchronous code, with no `async`/`await`s anywhere: `AwaitableFuture` may be useful for data modelling, and is likely more performant and friendlier than `concurrent.futures.Future`.
- Exclusively asynchronous code on a single, long-lived event loop: `AwaitableFuture` is probably _not_ the right tool; using a regular `asyncio.Future` is likely to be more performant and more compatible with other stdlib tools.
- Asynchronous code with multiple event loops that need to share data: `AwaitableFuture` provides a convenient way to avoid loop-mismatch errors.

### `StateTracker`

`StateTracker` is a useful general-purpose data modelling tool for Python code in general, and can be used in any code that wants to model "state machines without hooks"--that is, code that transitions between states in a (not necessarily acyclic) graph, but doesn't want to model all of its code as hooks on particular state transitions.

In general:
- Code that wants to model a state graph at compile time and traverse it may benefit from `StateTracker`.
- Code that wants to manage traversal of the state graph in a thread-safe and self-validating way--raising errors instead of allowing invalid transitions--may benefit from `StateTracker`. `StateTracker` is low-overhead and works well in high-transition-rate scenarios like managing the status of incomplete work items/callbacks/business transactions.
- Code that wants to efficiently synchronously or asynchronously wait on a state graph to enter a given state may benefit from `WaitableStateTracker`.
- Code that constructs many different state graphs dynamically based on data only available at runtime is probably *not* well-suited for `StateTracker`; you can declare `StateTracker` classes at runtime and pass them around, but better tools for modelling that are available; `StateTracker` is opinionatedly oriented towards state graphs that can be expressed in (relatively) static code at compile time. If it doesn't make sense to commit your state graph in source code, `StateTracker` is probably not the right tool for you.
- Code that wants a traditional state machine, where logic is attached to state transitions directly, should probably consider alternatives such as [`python-statemachine`](https://github.com/fgmacedo/python-statemachine), [`transitions`](https://github.com/pytransitions/transitions), [`sismic`](https://github.com/AlexandreDecan/sismic), or others (e.g. those listed on [`awesome-fsm`](https://github.com/leonardomso/awesome-fsm#python)).
- Code that wants to model state graphs as complex workflows of business logic declared across multiple modules, or that wants to durably record and reconstitute the state of a graph should probably consider alternatives such as [DBOS](https://github.com/dbos-inc/dbos-transact-py), [`runnable`](https://astrazeneca.github.io/runnable), [Temporal](https://docs.temporal.io/develop/python), [Celery](https://docs.celeryq.dev/en/stable/getting-started/introduction.html), or others.

## Development and Contributing

See [CONTRIBUTING.md](./CONTRIBUTING.md)

### LLM usage

Code in this module was written by hand. LLMs are sometimes used by maintainers to identify issues, prototype changes, or write tests. 

However, all non-test generated code and documentation is reviewed and re-written by hand before being released (even if that means re-typing it verbatim). No exceptions. We believe that the mechanical act of *code authorship* itself benefits comprehension and maintainability, and thus will hand-write/transcribe all non-test code even when LLM outputs are correct. 

Code supplied by contributors will be reviewed regardless of provenance, but we request that contributors participate in the contribution process as humans and not rely exclusively on agents, regardless of who wrote the code.

## License

`async-anywhere` is free and open source software. See `LICENSE` for more information.

