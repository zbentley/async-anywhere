# When to use `EventLoopExecutor`

{py:class}`.EventLoopExecutor` is the most powerful tool in this module. It may be appropriate for you if:

- You frequently need to invoke `async` code from a combination of synchronous code, multiple threads, and multiple different event loops/async contexts.
- You've had issues with [`asgiref.async_to_sync`](https://docs.djangoproject.com/en/stable/topics/async/#asgiref.sync.async_to_sync) (e.g. high overhead, a desire to `await` on its results rather than blocking-wait for them, or repeated uses of `async_to_sync` resulting in errors related to closed or mismatched event loops).
- You frequently struggle with `RuntimeError: This event loop is already running` errors, or `bound to a different event loop` errors.
- You've ever hit a problem where you think "I'll run an event loop in a thread and send calls to it", and then struggled with implementing `await`, or blocking waits, on the results of that thread.
- You've struggled to appropriately cancel jobs running on an event loop that is not the currently active one.
- You've fought with errors related to cleaning up jobs when event loops shut down.
- You have expensive-to-construct, stateful async routines that you need to interact with from other event loops/threads, and repeatedly constructing those routines (e.g. one-per-thread or one-per-loop) is undesirable.

## Integrating code that wants to own the `async` entry point

`EventLoopExecutor` is particularly useful when you need to integrate multiple libraries that want to own and drive the main `async` entry point of your program, and you need to use them from different loops and threads (e.g. worker threads or event loops started by a webserver framework).

The prototypical example here is a [DiscordPy bot client](https://discordpy.readthedocs.io/en/stable/quickstart.html): a constantly-running async routine that wants to control the entry point of your program so that it can remain connected to remote services. If a program contains a mix of threads and async code, or if your program already uses an entry-point-controlling framework like Django, integrating an always-running async client like DiscordPy's may prove challenging.

## Isolating `async` code that is sensitive to event loop blocking

`EventLoopExecutor` is particularly useful when you have some stateful, long-running `async` code that needs to continually perform certain actions with low latency, but you're using it from a large asynchronous codebase which may occasionally block its event loop.

`EventLoopExecutor` allows such code to run in a thread unobstructed by event loop hangs in the rest of the program, without requiring the complexity and overhead of isolating such connections into an external process.

The prototypical example here is an `async` database connection driver (e.g. [`psycopg_pool.AsyncConnectionPool`](https://www.psycopg.org/psycopg3/docs/api/pool.html#psycopg_pool.AsyncConnectionPool)) in a large `async` application codebase with many contributors. If the main event loop is ever blocked, that blocking can cause the database connection to fail application-level heartbeats and disconnect. While big codebases would ideally only use one event loop and avoid blocking it, it is our experience that this ideal rarely survives contact with reality, and that blocking, when it *does* occur, can result in unnecessarily costly issues: failures using the blocking-sensitive connection that are distant (in time and code location) from the source of the loop-blocking hang; the need for complicated and error-prone extraction of performance/application-critical code into external processes or networked services; the continual proliferation or replacement of connections that may be expensive to establish or maintain, and so on.

This is a situation where `asgiref.async_to_sync` can cause issues, since the event loop it uses may not be running once a given `async_to_sync` call finishes. To address that, users often wrap [`asyncio.run()`](https://docs.python.org/3/library/asyncio-runner.html#asyncio.run) or similar in [`asgiref.sync_to_async`](https://docs.djangoproject.com/en/stable/topics/async/#asgiref.sync.sync_to_async), and then struggle with ensuring the futures returned from `sync_to_async` can be waited on from other event loops, or have issues starting/stopping/cancelling the captive event loop running inside `asgiref`'s thread.

In this situation, routing the whole application's traffic to a given service's async driver in a shared `EventLoopExecutor` *might* limit throughput, since multiple threads and application routines may be using it simultaneously. That's sometimes a bottleneck, but we have found that it's often much *more* expensive to do connection-per-thread/connection-per-loop (or worse, async-connection-opened-and-closed-on-demand): many network service drivers support efficient multiplexing and request/response batching via their `async` APIs. Using multiple `async` handles in that context, even in separate threads/event loops that never block on or coordinate with each other, can be less efficient at the protocol level than using an `EventLoopExecutor`-wrapped handle shared across all routines in a process.

## When not to use `EventLoopExecutor`

`EventLoopExecutor` may **not** be appropriate for some situations:

- If `asgiref.async_to_sync` and `asgiref.sync_to_async` serve your needs, you may not need `EventLoopExecutor`. `asgiref` is an excellent, battle-tested module that serves a specific niche: if you fit in that niche, use `asgiref`. `EventLoopExecutor` is presented as something that *picks up where `asgiref` leaves off*, not as an alternative to its functionality.
- If your programs typically only have one main application thread and/or one event loop which is rarely blocked, the overhead resulting from `EventLoopExecutor` is likely not worth the functionality it provides.
- You need to coordinate and wait on _blocking_ work from `async` contexts. In that case, [`asyncio.to_thread()`](https://docs.python.org/3/library/asyncio-task.html#asyncio.to_thread), `asgiref.sync_to_async`, [`multiprocessing`](https://docs.python.org/3/library/multiprocessing.html), or other tools are likely better fits.
