# When to use `EventLoopExecutor`

`EventLoopExecutor` is the most powerful tool in this module. It may be appropriate for you if:

- You frequently need to invoke `async` code from a combination of synchronous code, multiple threads, and multiple different event loops/async contexts.
- You've had issues with `asgiref.async_to_sync` (e.g. high overhead, a desire to `await` on its results rather than blocking-wait for them, or repeated uses of `async_to_sync` resulting in errors related to closed or mismatched event loops).
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

The prototypical example here is an `async` database connection driver (e.g. `psycopg_pool.AsyncConnectionPool`) in a large `async` application codebase with many contributors. If the main event loop is ever blocked, that blocking can cause the database connection to fail application-level heartbeats and disconnect. While big codebases would ideally only use one event loop and avoid blocking it, it is our experience that this ideal rarely survives contact with reality, and that blocking, when it *does* occur, can result in unnecessarily costly issues: failures using the blocking-sensitive connection that are distant (in time and code location) from the source of the loop-blocking hang; the need for complicated and error-prone extraction of performance/application-critical code into external processes or networked services; the continual proliferation or replacement of connections that may be expensive to establish or maintain, and so on.

This is a situation where `asgiref.async_to_sync` can cause issues, since the event loop it uses may not be running once a given `async_to_sync` call finishes. To address that, users often wrap `asyncio.run` or similar in `asgiref.sync_to_async`, and then struggle with ensuring the futures returned from `sync_to_async` can be waited on from other event loops, or have issues starting/stopping/cancelling the captive event loop running inside `asgiref`'s thread. 

In this situation, routing the whole application's traffic to a given service's async driver in a shared `EventLoopExecutor` *might* limit throughput, since multiple threads and application routines may be using it simultaneously. That's sometimes a bottleneck, but we have found that it's often much *more* expensive to do connection-per-thread/connection-per-loop (or worse, async-connection-opened-and-closed-on-demand): many network service drivers support efficient multiplexing and request/response batching via their `async` APIs. Using multiple `async` handles in that context, even in separate threads/event loops that never block on or coordinate with each other, can be less efficient at the protocol level than using `EventLoopExecutor`-wrapped handle shared across all routines in a process.

## When not to use `EventLoopExecutor`

`EventLoopExecutor` may **not** be appropriate for some situations:

- If `asgiref.async_to_sync` and `asgiref.sync_to_async` serve your needs, you may not need `EventLoopExecutor`. `asgiref` is an excellent, battle-tested module that serves a specific niche: if you fit in that niche, use `asgiref`. `EventLoopExecutor` is presented as something that *picks up where `asgiref` leaves off*, not as an alternative to its functionality.
- If your programs typically only have one main application thread and/or one event loop which is rarely blocked, the overhead resulting from `EventLoopExecutor` is likely not worth the functionality it provides.
- You need to coordinate and wait on _blocking_ work from `async` contexts. In that case, `asyncio.to_thread`, `asgiref.sync_to_async`, `multiprocessing`, or other tools are likely better fits.

## Prior Art

["Running async code from sync code"](https://death.andgravity.com/asyncio-bridge) is a trenchant treatment of the issues this module tries to address, as are Armin Ronacher's [async posts](https://lucumr.pocoo.org/tags/async), and Rob Blackbourn's [ASGI event loop gotcha](https://rob-blackbourn.medium.com/asgi-event-loop-gotcha-76da9715e36d) article.

Many other Python modules handle bridging asynchronous and synchronous Python code, and overlap with `EventLoopExecutor` (which bridges from `async` to `async` *or* synchronous code):

`asgiref`, discussed above, is the easiest-to-use tool for bridging between sync and async code. However, it doesn't natively have an easy  "async-to-async" bridge, can impose substantial overhead when starting/stopping threads and event loops, and is written to run individual routines on/off of a given event loop rather than to maintain long-running captive asynchronous workers.

`asgiref` users often encounter bugs (e.g. https://github.com/django/asgiref/issues/194) due to loop-mismatch. This is a byproduct of `asgiref`'s design prioritizing simplicity of bridging individual functions between sync and async code--a good tradeoff that `asgiref` maintainers mindfully made, but one with drawbacks: specifically, it's hard to see and control *which* captive loops and/or executors `asgiref` is using for a given bridging operation.  `EventLoopExecutor` aims to make the opposite tradeoff: making the executor object explicit as the sole place where bridged calls between async/sync context can occur. Our approach requires users to engage with more concerns (executor blocking, coordinated stop/replacement, captive loop liveness), but provides additional performance and control. Again, the above should not be taken as criticism: `asgiref` is a very good tool and is appropriate for most sync/async bridging cases; `EventLoopExecutor` occupies a different niche.

Anyio's BlockingPortal is similar to `EventLoopExecutor` in that it provides a stateful handle to a captive worker. However, it has some key differences from `EventLoopExecutor`:
- BlockingPortal's submit methods wait for the portal to accept the job, blocking the caller until it does. If the portal is blocked, this can cause hangs or deadlocks.
- BlockingPortal is designed to run, well, *blocking* code. `EventLoopExecutor` is designed to run nonblocking/awaitable code concurrently (though it can run blocking code--necessarily in serial--as well).

Neither asgiref nor anyio provide:
- A means of observing or orchestrating the shutdown of a captive event loop that may be simultaneously running work.
- Documented, specific semantics with regards to cancellation of jobs running on the captive event loop.
- Documented fork-safety semantics.

Many other libraries address these issues in a purpose-specific way:

- fsspec's ["fsspecIO" thread](https://filesystem-spec.readthedocs.io/en/latest/_modules/fsspec/asyn.html) routes code to a captive async event loop, and is fork-safe for common usage.
- Prefect's [`run_coro_as_sync`](https://docs.prefect.io/v3/api-ref/python/prefect-utilities-asyncutils) runs on a shared loop thread, but in [#15008](https://github.com/PrefectHQ/prefect/issues/15008) Prefect began migrating away from that approach, citing "complex event loop and threading logic that risks performance degradation and more difficult to inspect failure modes".
- [unsync](https://pypi.org/project/unsync/) provides handles to a shared loop-containing thread, though it doesn't address lifecycle, cancellation, or fork safety.
- [asyncio-loop-local](https://github.com/t184256/asyncio-loop-local) solves the inverse problem of creating per-loop/per-caller captive singletons, rather than sharing one across all routines in a program.
- [SQLAlchemy](https://docs.sqlalchemy.org/en/20/orm/extensions/asyncio.html) addresses issues around multiple event loops by providing tools for replacing connections when new event loops are used.

### Related Issues

Many popular Python projects have struggled with the problems that `EventLoopExecutor` tries to address:

- redis-py [#3351](https://github.com/redis/redis-py/issues/3351): an async pool kept in Django settings fails as soon as two requests run concurrently.
- `httpx` is moving towards requiring `async with` management of HTTP clients ([#3163](https://github.com/encode/httpx/discussions/3163)) due to the risk of the kind of accidental loop-mismatch errors that `EventLoopExecutor` tries to help address.
  - See also `httpx` discussions [#2959](https://github.com/encode/httpx/discussions/2959) and [#3163](https://github.com/encode/httpx/discussions/3163).
- openai-python [#1254](https://github.com/openai/openai-python/issues/1254).
- channels_redis [#332](https://github.com/django/channels_redis/issues/332) and [#340](https://github.com/django/channels_redis/issues/340).
- conductor-oss [#507](https://github.com/conductor-oss/python-sdk/issues/507): a cached client whose pool is bound to a dead loop doesn't report itself as closed, so it gets reused.
- The async ClickHouse driver's [PR #21](https://github.com/Leechael/asynch/pull/21) recreates connections whenever the requesting event loop changes.
- SQLAlchemy [#5626](https://github.com/sqlalchemy/sqlalchemy/issues/5626) interacting pytest-asyncio [#38](https://github.com/pytest-dev/pytest-asyncio/issues/38)
- Prefect [#13181](https://github.com/PrefectHQ/prefect/issues/13181).
- Celery:
  - [#3884](https://github.com/celery/celery/issues/3884), a long-standing request for asyncio tasks, hits a lot of the challenges that `EventLoopExecutor` was built to address.
  - [Discussion #9058](https://github.com/celery/celery/discussions/9058): discusses how to preserve loop-locality to prevent connection churn with async-aware drivers.
  - [PR #10634](https://github.com/celery/celery/pull/10634): one shared loop per worker process.
  - [Hatchet's "problems with Celery"](https://hatchet.run/blog/problems-with-celery), discussed as a competitor to Celery's approach.
- 