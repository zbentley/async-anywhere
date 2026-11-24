# Prior Art

["Running async code from sync code"](https://death.andgravity.com/asyncio-bridge) is a trenchant treatment of the issues that `EventLoopExecutor` tries to address, as are Armin Ronacher's [async posts](https://lucumr.pocoo.org/tags/async), and Rob Blackbourn's [ASGI event loop gotcha](https://rob-blackbourn.medium.com/asgi-event-loop-gotcha-76da9715e36d) article.

Python itself has wrestled with the realities of codebases that combine multiple asyncio sections that overlap, and multiple intersecting loops ([CPython PR #93338](https://github.com/python/cpython/pull/93338), [bpo-33523](https://bugs.python.org/issue33523), [bpo-22239](https://bugs.python.org/issue22239)).

Many other Python modules handle bridging asynchronous and synchronous Python code, and overlap with `EventLoopExecutor` (which bridges from `async` to `async` *or* synchronous code):

## Asgiref

[`asgiref`](https://github.com/django/asgiref), discussed in [When to use `EventLoopExecutor`](https://async-anywhere.readthedocs.io/en/latest/reference/executors/when_to_use.html), is the easiest-to-use tool for bridging between sync and async code. However, it doesn't natively have an easy "async-to-async" bridge, can impose substantial overhead when starting/stopping threads and event loops, and is written to run individual routines on/off of a given event loop rather than to maintain long-running captive asynchronous workers.

`asgiref` users often encounter bugs (e.g. [asgiref #194](https://github.com/django/asgiref/issues/194)) due to loop-mismatch. This is a byproduct of `asgiref`'s design prioritizing simplicity of bridging individual functions between sync and async code--a good tradeoff that `asgiref` maintainers mindfully made, but one with drawbacks: specifically, it's hard to see and control *which* captive loops and/or executors `asgiref` is using for a given bridging operation. 

`EventLoopExecutor` aims to make the opposite tradeoff: making the executor object explicit as the sole place where bridged calls between async/sync context can occur. Our approach requires users to engage with more concerns (executor blocking, coordinated stop/replacement, captive loop liveness), but provides additional performance and control. Again, the above should not be taken as criticism: `asgiref` is a very good tool and is appropriate for most sync/async bridging cases; `EventLoopExecutor` occupies a different niche.

## AnyIO

AnyIO's [`BlockingPortal`](https://anyio.readthedocs.io/en/stable/api.html#anyio.from_thread.BlockingPortal) is similar to `EventLoopExecutor` in that it provides a stateful handle to a captive worker. However, it has some key differences from `EventLoopExecutor`:

- `BlockingPortal`'s submit methods (e.g. [`start_task_soon()`](https://anyio.readthedocs.io/en/stable/api.html#anyio.from_thread.BlockingPortal.start_task_soon)) wait for the portal to accept the job, blocking the caller until it does. If the portal is blocked, this can cause hangs or deadlocks.
- `BlockingPortal` is designed for, well, *blocking* callers: synchronous threads calling into an event loop. Its methods block the calling thread, and the [`concurrent.futures.Future`](https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.Future)s it returns can't be `await`ed from another event loop without wrapping them. `EventLoopExecutor` is designed to be called from both blocking and `async` code: its futures can be `await`ed from any event loop or blocking-waited from any thread (and it can run blocking code--necessarily in serial--as well). `BlockingPortal`'s methods also can't be called from the portal's own event loop thread, whereas code running inside an `EventLoopExecutor` can submit work to it.

Neither `asgiref` nor AnyIO provides:

- A means of observing or orchestrating the shutdown of a captive event loop that may be simultaneously running work.
- Documented, specific semantics with regards to cancellation of jobs running on the captive event loop.
- Documented fork-safety semantics.

## Other Libraries

Many other libraries address these issues in a purpose-specific way:

- `fsspec`'s [`fsspecIO` thread](https://filesystem-spec.readthedocs.io/en/latest/_modules/fsspec/asyn.html) routes code to a captive async event loop, and is fork-safe for common usage.
- Prefect's [`run_coro_as_sync`](https://www.prefect.io/docs/v3/api-ref/python/prefect-utilities-asyncutils) runs on a shared loop thread, but in [#15008](https://github.com/PrefectHQ/prefect/issues/15008) Prefect began migrating away from that approach, citing "complex event loop and threading logic that risks performance degradation and more difficult to inspect failure modes".
- [`unsync`](https://pypi.org/project/unsync/) provides handles to a shared loop-containing thread, though it doesn't address lifecycle, cancellation, or fork safety.
- [asyncio-loop-local](https://github.com/t184256/asyncio-loop-local) solves the inverse problem of creating per-loop/per-caller captive singletons, rather than sharing one across all routines in a program.
- [SQLAlchemy](https://docs.sqlalchemy.org/en/20/orm/extensions/asyncio.html) addresses issues around multiple event loops by providing tools for replacing connections when new event loops are used.

## Related Issues

Many popular Python projects have struggled with the problems that `EventLoopExecutor` tries to address:

- redis-py [#3351](https://github.com/redis/redis-py/issues/3351): an async pool kept in Django settings fails as soon as two requests run concurrently.
- `httpx` is moving towards requiring `async with` management of HTTP clients ([#3163](https://github.com/encode/httpx/discussions/3163)) due to the risk of the kind of accidental loop-mismatch errors that `EventLoopExecutor` tries to help address.
  - See also `httpx` discussions [#2959](https://github.com/encode/httpx/discussions/2959) and [#3163](https://github.com/encode/httpx/discussions/3163).
- openai-python [#1254](https://github.com/openai/openai-python/issues/1254).
- `channels_redis` [#332](https://github.com/django/channels_redis/issues/332) and [#340](https://github.com/django/channels_redis/issues/340).
- conductor-oss [#507](https://github.com/conductor-oss/python-sdk/issues/507): a cached client whose pool is bound to a dead loop doesn't report itself as closed, so it gets reused.
- The async ClickHouse driver's [PR #21](https://github.com/Leechael/asynch/pull/21) recreates connections whenever the requesting event loop changes.
- SQLAlchemy [#5626](https://github.com/sqlalchemy/sqlalchemy/issues/5626) interacting with pytest-asyncio [#38](https://github.com/pytest-dev/pytest-asyncio/issues/38).
- Prefect [#13181](https://github.com/PrefectHQ/prefect/issues/13181).
- Django users have [struggled with `asgiref`'s performance/overhead](https://forum.djangoproject.com/t/a-datapoint-for-asgiref-performance/34633).
- Celery:
  - [#3884](https://github.com/celery/celery/issues/3884), a long-standing request for asyncio tasks, hits a lot of the challenges that `EventLoopExecutor` was built to address.
  - [Discussion #9058](https://github.com/celery/celery/discussions/9058): discusses how to preserve loop-locality to prevent connection churn with async-aware drivers.
  - [PR #10634](https://github.com/celery/celery/pull/10634): one shared loop per worker process.
  - [Hatchet's "problems with Celery"](https://hatchet.run/blog/problems-with-celery), discussed as a competitor to Celery's approach.
- 