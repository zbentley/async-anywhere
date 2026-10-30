# TODOs

EventLoopExecutor todos;
- decorate loop-only methods
- add debug flag that causes decorator to assert, and runs loop in dbg
- _run() interrupting itself isn't cancelled. If _run() calls shutdown(interrupt=True) itself, shutdown() raises WorkerStopped into it, but _run() isn't cancelled.
- cope w/ interrupted jobs whose cleanup async-hangs
- cope with (really: yell about) interrupted jobs whose cleanup SYNC hangs
- worker *is* a state machine
- Log never-awaited on bridge futures

# Docs to incorporate into executor

Invariants
----------
- Every call resolves. One the loop drops unrun as it closes fails with why the worker stopped, which shutdown()'s waiters
  get in its result: nothing waits forever on a worker that has finished (bar wait_for_state() for a state it had
  passed before the future was made: see State).
- Nothing outside the worker blocks on its loop. Making, waiting on and cancelling a call only queue callbacks on it, so a
  worker loop blocked by synchronous code holds callers up only as long as they wait for results.
- Bar shutdown(cancel_futures=True), which cancels each time it's called (see Interruption), and a crash's teardown, the worker
  cancels each task at most once, so cleanup can await without being cut short.
- A caller cancelling its call cancels the job, but a job's ending never cancels its caller.
- Why the worker stopped (a WorkerStopped) is kept apart from what calls raise.
- Giving up waiting on shutdown() never cancels the stop, nor a drain.
- Idle expiry never fires while a call is in flight. (Once the worker has stopped, it may, but then does nothing.)

One invariant is the user's to uphold: never run another event loop on a worker's thread. While the worker's loop runs,
asyncio refuses to anyway; this is about code that thread runs outside it, such as finalizers as the loop starts and
stops. The worker takes any code on its thread to be in its loop (see in_worker), so a bridged function called from another
loop there would run the client on that loop.

Submitting a job, compared with awaiting a coroutine
----------------------------------------------------
Submitting a job (a call: with submit(), or by calling a bridged function) is like create_task() with an eager task factory: the job starts as soon as the worker's loop
takes the call, ahead of anything queued after it, and runs in the background, whatever the caller's await points (calls
made together run concurrently).

From any thread, it returns an AwaitableFuture: a concurrent.futures.Future, which can also be awaited on any loop. Once
the worker has stopped, it raises why instead, as other executors' submit() does once shut down (but with a
WorkerStopped, not RuntimeError).
- It can be waited on repeatedly, and from several threads at once. One nobody waits on runs all the same, but what it
  raises goes unreported (where asyncio would log a task's as never retrieved).
- Giving up a result() or exception() (as it times out, say) leaves the call running, as for any concurrent future.
  Cancelling the future, or a task awaiting it, cancels the job, without waiting for it to finish cancelling.
- Waiting on an unfinished call with result() or exception() on the worker's own loop raises RuntimeError, as nothing
  could finish it.
- But it isn't a coroutine, nor an asyncio future: create_task(), TaskGroup.create_task() and asyncio.wait() reject it, and
  inspect.iscoroutinefunction() is False for bridged functions (so frameworks may take them for synchronous). For APIs
  that take asyncio futures, use call.as_async_future(), which (unlike asyncio.wrap_future(call)) cancels the job as
  soon as it's cancelled. It can be awaited in a trio task too (anyio's, on trio, included), whose cancellation cancels
  the job, as on asyncio.
- The job runs in a task of its own on the worker's loop, with a copy of the caller's context variables as they were when
  the call was made, bar sniffio's record of the caller's async library (which anyio.run() sets): anyio, in the job,
  finds asyncio. Changes it makes to them don't reach the caller. Library state the copy carries, tied to
  the caller's thread, may or may not keep working on the worker's: for a synchronous caller (through asgiref's
  async_to_sync()), the job's thread-sensitive sync_to_async() calls run on the caller's thread with asgiref 3.11, but on
  another with 3.12.
- Its return value and exceptions reach the caller as the same objects, raised again there. So they, and the arguments,
  cross between loops and threads: they mustn't be bound to either loop (asyncio futures, tasks, streams, async
  generators, clients), and asyncio's Event, Queue and Lock aren't thread-safe.
- Exceptions differ in four cases:
  - Refused, as the worker stopped after the call was made, but before its job started: WorkerStopped.
  - Cut short by an interrupt: concurrent.futures.CancelledError (a private subclass), raised from a copy of the
    WorkerStopped, itself raised from whatever stopped the worker, if anything. Its traceback ends where the job was
    cancelled. It's not asyncio's CancelledError, which asyncio would take for the caller's own task being cancelled.
  - The function raising asyncio's CancelledError itself, without being cancelled: the caller gets that CancelledError,
    raised by its await or result(), rather than a cancelled future.
  - The caller cancelling: it gets its own cancellation, as usual.

Calls made on the worker's own thread are no different.

shutdown(wait=True, *, cancel_futures=False) isn't a call. It queues the stop on the worker's loop, behind calls already
made (which a drain lets finish), from any thread, the worker's own included. It returns wait_for_state(Stopped)'s
AwaitableFuture, resolving once the worker has finished to (old state, new state, why the worker stopped), why being a
WorkerStopped, which it returns rather than raises (a WorkerForked, in a process forked from the worker's). With wait=True
(as `with worker:` exits, too) it first waits for that future, blocking the calling thread, and any event loop running
on it: async code should await shutdown(wait=False) instead. On the worker's own thread, wait=True raises RuntimeError, as
it would wait for itself. cancel_futures=True interrupts (see Interruption), rather than cancelling calls not yet
started, as ThreadPoolExecutor's does: calls made before it start before the stop begins, then are cut short, failing
with WorkerInterrupted (a concurrent.futures.CancelledError, though the future isn't cancelled()), and those made after it
are refused. Either way, the parent task is cancelled: at once with cancel_futures=True, or else once the drain ends. On
the worker's own thread, cancel_futures=True cancels the calling task as it does any other. In a process forked from the
worker's, shutdown() raises WorkerForked, as submit() does.

Caveats
-------
- A task that ignores cancellation holds stopping up forever if it's the parent task or a call: see Interruption.
- Tasks that the parent task and calls spawn are abandoned as the loop closes (asyncio logs "Task was destroyed but it
  is pending!"), so the parent task should wait for whatever it spawns.
- Synchronous code blocking the worker's loop delays everything on it: calls start, and cancellations land, once it yields.
- KeyboardInterrupt or SystemExit from the parent task or a call crash the loop, as asyncio lets them escape any task. The crash is
  recorded as the stop (and logged, if it's the first), and re-raised on the worker's thread, and, as a copy, by the call
  that raised it, in its caller. Other calls in flight are interrupted, with only the teardown's 0.05s to clean up.
- The constructor waits for the worker's loop to start running, briefly holding up a loop it's called on. Make the client
  in the parent task, not before constructing the worker, perhaps in a caller's loop, which a client made there would be
  bound to.
- Once the worker has stopped, bridged functions are refused even to the parent task: its cleanup must use the client
  directly. And a drain waits for calls that the parent task's own tasks have in flight, as for any other.
- anyio's task groups cut their children's cleanup short as they're cancelled, unless it's shielded: as ever with anyio.
- A worker may stop just as a call is made, by expiry for one: callers that should carry on can retry on CleanStop, from
  the call or its result, with a fresh worker. On uvloop, which runs overdue timers ahead of callbacks queued while the
  loop was blocked, a call made while synchronous code blocks the worker's loop past its idle timeout is refused so,
  where asyncio's loop would take the call first.
- uvloop's executor shutdown, unlike asyncio's, ignores its timeout, so when the worker abandons executor threads (see
  Lifecycle), it's abandoned too: asyncio logs "Task was destroyed but it is pending!" for it, and it raises
  RuntimeError("Event loop is closed") on a thread of its own, once those threads finish.
- Workers run on daemon threads: one not shut down before the interpreter exits stops without cleaning up (the parent
  task's cleanup never runs). Shut workers down with atexit if that matters.
- A process forked with workers alive inherits copies of them, but not their threads, so none can be used there: see
  _fork_safety (unless they'd shut down already, they're Stopped there, why being a WorkerForked, which is a
  WorkerBroken, itself a WorkerStopped). That goes for a call that forked too, whose
  child runs on in a copy of the worker's loop (one asyncio no longer counts as running): the worker carries on only in
  the parent.
- If you perform blocking waits on this worker when completed or interrupted code inside the worker is garbage collected
  or during handling of its exception classes, you may break this class's invariant (this is generally also true for all
  other asyncio code in Python).
- Code that takes any concurrent.futures.Executor may notice how this one differs. A call's future can be cancelled
  while its job runs (cancelling the job), and never reports running(): so Executor.map() abandoning its results (on its
  timeout, an exception, or the caller no longer iterating) cancels calls in flight, not just those yet to start. Once
  the worker has stopped (perhaps of its own accord: by expiry, or the parent task ending), submit() and map() raise a
  WorkerStopped, not RuntimeError. And code that submits synchronous functions, as most code taking an executor does,
  has them run on the worker's loop, blocking it, then fail with TypeError, as what they return can't be awaited.
- How interrupted calls reach callers that await them relies on asyncio.futures._convert_future_exc() (which
  as_async_future() and asyncio.wrap_future() use) converting exactly concurrent.futures.CancelledError to asyncio's, and
  nothing else: a private detail of asyncio.
"""

# Some OUTDATED slop generated by an AI instructed to find issues in this repo, for triage

You're not wrong that the need is common. But the hard part was never calling a coroutine on another loop; that's five lines of `run_coroutine_threadsafe`. Look at what took the time: cancellation in both directions, what a synchronous timeout should mean, stop policy, refusal races, crashes, forks, context, the `TaskGroup` double cancel, and deciding what a waiter learns when things go wrong. Each of those is a policy choice, not a mechanism. A general-purpose library has to choose for everyone, and a wrong default is worse than none, so most stop at the easy part. The semantics are the product here, the way a connection pool's value is its failure handling, not `get()` and `put()`.

**Why nothing serves it well**
- **asyncio assumes one loop that owns the program.** Objects tied to a loop are meant to stay on it. The cross-thread tools (`run_coroutine_threadsafe`, `wrap_future`) date from 3.4–3.5. Structured cancellation (`TaskGroup`, `timeout()`, `uncancel()`) arrived in 3.11, and the two were never reconciled. That's why we kept leaning on private details: `_convert_future_exc` turning cancellations into asyncio's, no `context` parameter, no notion of "in flight". Anything built on them inherits the mismatch.
- **Each camp routes around the problem.**
  - An app that's fully async under ASGI creates its client at startup on its single loop, and the problem never arises.
  - A sync app uses sync clients.
  - Django is the big hybrid, and asgiref is built for the mirror image of your problem: *sync* singletons, like database connections, pinned to one thread (`thread_sensitive`).

  An async singleton in a hybrid app falls between the camps, so the experts in each never feel it.
- **Many async clients are written as applications, not libraries.** discord.py's `Client.run()` wants to own the entry point, nc_py_api binds to whatever loop first uses it, and OAuth helpers keep tokens private. The libraries create the problem and leave the lifecycle to you. Your `BackgroundDiscordClient` is the standard workaround: a daemon thread plus `run_coroutine_threadsafe`, with the lifecycle gaps we spent this session closing.
- **The right abstraction exists elsewhere under another name: an actor.** That means one owner of the stateful resource, a mailbox of calls, a reply per call, and supervision (drain, interrupt, expiry). It's routine in Erlang, Akka, Go (a goroutine owning state, with channels) and Rust (a tokio task plus a handle). Python never standardised an asyncio actor running in its own thread. anyio's `BlockingPortal` comes closest; it even has drain-or-cancel on stop. But it's a portal, not an actor: no idle expiry, no refusal semantics, no fork handling, and only basic semantics for cancelling from the caller's side.

**Your point 5 is real, and the mechanism is worse than thread hops.** In the installed asgiref 3.11.1, `async_to_sync` called from a plain thread creates a new single-thread `ThreadPoolExecutor` for that call. It then runs `asyncio.run()` in it, so each call gets a new thread and a new loop, both torn down afterwards. The thread hop isn't the expensive part. The real cost is that every loop-bound client gets rebuilt each time. Your NextCloud shim does exactly that by design: a new loop means a new adapter, a new connection pool, a new TLS handshake, and the server capabilities fetched again. A thread hop costs microseconds; a handshake plus auth costs tens of milliseconds. A long-lived worker loop pays those once. (Those figures are order-of-magnitude estimates; I didn't measure them.)

**Where your assumptions may be off**
- **Multiple processes.** This worker is one per *process*. Under Hypercorn with several workers, or with Celery or management commands alongside, you get one connection per process. The fork handling only stops a child from using the parent's copy. If an upstream's limit is truly cluster-wide (one socket, or two OAuth attempts a minute), an in-process actor doesn't enforce it. The usual answer is a separate process or service that owns the connection, such as the Discord bot running as its own service. That's also part of why no in-process library dominates: people with hard limits tend to go out of process.
- **Case 3 may be about data, not a connection.** If the token isn't tied to the connection, it's data, and caching it outside the process (in the database, say) handles the rate limit without keeping anything open. If it is tied to the connection, you're right.
- **Case 4 has conventional answers:**
  - Async pools such as asyncpg's and `psycopg_pool.AsyncConnectionPool` are tied to their loop too. Hybrid apps usually run one pool per loop, or use a sync driver with a thread-safe pool.
  - To make connections cheap, people put a pooler like PgBouncer in front, which moves the scarce resource out of the process again.
  - Routing a whole pool through one worker loop caps throughput at that one thread. That's fine for a single precious connection, but a bottleneck for a pool.

**So why does this feel like the state of the art?** Not because smarter people tried and failed. The problem sits where three things meet: a hybrid app, an async-only client, and a scarce connection. Each community routes around that intersection, and the difficulty is in policy, which general libraries avoid encoding. Your requirements were concrete enough to let you make choices a general library can't. That's also why this would publish well as a small library, "an asyncio actor in a thread with supervised shutdown", if you want it to outlive this repo. The docs you just wrote are most of its README.

-----

I've found a good set of reports so far: Django forum, asgiref, Celery, Prefect, httpx, redis-py, SQLAlchemy and blog posts. I'm reading the two most on-point pieces before writing up.

Next I'm checking whether Django supports ASGI lifespan events. Without them, Django has no sanctioned place to create and close a client on the server's loop.

One more check before writing up. My red-team argument depends on how asgiref behaves under ASGI, so I'm confirming it in the installed source.

**You're right that this is common. Where I was wrong is why nobody serves it.** It's common but rarely gets *named*. Most people hit it in places where a cheap workaround is tolerable. And the maintainers who own each piece have explicitly chosen a different answer. I read the starred items below in full; the rest I've seen only as search excerpts, so treat their details accordingly.

## Who else has hit this

**The same bug, reported against many libraries.** It always shows up as a "Future attached to a different loop" or "Event loop is closed" error:
- redis-py [#3351](https://github.com/redis/redis-py/issues/3351): an async pool kept in Django settings fails as soon as two requests run concurrently.
- httpx discussions [#2959](https://github.com/encode/httpx/discussions/2959) and [#3163](https://github.com/encode/httpx/discussions/3163).
- openai-python [#1254](https://github.com/openai/openai-python/issues/1254).
- channels_redis [#332](https://github.com/django/channels_redis/issues/332) and [#340](https://github.com/django/channels_redis/issues/340).
- conductor-oss [#507](https://github.com/conductor-oss/python-sdk/issues/507): a cached client whose pool is bound to a dead loop doesn't report itself as closed, so it gets reused.
- The asynch ClickHouse driver's [PR #21](https://github.com/Leechael/asynch/pull/21): Celery's `async_to_sync` hands pooled connections to a new loop on every run, so the PR reconnects when the running loop has changed.
- SQLAlchemy [#5626](https://github.com/sqlalchemy/sqlalchemy/issues/5626), pytest-asyncio [#38](https://github.com/pytest-dev/pytest-asyncio/issues/38), and Prefect [#13181](https://github.com/PrefectHQ/prefect/issues/13181).

**Performance and the bridging layer itself:**
- ★ [Django Forum, "A datapoint for asgiref performance"](https://forum.djangoproject.com/t/a-datapoint-for-asgiref-performance/34633), 2024: 3–5 ms per wrapped call in production. The poster later said only `sync_to_async` was measured. Adam Johnson replied "I agree that is slow"; there's no fix yet.
- asgiref [#194](https://github.com/django/asgiref/issues/194): `force_new_loop` "spins a new thread and event loop every time". It's also a hang after fork, the same hazard we handled.
- I confirmed in the installed asgiref 3.11.1 that `async_to_sync` called from a plain thread starts a new single-thread executor and runs `asyncio.run()` in it, so each call gets a fresh thread and loop.

**Celery, a big source of extra loops:**
- [#3884](https://github.com/celery/celery/issues/3884), the long-standing request for asyncio tasks.
- [Discussion #9058](https://github.com/celery/celery/discussions/9058): keep one loop thread so async resources aren't rebuilt on every task.
- [PR #10634](https://github.com/celery/celery/pull/10634): one shared loop per worker process.
- [Hatchet's "problems with Celery"](https://hatchet.run/blog/problems-with-celery).

**People who built your design:**
- ★ [death and gravity, "Running async code from sync code"](https://death.andgravity.com/asyncio-bridge), 2023, updated 2025. Same journey and same answer: a loop in a thread plus `run_coroutine_threadsafe`, published as `asyncio-thread-runner`. Signals and subprocesses are given up, and the conclusion is, verbatim, "I guess?"
- fsspec's ["fsspecIO" thread](https://filesystem-spec.readthedocs.io/en/latest/_modules/fsspec/asyn.html). s3fs and gcsfs serve every caller through one shared loop thread, with special handling after fork. This is the largest production instance I found.
- ★ Prefect: [`run_coro_as_sync`](https://docs.prefect.io/v3/api-ref/python/prefect-utilities-asyncutils) runs on a shared loop thread. Then in [#15008](https://github.com/PrefectHQ/prefect/issues/15008) they set out to migrate away from it, citing "complex event loop and threading logic that risks performance degradation and more difficult to inspect failure modes". The issue was closed in February 2026 and reopened in May.
- [unsync](https://pypi.org/project/unsync/) (an ambient loop thread), [asyncio-loop-local](https://github.com/t184256/asyncio-loop-local) (the opposite strategy: one singleton per loop), and anyio's portals. I couldn't confirm the docs for anyio's `BlockingPortalProvider`, which shares one portal across threads.

**Proposals to bend asyncio itself:**
- nest_asyncio was [archived in 2024](https://github.com/erdewit/nest_asyncio/) and forked as [nest-asyncio2](https://github.com/larsoner/nest-asyncio2).
- A re-entrant `asyncio.run()` was proposed in [CPython PR #93338](https://github.com/python/cpython/pull/93338), and earlier in [bpo-33523](https://bugs.python.org/issue33523) ("large interconnected codebases… in transition") and [bpo-22239](https://bugs.python.org/issue22239).
- [CPython PR #141696](https://github.com/python/cpython/pull/141696) (2025) is still fixing how `_chain_future` handles cancellation for `run_coroutine_threadsafe`. The standard library's bridge is still being repaired.

**The official stances:**
- The httpx maintainers ([#3163](https://github.com/encode/httpx/discussions/3163)) want to move toward allowing only `async with` clients, so that clients "aren't used across different invocations of an event loop".
- ★ The new [Python 3.15 page on asyncio and free threading](https://docs.python.org/3/library/asyncio-threading.html) says each thread "should have its own event loop which should not be shared across threads."
- [SQLAlchemy's asyncio docs](https://docs.sqlalchemy.org/en/20/orm/extensions/asyncio.html) cover multiple loops by disposing the engine or using `NullPool`.

**Wider commentary:** Armin Ronacher's [async posts](https://lucumr.pocoo.org/tags/async), Rob Blackbourn's [ASGI event loop gotcha](https://rob-blackbourn.medium.com/asgi-event-loop-gotcha-76da9715e36d), the [discord.py FAQ](https://discordpy.readthedocs.io/en/async/faq.html) (which recommends `run_coroutine_threadsafe`), and [django-asgi-lifespan](https://pypi.org/project/django-asgi-lifespan/0.2.0/), a third-party add-on because Django core doesn't support ASGI lifespan events.

## Red-teaming my essay

1. **"The intersection is small" was wrong.** It's large; it just has no name. Every instance gets reported as a library-specific error string. The fix offered in reply is almost always "create the client per call, or per loop", and the thread closes. Without a shared name for the problem, demand never collects around one tool. That's the factor I missed.
2. **The ecosystem has decided against long-lived cross-loop singletons, not merely neglected them.** httpx, SQLAlchemy and the asyncio docs all push for each client's lifetime to be explicit and confined to one loop. A tool like yours works against the sanctioned model, which is why it feels like the cutting edge: it's off the paved road on purpose.
3. **Under ASGI, Django already hides much of the problem, which I hadn't checked before.** I verified in the asgiref source: when `sync_to_async` runs a sync view in a thread, it records the server's loop for that thread. An `async_to_sync` call from that thread then runs on that loop. So the whole request path in a worker shares one loop, and a client created lazily on it mostly works. New loops come from elsewhere:
   - threads asgiref didn't start;
   - management commands and cron jobs;
   - Celery tasks;
   - tests;
   - startup code such as `AppConfig.ready()`, which runs outside any loop;
   - WSGI deployments.

   In those places, rebuilding the client every time is tolerable, unless connections are scarce. That's exactly your niche. So the pain is common, but severe pain is rarer, and severity is what produces libraries.
4. **Django gives you no place to own the singleton.** There are no lifespan events, so there's no sanctioned hook to create a client on the server's loop and close it on shutdown, even where that would work. Celery likewise has no loop of its own. Each owner (asyncio, Django, Celery, the client libraries) can reasonably say the hybrid case is someone else's problem.
5. **A large project tried the generic version and is retreating.** That's Prefect. Part of the trouble is that a general library must also handle being called from *inside* the loop it manages, and the hidden switch between sync and async behaviour is what bit Prefect. Your version avoids that by always returning a future, which is a deliberate trade a general library may not be able to make.
6. **Performance: your complaint holds up, and my earlier estimate was too rosy.** I said a thread hop costs microseconds. The real-world measurement for an asgiref crossing is milliseconds. On top of that, a fresh thread and loop per call defeats connection reuse.
7. **Assumptions worth questioning, as questions rather than verdicts:**
   - **Case 2:** does nc_py_api have a sync client you could share across threads, with one thread-safe connection pool and no loops at all? I believe it does, but I haven't checked.
   - **Case 1:** do the webapp's Discord reads need the gateway (the persistent websocket) at all, or would plain REST calls with the bot token do? Listing members still needs the privileged intent.
   - **Case 3:** is the OAuth token tied to the connection, or could it simply be cached and shared?
   - **Separate processes:** management commands, Celery and cron are all separate processes, each with its own singleton. If any upstream limit is really per account rather than per process, that matters.

**Net:** your constraints really are common, and the evidence is scattered across dozens of single-library threads. No library serves the niche because the problem has no name, upstream policy discourages the pattern, Django's request path hides it, and the one big project that generalised it is backing away. That doesn't make your solution wrong. It means it's a policy-heavy actor that each project ends up building privately. fsspec, Prefect and the death and gravity author all built one. Yours has the most complete stop, fork and crash handling of any I found.