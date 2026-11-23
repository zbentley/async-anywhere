(future_fork_safety)=

# `AwaitableFuture` Fork Safety

Stdlib Future-like primitives have several problems related to `fork(2)`:

- In child processes created by [`os.fork()`](https://docs.python.org/3/library/os.html#os.fork),
  [`concurrent.futures.Future`](https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.Future)s
  can easily become deadlocked and/or corrupt later calls to
  [`concurrent.futures.wait()`](https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.wait) or
  [`concurrent.futures.as_completed()`](https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.as_completed).
- Neither [`asyncio.Future`](https://docs.python.org/3/library/asyncio-future.html#asyncio.Future) nor any other
  awaitable primitive allows waits to occur on the far side of a forked process boundary.
- Done callbacks added via `add_done_callback` can run twice (once in the parent and once in the child) when
  added to futures that are interacted with on both sides of a `fork(2)`.

[`AwaitableFuture`](https://async-anywhere.readthedocs.io/en/latest/reference/futures/awaitable.html#async_anywhere.futures.AwaitableFuture) addresses all of those issues:

- `AwaitableFuture`s will never deadlock or break `concurrent.futures` code after fork.
- `AwaitableFuture`s can be blocked on or `await`ed from any process. However, results aren't
  transmitted across process boundaries, so, depending on what you're using futures for, results may never
  arrive. Still, a cancellable `await` or time-outable blocking wait are much preferable to an uninterruptible
  deadlock after `fork(2)` caused by `concurrent.futures.Future`'s internal condition lock corruption.
- `AwaitableFuture`s can be configured to cancel themselves immediately when forked into a child process,
  to prevent easy-to-make mistakes.
- [`AwaitableFuture.add_done_callback()`](https://async-anywhere.readthedocs.io/en/latest/reference/futures/awaitable.html#async_anywhere.futures.AwaitableFuture.add_done_callback) can configure callbacks to always (or never) run in forked child
  processes.
