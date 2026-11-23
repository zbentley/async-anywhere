# When to use `AwaitableFuture`

Use `AwaitableFuture` if you have code which heavily uses `asyncio.Future` or `concurrent.futures.Future` directly, and you want a friendlier (and more performant in many cases) API. 

`concurrent.futures.Future` objects in the stdlib are recommended only for internal use; if you've ever wanted to create and pass those around as part of your regular business logic, then `AwaitableFuture` may meet your needs. 

`AwaitableFuture` is well-suited for modelling "coalescing" waits: when multiple async or threaded routines want to wait on the result of a single computation, or when code wants to provide all callers with cached results of a computation that may or may not be complete.

In general:
- If you've ever wanted to wait on the same result to arrive from multiple threads and `async`/`await` routines simultaneously, and are tired of chaining futures/callbacks by hand, `AwaitableFuture` may be appropriate for you.
- Exclusively synchronous code, with no `async`/`await`s anywhere: `AwaitableFuture` may be useful for data modelling, and is likely more performant and friendlier than `concurrent.futures.Future`.
- Exclusively asynchronous code on a single, long-lived event loop: `AwaitableFuture` is probably _not_ the right tool; using a regular `asyncio.Future` is likely to be more performant and more compatible with other stdlib tools.
- Asynchronous code with multiple event loops that need to share data: `AwaitableFuture` provides a convenient way to avoid loop-mismatch errors.
