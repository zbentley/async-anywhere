# async-anywhere
Python tools for invoking async code across different threads and event loops

# TODOs

captive worker todos;
- decorate loop-only methods
- add debug flag that causes decorator to assert, and runs loop in dbg
- _run() interrupting itself isn't cancelled. If _run() calls shutdown(interrupt=True) itself, shutdown() raises WorkerStopped into it, but _run() isn't cancelled.
- cope w/ interrupted jobs whose cleanup async-hangs
- cope with (really: yell about) interrupted jobs whose cleanup SYNC hangs
- worker *is* a state machine
- Log never-awaited on bridge futures


async_anywhere general:
- Future chainer that's customizable
- Dual-future that can be awaited on anywhere, with as_async_future.
- Async state machine
- Volunteer cache (probably sans-asgiref threading)

now:
attrify
future caching
fast tests
torture tests