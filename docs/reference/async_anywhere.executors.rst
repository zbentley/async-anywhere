``async_anywhere.executors``
============================

.. automodule:: async_anywhere.executors
   :members:
   :exclude-members: EventLoopExecutor

.. autoclass:: async_anywhere.executors.EventLoopExecutor
   :members:
   :exclude-members: __init__, __weakref__, __new__

   .. Directive content renders after the class docstring and before its members, so __init__ comes first
   .. automethod:: __init__

Fork Safety
-----------

.. automodule:: async_anywhere.executors._fork_safety

Glossary
--------

.. glossary::

    executor, worker
        An instance of EventLoopExecutor.

    "code in a worker", "code in an executor"
        User-supplied Python code that executes in the captive thread of an EventLoopExecutor, including code in the user-supplied ``main`` constructor parameter, code passed to the worker via ``executor.submit()``, and code decorated with ``@executor.bridge``.

    interrupt, cancel [an EventLoopExecutor job]
        Call `.cancel()` on the asyncio.Task running a given job in an executor. Calling `AwaitableFuture.cancel()` on the futures returned from `EventLoopExecutor.submit()` cancels their jobs. Certain executor stop conditions also cancel running jobs.