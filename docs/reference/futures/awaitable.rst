``AwaitableFuture``
===================

.. autoclass:: async_anywhere.futures.AwaitableFuture
    :members:
    :exclude-members: AnyEventLoop, BlockingNotAllowed, __weakref__, __new__

    .. Sentinels: functions at runtime (BlockingChecker calls them), so documented by hand, as the values users pass
    .. py:attribute:: AnyEventLoop

        Sentinel for the ``prevent_blocking_wait_when`` parameter of :py:meth:`AwaitableFuture.__init__`: raises
        :py:class:`.WouldBlock` instead of blocking if any event loop or :doc:`Trio <trio:index>` task is running.

    .. py:attribute:: BlockingNotAllowed

        Sentinel for the ``prevent_blocking_wait_when`` parameter of :py:meth:`AwaitableFuture.__init__`: raises
        :py:class:`.WouldBlock` instead of blocking in all cases.
