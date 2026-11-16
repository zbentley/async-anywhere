API Reference
=============

This is the documentation reference for ``async_anywhere``, a Python module that makes interoperation between arbitrary combinations
of ``async``/``await`` event loops and synchronous code easier.

General Information
-------------------

This module is implemented in pure Python. The source code is `available on GitHub <https://github.com/zbentley/async-anywhere>`__.

Some ``async_anywhere`` functionality interoperates with other async Python tools (e.g. :doc:`uvloop <uvloop:index>`, :doc:`Trio <trio:index>` or :doc:`anyio <anyio:index>`). Such tools are not listed as direct or optional dependencies of this module. When deciding whether or not to integrate with other tools, **this module will never** ``import`` **them directly**, as many Python modules have global side effects at import time. ``async_anywhere`` will only use them if they have already been imported when related functionality is invoked.

.. admonition:: Trio Support

    Several APIs provided by this module (e.g. :py:obj:`AwaitableFuture` and :py:obj:`EventLoopExecutor`) can be used with :doc:`Trio <trio:index>`. While possible, Trio support has some sharp edges; thoroughly read associated API documentation before using this module's facilities with Trio. Improved Trio support is actively being developed for a future release. If you find any bugs or usability issues when using Trio with ``async_anywhere``, please :ref:`file an issue <reporting_issues>`!

.. _reporting_issues:

Reporting Issues
----------------

Bug reports and feature requests can be filed via `GitHub Issues <https://github.com/zbentley/async-anywhere/issues>`__.

API Usage Guidelines
--------------------

* Per Python conventions, using ``_private`` members of modules or classes in this package is unsupported and can cause undefined behavior.
    * The same is true for modules with ``_private`` names. Do not import symbols from those modules, even if the symbols' names are public. If your IDE auto-import functionality generates imports from those modules, update your code to import from public modules instead; public symbols should be re-exported via |all|_. If IDE-generated auto-imports of needed symbols frequently resolve to ``_private`` submodules and cause issues, :ref:`file an issue <reporting_issues>`.
    * A ``_private`` member with a docstring is still private unless documented otherwise.
* Some things provided by this package may be :py:obj:`dataclasses` or :py:mod:`attrs`-based classes. Whether an exported class does or does not do this is an implementation detail and not part of its public contract; do not rely on e.g. an exported class using ``@dataclass`` internally.
* Some APIs in this package may be annotated with a type lower than the one they return. Always write code against the annotated type, not the concrete type.

Module Index
------------

.. automodule:: async_anywhere
    :members:

.. toctree::
    :maxdepth: 4
    :glob:

    *
    executors/index
    futures/index
    state_tracker/index

.. |all| replace:: ``__all__``
.. _all: https://docs.python.org/3/tutorial/modules.html#importing-from-a-package
