``async_anywhere``
==================

API Usage Guidelines
--------------------

* Per Python conventions, using ``_private`` members of modules or classes in this package is unsupported and can cause undefined behavior.
    * The same is true for modules with ``_private`` names. Do not import symbols from those modules, even if the symbols' names are public. If your IDE auto-import functionality generates imports from those modules, update your code to import from public modules instead; public symbols should be re-exported via |all|_.  If IDE generated auto-imports of needed symbols frequently resolve to ``_private`` submodules and cause issues, file an issue.
    * A ``_private`` member with a docstring is still private unless documented otherwise.
* Some things provided by this package may be :py:mod:`dataclasses` or |attrs|_-based classes. Whether an exported class does or does not do this is an implementation detail and not part of its public contract; do not rely on e.g. an exported class using ``@dataclass`` internally.

API
---

.. automodule:: async_anywhere
    :members:

.. toctree::
    :maxdepth: 4
    :glob:

    *
    executors/index

.. |attrs| replace:: ``attrs``
.. _attrs: https://www.attrs.org/en/stable/
.. |all| replace:: ``__all__``
.. _all: https://docs.python.org/3/tutorial/modules.html#importing-from-a-package
