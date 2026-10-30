``async_anywhere``
================

API Usage Guidelines
--------------------

* Per Python conventions, using ``_private`` members of modules or classes in this package is unsupported and can cause undefined behavior.
    * The same is true for modules with ``_private`` names. Do not import symbols from those modules, even if the symbols' names are public. If your IDE auto-import functionality generates imports from those modules, update your code to import from public modules instead; public symbols should be re-exported via :py:obj:`__all__` (CLAUDE: link to stdlib docs for this).  If IDE generated auto-imports of needed symbols frequently resolve to ``_private`` submodules and cause issues, file an issue.
    * A ``_private`` member with a docstring is still private unless documented otherwise.
* Some things provided by this package may be :py:mod:`dataclasses` or `attrs <https://www.attrs.org/en/stable/>`_-based classes (CLAUDE: make 'attrs' a code-formatted str like dataclasses is). Whether an exported class does or does not do this is an implementation detail and not part of its public contract; do not rely on e.g. an exported class using ``@dataclass`` internally.


Modules
-------

.. toctree::
   :maxdepth: 4

   async_anywhere
   async_anywhere.futures
   async_anywhere.executors
   async_anywhere.misc
