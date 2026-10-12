from __future__ import annotations

from inspect import isabstract


project = "async-anywhere"
author = "Zac Bentley"
copyright = "2026, Zac Bentley"  # noqa: A001
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    'sphinx.ext.intersphinx',
    "sphinx_click",
    "myst_parser",
    "pytest_doctestplus.sphinx.doctestplus",
]
autodoc_typehints = "both"
intersphinx_mapping = {'python': ('https://docs.python.org/3', None)}
autodoc_class_signature = "separated"
autodoc_default_options = {
    'show-inheritance': True,
    'member-order': 'bysource',
    'inherited-members': True,
    'special-members': '__call__,__enter__,__exit__,__aenter__,__aexit__',
    'undoc-members': True,
    'exclude-members': '__weakref__,__new__'
}
html_theme = "furo"


def _colliding_module_name(cls):
    tp, tep = ('typing.Protocol', 'typing_extensions.Protocol')
    if '{}.{}'.format(cls.__module__, cls.__name__) in (tp, tep):
        return ':py:class:`{tp}<python:{tp}>` (or ``{tep}`` if ``{tp}`` is not available)'.format(tp=tp, tep=tep)

    disambiguate = ['threading', 'asyncio', 'anyio', 'trio', 'curio', 'concurrent.futures']
    if cls.__name__ in ('TimeoutError', 'CancelledError'):
        disambiguate.append('builtins')
    for mod in disambiguate:
        if cls.__module__.startswith(mod):
            name = cls.__name__
            if mod == "builtins":
                return ':py:class:`{}.{}<python:{}>`'.format(mod, name, name)
            return ':py:class:`{}.{}`'.format(mod, name)
    return cls

def process_bases_hook(*args):

    rv = []
    tail = set()
    for base in args[-1]:
        if base.__module__.startswith('async_anywhere'):
            rv.append(base)
        else:
            rv.append(_colliding_module_name(base))

    args[-1][:] = rv + sorted(tail, key=str)


def setup(app):
    # https://www.sphinx-doc.org/en/master/usage/extensions/autodoc.html#event-autodoc-process-bases
    app.connect('autodoc-process-bases', process_bases_hook)
