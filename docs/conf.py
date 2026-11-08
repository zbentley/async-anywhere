from __future__ import annotations

import functools
import importlib
import inspect
import sys
from abc import ABC

import attrs
from sphinx.domains.python import PyClasslike
from sphinx.pycode import ModuleAnalyzer


project = "async-anywhere"
author = "Zac Bentley"
copyright = "2026, Zac Bentley"  # noqa: A001
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    'sphinx.ext.intersphinx',
    "myst_parser",
    "pytest_doctestplus.sphinx.doctestplus",
]
autodoc_typehints = "both"
intersphinx_mapping = {'python': ('https://docs.python.org/3', None)}
autodoc_class_signature = "separated"
autodoc_default_options = {
    'show-inheritance': True,
    'member-order': 'bysource',
    # 'inherited-members': True,
    # 'special-members': '__call__,__enter__,__exit__,__aenter__,__aexit__',
    # 'undoc-members': True,
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
    for base in args[-1]:
        if base.__module__.startswith('async_anywhere'):
            rv.append(base)
        else:
            rv.append(_colliding_module_name(base))

    args[-1][:] = rv


def attrs_init_param_docs(app, what, name, obj, options, lines):  # noqa: PLR0917
    # Feed attrs fields' attribute docstrings (string after, or #: before) into the generated __init__'s :param <alias>: fields
    # Python functions, as attrs makes: not inherited C slot wrappers (BaseException.__init__), which lack __module__
    if what != "method" or not inspect.isfunction(obj) or obj.__name__ != "__init__":
        return
    owner = functools.reduce(getattr, obj.__qualname__.split(".")[:-1], sys.modules[obj.__module__])
    if not attrs.has(owner) or owner.__init__ is not obj:
        return
    attr_docs = ModuleAnalyzer.for_module(owner.__module__).find_attr_docs()
    for a in attrs.fields(owner):
        doc = attr_docs.get((owner.__qualname__, a.name), [""])
        if a.init and doc[0]:
            lines.extend(["", f":param {a.alias}: {doc[0]}", *("   " + line for line in doc[1:])])


def executor_map_doc(app, what, name, obj, options, lines):  # noqa: PLR0917
    # EventLoopExecutor inherits map() unchanged: point at the base's docs rather than repeating them
    if name == "async_anywhere.executors.EventLoopExecutor.map":
        lines[:] = ["See :py:meth:`concurrent.futures.Executor.map`."]


class AbstractAwarePyClasslike(PyClasslike):
    # Autodoc never emits :abstract: for classes: add it for ABC-marked (ABC in own bases) or inspect.isabstract classes
    def run(self):
        qualname = [*filter(None, [self.env.ref_context.get('py:class')]), self.arguments[0].split('(')[0].strip()]
        try:
            obj = functools.reduce(
                getattr, '.'.join(qualname).split('.'), importlib.import_module(self.env.ref_context.get('py:module'))
            )
        except (ImportError, AttributeError, TypeError):
            obj = None
        if inspect.isclass(obj) and (ABC in obj.__bases__ or inspect.isabstract(obj)):
            self.options['abstract'] = True
        return super().run()


def expand_sidebar(app, pagename, templatename, context, doctree):  # noqa: PLR0917
    # Furo only pre-checks the expand toggles of the current page's ancestors: check every one so the nav starts fully expanded
    context["furo_navigation_tree"] = context["furo_navigation_tree"].replace('role="switch"', 'checked="" role="switch"')


def setup(app):
    # https://www.sphinx-doc.org/en/master/usage/extensions/autodoc.html#event-autodoc-process-bases
    app.connect('autodoc-process-bases', process_bases_hook)
    app.connect('autodoc-process-docstring', attrs_init_param_docs)
    app.connect('autodoc-process-docstring', executor_map_doc)
    # Separate registrations of the same class: overriding one leaves the other as stock PyClasslike
    app.add_directive_to_domain('py', 'class', AbstractAwarePyClasslike, override=True)
    app.add_directive_to_domain('py', 'exception', AbstractAwarePyClasslike, override=True)
    # After furo's own html-page-context handler (default priority 500), which builds furo_navigation_tree
    app.connect('html-page-context', expand_sidebar, priority=501)
