from __future__ import annotations

import functools
import importlib
import inspect
import sys
import typing
from abc import ABC

import attrs
from docutils import nodes
from sphinx import addnodes
from sphinx.domains.python import PyClasslike
from sphinx.pycode import ModuleAnalyzer
from sphinx.util.typing import restify


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
intersphinx_mapping = {
    'python': ('https://docs.python.org/3', None),
    'trio': ('https://trio.readthedocs.io/en/stable', None),
    'attrs': ('https://www.attrs.org/en/stable', None),
    'uvloop': ('https://uvloop.readthedocs.io', None),  # Its pages only: it documents no Python objects
}
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
nitpicky = True  # Warn about every cross-reference that doesn't resolve, which Sphinx is otherwise silent about


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
        # Split, as a generic alias (Future[T]) passes for its origin class, so would lose its [T]
        origin, params = typing.get_origin(base) or base, typing.get_args(base)
        if origin is typing.Generic:  # Never listed: PEP 695 classes get it implicitly
            continue
        ref = origin if origin.__module__.startswith('async_anywhere') else _colliding_module_name(origin)
        if len(params) > 0:
            # Type params as plain text: Sphinx has nothing to link PEP 695 ones to, and takes them for typing's own
            ref = '{}\\ [{}]'.format(restify(ref, 'smart'), ', '.join(
                p.__name__ if isinstance(p, (typing.TypeVar, typing.ParamSpec, typing.TypeVarTuple)) else restify(p, 'smart')
                for p in params
            ))
        rv.append(ref)

    args[-1][:] = rv


# Stdlib classes, by every name Sphinx may give them (their private modules' included), and their public ones
_STDLIB_PUBLIC_NAMES = {
    '_asyncio.Future': 'asyncio.Future',
    'asyncio.futures.Future': 'asyncio.Future',
    'asyncio.Future': 'asyncio.Future',
    'concurrent.futures._base.Future': 'concurrent.futures.Future',
    'concurrent.futures.Future': 'concurrent.futures.Future',
    'asyncio.events.AbstractEventLoop': 'asyncio.AbstractEventLoop',
}


def qualify_stdlib_names(app, doctree):
    # Every reference to these is shown and linked by its full public name, so the two Futures are never confused
    for xref in list(doctree.findall(addnodes.pending_xref)):
        target = _STDLIB_PUBLIC_NAMES.get(xref['reftarget'])
        if target is None or xref.get('refdomain') != 'py':
            continue
        xref['reftarget'] = target
        for text in list(xref.findall(nodes.Text)):
            text.parent.replace(text, nodes.Text(target))


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
    app.connect('doctree-read', qualify_stdlib_names)
    # Separate registrations of the same class: overriding one leaves the other as stock PyClasslike
    app.add_directive_to_domain('py', 'class', AbstractAwarePyClasslike, override=True)
    app.add_directive_to_domain('py', 'exception', AbstractAwarePyClasslike, override=True)
    # After furo's own html-page-context handler (default priority 500), which builds furo_navigation_tree
    app.connect('html-page-context', expand_sidebar, priority=501)
