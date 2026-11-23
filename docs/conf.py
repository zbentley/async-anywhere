from __future__ import annotations

import functools
import importlib
import inspect
import typing
from abc import ABC

from docutils import nodes
from sphinx import addnodes
from sphinx.domains.python import PyClasslike
import sphinx.util.typing
from sphinx.util.typing import restify


project = "async-anywhere"
author = "Zac Bentley"
copyright = "2026, Zac Bentley"  # noqa: A001
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx_autodoc_typehints",
    "sphinx.ext.intersphinx",
    "myst_parser",
    "pytest_doctestplus.sphinx.doctestplus",
]
myst_enable_extensions = ["alert"]  # GitHub-style "> [!NOTE]" admonitions, so .md pages render in both places
autodoc_typehints = "both"
# sphinx_autodoc_typehints strips signature annotations by default: keep them
typehints_use_signature = True
typehints_use_signature_return = True
intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
    "trio": ("https://trio.readthedocs.io/en/stable", None),
    "attrs": ("https://www.attrs.org/en/stable", None),
    "uvloop": ("https://uvloop.readthedocs.io", None),  # Its pages only: it documents no Python objects
    "anyio": ("https://anyio.readthedocs.io/en/stable", None),
}
html_static_path = ["_static"]
html_css_files = ["custom.css"]
autodoc_class_signature = "separated"
autodoc_preserve_defaults = True  # Defaults as written: <function ...> reprs break signature parsing (sphinx#13340)
autodoc_default_options = {
    "show-inheritance": True,
    "member-order": "bysource",
    # 'inherited-members': True,
    # 'special-members': '__call__,__enter__,__exit__,__aenter__,__aexit__',
    # 'undoc-members': True,
    "exclude-members": "__weakref__,__new__",
}
html_theme = "furo"
nitpicky = True  # Warn about every cross-reference that doesn't resolve, which Sphinx is otherwise silent about
nitpick_ignore = [("py:exc", "<Various Exceptions>")]  # Raises: placeholder for "whatever the referenced method raises"
# Type params: Sphinx has nothing to link them to
nitpick_ignore_regex = [(r"py:(class|obj)", r"(.*\.)?[PT](\.args|\.kwargs)?")]


def _colliding_module_name(cls):
    tp, tep = ("typing.Protocol", "typing_extensions.Protocol")
    if "{}.{}".format(cls.__module__, cls.__name__) in (tp, tep):
        return ":py:class:`{tp}<python:{tp}>` (or ``{tep}`` if ``{tp}`` is not available)".format(tp=tp, tep=tep)

    disambiguate = ["threading", "asyncio", "anyio", "trio", "curio", "concurrent.futures"]
    if cls.__name__ in ("TimeoutError", "CancelledError"):
        disambiguate.append("builtins")
    for mod in disambiguate:
        if cls.__module__.startswith(mod):
            name = cls.__name__
            if mod == "builtins":
                return ":py:class:`{}.{}<python:{}>`".format(mod, name, name)
            return ":py:class:`{}.{}`".format(mod, name)
    return cls


# PEP 695 type params claim __module__ 'typing', so Sphinx takes them for typing's own and strips them (Awaitable[T] -> Awaitable)
sphinx.util.typing.is_system_TypeVar = (
    lambda typ: isinstance(typ, typing.TypeVar) and getattr(typing, typ.__name__, None) is typ
)


def process_bases_hook(*args):

    rv = []
    for base in args[-1]:
        # Split, as a generic alias (Future[T]) passes for its origin class, so would lose its [T]
        origin, params = typing.get_origin(base) or base, typing.get_args(base)
        if origin in (typing.Generic, object):  # Never listed: PEP 695 classes get it implicitly
            continue
        ref = origin if origin.__module__.startswith("async_anywhere") else _colliding_module_name(origin)
        if len(params) > 0:
            # Type params as plain text: Sphinx has nothing to link PEP 695 ones to, and takes them for typing's own
            ref = "{}\\ [{}]".format(
                restify(ref, "smart"),
                ", ".join(
                    (
                        p.__name__
                        if isinstance(p, (typing.TypeVar, typing.ParamSpec, typing.TypeVarTuple))
                        else restify(p, "smart")
                    )
                    for p in params
                ),
            )
        rv.append(ref)

    args[-1][:] = rv


# Stdlib classes, by every name Sphinx may give them (their private modules' included), and their public ones
_STDLIB_PUBLIC_NAMES = {
    "_asyncio.Future": "asyncio.Future",
    "asyncio.futures.Future": "asyncio.Future",
    "asyncio.Future": "asyncio.Future",
    "_asyncio.Task": "asyncio.Task",
    "asyncio.Task": "asyncio.Task",
    "concurrent.futures._base.Future": "concurrent.futures.Future",
    "concurrent.futures.Future": "concurrent.futures.Future",
    "asyncio.events.AbstractEventLoop": "asyncio.AbstractEventLoop",
}


def qualify_stdlib_names(app, doctree):
    # Every reference to these is shown and linked by its full public name, so the two Futures are never confused
    for xref in list(doctree.findall(addnodes.pending_xref)):
        target = _STDLIB_PUBLIC_NAMES.get(xref["reftarget"])
        if target is None or xref.get("refdomain") != "py":
            continue
        xref["reftarget"] = target
        for text in list(xref.findall(nodes.Text)):
            text.parent.replace(text, nodes.Text(target))


# .md pages link to the hosted docs, so that their links work on GitHub and PyPI too
_HOSTED_DOCS = "https://async-anywhere.readthedocs.io/en/latest/"


def localize_hosted_links(app, doctree, docname):
    # Point links to the hosted docs at this build's own pages, so each version's docs link within that version
    for ref in doctree.findall(nodes.reference):
        uri = ref.get("refuri", "")
        if not uri.startswith(_HOSTED_DOCS):
            continue
        path, _, anchor = uri.removeprefix(_HOSTED_DOCS).partition("#")
        target = path.removesuffix(".html") or "index"
        if target not in app.env.all_docs or (
            len(anchor) > 0
            and not any(anchor in node["ids"] for node in app.env.get_doctree(target).findall(nodes.Element))
        ):
            raise ValueError(f"{docname}: hosted docs link has no target in this build: {uri}")
        ref["refuri"] = app.builder.get_relative_uri(docname, target) + uri.removeprefix(_HOSTED_DOCS + path)
        ref["internal"] = True


def auto_dot(app, env, node, contnode):
    # Retry an unresolved bare Python ref as though dotted: suffix search, with the domain's own ambiguity warning
    if node["refdomain"] != "py" or node.get("refspecific"):
        return None
    node["refspecific"] = True
    return env.domains.python_domain.resolve_xref(
        env, node["refdoc"], app.builder, node["reftype"], node["reftarget"], node, contnode
    )


def hide_private_parameters(app, obj, bound_method):
    # Private (_-prefixed) parameters, like EventLoopExecutor's _blocking_checker, aren't part of the documented API.
    # After autodoc_preserve_defaults has set __signature__, which autodoc's inspect.signature() call honors next.
    sig = inspect.signature(obj)
    if any(name.startswith("_") for name in sig.parameters):
        obj.__signature__ = sig.replace(parameters=[p for p in sig.parameters.values() if not p.name.startswith("_")])


def executor_map_doc(app, what, name, obj, options, lines):  # noqa: PLR0917
    # EventLoopExecutor inherits map() unchanged: point at the base's docs rather than repeating them
    if name == "async_anywhere.executors.EventLoopExecutor.map":
        lines[:] = ["See :py:meth:`concurrent.futures.Executor.map`."]


class AbstractAwarePyClasslike(PyClasslike):
    # Autodoc never emits :abstract: for classes: add it for ABC-marked (ABC in own bases) or inspect.isabstract classes
    def run(self):
        qualname = [*filter(None, [self.env.ref_context.get("py:class")]), self.arguments[0].split("(")[0].strip()]
        try:
            obj = functools.reduce(
                getattr,
                ".".join(qualname).split("."),
                importlib.import_module(self.options.get("module", self.env.ref_context.get("py:module"))),
            )
        except (ImportError, AttributeError, TypeError):
            obj = None
        if inspect.isclass(obj) and (ABC in obj.__bases__ or inspect.isabstract(obj)):
            self.options["abstract"] = True
        if len(self.content) > 0 and self.content[0].strip() == "Bases:":  # Sphinx emits it even when no bases remain
            self.content.trim_start(1)  # Not del: that also deletes from autodoc's whole-page line list
            self.content_offset += 1
        return super().run()


def expand_sidebar(app, pagename, templatename, context, doctree):  # noqa: PLR0917
    # Furo only pre-checks the expand toggles of the current page's ancestors: check every one so the nav starts fully expanded
    context["furo_navigation_tree"] = context["furo_navigation_tree"].replace(
        'role="switch"', 'checked="" role="switch"'
    )


def setup(app):
    # https://www.sphinx-doc.org/en/master/usage/extensions/autodoc.html#event-autodoc-process-bases
    # Before sphinx_autodoc_typehints' handler (default 500), which turns every base into a string
    app.connect("autodoc-process-bases", process_bases_hook, priority=499)
    app.connect("autodoc-before-process-signature", hide_private_parameters)
    app.connect("autodoc-process-docstring", executor_map_doc)
    app.connect("doctree-read", qualify_stdlib_names)
    app.connect("doctree-resolved", localize_hosted_links)
    app.connect("missing-reference", auto_dot, priority=501)  # After intersphinx (500): exact external names win
    # Separate registrations of the same class: overriding one leaves the other as stock PyClasslike
    app.add_directive_to_domain("py", "class", AbstractAwarePyClasslike, override=True)
    app.add_directive_to_domain("py", "exception", AbstractAwarePyClasslike, override=True)
    # After furo's own html-page-context handler (default priority 500), which builds furo_navigation_tree
    app.connect("html-page-context", expand_sidebar, priority=501)
