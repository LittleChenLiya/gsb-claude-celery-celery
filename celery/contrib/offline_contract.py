"""Offline contract checker for Celery canvas structures.

This module performs a **static, read-only** contract check over a canvas
(``Signature`` / ``chain`` / ``group`` / ``chord``) *before* it is published:

- it only reads tasks already registered in the current (or given) app;
- it never connects to a broker, never sends a message and never executes
  a task body;
- it recognises that *immutable* signatures (``.si()``) do **not** accept
  the result of the previous step;
- it locates definite problems (unregistered tasks, argument-count
  conflicts caused by result injection) to a canvas path such as
  ``chain[1].group[0]``;
- it never mutates the inspected ``Signature`` objects.

Unknown items (things this checker refuses to guess about)
==========================================================

The checker is deliberately honest about what cannot be decided without
executing anything.  The following are reported as *unknown* (see
:attr:`ContractReport.unknowns`) instead of being silently treated as
valid:

``dynamic-return-value``
    The value injected into a downstream task is produced by a task
    return value at runtime.  Its type and content cannot be verified
    statically; only the *argument count* contract is checked.  We never
    pretend a type validation succeeded.

``variadic-signature``
    The task body accepts ``*args`` and/or ``**kwargs`` so no static
    upper bound on the number of arguments exists.  Only the definite
    lower bound (missing required parameters) can still be reported.

``uninspectable-task``
    ``inspect.signature`` could not obtain a signature for the task body
    (e.g. C-implemented callables), so no arity statement is made.

Informational notes (:attr:`ContractReport.notes`) record links where an
immutable signature causes the upstream result to be dropped on purpose.
"""
from __future__ import annotations

import inspect
from dataclasses import dataclass, field
from typing import List, Optional

from celery.canvas import Signature, _chain, _chord, group

__all__ = (
    'ContractIssue', 'UnknownItem', 'ContractNote',
    'ContractReport', 'check_contract',
)

#: issue kinds
UNREGISTERED_TASK = 'unregistered-task'
TOO_MANY_ARGS = 'too-many-args'
TOO_FEW_ARGS = 'too-few-args'

#: unknown kinds
DYNAMIC_RETURN_VALUE = 'dynamic-return-value'
VARIADIC_SIGNATURE = 'variadic-signature'
UNINSPECTABLE_TASK = 'uninspectable-task'

#: note kinds
IMMUTABLE_SKIPS_INJECTION = 'immutable-skips-injection'


@dataclass(frozen=True)
class ContractIssue:
    """A definite contract violation located at a canvas path."""

    path: str
    kind: str
    task: Optional[str]
    message: str


@dataclass(frozen=True)
class UnknownItem:
    """Something the offline check cannot decide statically."""

    path: str
    kind: str
    message: str


@dataclass(frozen=True)
class ContractNote:
    """Informational finding (not an error)."""

    path: str
    kind: str
    message: str


@dataclass
class ContractReport:
    """Result of :func:`check_contract`."""

    checked: int = 0
    issues: List[ContractIssue] = field(default_factory=list)
    unknowns: List[UnknownItem] = field(default_factory=list)
    notes: List[ContractNote] = field(default_factory=list)

    @property
    def ok(self):
        """True when no definite contract violation was found."""
        return not self.issues

    def render(self):
        """Render a human-readable summary of the report."""
        lines = [
            f'checked task signatures : {self.checked}',
            f'status                  : {"OK" if self.ok else "FAILED"}',
        ]
        for issue in self.issues:
            lines.append(
                f'  ISSUE   [{issue.kind}] {issue.path}: {issue.message}')
        for unknown in self.unknowns:
            lines.append(
                f'  UNKNOWN [{unknown.kind}] {unknown.path}: '
                f'{unknown.message}')
        for note in self.notes:
            lines.append(
                f'  NOTE    [{note.kind}] {note.path}: {note.message}')
        return '\n'.join(lines)


def _node_kind(sig):
    if isinstance(sig, _chord):
        return 'chord'
    if isinstance(sig, group):
        return 'group'
    if isinstance(sig, _chain):
        return 'chain'
    return 'task'


class _ContractChecker:
    """Walks a canvas without mutating it, filling a report."""

    def __init__(self, app):
        self.app = app
        self.report = ContractReport()

    # -- traversal ----------------------------------------------------
    def walk(self, sig):
        kind = _node_kind(sig)
        root = kind if kind != 'task' else 'signature'
        self._walk(sig, root, injected=False)
        return self.report

    def _child_path(self, parent_path, index, node):
        base = f'{parent_path}[{index}]'
        kind = _node_kind(node)
        return base if kind == 'task' else f'{base}.{kind}'

    def _walk(self, sig, path, injected):
        kind = _node_kind(sig)
        if kind == 'chain':
            for i, task in enumerate(sig.tasks):
                # every link after the first receives the previous result
                self._walk(task, self._child_path(path, i, task),
                           injected if i == 0 else True)
        elif kind == 'group':
            for i, task in enumerate(sig.tasks):
                self._walk(task, self._child_path(path, i, task), injected)
        elif kind == 'chord':
            header = sig.kwargs.get('header') or ()
            if isinstance(header, group):
                header = header.tasks
            for i, task in enumerate(header):
                self._walk(
                    task, self._child_path(f'{path}.header', i, task),
                    injected)
            body = sig.kwargs.get('body')
            if body is not None:
                body_path = f'{path}.body'
                if _node_kind(body) != 'task':
                    body_path = f'{body_path}.{_node_kind(body)}'
                # the chord body receives the header results as one argument
                self._walk(body, body_path, True)
        elif isinstance(sig, Signature):
            self._check_task(sig, path, injected)
        else:
            self.report.unknowns.append(UnknownItem(
                path, UNINSPECTABLE_TASK,
                f'object of type {type(sig).__name__!r} is not a '
                'Signature; skipped'))

    # -- leaf checks ----------------------------------------------------
    def _check_task(self, sig, path, injected):
        report = self.report
        report.checked += 1
        name = sig.task if isinstance(sig.task, str) else getattr(
            sig.task, 'name', None) or str(sig.task)
        task = self.app.tasks.get(name)
        if task is None:
            report.issues.append(ContractIssue(
                path, UNREGISTERED_TASK, name,
                f'task {name!r} is not registered in the current app; '
                'the canvas cannot be published as-is'))
            return

        receives = injected and not sig.immutable
        if injected and sig.immutable:
            report.notes.append(ContractNote(
                path, IMMUTABLE_SKIPS_INJECTION,
                f'immutable signature {name} does not accept the upstream '
                "result; the previous step's return value is dropped at "
                'this link'))
        elif injected:
            report.unknowns.append(UnknownItem(
                path, DYNAMIC_RETURN_VALUE,
                f'argument injected into {name} is a runtime task return '
                'value; only its count is checked, its type/content is '
                'unknown'))

        declared = tuple(sig.args or ())
        kwargs = dict(sig.kwargs or {})
        try:
            params = list(inspect.signature(task.run).parameters.values())
        except (TypeError, ValueError) as exc:
            report.unknowns.append(UnknownItem(
                path, UNINSPECTABLE_TASK,
                f'cannot inspect the signature of task {name} ({exc}); '
                'arity not verified'))
            return

        positional = [
            p for p in params
            if p.kind in (inspect.Parameter.POSITIONAL_ONLY,
                          inspect.Parameter.POSITIONAL_OR_KEYWORD)
        ]
        required = [p for p in positional
                    if p.default is inspect.Parameter.empty]
        kwonly_missing = [
            p.name for p in params
            if p.kind is inspect.Parameter.KEYWORD_ONLY
            and p.default is inspect.Parameter.empty
            and p.name not in kwargs
        ]
        has_var_pos = any(p.kind is inspect.Parameter.VAR_POSITIONAL
                          for p in params)
        variadic = has_var_pos or any(
            p.kind is inspect.Parameter.VAR_KEYWORD for p in params)
        if variadic:
            report.unknowns.append(UnknownItem(
                path, VARIADIC_SIGNATURE,
                f'task {name} accepts *args/**kwargs; no static upper '
                'bound on the argument count'))

        provided = len(declared) + (1 if receives else 0)
        pos_names = [p.name for p in positional]
        kw_filled = sum(
            1 for key in kwargs
            if key in pos_names and pos_names.index(key) >= provided)
        effective = provided + kw_filled

        if not has_var_pos and provided > len(positional):
            source = (f'{len(declared)} declared + 1 injected from the '
                      'upstream result' if receives else
                      f'{len(declared)} declared')
            report.issues.append(ContractIssue(
                path, TOO_MANY_ARGS, name,
                f'task {name} accepts at most {len(positional)} positional '
                f'argument(s) but would be called with {provided} '
                f'({source}); the extra injected argument has no '
                'parameter to bind to'))
        elif effective < len(required):
            missing = [p.name for p in required
                       if pos_names.index(p.name) >= effective]
            report.issues.append(ContractIssue(
                path, TOO_FEW_ARGS, name,
                f'task {name} requires {len(required)} positional '
                f'argument(s) but only {effective} can be bound '
                f"(missing: {', '.join(missing)})"))
        if kwonly_missing:
            report.issues.append(ContractIssue(
                path, TOO_FEW_ARGS, name,
                f'task {name} has required keyword-only parameter(s) not '
                f"provided: {', '.join(kwonly_missing)}"))


def check_contract(canvas, app=None):
    """Statically check a canvas against the registered tasks of an app.

    Arguments:
        canvas (Signature): a signature, chain, group or chord.
        app (Celery): app whose task registry is consulted; defaults to
            the canvas' own app, then to the current app.

    Returns:
        ContractReport: with ``issues`` (definite violations located by
        canvas path), ``unknowns`` (things not statically decidable) and
        ``notes`` (informational findings).  The canvas is not modified
        and no task is executed or published.
    """
    if app is None:
        app = getattr(canvas, 'app', None)
    if app is None:
        from celery import current_app
        app = current_app
    return _ContractChecker(app).walk(canvas)
