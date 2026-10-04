"""Offline contract checker for Celery canvas.

This module provides a static analysis tool for checking the contract
between canvas signatures (``Signature``, ``chain``, ``group``, ``chord``)
and the tasks registered in the current app.

It does **not** connect to the broker, execute any task, or mutate the
original :class:`~celery.canvas.Signature` objects.  It only reads the
task registry of the current app and inspects the canvas structure.

The checker reports:

* **Unregistered tasks** -- a signature references a task name that is not
  registered in the app.
* **Parameter count conflicts** -- a task accepts fewer positional arguments
  than the number of bound arguments plus the injected upstream result.
* **Immutable signatures** -- a signature marked as immutable will not
  accept the upstream result (informational).
* **Unknowns** -- when the parameter count or the upstream return value
  cannot be statically determined, the checker marks them as unknown
  instead of pretending that validation succeeded.

Every finding is located by a *canvas path* such as ``chain[1].group[0]``.
"""

from __future__ import annotations

import inspect
from dataclasses import dataclass, field
from typing import List, Optional, Tuple

from celery.canvas import Signature, _chain, _chord, group

__all__ = (
    'ContractChecker',
    'ContractReport',
    'SignatureInfo',
    'ContractIssue',
)


@dataclass
class SignatureInfo:
    """Static information about a single signature in the canvas.

    Attributes:
        path: Canvas path, e.g. ``chain[1].group[0]``.
        task_name: Name of the referenced task.
        registered: Whether the task is registered in the app.
        param_count: Number of positional parameters the task accepts,
            or ``None`` when it cannot be determined.
        has_varargs: Whether the task accepts ``*args``.
        has_varkwargs: Whether the task accepts ``**kwargs``.
        bound_args: Number of positional arguments already bound.
        bound_kwargs: Number of keyword arguments already bound.
        immutable: Whether the signature is immutable (``.si(...)``).
        receives_injection: Whether the upstream result will be injected
            as the first positional argument.
        injection_kind: One of ``"none"``, ``"single"``, ``"list"``,
            ``"unknown"``.
        return_type: The statically determined return type of the task,
            or ``"unknown"`` when it cannot be determined.
    """

    path: str
    task_name: str
    registered: bool
    param_count: Optional[int]
    has_varargs: bool
    has_varkwargs: bool
    bound_args: int
    bound_kwargs: int
    immutable: bool
    receives_injection: bool
    injection_kind: str
    return_type: str = "unknown"


@dataclass
class ContractIssue:
    """A single contract finding.

    Attributes:
        severity: ``"error"``, ``"warning"``, or ``"info"``.
        type: Machine-readable issue type.
        path: Canvas path where the issue was found.
        message: Human-readable description.
    """

    severity: str
    type: str
    path: str
    message: str


@dataclass
class ContractReport:
    """The result of a contract check.

    Attributes:
        signatures: Static information about every signature walked.
        issues: All findings, ordered by canvas path / discovery.
    """

    signatures: List[SignatureInfo] = field(default_factory=list)
    issues: List[ContractIssue] = field(default_factory=list)

    @property
    def errors(self) -> List[ContractIssue]:
        """All error-severity issues."""
        return [i for i in self.issues if i.severity == "error"]

    @property
    def warnings(self) -> List[ContractIssue]:
        """All warning-severity issues."""
        return [i for i in self.issues if i.severity == "warning"]

    @property
    def infos(self) -> List[ContractIssue]:
        """All info-severity issues."""
        return [i for i in self.issues if i.severity == "info"]

    @property
    def is_valid(self) -> bool:
        """``True`` if no error-severity issues were found."""
        return not self.errors

    def by_path(self, path: str) -> List[ContractIssue]:
        """Return all issues at the given canvas path."""
        return [i for i in self.issues if i.path == path]


class ContractChecker:
    """Offline contract checker for Celery canvas.

    Reads only the tasks registered in the given app.  Does not connect to
    the broker or execute any task.

    Arguments:
        app: The :class:`~celery.Celery` application whose task registry
            will be read.
    """

    def __init__(self, app) -> None:
        self.app = app

    # -- public API --------------------------------------------------------

    def check(self, canvas) -> ContractReport:
        """Check the contract of the given canvas.

        Arguments:
            canvas: A :class:`~celery.canvas.Signature`,
                :class:`~celery.canvas.chain`, :class:`~celery.canvas.group`,
                or :class:`~celery.canvas.chord`.

        Returns:
            ContractReport: The result of the check.
        """
        report = ContractReport()
        self._walk(canvas, [], report,
                   receives_injection=False, injection_kind="none")
        return report

    # -- walking -----------------------------------------------------------

    def _walk(self, sig, path, report, receives_injection, injection_kind,
              upstream_return_type="unknown"):
        if isinstance(sig, _chain):
            self._walk_chain(sig, path, report,
                             receives_injection, injection_kind)
        elif isinstance(sig, group):
            self._walk_group(sig, path, report,
                             receives_injection, injection_kind)
        elif isinstance(sig, _chord):
            self._walk_chord(sig, path, report,
                             receives_injection, injection_kind)
        elif isinstance(sig, Signature):
            self._walk_signature(sig, path, report,
                                 receives_injection, injection_kind,
                                 upstream_return_type)
        else:
            report.issues.append(ContractIssue(
                severity="error",
                type="unknown_canvas_type",
                path=self._path_str(path),
                message=f"Unknown canvas type: {type(sig).__name__!r}",
            ))

    def _walk_chain(self, chain_sig, path, report,
                    receives_injection=False, injection_kind="none"):
        tasks = list(chain_sig.tasks)
        prev_was_group = False
        prev_return_type = "unknown"
        for i, task in enumerate(tasks):
            task_path = path + [("chain", i)]
            if i == 0:
                cur_receives = receives_injection
                cur_kind = injection_kind
                cur_upstream_return_type = prev_return_type
            else:
                cur_receives = True
                cur_kind = "list" if prev_was_group else "single"
                cur_upstream_return_type = prev_return_type

            if isinstance(task, Signature) and task.immutable:
                if cur_receives:
                    report.issues.append(ContractIssue(
                        severity="info",
                        type="immutable_injection",
                        path=self._path_str(task_path),
                        message=(
                            "Immutable signature (.si) will not accept the "
                            "upstream result"
                        ),
                    ))
                cur_receives = False
                cur_kind = "none"
                cur_upstream_return_type = "unknown"

            self._walk(task, task_path, report,
                       cur_receives, cur_kind, cur_upstream_return_type)

            prev_was_group = isinstance(task, group)
            if isinstance(task, Signature):
                prev_return_type = self._get_return_type(task)
            else:
                prev_return_type = "unknown"

    def _walk_group(self, group_sig, path, report,
                    receives_injection, injection_kind):
        tasks = list(group_sig.tasks)
        for i, task in enumerate(tasks):
            task_path = path + [("group", i)]
            self._walk(task, task_path, report,
                       receives_injection, injection_kind)

    def _walk_chord(self, chord_sig, path, report,
                    receives_injection=False, injection_kind="none"):
        header = chord_sig.tasks
        # The header is always a sequence of Signature objects (possibly
        # wrapped in a group by older code paths).
        if isinstance(header, group):
            header_tasks = list(header.tasks)
        elif isinstance(header, (list, tuple)):
            header_tasks = list(header)
        else:
            header_tasks = [header]

        for i, task in enumerate(header_tasks):
            task_path = path + [("chord.header", i)]
            self._walk(task, task_path, report,
                       receives_injection, injection_kind)

        body = chord_sig.body
        if body is not None:
            body_path = path + [("chord.body", 0)]
            # The body always receives the group result as a list.
            # If the body is a chain, walk it with the first task
            # receiving the list injection.
            if isinstance(body, _chain):
                self._walk_chain(body, body_path, report,
                                 receives_injection=True,
                                 injection_kind="list")
            elif isinstance(body, group):
                self._walk_group(body, body_path, report, True, "list")
            else:
                self._walk(body, body_path, report, True, "list")

    def _walk_signature(self, sig, path, report,
                        receives_injection, injection_kind,
                        upstream_return_type="unknown"):
        task_name = sig.task
        registered = task_name in self.app.tasks

        param_count = None
        has_varargs = False
        has_varkwargs = False
        return_type = "unknown"
        if registered:
            task = self.app.tasks[task_name]
            param_info = self._get_param_info(task)
            if param_info is not None:
                param_count, has_varargs, has_varkwargs = param_info
            return_type = self._get_return_type(task)

        bound_args = len(sig.args) if sig.args else 0
        bound_kwargs = len(sig.kwargs) if sig.kwargs else 0

        actual_injection = receives_injection
        actual_injection_kind = injection_kind
        if sig.immutable:
            actual_injection = False
            actual_injection_kind = "none"

        info = SignatureInfo(
            path=self._path_str(path),
            task_name=task_name,
            registered=registered,
            param_count=param_count,
            has_varargs=has_varargs,
            has_varkwargs=has_varkwargs,
            bound_args=bound_args,
            bound_kwargs=bound_kwargs,
            immutable=sig.immutable,
            receives_injection=actual_injection,
            injection_kind=actual_injection_kind,
            return_type=return_type,
        )
        report.signatures.append(info)

        if not registered:
            report.issues.append(ContractIssue(
                severity="error",
                type="unregistered_task",
                path=self._path_str(path),
                message=(
                    f"Task {task_name!r} is not registered in the current app"
                ),
            ))
            return

        if param_count is None:
            report.issues.append(ContractIssue(
                severity="info",
                type="unknown_param_count",
                path=self._path_str(path),
                message=(
                    f"Cannot statically determine the parameter count for "
                    f"task {task_name!r}; skipping parameter-count validation"
                ),
            ))
            return

        total_args = bound_args + (1 if actual_injection else 0)
        if not has_varargs and total_args > param_count:
            report.issues.append(ContractIssue(
                severity="error",
                type="param_count_conflict",
                path=self._path_str(path),
                message=(
                    f"Task {task_name!r} accepts {param_count} positional "
                    f"argument(s), but {bound_args} bound + "
                    f"{1 if actual_injection else 0} injected = {total_args} "
                    f"would be passed"
                ),
            ))

        if actual_injection and upstream_return_type == "unknown":
            report.issues.append(ContractIssue(
                severity="info",
                type="unknown_injection",
                path=self._path_str(path),
                message=(
                    f"Cannot statically determine the upstream return type "
                    f"for task {task_name!r}; not pretending type validation "
                    f"succeeded"
                ),
            ))

    # -- helpers -----------------------------------------------------------

    @staticmethod
    def _get_return_type(task) -> str:
        """Get the return type annotation of a registered task.

        Returns the string representation of the return type annotation,
        or ``"unknown"`` when it cannot be determined.
        """
        fun = None
        for attr in ('__wrapped__', 'run'):
            try:
                fun = getattr(task, attr, None)
            except Exception:  # pragma: no cover
                fun = None
            if fun is not None:
                break

        if fun is None:
            return "unknown"

        if isinstance(fun, staticmethod):
            fun = fun.__func__

        try:
            sig = inspect.signature(fun)
        except (ValueError, TypeError):
            return "unknown"

        ret = sig.return_annotation
        if ret is inspect.Signature.empty:
            return "unknown"
        return str(ret)

    @staticmethod
    def _get_param_info(task):
        """Get parameter info for a registered task.

        Returns ``(count, has_varargs, has_varkwargs)`` or ``None`` when
        the signature cannot be determined.
        """
        fun = None
        for attr in ('__wrapped__', 'run'):
            try:
                fun = getattr(task, attr, None)
            except Exception:  # pragma: no cover
                fun = None
            if fun is not None:
                break

        if fun is None:
            return None

        if isinstance(fun, staticmethod):
            fun = fun.__func__

        try:
            sig = inspect.signature(fun)
        except (ValueError, TypeError):
            return None

        params = list(sig.parameters.values())

        # For bound tasks (bind=True), the first parameter is the task
        # instance itself; skip it.
        if params and params[0].name == 'self':
            params = params[1:]

        count = 0
        has_varargs = False
        has_varkwargs = False
        for p in params:
            if p.kind == inspect.Parameter.VAR_POSITIONAL:
                has_varargs = True
            elif p.kind == inspect.Parameter.VAR_KEYWORD:
                has_varkwargs = True
            elif p.kind in (
                inspect.Parameter.POSITIONAL_ONLY,
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
            ):
                count += 1
            # KEYWORD_ONLY parameters do not count toward the positional
            # argument limit.

        return (count, has_varargs, has_varkwargs)

    @staticmethod
    def _path_str(path):
        if not path:
            return "signature"
        return ".".join(f"{kind}[{idx}]" for kind, idx in path)