#!/usr/bin/env python
"""Demo script for the offline canvas contract checker.

This script demonstrates how to use :class:`~celery.canvas_contract.ContractChecker`
to perform a static contract check on a Celery canvas (signature, chain,
group, or chord) without connecting to the broker or executing any task.

Usage::

    python examples/canvas_contract_demo.py

The script registers two tasks (``add(x, y)`` and ``one(x)``) and checks
several canvas configurations:

1. ``add.s(1,2)|one.s()`` -- legal (one accepts 1 injected arg).
2. ``add.s(1,2)|one.s(9)`` -- extra injected parameter (error).
3. ``add.s(1,2)|one.si(9)`` -- immutable signature (no injection).
4. Nested group -- path is preserved in the report.
5. Unregistered task -- located to the canvas path.

The original Signature objects are never modified, and the task bodies
are never called.
"""

from __future__ import annotations

import json
import os
import sys

# Ensure the worktree root is on sys.path so the local celery package is
# imported even when the editable install points to another worktree.
_worktree_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _worktree_root not in sys.path:
    sys.path.insert(0, _worktree_root)

from celery import Celery, signature
from celery.canvas import chain, chord, group
from celery.canvas_contract import ContractChecker


def make_app() -> Celery:
    """Create a minimal Celery app with add(x, y) and one(x)."""
    app = Celery(set_as_current=False)
    app.conf.update(
        broker_url='memory://',
        result_backend='cache+memory://',
    )

    call_count = {'add': 0, 'one': 0}

    @app.task(name='add')
    def add(x, y):
        call_count['add'] += 1
        return x + y

    @app.task(name='one')
    def one(x):
        call_count['one'] += 1
        return x

    app.finalize()
    app._call_count = call_count
    return app


def print_report(title: str, report) -> None:
    """Print a contract report in a human-readable format."""
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print(f"{'=' * 60}")
    print(f"  Valid: {report.is_valid}")
    print(f"  Signatures walked: {len(report.signatures)}")
    print(f"  Errors: {len(report.errors)}")
    print(f"  Warnings: {len(report.warnings)}")
    print(f"  Infos: {len(report.infos)}")

    if report.signatures:
        print("\n  Signature details:")
        for s in report.signatures:
            print(
                f"    - path={s.path!r}, task={s.task_name!r}, "
                f"registered={s.registered}, param_count={s.param_count}, "
                f"bound_args={s.bound_args}, bound_kwargs={s.bound_kwargs}, "
                f"immutable={s.immutable}, receives_injection={s.receives_injection}, "
                f"injection_kind={s.injection_kind}, return_type={s.return_type}"
            )

    if report.issues:
        print("\n  Issues:")
        for issue in report.issues:
            print(
                f"    [{issue.severity}] {issue.type} at {issue.path!r}: "
                f"{issue.message}"
            )


def main() -> int:
    """Run the demo."""
    app = make_app()
    checker = ContractChecker(app)

    # Case 1: legal chain -- add.s(1,2)|one.s()
    canvas1 = chain(
        app.tasks['add'].s(1, 2),
        app.tasks['one'].s(),
    )
    report1 = checker.check(canvas1)
    print_report("Case 1: add.s(1,2)|one.s() -- legal", report1)

    # Case 2: extra injected parameter -- add.s(1,2)|one.s(9)
    canvas2 = chain(
        app.tasks['add'].s(1, 2),
        app.tasks['one'].s(9),
    )
    report2 = checker.check(canvas2)
    print_report("Case 2: add.s(1,2)|one.s(9) -- extra injected parameter", report2)

    # Case 3: immutable signature -- add.s(1,2)|one.si(9)
    canvas3 = chain(
        app.tasks['add'].s(1, 2),
        app.tasks['one'].si(9),
    )
    report3 = checker.check(canvas3)
    print_report("Case 3: add.s(1,2)|one.si(9) -- immutable signature", report3)

    # Case 4: nested group -- path is preserved
    canvas4 = chain(
        app.tasks['add'].s(1, 2),
        group(
            app.tasks['one'].s(),
            app.tasks['one'].s(9),
        ),
    )
    report4 = checker.check(canvas4)
    print_report("Case 4: nested group -- path preserved", report4)

    # Case 5: unregistered task
    canvas5 = chain(
        app.tasks['add'].s(1, 2),
        signature('nonexistent_task', args=(1,)),
    )
    report5 = checker.check(canvas5)
    print_report("Case 5: unregistered task", report5)

    # Case 6: chord standalone
    canvas6 = chord(
        group(
            app.tasks['add'].s(1, 2),
            app.tasks['add'].s(3, 4),
        ),
        app.tasks['one'].s(),
    )
    report6 = checker.check(canvas6)
    print_report("Case 6: chord standalone", report6)

    # Verify that the original Signature objects were not modified.
    print(f"\n{'=' * 60}")
    print("  Verification: original Signature objects unchanged")
    print(f"{'=' * 60}")
    canvas = chain(
        app.tasks['add'].s(1, 2),
        app.tasks['one'].s(9),
    )
    before = canvas.__json__()
    checker.check(canvas)
    after = canvas.__json__()
    print(f"  Before check: {json.dumps(before, sort_keys=True)}")
    print(f"  After check:  {json.dumps(after, sort_keys=True)}")
    print(f"  Identical: {before == after}")

    # Verify that the task bodies were never called.
    print(f"\n{'=' * 60}")
    print("  Verification: task bodies never called")
    print(f"{'=' * 60}")
    print(f"  add call count: {app._call_count['add']}")
    print(f"  one call count: {app._call_count['one']}")
    print(f"  All zero: {all(v == 0 for v in app._call_count.values())}")

    # Summary.
    print(f"\n{'=' * 60}")
    print("  Summary")
    print(f"{'=' * 60}")
    print(f"  Case 1 (legal): valid={report1.is_valid}")
    print(f"  Case 2 (extra param): valid={report2.is_valid}")
    print(f"  Case 3 (immutable): valid={report3.is_valid}")
    print(f"  Case 4 (nested group): valid={report4.is_valid}")
    print(f"  Case 5 (unregistered): valid={report5.is_valid}")
    print(f"  Case 6 (chord): valid={report6.is_valid}")

    return 0


if __name__ == '__main__':
    sys.exit(main())
