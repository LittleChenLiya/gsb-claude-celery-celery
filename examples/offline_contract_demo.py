#!/usr/bin/env python
"""Demo: offline contract check of a Celery canvas before publishing.

Run from the repository root:

    python examples/offline_contract_demo.py

The script registers ``add(x, y)`` and ``one(x)`` on a local app and
statically checks several canvases with
:func:`celery.contrib.offline_contract.check_contract`:

1. ``add.s(1, 2) | one.s()``   -> legal
2. ``add.s(1, 2) | one.s(9)``  -> extra injected argument at ``chain[1]``
3. ``add.s(1, 2) | one.si(9)`` -> legal; immutable link drops upstream value
4. nested groups               -> conflicts located by full canvas path
5. unregistered task           -> located by canvas path
6. variadic task               -> arity honestly reported as unknown

No broker connection is made and no task body is executed (the CALLS
counter stays empty), and the checked signatures are not mutated (their
serialization is compared before and after).
"""
import json
import os
import sys

# make sure the repository's own celery package is used, even when the
# script is invoked as ``python examples/offline_contract_demo.py`` and an
# unrelated celery is installed site-wide
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from celery import Celery, chain, group  # noqa: E402
from celery.contrib import offline_contract  # noqa: E402
from celery.contrib.offline_contract import check_contract  # noqa: E402

CALLS = []

app = Celery('offline_contract_demo')


@app.task
def add(x, y):
    CALLS.append('add')
    return x + y


@app.task
def one(x):
    CALLS.append('one')
    return x


@app.task
def anything(*args, **kwargs):
    CALLS.append('anything')
    return args


def snapshot(canvas):
    return json.dumps(canvas, sort_keys=True, default=repr)


def main():
    failures = []

    def scenario(title, canvas, expect_ok, expect_paths=()):
        before = snapshot(canvas)
        report = check_contract(canvas, app=app)
        print(f'=== {title}')
        print(report.render())
        print()
        ok = report.ok == expect_ok
        ok &= [i.path for i in report.issues] == list(expect_paths)
        ok &= snapshot(canvas) == before  # signatures not mutated
        if not ok:
            failures.append(title)
        return report

    scenario('1. add.s(1, 2) | one.s()  (legal)',
             add.s(1, 2) | one.s(), expect_ok=True)

    scenario('2. add.s(1, 2) | one.s(9)  (extra injected argument)',
             add.s(1, 2) | one.s(9), expect_ok=False,
             expect_paths=['chain[1]'])

    scenario('3. add.s(1, 2) | one.si(9)  (immutable: drops upstream value)',
             add.s(1, 2) | one.si(9), expect_ok=True)

    scenario('4. nested groups keep full canvas paths',
             add.s(1, 2) | group([one.s(9),
                                  group([one.s(9), one.si(3)])]),
             expect_ok=False,
             expect_paths=['chain[1].group[0]',
                           'chain[1].group[1].group[0]'])

    scenario('5. unregistered task located by path',
             chain(app.signature('missing.task', args=()), one.s()),
             expect_ok=False, expect_paths=['chain[0]'])

    report = scenario('6. variadic task: arity honestly unknown',
                      add.s(1, 2) | anything.s(1, 2, 3), expect_ok=True)
    if not any(u.kind == offline_contract.VARIADIC_SIGNATURE
               for u in report.unknowns):
        failures.append('6. variadic unknown missing')

    print('=== side-effect proof')
    print(f'task body invocations during checks: {len(CALLS)} '
          f'(expected 0)')
    if CALLS:
        failures.append('task bodies were called')

    print()
    print('=== unknown items explanation (module docstring)')
    print(offline_contract.__doc__)

    if failures:
        print('DEMO FAILED:', ', '.join(failures))
        return 1
    print('DEMO OK: all scenarios behaved as expected, '
          '0 task bodies executed, signatures unmodified.')
    return 0


if __name__ == '__main__':
    sys.exit(main())
