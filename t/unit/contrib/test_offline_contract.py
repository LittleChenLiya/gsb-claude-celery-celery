"""Unit tests for celery.contrib.offline_contract.

These tests are fully offline: no broker connection is made and no task
body is ever executed (proven by the CALLS counter staying empty).
"""
import copy
import json
from collections import namedtuple

from celery import Celery, chain, group
from celery.contrib.offline_contract import (
    DYNAMIC_RETURN_VALUE,
    IMMUTABLE_SKIPS_INJECTION,
    TOO_MANY_ARGS,
    UNREGISTERED_TASK,
    VARIADIC_SIGNATURE,
    check_contract,
)

import pytest

#: records every task body invocation; the checker must never add to it
CALLS = []

Env = namedtuple('Env', 'app add one anything')


@pytest.fixture()
def env():
    app = Celery('offline_contract_test')

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

    return Env(app, add, one, anything)


def snapshot(canvas):
    """Serializable snapshot of a canvas, used to prove non-mutation."""
    return json.dumps(canvas, sort_keys=True, default=repr)


class test_chain_contracts:

    def test_legal_chain(self, env):
        report = check_contract(env.add.s(1, 2) | env.one.s(), app=env.app)
        assert report.ok
        assert report.checked == 2
        assert report.issues == []

    def test_extra_injected_argument(self, env):
        report = check_contract(env.add.s(1, 2) | env.one.s(9), app=env.app)
        assert not report.ok
        assert len(report.issues) == 1
        issue = report.issues[0]
        assert issue.kind == TOO_MANY_ARGS
        assert issue.path == 'chain[1]'
        assert issue.task == env.one.name
        assert 'injected' in issue.message

    def test_immutable_signature_ignores_upstream(self, env):
        report = check_contract(env.add.s(1, 2) | env.one.si(9), app=env.app)
        assert report.ok
        assert report.issues == []
        # the dropped upstream value is reported as a note, not an error
        assert [n.kind for n in report.notes] == [IMMUTABLE_SKIPS_INJECTION]
        assert report.notes[0].path == 'chain[1]'
        # and no dynamic-return-value unknown is emitted for this link
        assert not any(u.path == 'chain[1]' and
                       u.kind == DYNAMIC_RETURN_VALUE
                       for u in report.unknowns)


class test_nested_groups:

    def test_paths_preserved_in_nested_groups(self, env):
        canvas = env.add.s(1, 2) | group([
            env.one.s(9),   # conflict: 1 declared + 1 injected
            group([env.one.s(9), env.one.si(3)]),  # conflict, then immutable
        ])
        report = check_contract(canvas, app=env.app)
        paths = [i.path for i in report.issues]
        assert paths == ['chain[1].group[0]', 'chain[1].group[1].group[0]']
        assert all(i.kind == TOO_MANY_ARGS for i in report.issues)
        # immutable leaf deep inside the nested group is still found
        assert [n.path for n in report.notes] == ['chain[1].group[1].group[1]']

    def test_group_members_receive_upstream_result(self, env):
        # one.s() takes exactly the injected value: legal inside a group
        canvas = env.add.s(1, 2) | group([env.one.s(), env.one.si(5)])
        report = check_contract(canvas, app=env.app)
        assert report.ok
        assert report.checked == 3


class test_registration:

    def test_unregistered_task_located_by_path(self, env):
        canvas = chain(env.app.signature('missing.task', args=()),
                       env.one.s())
        report = check_contract(canvas, app=env.app)
        assert not report.ok
        assert report.issues[0].kind == UNREGISTERED_TASK
        assert report.issues[0].path == 'chain[0]'
        assert report.issues[0].task == 'missing.task'


class test_unknowns_are_honest:

    def test_dynamic_return_value_marked_unknown(self, env):
        report = check_contract(env.add.s(1, 2) | env.one.s(), app=env.app)
        # the injected value is a runtime return value: arity is checked
        # but the value/type must be reported as unknown, never as valid
        dynamic = [u for u in report.unknowns
                   if u.kind == DYNAMIC_RETURN_VALUE]
        assert [u.path for u in dynamic] == ['chain[1]']

    def test_variadic_task_arity_is_unknown(self, env):
        report = check_contract(env.add.s(1, 2) | env.anything.s(1, 2, 3),
                                app=env.app)
        # no fake "too many args" verdict against a *args/**kwargs task
        assert report.issues == []
        assert any(u.kind == VARIADIC_SIGNATURE and u.path == 'chain[1]'
                   for u in report.unknowns)


class test_no_side_effects:

    def test_task_bodies_never_called_and_canvas_untouched(self, env):
        canvas = env.add.s(1, 2) | group([env.one.s(9),
                                          group([env.one.si(3)])])
        before = snapshot(canvas)
        before_copy = copy.deepcopy(canvas)
        CALLS.clear()

        report = check_contract(canvas, app=env.app)

        assert report.checked == 3
        assert CALLS == []                     # zero task body invocations
        assert snapshot(canvas) == before      # serialization unchanged
        assert canvas == before_copy           # structure unchanged
