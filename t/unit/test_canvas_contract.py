"""Tests for the offline canvas contract checker."""

import pytest

from celery import Celery, signature
from celery.canvas import chain, chord, group
from celery.canvas_contract import (
    ContractChecker,
    ContractReport,
    ContractIssue,
    SignatureInfo,
)


@pytest.fixture()
def app():
    """A minimal Celery app with add(x, y) and one(x) registered."""
    app = Celery(set_as_current=False)
    app.conf.update(
        broker_url='memory://',
        result_backend='cache+memory://',
    )

    @app.task(name='add')
    def add(x, y):
        return x + y

    @app.task(name='one')
    def one(x):
        return x

    app.finalize()
    return app


@pytest.fixture()
def checker(app):
    return ContractChecker(app)


class test_ContractChecker:
    """Tests for ContractChecker."""

    def test_legal_chain(self, app, checker):
        """add.s(1,2)|one.s() is legal: one accepts 1 injected arg."""
        canvas = chain(
            app.tasks['add'].s(1, 2),
            app.tasks['one'].s(),
        )
        report = checker.check(canvas)

        assert report.is_valid
        assert report.errors == []
        # The upstream return type is dynamic, so it must be marked unknown.
        assert any(
            i.type == 'unknown_injection' and i.path == 'chain[1]'
            for i in report.infos
        )

    def test_extra_injected_parameter(self, app, checker):
        """add.s(1,2)|one.s(9) has an extra injected parameter."""
        canvas = chain(
            app.tasks['add'].s(1, 2),
            app.tasks['one'].s(9),
        )
        report = checker.check(canvas)

        assert not report.is_valid
        errors = report.errors
        assert len(errors) == 1
        assert errors[0].type == 'param_count_conflict'
        assert errors[0].path == 'chain[1]'
        assert '1' in errors[0].message  # one accepts 1 arg
        assert '2' in errors[0].message  # 1 bound + 1 injected = 2

    def test_immutable_signature_no_injection(self, app, checker):
        """one.si(9) does not accept the upstream result."""
        canvas = chain(
            app.tasks['add'].s(1, 2),
            app.tasks['one'].si(9),
        )
        report = checker.check(canvas)

        assert report.is_valid
        # Immutable signature is reported as info (won't accept upstream).
        assert any(
            i.type == 'immutable_injection' and i.path == 'chain[1]'
            for i in report.infos
        )
        # The immutable signature must not receive the injection.
        sig_infos = [s for s in report.signatures if s.path == 'chain[1]']
        assert len(sig_infos) == 1
        assert sig_infos[0].receives_injection is False
        assert sig_infos[0].immutable is True

    def test_nested_group_path_preserved(self, app, checker):
        """Nested group paths are preserved in the report."""
        canvas = chain(
            app.tasks['add'].s(1, 2),
            group(
                app.tasks['one'].s(),
                app.tasks['one'].s(9),
            ),
        )
        report = checker.check(canvas)

        assert not report.is_valid
        errors = report.errors
        assert len(errors) == 1
        assert errors[0].type == 'param_count_conflict'
        # The path must include the nested group index.
        assert errors[0].path == 'chain[1].group[1]'

        # All signatures must be walked with their full paths.
        paths = [s.path for s in report.signatures]
        assert 'chain[0]' in paths
        assert 'chain[1].group[0]' in paths
        assert 'chain[1].group[1]' in paths

    def test_unregistered_task(self, app, checker):
        """An unregistered task is located to the canvas path."""
        canvas = chain(
            app.tasks['add'].s(1, 2),
            signature('nonexistent_task', args=(1,)),
        )
        report = checker.check(canvas)

        assert not report.is_valid
        errors = report.errors
        assert len(errors) == 1
        assert errors[0].type == 'unregistered_task'
        assert errors[0].path == 'chain[1]'
        assert 'nonexistent_task' in errors[0].message

    def test_single_signature(self, app, checker):
        """A single signature is checked without injection."""
        canvas = app.tasks['add'].s(1, 2)
        report = checker.check(canvas)

        assert report.is_valid
        assert len(report.signatures) == 1
        info = report.signatures[0]
        assert info.path == 'signature'
        assert info.task_name == 'add'
        assert info.param_count == 2
        assert info.bound_args == 2
        assert info.receives_injection is False

    def test_chord_standalone(self, app, checker):
        """A standalone chord's header tasks do not receive injection."""
        canvas = chord(
            group(
                app.tasks['add'].s(1, 2),
                app.tasks['add'].s(3, 4),
            ),
            app.tasks['one'].s(),
        )
        report = checker.check(canvas)

        assert report.is_valid
        # Header tasks do not receive injection (standalone chord).
        for s in report.signatures:
            if s.path.startswith('chord.header'):
                assert s.receives_injection is False
        # Body receives the group result as a list.
        body_infos = [s for s in report.signatures
                      if s.path == 'chord.body[0]']
        assert len(body_infos) == 1
        assert body_infos[0].receives_injection is True
        assert body_infos[0].injection_kind == 'list'

    def test_chord_in_chain(self, app, checker):
        """A chord in a chain passes upstream to its header."""
        # The header tasks add.s(3,4) and add.s(5,6) each accept 2 args,
        # but they also receive the upstream result as a third arg, so
        # this is a parameter count conflict.
        canvas = chain(
            app.tasks['add'].s(1, 2),
            chord(
                group(
                    app.tasks['add'].s(3, 4),
                    app.tasks['add'].s(5, 6),
                ),
                app.tasks['one'].s(),
            ),
        )
        report = checker.check(canvas)

        assert not report.is_valid
        errors = report.errors
        assert len(errors) == 2
        assert all(e.type == 'param_count_conflict' for e in errors)
        assert errors[0].path == 'chain[1].chord.header[0]'
        assert errors[1].path == 'chain[1].chord.header[1]'
        # Header tasks receive the upstream injection.
        for s in report.signatures:
            if s.path.startswith('chain[1].chord.header'):
                assert s.receives_injection is True
                assert s.injection_kind == 'single'

    def test_chord_in_chain_legal(self, app, checker):
        """A chord in a chain is legal when header tasks accept the injection."""
        canvas = chain(
            app.tasks['add'].s(1, 2),
            chord(
                group(
                    app.tasks['one'].s(),
                    app.tasks['one'].s(),
                ),
                app.tasks['one'].s(),
            ),
        )
        report = checker.check(canvas)

        assert report.is_valid
        # Header tasks receive the upstream injection.
        for s in report.signatures:
            if s.path.startswith('chain[1].chord.header'):
                assert s.receives_injection is True
                assert s.injection_kind == 'single'

    def test_chord_body_is_chain(self, app, checker):
        """A chord body that is a chain is walked correctly."""
        canvas = chord(
            group(
                app.tasks['add'].s(1, 2),
                app.tasks['add'].s(3, 4),
            ),
            chain(
                app.tasks['one'].s(),
                app.tasks['one'].s(),
            ),
        )
        report = checker.check(canvas)

        assert report.is_valid
        # First body task receives the group result (list).
        body0 = [s for s in report.signatures
                 if s.path == 'chord.body[0].chain[0]']
        assert len(body0) == 1
        assert body0[0].receives_injection is True
        assert body0[0].injection_kind == 'list'
        # Second body task receives the previous result (single).
        body1 = [s for s in report.signatures
                 if s.path == 'chord.body[0].chain[1]']
        assert len(body1) == 1
        assert body1[0].receives_injection is True
        assert body1[0].injection_kind == 'single'

    def test_original_signature_unchanged(self, app, checker):
        """The original Signature objects are not modified."""
        canvas = chain(
            app.tasks['add'].s(1, 2),
            app.tasks['one'].s(9),
        )
        # Snapshot the serialized form before checking.
        before = canvas.__json__()
        before_sig0 = canvas.tasks[0].__json__()
        before_sig1 = canvas.tasks[1].__json__()

        checker.check(canvas)

        # The serialized form must be identical after checking.
        after = canvas.__json__()
        assert after == before
        assert canvas.tasks[0].__json__() == before_sig0
        assert canvas.tasks[1].__json__() == before_sig1

    def test_task_body_not_called(self, app, checker):
        """The task body (run method) is never called during checking."""
        call_count = 0

        @app.task(name='counting_task')
        def counting_task(x):
            nonlocal call_count
            call_count += 1
            return x

        canvas = chain(
            app.tasks['add'].s(1, 2),
            app.tasks['counting_task'].s(),
        )
        checker.check(canvas)

        assert call_count == 0

    def test_varargs_accepts_any_number(self, app, checker):
        """A task with *args accepts any number of positional arguments."""
        @app.task(name='varargs_task')
        def varargs_task(*args):
            return args

        canvas = chain(
            app.tasks['add'].s(1, 2),
            app.tasks['varargs_task'].s(99),
        )
        report = checker.check(canvas)

        # No parameter count conflict because *args accepts everything.
        assert report.is_valid
        # But the return type is still unknown.
        assert any(
            i.type == 'unknown_injection' for i in report.infos
        )

    def test_unknown_param_count_marked(self, app, checker):
        """A task whose parameter count cannot be determined is marked."""
        # Create a task with a signature that inspect cannot read.
        class OpaqueTask:
            name = 'opaque_task'
            __wrapped__ = None
            run = None

        app._tasks['opaque_task'] = OpaqueTask()

        canvas = signature('opaque_task')
        report = checker.check(canvas)

        assert report.is_valid
        assert any(
            i.type == 'unknown_param_count' for i in report.infos
        )
        # The parameter count must be None (unknown).
        info = report.signatures[0]
        assert info.param_count is None


class test_ContractReport:
    """Tests for ContractReport."""

    def test_is_valid_with_errors(self):
        report = ContractReport()
        report.issues.append(ContractIssue(
            severity='error', type='test', path='p', message='m',
        ))
        assert report.is_valid is False

    def test_is_valid_without_errors(self):
        report = ContractReport()
        report.issues.append(ContractIssue(
            severity='info', type='test', path='p', message='m',
        ))
        assert report.is_valid is True

    def test_by_path(self):
        report = ContractReport()
        report.issues.append(ContractIssue(
            severity='error', type='t1', path='chain[0]', message='m1',
        ))
        report.issues.append(ContractIssue(
            severity='info', type='t2', path='chain[0]', message='m2',
        ))
        report.issues.append(ContractIssue(
            severity='error', type='t3', path='chain[1]', message='m3',
        ))
        assert len(report.by_path('chain[0]')) == 2
        assert len(report.by_path('chain[1]')) == 1
        assert len(report.by_path('chain[2]')) == 0
