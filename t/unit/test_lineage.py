"""Tests for the logical call lineage record.

These tests use injected message fixtures only -- no broker interaction.
"""
import pytest

from celery import uuid
from celery.contrib.testing.mocks import TaskMessage
from celery.lineage import (
    LINEAGE_HEADER, Lineage, SOURCE_CHAIN, SOURCE_INITIAL, SOURCE_REDELIVERY,
    SOURCE_RETRY, lineage_from_headers, lineage_to_headers, validate_lineage,
)
from celery.worker.request import Request


class test_Lineage:
    """Tests for the :class:`Lineage` value object."""

    def test_initial_has_zero_attempt(self):
        lin = Lineage.initial()
        assert lin.attempt == 0
        assert lin.source == SOURCE_INITIAL
        assert lin.parent_call_id is None
        assert lin.call_id

    def test_initial_with_explicit_call_id(self):
        lin = Lineage.initial(call_id='fixed-id')
        assert lin.call_id == 'fixed-id'

    def test_retry_keeps_call_id_and_increments_attempt(self):
        lin = Lineage.initial()
        r1 = lin.retry()
        assert r1.call_id == lin.call_id
        assert r1.parent_call_id == lin.parent_call_id
        assert r1.attempt == 1
        assert r1.source == SOURCE_RETRY
        r2 = r1.retry()
        assert r2.call_id == lin.call_id
        assert r2.attempt == 2
        assert r2.source == SOURCE_RETRY

    def test_retry_twice_forms_0_1_2(self):
        lin = Lineage.initial()
        attempts = [lin.attempt]
        current = lin
        for _ in range(2):
            current = current.retry()
            attempts.append(current.attempt)
        assert attempts == [0, 1, 2]
        assert all(c.call_id == lin.call_id for c in [lin, current])

    def test_child_creates_independent_call(self):
        lin = Lineage.initial()
        child = lin.child()
        assert child.call_id != lin.call_id
        assert child.parent_call_id == lin.call_id
        assert child.attempt == 0
        assert child.source == SOURCE_CHAIN

    def test_redelivery_keeps_attempt(self):
        lin = Lineage.initial()
        redelivered = lin.redelivery()
        assert redelivered.call_id == lin.call_id
        assert redelivered.parent_call_id == lin.parent_call_id
        assert redelivered.attempt == lin.attempt
        assert redelivered.source == SOURCE_REDELIVERY

    def test_redelivery_of_retry_keeps_attempt(self):
        lin = Lineage.initial().retry()
        redelivered = lin.redelivery()
        assert redelivered.attempt == 1
        assert redelivered.call_id == lin.call_id

    def test_round_trip(self):
        lin = Lineage.initial()
        data = lin.to_dict()
        restored = Lineage.from_dict(data)
        assert restored == lin
        assert restored.call_id == lin.call_id
        assert restored.parent_call_id == lin.parent_call_id
        assert restored.attempt == lin.attempt
        assert restored.source == lin.source

    def test_round_trip_retry(self):
        lin = Lineage.initial().retry()
        restored = Lineage.from_dict(lin.to_dict())
        assert restored == lin
        assert restored.attempt == 1

    def test_round_trip_child(self):
        lin = Lineage.initial()
        child = lin.child()
        restored = Lineage.from_dict(child.to_dict())
        assert restored == child
        assert restored.parent_call_id == lin.call_id

    def test_from_dict_empty_returns_none(self):
        assert Lineage.from_dict(None) is None
        assert Lineage.from_dict({}) is None

    def test_to_dict_contains_no_parameter_values(self):
        lin = Lineage.initial()
        data = lin.to_dict()
        assert set(data.keys()) == {
            'call_id', 'parent_call_id', 'attempt', 'source',
        }
        # No args/kwargs/body fields.
        assert 'args' not in data
        assert 'kwargs' not in data
        assert 'body' not in data

    def test_equality(self):
        a = Lineage.initial(call_id='x')
        b = Lineage.initial(call_id='x')
        assert a == b
        c = Lineage.initial(call_id='y')
        assert a != c

    def test_validate_well_formed(self):
        assert validate_lineage(Lineage.initial()) is None
        assert validate_lineage(Lineage.initial().retry()) is None
        assert validate_lineage(Lineage.initial().child()) is None

    def test_validate_missing_call_id(self):
        lin = Lineage(call_id='')
        diag = validate_lineage(lin)
        assert diag is not None
        assert 'call_id' in diag

    def test_validate_negative_attempt(self):
        lin = Lineage(call_id='x', attempt=-1)
        diag = validate_lineage(lin)
        assert diag is not None
        assert 'negative' in diag

    def test_validate_retry_without_attempt(self):
        lin = Lineage(call_id='x', attempt=0, source=SOURCE_RETRY)
        diag = validate_lineage(lin)
        assert diag is not None
        assert 'attempt >= 1' in diag

    def test_validate_chain_without_parent(self):
        lin = Lineage(call_id='x', attempt=0, source=SOURCE_CHAIN)
        diag = validate_lineage(lin)
        assert diag is not None
        assert 'parent_call_id' in diag

    def test_validate_invalid_source(self):
        lin = Lineage(call_id='x', attempt=0, source='bogus')
        diag = validate_lineage(lin)
        assert diag is not None
        assert 'source' in diag

    def test_validate_non_integer_attempt(self):
        lin = Lineage(call_id='x', attempt='1')
        diag = validate_lineage(lin)
        assert diag is not None
        assert 'integer' in diag

    def test_validate_none(self):
        assert validate_lineage(None) is None

    def test_validate_non_lineage(self):
        diag = validate_lineage({'call_id': 'x'})
        assert diag is not None
        assert 'Lineage' in diag


class test_lineage_headers:
    """Tests for header serialization helpers."""

    def test_lineage_to_headers(self):
        lin = Lineage.initial()
        headers = lineage_to_headers(lin)
        assert headers == lin.to_dict()

    def test_lineage_to_headers_none(self):
        assert lineage_to_headers(None) is None

    def test_lineage_from_headers(self):
        lin = Lineage.initial()
        headers = {LINEAGE_HEADER: lin.to_dict()}
        restored = lineage_from_headers(headers)
        assert restored == lin

    def test_lineage_from_headers_missing(self):
        assert lineage_from_headers({}) is None
        assert lineage_from_headers(None) is None

    def test_lineage_from_headers_with_lineage_instance(self):
        lin = Lineage.initial()
        headers = {LINEAGE_HEADER: lin}
        assert lineage_from_headers(headers) is lin


class test_amqp_lineage:
    """Tests for lineage in AMQP message generation."""

    def test_as_task_v2_includes_lineage(self, app):
        lin = Lineage.initial()
        msg = app.amqp.as_task_v2(uuid(), 'foo', lineage=lin)
        assert LINEAGE_HEADER in msg.headers
        assert msg.headers[LINEAGE_HEADER] == lin.to_dict()

    def test_as_task_v2_without_lineage(self, app):
        msg = app.amqp.as_task_v2(uuid(), 'foo')
        assert LINEAGE_HEADER not in msg.headers

    def test_as_task_v1_includes_lineage(self, app):
        lin = Lineage.initial()
        msg = app.amqp.as_task_v1(uuid(), 'foo', lineage=lin)
        assert LINEAGE_HEADER in msg.body
        assert msg.body[LINEAGE_HEADER] == lin.to_dict()

    def test_as_task_v1_without_lineage(self, app):
        msg = app.amqp.as_task_v1(uuid(), 'foo')
        assert LINEAGE_HEADER not in msg.body


class test_request_lineage:
    """Tests for lineage extraction in the worker Request."""

    def _make_request(self, app, lineage=None, name=None):
        from unittest.mock import Mock
        name = name or 't.unit.conftest.add'
        kwargs = {'lineage': lineage} if lineage is not None else {}
        message = TaskMessage(name, id=uuid(), **kwargs)
        on_ack = Mock(name='on_ack')
        on_reject = Mock(name='on_reject')
        return Request(message, app=app, on_ack=on_ack, on_reject=on_reject)

    def test_request_extracts_lineage(self, app):
        lin = Lineage.initial()
        req = self._make_request(app, lineage=lin.to_dict())
        assert req.lineage is not None
        assert req.lineage == lin
        assert req.lineage_diagnostic is None

    def test_request_without_lineage(self, app):
        req = self._make_request(app)
        assert req.lineage is None
        assert req.lineage_diagnostic is None

    def test_request_lineage_diagnostic_for_invalid(self, app):
        # A retry lineage with attempt 0 is invalid.
        bad = Lineage(call_id='x', attempt=0, source=SOURCE_RETRY)
        req = self._make_request(app, lineage=bad.to_dict())
        assert req.lineage is not None
        assert req.lineage_diagnostic is not None
        assert 'attempt >= 1' in req.lineage_diagnostic

    def test_request_info_includes_lineage(self, app):
        lin = Lineage.initial()
        req = self._make_request(app, lineage=lin.to_dict())
        info = req.info()
        assert 'lineage' in info
        assert info['lineage'] == lin.to_dict()

    def test_request_info_includes_diagnostic(self, app):
        bad = Lineage(call_id='x', attempt=-1)
        req = self._make_request(app, lineage=bad.to_dict())
        info = req.info()
        assert 'lineage_diagnostic' in info

    def test_request_does_not_ack_on_invalid_lineage(self, app):
        # An invalid lineage must not cause the message to be acked or
        # rejected by the Request itself.
        from unittest.mock import Mock
        bad = Lineage(call_id='', attempt=-1)
        message = TaskMessage('t.unit.conftest.add', id=uuid(), lineage=bad.to_dict())
        on_ack = Mock(name='on_ack')
        on_reject = Mock(name='on_reject')
        req = Request(message, app=app, on_ack=on_ack, on_reject=on_reject)
        # The Request constructor must not have acked or rejected.
        on_ack.assert_not_called()
        on_reject.assert_not_called()
        assert req.acknowledged is False

    def test_redelivery_keeps_attempt(self, app):
        # Redelivering the same message must not increase the attempt.
        # The broker's redelivered flag is the authoritative signal; the
        # lineage itself is unchanged.
        lin = Lineage.initial().retry()
        message = TaskMessage('t.unit.conftest.add', id=uuid(), lineage=lin.to_dict())
        # Simulate a broker redelivery by setting the redelivered flag.
        message.delivery_info = {'redelivered': True}
        req = Request(message, app=app)
        assert req.lineage is not None
        assert req.lineage.attempt == 1
        assert req.lineage.call_id == lin.call_id
        # The delivery_info records the redelivered flag.
        assert req.delivery_info['redelivered'] is True

    def test_redelivery_does_not_increment_attempt(self, app):
        # A redelivered message has the same attempt as the original.
        lin = Lineage.initial()
        message = TaskMessage('t.unit.conftest.add', id=uuid(), lineage=lin.to_dict())
        message.delivery_info = {'redelivered': True}
        req = Request(message, app=app)
        assert req.lineage.attempt == 0
        # The lineage source is not changed to 'redelivery' by the worker;
        # the broker's redelivered flag is the authoritative signal.
        assert req.lineage.source == SOURCE_INITIAL


class test_retry_lineage:
    """Tests for lineage in task retry."""

    def test_retry_continues_same_call_with_incremented_attempt(self, app):
        from celery.exceptions import Retry

        @app.task(bind=True, shared=False)
        def retryable(self):
            raise self.retry(countdown=0)

        lin = Lineage.initial()
        retryable.push_request(lineage=lin, called_directly=False, retries=0)
        try:
            retryable.retry()
        except Retry as exc:
            sig = exc.sig
            assert 'lineage' in sig.options
            retry_lin = Lineage.from_dict(sig.options['lineage'])
            assert retry_lin.call_id == lin.call_id
            assert retry_lin.attempt == 1
            assert retry_lin.source == SOURCE_RETRY
        else:
            pytest.fail('Retry exception was not raised')
        finally:
            retryable.pop_request()

    def test_retry_twice_forms_0_1_2(self, app):
        from celery.exceptions import Retry

        @app.task(bind=True, shared=False)
        def retryable(self):
            raise self.retry(countdown=0)

        lin = Lineage.initial()
        retryable.push_request(lineage=lin, called_directly=False, retries=0)
        try:
            retryable.retry()
        except Retry as exc:
            sig = exc.sig
            retry_lin = Lineage.from_dict(sig.options['lineage'])
            assert retry_lin.attempt == 1
            # Simulate the second retry by updating the request lineage.
            retryable.request.lineage = retry_lin
            retryable.request.retries = 1
            try:
                retryable.retry()
            except Retry as exc2:
                sig2 = exc2.sig
                retry_lin2 = Lineage.from_dict(sig2.options['lineage'])
                assert retry_lin2.attempt == 2
                assert retry_lin2.call_id == lin.call_id
            else:
                pytest.fail('Second Retry exception was not raised')
        else:
            pytest.fail('Retry exception was not raised')
        finally:
            retryable.pop_request()

    def test_retry_without_lineage_starts_fresh(self, app):
        from celery.exceptions import Retry

        @app.task(bind=True, shared=False)
        def retryable(self):
            raise self.retry(countdown=0)

        retryable.push_request(called_directly=False, retries=0)
        try:
            retryable.retry()
        except Retry as exc:
            sig = exc.sig
            assert 'lineage' in sig.options
            retry_lin = Lineage.from_dict(sig.options['lineage'])
            assert retry_lin.attempt == 1
            assert retry_lin.source == SOURCE_RETRY
        else:
            pytest.fail('Retry exception was not raised')
        finally:
            retryable.pop_request()


class test_chain_lineage:
    """Tests for lineage in canvas chain steps."""

    def test_chain_creates_child_lineage(self, app):
        from celery.canvas import chain, Signature
        # Build a chain of two tasks and freeze it.
        t1 = Signature('t1', app=app)
        t2 = Signature('t2', app=app)
        c = chain(t1, t2)
        tasks, results = c.prepare_steps(
            (), {}, c.tasks, app=app, clone=False,
        )
        # Both tasks should have a lineage recorded.
        for task in tasks:
            assert 'lineage' in task.options
            lin = Lineage.from_dict(task.options['lineage'])
            assert lin.source == SOURCE_CHAIN
            assert lin.parent_call_id is not None

    def test_chain_tasks_share_parent_but_have_independent_call_ids(self, app):
        from celery.canvas import chain, Signature
        t1 = Signature('t1', app=app)
        t2 = Signature('t2', app=app)
        c = chain(t1, t2)
        tasks, results = c.prepare_steps(
            (), {}, c.tasks, app=app, clone=False,
        )
        lineages = [Lineage.from_dict(t.options['lineage']) for t in tasks]
        # Each task has its own call_id.
        call_ids = [lin.call_id for lin in lineages]
        assert len(set(call_ids)) == len(call_ids)
        # All share the same parent_call_id (the parent chain call).
        parent_ids = [lin.parent_call_id for lin in lineages]
        assert len(set(parent_ids)) == 1
        # All start at attempt 0.
        assert all(lin.attempt == 0 for lin in lineages)
