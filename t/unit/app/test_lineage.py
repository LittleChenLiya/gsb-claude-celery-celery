"""Tests for optional logical call lineage tracking.

All tests use injected message fixtures only: messages are generated
in-process and their headers are fed directly to the lineage tracker,
no broker is involved.
"""
import json
import logging
from unittest.mock import patch

import pytest
from kombu.serialization import dumps, loads

from celery.app.lineage import (ATTEMPT_REGRESSION, MALFORMED_LINEAGE, ORIGIN_CHAIN, ORIGIN_NEW, ORIGIN_RETRY,
                                PARENT_MISMATCH, CallLineage, LineageDiagnostic, LineageTracker, chain_child_lineage,
                                observe_message_lineage, retry_lineage)
from celery.app.task import Context
from celery.contrib.testing.mocks import TaskMessage
from celery.worker import state


@pytest.fixture
def tracker():
    return LineageTracker()


@pytest.fixture
def message_headers():
    """Injected task message headers factory (protocol 2 style)."""
    def make(lineage=None, **overrides):
        headers = {
            'lang': 'py',
            'task': 't.unit.add',
            'id': 'task-id-1',
            'retries': 0,
            'root_id': 'task-id-1',
            'parent_id': None,
            'argsrepr': '(1, 2)',
            'kwargsrepr': '{}',
        }
        if lineage is not None:
            headers['lineage'] = (
                lineage.to_dict()
                if isinstance(lineage, CallLineage) else dict(lineage)
            )
        headers.update(overrides)
        return headers
    return make


class test_CallLineage:

    def test_new_call_defaults(self):
        lineage = CallLineage.new_call()
        assert lineage.call_id
        assert lineage.parent_call_id is None
        assert lineage.attempt == 0
        assert lineage.origin == ORIGIN_NEW

    def test_record_contains_no_argument_values(self):
        lineage = CallLineage.new_call()
        # The lineage record must only carry identities and counters,
        # never task argument values.
        assert set(lineage.to_dict()) == {
            'call_id', 'parent_call_id', 'attempt', 'origin'}

    def test_serialization_roundtrip_dict(self):
        lineage = CallLineage(
            call_id='call-1', parent_call_id='call-0',
            attempt=2, origin=ORIGIN_RETRY)
        assert CallLineage.from_dict(lineage.to_dict()) == lineage

    def test_serialization_roundtrip_json(self):
        lineage = CallLineage(
            call_id='call-1', parent_call_id=None,
            attempt=0, origin=ORIGIN_NEW)
        payload = json.dumps(lineage.to_dict())
        assert CallLineage.from_dict(json.loads(payload)) == lineage

    @pytest.mark.parametrize('record', [
        {},                                            # missing call_id
        {'call_id': None},                             # missing call_id
        {'call_id': 42},                               # call_id not a string
        {'call_id': 'c', 'attempt': -1},               # negative attempt
        {'call_id': 'c', 'attempt': '1'},              # attempt not an int
        {'call_id': 'c', 'origin': 'bogus'},           # unknown origin
        {'call_id': 'c', 'args': (1, 2)},              # unknown field
        {'call_id': 'c', 'parent_call_id': 3},         # bad parent type
    ])
    def test_from_dict_rejects_malformed(self, record):
        with pytest.raises(ValueError):
            CallLineage.from_dict(record)

    def test_from_dict_rejects_non_mapping(self):
        with pytest.raises(ValueError):
            CallLineage.from_dict('call-1')

    def test_retry_continues_same_logical_call(self):
        lineage = CallLineage.new_call()
        attempt1 = lineage.next_retry()
        assert attempt1.call_id == lineage.call_id
        assert attempt1.parent_call_id == lineage.parent_call_id
        assert attempt1.attempt == 1
        assert attempt1.origin == ORIGIN_RETRY

    def test_chain_child_is_independent_subcall(self):
        lineage = CallLineage.new_call()
        child = lineage.chain_child()
        assert child.call_id != lineage.call_id
        assert child.parent_call_id == lineage.call_id
        assert child.attempt == 0
        assert child.origin == ORIGIN_CHAIN


class test_LineageTracker:

    def test_retry_twice_forms_attempts_0_1_2(self, tracker):
        call = CallLineage.new_call()
        attempt1 = call.next_retry()
        attempt2 = attempt1.next_retry()
        for lineage in (call, attempt1, attempt2):
            assert tracker.observe(lineage) == []
        # all three attempts belong to the same logical call
        assert attempt2.call_id == call.call_id
        assert tracker.attempt_of(call.call_id) == 2

    def test_redelivery_does_not_increase_attempt(self, tracker):
        call = CallLineage.new_call()
        assert tracker.observe(call) == []
        # broker redelivers the very same message
        assert tracker.observe(call, redelivered=True) == []
        assert tracker.attempt_of(call.call_id) == 0

    def test_attempt_regression_returns_diagnostic(self, tracker):
        call = CallLineage.new_call()
        attempt2 = call.next_retry().next_retry()
        tracker.observe(attempt2)
        diagnostics = tracker.observe(call)
        assert len(diagnostics) == 1
        assert diagnostics[0].code == ATTEMPT_REGRESSION
        assert diagnostics[0].call_id == call.call_id
        # the diagnostic must not corrupt the recorded state
        assert tracker.attempt_of(call.call_id) == 2

    def test_parent_mismatch_returns_diagnostic(self, tracker):
        call = CallLineage.new_call()
        tracker.observe(call)
        impostor = CallLineage(
            call_id=call.call_id, parent_call_id='someone-else',
            attempt=1, origin=ORIGIN_RETRY)
        diagnostics = tracker.observe(impostor)
        assert len(diagnostics) == 1
        assert diagnostics[0].code == PARENT_MISMATCH
        assert tracker.parent_of(call.call_id) is None
        assert tracker.attempt_of(call.call_id) == 0

    def test_chain_child_tracked_as_independent_call(self, tracker):
        parent = CallLineage.new_call()
        child = parent.chain_child()
        tracker.observe(parent)
        assert tracker.observe(child) == []
        assert tracker.attempt_of(parent.call_id) == 0
        assert tracker.attempt_of(child.call_id) == 0
        assert tracker.parent_of(child.call_id) == parent.call_id


class test_helpers:

    def test_retry_lineage_without_prior_record(self):
        lineage = retry_lineage(None, retries=2)
        assert lineage.attempt == 2
        assert lineage.origin == ORIGIN_RETRY

    def test_retry_lineage_accepts_serialized_record(self):
        call = CallLineage.new_call()
        lineage = retry_lineage(call.to_dict())
        assert lineage.call_id == call.call_id
        assert lineage.attempt == 1

    def test_chain_child_lineage_without_prior_record(self):
        lineage = chain_child_lineage(None)
        assert lineage.attempt == 0
        assert lineage.origin == ORIGIN_NEW


class test_message_generation:

    def test_as_task_v2_includes_lineage_header(self, app):
        lineage = CallLineage.new_call()
        message = app.amqp.as_task_v2(
            'id1', 't.unit.add', args=(1, 2), kwargs={},
            lineage=lineage)
        assert message.headers['lineage'] == lineage.to_dict()

    def test_as_task_v2_accepts_serialized_lineage(self, app):
        record = CallLineage.new_call().to_dict()
        message = app.amqp.as_task_v2(
            'id1', 't.unit.add', args=(1, 2), kwargs={},
            lineage=record)
        assert message.headers['lineage'] == record

    def test_as_task_v2_omits_lineage_by_default(self, app):
        message = app.amqp.as_task_v2(
            'id1', 't.unit.add', args=(1, 2), kwargs={})
        assert 'lineage' not in message.headers

    def test_message_headers_serialization_roundtrip(self, app):
        lineage = CallLineage.new_call()
        message = app.amqp.as_task_v2(
            'id1', 't.unit.add', args=(1, 2), kwargs={},
            lineage=lineage)
        content_type, content_encoding, data = dumps(
            message.headers, serializer='json')
        headers = loads(
            data, content_type, content_encoding, accept={content_type})
        assert CallLineage.from_dict(headers['lineage']) == lineage

    def test_apply_async_attaches_lineage_when_enabled(self, app):
        app.conf.task_track_lineage = True

        @app.task(shared=False)
        def add(x, y):
            return x + y

        with patch.object(app, 'send_task') as send_task:
            add.apply_async((1, 2))
        options = send_task.call_args[1]
        lineage = CallLineage.from_dict(options['lineage'])
        assert lineage.attempt == 0
        assert lineage.origin == ORIGIN_NEW
        assert lineage.parent_call_id is None

    def test_apply_async_no_lineage_when_disabled(self, app):
        app.conf.task_track_lineage = False

        @app.task(shared=False)
        def add(x, y):
            return x + y

        with patch.object(app, 'send_task') as send_task:
            add.apply_async((1, 2))
        assert 'lineage' not in send_task.call_args[1]


class test_retry_integration:

    @pytest.fixture
    def bound_task(self, app):
        app.conf.task_track_lineage = True

        @app.task(bind=True, shared=False)
        def t(self):
            pass

        return t

    def _retry(self, task, retries, lineage):
        task.push_request(
            retries=retries, lineage=lineage,
            called_directly=False, is_eager=True)
        try:
            return task.retry(throw=False)
        finally:
            task.pop_request()

    def test_retry_continues_logical_call_identity(self, bound_task):
        call = CallLineage.new_call()
        ret = self._retry(bound_task, retries=0, lineage=call.to_dict())
        lineage = CallLineage.from_dict(ret.sig.options['lineage'])
        assert lineage.call_id == call.call_id
        assert lineage.attempt == 1
        assert lineage.origin == ORIGIN_RETRY

    def test_retry_twice_forms_attempts_0_1_2(self, bound_task, tracker):
        call = CallLineage.new_call()
        lineage = call
        assert tracker.observe(lineage) == []
        for expected_attempt in (1, 2):
            ret = self._retry(
                bound_task, retries=expected_attempt - 1,
                lineage=lineage.to_dict())
            lineage = CallLineage.from_dict(ret.sig.options['lineage'])
            assert lineage.call_id == call.call_id
            assert lineage.attempt == expected_attempt
            assert tracker.observe(lineage) == []
        assert tracker.attempt_of(call.call_id) == 2

    def test_retry_without_prior_lineage_starts_at_retry_number(
            self, bound_task):
        ret = self._retry(bound_task, retries=1, lineage=None)
        lineage = CallLineage.from_dict(ret.sig.options['lineage'])
        assert lineage.attempt == 2
        assert lineage.origin == ORIGIN_RETRY


class test_observe_message_lineage:

    def test_no_lineage_header_is_noop(self, tracker, message_headers):
        assert observe_message_lineage(tracker, message_headers()) == []

    def test_empty_headers_is_noop(self, tracker):
        assert observe_message_lineage(tracker, None) == []
        assert observe_message_lineage(tracker, {}) == []

    def test_malformed_lineage_returns_diagnostic(
            self, tracker, message_headers):
        headers = message_headers(lineage={'attempt': 'not-an-int'})
        diagnostics = observe_message_lineage(tracker, headers)
        assert len(diagnostics) == 1
        assert diagnostics[0].code == MALFORMED_LINEAGE

    def test_retry_sequence_via_injected_messages(
            self, tracker, message_headers):
        call = CallLineage.new_call()
        lineage = call
        for expected_attempt in (0, 1, 2):
            headers = message_headers(lineage=lineage)
            assert observe_message_lineage(tracker, headers) == []
            assert tracker.attempt_of(call.call_id) == expected_attempt
            lineage = lineage.next_retry()

    def test_redelivered_message_does_not_increase_attempt(
            self, tracker, message_headers):
        call = CallLineage.new_call()
        headers = message_headers(lineage=call)
        assert observe_message_lineage(tracker, headers) == []
        assert observe_message_lineage(
            tracker, headers, redelivered=True) == []
        assert tracker.attempt_of(call.call_id) == 0

    def test_attempt_regression_via_injected_message(
            self, tracker, message_headers):
        call = CallLineage.new_call()
        attempt2 = call.next_retry().next_retry()
        observe_message_lineage(tracker, message_headers(lineage=attempt2))
        diagnostics = observe_message_lineage(
            tracker, message_headers(lineage=call))
        assert [d.code for d in diagnostics] == [ATTEMPT_REGRESSION]
        assert tracker.attempt_of(call.call_id) == 2

    def test_parent_mismatch_via_injected_message(
            self, tracker, message_headers):
        call = CallLineage.new_call()
        observe_message_lineage(tracker, message_headers(lineage=call))
        impostor = CallLineage(
            call_id=call.call_id, parent_call_id='other-parent',
            attempt=1, origin=ORIGIN_RETRY)
        diagnostics = observe_message_lineage(
            tracker, message_headers(lineage=impostor))
        assert [d.code for d in diagnostics] == [PARENT_MISMATCH]

    def test_chain_child_message_is_independent_call(
            self, tracker, message_headers):
        parent = CallLineage.new_call()
        child = parent.chain_child()
        observe_message_lineage(tracker, message_headers(lineage=parent))
        assert observe_message_lineage(
            tracker, message_headers(lineage=child)) == []
        assert tracker.parent_of(child.call_id) == parent.call_id


class test_LineageDiagnostic:

    def test_repr_and_eq(self):
        d1 = LineageDiagnostic(ATTEMPT_REGRESSION, 'msg', call_id='c')
        d2 = LineageDiagnostic(ATTEMPT_REGRESSION, 'msg', call_id='c')
        assert d1 == d2
        assert 'attempt-regression' in repr(d1)


class test_context:

    def test_lineage_not_leaked_into_custom_headers(self):
        # 'lineage' is a first-class request attribute and must not be
        # republished as a custom header overlay.
        context = Context(
            id='id1', task='t.unit.add',
            lineage=CallLineage.new_call().to_dict(),
            retries=1)
        assert context.lineage is not None
        assert 'lineage' not in (context.headers or {})


class test_worker_strategy_observation:
    """The worker strategy observes lineage without touching acks."""

    def setup_method(self):
        self.app.conf.task_track_lineage = True

        @self.app.task(shared=False)
        def add(x, y):
            return x + y

        self.add = add

    def teardown_method(self):
        state.reset_state()

    def _handle(self, sig, message):
        from unittest.mock import Mock
        reserved = Mock(name='task_reserved')
        consumer = Mock(name='consumer')
        task_buckets = Mock(name='task_buckets')
        task_buckets.__getitem__ = Mock(side_effect=lambda key: None)
        consumer.task_buckets = task_buckets
        consumer.controller.state.revoked = set()
        consumer.disable_rate_limits = True
        consumer.event_dispatcher.enabled = False
        handler = sig.type.start_strategy(
            self.app, consumer, task_reserved=reserved)
        handler(message, None, message.ack, message.reject, [])
        return reserved

    def _message(self, lineage, redelivered=False):
        message = TaskMessage(
            self.add.name, args=(1, 2),
            lineage=(lineage.to_dict()
                     if isinstance(lineage, CallLineage) else lineage),
        )
        message.delivery_info = {
            'exchange': 'celery', 'routing_key': 'celery',
            'redelivered': redelivered,
        }
        return message

    def test_lineage_observed_and_request_still_reserved(self):
        call = CallLineage.new_call()
        reserved = self._handle(self.add.s(1, 2), self._message(call))
        # the message flow is untouched: the request is reserved as usual
        assert reserved.called
        assert self.app.lineage_tracker.attempt_of(call.call_id) == 0

    def test_redelivery_does_not_increase_attempt(self):
        call = CallLineage.new_call()
        self._handle(self.add.s(1, 2), self._message(call))
        self._handle(self.add.s(1, 2), self._message(call, redelivered=True))
        assert self.app.lineage_tracker.attempt_of(call.call_id) == 0

    def test_attempt_regression_logged_but_request_reserved(self, caplog):
        call = CallLineage.new_call()
        attempt2 = call.next_retry().next_retry()
        self._handle(self.add.s(1, 2), self._message(attempt2))
        caplog.clear()
        with caplog.at_level(logging.WARNING, 'celery.worker.strategy'):
            reserved = self._handle(self.add.s(1, 2), self._message(call))
        assert reserved.called
        assert self.app.lineage_tracker.attempt_of(call.call_id) == 2
        assert any(ATTEMPT_REGRESSION in record.getMessage()
                   for record in caplog.records)
