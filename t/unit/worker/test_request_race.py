"""Race/timing tests for request acknowledgement and terminal states.

These tests pin down the exactly-once terminal-state guarantees of
:class:`celery.worker.request.Request` when worker restarts, time limits,
broker reconnects, revocation and manual retries coincide, and the bounds
of exponential retry backoff.
"""
import threading
from time import monotonic
from unittest.mock import Mock

import pytest
from billiard.einfo import ExceptionInfo
from kombu.utils.uuid import uuid

from celery.exceptions import MaxRetriesExceededError, Retry
from celery.worker.request import Request


def _run_concurrently(*callbacks):
    """Run callbacks from concurrent threads, all released at once."""
    barrier = threading.Barrier(len(callbacks))

    def guarded(fn):
        barrier.wait()
        fn()

    threads = [
        threading.Thread(target=guarded, args=(fn,)) for fn in callbacks
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(10)
    for thread in threads:
        assert not thread.is_alive()


class RaceCase:

    def setup_method(self):
        @self.app.task(shared=False)
        def race_task(i):
            return i

        self.race_task = race_task

    def xRequest(self, name=None, id=None, args=None, kwargs=None,
                 on_ack=None, on_reject=None, **head):
        args = [1] if args is None else args
        kwargs = {'f': 'x'} if kwargs is None else kwargs
        on_ack = on_ack or Mock(name='on_ack')
        on_reject = on_reject or Mock(name='on_reject')
        message = self.TaskMessage(
            name or self.race_task.name, id, args=args, kwargs=kwargs, **head
        )
        return Request(message, app=self.app,
                       on_ack=on_ack, on_reject=on_reject)

    def failure_info(self):
        try:
            raise KeyError('boom')
        except KeyError:
            return ExceptionInfo()


class test_ack_resolution(RaceCase):

    def test_concurrent_ack_and_reject_resolve_exactly_once(self):
        on_ack = Mock(name='on_ack')
        on_reject = Mock(name='on_reject')
        req = self.xRequest(on_ack=on_ack, on_reject=on_reject)
        _run_concurrently(
            req.acknowledge, req.reject,
            req.acknowledge, req.reject,
            req.acknowledge, req.reject,
        )
        assert req.acknowledged
        assert on_ack.call_count + on_reject.call_count == 1

    def test_ack_connection_error_leaves_request_unacknowledged(self):
        # Simulates a broker reconnect racing the ack: the first attempt
        # fails and the request must stay unacknowledged so the ack can
        # be retried after the connection is re-established.
        attempts = []

        def flaky_ack(logger, connection_errors):
            attempts.append(1)
            if len(attempts) == 1:
                raise ConnectionError('broker connection lost')

        req = self.xRequest(on_ack=flaky_ack)
        with pytest.raises(ConnectionError):
            req.acknowledge()
        assert not req.acknowledged

        req.acknowledge()
        assert req.acknowledged
        assert len(attempts) == 2

    def test_acks_late_not_acknowledged_before_success(self):
        on_ack = Mock(name='on_ack')
        req = self.xRequest(on_ack=on_ack)
        req.task.acks_late = True

        req.on_accepted(pid=4242, time_accepted=monotonic())
        on_ack.assert_not_called()

        req.on_success((False, 42, 0.01))
        on_ack.assert_called_once()

    def test_acks_late_failure_resolves_exactly_once(self):
        on_ack = Mock(name='on_ack')
        on_reject = Mock(name='on_reject')
        req = self.xRequest(on_ack=on_ack, on_reject=on_reject)
        req.task.acks_late = True
        req.task.backend = Mock(name='backend')

        req.on_failure(self.failure_info())

        assert req.acknowledged
        assert on_ack.call_count + on_reject.call_count == 1
        assert req.task.backend.mark_as_failure.call_count == 1


class test_terminal_result_writes(RaceCase):

    def test_concurrent_timeout_and_failure_write_result_once(self):
        # A hard time limit (timer thread) and the pool error callback may
        # fire for the same request; the result backend must see exactly
        # one terminal write.
        for _ in range(25):
            req = self.xRequest()
            req.task.backend = Mock(name='backend')
            exc_info = self.failure_info()
            _run_concurrently(
                lambda: req.on_timeout(soft=False, timeout=10),
                lambda: req.on_failure(exc_info),
            )
            assert req.task.backend.mark_as_failure.call_count == 1

    def test_requeued_redelivery_gets_fresh_terminal_claim(self):
        # First delivery times out and is requeued; the redelivered copy
        # is a new request and must be allowed to write its own result.
        req1 = self.xRequest()
        req1.task.acks_late = True
        req1.task.acks_on_timeout = False
        req1.task.backend = Mock(name='backend1')
        req1.on_timeout(soft=False, timeout=10)
        assert req1.acknowledged  # resolved via reject(requeue=True)
        assert req1.task.backend.mark_as_failure.call_count == 1

        req2 = self.xRequest(id=req1.id)
        req2.task.acks_late = True
        req2.task.backend = Mock(name='backend2')
        req2.on_success((False, 42, 0.01))
        assert req2.acknowledged

    def test_announce_revoked_writes_result_once(self):
        req = self.xRequest()
        req.task.backend = Mock(name='backend')
        req._announce_revoked('terminated', True, 'SIGTERM', False)
        req._announce_revoked('terminated', True, 'SIGTERM', False)
        assert req.task.backend.mark_as_revoked.call_count == 1

    def test_announce_cancelled_writes_retry_once(self):
        req = self.xRequest()
        req.task.backend = Mock(name='backend')
        req._announce_cancelled(emit_retry=True)
        req._announce_cancelled(emit_retry=True)
        assert req.task.backend.mark_as_retry.call_count == 1

    def test_concurrent_revoke_and_failure_write_result_once(self):
        for _ in range(25):
            req = self.xRequest()
            req.task.backend = Mock(name='backend')
            exc_info = self.failure_info()
            _run_concurrently(
                lambda: req._announce_revoked('revoked', False, None, False),
                lambda: req.on_failure(exc_info),
            )
            writes = (
                req.task.backend.mark_as_revoked.call_count +
                req.task.backend.mark_as_failure.call_count
            )
            assert writes == 1


class test_retry_backoff_bounds:

    def setup_method(self):
        @self.app.task(bind=True, shared=False, retry_backoff=2,
                       retry_backoff_max=10, retry_jitter=False,
                       max_retries=None)
        def backoff_task(self):
            return True

        self.backoff_task = backoff_task

    def retry_exc(self, task, retries, **kwargs):
        task.push_request()
        try:
            task.request.called_directly = False
            task.request.id = uuid()
            task.request.retries = retries
            task.apply_async = Mock(name='apply_async')
            with pytest.raises(Retry) as exc_info:
                task.retry(**kwargs)
            return exc_info.value
        finally:
            task.pop_request()

    def test_backoff_grows_exponentially(self):
        assert self.retry_exc(self.backoff_task, 0).when == 2
        assert self.retry_exc(self.backoff_task, 1).when == 4
        assert self.retry_exc(self.backoff_task, 2).when == 8

    def test_backoff_capped_by_retry_backoff_max(self):
        ret = self.retry_exc(self.backoff_task, 20)
        assert ret.when == 10
        assert ret.sig.options['countdown'] == 10

    def test_backoff_jitter_stays_within_cap(self):
        self.backoff_task.retry_jitter = True
        for _ in range(10):
            ret = self.retry_exc(self.backoff_task, 3)
            assert 0 <= ret.when <= 16

    def test_backoff_bounded_by_max_retries(self):
        self.backoff_task.max_retries = 3
        with pytest.raises(MaxRetriesExceededError):
            self.retry_exc(self.backoff_task, 3)

    def test_explicit_countdown_not_overridden(self):
        ret = self.retry_exc(self.backoff_task, 1, countdown=42)
        assert ret.when == 42

    def test_default_retry_delay_without_backoff(self):
        self.backoff_task.retry_backoff = False
        ret = self.retry_exc(self.backoff_task, 1)
        assert ret.when == self.backoff_task.default_retry_delay

    def test_retry_preserves_exception_and_request_metadata(self):
        cause = ValueError('boom')
        ret = self.retry_exc(self.backoff_task, 2, exc=cause)
        assert ret.exc is cause
        assert ret.sig.options['retries'] == 3

    def test_max_retries_exceeded_preserves_exception_chain(self):
        self.backoff_task.max_retries = 0
        self.backoff_task.push_request()
        try:
            self.backoff_task.request.called_directly = False
            self.backoff_task.request.id = uuid()
            self.backoff_task.request.retries = 0
            try:
                raise ValueError('original')
            except ValueError:
                with pytest.raises(RuntimeError) as exc_info:
                    self.backoff_task.retry(exc=RuntimeError('gave up'))
            assert isinstance(exc_info.value.__cause__, ValueError)
        finally:
            self.backoff_task.pop_request()
