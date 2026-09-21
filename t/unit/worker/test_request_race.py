"""Race/timing tests for request ack, retry and terminal-state handling.

These tests pin down the "single final state" guarantees of
:class:`celery.worker.request.Request` when several terminal paths
(worker restart, soft/hard time limits, broker reconnect, manual retry,
revocation) race each other.
"""

import threading
import time
from unittest.mock import Mock, patch

import pytest
from billiard.einfo import ExceptionInfo

from celery.exceptions import Reject, Retry
from celery.worker.request import Request
from celery.worker.state import revoked


def _einfo(exc):
    try:
        raise exc
    except type(exc):
        return ExceptionInfo()


class RaceRequestCase:

    def setup_method(self):
        @self.app.task(shared=False)
        def mytask(i, **kwargs):
            return i ** i

        self.mytask = mytask

    def xRequest(self, task=None, id=None, on_ack=None, on_reject=None,
                 **headers):
        on_ack = on_ack if on_ack is not None else Mock(name='on_ack')
        on_reject = on_reject if on_reject is not None else Mock(
            name='on_reject')
        message = self.TaskMessage(
            (task or self.mytask).name, id, args=[1], kwargs={}, **headers
        )
        return Request(message, app=self.app,
                       on_ack=on_ack, on_reject=on_reject)

    def run_concurrently(self, *callbacks, rounds=25):
        """Run all callbacks concurrently, ``rounds`` times.

        A barrier forces the threads to hit the raced code path at (nearly)
        the same moment in every round.
        """
        for _ in range(rounds):
            barrier = threading.Barrier(len(callbacks))

            def run(callback):
                barrier.wait()
                callback()

            threads = [
                threading.Thread(target=run, args=(callback,))
                for callback in callbacks
            ]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(timeout=10)
            for thread in threads:
                assert not thread.is_alive()


class test_ack_reject_race(RaceRequestCase):

    def test_concurrent_ack_and_reject_single_broker_operation(self):
        """A success ack racing a revoke/timeout reject performs exactly
        one broker operation."""
        for _ in range(25):
            on_ack = Mock(name='on_ack',
                          side_effect=lambda *a: time.sleep(0.001))
            on_reject = Mock(name='on_reject',
                             side_effect=lambda *a: time.sleep(0.001))
            req = self.xRequest(on_ack=on_ack, on_reject=on_reject)
            self.run_concurrently(
                req.acknowledge,
                lambda: req.reject(requeue=True),
                rounds=1,
            )
            assert on_ack.call_count + on_reject.call_count == 1
            assert req.acknowledged

    def test_repeated_acknowledge_is_idempotent(self):
        on_ack = Mock(name='on_ack')
        req = self.xRequest(on_ack=on_ack)
        self.run_concurrently(req.acknowledge, req.acknowledge,
                              req.acknowledge)
        assert on_ack.call_count == 1

    def test_reject_after_acknowledge_is_ignored(self):
        on_ack = Mock(name='on_ack')
        on_reject = Mock(name='on_reject')
        req = self.xRequest(on_ack=on_ack, on_reject=on_reject)
        req.acknowledge()
        req.reject(requeue=True)
        assert on_ack.call_count == 1
        on_reject.assert_not_called()


class test_single_terminal_state(RaceRequestCase):

    def test_hard_timeout_racing_pool_failure_writes_result_once(self):
        """Hard time limit and pool-reported failure must not both write
        a terminal result."""
        for _ in range(25):
            req = self.xRequest()
            req.task.backend = Mock(name='backend')
            einfo = _einfo(KeyError('boom'))
            self.run_concurrently(
                lambda: req.on_timeout(soft=False, timeout=1),
                lambda: req.on_failure(einfo),
                rounds=1,
            )
            assert req.task.backend.mark_as_failure.call_count == 1

    def test_on_failure_is_idempotent(self):
        req = self.xRequest()
        req.task.backend = Mock(name='backend')
        einfo = _einfo(KeyError('boom'))
        req.on_failure(einfo)
        req.on_failure(einfo)
        assert req.task.backend.mark_as_failure.call_count == 1

    def test_reject_racing_failure_writes_result_once(self):
        for _ in range(25):
            req = self.xRequest()
            req.task.backend = Mock(name='backend')
            reject_info = _einfo(Reject('lost', requeue=False))
            failure_info = _einfo(KeyError('boom'))
            self.run_concurrently(
                lambda: req.on_failure(reject_info),
                lambda: req.on_failure(failure_info),
                rounds=1,
            )
            assert req.task.backend.mark_as_failure.call_count == 1

    def test_backend_reconnect_releases_terminal_claim(self):
        """A failing backend write (e.g. backend reconnecting) must not
        permanently block the terminal state from being recorded."""
        req = self.xRequest()
        backend = Mock(name='backend')
        backend.mark_as_failure.side_effect = [
            ConnectionError('backend reconnecting'), None,
        ]
        req.task.backend = backend
        einfo = _einfo(KeyError('boom'))
        with pytest.raises(ConnectionError):
            req.on_failure(einfo)
        req.on_failure(einfo)
        assert backend.mark_as_failure.call_count == 2


class test_acks_late_guarantees(RaceRequestCase):

    def test_not_acknowledged_before_business_success(self):
        """acks_late tasks stay unacked while the task body is running."""
        self.mytask.acks_late = True
        on_ack = Mock(name='on_ack')
        req = self.xRequest(on_ack=on_ack)
        req.on_accepted(pid=1, time_accepted=0.0)
        assert not req.acknowledged
        on_ack.assert_not_called()

    def test_acknowledged_after_business_success(self):
        self.mytask.acks_late = True
        on_ack = Mock(name='on_ack')
        req = self.xRequest(on_ack=on_ack)
        req.on_accepted(pid=1, time_accepted=0.0)
        req.on_success((False, 1, 0.01))
        assert req.acknowledged
        assert on_ack.call_count == 1

    def test_failure_before_success_rejects_instead_of_acking(self):
        self.mytask.acks_late = True
        self.mytask.acks_on_failure = False
        on_ack = Mock(name='on_ack')
        on_reject = Mock(name='on_reject')
        req = self.xRequest(on_ack=on_ack, on_reject=on_reject)
        req.task.backend = Mock(name='backend')
        req.on_failure(_einfo(KeyError('boom')))
        on_ack.assert_not_called()
        assert on_reject.call_count == 1


class test_revocation_race(RaceRequestCase):

    def test_revoke_racing_success_acks_exactly_once(self):
        """Revocation acknowledges the message; a concurrently finishing
        task must not ack it a second time."""
        for _ in range(25):
            on_ack = Mock(name='on_ack')
            req = self.xRequest(on_ack=on_ack)
            revoked.add(req.id)
            try:
                self.run_concurrently(
                    req.revoked,
                    lambda: req.on_success((False, 1, 0.01)),
                    rounds=1,
                )
            finally:
                revoked.discard(req.id)
            assert on_ack.call_count == 1

    def test_revoked_task_failure_records_no_extra_terminal_state(self):
        req = self.xRequest()
        req.task.backend = Mock(name='backend')
        revoked.add(req.id)
        try:
            assert req.revoked()
        finally:
            revoked.discard(req.id)
        req.on_failure(_einfo(KeyError('boom')))
        # revocation already recorded the terminal state (REVOKED);
        # the late failure must not add a FAILURE write on top.
        req.task.backend.mark_as_revoked.assert_called_once()
        req.task.backend.mark_as_failure.assert_not_called()


class test_retry_semantics:

    def setup_method(self):
        @self.app.task(shared=False, bind=True, max_retries=3)
        def retrying_task(self):
            raise KeyError('original')

        self.retrying_task = retrying_task

    def test_retry_publish_failure_preserves_exception_chain(self):
        """If the retry message cannot be published (broker reconnecting),
        the original exception stays on the chain."""
        task = self.retrying_task
        task.request.called_directly = False
        task.request.is_eager = False
        original = KeyError('original')
        with patch.object(task, 'signature_from_request') as sfr:
            sig = sfr.return_value
            sig.apply_async.side_effect = ConnectionError(
                'broker reconnecting')
            with pytest.raises(Reject) as excinfo:
                task.retry(exc=original)
        assert excinfo.value.__cause__ is original
        assert isinstance(excinfo.value.__context__, ConnectionError)

    def test_retry_carries_request_metadata(self):
        """The retried signature keeps id/retries/delivery info."""
        task = self.retrying_task
        task.request.called_directly = False
        task.request.is_eager = False
        task.request.retries = 2
        with patch.object(task, 'signature_from_request') as sfr:
            with pytest.raises(Retry) as excinfo:
                task.retry(exc=KeyError('original'), countdown=1)
        ret = excinfo.value
        assert isinstance(ret.exc, KeyError)
        sfr.assert_called_once()
        # retries is incremented and passed on to the new signature
        assert sfr.call_args[1]['retries'] == 3
        assert ret.sig is sfr.return_value


class test_autoretry_backoff_bounds:

    def test_backoff_exponent_bounded_by_max_retries(self):
        @self.app.task(bind=True, shared=False,
                       autoretry_for=(ZeroDivisionError,),
                       retry_backoff=2, retry_backoff_max=10 ** 9,
                       retry_jitter=False, max_retries=2)
        def task(self_, x, y):
            return x / y

        with patch.object(task, 'retry', wraps=task.retry) as fake_retry:
            # simulate a message whose retry counter grew past the
            # retry budget (e.g. after repeated broker redelivery).
            task.request.retries = 10
            with pytest.raises(ZeroDivisionError):
                task.run(1, 0)

        # the exponent is capped at max_retries=2, so the delay is
        # 2*2**2 == 8, not 2*2**10 == 1024.
        assert fake_retry.call_args[1]['countdown'] == 8

    def test_backoff_bounded_by_time_limit(self):
        @self.app.task(bind=True, shared=False,
                       autoretry_for=(ZeroDivisionError,),
                       retry_backoff=100, retry_backoff_max=150,
                       retry_jitter=False, max_retries=5)
        def task(self_, x, y):
            return x / y

        with patch.object(task, 'retry', wraps=task.retry) as fake_retry:
            task.apply((1, 0))

        countdowns = [
            call_[1].get('countdown') for call_ in fake_retry.call_args_list
        ]
        # 100*2**n would exceed the 150s cap from the third retry on.
        assert countdowns == [100, 150, 150, 150, 150, 150]
        assert all(countdown <= 150 for countdown in countdowns)
