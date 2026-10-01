"""Tests for the revocation execution fence in :mod:`celery.worker.request`.

The fence closes the race where a task is revoked *after* its request was
claimed (acknowledged / handed to the pool) but *before* the pool calls the
task: the revoke must win and the user function must never run, while the
message is still acknowledged exactly once.

Each test uses a :class:`threading.Barrier` to deterministically insert the
revoke into the window between claim and call.
"""
import threading
from unittest.mock import Mock

import pytest

from celery.concurrency import solo
from celery.exceptions import TaskRevokedError
from celery.worker.request import Request
from celery.worker.state import revoked

BARRIER_TIMEOUT = 30


class FenceCase:
    """Build a request for a task recording its own execution."""

    def setup_method(self):
        self.app.conf.result_serializer = 'pickle'
        self.executed = []

        @self.app.task(shared=False)
        def fence_task(x):
            self.executed.append(x)
            return x

        self.fence_task = fence_task

    def xRequest(self, on_ack=None, on_reject=None, **head):
        on_ack = on_ack if on_ack is not None else Mock(name='on_ack')
        on_reject = on_reject if on_reject is not None else Mock(name='on_reject')
        message = self.TaskMessage(
            self.fence_task.name, args=[1], kwargs={}, **head,
        )
        return Request(message, app=self.app,
                       on_ack=on_ack, on_reject=on_reject)

    def revoke_during_claim(self, job):
        """Arm a barrier inserting a revoke between claim and call.

        Returns an ``on_ack`` mock whose side effect blocks the executing
        thread -- right after the message is claimed, right before the
        revocation fence -- until a helper thread has added the task id to
        the revoked set.  Also returns the helper thread (not yet started).
        """
        claimed = threading.Barrier(2)
        revoke_landed = threading.Event()

        def _hold_between_claim_and_call(*args, **kwargs):
            claimed.wait(timeout=BARRIER_TIMEOUT)
            revoke_landed.wait(timeout=BARRIER_TIMEOUT)

        on_ack = Mock(name='on_ack', side_effect=_hold_between_claim_and_call)

        def _revoke():
            claimed.wait(timeout=BARRIER_TIMEOUT)
            revoked.add(job.id)
            revoke_landed.set()

        return on_ack, threading.Thread(target=_revoke)


class test_execute_fence(FenceCase):
    """Fence in :meth:`Request.execute` (synchronous execution path)."""

    def test_revoke_between_claim_and_call_never_runs_user_code(self):
        job = self.xRequest()
        on_ack, revoker = self.revoke_during_claim(job)
        job._on_ack = on_ack

        revoker.start()
        try:
            job.execute()
        finally:
            revoker.join(BARRIER_TIMEOUT)
            revoked.discard(job.id)

        assert not revoker.is_alive()
        # the revoke won the race: the user function never ran
        assert self.executed == []
        # the revoke was announced exactly once ...
        assert job._already_revoked
        # ... and the message was acknowledged exactly once
        assert job.acknowledged
        assert on_ack.call_count == 1

    def test_not_revoked_task_still_runs(self):
        on_ack = Mock(name='on_ack')
        job = self.xRequest(on_ack=on_ack)

        job.execute()

        assert self.executed == [1]
        assert on_ack.call_count == 1


class test_execute_using_pool_fence(FenceCase):
    """Fence in :meth:`Request.execute_using_pool` (solo pool, in-process)."""

    def test_revoke_between_claim_and_call_never_runs_user_code(self):
        job = self.xRequest()
        on_ack, revoker = self.revoke_during_claim(job)
        job._on_ack = on_ack

        pool = solo.TaskPool()
        revoker.start()
        try:
            with pytest.raises(TaskRevokedError):
                job.execute_using_pool(pool)
        finally:
            revoker.join(BARRIER_TIMEOUT)
            revoked.discard(job.id)

        assert not revoker.is_alive()
        # the revoke won the race: the user function never ran
        assert self.executed == []
        # the revoke was announced exactly once ...
        assert job._already_revoked
        # ... and the message was acknowledged exactly once
        assert job.acknowledged
        assert on_ack.call_count == 1

    def test_not_revoked_task_still_runs(self, depends_on_current_app):
        on_ack = Mock(name='on_ack')
        job = self.xRequest(on_ack=on_ack)

        pool = solo.TaskPool()
        job.execute_using_pool(pool)

        assert self.executed == [1]
        assert on_ack.call_count == 1
