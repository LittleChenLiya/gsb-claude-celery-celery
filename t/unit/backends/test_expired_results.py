"""Tests for result backend expiration read semantics.

These tests verify that a result which was once stored but has since been
cleaned up (TTL expired) is reported as
:state:`~celery.states.EXPIRED`, while a result that never existed stays
:state:`~celery.states.PENDING`. The historical read interfaces keep their
default behavior (expired results look like PENDING) unless the caller
opts in with ``include_expired=True``.

The in-memory backend's default client ignores TTL, so a controllable clock
and a TTL-aware client are used to drive the lifecycle deterministically.
"""
import pytest

from celery import Celery, states
from celery.backends.cache import CacheBackend


class ControllableClock:
    """A clock whose value can be advanced manually."""

    def __init__(self, start=0):
        self.now = start

    def advance(self, seconds):
        self.now += seconds

    def __call__(self):
        return self.now


class TTLClient:
    """In-memory client enforcing per-key TTL against a controllable clock."""

    def __init__(self, clock):
        self.clock = clock
        self.data = {}  # key -> (value, expire_at or None)

    def get(self, key):
        if key not in self.data:
            return None
        value, expire_at = self.data[key]
        if expire_at is not None and self.clock() >= expire_at:
            del self.data[key]
            return None
        return value

    def get_multi(self, keys):
        return {k: self.get(k) for k in keys if self.get(k) is not None}

    def set(self, key, value, expire=None):
        expire_at = self.clock() + expire if expire else None
        self.data[key] = (value, expire_at)

    def delete(self, key):
        self.data.pop(key, None)

    def incr(self, key, delta=1):
        value, expire_at = self.data.get(key, (0, None))
        new = int(value) + delta
        self.data[key] = (str(new), expire_at)
        return new

    def touch(self, key, expire):
        if key in self.data:
            value, _ = self.data[key]
            self.data[key] = (value, self.clock() + expire if expire else None)


@pytest.fixture
def clock():
    return ControllableClock(start=1000)


@pytest.fixture
def backend(clock):
    app = Celery('test-expired')
    app.conf.result_backend = 'cache+memory://'
    app.conf.result_expires = 10
    app.conf.result_serializer = 'json'
    backend = CacheBackend(backend='memory://', app=app, expires=10)
    backend.tombstone_ttl_grace = 5
    backend.client = TTLClient(clock)
    return backend


class test_expired_result_reads:

    def test_never_existed_is_pending(self, backend):
        meta = backend.get_task_meta('never-existed')
        assert meta['status'] == states.PENDING

        meta = backend.get_task_meta('never-existed', include_expired=True)
        assert meta['status'] == states.PENDING

    def test_existing_result_not_misjudged_as_expired(self, backend):
        backend.mark_as_done('tid', 42)

        meta = backend.get_task_meta('tid')
        assert meta['status'] == states.SUCCESS
        assert meta['result'] == 42

        # Even with include_expired=True, an existing result must not be
        # reported as expired.
        meta = backend.get_task_meta('tid', include_expired=True)
        assert meta['status'] == states.SUCCESS
        assert meta['result'] == 42

    def test_expired_result_reports_expired_when_opted_in(self, backend, clock):
        backend.mark_as_done('tid', 'value')
        clock.advance(11)  # past the 10s result TTL, within tombstone TTL (15s)

        # Default (historical) behavior: expired looks like PENDING.
        meta = backend.get_task_meta('tid')
        assert meta['status'] == states.PENDING

        # Opting in surfaces the EXPIRED state.
        meta = backend.get_task_meta('tid', include_expired=True)
        assert meta['status'] == states.EXPIRED
        assert meta['result'] is None

    def test_task_result_exists_false_for_expired(self, backend, clock):
        backend.mark_as_done('tid', 'value')
        clock.advance(11)

        assert backend.task_result_exists('tid') is False

    def test_tombstone_expiry_returns_to_pending(self, backend, clock):
        backend.mark_as_done('tid', 'value')
        clock.advance(16)  # past both result TTL and tombstone TTL

        meta = backend.get_task_meta('tid', include_expired=True)
        assert meta['status'] == states.PENDING

    def test_forget_clears_result_and_tombstone(self, backend, clock):
        backend.mark_as_done('tid', 'value')
        clock.advance(11)
        assert backend.get_task_meta('tid', include_expired=True)['status'] == states.EXPIRED

        backend.forget('tid')

        assert backend.get_task_meta('tid')['status'] == states.PENDING
        assert backend.get_task_meta('tid', include_expired=True)['status'] == states.PENDING

    def test_old_interface_defaults_to_pending(self, backend, clock):
        backend.mark_as_done('tid', 'hello')
        clock.advance(11)

        assert backend.get_state('tid') == states.PENDING
        assert backend.get_result('tid') is None
        assert backend.get_traceback('tid') is None

    def test_rewrite_after_expiry_stores_new_result(self, backend, clock):
        backend.mark_as_done('tid', 'first')
        clock.advance(11)
        assert backend.get_task_meta('tid', include_expired=True)['status'] == states.EXPIRED

        # Re-storing after expiry must succeed (not be treated as a duplicate
        # SUCCESS that should be ignored).
        clock.advance(100)
        backend.mark_as_done('tid', 'second')
        meta = backend.get_task_meta('tid')
        assert meta['status'] == states.SUCCESS
        assert meta['result'] == 'second'

    def test_wait_for_terminates_on_expired(self, backend, clock):
        backend.mark_as_done('tid', 'value')
        clock.advance(11)

        meta = backend.wait_for('tid', timeout=1, interval=0.1)
        assert meta['status'] == states.EXPIRED

    def test_get_many_terminates_on_expired(self, backend, clock):
        backend.mark_as_done('tid', 'value')
        clock.advance(11)

        results = dict(backend.get_many({'tid'}, timeout=1, interval=0.1))
        assert results['tid']['status'] == states.EXPIRED
