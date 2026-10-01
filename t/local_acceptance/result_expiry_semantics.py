"""Local acceptance: result-backend expired-read semantics.

Uses the in-memory result backend (memory://) with a controllable clock,
so no Docker/external service and no real sleeping is required.

Demonstrates:
  1. write  -> SUCCESS / value readable
  2. before expiry -> existing result is never mistaken for expired
  3. cleanup after the TTL -> explicit EXPIRED state (new interface)
  4. old interface defaults stay compatible (PENDING, result None)
  5. id that never existed stays PENDING even with distinguish_expired=True
  6. forget() removes result and expiry marker -> back to never-seen PENDING
  7. lazy expiry on non-autoexpire KV backends (filesystem-style subclass)
"""
from datetime import timedelta

from celery import Celery, states
from celery.backends.base import KeyValueStoreBackend
from celery.backends.cache import CacheBackend
from celery.utils.functional import LRUCache

PASS, FAIL = 'PASS', 'FAIL'
results = []


def check(name, cond, detail=''):
    results.append((name, cond, detail))
    outcome = PASS if cond else FAIL
    suffix = (' -- ' + detail) if detail else ''
    print(f'  [{outcome}] {name}{suffix}')


class ControllableClock:
    def __init__(self, start):
        self.t = start

    def __call__(self):
        return self.t

    def advance(self, **kwargs):
        self.t += timedelta(**kwargs)


def fresh_memory_backend(app, expires):
    # The dummy client normally shares a process-global LRU cache; give the
    # backend an isolated store for this run.
    backend = CacheBackend(backend='memory://', expires=expires, app=app)
    backend.client.cache = LRUCache(limit=1000)
    return backend


def main():
    app = Celery(set_as_current=False)
    app.conf.result_backend = 'cache+memory://'
    app.conf.result_serializer = 'json'
    app.conf.result_cache_max = -1  # disable the result LRU cache layer

    print('\n=== Scenario A: write -> not expired -> expired/cleaned ===')
    expires = 3600
    backend = fresh_memory_backend(app, expires=expires)
    clock = ControllableClock(app.now())
    backend._now = clock

    tid_done, tid_seen, tid_never = 'task-done', 'task-seen', 'task-never'

    # 1) write
    backend.mark_as_done(tid_done, 42)
    check('written result reads SUCCESS',
          backend.get_state(tid_done) == states.SUCCESS)
    check('written result value reads back',
          backend.get_result(tid_done) == 42)
    check('never-seen id is PENDING (old & new API)',
          backend.get_state(tid_never) == states.PENDING
          and backend.get_state(
              tid_never, distinguish_expired=True) == states.PENDING)
    check('is_result_expired False for never-seen id',
          backend.is_result_expired(tid_never) is False)

    # 2) before TTL: existing result must never be mistaken as expired
    clock.advance(seconds=expires - 1)
    check('just before TTL still SUCCESS',
          backend.get_state(
              tid_done, distinguish_expired=True) == states.SUCCESS)
    check('is_result_expired False before TTL',
          backend.is_result_expired(tid_done) is False)
    check('task_result_exists True before TTL',
          backend.task_result_exists(tid_done) is True)

    # an unready task whose payload still exists must never be reported as
    # expired, however stale its metadata looks
    backend.store_result(tid_seen, None, states.STARTED)
    check('existing STARTED result not misread as expired',
          backend.get_state(
              tid_seen, distinguish_expired=True) == states.STARTED)
    check('existing STARTED result not "expired"',
          backend.is_result_expired(tid_seen) is False)

    # cleanup before TTL removes nothing
    backend.cleanup()
    check('cleanup before TTL keeps result',
          backend.get_state(tid_done) == states.SUCCESS)

    # 3) cross TTL, run periodic cleanup
    clock.advance(seconds=2)  # now 1s past expiry
    backend.cleanup()
    check('after cleanup: new API reports EXPIRED',
          backend.get_state(
              tid_done, distinguish_expired=True) == states.EXPIRED)
    check('after cleanup: is_result_expired True',
          backend.is_result_expired(tid_done) is True)
    check('after cleanup: task_result_exists False',
          backend.task_result_exists(tid_done) is False)

    # 4) old interface compatibility: EXPIRED looks like PENDING by default
    old_meta = backend.get_task_meta(tid_done)
    check('old get_state() still PENDING after expiry',
          backend.get_state(tid_done) == states.PENDING)
    check('old get_task_meta() still PENDING-like dict',
          old_meta == {'status': states.PENDING, 'result': None},
          detail=repr(old_meta))
    check('old get_result() still None after expiry',
          backend.get_result(tid_done) is None)
    check('never-seen id remains PENDING (not EXPIRED)',
          backend.get_state(
              tid_never, distinguish_expired=True) == states.PENDING)
    check('never-seen id is_result_expired False',
          backend.is_result_expired(tid_never) is False)

    # 6) forget clears marker too
    backend.forget(tid_done)
    check('forget() removes expiry marker -> PENDING even via new API',
          backend.get_state(
              tid_done, distinguish_expired=True) == states.PENDING)
    check('forget() -> is_result_expired False',
          backend.is_result_expired(tid_done) is False)

    # rewrite after expiry: existing result must shadow any stale marker
    backend.mark_as_done(tid_done, 7)
    check('rewritten result reads SUCCESS, not EXPIRED',
          backend.get_state(
              tid_done, distinguish_expired=True) == states.SUCCESS)
    check('rewritten value reads back',
          backend.get_result(tid_done) == 7)

    print('\n=== Scenario B: lazy expiry on a non-autoexpire KV backend ===')

    class MemoryKV(KeyValueStoreBackend):
        supports_autoexpire = False

        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self._store = {}

        def get(self, key):
            return self._store.get(key)

        def set(self, key, value):
            self._store[key] = value

        def delete(self, key):
            self._store.pop(key, None)

    lazy = MemoryKV(app=app, expires=expires)
    lazy_clock = ControllableClock(app.now())
    lazy._now = lazy_clock
    tid = 'lazy-task'
    lazy.mark_as_done(tid, 'hello')
    lazy_clock.advance(seconds=expires + 1)
    # old API keeps today's semantics untouched: as long as the stale payload
    # has not been removed by cleanup, it is returned as-is (SUCCESS)
    check('lazy expired task: old API unchanged (stale SUCCESS payload)',
          lazy.get_state(tid) == states.SUCCESS)
    # new API lazily marks expiry on first explicit read
    check('lazy expired task: new API EXPIRED',
          lazy.get_state(
              tid, distinguish_expired=True) == states.EXPIRED)
    check('lazy expiry marker left in store',
          lazy.get(lazy.get_key_for_expired_task(tid)) is not None)
    # subsequent reads agree, and never-seen ids are unaffected
    check('lazy expired task: second read still EXPIRED',
          lazy.is_result_expired(tid) is True)
    check('lazy backend: never-seen id PENDING',
          lazy.get_state(
              'never', distinguish_expired=True) == states.PENDING)

    failed = [r for r in results if not r[1]]
    print(f'\n{len(results) - len(failed)}/{len(results)} checks passed')
    if failed:
        print('FAILED:')
        for name, _, detail in failed:
            print(f'  - {name} {detail}')
        raise SystemExit(1)
    print('ALL CHECKS PASSED')


if __name__ == '__main__':
    main()
