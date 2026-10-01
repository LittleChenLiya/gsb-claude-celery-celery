"""Tasks auto-retry functionality."""
from datetime import datetime, timezone

from vine.utils import wraps

from celery.exceptions import Ignore, Retry, RetryDeadlineExceededError
from celery.utils.time import get_exponential_backoff_interval

__all__ = ('add_autoretry_behaviour',)


def _utcnow():
    """Return the current UTC time as an aware :class:`~datetime.datetime`.

    Kept as a module-level function so tests can freeze the clock.
    """
    return datetime.now(timezone.utc)


def _deadline_to_timestamp(deadline):
    """Normalize an absolute retry deadline to a POSIX timestamp.

    Accepts aware or naive :class:`~datetime.datetime` instances (naive
    values are assumed to be in UTC) and numeric POSIX timestamps.
    """
    if isinstance(deadline, datetime):
        if deadline.tzinfo is None:
            deadline = deadline.replace(tzinfo=timezone.utc)
        return deadline.timestamp()
    return float(deadline)


def add_autoretry_behaviour(task, **options):
    """Wrap task's `run` method with auto-retry functionality."""
    autoretry_for = tuple(
        options.get('autoretry_for',
                    getattr(task, 'autoretry_for', ()))
    )
    dont_autoretry_for = tuple(
        options.get('dont_autoretry_for',
                    getattr(task, 'dont_autoretry_for', ()))
    )
    retry_kwargs = options.get(
        'retry_kwargs', getattr(task, 'retry_kwargs', {})
    )
    retry_backoff = float(
        options.get('retry_backoff',
                    getattr(task, 'retry_backoff', False))
    )
    retry_backoff_max = int(
        options.get('retry_backoff_max',
                    getattr(task, 'retry_backoff_max', 600))
    )
    retry_jitter = options.get(
        'retry_jitter', getattr(task, 'retry_jitter', True)
    )
    retry_deadline = options.get(
        'retry_deadline', getattr(task, 'retry_deadline', None)
    )
    if retry_deadline is not None:
        retry_deadline = _deadline_to_timestamp(retry_deadline)

    if autoretry_for and not hasattr(task, '_orig_run'):

        @wraps(task.run)
        def run(*args, **kwargs):
            try:
                return task._orig_run(*args, **kwargs)
            except Ignore:
                # If Ignore signal occurs task shouldn't be retried,
                # even if it suits autoretry_for list
                raise
            except Retry:
                raise
            except dont_autoretry_for:
                raise
            except autoretry_for as exc:
                retry_kwargs_for_attempt = retry_kwargs.copy()
                if retry_backoff:
                    retry_kwargs_for_attempt['countdown'] = \
                        get_exponential_backoff_interval(
                            factor=int(max(1.0, retry_backoff)),
                            retries=task.request.retries,
                            maximum=retry_backoff_max,
                            full_jitter=retry_jitter)
                # Override max_retries
                if hasattr(task, 'override_max_retries'):
                    retry_kwargs_for_attempt['max_retries'] = getattr(
                        task, 'override_max_retries', task.max_retries)
                # The max_retries limit takes precedence over the
                # absolute retry deadline: if this attempt already
                # exhausts max_retries, let task.retry() report it.
                max_retries = retry_kwargs_for_attempt.get(
                    'max_retries', task.max_retries)
                max_retries_exceeded = (
                    max_retries is not None and
                    task.request.retries + 1 > max_retries
                )
                if (retry_deadline is not None and
                        not max_retries_exceeded and
                        _utcnow().timestamp() >= retry_deadline):
                    # Past the absolute deadline: report the termination
                    # reason instead of re-queueing the task.  The
                    # comparison is inclusive so that hitting the exact
                    # deadline does not issue one more retry.
                    raise RetryDeadlineExceededError(
                        "Can't retry {}[{}]: auto-retry deadline {} "
                        "exceeded".format(
                            task.name, task.request.id,
                            datetime.fromtimestamp(
                                retry_deadline, tz=timezone.utc).isoformat(),
                        ),
                        task_args=args, task_kwargs=kwargs,
                        deadline=retry_deadline,
                    ) from exc
                ret = task.retry(exc=exc, **retry_kwargs_for_attempt)
                # Stop propagation
                if hasattr(task, 'override_max_retries'):
                    delattr(task, 'override_max_retries')
                raise ret

        task._orig_run, task.run = task.run, run
