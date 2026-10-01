"""Tasks auto-retry functionality."""
from datetime import datetime, timezone

from vine.utils import wraps

from celery.exceptions import AutoretryDeadlineExceeded, Ignore, Retry
from celery.utils.log import get_logger
from celery.utils.time import get_exponential_backoff_interval

logger = get_logger(__name__)


def _now():
    """Return the current time as a timezone-aware UTC datetime.

    This is a separate function so tests can freeze the clock by patching
    :func:`celery.app.autoretry._now`.
    """
    return datetime.now(timezone.utc)


def _resolve_retry_deadline(deadline):
    """Resolve the configured auto-retry deadline.

    The deadline is expected to be an absolute :class:`~datetime.datetime`.
    A callable returning such a datetime is also accepted and evaluated on
    every failure, so it must return a stable absolute point in time.
    """
    if callable(deadline):
        return deadline()
    return deadline


def _max_retries_will_be_exceeded(task, retry_kwargs):
    """Return true if the next retry would exceed the max_retries limit.

    This mirrors the limit check performed by
    :meth:`~celery.Task.retry`, so that ``max_retries`` keeps its priority:
    when both limits are reached, :class:`~celery.exceptions.MaxRetriesExceededError`
    is raised just like it was before the deadline feature existed.
    """
    retries = task.request.retries + 1
    max_retries = retry_kwargs.get('max_retries', task.max_retries)
    return max_retries is not None and retries > max_retries


def _is_past_retry_deadline(deadline, now=None):
    """Return true if *now* is at or past the absolute *deadline*.

    The comparison uses ``>=`` so that at the exact clock boundary no extra
    retry is queued.  Naive deadlines are compared against naive UTC now.
    """
    if now is None:
        now = _now()
    if deadline.tzinfo is None:
        now = now.replace(tzinfo=None)
    return now >= deadline


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

                # An optional absolute deadline after which the task is
                # terminated instead of being queued for another retry.
                # max_retries keeps its priority: if it would be exceeded
                # on this attempt, let task.retry raise MaxRetriesExceeded.
                deadline = _resolve_retry_deadline(retry_deadline)
                if (deadline is not None
                        and not _max_retries_will_be_exceeded(
                            task, retry_kwargs_for_attempt)
                        and _is_past_retry_deadline(deadline)):
                    logger.warning(
                        "Task %s[%s] auto-retry deadline %s exceeded; "
                        "terminating instead of retrying again.",
                        task.name, task.request.id, deadline,
                    )
                    raise AutoretryDeadlineExceeded(
                        "Can't retry {}[{}]: auto-retry deadline {} "
                        "exceeded".format(
                            task.name, task.request.id, deadline),
                        deadline=deadline,
                        task_args=args, task_kwargs=kwargs,
                    ) from exc

                ret = task.retry(exc=exc, **retry_kwargs_for_attempt)
                # Stop propagation
                if hasattr(task, 'override_max_retries'):
                    delattr(task, 'override_max_retries')
                raise ret

        task._orig_run, task.run = task.run, run
