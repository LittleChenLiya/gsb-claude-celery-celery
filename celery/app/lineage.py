"""Optional logical call lineage tracking for task messages.

This module implements an *optional* record of the logical call a task
message belongs to, so that retries of one task, the next step of a
canvas chain, and broker redeliveries can be told apart without
guessing from the ``task_id`` alone.

The lineage is recorded when a message is generated and consists only
of identities and counters -- never of argument values:

``call_id``
    Identity of the logical call.  A retry keeps the ``call_id`` of the
    original message and only increments the attempt number, while the
    next item of a chain creates a new child ``call_id``.

``parent_call_id``
    Identity of the logical call this call was spawned from (e.g. the
    chain step that completed before this one), or :const:`None` for
    root calls.

``attempt``
    Zero-based attempt number of the logical call.  The first message
    of a call is attempt 0; each retry continues the same logical call
    with ``attempt + 1``.  Broker redelivery of the same message does
    not create a new attempt.

``origin``
    How this message was generated: one of ``'new'``, ``'retry'`` or
    ``'chain'``.

Tracking is enabled with the :setting:`task_track_lineage` setting and
is purely observational: observing a message may yield *diagnostics*
(for example on attempt regression or parent identity mismatch) but
never changes broker acknowledgements or retry counts.
"""

from collections.abc import Mapping

from kombu.utils.uuid import uuid

__all__ = (
    'ORIGIN_NEW', 'ORIGIN_RETRY', 'ORIGIN_CHAIN', 'ORIGINS',
    'ATTEMPT_REGRESSION', 'PARENT_MISMATCH', 'MALFORMED_LINEAGE',
    'CallLineage', 'LineageDiagnostic', 'LineageTracker',
    'coerce_lineage', 'retry_lineage', 'chain_child_lineage',
    'observe_message_lineage',
)

#: Origin of a freshly published call.
ORIGIN_NEW = 'new'
#: Origin of a message republished by :meth:`@Task.retry`.
ORIGIN_RETRY = 'retry'
#: Origin of the next item of a canvas chain.
ORIGIN_CHAIN = 'chain'

#: All valid lineage origin values.
ORIGINS = frozenset({ORIGIN_NEW, ORIGIN_RETRY, ORIGIN_CHAIN})

#: Diagnostic code: an attempt number lower than the one already
#: observed for the same logical call was seen.
ATTEMPT_REGRESSION = 'attempt-regression'
#: Diagnostic code: the same logical call was observed with a
#: different parent call identity.
PARENT_MISMATCH = 'parent-mismatch'
#: Diagnostic code: the lineage header could not be parsed.
MALFORMED_LINEAGE = 'malformed-lineage'


class LineageDiagnostic:
    """A non-fatal problem detected while observing message lineage.

    Diagnostics are returned to the caller (and logged by the worker)
    but never affect message acknowledgement or retry bookkeeping.
    """

    __slots__ = ('code', 'message', 'call_id')

    def __init__(self, code, message, call_id=None):
        self.code = code
        self.message = message
        self.call_id = call_id

    def __eq__(self, other):
        if isinstance(other, LineageDiagnostic):
            return (self.code, self.message, self.call_id) == (
                other.code, other.message, other.call_id)
        return NotImplemented

    def __hash__(self):
        return hash((self.code, self.message, self.call_id))

    def __repr__(self):
        return f'<LineageDiagnostic: {self.code}: {self.message}>'


class CallLineage:
    """Logical call lineage record carried in the message headers.

    The record intentionally contains no argument or keyword argument
    values -- only call identities, the attempt number and the origin
    type.
    """

    __slots__ = ('call_id', 'parent_call_id', 'attempt', 'origin')

    def __init__(self, call_id=None, parent_call_id=None,
                 attempt=0, origin=ORIGIN_NEW):
        if not call_id:
            raise ValueError('lineage requires a call_id')
        if not isinstance(attempt, int) or attempt < 0:
            raise ValueError(
                f'lineage attempt must be a non-negative int, got {attempt!r}')
        if origin not in ORIGINS:
            raise ValueError(
                f'lineage origin must be one of {sorted(ORIGINS)}, '
                f'got {origin!r}')
        self.call_id = call_id
        self.parent_call_id = parent_call_id
        self.attempt = attempt
        self.origin = origin

    @classmethod
    def new_call(cls):
        """Create lineage for a freshly published logical call."""
        return cls(call_id=uuid(), attempt=0, origin=ORIGIN_NEW)

    def next_retry(self):
        """Return lineage for the next retry attempt of this call.

        The retry keeps the identity of the original logical call and
        only increments the attempt number.
        """
        return type(self)(
            call_id=self.call_id,
            parent_call_id=self.parent_call_id,
            attempt=self.attempt + 1,
            origin=ORIGIN_RETRY,
        )

    def chain_child(self):
        """Return lineage for the next item of a chain.

        The next chain item is an independent child call: it gets its
        own ``call_id``, starts at attempt 0, and records this call as
        its parent.
        """
        return type(self)(
            call_id=uuid(),
            parent_call_id=self.call_id,
            attempt=0,
            origin=ORIGIN_CHAIN,
        )

    def to_dict(self):
        """Serialize to a JSON-safe mapping (no argument values)."""
        return {
            'call_id': self.call_id,
            'parent_call_id': self.parent_call_id,
            'attempt': self.attempt,
            'origin': self.origin,
        }

    @classmethod
    def from_dict(cls, mapping):
        """Deserialize from a mapping, validating all fields.

        Raises:
            ValueError: if the mapping is not a valid lineage record.
        """
        if not isinstance(mapping, Mapping):
            raise ValueError(
                f'lineage must be a mapping, got {type(mapping).__name__}')
        unknown = set(mapping) - {
            'call_id', 'parent_call_id', 'attempt', 'origin'}
        if unknown:
            raise ValueError(
                f'lineage record has unknown fields: {sorted(unknown)}')
        try:
            call_id = mapping['call_id']
        except KeyError:
            raise ValueError('lineage record is missing call_id') from None
        if not isinstance(call_id, str):
            raise ValueError(
                f'lineage call_id must be a string, got {call_id!r}')
        parent_call_id = mapping.get('parent_call_id')
        if parent_call_id is not None and not isinstance(parent_call_id, str):
            raise ValueError(
                'lineage parent_call_id must be a string or None, '
                f'got {parent_call_id!r}')
        return cls(
            call_id=call_id,
            parent_call_id=parent_call_id,
            attempt=mapping.get('attempt', 0),
            origin=mapping.get('origin', ORIGIN_NEW),
        )

    def __eq__(self, other):
        if isinstance(other, CallLineage):
            return self.to_dict() == other.to_dict()
        return NotImplemented

    def __hash__(self):
        return hash((self.call_id, self.parent_call_id,
                     self.attempt, self.origin))

    def __repr__(self):
        return (
            f'<CallLineage: {self.call_id} attempt={self.attempt} '
            f'origin={self.origin} parent={self.parent_call_id}>')


def coerce_lineage(value):
    """Coerce ``value`` to a :class:`CallLineage` or :const:`None`.

    Accepts :class:`CallLineage` instances, serialized mappings and
    :const:`None`.  Raises :exc:`ValueError` for malformed input.
    """
    if value is None:
        return None
    if isinstance(value, CallLineage):
        return value
    return CallLineage.from_dict(value)


def retry_lineage(current, retries=None):
    """Return the lineage for a retry republished after ``current``.

    ``current`` is the lineage of the message being retried (a
    :class:`CallLineage`, a serialized mapping, or :const:`None`).
    When no prior lineage exists a new logical call identity is
    started at attempt ``retries`` so the attempt counter still
    reflects the retry number of the task.
    """
    lineage = coerce_lineage(current)
    if lineage is None:
        return CallLineage(
            call_id=uuid(), attempt=retries or 0, origin=ORIGIN_RETRY)
    return lineage.next_retry()


def chain_child_lineage(current):
    """Return the lineage for the next chain item after ``current``.

    The next chain item is always an independent child call.  When no
    prior lineage exists a fresh root call is created.
    """
    lineage = coerce_lineage(current)
    if lineage is None:
        return CallLineage.new_call()
    return lineage.chain_child()


class LineageTracker:
    """Process-local registry of observed logical calls.

    The tracker records the highest attempt number and the parent
    identity seen for every logical call.  Observing a message never
    raises: inconsistencies are reported as :class:`LineageDiagnostic`
    instances and leave both the tracker state and any broker
    acknowledgement/retry bookkeeping untouched.
    """

    def __init__(self):
        # call_id -> (attempt, parent_call_id)
        self._calls = {}

    def observe(self, lineage, redelivered=False):
        """Observe one message lineage record.

        Arguments:
            lineage (CallLineage): lineage record of the message.
            redelivered (bool): whether the broker marked the message
                as redelivered.  Redelivery of the same message never
                increases the recorded attempt.

        Returns:
            list[LineageDiagnostic]: possibly empty list of
            diagnostics.  An empty list means the observation was
            consistent with everything seen so far.
        """
        diagnostics = []
        known = self._calls.get(lineage.call_id)
        if known is None:
            self._calls[lineage.call_id] = (
                lineage.attempt, lineage.parent_call_id)
            return diagnostics
        known_attempt, known_parent = known
        if known_parent != lineage.parent_call_id:
            diagnostics.append(LineageDiagnostic(
                PARENT_MISMATCH,
                f'logical call {lineage.call_id} observed with parent '
                f'{lineage.parent_call_id!r}, expected {known_parent!r}',
                call_id=lineage.call_id,
            ))
            return diagnostics
        if lineage.attempt < known_attempt:
            diagnostics.append(LineageDiagnostic(
                ATTEMPT_REGRESSION,
                f'logical call {lineage.call_id} regressed from attempt '
                f'{known_attempt} to {lineage.attempt}',
                call_id=lineage.call_id,
            ))
            return diagnostics
        if lineage.attempt > known_attempt:
            self._calls[lineage.call_id] = (
                lineage.attempt, lineage.parent_call_id)
        # attempt == known_attempt: re-observation of the same message
        # (e.g. broker redelivery); state is left unchanged.
        return diagnostics

    def attempt_of(self, call_id):
        """Return the highest attempt recorded for ``call_id``."""
        known = self._calls.get(call_id)
        return known[0] if known is not None else None

    def parent_of(self, call_id):
        """Return the parent call identity recorded for ``call_id``."""
        known = self._calls.get(call_id)
        return known[1] if known is not None else None

    def clear(self):
        """Forget all observed logical calls."""
        self._calls.clear()


def observe_message_lineage(tracker, headers, redelivered=False):
    """Observe the lineage header of an incoming task message.

    Arguments:
        tracker (LineageTracker): tracker recording the observation.
        headers (Mapping): task message headers (may contain no
            lineage at all, in which case this is a no-op).
        redelivered (bool): broker redelivery flag of the message.

    Returns:
        list[LineageDiagnostic]: diagnostics for malformed or
        inconsistent lineage records; empty when the message is fine
        or carries no lineage.
    """
    if not headers:
        return []
    raw = headers.get('lineage')
    if raw is None:
        return []
    try:
        lineage = coerce_lineage(raw)
    except (TypeError, ValueError) as exc:
        return [LineageDiagnostic(MALFORMED_LINEAGE, str(exc))]
    return tracker.observe(lineage, redelivered=redelivered)
