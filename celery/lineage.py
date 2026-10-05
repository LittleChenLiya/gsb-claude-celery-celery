"""Logical call lineage for task messages.

This module provides an optional lineage record that distinguishes retries of
the same logical task call, the next task in a canvas (chain/chord/group), and
broker redeliveries.  It is intentionally *not* derived from ``task_id``: a
retry keeps the same logical call identity while a chain step creates a child
identity, and a redelivery is detected by the broker rather than by guessing.

The lineage never carries parameter values -- only the call identity, the
parent call identity, the attempt number, and the source type.

Message header fields
---------------------

When a task message is generated (via :meth:`~celery.app.amqp.AMQP.as_task_v2`
or :meth:`~celery.app.amqp.AMQP.as_task_v1`), the lineage is stored under the
``lineage`` message header (protocol 2) or in the message body (protocol 1).
The value is a plain dict with the following fields:

``call_id`` (str)
    Unique identifier for the logical call.  Retries of the same logical call
    share the same ``call_id``; a canvas step creates a new logical call with
    its own ``call_id``.

``parent_call_id`` (str or None)
    The ``call_id`` of the parent call that spawned this call.  ``None`` for
    root calls.  A chain step records the parent chain call's ``call_id``.

``attempt`` (int)
    The attempt number, 0-based.  The first attempt is ``0``; each retry
    increments it by 1.  A broker redelivery keeps the attempt unchanged.

``source`` (str)
    The source type of the call.  One of:

    ``initial``
        The first attempt of a new logical call.
    ``retry``
        A retry of the same logical call (same ``call_id``, incremented
        ``attempt``).
    ``chain``
        A child call created by a canvas step (new ``call_id``,
        ``parent_call_id`` set to the parent's ``call_id``).
    ``redelivery``
        A broker redelivery of the same message.  The call identity and
        attempt are unchanged; the broker's ``redelivered`` delivery flag is
        the authoritative signal.

Validation
----------

:func:`validate_lineage` checks the lineage record for consistency and returns
a diagnostic string (or ``None`` when valid).  Validation is purely
diagnostic: it never raises, acks/rejects the message, or changes the retry
count.  The worker's :class:`~celery.worker.request.Request` records the
diagnostic on the request for observability.

Serialization round-trip
------------------------

The lineage is serialized to a plain dict via :meth:`Lineage.to_dict` and
deserialized via :meth:`Lineage.from_dict`.  The round-trip preserves all
fields.  The lineage never carries parameter values, so it is safe to log and
store without leaking task arguments.
"""
from __future__ import annotations

from typing import Mapping

from kombu.utils.uuid import uuid

__all__ = ("Lineage", "LINEAGE_HEADER", "SOURCE_INITIAL", "SOURCE_RETRY",
           "SOURCE_CHAIN", "SOURCE_REDELIVERY", "validate_lineage")

#: Message header key under which the lineage record is stored.
LINEAGE_HEADER = "lineage"

#: Source type for the first attempt of a logical call.
SOURCE_INITIAL = "initial"

#: Source type for a retry of the same logical call.
SOURCE_RETRY = "retry"

#: Source type for a child call created by a canvas step (chain/chord/group).
SOURCE_CHAIN = "chain"

#: Source type for a broker redelivery of the same message.
SOURCE_REDELIVERY = "redelivery"

_VALID_SOURCES = frozenset({
    SOURCE_INITIAL, SOURCE_RETRY, SOURCE_CHAIN, SOURCE_REDELIVERY,
})


class Lineage:
    """Logical call lineage record.

    A lineage identifies a single logical call (``call_id``), optionally the
    parent call that spawned it (``parent_call_id``), the attempt number
    (``attempt``, 0-based), and the source type (``source``).

    Retries of the same logical call share the same ``call_id`` and increment
    ``attempt``.  A canvas step creates a new logical call with its own
    ``call_id`` and the parent's ``call_id`` as ``parent_call_id``.  A broker
    redelivery keeps the lineage unchanged (the broker's ``redelivered`` flag
    is the authoritative signal).
    """

    __slots__ = ("call_id", "parent_call_id", "attempt", "source")

    def __init__(self, call_id, parent_call_id=None, attempt=0,
                 source=SOURCE_INITIAL):
        self.call_id = call_id
        self.parent_call_id = parent_call_id
        self.attempt = attempt
        self.source = source

    @classmethod
    def initial(cls, call_id=None):
        """Create the initial lineage for a new logical call."""
        return cls(
            call_id=call_id or uuid(),
            parent_call_id=None,
            attempt=0,
            source=SOURCE_INITIAL,
        )

    def retry(self):
        """Create a retry lineage continuing this logical call.

        The retry keeps the same ``call_id`` and ``parent_call_id`` and
        increments ``attempt``.
        """
        return Lineage(
            call_id=self.call_id,
            parent_call_id=self.parent_call_id,
            attempt=self.attempt + 1,
            source=SOURCE_RETRY,
        )

    def child(self, source=SOURCE_CHAIN):
        """Create a child lineage for a canvas step.

        The child gets a fresh ``call_id``, this call's ``call_id`` as
        ``parent_call_id``, and ``attempt`` reset to 0.
        """
        return Lineage(
            call_id=uuid(),
            parent_call_id=self.call_id,
            attempt=0,
            source=source,
        )

    def redelivery(self):
        """Return a lineage marked as a broker redelivery.

        The call identity and attempt are unchanged; only the source type is
        updated to reflect that the broker redelivered the same message.
        """
        return Lineage(
            call_id=self.call_id,
            parent_call_id=self.parent_call_id,
            attempt=self.attempt,
            source=SOURCE_REDELIVERY,
        )

    def to_dict(self):
        """Serialize the lineage to a plain dict."""
        return {
            "call_id": self.call_id,
            "parent_call_id": self.parent_call_id,
            "attempt": self.attempt,
            "source": self.source,
        }

    @classmethod
    def from_dict(cls, data):
        """Deserialize a lineage from a dict.

        Returns ``None`` when ``data`` is ``None`` or empty so callers can
        distinguish "no lineage recorded" from "lineage recorded".
        """
        if not data:
            return None
        return cls(
            call_id=data["call_id"],
            parent_call_id=data.get("parent_call_id"),
            attempt=data.get("attempt", 0),
            source=data.get("source", SOURCE_INITIAL),
        )

    def validate(self):
        """Validate the lineage record.

        Returns a diagnostic string describing the first inconsistency found,
        or ``None`` when the lineage is well-formed.  Validation is purely
        diagnostic: it never raises and never mutates the broker state.
        """
        return validate_lineage(self)

    def __eq__(self, other):
        if not isinstance(other, Lineage):
            return NotImplemented
        return (
            self.call_id == other.call_id
            and self.parent_call_id == other.parent_call_id
            and self.attempt == other.attempt
            and self.source == other.source
        )

    def __hash__(self):
        return hash((self.call_id, self.parent_call_id, self.attempt,
                     self.source))

    def __repr__(self):
        return (
            f"<Lineage: call_id={self.call_id!r} "
            f"parent_call_id={self.parent_call_id!r} "
            f"attempt={self.attempt} source={self.source!r}>"
        )


def validate_lineage(lineage):
    """Validate a lineage record.

    Returns a diagnostic string describing the first inconsistency found, or
    ``None`` when the lineage is well-formed.  The check is diagnostic only:
    it does not raise, ack/reject the message, or change the retry count.
    """
    if lineage is None:
        return None
    if not isinstance(lineage, Lineage):
        return f"lineage must be a Lineage instance, got {type(lineage)!r}"
    if not lineage.call_id:
        return "lineage call_id is missing or empty"
    if lineage.source not in _VALID_SOURCES:
        return f"lineage source {lineage.source!r} is not one of {sorted(_VALID_SOURCES)}"
    if not isinstance(lineage.attempt, int) or isinstance(lineage.attempt, bool):
        return f"lineage attempt must be an integer, got {type(lineage.attempt)!r}"
    if lineage.attempt < 0:
        return f"lineage attempt {lineage.attempt} is negative"
    if lineage.source == SOURCE_RETRY and lineage.attempt < 1:
        return f"retry lineage must have attempt >= 1, got {lineage.attempt}"
    if lineage.source == SOURCE_CHAIN and not lineage.parent_call_id:
        return "chain lineage must record the parent_call_id"
    return None


def lineage_from_headers(headers):
    """Extract a :class:`Lineage` from a message headers mapping.

    Returns ``None`` when no lineage header is present.
    """
    if not headers:
        return None
    raw = headers.get(LINEAGE_HEADER)
    if raw is None:
        return None
    if isinstance(raw, Lineage):
        return raw
    if isinstance(raw, Mapping):
        return Lineage.from_dict(raw)
    return None


def lineage_to_headers(lineage):
    """Serialize a :class:`Lineage` to a message headers value.

    Returns ``None`` when ``lineage`` is ``None`` so the header can be omitted.
    """
    if lineage is None:
        return None
    if isinstance(lineage, Lineage):
        return lineage.to_dict()
    if isinstance(lineage, Mapping):
        return dict(lineage)
    return None
