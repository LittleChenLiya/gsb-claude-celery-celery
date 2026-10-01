Local acceptance: result-backend expired-read semantics
=======================================================

``result_expiry_semantics.py`` verifies the explicit expired-result state
using the in-memory result backend (``memory://``) and a controllable
clock -- no Docker or external services, no real sleeping.

Run::

    python t/local_acceptance/result_expiry_semantics.py

Expected: ``27/27 checks passed`` / ``ALL CHECKS PASSED``.

The script covers:

* write and read-back (SUCCESS / value),
* existing results are never mistaken for expired (before TTL, and for
  unready states such as STARTED),
* expiry + cleanup producing the new explicit ``EXPIRED`` state,
* the old interface defaulting to the legacy ``PENDING``/``None`` view,
* never-seen task ids staying ``PENDING`` even via the new interface,
* ``forget()`` clearing both result and expiry marker,
* re-writing a result clearing a stale marker,
* lazy expiry on non-autoexpire key/value backends.

The equivalent behaviors are also covered as unit tests in
``t/unit/backends/test_base.py`` and ``t/unit/backends/test_cache.py``.
