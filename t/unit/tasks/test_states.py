import pytest

from celery import states


class test_state_precedence:

    @pytest.mark.parametrize('r,l', [
        (states.SUCCESS, states.PENDING),
        (states.FAILURE, states.RECEIVED),
        (states.REVOKED, states.STARTED),
        (states.SUCCESS, 'CRASHED'),
        (states.FAILURE, 'CRASHED'),
    ])
    def test_gt(self, r, l):
        assert states.state(r) > states.state(l)

    @pytest.mark.parametrize('r,l', [
        ('CRASHED', states.REVOKED),
    ])
    def test_gte(self, r, l):
        assert states.state(r) >= states.state(l)

    @pytest.mark.parametrize('r,l', [
        (states.PENDING, states.EXPIRED),
    ])
    def test_gt_expired(self, r, l):
        assert states.state(r) > states.state(l)

    @pytest.mark.parametrize('r,l', [
        (states.PENDING, states.SUCCESS),
        (states.RECEIVED, states.FAILURE),
        (states.STARTED, states.REVOKED),
        ('CRASHED', states.SUCCESS),
        ('CRASHED', states.FAILURE),
        (states.REVOKED, 'CRASHED'),
    ])
    def test_lt(self, r, l):
        assert states.state(r) < states.state(l)

    @pytest.mark.parametrize('r,l', [
        (states.REVOKED, 'CRASHED'),
    ])
    def test_lte(self, r, l):
        assert states.state(r) <= states.state(l)


class test_expired_state:

    def test_expired_in_all_states(self):
        assert states.EXPIRED in states.ALL_STATES
        assert states.EXPIRED in states.EXPIRED_STATES

    def test_expired_not_ready_or_unready(self):
        assert states.EXPIRED not in states.READY_STATES
        assert states.EXPIRED not in states.UNREADY_STATES
        assert states.EXPIRED not in states.EXCEPTION_STATES
