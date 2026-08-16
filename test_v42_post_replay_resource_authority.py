from __future__ import annotations

import time

import numpy as np

import v42_post_replay_resource_authority as v42


class _Core:
    START_TS = 1
    TIMEFRAME_SECONDS = {'15m': 900}

    def __init__(self):
        self.state = {'runtime_role': {'role': 'LEADER'}}
        self.persisted = {}

    def get_state(self, key, default=None):
        return self.persisted.get(key, default)


class _DoneFuture:
    def done(self):
        return True

    def exception(self):
        return None


class _Transition:
    STATE_KEY = 'transition'
    COMPLETION_COOLDOWN_SECONDS = 180
    _CERT_FUTURE = _DoneFuture()

    @staticmethod
    def _persist(core, patch):
        state = dict(core.persisted.get(_Transition.STATE_KEY, {}))
        state.update(patch)
        core.persisted[_Transition.STATE_KEY] = state
        core.state['replay_transition_stability'] = state
        return state


class _Autonomous:
    RESEARCH_END_EXCLUSIVE_TS = 10_000


def test_array_close_lookup_is_exact_grid_only():
    ts = np.asarray([900, 1800, 2700], dtype=np.int64)
    close = np.asarray([10.0, 11.0, 12.0], dtype=np.float64)
    lookup = v42._CloseLookup(ts, close)
    assert lookup.get(900) == 10.0
    assert lookup.get(2700) == 12.0
    assert lookup.get(901) is None
    assert lookup.get(3600) is None


def test_stale_queued_state_uses_actual_future_truth(monkeypatch):
    core = _Core()
    core.persisted['transition'] = {
        'status': 'CERTIFICATION_QUEUED_BACKGROUND',
        'replay_complete_detected_at': int(time.time()) - 1000,
        'ready_after': 0,
    }
    monkeypatch.setattr(v42, '_fast_replay_complete', lambda _c, _a: True)
    state = v42._reconcile_transition(core, _Autonomous(), _Transition())
    assert state['status'] == 'CERTIFICATION_RETRY_READY'
    assert state['stale_queue_previous_status'] == 'CERTIFICATION_QUEUED_BACKGROUND'


def test_canonical_expression_preserves_fixed_source_priority():
    expr = v42._canonical_expr('c')
    gate = expr.index("source='gate'")
    bybit = expr.index("source='bybit'")
    binance = expr.index("source='binance'")
    okx = expr.index("source='okx'")
    bitget = expr.index("source='bitget'")
    assert gate < bybit < binance < okx < bitget
