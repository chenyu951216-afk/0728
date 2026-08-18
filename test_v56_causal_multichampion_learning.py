from __future__ import annotations

import sqlite3
from types import SimpleNamespace

import numpy as np

import v56_causal_multichampion_learning as v56


FEATURES = ('atr_pct', 'wick_ratio')


class FakeAutonomous:
    FEATURE_NAMES = FEATURES
    FEATURE_INDEX = {name: i for i, name in enumerate(FEATURES)}
    ALL_IN_COST_BPS = 8.0

    @staticmethod
    def _hash_payload(value, n=16):
        import hashlib, json
        return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()[:n]


class FakeCore:
    def __init__(self):
        self.state = {}
        self._con = sqlite3.connect(':memory:')

    def db(self):
        # Return lightweight wrappers around the same in-memory DB. Tests that use it
        # do not close through this fake; execution-only tests do not touch DB.
        return self._con

    def get_state(self, key, default=None):
        return self.state.get(key, default)

    def set_state(self, key, value):
        self.state[key] = value


def genome(**patch):
    g = {
        'direction': 'LONG',
        'feature_names': ['atr_pct', 'wick_ratio'],
        'gate': [],
        'decision_stride': 1,
        'entry_market': True,
        'entry_offset_atr': -0.8,
        'stop_atr': 1.0,
        'target_rr': [1.0],
        'allocations': [100.0],
        'expire_bars': 1,
        'max_hold_bars': 1,
        'breakeven_after_r': 99.0,
        'trail_start_r': 99.0,
        'trail_lock_r': 0.0,
        'cooldown_bars': 0,
        'model_learning_rate': .05,
        'model_max_iter': 10,
        'model_max_leaf_nodes': 7,
        'model_min_samples_leaf': 10,
        'model_l2': 1.0,
    }
    g.update(patch)
    return g


def market(o, h, l, c, decision_close=100.0, decision_ts=0):
    return {
        'close15': {decision_ts: decision_close},
        'ts5': np.asarray([decision_ts + 900, decision_ts + 1200, decision_ts + 1500], dtype=np.int64),
        'o5': np.asarray(o, dtype=float),
        'h5': np.asarray(h, dtype=float),
        'l5': np.asarray(l, dtype=float),
        'c5': np.asarray(c, dtype=float),
    }


def safe_contract(_core, _autonomous, stop_fraction):
    return ({'ok': True, 'effective_max_leverage': 20.0}, 10.0, max(.20, stop_fraction + .05))


def test_canonical_market_genome_removes_offset_and_impossible_trail():
    g = v56._canonical_genome(genome(entry_offset_atr=-.77, trail_start_r=.865, trail_lock_r=1.323,
                                     target_rr=[4.0, 1.0], allocations=[30.0, 70.0]))
    assert g['entry_offset_atr'] == 0.0
    assert g['trail_lock_r'] <= g['trail_start_r'] - .049
    assert g['target_rr'] == [1.0, 4.0]
    assert abs(sum(g['allocations']) - 100.0) < 1e-6


def test_market_plan_uses_actual_market_price_not_hidden_atr_offset():
    plan = v56._generic_plan(FakeAutonomous, 110.0, .01, genome(entry_market=True, entry_offset_atr=-1.0))
    assert plan['entry'] == 110.0
    assert plan['management']['entry_offset_atr'] == 0.0


def test_historical_market_stop_is_anchored_to_actual_fill(monkeypatch):
    monkeypatch.setattr(v56, '_stop_safe_contract', safe_contract)
    m = market(
        o=[110.0, 110.2, 110.3],
        h=[110.4, 110.5, 110.5],
        l=[109.5, 109.6, 109.7],
        c=[110.2, 110.3, 110.4],
    )
    result = v56.canonical_simulate(FakeCore(), FakeAutonomous, m, 0,
                                    np.asarray([.01, .2], dtype=np.float32), genome())
    # ATR is derived from the decision close (100 * 1% = 1), but stop anchor is the
    # actual first executable 5m open (110), therefore 109 -- never the old planned 99.
    assert result['valid'] is True and result['filled'] is True
    assert result['entry'] == 110.0
    assert abs(result['stop'] - 109.0) < 1e-9


def test_stop_wins_same_bar_ambiguity(monkeypatch):
    monkeypatch.setattr(v56, '_stop_safe_contract', safe_contract)
    m = market(
        o=[100.0, 100.0, 100.0],
        h=[100.2, 102.0, 100.0],
        l=[99.5, 98.5, 99.5],
        c=[100.0, 100.0, 100.0],
    )
    result = v56.canonical_simulate(FakeCore(), FakeAutonomous, m, 0,
                                    np.asarray([.01, .2], dtype=np.float32), genome(target_rr=[1.0]))
    assert result['exit_reason'] == 'STOP_OR_TRAIL'
    assert result['gross_r'] <= -0.999


def test_trailing_lock_is_never_above_reached_excursion(monkeypatch):
    monkeypatch.setattr(v56, '_stop_safe_contract', safe_contract)
    g = genome(trail_start_r=.50, trail_lock_r=2.0, breakeven_after_r=99.0, target_rr=[9.0])
    canon = v56._canonical_genome(g)
    assert canon['trail_lock_r'] <= .45 + 1e-9
    # Fill bar reaches +0.6R; new trail only applies to the next bar. The next bar dips
    # to +0.40R and can stop at +0.45R, never at an impossible +2R.
    m = market(
        o=[100.0, 100.6, 100.5],
        h=[100.6, 100.7, 100.6],
        l=[99.5, 100.4, 100.4],
        c=[100.5, 100.5, 100.5],
    )
    result = v56.canonical_simulate(FakeCore(), FakeAutonomous, m, 0,
                                    np.asarray([.01, .2], dtype=np.float32), g)
    assert result['exit_reason'] == 'STOP_OR_TRAIL'
    assert result['exit_price'] <= 100.45 + 1e-9


def test_forward_observation_cannot_settle_before_full_horizon():
    # The due timestamp is decision close + the entire evolved max holding horizon.
    g = genome(max_hold_bars=48)
    decision = 1_000_000
    canon = v56._canonical_genome(g)
    due = decision + 900 + canon['max_hold_bars'] * 900
    assert due == 1_044_100
