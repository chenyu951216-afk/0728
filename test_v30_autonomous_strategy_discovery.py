from __future__ import annotations

import inspect
import random
from pathlib import Path

import numpy as np

import v30_autonomous_strategy_discovery as auto


def _genome(**patch):
    g = auto._new_genome(random.Random(7))
    g.update({
        'direction': 'LONG',
        'feature_names': ['atr_pct', 'ret_1', 'ret_4'],
        'gate': [],
        'decision_stride': 1,
        'entry_market': True,
        'entry_offset_atr': 0.0,
        'stop_atr': 1.0,
        'target_rr': [0.5],
        'allocations': [100.0],
        'expire_bars': 1,
        'max_hold_bars': 1,
        'breakeven_after_r': 99.0,
        'trail_start_r': 99.0,
        'trail_lock_r': 0.0,
    })
    g.update(patch)
    return g


def _features(atr_pct=.01):
    x = np.zeros(len(auto.FEATURE_NAMES), dtype=np.float32)
    x[auto.FEATURE_INDEX['atr_pct']] = atr_pct
    return x


def test_autonomous_model_excludes_manual_regime_codes_and_has_no_strategy_taxonomy():
    for name in auto.EXCLUDED_FEATURES:
        assert name not in auto.FEATURE_NAMES
    g = auto._new_genome(random.Random(123))
    assert 'strategy' not in g
    assert g['direction'] in ('LONG', 'SHORT')
    assert abs(sum(g['allocations']) - 100.0) < 0.05
    src = inspect.getsource(auto._evaluate_candidate)
    assert "['success']" not in src
    assert 'strategy_affinity' not in src


def test_future_path_starts_only_after_decision_close_and_fill_bar_gets_no_target_credit():
    ts = 1_000
    market = {
        'close15': {ts: 100.0},
        'ts5': np.array([1_900, 2_200, 2_500], dtype=np.int64),
        'o5': np.array([100.0, 100.0, 100.0]),
        'h5': np.array([101.0, 100.1, 100.1]),
        'l5': np.array([99.5, 99.9, 99.9]),
        'c5': np.array([100.0, 100.0, 100.0]),
    }
    result = auto._simulate_trade(market, ts, _features(), _genome())
    assert result['valid'] is True and result['filled'] is True
    # 100.5 target is touched on the fill bar but must not receive credit.
    assert result['gross_r'] <= 1e-9


def test_intrabar_ambiguity_is_stop_first_and_missing_5m_path_is_rejected():
    ts = 1_000
    market = {
        'close15': {ts: 100.0},
        'ts5': np.array([1_900, 2_200], dtype=np.int64),
        'o5': np.array([100.0, 100.0]),
        'h5': np.array([101.0, 100.0]),
        'l5': np.array([98.0, 100.0]),
        'c5': np.array([100.0, 100.0]),
    }
    stopped = auto._simulate_trade(market, ts, _features(), _genome())
    assert stopped['valid'] is True and stopped['pnl_r'] < -0.99

    market_gap = dict(market)
    market_gap['ts5'] = np.array([1_900, 2_500], dtype=np.int64)
    invalid = auto._simulate_trade(market_gap, ts, _features(), _genome())
    assert invalid['valid'] is False
    assert invalid['reason'] == 'future_5m_gap'


def test_training_gate_quantiles_are_fit_from_training_matrix_only():
    x = np.zeros((100, len(auto.FEATURE_NAMES)), dtype=np.float32)
    idx = auto.FEATURE_INDEX['ret_4']
    x[:, idx] = np.arange(100, dtype=np.float32)
    gate = [{'feature': 'ret_4', 'op': 'GE', 'quantile': .5}]
    fitted = auto._gate_thresholds(x[:50], gate)
    assert len(fitted) == 1
    # It must use only the first 0..49 training slice, not the later 50..99 rows.
    assert 20.0 <= fitted[0]['value'] <= 30.0


def test_production_entry_installs_all_autonomous_authorities_before_v26_capture():
    text = Path('server_entry_v27.py').read_text(encoding='utf-8')
    for module_name in (
        'v30_autonomous_strategy_discovery',
        'v31_autonomous_runtime_hardening',
        'v32_autonomous_ui_compat',
        'v33_autonomous_compute_efficiency',
        'v34_autonomous_checkpoint_recovery',
    ):
        assert module_name in text
    capture = text.index("v26_replay_transition_stability")
    for call in ('autonomous.install', 'hardening.install', 'compute.install', 'ui_compat.install', 'recovery.install'):
        assert text.index(call) < capture
    assert "1577808000" in text
    assert "HISTORICAL_RESEARCH_START_TS" in text
