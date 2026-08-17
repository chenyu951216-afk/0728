from __future__ import annotations

import random
from types import SimpleNamespace

import numpy as np

import v30_autonomous_strategy_discovery as legacy
import v43_unified_performance_authority as v43


class _Core:
    def __init__(self):
        self.state = {}


class _Leverage:
    @staticmethod
    def _frozen_contract(core, autonomous, create=False):
        return {
            'ok': True,
            'notional_usdt': 20000.0,
            'conservative_stop_headroom_fraction': 1.0,
            'effective_max_leverage': 5.0,
            'maintenance_margin_rate': 0.005,
        }


class _Autonomous:
    FEATURE_INDEX = legacy.FEATURE_INDEX
    ALL_IN_COST_BPS = legacy.ALL_IN_COST_BPS
    PAPER_NOTIONAL_USDT = 20000.0


def _market(seed: int = 7):
    rng = np.random.default_rng(seed)
    start = 1_700_000_100
    start -= start % 300
    n = 12000
    ts = np.arange(start, start + n * 300, 300, dtype=np.int64)
    close = 1900.0 + np.cumsum(rng.normal(0.0, 1.5, n))
    opened = np.r_[close[0], close[:-1]]
    span = np.abs(rng.normal(1.3, 0.6, n)) + 0.15
    high = np.maximum(opened, close) + span
    low = np.minimum(opened, close) - span
    decisions = np.arange(start + 1800, start + 2200 * 900, 900, dtype=np.int64)
    close15 = {int(t): float(1900.0 + rng.normal(0.0, 18.0)) for t in decisions}
    return {
        'source5': 'canonical-sql-fixed-priority-v42',
        'source15': 'canonical-sql-fixed-priority-v42',
        'ts5': ts,
        'o5': opened,
        'h5': high,
        'l5': low,
        'c5': close,
        'close15': close15,
    }, decisions


def _base_equivalent(a: dict, b: dict) -> bool:
    for key in ('valid', 'filled', 'reason'):
        if a.get(key) != b.get(key):
            return False
    for key in ('pnl_r', 'gross_r', 'cost_r', 'entry', 'stop', 'fill_ts'):
        if key not in a and key not in b:
            continue
        if key not in a or key not in b:
            return False
        if key == 'fill_ts':
            if int(a[key]) != int(b[key]):
                return False
        elif not np.isclose(float(a[key]), float(b[key]), rtol=1e-10, atol=1e-10):
            return False
    return True


def test_fast_trade_matches_legacy_trade_semantics_across_random_packages():
    market, decisions = _market()
    rng = random.Random(20260817)
    features = np.zeros(len(legacy.FEATURE_NAMES), dtype=np.float32)
    features[legacy.FEATURE_INDEX['atr_pct']] = 0.0065
    core = _Core()

    for i in range(96):
        genome = legacy._new_genome(rng)
        # Keep the test inside a fully available future horizon; V33's production
        # wrapper separately fail-closes an incomplete evolved holding horizon.
        genome['max_hold_bars'] = rng.choice((1, 2, 4, 8, 16, 32, 64, 96, 192))
        ts = int(decisions[i * 7])
        old = legacy._simulate_trade(market, ts, features, genome)
        fast = v43._fast_trade(core, _Autonomous, _Leverage, market, ts, features, genome)
        assert _base_equivalent(old, fast), (genome, ts, old, fast)


def test_fast_trade_preserves_full_horizon_fail_closed_rule():
    market, decisions = _market()
    features = np.zeros(len(legacy.FEATURE_NAMES), dtype=np.float32)
    features[legacy.FEATURE_INDEX['atr_pct']] = 0.005
    genome = legacy._new_genome(random.Random(11))
    genome['entry_market'] = True
    genome['max_hold_bars'] = 1152
    # Pick a decision too close to the end for the complete evolved holding horizon.
    ts = int(market['ts5'][-100]) - 900
    market['close15'][ts] = float(market['c5'][-103])
    out = v43._fast_trade(_Core(), _Autonomous, _Leverage, market, ts, features, genome)
    assert out['valid'] is False
    assert out['reason'] == 'incomplete_full_evolved_holding_horizon'


def test_frozen_execution_contract_is_read_once_on_historical_hot_path():
    v43._FROZEN_CONTRACT.clear()
    calls = {'n': 0}

    class Lev:
        @staticmethod
        def _frozen_contract(core, autonomous, create=False):
            calls['n'] += 1
            return {'ok': True, 'notional_usdt': 20000.0, 'fetched': calls['n']}

    core = _Core()
    auto = SimpleNamespace(PAPER_NOTIONAL_USDT=20000.0)
    v43._install_frozen_contract_cache(core, auto, Lev)
    for _ in range(20):
        assert Lev._frozen_contract(core, auto, create=False)['ok']
    assert calls['n'] == 1
    # An explicit create/refresh is still authoritative and refreshes the memory copy.
    Lev._frozen_contract(core, auto, create=True)
    assert calls['n'] == 2


def test_decision_mask_cache_is_semantically_identical():
    v43._DECISION_MASK_CACHE.clear()
    v43._DECISION_MASK_TOKEN = None
    calls = {'n': 0}

    def original(ts, stride):
        calls['n'] += 1
        slot = (ts // 900).astype(np.int64)
        return (slot % int(stride)) == 0

    auto = SimpleNamespace(_decision_mask=original)
    core = _Core()
    v43._install_decision_mask_cache(core, auto)
    ts = np.arange(1_700_000_100, 1_700_000_100 + 500 * 900, 900, dtype=np.int64)
    expected = original(ts, 8)
    a = auto._decision_mask(ts, 8)
    b = auto._decision_mask(ts, 8)
    assert np.array_equal(a, expected)
    assert np.array_equal(b, expected)
    # one direct expected call + one cache-populating call; the second cached call adds none
    assert calls['n'] == 2
