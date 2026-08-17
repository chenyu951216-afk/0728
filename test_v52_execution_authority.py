from __future__ import annotations

from collections import Counter

import v51_evolution_survivability_authority as v51
import v52_execution_authority as v52


def test_safe_leverage_reduces_exchange_max_when_stop_needs_more_headroom():
    contract = {
        'effective_max_leverage': 100.0,
        'maintenance_margin_rate': 0.004,
    }
    stop_fraction = 0.020
    selected, headroom = v52.safe_leverage(contract, stop_fraction)
    assert 1.0 <= selected < 100.0
    assert headroom > stop_fraction


def test_safe_leverage_keeps_exchange_max_when_already_stop_safe():
    contract = {
        'effective_max_leverage': 10.0,
        'maintenance_margin_rate': 0.005,
    }
    selected, headroom = v52.safe_leverage(contract, 0.010)
    assert selected == 10.0
    assert headroom > 0.010


def test_leverage_rejection_is_not_misclassified_as_broken_causal_price_path():
    original = v51._reason_counts
    try:
        v52._install_reason_classifier()
        reasons = Counter()
        attempted, invalid = v51._reason_counts([
            {'valid': False, 'filled': False,
             'reason': 'initial_stop_outside_conservative_max_leverage_headroom'},
            {'valid': True, 'filled': True, 'reason': 'filled'},
        ], reasons)
        assert attempted == 2
        assert invalid == 0
        assert reasons['initial_stop_outside_conservative_max_leverage_headroom'] == 1

        attempted, invalid = v51._reason_counts([
            {'valid': False, 'filled': False, 'reason': 'future_5m_gap'},
            {'valid': False, 'filled': False, 'reason': 'missing_decision_close'},
            {'valid': True, 'filled': True, 'reason': 'filled'},
        ], Counter())
        assert attempted == 3
        assert invalid == 2
    finally:
        v51._reason_counts = original


def test_safe_leverage_math_never_uses_future_price_path():
    contract = {
        'effective_max_leverage': 50.0,
        'maintenance_margin_rate': 0.004,
    }
    first = v52.safe_leverage(contract, 0.015)
    second = v52.safe_leverage(dict(contract), 0.015)
    assert first == second
