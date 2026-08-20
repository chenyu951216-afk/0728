from __future__ import annotations

import v61_risk_adjusted_provisional as v61


class Auto:
    MIN_OOS_FILLS = 80
    MIN_OOS_PF = 1.30
    MIN_OOS_EV_R = .10
    MAX_OOS_DD_R = 10.0
    MIN_BOOTSTRAP_CI05 = 0.0
    MIN_WF_STABILITY = .65
    MIN_PROFITABLE_FOLDS = .66
    MIN_WORST_FOLD_EV = -.05


def metrics(**patch):
    base = {
        'oos_fills': 753,
        'profit_factor': 1.57,
        'expectancy_r': .256,
        'max_drawdown_r': 70.846,
        'total_oos_r': 192.768,
        'bootstrap_ci05_r': .112,
        'invalid_future_paths': 0,
        'stability': .747,
        'profitable_folds': .667,
        'worst_fold_ev': -.029,
    }
    base.update(patch)
    return base


def test_rank4_like_candidate_is_provisional_eligible_but_not_strict_certified():
    ok, why = v61._eligible(metrics(), Auto)
    assert ok is True
    assert why['failed_strict_gates'] == ['OOS DD']
    assert why['return_to_drawdown'] > 2.5


def test_candidate_with_negative_ci_is_not_admitted():
    ok, why = v61._eligible(metrics(max_drawdown_r=47.333, bootstrap_ci05_r=-.071,
                                    profit_factor=1.31, expectancy_r=.134,
                                    total_oos_r=57.486, stability=.848,
                                    profitable_folds=1.0, worst_fold_ev=.119), Auto)
    assert ok is False
    assert 'Bootstrap CI05' in why['failed_strict_gates']


def test_candidate_with_bad_walk_forward_is_not_admitted_even_if_oos_is_great():
    ok, why = v61._eligible(metrics(oos_fills=78, profit_factor=4.58, expectancy_r=.852,
                                    max_drawdown_r=9.26, total_oos_r=66.456,
                                    bootstrap_ci05_r=.540, stability=0.0,
                                    profitable_folds=.50, worst_fold_ev=-.269), Auto)
    assert ok is False
    assert 'WF stability' in why['failed_strict_gates']
    assert 'Worst fold EV' in why['failed_strict_gates']


def test_absolute_dd_exception_still_has_hard_ceiling_and_return_dd_quality_gate():
    ok, why = v61._eligible(metrics(max_drawdown_r=100.0, total_oos_r=400.0), Auto)
    assert ok is False
    assert why['checks']['dd_hard_ceiling'] is False
    ok2, why2 = v61._eligible(metrics(max_drawdown_r=70.0, total_oos_r=100.0), Auto)
    assert ok2 is False
    assert why2['checks']['return_to_drawdown'] is False


def test_v61_never_claims_posthoc_exception_is_strict_historical_certification():
    assert v61.TIER == 'PROVISIONAL_RISK_ADJUSTED_PAPER'
    assert v61.MIN_PF >= 1.50
    assert v61.MIN_EV_R >= .20
    assert v61.MIN_CI05_R >= .08
    assert v61.FORWARD_DD_QUARANTINE_R <= 12.0
