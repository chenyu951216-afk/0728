from __future__ import annotations

import v62_relaxed_multistrategy_provisional as v62


class Auto:
    MIN_OOS_FILLS = 80
    MIN_OOS_PF = 1.30
    MIN_OOS_EV_R = .10
    MAX_OOS_DD_R = 10.0
    MIN_BOOTSTRAP_CI05 = 0.0
    MIN_WF_STABILITY = .65
    MIN_PROFITABLE_FOLDS = .66
    MIN_WORST_FOLD_EV = -.05


def m(**patch):
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


def test_rank4_passes_relaxed_provisional():
    ok, why = v62.eligible(m(), Auto)
    assert ok is True
    assert why['strict_historical_certified'] is False
    assert why['failed_strict_gates'] == ['OOS DD']


def test_rank3_passes_despite_slight_negative_ci_and_high_dd():
    ok, why = v62.eligible(m(oos_fills=429, profit_factor=1.31, expectancy_r=.134,
                                max_drawdown_r=47.333, total_oos_r=57.486,
                                bootstrap_ci05_r=-.071, stability=.848,
                                profitable_folds=1.0, worst_fold_ev=.119), Auto)
    assert ok is True
    assert 'Bootstrap CI05' in why['failed_strict_gates']
    assert 'OOS DD' in why['failed_strict_gates']
    assert why['checks']['high_dd_quality_guard'] is True


def test_rank19_small_sample_can_pass_only_with_exceptional_wf_consistency():
    ok, why = v62.eligible(m(oos_fills=38, profit_factor=1.37, expectancy_r=.048,
                                max_drawdown_r=1.77, total_oos_r=1.824,
                                bootstrap_ci05_r=-.037, stability=.976,
                                profitable_folds=1.0, worst_fold_ev=.018), Auto)
    assert ok is True
    assert why['checks']['small_sample_quality_guard'] is True


def test_rank21_like_candidate_also_passes_relaxed_paper():
    ok, _ = v62.eligible(m(oos_fills=57, profit_factor=1.39, expectancy_r=.052,
                             max_drawdown_r=2.54, total_oos_r=2.964,
                             bootstrap_ci05_r=-.058, stability=.975,
                             profitable_folds=1.0, worst_fold_ev=.016), Auto)
    assert ok is True


def test_rank2_pf_is_not_enough_when_walk_forward_is_unstable():
    ok, why = v62.eligible(m(oos_fills=78, profit_factor=4.58, expectancy_r=.852,
                                max_drawdown_r=9.26, total_oos_r=66.456,
                                bootstrap_ci05_r=.540, stability=0.0,
                                profitable_folds=.50, worst_fold_ev=-.269), Auto)
    assert ok is False
    assert why['checks']['wf_stability'] is False
    assert why['checks']['profitable_folds'] is False
    assert why['checks']['worst_fold_ev_r'] is False


def test_relaxed_tier_is_still_paper_only_not_strict_certification():
    assert v62.TIER == 'PROVISIONAL_RELAXED_PAPER'
    assert v62.MAX_PROVISIONALS >= 4
    assert v62.MIN_PF >= 1.30
    assert v62.MIN_EV_R >= .04
    assert v62.MIN_WF_STABILITY >= .70
