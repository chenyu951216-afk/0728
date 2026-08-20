from __future__ import annotations

import v63_score_arbiter_notifications as v63


class Auto:
    MIN_OOS_FILLS = 80
    MIN_OOS_PF = 1.30
    MIN_OOS_EV_R = 0.10
    MAX_OOS_DD_R = 10.0
    MIN_BOOTSTRAP_CI05 = 0.0
    MIN_WF_STABILITY = 0.65
    MIN_PROFITABLE_FOLDS = 0.66
    MIN_WORST_FOLD_EV = -0.05


def metrics(**kw):
    base = {
        'oos_fills': 100,
        'profit_factor': 1.4,
        'expectancy_r': 0.12,
        'max_drawdown_r': 8.0,
        'bootstrap_ci05_r': 0.03,
        'stability': 0.8,
        'profitable_folds': 0.67,
        'worst_fold_ev': -0.02,
        'invalid_future_paths': 0,
    }
    base.update(kw)
    return base


def test_score_caps_sum_to_100_and_components_never_exceed_caps():
    s = v63.historical_score(metrics(profit_factor=99, expectancy_r=99, bootstrap_ci05_r=99,
                                     stability=99, profitable_folds=99, worst_fold_ev=99,
                                     max_drawdown_r=0.01, oos_fills=9999))
    assert sum(v63.HIST_CAPS.values()) == 100.0
    assert 0 <= s['score_total'] <= 100
    for name, value in s['components'].items():
        assert 0 <= value <= v63.HIST_CAPS[name]


def test_pf_alone_cannot_force_a_bad_strategy_through():
    m = metrics(profit_factor=20.0, expectancy_r=-0.20, bootstrap_ci05_r=-0.30,
                stability=0.0, profitable_folds=0.0, worst_fold_ev=-0.5,
                max_drawdown_r=90.0)
    ok, why = v63.score_eligible(m, Auto)
    assert ok is False
    assert why['pf_alone_can_dominate'] is False
    assert why['hard_checks_passed'] is False


def test_previous_rank2_high_pf_can_enter_paper_score_tier_but_not_strict():
    # Mirrors the user's #2: enormous OOS PF/EV/CI, but weak WF consistency.
    # V63 intentionally permits it only in the post-OOS PAPER score tier because
    # PF/EV/CI are capped and cannot contribute more than their fixed weights.
    m = metrics(oos_fills=78, profit_factor=4.58, expectancy_r=0.852,
                max_drawdown_r=9.26, bootstrap_ci05_r=0.540,
                stability=0.0, profitable_folds=0.50, worst_fold_ev=-0.269)
    ok, why = v63.score_eligible(m, Auto)
    assert ok is True
    assert why['score_total'] >= v63.HIST_MIN_SCORE
    assert 'WF stability' in why['failed_strict_gates']
    assert why['strict_historical_certified'] is False
    assert why['paper_only'] is True


def test_rank4_rank3_rank19_profiles_are_score_eligible():
    cases = [
        metrics(oos_fills=753, profit_factor=1.57, expectancy_r=.256, max_drawdown_r=70.85,
                bootstrap_ci05_r=.112, stability=.747, profitable_folds=.667, worst_fold_ev=-.029),
        metrics(oos_fills=429, profit_factor=1.31, expectancy_r=.134, max_drawdown_r=47.33,
                bootstrap_ci05_r=-.071, stability=.848, profitable_folds=1.0, worst_fold_ev=.119),
        metrics(oos_fills=38, profit_factor=1.37, expectancy_r=.048, max_drawdown_r=1.77,
                bootstrap_ci05_r=-.037, stability=.976, profitable_folds=1.0, worst_fold_ev=.018),
    ]
    for m in cases:
        ok, why = v63.score_eligible(m, Auto)
        assert ok, why
        assert why['score_total'] >= v63.HIST_MIN_SCORE


def test_top_card_and_nonentry_reason_ui_are_injected():
    html = '<html><body><main><h1>X</h1></main></body></html>'
    out = v63._inject(html)
    assert out.index('v63-top-authority') < out.index('<h1>X</h1>')
    assert '/api/v63/score-authority' in out
    assert '目前不進場' in out
