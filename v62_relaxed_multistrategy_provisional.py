from __future__ import annotations

"""V62 relaxed multi-strategy provisional Current Paper authority.

Strict 9/9 historical certification remains unchanged. V62 only broadens the PAPER-ONLY
provisional tier after terminal OOS so statistically useful frozen finalists are not
thrown away solely because the strict production gate is intentionally conservative.

The relaxed tier is still not allowed to rewrite historical OOS or claim strict
certification. Current-time forward evidence remains authoritative for quarantine and
future confirmation.
"""

import os
import time
from typing import Any

import runtime_identity

VERSION = 'V62_RELAXED_MULTISTRATEGY_PROVISIONAL_PAPER'
SCHEMA = 62
STATE_KEY = 'v62_relaxed_multistrategy_provisional'
TIER = 'PROVISIONAL_RELAXED_PAPER'

MAX_PROVISIONALS = max(1, min(6, int(os.getenv('AUTONOMOUS_V62_MAX_PROVISIONALS', '4'))))
MIN_FILLS = max(20, int(os.getenv('AUTONOMOUS_V62_MIN_FILLS', '30')))
MIN_PF = max(1.15, float(os.getenv('AUTONOMOUS_V62_MIN_PF', '1.30')))
MIN_EV_R = max(.02, float(os.getenv('AUTONOMOUS_V62_MIN_EV_R', '.04')))
MIN_CI05_R = max(-.20, float(os.getenv('AUTONOMOUS_V62_MIN_CI05_R', '-.08')))
MIN_WF_STABILITY = max(.50, float(os.getenv('AUTONOMOUS_V62_MIN_WF_STABILITY', '.70')))
MIN_PROFITABLE_FOLDS = max(.50, float(os.getenv('AUTONOMOUS_V62_MIN_PROFITABLE_FOLDS', '.66')))
MIN_WORST_FOLD_EV_R = max(-.20, float(os.getenv('AUTONOMOUS_V62_MIN_WORST_FOLD_EV_R', '-.08')))
MAX_DD_R = max(10.0, float(os.getenv('AUTONOMOUS_V62_MAX_DD_R', '80.0')))
MIN_RETURN_DD = max(.50, float(os.getenv('AUTONOMOUS_V62_MIN_RETURN_DD', '1.00')))

# A small OOS sample is allowed only when the chronological WF evidence is unusually
# consistent. This lets low-count but stable candidates continue in Paper without
# treating a lucky high PF as sufficient evidence.
SMALL_SAMPLE_FILLS = max(MIN_FILLS, int(os.getenv('AUTONOMOUS_V62_SMALL_SAMPLE_FILLS', '80')))
SMALL_SAMPLE_MIN_PF = max(MIN_PF, float(os.getenv('AUTONOMOUS_V62_SMALL_SAMPLE_MIN_PF', '1.35')))
SMALL_SAMPLE_MIN_STABILITY = max(MIN_WF_STABILITY, float(os.getenv('AUTONOMOUS_V62_SMALL_SAMPLE_MIN_STABILITY', '.90')))
SMALL_SAMPLE_MIN_PROFITABLE_FOLDS = max(MIN_PROFITABLE_FOLDS, float(os.getenv('AUTONOMOUS_V62_SMALL_SAMPLE_MIN_PROFITABLE_FOLDS', '.99')))
SMALL_SAMPLE_MIN_WORST_FOLD_EV_R = max(MIN_WORST_FOLD_EV_R, float(os.getenv('AUTONOMOUS_V62_SMALL_SAMPLE_MIN_WORST_FOLD_EV_R', '0.0')))

# High absolute drawdown is permitted only with a large OOS sample and positive EV.
HIGH_DD_TRIGGER_R = max(10.0, float(os.getenv('AUTONOMOUS_V62_HIGH_DD_TRIGGER_R', '20.0')))
HIGH_DD_MIN_FILLS = max(100, int(os.getenv('AUTONOMOUS_V62_HIGH_DD_MIN_FILLS', '300')))
HIGH_DD_MIN_EV_R = max(MIN_EV_R, float(os.getenv('AUTONOMOUS_V62_HIGH_DD_MIN_EV_R', '.12')))

_INSTALLED = False


def _now() -> int:
    return int(time.time())


def _f(value: Any, default: float = 0.0) -> float:
    try:
        x = float(value)
        return x if x == x and abs(x) != float('inf') else default
    except (TypeError, ValueError):
        return default


def _strict_failed(metrics: dict[str, Any], autonomous: Any) -> list[str]:
    failures: list[str] = []
    if int(metrics.get('oos_fills') or 0) < int(autonomous.MIN_OOS_FILLS): failures.append('OOS fills')
    if _f(metrics.get('profit_factor')) < float(autonomous.MIN_OOS_PF): failures.append('OOS PF')
    if _f(metrics.get('expectancy_r')) < float(autonomous.MIN_OOS_EV_R): failures.append('OOS EV')
    if _f(metrics.get('max_drawdown_r'), 1e9) > float(autonomous.MAX_OOS_DD_R): failures.append('OOS DD')
    if _f(metrics.get('bootstrap_ci05_r'), -1e9) <= float(autonomous.MIN_BOOTSTRAP_CI05): failures.append('Bootstrap CI05')
    if int(metrics.get('invalid_future_paths') or 0) != 0: failures.append('Invalid future paths')
    if _f(metrics.get('stability')) < float(autonomous.MIN_WF_STABILITY): failures.append('WF stability')
    if _f(metrics.get('profitable_folds')) < float(autonomous.MIN_PROFITABLE_FOLDS): failures.append('Profitable folds')
    if _f(metrics.get('worst_fold_ev'), -1e9) < float(autonomous.MIN_WORST_FOLD_EV): failures.append('Worst fold EV')
    return failures


def eligible(metrics: dict[str, Any], autonomous: Any) -> tuple[bool, dict[str, Any]]:
    fills = int(metrics.get('oos_fills') or 0)
    pf = _f(metrics.get('profit_factor'))
    ev = _f(metrics.get('expectancy_r'))
    ci = _f(metrics.get('bootstrap_ci05_r'), -999.0)
    dd = _f(metrics.get('max_drawdown_r'), 1e9)
    stability = _f(metrics.get('stability'))
    profitable = _f(metrics.get('profitable_folds'))
    worst = _f(metrics.get('worst_fold_ev'), -999.0)
    invalid = int(metrics.get('invalid_future_paths') or 0)
    total = _f(metrics.get('total_oos_r'), ev * fills)
    ratio = total / max(dd, 1e-9) if total > 0 and dd > 0 else 0.0

    core_checks = {
        'fills': fills >= MIN_FILLS,
        'pf': pf >= MIN_PF,
        'ev_r': ev >= MIN_EV_R,
        'ci05_r': ci >= MIN_CI05_R,
        'wf_stability': stability >= MIN_WF_STABILITY,
        'profitable_folds': profitable >= MIN_PROFITABLE_FOLDS,
        'worst_fold_ev_r': worst >= MIN_WORST_FOLD_EV_R,
        'invalid_paths_zero': invalid == 0,
        'dd_hard_ceiling': dd <= MAX_DD_R,
        'return_to_drawdown': ratio >= MIN_RETURN_DD,
    }

    if fills < SMALL_SAMPLE_FILLS:
        small_sample_guard = bool(
            pf >= SMALL_SAMPLE_MIN_PF
            and stability >= SMALL_SAMPLE_MIN_STABILITY
            and profitable >= SMALL_SAMPLE_MIN_PROFITABLE_FOLDS
            and worst >= SMALL_SAMPLE_MIN_WORST_FOLD_EV_R
        )
    else:
        small_sample_guard = True

    if dd > HIGH_DD_TRIGGER_R:
        high_dd_guard = bool(fills >= HIGH_DD_MIN_FILLS and ev >= HIGH_DD_MIN_EV_R)
    else:
        high_dd_guard = True

    checks = {**core_checks, 'small_sample_quality_guard': small_sample_guard,
              'high_dd_quality_guard': high_dd_guard}
    failed_strict = _strict_failed(metrics, autonomous)
    return bool(all(checks.values())), {
        'failed_strict_gates': failed_strict,
        'checks': checks,
        'return_to_drawdown': ratio,
        'paper_only': True,
        'strict_historical_certified': False,
        'selection_after_oos_visibility': True,
        'thresholds': {
            'min_fills': MIN_FILLS, 'min_pf': MIN_PF, 'min_ev_r': MIN_EV_R,
            'min_ci05_r': MIN_CI05_R, 'min_wf_stability': MIN_WF_STABILITY,
            'min_profitable_folds': MIN_PROFITABLE_FOLDS,
            'min_worst_fold_ev_r': MIN_WORST_FOLD_EV_R, 'max_dd_r': MAX_DD_R,
            'min_return_to_drawdown': MIN_RETURN_DD,
            'small_sample_fills': SMALL_SAMPLE_FILLS,
            'small_sample_min_pf': SMALL_SAMPLE_MIN_PF,
            'small_sample_min_stability': SMALL_SAMPLE_MIN_STABILITY,
            'small_sample_min_profitable_folds': SMALL_SAMPLE_MIN_PROFITABLE_FOLDS,
            'small_sample_min_worst_fold_ev_r': SMALL_SAMPLE_MIN_WORST_FOLD_EV_R,
            'high_dd_trigger_r': HIGH_DD_TRIGGER_R,
            'high_dd_min_fills': HIGH_DD_MIN_FILLS,
            'high_dd_min_ev_r': HIGH_DD_MIN_EV_R,
        },
    }


def install(production: Any, autonomous: Any, pipeline52: Any, pipeline: Any,
            v56: Any, v61: Any) -> None:
    global _INSTALLED
    if _INSTALLED:
        return
    _INSTALLED = True
    core = production.core

    # Configure the proven V61 handoff/refit machinery BEFORE its worker starts.
    v61.TIER = TIER
    v61.MAX_PROVISIONALS = MAX_PROVISIONALS
    v61._eligible = eligible

    base_refit = v61._refit_frozen_package
    def refit(core_: Any, autonomous_: Any, row: dict[str, Any]) -> dict[str, Any]:
        out = base_refit(core_, autonomous_, row)
        metrics = dict(out.get('metrics') or {})
        why = dict(row.get('rationale') or {})
        metrics.update({
            'schema': SCHEMA,
            'certification_tier': TIER,
            'strict_historical_certified': False,
            'strict_historical_failed_gate': None,
            'strict_historical_failed_gates': list(why.get('failed_strict_gates') or []),
            'selection_after_oos_visibility': True,
            'paper_only': True,
            'forward_confirmation_required': True,
            'historical_oos_rewritten': False,
            'historical_oos_frozen': True,
            'provisional_reason': (
                'V62 relaxed provisional paper gate passed; strict 9/9 historical verdict '
                'remains rejected and current-time forward evidence is required.'
            ),
            'v62_gate': why,
        })
        out['metrics'] = metrics
        return out
    v61._refit_frozen_package = refit

    base_api = v61._api
    def api(core_: Any, autonomous_: Any) -> dict[str, Any]:
        z = dict(base_api(core_, autonomous_) or {})
        z.update({'schema': SCHEMA, 'runtime': VERSION, 'tier': TIER,
                  'max_provisionals': MAX_PROVISIONALS})
        z['rules'] = {
            'strict_9_of_9_certification_unchanged': True,
            'historical_oos_rewritten': False,
            'paper_only': True,
            'future_only_confirmation': True,
            'future_peeking_enabled': False,
            'pf_alone_is_sufficient': False,
            'small_sample_requires_stronger_wf_consistency': True,
            'high_dd_requires_large_sample_and_positive_ev': True,
        }
        z['relaxed_thresholds'] = {
            'min_fills': MIN_FILLS, 'min_pf': MIN_PF, 'min_ev_r': MIN_EV_R,
            'min_ci05_r': MIN_CI05_R, 'min_wf_stability': MIN_WF_STABILITY,
            'min_profitable_folds': MIN_PROFITABLE_FOLDS,
            'min_worst_fold_ev_r': MIN_WORST_FOLD_EV_R,
            'max_dd_r': MAX_DD_R, 'min_return_to_drawdown': MIN_RETURN_DD,
        }
        return z
    v61._api = api

    base_inject = v61._inject
    def inject(html: str) -> str:
        out = base_inject(html)
        out = out.replace('🟠 V61 風險調整 Provisional Current Paper',
                          '🟠 V62 多策略 Provisional Current Paper')
        out = out.replace('讀取 V61 狀態…', '讀取 V62 狀態…')
        out = out.replace('沒有符合 V61 高可信 sole-DD 例外的策略。',
                          '沒有符合 V62 放寬但仍受控的 Provisional Paper 策略。')
        out = out.replace('V61 讀取失敗：', 'V62 讀取失敗：')
        return out
    v61._inject = inject

    # V61 now installs with V62 eligibility/tier already in place, so no race can occur
    # between the terminal worker and the relaxed policy.
    v61.install(production, autonomous, pipeline52, pipeline, v56)

    if not any(getattr(r, 'path', None) == '/api/v62/provisional' for r in core.app.router.routes):
        core.app.add_api_route('/api/v62/provisional', lambda: api(core, autonomous),
                               methods=['GET'], name='v62_provisional')

    core.state[STATE_KEY] = {
        'schema': SCHEMA, 'runtime': VERSION, 'status': 'READY',
        'tier': TIER, 'max_provisionals': MAX_PROVISIONALS,
        'strict_historical_oos_changed': False, 'historical_oos_rewritten': False,
        'paper_only': True, 'future_peeking_enabled': False,
        'current_forward_confirmation_required': True,
        'updated_at': _now(),
    }
    role = core.state.get('bootstrap_replica_role')
    if isinstance(role, dict):
        role.update({
            'final_runtime_overlay': VERSION,
            'provisional_paper_authority': VERSION,
            'relaxed_multistrategy_provisional': True,
            'strict_historical_oos_rewritten_by_v62': False,
            'pf_alone_is_sufficient_for_v62': False,
        })
    runtime_identity.stamp(core)
