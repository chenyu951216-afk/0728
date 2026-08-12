from __future__ import annotations

import json
import math
import os
import statistics
from collections import Counter
from typing import Any

import adaptive_v5 as signal
import execution_v7 as execution
import v8_evolution
import runtime_identity

WALKFORWARD_VERSION = runtime_identity.RUNTIME_VERSION
WALKFORWARD_SCHEMA = 2
MIN_WF_OPPORTUNITIES = max(110, int(os.getenv('EXECUTION_WF_MIN_OPPORTUNITIES', '126')))
MIN_WF_FOLDS = max(3, int(os.getenv('EXECUTION_WF_MIN_FOLDS', '3')))
MIN_FOLD_AUDIT_FILLS = max(6, int(os.getenv('EXECUTION_WF_MIN_FOLD_FILLS', '8')))
TOP_DEV_POLICIES = max(4, min(12, int(os.getenv('EXECUTION_WF_TOP_POLICIES', '6'))))


def _selection_score(stats: dict[str, float], worst_segment_ev: float, opportunity_n: int, validation: bool = False) -> float:
    """Inner dev/validation selector only; certification happens on later untouched folds."""
    min_fills = max(6 if validation else 10, min(22 if validation else 28, max(1, opportunity_n // 5)))
    if int(stats.get('fills') or 0) < min_fills or float(stats.get('fill_rate') or 0) < .12:
        return -999.0
    pf = max(float(stats.get('profit_factor') or 0), 1e-6)
    return (
        float(stats.get('expectancy_r') or 0) * 3.0
        + math.log(pf) * .24
        - float(stats.get('max_drawdown_r') or 0) * .008
        + min(int(stats.get('fills') or 0), 160) / 160.0 * .05
        + min(float(worst_segment_ev), 0.0) * 1.8
    )


def _select_policy(history: list[dict[str, Any]], data: dict[str, Any], strategy: str, direction: str) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]] | None:
    """Select a policy using only opportunities strictly before the next audit fold."""
    if len(history) < 72:
        return None
    purge = max(5, min(12, len(history) // 18))
    val_n = max(20, min(48, int(len(history) * .24)))
    dev_end = len(history) - val_n - purge
    if dev_end < 42:
        return None
    dev = history[:dev_end]
    val = history[dev_end + purge:]
    if len(val) < 18:
        return None

    ranked: list[tuple[float, dict[str, Any], dict[str, Any]]] = []
    for policy in execution.policy_candidates(strategy):
        results = [execution.simulate_policy(data, x, strategy, direction, policy) for x in dev]
        stats = execution._stats(results)
        worst, profitable = execution._segment_worst(data, dev, strategy, direction, policy)
        meta = {**stats, 'worst_segment_ev_r': worst, 'profitable_segment_ratio': profitable}
        ranked.append((_selection_score(stats, worst, len(dev), False), policy, meta))
    ranked.sort(key=lambda x: x[0], reverse=True)
    top = [x for x in ranked[:TOP_DEV_POLICIES] if x[0] > -998.0]
    if not top:
        return None

    val_ranked: list[tuple[float, dict[str, Any], dict[str, Any], dict[str, Any]]] = []
    for _, base_policy, dev_meta in top:
        for allocation in execution.ALLOCATIONS:
            policy = {**base_policy, 'allocations': list(allocation)}
            results = [execution.simulate_policy(data, x, strategy, direction, policy) for x in val]
            stats = execution._stats(results)
            score = _selection_score(stats, float(dev_meta.get('worst_segment_ev_r', -9.0)), len(val), True)
            val_ranked.append((score, policy, dev_meta, stats))
    val_ranked.sort(key=lambda x: x[0], reverse=True)
    if not val_ranked or val_ranked[0][0] <= -998.0:
        return None
    _, policy, dev_meta, val_meta = val_ranked[0]
    return policy, dev_meta, val_meta


def _walkforward_ranges(n: int) -> list[tuple[int, int]]:
    if n < MIN_WF_OPPORTUNITIES:
        return []
    initial = max(72, int(n * .42))
    remaining = n - initial
    if remaining < 54:
        return []
    folds = min(5, max(MIN_WF_FOLDS, remaining // 24))
    folds = min(folds, max(1, remaining // 18))
    if folds < MIN_WF_FOLDS:
        return []
    block = remaining // folds
    out: list[tuple[int, int]] = []
    for fold in range(folds):
        start = initial + fold * block
        end = n if fold == folds - 1 else initial + (fold + 1) * block
        if end - start >= 18:
            out.append((start, end))
    return out


def _policy_signature(policy: dict[str, Any]) -> str:
    return json.dumps({
        'entry_atr': policy.get('entry_atr'), 'stop_atr': policy.get('stop_atr'),
        'noise_floor_mult': policy.get('noise_floor_mult'),
        'structure_mode': policy.get('structure_mode'), 'target_rr': policy.get('target_rr'),
        'allocations': policy.get('allocations'), 'lock_after_tp2_r': policy.get('lock_after_tp2_r'),
        'lock_after_tp3_r': policy.get('lock_after_tp3_r'), 'expire_bars': policy.get('expire_bars'),
        'max_hold_bars': policy.get('max_hold_bars'),
    }, sort_keys=True)


def optimize_pair_walkforward(core: Any, strategy: str, direction: str, force: bool = False) -> dict[str, Any] | None:
    con = core.db()
    signal_store = signal.ModelStore(con)
    model, signal_meta = signal_store.champion(strategy, direction)
    if model is None:
        con.close()
        return None
    model_version = int(signal_meta.get('version') or 0)
    exec_store = execution.ExecutionStore(con)
    existing, existing_meta = exec_store.champion(strategy, direction, model_version)
    if existing is not None and not force and str(existing_meta.get('validation_method') or '').startswith('EXPANDING_WALK_FORWARD'):
        con.close()
        return {'strategy': strategy, 'direction': direction, 'model_version': model_version, 'status': 'UNCHANGED', **existing_meta}
    con.close()

    opportunities = execution._signal_oof_opportunities(core, strategy, direction)
    data = execution._market_data(core)
    if not data:
        return {'strategy': strategy, 'direction': direction, 'model_version': model_version, 'status': 'NO_MARKET_DATA'}
    opportunities = [
        x for x in opportunities
        if x['ts'] in data['index15']
        and data['index15'][x['ts']] >= 100
        and data['index15'][x['ts']] + execution.MAX_HOLD_BARS + 2 < len(data['m15'])
    ]
    ranges = _walkforward_ranges(len(opportunities))
    if not ranges:
        return {
            'strategy': strategy, 'direction': direction, 'model_version': model_version,
            'status': 'INSUFFICIENT_WALK_FORWARD_EVIDENCE', 'opportunities': len(opportunities),
            'minimum_opportunities': MIN_WF_OPPORTUNITIES, 'minimum_folds': MIN_WF_FOLDS,
        }

    fold_metrics: list[dict[str, Any]] = []
    all_audit_results: list[dict[str, Any]] = []
    chosen_policies: list[dict[str, Any]] = []
    validation_evs: list[float] = []
    validation_pfs: list[float] = []

    for fold_idx, (audit_start, audit_end) in enumerate(ranges):
        history = opportunities[:audit_start]
        audit = opportunities[audit_start:audit_end]
        selected = _select_policy(history, data, strategy, direction)
        if selected is None:
            fold_metrics.append({'fold': fold_idx, 'status': 'NO_STABLE_INNER_POLICY', 'history_opportunities': len(history), 'audit_opportunities': len(audit)})
            continue
        policy, dev_meta, val_meta = selected
        audit_results = [execution.simulate_policy(data, x, strategy, direction, policy) for x in audit]
        audit_meta = execution._stats(audit_results)
        fold_metrics.append({
            'fold': fold_idx, 'status': 'AUDITED', 'history_opportunities': len(history),
            'audit_start_ts': int(audit[0]['ts']) if audit else None,
            'audit_end_ts': int(audit[-1]['ts']) if audit else None,
            'development': dev_meta, 'validation': val_meta, 'audit': audit_meta,
            'policy': policy,
        })
        all_audit_results.extend(audit_results)
        chosen_policies.append(policy)
        validation_evs.append(float(val_meta.get('expectancy_r') or 0))
        validation_pfs.append(float(val_meta.get('profit_factor') or 0))

    audited_folds = [x for x in fold_metrics if x.get('status') == 'AUDITED']
    qualified_folds = [x for x in audited_folds if int((x.get('audit') or {}).get('fills') or 0) >= MIN_FOLD_AUDIT_FILLS]
    if len(qualified_folds) < MIN_WF_FOLDS:
        return {
            'strategy': strategy, 'direction': direction, 'model_version': model_version,
            'status': 'INSUFFICIENT_WALK_FORWARD_FOLDS', 'opportunities': len(opportunities),
            'folds': fold_metrics, 'qualified_folds': len(qualified_folds), 'required_folds': MIN_WF_FOLDS,
        }

    aggregate = execution._stats(all_audit_results)
    pnls = [float(x.get('pnl_r') or 0) for x in all_audit_results if x.get('filled')]
    ci_low, ci_high = execution._block_bootstrap_ev(pnls, seed=172)
    shrunk_ev = float(aggregate['expectancy_r']) * int(aggregate['fills']) / max(int(aggregate['fills']) + 80, 1)
    fold_evs = [float(x['audit']['expectancy_r']) for x in qualified_folds]
    worst_fold_ev = min(fold_evs)
    profitable_fold_ratio = sum(x > 0 for x in fold_evs) / len(fold_evs)
    recent_fold = qualified_folds[-1]['audit']
    mean_val_ev = statistics.mean(validation_evs) if validation_evs else -9.0
    mean_val_pf = statistics.mean(validation_pfs) if validation_pfs else 0.0

    suspicious = bool(
        (float(aggregate['profit_factor']) > 5.0 and int(aggregate['fills']) < 150)
        or (float(aggregate['expectancy_r']) > .70 and int(aggregate['fills']) < 150)
        or float(aggregate['win_rate']) > .82
    )

    regime_metrics: dict[str, Any] = {}
    blocked_regimes: list[str] = []
    for regime in sorted({str(x.get('regime')) for x in all_audit_results if x.get('filled') and x.get('regime')}):
        rows = [x for x in all_audit_results if x.get('regime') == regime]
        stats = execution._stats(rows)
        regime_metrics[regime] = stats
        if int(stats['fills']) >= 12 and (float(stats['expectancy_r']) <= 0 or float(stats['profit_factor']) < 1.0):
            blocked_regimes.append(regime)

    deployment = _select_policy(opportunities, data, strategy, direction)
    if deployment is None:
        return {
            'strategy': strategy, 'direction': direction, 'model_version': model_version,
            'status': 'NO_DEPLOYMENT_POLICY', 'opportunities': len(opportunities), 'folds': fold_metrics,
        }
    deployment_policy, deployment_dev, deployment_val = deployment

    core_ok = bool(
        len(qualified_folds) >= MIN_WF_FOLDS
        and int(aggregate['fills']) >= execution.MIN_AUDIT_FILLS
        and float(aggregate['profit_factor']) >= execution.MIN_AUDIT_PF
        and float(aggregate['expectancy_r']) >= execution.MIN_AUDIT_EV_R
        and shrunk_ev >= .04
        and ci_low > 0.0
        and float(aggregate['max_drawdown_r']) <= 10.0
        and .15 <= float(aggregate['fill_rate']) <= .95
        and profitable_fold_ratio >= .67
        and worst_fold_ev >= -.08
        and int(recent_fold['fills']) >= MIN_FOLD_AUDIT_FILLS
        and float(recent_fold['expectancy_r']) > .02
        and float(recent_fold['profit_factor']) >= 1.03
        and mean_val_ev > .01
        and mean_val_pf >= 1.03
        and float(aggregate['avg_stop_pct']) >= execution.MIN_STOP_PCT * .95
        and not suspicious
    )

    policy_votes = Counter(_policy_signature(x) for x in chosen_policies)
    metrics = {
        'schema': execution.EXECUTION_SCHEMA,
        'walkforward_schema': WALKFORWARD_SCHEMA,
        'validation_method': 'EXPANDING_WALK_FORWARD_POINT_IN_TIME_SIGNAL_OOF -> INNER_DEV/VALIDATION -> NEXT_UNTOUCHED_AUDIT -> AGGREGATE_AUDITS',
        'strategy': strategy, 'direction': direction, 'model_version': model_version,
        'certified': core_ok, 'signal_oof_opportunities': len(opportunities),
        'walkforward_folds': len(audited_folds), 'qualified_walkforward_folds': len(qualified_folds),
        'folds': fold_metrics, 'audit': aggregate,
        'profit_factor': float(aggregate['profit_factor']), 'expectancy_r': float(aggregate['expectancy_r']),
        'win_rate': float(aggregate['win_rate']), 'max_drawdown_r': float(aggregate['max_drawdown_r']),
        'fill_rate': float(aggregate['fill_rate']), 'oos_fills': int(aggregate['fills']),
        'oos_opportunities': int(aggregate['opportunities']), 'ev_bootstrap_05': ci_low,
        'ev_bootstrap_95': ci_high, 'shrunk_ev_r': shrunk_ev,
        'worst_fold_ev_r': worst_fold_ev, 'profitable_fold_ratio': profitable_fold_ratio,
        'recent_fold_ev_r': float(recent_fold['expectancy_r']), 'recent_fold_pf': float(recent_fold['profit_factor']),
        'mean_inner_validation_ev_r': mean_val_ev, 'mean_inner_validation_pf': mean_val_pf,
        'suspicious_metrics': suspicious, 'blocked_regimes': blocked_regimes, 'regime_metrics': regime_metrics,
        'estimated_all_in_cost_bps': execution.ALL_IN_COST_BPS,
        'minimum_audit_profit_factor': execution.MIN_AUDIT_PF,
        'minimum_audit_expectancy_r': execution.MIN_AUDIT_EV_R,
        'policy_stability_top_share': max(policy_votes.values()) / max(len(chosen_policies), 1),
        'deployment_selection': {'development': deployment_dev, 'validation': deployment_val},
        'reason': (
            'expanding walk-forward execution selection passed aggregated untouched audits'
            if core_ok else
            f"rejected walk-forward execution: PF={aggregate['profit_factor']:.2f}, EV={aggregate['expectancy_r']:.3f}R, CI05={ci_low:.3f}R, fills={aggregate['fills']}, folds={len(qualified_folds)}, worstFold={worst_fold_ev:.3f}R, recentEV={float(recent_fold['expectancy_r']):.3f}R, DD={aggregate['max_drawdown_r']:.1f}R, valEV={mean_val_ev:.3f}R, suspicious={suspicious}"
        ),
    }
    con = core.db()
    store = execution.ExecutionStore(con)
    version = store.save(strategy, direction, model_version, deployment_policy, metrics, core_ok)
    con.close()
    return {
        'strategy': strategy, 'direction': direction, 'model_version': model_version,
        'execution_version': version, 'status': 'CHAMPION' if core_ok else 'REJECTED',
        'policy': deployment_policy, **metrics,
    }


def optimize_all_walkforward(core: Any, force: bool = False) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for strategy in signal.STRATEGIES:
        for direction in signal.DIRECTIONS:
            item = optimize_pair_walkforward(core, strategy, direction, force=force)
            if item:
                out.append(item)
    return out


def _migrate(core: Any) -> None:
    key = 'execution_walkforward_schema'
    if int(core.get_state(key, 0) or 0) == WALKFORWARD_SCHEMA:
        return
    con = core.db()
    execution.ExecutionStore(con)
    # Keep every cached candle, derivative row and point-in-time learning sample.
    # Only retire execution artifacts created by the older single-split audit.
    con.execute("UPDATE execution_registry_v7 SET status='ARCHIVED' WHERE status IN ('CHAMPION','REJECTED')")
    con.commit()
    con.close()
    core.set_state('v7_execution_signal_signature', [])
    core.set_state('v7_execution_last_attempt_ts', 0)
    core.set_state(key, WALKFORWARD_SCHEMA)


def install(core: Any) -> None:
    _migrate(core)
    execution.optimize_pair = optimize_pair_walkforward
    execution.optimize_all = optimize_all_walkforward
    v8_evolution.EVOLUTION_VERSION = WALKFORWARD_VERSION
    core.state['runtime_version'] = WALKFORWARD_VERSION
    runtime_identity.stamp(core)
    core.state['execution_validation_method'] = 'EXPANDING_WALK_FORWARD_AGGREGATED_UNTOUCHED_AUDITS'
    if not any(getattr(r, 'path', None) == '/api/v8/execution-walkforward' for r in core.app.router.routes):
        @core.app.get('/api/v8/execution-walkforward')
        def execution_walkforward_status() -> dict[str, Any]:
            return {
                'runtime': WALKFORWARD_VERSION,
                'schema': WALKFORWARD_SCHEMA,
                'minimum_opportunities': MIN_WF_OPPORTUNITIES,
                'minimum_folds': MIN_WF_FOLDS,
                'minimum_fold_audit_fills': MIN_FOLD_AUDIT_FILLS,
                'minimum_aggregate_audit_fills': execution.MIN_AUDIT_FILLS,
                'method': core.state.get('execution_validation_method'),
                'historical_data_reset_required': False,
                'safety': 'each audit fold is strictly later than the history used to select its Entry/SL/TP policy; live trade outcomes remain separate deployment evidence',
            }
