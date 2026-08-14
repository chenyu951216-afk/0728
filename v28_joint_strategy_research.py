from __future__ import annotations

import bisect
import gc
import hashlib
import json
import math
import os
import random
import statistics
import time
from typing import Any

import numpy as np

import adaptive_v5 as signal
import execution_v7
import runtime_identity
import v5_runtime
import v8_evolution as evo
import v13_replay_cursor_integrity as cursor_guard
import v16_runtime_integrity as runtime_integrity
import v17_certification_orchestrator as cert17
import v18_final_system as final_system
import v18_operational_guard as operational_guard
import v20_historical_signal_evolution as signal_evolution
import v25_fixed_horizon_runtime as fixed_horizon

VERSION = runtime_identity.RUNTIME_VERSION
SCHEMA = 1
STATE_KEY = 'v28_joint_strategy_research'
RESET_MARKER = 'v28_joint_research_reset_20260801'
RUN_TABLE = 'joint_strategy_runs_v28'
RESEARCH_START_TS = int(os.getenv('HISTORICAL_RESEARCH_START_TS', '1577836800'))
# Exclusive end: 2026-08-02 00:00:00 Asia/Taipei = all of 2026-08-01.
RESEARCH_END_EXCLUSIVE_TS = int(os.getenv('HISTORICAL_RESEARCH_END_TS', '1785600000'))
FINAL_HOLDOUT_PCT = max(.15, min(.25, float(os.getenv('JOINT_FINAL_HOLDOUT_PCT', '.20'))))
MIN_OOS_FILLS = max(45, int(os.getenv('JOINT_MIN_OOS_FILLS', '60')))
MIN_OOS_PF = max(1.20, float(os.getenv('JOINT_MIN_OOS_PF', '1.30')))
MIN_OOS_EV_R = max(.06, float(os.getenv('JOINT_MIN_OOS_EV_R', '.10')))
MAX_OOS_DD_R = max(6.0, float(os.getenv('JOINT_MAX_OOS_DD_R', '10.0')))
GENERATIONS = signal_evolution.GENERATIONS
POPULATION = signal_evolution.POPULATION
ELITES = signal_evolution.ELITES


def _json_default(value: Any) -> Any:
    if hasattr(value, 'item'):
        return value.item()
    raise TypeError(f'{type(value).__name__} is not JSON serializable')


def _ensure_run_table(con: Any) -> None:
    con.execute(f'''CREATE TABLE IF NOT EXISTS {RUN_TABLE}(
        run_id INTEGER PRIMARY KEY AUTOINCREMENT,
        strategy TEXT NOT NULL,
        direction TEXT NOT NULL,
        created_at INTEGER NOT NULL,
        status TEXT NOT NULL,
        candidates_evaluated INTEGER NOT NULL,
        holdout_start_ts INTEGER,
        holdout_end_ts INTEGER,
        signal_model_version INTEGER,
        execution_version INTEGER,
        metrics TEXT NOT NULL,
        signal_genome TEXT,
        execution_policy TEXT
    )''')
    con.execute(f'CREATE INDEX IF NOT EXISTS ix_joint_v28_pair ON {RUN_TABLE}(strategy,direction,run_id)')
    con.commit()


def _latest_rows(core: Any) -> list[dict[str, Any]]:
    con = core.db()
    try:
        _ensure_run_table(con)
        rows = con.execute(f'''SELECT r.strategy,r.direction,r.created_at,r.status,
                    r.candidates_evaluated,r.holdout_start_ts,r.holdout_end_ts,
                    r.signal_model_version,r.execution_version,r.metrics,r.signal_genome,r.execution_policy
             FROM {RUN_TABLE} r
             JOIN (SELECT strategy,direction,MAX(run_id) rid FROM {RUN_TABLE} GROUP BY strategy,direction) x
               ON x.rid=r.run_id
             ORDER BY r.strategy,r.direction''').fetchall()
    finally:
        con.close()
    out = []
    for r in rows:
        try:
            metrics = json.loads(r[9]) if r[9] else {}
            genome = json.loads(r[10]) if r[10] else {}
            policy = json.loads(r[11]) if r[11] else {}
        except Exception:
            metrics, genome, policy = {}, {}, {}
        out.append({
            'strategy': str(r[0]), 'direction': str(r[1]), 'created_at': int(r[2]),
            'status': str(r[3]), 'candidates_evaluated': int(r[4] or 0),
            'holdout_start_ts': int(r[5] or 0), 'holdout_end_ts': int(r[6] or 0),
            'signal_model_version': int(r[7] or 0), 'execution_version': int(r[8] or 0),
            'metrics': metrics, 'signal_genome': genome, 'execution_policy': policy,
            'profit_factor': metrics.get('profit_factor'), 'expectancy_r': metrics.get('expectancy_r'),
            'oos_fills': metrics.get('oos_fills'), 'allowed_regimes': metrics.get('allowed_regimes') or [],
            'allowed_phases': metrics.get('allowed_phases') or [], 'reason': metrics.get('reason'),
        })
    return out


def _record_run(core: Any, strategy: str, direction: str, status: str, evaluated: int,
                holdout_start: int, holdout_end: int, metrics: dict[str, Any],
                genome: dict[str, Any] | None, policy: dict[str, Any] | None,
                signal_version: int = 0, execution_version: int = 0) -> None:
    con = core.db()
    try:
        _ensure_run_table(con)
        con.execute(f'''INSERT INTO {RUN_TABLE}(
            strategy,direction,created_at,status,candidates_evaluated,holdout_start_ts,holdout_end_ts,
            signal_model_version,execution_version,metrics,signal_genome,execution_policy
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)''', (
            strategy, direction, int(time.time()), status, int(evaluated), int(holdout_start or 0),
            int(holdout_end or 0), int(signal_version or 0), int(execution_version or 0),
            json.dumps(metrics, ensure_ascii=False, separators=(',', ':'), default=_json_default),
            json.dumps(genome, ensure_ascii=False, separators=(',', ':'), default=_json_default) if genome else None,
            json.dumps(policy, ensure_ascii=False, separators=(',', ':'), default=_json_default) if policy else None,
        ))
        con.commit()
    finally:
        con.close()


def _reset_derived_once(core: Any) -> dict[str, Any]:
    if core.get_state(RESET_MARKER, None):
        return dict(core.get_state(RESET_MARKER, {}) or {})
    cursor_guard._reset_derived_replay(
        core,
        'v28 joint Signal+Entry+SL+TP research requires a clean replay on fixed 2020-01-01..2026-08-01 history',
    )
    con = core.db()
    try:
        tables = {str(r[0]) for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
        # Derived research products are intentionally cleared. Raw market_bars and
        # derivative_history are preserved so the expensive historical download is reused.
        for table in ('signal_evolution_runs', RUN_TABLE):
            if table in tables:
                con.execute(f'DELETE FROM {table}')
        if 'model_registry' in tables:
            con.execute('DELETE FROM model_registry')
        if 'execution_registry_v7' in tables:
            con.execute('DELETE FROM execution_registry_v7')
        if 'system_state' in tables:
            keys = (
                'v17_certification_state', 'v18_final_system_state', 'v18_final_dataset_audit',
                'v18_derived_failure_confirmation', 'v20_last_cert_notice_fingerprint',
                'v7_execution_signal_signature', 'v7_execution_last_attempt_ts',
                'v26_replay_transition_stability', 'v27_signal_certification_progress',
                'fixed_horizon_live_handoff', 'evolution_recertification_gate',
            )
            con.executemany('DELETE FROM system_state WHERE key=?', [(x,) for x in keys])
        con.commit()
    finally:
        con.close()
    core.set_state(fixed_horizon.FIXED_CUTOFF_KEY, RESEARCH_END_EXCLUSIVE_TS)
    marker = {
        'at': int(time.time()), 'schema': SCHEMA,
        'research_start_ts': RESEARCH_START_TS,
        'research_end_exclusive_ts': RESEARCH_END_EXCLUSIVE_TS,
        'replay_reset': True, 'raw_market_preserved': True, 'raw_derivatives_preserved': True,
        'old_models_removed': True, 'old_execution_policies_removed': True,
    }
    core.set_state(RESET_MARKER, marker)
    return marker


def _policy_pool(strategy: str) -> list[dict[str, Any]]:
    out = []
    for base in execution_v7.policy_candidates(strategy):
        for allocation in execution_v7.ALLOCATIONS:
            p = dict(base)
            p['target_rr'] = list(base['target_rr'])
            p['allocations'] = list(allocation)
            out.append(p)
    return out

_POLICY_CACHE: dict[str, list[dict[str, Any]]] = {}


def _new_policy(strategy: str, rng: random.Random, parent: dict[str, Any] | None = None) -> dict[str, Any]:
    pool = _POLICY_CACHE.setdefault(strategy, _policy_pool(strategy))
    if parent is None:
        return dict(rng.choice(pool))
    out = dict(parent)
    out['target_rr'] = list(parent['target_rr'])
    out['allocations'] = list(parent['allocations'])
    donor = rng.choice(pool)
    fields = rng.sample(
        ('entry_atr', 'stop_atr', 'structure_mode', 'target_rr', 'allocations', 'expire_bars'),
        rng.randint(1, 3),
    )
    for key in fields:
        value = donor[key]
        out[key] = list(value) if isinstance(value, list) else value
    stop_atr = float(out.get('stop_atr') or 1.0)
    out['noise_floor_mult'] = round(signal.clamp(.62 + .30 * stop_atr, .75, 1.35), 2)
    return out


def _joint_id(signal_genome: dict[str, Any], policy: dict[str, Any]) -> str:
    payload = json.dumps({'signal': signal_genome, 'execution': policy}, sort_keys=True, separators=(',', ':'))
    return hashlib.sha256(payload.encode()).hexdigest()[:18]


def _candidate(strategy: str, rng: random.Random, generation: int,
               parent: dict[str, Any] | None = None) -> dict[str, Any]:
    if parent is None:
        sg = signal_evolution._candidate(rng, generation)
        policy = _new_policy(strategy, rng)
    else:
        sg = signal_evolution._candidate(rng, generation, parent['signal'])
        policy = _new_policy(strategy, rng, parent['execution'])
    return {'id': _joint_id(sg, policy), 'signal': sg, 'execution': policy, 'generation': generation}


def _load_market(core: Any) -> dict[str, Any]:
    sources = {tf: core._best_source('ETH', tf) for tf in ('5m', '15m', '30m', '1h')}
    if not all(sources.values()):
        return {}
    con = core.db()
    try:
        rows5 = con.execute('''SELECT ts,l,h,c FROM market_bars
            WHERE source=? AND asset='ETH' AND tf='5m' AND ts>=? AND ts<? ORDER BY ts''',
            (sources['5m'], RESEARCH_START_TS, RESEARCH_END_EXCLUSIVE_TS)).fetchall()
    finally:
        con.close()
    if len(rows5) < 1000:
        return {}
    ts5 = np.asarray([int(r[0]) for r in rows5], dtype=np.int64)
    lo5 = np.asarray([float(r[1]) for r in rows5], dtype=float)
    hi5 = np.asarray([float(r[2]) for r in rows5], dtype=float)
    cl5 = np.asarray([float(r[3]) for r in rows5], dtype=float)
    out: dict[str, Any] = {'ts5': ts5, 'lo5': lo5, 'hi5': hi5, 'cl5': cl5, 'sources': sources}
    for tf in ('15m', '30m', '1h'):
        rows = [x for x in core.load_bars('ETH', tf, sources[tf]) if RESEARCH_START_TS <= int(x['ts']) < RESEARCH_END_EXCLUSIVE_TS]
        out[tf] = rows
        out[f'ts{tf}'] = [int(x['ts']) for x in rows]
    out['index15'] = {int(x['ts']): i for i, x in enumerate(out['15m'])}
    return out


def _simulate_5m(data: dict[str, Any], row: dict[str, Any], strategy: str, direction: str,
                 policy: dict[str, Any]) -> dict[str, Any]:
    ts = int(row['ts'])
    i = data['index15'].get(ts)
    if i is None or i < 100:
        return {'valid': False, 'filled': False, 'pnl_r': 0.0}
    m15 = data['15m']
    past15 = m15[max(0, i - 500):i + 1]
    decision_close = ts + 900
    past30 = execution_v7._slice_closed_to(data['30m'], data['ts30m'], 1800, decision_close, 300)
    past1h = execution_v7._slice_closed_to(data['1h'], data['ts1h'], 3600, decision_close, 300)
    plan = execution_v7.plan_from_policy(strategy, direction, float(m15[i]['c']), past15, policy, past30, past1h)
    entry, stop0 = float(plan['entry']), float(plan['stop'])
    risk = abs(entry - stop0)
    if risk <= 1e-9:
        return {'valid': False, 'filled': False, 'pnl_r': 0.0}
    ts5 = data['ts5']
    start = int(np.searchsorted(ts5, decision_close, side='left'))
    hold5 = int(policy.get('max_hold_bars', execution_v7.MAX_HOLD_BARS)) * 3
    end = min(len(ts5), start + hold5)
    if start >= end or int(ts5[start]) != decision_close:
        return {'valid': False, 'filled': False, 'pnl_r': 0.0}
    segment_ts = ts5[start:end]
    if len(segment_ts) > 1 and bool(np.any(np.diff(segment_ts) != 300)):
        return {'valid': False, 'filled': False, 'pnl_r': 0.0}
    expire5 = min(end - start, int(policy.get('expire_bars', 6)) * 3)
    lo, hi = data['lo5'], data['hi5']
    fill_idx = next((j for j in range(start, start + expire5) if lo[j] <= entry <= hi[j]), None)
    if fill_idx is None:
        return {'valid': True, 'filled': False, 'pnl_r': 0.0, 'regime': row['regime'], 'phase': row['phase']}
    sign = 1 if direction == 'LONG' else -1
    remaining, realized, current_stop = 1.0, 0.0, stop0
    hit: set[int] = set()
    last = entry
    for j in range(fill_idx, end):
        low, high, close = float(lo[j]), float(hi[j]), float(data['cl5'][j])
        last = close
        stop_hit = low <= current_stop if direction == 'LONG' else high >= current_stop
        if stop_hit:
            realized += remaining * ((current_stop - entry) * sign / risk)
            remaining = 0.0
            break
        # Conservative intrabar rule: the fill bar cannot receive target credit.
        if j != fill_idx:
            for k, target in enumerate(plan['targets']):
                if k in hit:
                    continue
                px = float(target['price'])
                target_hit = high >= px if direction == 'LONG' else low <= px
                if target_hit:
                    frac = min(remaining, float(target['allocation']) / 100.0)
                    realized += frac * float(target['rr'])
                    remaining -= frac
                    hit.add(k)
        if 0 in hit:
            current_stop = max(current_stop, entry) if direction == 'LONG' else min(current_stop, entry)
        if 1 in hit:
            lock = entry + sign * float(policy.get('lock_after_tp2_r', .55)) * risk
            current_stop = max(current_stop, lock) if direction == 'LONG' else min(current_stop, lock)
        if 2 in hit:
            lock = entry + sign * float(policy.get('lock_after_tp3_r', 1.05)) * risk
            current_stop = max(current_stop, lock) if direction == 'LONG' else min(current_stop, lock)
        if remaining <= 1e-9:
            break
    if remaining > 1e-9:
        realized += remaining * ((last - entry) * sign / risk)
    cost_r = (float(policy.get('all_in_cost_bps', execution_v7.ALL_IN_COST_BPS)) / 10000.0) * entry / risk
    return {
        'valid': True, 'filled': True, 'pnl_r': realized - cost_r,
        'gross_r': realized, 'cost_r': cost_r, 'regime': row['regime'], 'phase': row['phase'],
    }


def _stats(results: list[dict[str, Any]]) -> dict[str, float]:
    filled = [x for x in results if x.get('valid') and x.get('filled')]
    p = [float(x['pnl_r']) for x in filled]
    gains = sum(max(x, 0.0) for x in p)
    losses = sum(max(-x, 0.0) for x in p)
    eq = peak = dd = 0.0
    for x in p:
        eq += x
        peak = max(peak, eq)
        dd = max(dd, peak - eq)
    return {
        'fills': float(len(filled)), 'pf': gains / max(losses, 1e-9),
        'ev': statistics.mean(p) if p else -9.0,
        'win': (sum(x > 0 for x in p) / len(p)) if p else 0.0,
        'dd': dd,
    }


def _threshold_joint(cal: list[dict[str, Any]], probs: np.ndarray, data: dict[str, Any], strategy: str,
                     direction: str, policy: dict[str, Any]) -> tuple[float, dict[str, Any]] | None:
    cache: dict[int, dict[str, Any]] = {}
    best: tuple[float, float, dict[str, Any]] | None = None
    for threshold in np.arange(.50, .801, .02):
        selected = [(r, float(p)) for r, p in zip(cal, probs) if float(p) >= float(threshold)]
        if len(selected) < 20:
            continue
        results = []
        for row, _ in selected:
            ts = int(row['ts'])
            if ts not in cache:
                cache[ts] = _simulate_5m(data, row, strategy, direction, policy)
            results.append(cache[ts])
        st = _stats(results)
        if st['fills'] < 12:
            continue
        score = st['ev'] * 3.8 + math.log(max(st['pf'], 1e-6)) * .30 - st['dd'] * .006 + min(st['fills'], 100.0) / 100.0 * .08
        if best is None or score > best[0]:
            best = (score, round(float(threshold), 2), st)
    return None if best is None else (best[1], best[2])


def _score_candidate(development: list[dict[str, Any]], candidate: dict[str, Any], data: dict[str, Any],
                     strategy: str, direction: str, seed: int) -> dict[str, Any] | None:
    sg, policy = candidate['signal'], candidate['execution']
    rows = signal_evolution._scope(development, sg)
    if len(rows) < 520:
        return None
    idx = signal_evolution._indices(sg)
    if len(idx) < 7:
        return None
    x = signal_evolution._matrix(rows, idx)
    y = np.asarray([r['success'] for r in rows])
    purge = 32
    anchors = (.58, .72, .86)
    fold_stats = []
    thresholds = []
    all_results: list[dict[str, Any]] = []
    for fi, frac in enumerate(anchors):
        test_start = int(len(rows) * frac)
        test_end = int(len(rows) * (anchors[fi + 1] if fi + 1 < len(anchors) else .98))
        train_end = max(0, test_start - purge)
        train = rows[:train_end]
        test = rows[test_start:test_end]
        if len(train) < 300 or len(test) < 55:
            continue
        cal_n = max(70, int(len(train) * .18))
        fit_end = max(0, len(train) - cal_n - purge)
        cal_start = fit_end + purge
        fit = rows[:fit_end]
        cal = rows[cal_start:train_end]
        if len(fit) < 220 or len(cal) < 60 or len(set(y[:fit_end])) < 2:
            continue
        m = signal_evolution._model(sg, seed + fi)
        m.fit(x[:fit_end], y[:fit_end], sample_weight=signal_evolution._weights(fit, float(sg['half_life_days'])))
        cp = m.predict_proba(x[cal_start:train_end])[:, 1]
        picked = _threshold_joint(cal, cp, data, strategy, direction, policy)
        if picked is None:
            continue
        threshold, _ = picked
        m2 = signal_evolution._model(sg, seed + 100 + fi)
        m2.fit(x[:train_end], y[:train_end], sample_weight=signal_evolution._weights(train, float(sg['half_life_days'])))
        tp = m2.predict_proba(x[test_start:test_end])[:, 1]
        selected = [r for r, p in zip(test, tp) if float(p) >= threshold]
        results = [_simulate_5m(data, r, strategy, direction, policy) for r in selected]
        st = _stats(results)
        if st['fills'] < 16:
            continue
        fold_stats.append(st)
        thresholds.append(threshold)
        all_results.extend(results)
    if len(fold_stats) < 2 or len(all_results) < 45:
        return None
    overall = _stats(all_results)
    evs = [x['ev'] for x in fold_stats]
    pfs = [x['pf'] for x in fold_stats]
    stability = signal.clamp(1.0 - .70 * statistics.pstdev(evs) - .03 * statistics.pstdev(pfs), 0.0, 1.0)
    profitable = sum(x > 0 for x in evs) / len(evs)
    worst = min(evs)
    regime_metrics: dict[str, dict[str, float]] = {}
    phase_metrics: dict[str, dict[str, float]] = {}
    for name in sorted({str(x.get('regime')) for x in all_results if x.get('filled')}):
        subset = [x for x in all_results if x.get('filled') and str(x.get('regime')) == name]
        if len(subset) >= 10:
            regime_metrics[name] = _stats(subset)
    for name in sorted({str(x.get('phase')) for x in all_results if x.get('filled')}):
        subset = [x for x in all_results if x.get('filled') and str(x.get('phase')) == name]
        if len(subset) >= 10:
            phase_metrics[name] = _stats(subset)
    allowed_regimes = [k for k, z in regime_metrics.items() if z['fills'] >= 12 and z['ev'] > 0 and z['pf'] >= 1.03]
    allowed_phases = [k for k, z in phase_metrics.items() if z['fills'] >= 12 and z['ev'] > 0 and z['pf'] >= 1.02]
    if not allowed_regimes or not allowed_phases:
        return None
    score = overall['ev'] * 4.5 + math.log(max(overall['pf'], 1e-6)) * .35 + stability * .30 + profitable * .20 - overall['dd'] * .005 + min(worst, .10)
    return {
        'score': score, 'pf': overall['pf'], 'ev': overall['ev'], 'win': overall['win'],
        'fills': int(overall['fills']), 'dd': overall['dd'], 'stability': stability,
        'profitable_folds': profitable, 'worst_fold_ev': worst,
        'threshold': round(statistics.median(thresholds), 2),
        'allowed_regimes': allowed_regimes, 'allowed_phases': allowed_phases,
        'regime_metrics': regime_metrics, 'phase_metrics': phase_metrics,
    }


def _bootstrap_ci05(pnls: list[float], seed: int = 2828, reps: int = 300, block: int = 8) -> float:
    if len(pnls) < 20:
        return -9.0
    rng = np.random.default_rng(seed)
    arr = np.asarray(pnls, dtype=float)
    vals = []
    for _ in range(reps):
        sample = []
        while len(sample) < len(arr):
            start = int(rng.integers(0, max(1, len(arr) - block + 1)))
            sample.extend(arr[start:start + block].tolist())
        vals.append(float(np.mean(sample[:len(arr)])))
    return float(np.quantile(vals, .05))


def _train_lineage(core: Any, data: dict[str, Any], strategy: str, direction: str) -> dict[str, Any]:
    con = core.db()
    try:
        store = v5_runtime.ModelStore(con)
        rows = [x for x in store.samples(strategy, direction=direction) if x['source_quality'] >= 55 and RESEARCH_START_TS <= int(x['ts']) < RESEARCH_END_EXCLUSIVE_TS]
    finally:
        con.close()
    if len(rows) < 1100:
        return {'strategy': strategy, 'direction': direction, 'status': 'INSUFFICIENT_REPLAY_ROWS', 'promoted': False, 'candidates_evaluated': 0}
    purge = 32
    holdout_start = max(760, int(len(rows) * (1.0 - FINAL_HOLDOUT_PCT)))
    development = rows[:max(0, holdout_start - purge)]
    holdout = rows[holdout_start:]
    if len(development) < 700 or len(holdout) < 160:
        return {'strategy': strategy, 'direction': direction, 'status': 'INSUFFICIENT_CHRONO_SPLIT', 'promoted': False, 'candidates_evaluated': 0}
    rng = random.Random(int(hashlib.sha256(f'v28|{strategy}|{direction}|{holdout[-1]["ts"]}'.encode()).hexdigest()[:12], 16))
    population = [_candidate(strategy, rng, 0) for _ in range(POPULATION)]
    seen: set[str] = set()
    elites: list[tuple[float, dict[str, Any], dict[str, Any]]] = []
    evaluated = 0
    generation_history = []
    for generation in range(GENERATIONS):
        scored = []
        for ci, candidate in enumerate(population):
            if candidate['id'] in seen:
                continue
            seen.add(candidate['id'])
            evaluated += 1
            core.state['joint_strategy_live_progress'] = {
                'strategy': strategy, 'direction': direction, 'generation': generation,
                'candidate': ci + 1, 'population': len(population), 'candidates_evaluated': evaluated,
                'stage': 'JOINT_SIGNAL_ENTRY_SL_TP_DEVELOPMENT', 'updated_at': int(time.time()),
            }
            result = _score_candidate(development, candidate, data, strategy, direction, 28000 + generation * 1000 + ci * 7)
            if result is not None:
                scored.append((float(result['score']), candidate, result))
        carry = elites if generation == 0 or signal_evolution._evolution_stage(generation) == signal_evolution._evolution_stage(generation - 1) else []
        pool = {item[1]['id']: item for item in carry + scored}
        elites = sorted(pool.values(), key=lambda x: x[0], reverse=True)[:ELITES]
        if elites:
            generation_history.append({'generation': generation, 'new_candidates': len(scored), 'best': elites[0][2], 'joint_id': elites[0][1]['id']})
        if generation == GENERATIONS - 1 or not elites:
            break
        population = []
        while len(population) < POPULATION:
            if len(population) >= int(POPULATION * .82):
                population.append(_candidate(strategy, rng, generation + 1))
            else:
                population.append(_candidate(strategy, rng, generation + 1, rng.choice(elites)[1]))
    if not elites:
        metrics = {'reason': 'no complete Signal+Entry+SL+TP candidate survived development walk-forward', 'development_generations': generation_history}
        _record_run(core, strategy, direction, 'NO_ELIGIBLE_JOINT_DEVELOPMENT_CANDIDATE', evaluated, int(holdout[0]['ts']), int(holdout[-1]['ts']), metrics, None, None)
        return {'strategy': strategy, 'direction': direction, 'status': 'NO_ELIGIBLE_JOINT_DEVELOPMENT_CANDIDATE', 'promoted': False, 'candidates_evaluated': evaluated, **metrics}

    _, winner, dev = elites[0]
    sg = dict(winner['signal'])
    policy = dict(winner['execution'])
    sg['regimes'] = list(dev['allowed_regimes'])
    sg['phases'] = list(dev['allowed_phases'])
    sg['id'] = f"joint28_{winner['id']}"
    scoped_dev = signal_evolution._scope(development, sg)
    scoped_holdout = signal_evolution._scope(holdout, sg)
    idx = signal_evolution._indices(sg)
    if len(scoped_dev) < 500 or len(scoped_holdout) < 100 or len(idx) < 7:
        metrics = {'reason': 'joint winner has insufficient scoped chronological support', 'development_generations': generation_history}
        _record_run(core, strategy, direction, 'NO_JOINT_SCOPE_SUPPORT', evaluated, int(holdout[0]['ts']), int(holdout[-1]['ts']), metrics, sg, policy)
        return {'strategy': strategy, 'direction': direction, 'status': 'NO_JOINT_SCOPE_SUPPORT', 'promoted': False, 'candidates_evaluated': evaluated, **metrics}

    cal_n = max(90, int(len(scoped_dev) * .16))
    fit_end = max(0, len(scoped_dev) - cal_n - purge)
    cal_start = fit_end + purge
    fit, cal = scoped_dev[:fit_end], scoped_dev[cal_start:]
    if len(fit) < 320 or len(cal) < 70:
        metrics = {'reason': 'insufficient joint development fit/calibration support'}
        _record_run(core, strategy, direction, 'NO_JOINT_CALIBRATION_SUPPORT', evaluated, int(holdout[0]['ts']), int(holdout[-1]['ts']), metrics, sg, policy)
        return {'strategy': strategy, 'direction': direction, 'status': 'NO_JOINT_CALIBRATION_SUPPORT', 'promoted': False, 'candidates_evaluated': evaluated, **metrics}

    x_dev = signal_evolution._matrix(scoped_dev, idx)
    y_dev = np.asarray([r['success'] for r in scoped_dev])
    if len(set(y_dev[:fit_end])) < 2 or len(set(y_dev)) < 2:
        metrics = {'reason': 'joint development contains only one signal outcome class'}
        _record_run(core, strategy, direction, 'NO_CLASS_SUPPORT', evaluated, int(holdout[0]['ts']), int(holdout[-1]['ts']), metrics, sg, policy)
        return {'strategy': strategy, 'direction': direction, 'status': 'NO_CLASS_SUPPORT', 'promoted': False, 'candidates_evaluated': evaluated, **metrics}

    model_cal = signal_evolution._model(sg, 28801)
    model_cal.fit(x_dev[:fit_end], y_dev[:fit_end], sample_weight=signal_evolution._weights(fit, float(sg['half_life_days'])))
    cal_probs = model_cal.predict_proba(x_dev[cal_start:])[:, 1]
    picked = _threshold_joint(cal, cal_probs, data, strategy, direction, policy)
    if picked is None:
        metrics = {'reason': 'joint development could not calibrate a viable probability threshold'}
        _record_run(core, strategy, direction, 'NO_JOINT_THRESHOLD', evaluated, int(holdout[0]['ts']), int(holdout[-1]['ts']), metrics, sg, policy)
        return {'strategy': strategy, 'direction': direction, 'status': 'NO_JOINT_THRESHOLD', 'promoted': False, 'candidates_evaluated': evaluated, **metrics}
    threshold, _ = picked

    model = signal_evolution._model(sg, 28802)
    model.fit(x_dev, y_dev, sample_weight=signal_evolution._weights(scoped_dev, float(sg['half_life_days'])))
    x_hold = signal_evolution._matrix(scoped_holdout, idx)
    probs = model.predict_proba(x_hold)[:, 1]
    selected = [r for r, p in zip(scoped_holdout, probs) if float(p) >= threshold]
    audit_results = [_simulate_5m(data, r, strategy, direction, policy) for r in selected]
    audit = _stats(audit_results)
    pnls = [float(x['pnl_r']) for x in audit_results if x.get('valid') and x.get('filled')]
    ci05 = _bootstrap_ci05(pnls)
    labels = np.asarray([r['success'] for r in scoped_holdout], dtype=float)
    brier = float(np.mean((probs - labels) ** 2)) if len(labels) else 1.0
    valid_paths = sum(1 for x in audit_results if x.get('valid'))
    invalid_paths = len(audit_results) - valid_paths
    promoted = bool(
        audit['fills'] >= MIN_OOS_FILLS and audit['pf'] >= MIN_OOS_PF and audit['ev'] >= MIN_OOS_EV_R and
        ci05 > 0 and audit['dd'] <= MAX_OOS_DD_R and brier <= .27 and dev['stability'] >= .65 and
        dev['profitable_folds'] >= .66 and dev['worst_fold_ev'] >= -.08 and invalid_paths == 0
    )
    status = 'PROMOTED' if promoted else 'REJECTED_JOINT_SEALED_OOS'
    reason = (
        'complete Signal+Entry+SL+TP package passed one-time chronological OOS and was refit on all fixed history'
        if promoted else
        f"joint sealed OOS rejected: fills={int(audit['fills'])}, PF={audit['pf']:.2f}, EV={audit['ev']:+.3f}R, CI05={ci05:+.3f}R, DD={audit['dd']:.2f}R, Brier={brier:.3f}, invalid_paths={invalid_paths}"
    )
    metrics = {
        'schema_version': 28, 'joint_research_schema': SCHEMA, 'strategy': strategy, 'direction': direction,
        'joint_signal_execution': True, 'joint_id': winner['id'], 'threshold': threshold,
        'profit_factor': audit['pf'], 'expectancy_r': audit['ev'], 'test_win': audit['win'],
        'selected_n': int(audit['fills']), 'effective_oos_selected_n': int(audit['fills']),
        'oos_fills': int(audit['fills']), 'max_drawdown_r': audit['dd'], 'clustered_ev_bootstrap_05': ci05,
        'brier': brier, 'stability': dev['stability'], 'profitable_folds': dev['profitable_folds'],
        'worst_fold_ev': dev['worst_fold_ev'], 'allowed_regimes': list(dev['allowed_regimes']),
        'allowed_phases': list(dev['allowed_phases']), 'execution_policy': policy,
        'development_generations': generation_history, 'candidates_evaluated': evaluated,
        'trained_through_ts': int(development[-1]['ts']), 'evaluated_through_ts': int(holdout[-1]['ts']),
        'holdout_start_ts': int(holdout[0]['ts']), 'holdout_end_ts': int(holdout[-1]['ts']),
        'overfit_guard_passed': promoted, 'absolute_guard_passed': promoted,
        'validation_method': 'JOINT_SIGNAL_ENTRY_SL_TP_DEV_WALK_FORWARD_THEN_ONE_TIME_SEALED_CHRONOLOGICAL_OOS',
        'final_refit_policy': 'after one-time OOS pass, keep genome/threshold/execution policy frozen and refit estimator on all fixed historical samples',
        'historical_no_lookahead': True, 'future_path_visible_before_plan_freeze': False,
        'future_path_after_plan_freeze': '5m sequential execution simulation only',
        'reason': reason,
    }
    signal_version = execution_version = 0
    if promoted:
        # Validation is finished before this refit. The holdout is never used to change
        # genome, threshold, entry, stop, targets, allocation or management rules.
        all_scoped = signal_evolution._scope(rows, sg)
        x_all = signal_evolution._matrix(all_scoped, idx)
        y_all = np.asarray([r['success'] for r in all_scoped])
        final_model = signal_evolution._model(sg, 28803)
        final_model.fit(x_all, y_all, sample_weight=signal_evolution._weights(all_scoped, float(sg['half_life_days'])))
        wrapped = evo.GenomeModel(final_model, idx, sg['id'])
        con = core.db()
        try:
            store = v5_runtime.ModelStore(con)
            signal_version = int(store.save_challenger(strategy, direction, wrapped, metrics, True))
            exec_store = execution_v7.ExecutionStore(con)
            exec_metrics = {
                **metrics, 'certified': True, 'model_version': signal_version,
                'execution_validation_included_in_joint_search': True,
                'ev_bootstrap_05': ci05, 'suspicious_metrics': False,
            }
            execution_version = int(exec_store.save(strategy, direction, signal_version, policy, exec_metrics, True))
        finally:
            con.close()
        metrics['signal_model_version'] = signal_version
        metrics['execution_version'] = execution_version
    _record_run(core, strategy, direction, status, evaluated, int(holdout[0]['ts']), int(holdout[-1]['ts']), metrics, sg, policy, signal_version, execution_version)
    return {'strategy': strategy, 'direction': direction, 'status': status, 'promoted': promoted, 'candidates_evaluated': evaluated, **metrics}


def _existing_complete(core: Any) -> bool:
    rows = _latest_rows(core)
    expected = len(v5_runtime.STRATEGIES) * len(v5_runtime.DIRECTIONS)
    return len(rows) >= expected and all(str(x.get('status') or '') not in ('RUNNING', 'WAITING', '') for x in rows)


def joint_certify(core: Any, force: bool = False) -> list[dict[str, Any]]:
    _ = force
    replay = runtime_integrity.replay_progress(core)
    if not replay.get('complete'):
        core.state[STATE_KEY] = {'status': 'WAITING_FOR_REPLAY', 'replay': replay, 'updated_at': int(time.time())}
        return []
    audit = final_system.final_audit(core, allow_auto_rebuild=False)
    if not audit.get('valid'):
        core.state[STATE_KEY] = {'status': 'WAITING_DATA_AUDIT', 'audit': audit, 'updated_at': int(time.time())}
        return []
    preflight = core.state.get('startup_preflight') or {}
    if isinstance(preflight, dict) and preflight and not bool(preflight.get('ready')):
        core.state[STATE_KEY] = {'status': 'WAITING_SOURCE_PREFLIGHT', 'preflight': preflight, 'updated_at': int(time.time())}
        return []
    if _existing_complete(core):
        rows = _latest_rows(core)
        core.state[STATE_KEY] = {'status': 'COMPLETE', 'results': rows, 'updated_at': int(time.time())}
        return rows
    data = _load_market(core)
    if not data:
        core.state[STATE_KEY] = {'status': 'WAITING_MARKET_CACHE', 'reason': 'fixed historical 5m/15m/30m/1h cache is incomplete', 'updated_at': int(time.time())}
        return []
    terminal = {(x['strategy'], x['direction']) for x in _latest_rows(core)}
    results = []
    try:
        for strategy in v5_runtime.STRATEGIES:
            for direction in v5_runtime.DIRECTIONS:
                if (strategy, direction) in terminal:
                    continue
                core.state[STATE_KEY] = {
                    'status': 'RUNNING', 'strategy': strategy, 'direction': direction,
                    'research_start_ts': RESEARCH_START_TS, 'research_end_exclusive_ts': RESEARCH_END_EXCLUSIVE_TS,
                    'joint_signal_execution': True, 'updated_at': int(time.time()),
                }
                item = _train_lineage(core, data, strategy, direction)
                results.append(item)
                core.state['last_training'] = _latest_rows(core)
                gc.collect()
    finally:
        data.clear()
        gc.collect()
    rows = _latest_rows(core)
    sig, exe = runtime_integrity._champion_counts(core)
    core.set_state('v7_execution_signal_signature', [list(x) for x in __import__('v7_runtime')._champion_signature(core)])
    core.set_state('v7_execution_last_attempt_ts', int(time.time()))
    core.state['execution_learning'] = {
        'version': VERSION, 'mode': 'JOINT_WITH_SIGNAL', 'results': rows,
        'updated_at': int(time.time()), 'separate_execution_retune_disabled': True,
    }
    core.state[STATE_KEY] = {
        'status': 'COMPLETE' if len(rows) >= len(v5_runtime.STRATEGIES) * len(v5_runtime.DIRECTIONS) else 'PARTIAL',
        'results': rows, 'signal_champions': sig, 'execution_champions': exe,
        'research_start_ts': RESEARCH_START_TS, 'research_end_exclusive_ts': RESEARCH_END_EXCLUSIVE_TS,
        'updated_at': int(time.time()),
    }
    return results or rows


def joint_status(core: Any) -> dict[str, Any]:
    rows = _latest_rows(core)
    expected = len(v5_runtime.STRATEGIES) * len(v5_runtime.DIRECTIONS)
    promoted = sum(1 for x in rows if x['status'] == 'PROMOTED')
    active = core.state.get('joint_strategy_live_progress') or {}
    return {
        'runtime': VERSION, 'schema': SCHEMA,
        'research_start_ts': RESEARCH_START_TS, 'research_end_exclusive_ts': RESEARCH_END_EXCLUSIVE_TS,
        'joint_signal_entry_sl_tp': True, 'terminal_lineages': len(rows), 'expected_lineages': expected,
        'percent': round(100.0 * len(rows) / max(1, expected), 2), 'promoted_packages': promoted,
        'active': active, 'lineages': rows,
        'no_lookahead': {
            'features_closed_at_decision': True, 'execution_plan_frozen_before_future_path': True,
            'future_path_resolution': '5m sequential', 'same_bar_ambiguity': 'stop-first and no target credit on fill bar',
        },
    }


def _joint_lineage_progress(_evolution_module: Any, core: Any) -> dict[str, Any]:
    status = joint_status(core)
    active = status.get('active') or {}
    pct = float(status['percent'])
    if active and len(status['lineages']) < status['expected_lineages']:
        gen = int(active.get('generation') or 0)
        candidate = int(active.get('candidate') or 0)
        pop = max(1, int(active.get('population') or POPULATION))
        within = min(.995, (gen + min(1.0, candidate / pop)) / max(1, GENERATIONS))
        pct = 100.0 * min(status['expected_lineages'], status['terminal_lineages'] + within) / max(1, status['expected_lineages'])
    lineages = []
    for x in status['lineages']:
        lineages.append({
            'strategy': x['strategy'], 'direction': x['direction'], 'status': x['status'],
            'candidates_evaluated': x['candidates_evaluated'], 'profit_factor': x.get('profit_factor'),
            'expectancy_r': x.get('expectancy_r'), 'selected_n': x.get('oos_fills'),
            'reason': x.get('reason'), 'holdout_end_ts': x.get('holdout_end_ts'),
        })
    if active:
        key = (str(active.get('strategy') or ''), str(active.get('direction') or ''))
        if key[0] and key[1] and not any((x['strategy'], x['direction']) == key for x in lineages):
            lineages.append({
                'strategy': key[0], 'direction': key[1],
                'status': 'RUNNING_JOINT_SIGNAL_ENTRY_SL_TP',
                'generation': active.get('generation'), 'candidates_evaluated': active.get('candidates_evaluated', 0),
                'profit_factor': None, 'expectancy_r': None,
                'reason': 'Signal + Entry + SL + TP + management are evolving as one package; sealed OOS remains untouched',
            })
    return {
        'percent': round(pct, 2), 'terminal_lineages': status['terminal_lineages'],
        'expected_lineages': status['expected_lineages'],
        'sealed_oos_percent': status['percent'], 'sealed_oos_opened': status['terminal_lineages'],
        'candidates_evaluated': sum(int(x.get('candidates_evaluated') or 0) for x in lineages),
        'lineages': lineages, 'joint': True,
    }


def _joint_execution_progress(core: Any) -> dict[str, Any]:
    rows = _latest_rows(core)
    promoted = [x for x in rows if x['status'] == 'PROMOTED']
    return {
        'percent': 100.0 if _existing_complete(core) else round(100.0 * len(rows) / max(1, len(v5_runtime.STRATEGIES) * len(v5_runtime.DIRECTIONS)), 2),
        'signal_champions': len(promoted), 'execution_champions': len(promoted),
        'rejected': len(rows) - len(promoted), 'recent': rows[-24:],
        'mode': 'JOINT_SIGNAL_ENTRY_SL_TP',
    }


def _no_separate_execution(core: Any, force: bool = False) -> list[dict[str, Any]]:
    _ = force
    return [x for x in _latest_rows(core) if x['status'] == 'PROMOTED']


def install(production: Any) -> None:
    core = production.core
    reset = _reset_derived_once(core)
    core.set_state(fixed_horizon.FIXED_CUTOFF_KEY, RESEARCH_END_EXCLUSIVE_TS)
    core.state['joint_research_contract'] = {
        'schema': SCHEMA, 'research_start_ts': RESEARCH_START_TS,
        'research_end_exclusive_ts': RESEARCH_END_EXCLUSIVE_TS,
        'history_policy': 'reuse raw cache; replay-derived samples/models/policies are rebuilt from scratch',
        'joint_optimization': 'signal + regime/phase + entry + stop + targets + allocations + trade management',
        'sealed_oos': 'one-time chronological validation of the complete frozen package',
        'after_pass': 'refit estimator on all fixed history without changing the validated package',
        'reset': reset,
    }
    final_system.certify_and_execute = joint_certify
    operational_guard.certify_and_execute = joint_certify
    cert17.train_v17 = joint_certify
    v5_runtime.train_v5 = joint_certify
    core.train_if_due = lambda force=False: joint_certify(core, force)
    execution_v7.optimize_all = _no_separate_execution
    fixed_horizon._lineage_progress = _joint_lineage_progress
    fixed_horizon._execution_progress = _joint_execution_progress

    if not any(getattr(route, 'path', None) == '/api/v28/joint-research' for route in core.app.router.routes):
        @core.app.get('/api/v28/joint-research')
        def joint_research_api() -> dict[str, Any]:
            return joint_status(core)
