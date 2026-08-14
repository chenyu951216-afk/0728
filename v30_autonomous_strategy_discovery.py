from __future__ import annotations

"""Autonomous, no-template strategy discovery for ETH paper research.

The research engine deliberately ignores legacy strategy names, regime labels and
success labels. It uses causal feature snapshots only as state observations and
learns complete trading packages directly from realised R under a frozen execution
plan. Development is chronological walk-forward; the final chronological block is
read only after the complete package has been frozen.

This module is an install-time overlay. Raw market/derivative downloads are reused;
a new reset marker forces replay-derived samples and old models/policies to rebuild.
"""

import gc
import hashlib
import json
import math
import os
import pickle
import random
import statistics
import time
from typing import Any, Iterable

import numpy as np
from sklearn.ensemble import HistGradientBoostingRegressor

import adaptive_v5
import runtime_identity
import v5_runtime
import v16_runtime_integrity as runtime_integrity
import v17_certification_orchestrator as cert17
import v18_final_system as final_system
import v18_operational_guard as operational_guard
import v22_hierarchical_pipeline as pipeline

VERSION = runtime_identity.RUNTIME_VERSION
SCHEMA = 30
RESET_MARKER = 'v30_autonomous_direct_r_reset_20260801'
STATE_KEY = 'v30_autonomous_strategy_discovery'
CHECKPOINT_KEY = 'v30_autonomous_evolution_checkpoint'
REGISTRY_TABLE = 'autonomous_strategy_registry_v30'
AUDIT_TABLE = 'autonomous_strategy_audit_v30'

RESEARCH_START_TS = int(os.getenv('AUTONOMOUS_RESEARCH_START_TS', '1577836800'))
RESEARCH_END_EXCLUSIVE_TS = int(os.getenv('AUTONOMOUS_RESEARCH_END_TS', '1785600000'))
SETTLEMENT_END_EXCLUSIVE_TS = int(os.getenv('AUTONOMOUS_SETTLEMENT_END_TS', '1786204800'))

POPULATION = max(24, min(96, int(os.getenv('AUTONOMOUS_POPULATION', '48'))))
GENERATIONS = max(4, min(16, int(os.getenv('AUTONOMOUS_GENERATIONS', '8'))))
ELITES = max(4, min(20, int(os.getenv('AUTONOMOUS_ELITES', '10'))))
FINALISTS = max(6, min(36, int(os.getenv('AUTONOMOUS_FINALISTS', '20'))))
MAX_CHAMPIONS = max(2, min(20, int(os.getenv('AUTONOMOUS_MAX_CHAMPIONS', '10'))))
TRAIN_SIM_CAP = max(1200, min(12000, int(os.getenv('AUTONOMOUS_TRAIN_SIM_CAP', '3500'))))
CAL_SIM_CAP = max(600, min(6000, int(os.getenv('AUTONOMOUS_CAL_SIM_CAP', '1800'))))
TEST_SIM_CAP = max(800, min(8000, int(os.getenv('AUTONOMOUS_TEST_SIM_CAP', '2200'))))
FINAL_REFIT_CAP = max(10000, min(80000, int(os.getenv('AUTONOMOUS_FINAL_REFIT_CAP', '50000'))))

FINAL_HOLDOUT_PCT = max(.12, min(.25, float(os.getenv('AUTONOMOUS_FINAL_HOLDOUT_PCT', '.18'))))
MIN_OOS_FILLS = max(35, int(os.getenv('AUTONOMOUS_MIN_OOS_FILLS', '60')))
MIN_OOS_PF = max(1.10, float(os.getenv('AUTONOMOUS_MIN_OOS_PF', '1.25')))
MIN_OOS_EV_R = max(.04, float(os.getenv('AUTONOMOUS_MIN_OOS_EV_R', '.08')))
MAX_OOS_DD_R = max(6.0, float(os.getenv('AUTONOMOUS_MAX_OOS_DD_R', '12.0')))
MIN_WF_STABILITY = max(.45, min(.90, float(os.getenv('AUTONOMOUS_MIN_WF_STABILITY', '.60'))))
MIN_PROFITABLE_FOLDS = max(.50, min(1.0, float(os.getenv('AUTONOMOUS_MIN_PROFITABLE_FOLDS', '.66'))))
MIN_WORST_FOLD_EV = max(-.20, min(.05, float(os.getenv('AUTONOMOUS_MIN_WORST_FOLD_EV', '-.08'))))
MIN_BOOTSTRAP_CI05 = float(os.getenv('AUTONOMOUS_MIN_BOOTSTRAP_CI05', '0.0'))
ALL_IN_COST_BPS = max(1.0, float(os.getenv('EXECUTION_ALL_IN_COST_BPS', '8.0')))
PAPER_NOTIONAL_USDT = max(100.0, float(os.getenv('AUTONOMOUS_PAPER_NOTIONAL_USDT', '20000')))
LIVE_MIN_PREDICTED_EV_R = max(.0, float(os.getenv('AUTONOMOUS_LIVE_MIN_PREDICTED_EV_R', '.04')))
LIVE_MAX_OOD_FRACTION = max(.05, min(.80, float(os.getenv('AUTONOMOUS_LIVE_MAX_OOD_FRACTION', '.35'))))

EXCLUDED_FEATURES = {
    'macro_code', 'phase_code', 'daily_direction', 'h4_direction', 'h1_direction',
    'macro_alignment', 'structure_alignment',
}
FEATURE_NAMES = tuple(x for x in adaptive_v5.FEATURE_NAMES if x not in EXCLUDED_FEATURES)
FEATURE_INDEX = {name: i for i, name in enumerate(FEATURE_NAMES)}

HOLD_BARS_15M = (4, 8, 16, 32, 64, 96, 192, 384, 672)
EXPIRE_BARS_15M = (1, 2, 4, 8, 16, 32)
DECISION_STRIDES = (1, 2, 4, 8, 16, 32)
MODEL_MAX_LEAVES = (7, 11, 15, 23, 31)
MODEL_MIN_LEAF = (20, 30, 45, 70, 110)
MODEL_ITERS = (100, 160, 220, 300)

_INSTALLED = False
_ORIGINAL_PIPELINE_STATUS: Any | None = None


def _finite(x: Any, default: float = 0.0) -> float:
    try:
        y = float(x)
        return y if math.isfinite(y) else default
    except (TypeError, ValueError):
        return default


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def _json_default(value: Any) -> Any:
    if hasattr(value, 'item'):
        return value.item()
    raise TypeError(f'{type(value).__name__} is not JSON serializable')


def _ensure_tables(core: Any) -> None:
    con = core.db()
    try:
        con.execute(f'''CREATE TABLE IF NOT EXISTS {REGISTRY_TABLE}(
            strategy_id TEXT PRIMARY KEY,
            created_at INTEGER NOT NULL,
            status TEXT NOT NULL,
            direction TEXT NOT NULL,
            behavior_label TEXT NOT NULL,
            genome TEXT NOT NULL,
            metrics TEXT NOT NULL,
            model BLOB,
            active INTEGER NOT NULL DEFAULT 0
        )''')
        con.execute(f'CREATE INDEX IF NOT EXISTS ix_auto_v30_active ON {REGISTRY_TABLE}(active,status,created_at)')
        con.execute(f'''CREATE TABLE IF NOT EXISTS {AUDIT_TABLE}(
            finalist_id TEXT PRIMARY KEY,
            created_at INTEGER NOT NULL,
            status TEXT NOT NULL,
            genome TEXT NOT NULL,
            metrics TEXT NOT NULL
        )''')
        con.commit()
    finally:
        con.close()


def _clear_autonomous_products(core: Any) -> None:
    _ensure_tables(core)
    con = core.db()
    try:
        con.execute(f'DELETE FROM {REGISTRY_TABLE}')
        con.execute(f'DELETE FROM {AUDIT_TABLE}')
        con.execute('DELETE FROM system_state WHERE key IN (?,?,?)', (STATE_KEY, CHECKPOINT_KEY, 'v30_autonomous_live_state'))
        con.commit()
    finally:
        con.close()


def _hash_payload(payload: Any, n: int = 18) -> str:
    raw = json.dumps(payload, sort_keys=True, separators=(',', ':'), default=_json_default)
    return hashlib.sha256(raw.encode()).hexdigest()[:n]


def _strategy_id(genome: dict[str, Any]) -> str:
    return f'AUTO_{_hash_payload(genome, 14).upper()}'


def _sample_evenly(indices: np.ndarray, cap: int) -> np.ndarray:
    if len(indices) <= cap:
        return indices
    pos = np.linspace(0, len(indices) - 1, cap, dtype=np.int64)
    return indices[pos]


def _load_feature_snapshots(core: Any) -> dict[str, Any]:
    con = core.db()
    try:
        rows = con.execute('''SELECT ts,features,MAX(source_quality) quality
            FROM learning_samples
            WHERE ts>=? AND ts<?
            GROUP BY ts
            ORDER BY ts''', (RESEARCH_START_TS, RESEARCH_END_EXCLUSIVE_TS)).fetchall()
    finally:
        con.close()
    if len(rows) < 5000:
        return {}
    ts = np.asarray([int(r[0]) for r in rows], dtype=np.int64)
    x = np.empty((len(rows), len(FEATURE_NAMES)), dtype=np.float32)
    quality = np.asarray([_finite(r[2], 0.0) for r in rows], dtype=np.float32)
    for i, r in enumerate(rows):
        try:
            f = json.loads(r[1]) if isinstance(r[1], str) else dict(r[1] or {})
        except Exception:
            f = {}
        x[i] = np.asarray([_finite(f.get(name), 0.0) for name in FEATURE_NAMES], dtype=np.float32)
    return {'ts': ts, 'x': x, 'quality': quality}


def _deterministic_source(core: Any, asset: str, tf: str) -> str | None:
    try:
        import v10_final_integrity as fi
        return fi.deterministic_best_source(core, asset, tf)
    except Exception:
        return core._best_source(asset, tf)


def _load_market(core: Any) -> dict[str, Any]:
    src5 = _deterministic_source(core, 'ETH', '5m')
    src15 = _deterministic_source(core, 'ETH', '15m')
    if not src5 or not src15:
        return {}
    con = core.db()
    try:
        rows5 = con.execute('''SELECT ts,o,h,l,c FROM market_bars
            WHERE source=? AND asset='ETH' AND tf='5m' AND ts>=? AND ts<? ORDER BY ts''',
            (src5, RESEARCH_START_TS, SETTLEMENT_END_EXCLUSIVE_TS)).fetchall()
        rows15 = con.execute('''SELECT ts,c FROM market_bars
            WHERE source=? AND asset='ETH' AND tf='15m' AND ts>=? AND ts<? ORDER BY ts''',
            (src15, RESEARCH_START_TS, RESEARCH_END_EXCLUSIVE_TS)).fetchall()
    finally:
        con.close()
    if len(rows5) < 10000 or len(rows15) < 3000:
        return {}
    ts5 = np.asarray([int(r[0]) for r in rows5], dtype=np.int64)
    return {
        'source5': src5, 'source15': src15,
        'ts5': ts5,
        'o5': np.asarray([float(r[1]) for r in rows5], dtype=np.float64),
        'h5': np.asarray([float(r[2]) for r in rows5], dtype=np.float64),
        'l5': np.asarray([float(r[3]) for r in rows5], dtype=np.float64),
        'c5': np.asarray([float(r[4]) for r in rows5], dtype=np.float64),
        'close15': {int(r[0]): float(r[1]) for r in rows15},
    }


def _new_gate(rng: random.Random) -> list[dict[str, Any]]:
    n = rng.randint(0, 4)
    names = rng.sample(list(FEATURE_NAMES), n) if n else []
    return [
        {'feature': name, 'op': rng.choice(('GE', 'LE')), 'quantile': round(rng.uniform(.08, .92), 3)}
        for name in names
    ]


def _normalize_allocations(values: Iterable[float]) -> list[float]:
    raw = [max(0.01, float(x)) for x in values]
    s = sum(raw) or 1.0
    return [round(100.0 * x / s, 3) for x in raw]


def _new_genome(rng: random.Random, parent: dict[str, Any] | None = None) -> dict[str, Any]:
    if parent is None:
        n_targets = rng.randint(1, 4)
        rrs = sorted(round(rng.uniform(.45, 7.5), 3) for _ in range(n_targets))
        alloc = _normalize_allocations(rng.random() + .1 for _ in range(n_targets))
        subset_n = rng.randint(8, min(28, len(FEATURE_NAMES)))
        return {
            'direction': rng.choice(('LONG', 'SHORT')),
            'feature_names': sorted(rng.sample(list(FEATURE_NAMES), subset_n)),
            'gate': _new_gate(rng),
            'decision_stride': rng.choice(DECISION_STRIDES),
            'entry_market': bool(rng.getrandbits(1)),
            'entry_offset_atr': round(rng.uniform(-1.10, 1.10), 3),
            'stop_atr': round(rng.uniform(.55, 5.50), 3),
            'target_rr': rrs,
            'allocations': alloc,
            'expire_bars': rng.choice(EXPIRE_BARS_15M),
            'max_hold_bars': rng.choice(HOLD_BARS_15M),
            'breakeven_after_r': round(rng.uniform(.35, 2.2), 3),
            'trail_start_r': round(rng.uniform(.8, 4.5), 3),
            'trail_lock_r': round(rng.uniform(.05, 2.5), 3),
            'cooldown_bars': rng.choice((0, 1, 2, 4, 8, 16, 32)),
            'model_learning_rate': round(10 ** rng.uniform(-1.7, -0.75), 5),
            'model_max_iter': rng.choice(MODEL_ITERS),
            'model_max_leaf_nodes': rng.choice(MODEL_MAX_LEAVES),
            'model_min_samples_leaf': rng.choice(MODEL_MIN_LEAF),
            'model_l2': round(10 ** rng.uniform(-1.0, .8), 4),
        }
    g = json.loads(json.dumps(parent))
    fields = rng.sample(
        ['direction', 'feature_names', 'gate', 'decision_stride', 'entry_market', 'entry_offset_atr',
         'stop_atr', 'target_rr', 'allocations', 'expire_bars', 'max_hold_bars',
         'breakeven_after_r', 'trail_start_r', 'trail_lock_r', 'cooldown_bars',
         'model_learning_rate', 'model_max_iter', 'model_max_leaf_nodes', 'model_min_samples_leaf', 'model_l2'],
        rng.randint(2, 6),
    )
    donor = _new_genome(rng, None)
    for key in fields:
        if key in ('entry_offset_atr', 'stop_atr', 'breakeven_after_r', 'trail_start_r', 'trail_lock_r', 'model_learning_rate', 'model_l2'):
            base = float(g[key]); alt = float(donor[key]); g[key] = round(.65 * base + .35 * alt, 5)
        else:
            g[key] = donor[key]
    if len(g['target_rr']) != len(g['allocations']):
        g['allocations'] = _normalize_allocations([1.0] * len(g['target_rr']))
    g['target_rr'] = sorted(float(x) for x in g['target_rr'])
    g['allocations'] = _normalize_allocations(g['allocations'])
    g['stop_atr'] = round(_clamp(float(g['stop_atr']), .45, 6.0), 3)
    g['trail_lock_r'] = round(min(float(g['trail_lock_r']), max(.0, float(g['trail_start_r']) - .05)), 3)
    return g


def _gate_thresholds(x_train: np.ndarray, gate: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for cond in gate:
        idx = FEATURE_INDEX.get(str(cond.get('feature')))
        if idx is None:
            continue
        q = _clamp(_finite(cond.get('quantile'), .5), .01, .99)
        value = float(np.quantile(x_train[:, idx], q))
        out.append({'feature': FEATURE_NAMES[idx], 'op': str(cond.get('op') or 'GE'), 'value': value, 'quantile': q})
    return out


def _gate_mask(x: np.ndarray, thresholds: list[dict[str, Any]]) -> np.ndarray:
    mask = np.ones(len(x), dtype=bool)
    for cond in thresholds:
        idx = FEATURE_INDEX[str(cond['feature'])]
        if cond['op'] == 'LE':
            mask &= x[:, idx] <= float(cond['value'])
        else:
            mask &= x[:, idx] >= float(cond['value'])
    return mask


def _decision_mask(ts: np.ndarray, stride: int) -> np.ndarray:
    if stride <= 1:
        return np.ones(len(ts), dtype=bool)
    slot = (ts // 900).astype(np.int64)
    return (slot % int(stride)) == 0


def _simulate_trade(market: dict[str, Any], ts: int, features: np.ndarray, genome: dict[str, Any]) -> dict[str, Any]:
    close = market['close15'].get(int(ts))
    if close is None or close <= 0:
        return {'valid': False, 'filled': False, 'pnl_r': 0.0, 'reason': 'missing_decision_close'}
    atr_pct = max(abs(float(features[FEATURE_INDEX.get('atr_pct', 0)])), .00035)
    atr_abs = max(close * atr_pct, close * .00035)
    sign = 1.0 if genome['direction'] == 'LONG' else -1.0
    decision_close_ts = int(ts) + 900
    start = int(np.searchsorted(market['ts5'], decision_close_ts, side='left'))
    if start >= len(market['ts5']) or int(market['ts5'][start]) != decision_close_ts:
        return {'valid': False, 'filled': False, 'pnl_r': 0.0, 'reason': 'missing_first_future_5m'}

    max_hold_5 = int(genome['max_hold_bars']) * 3
    end = min(len(market['ts5']), start + max_hold_5)
    if end <= start:
        return {'valid': False, 'filled': False, 'pnl_r': 0.0, 'reason': 'no_future_path'}
    segment = market['ts5'][start:end]
    if len(segment) > 1 and bool(np.any(np.diff(segment) != 300)):
        return {'valid': False, 'filled': False, 'pnl_r': 0.0, 'reason': 'future_5m_gap'}

    planned_entry = float(close + sign * float(genome['entry_offset_atr']) * atr_abs)
    stop_distance = max(float(genome['stop_atr']) * atr_abs, close * .0008)
    planned_stop = planned_entry - sign * stop_distance
    expire_5 = min(end - start, int(genome['expire_bars']) * 3)

    if genome.get('entry_market'):
        fill_idx = start
        entry = float(market['o5'][start])
    else:
        fill_idx = None
        for j in range(start, start + expire_5):
            if market['l5'][j] <= planned_entry <= market['h5'][j]:
                fill_idx = j
                break
        if fill_idx is None:
            return {'valid': True, 'filled': False, 'pnl_r': 0.0, 'reason': 'entry_not_filled'}
        entry = planned_entry

    risk = abs(entry - planned_stop)
    if risk <= max(entry * 1e-6, 1e-9):
        return {'valid': False, 'filled': False, 'pnl_r': 0.0, 'reason': 'invalid_risk'}
    stop = planned_stop
    targets = [entry + sign * risk * float(rr) for rr in genome['target_rr']]
    allocations = [float(x) / 100.0 for x in genome['allocations']]
    remaining = 1.0
    realized = 0.0
    hit: set[int] = set()
    last = entry
    max_fav_r = 0.0

    for j in range(int(fill_idx), end):
        low = float(market['l5'][j]); high = float(market['h5'][j]); last = float(market['c5'][j])
        stop_hit = low <= stop if sign > 0 else high >= stop
        if stop_hit:
            realized += remaining * ((stop - entry) * sign / risk)
            remaining = 0.0
            break
        fav = (high - entry) / risk if sign > 0 else (entry - low) / risk
        max_fav_r = max(max_fav_r, fav)
        if j != fill_idx:
            for k, px in enumerate(targets):
                if k in hit or remaining <= 1e-12:
                    continue
                target_hit = high >= px if sign > 0 else low <= px
                if target_hit:
                    frac = min(remaining, allocations[k])
                    realized += frac * float(genome['target_rr'][k])
                    remaining -= frac
                    hit.add(k)
        if max_fav_r >= float(genome['breakeven_after_r']):
            stop = max(stop, entry) if sign > 0 else min(stop, entry)
        if max_fav_r >= float(genome['trail_start_r']):
            lock = entry + sign * float(genome['trail_lock_r']) * risk
            stop = max(stop, lock) if sign > 0 else min(stop, lock)
        if remaining <= 1e-12:
            break

    if remaining > 1e-12:
        realized += remaining * ((last - entry) * sign / risk)
    cost_r = (ALL_IN_COST_BPS / 10000.0) * entry / risk
    return {
        'valid': True, 'filled': True, 'pnl_r': float(realized - cost_r),
        'gross_r': float(realized), 'cost_r': float(cost_r), 'fill_ts': int(market['ts5'][fill_idx]),
        'entry': float(entry), 'stop': float(planned_stop),
    }


def _stats(results: list[dict[str, Any]]) -> dict[str, float]:
    filled = [x for x in results if x.get('valid') and x.get('filled')]
    pnls = [float(x['pnl_r']) for x in filled]
    gains = sum(max(x, 0.0) for x in pnls)
    losses = sum(max(-x, 0.0) for x in pnls)
    eq = peak = dd = 0.0
    for p in pnls:
        eq += p; peak = max(peak, eq); dd = max(dd, peak - eq)
    return {
        'fills': float(len(filled)), 'pf': gains / max(losses, 1e-9),
        'ev': statistics.mean(pnls) if pnls else -9.0,
        'win': sum(p > 0 for p in pnls) / len(pnls) if pnls else 0.0,
        'dd': dd, 'sum_r': sum(pnls),
    }


def _model(genome: dict[str, Any], seed: int) -> HistGradientBoostingRegressor:
    return HistGradientBoostingRegressor(
        loss='squared_error', learning_rate=float(genome['model_learning_rate']),
        max_iter=int(genome['model_max_iter']), max_leaf_nodes=int(genome['model_max_leaf_nodes']),
        min_samples_leaf=int(genome['model_min_samples_leaf']), l2_regularization=float(genome['model_l2']),
        random_state=int(seed),
    )


def _simulate_indices(indices: np.ndarray, snapshots: dict[str, Any], market: dict[str, Any], genome: dict[str, Any]) -> tuple[np.ndarray, np.ndarray, list[dict[str, Any]]]:
    xs: list[np.ndarray] = []; ys: list[float] = []; results: list[dict[str, Any]] = []
    for idx in indices.tolist():
        res = _simulate_trade(market, int(snapshots['ts'][idx]), snapshots['x'][idx], genome)
        results.append(res)
        if res.get('valid') and res.get('filled'):
            xs.append(snapshots['x'][idx]); ys.append(float(res['pnl_r']))
    if not xs:
        return np.empty((0, snapshots['x'].shape[1]), dtype=np.float32), np.empty(0, dtype=np.float32), results
    return np.asarray(xs, dtype=np.float32), np.asarray(ys, dtype=np.float32), results


def _threshold_from_cal(pred: np.ndarray, realised: np.ndarray) -> tuple[float, dict[str, float]] | None:
    if len(pred) < 35:
        return None
    candidates = np.unique(np.quantile(pred, np.linspace(.40, .90, 11)))
    best: tuple[float, dict[str, float], float] | None = None
    for t in candidates:
        mask = pred >= float(t)
        if int(mask.sum()) < 18:
            continue
        pnls = realised[mask].astype(float).tolist()
        st = _stats([{'valid': True, 'filled': True, 'pnl_r': p} for p in pnls])
        utility = st['ev'] * 4.0 + math.log(max(st['pf'], 1e-6)) * .28 - st['dd'] * .006 + min(st['fills'], 80.0) / 80.0 * .08
        if best is None or utility > best[2]:
            best = (float(t), st, utility)
    return None if best is None else (best[0], best[1])


def _feature_subset_matrix(x: np.ndarray, genome: dict[str, Any]) -> np.ndarray:
    idx = [FEATURE_INDEX[n] for n in genome['feature_names'] if n in FEATURE_INDEX]
    return x[:, idx] if idx else x


def _evaluate_candidate(snapshots: dict[str, Any], market: dict[str, Any], genome: dict[str, Any], seed: int) -> dict[str, Any] | None:
    ts = snapshots['ts']; x = snapshots['x']; n = len(ts)
    holdout_start = int(n * (1.0 - FINAL_HOLDOUT_PCT))
    dev_end = max(2000, holdout_start - 32)
    if dev_end < 3000:
        return None
    anchors = (.48, .66, .82)
    fold_stats: list[dict[str, float]] = []; fold_thresholds: list[float] = []
    for fi, frac in enumerate(anchors):
        test_start = int(dev_end * frac)
        test_end = int(dev_end * (anchors[fi + 1] if fi + 1 < len(anchors) else .98))
        train_end = max(800, test_start - 32)
        if test_end - test_start < 200:
            continue
        fit_end = max(600, int(train_end * .80) - 16)
        cal_start = min(train_end - 120, fit_end + 16)
        if cal_start <= fit_end or train_end - cal_start < 100:
            continue
        thresholds = _gate_thresholds(x[:fit_end], genome['gate'])
        stride_mask = _decision_mask(ts, int(genome['decision_stride']))
        fit_mask = _gate_mask(x[:fit_end], thresholds) & stride_mask[:fit_end]
        cal_mask = _gate_mask(x[cal_start:train_end], thresholds) & stride_mask[cal_start:train_end]
        test_mask = _gate_mask(x[test_start:test_end], thresholds) & stride_mask[test_start:test_end]
        fit_idx = np.where(fit_mask)[0]
        cal_idx = np.where(cal_mask)[0] + cal_start
        test_idx = np.where(test_mask)[0] + test_start
        if len(fit_idx) < 140 or len(cal_idx) < 50 or len(test_idx) < 60:
            continue
        fit_idx = _sample_evenly(fit_idx, TRAIN_SIM_CAP); cal_idx = _sample_evenly(cal_idx, CAL_SIM_CAP); test_idx = _sample_evenly(test_idx, TEST_SIM_CAP)
        x_fit, y_fit, _ = _simulate_indices(fit_idx, snapshots, market, genome)
        x_cal, y_cal, _ = _simulate_indices(cal_idx, snapshots, market, genome)
        if len(y_fit) < 100 or len(y_cal) < 30 or float(np.std(y_fit)) < 1e-6:
            continue
        m = _model(genome, seed + fi * 31)
        m.fit(_feature_subset_matrix(x_fit, genome), y_fit)
        pred_cal = m.predict(_feature_subset_matrix(x_cal, genome))
        picked = _threshold_from_cal(pred_cal, y_cal)
        if picked is None:
            continue
        threshold, _ = picked
        pred_test = m.predict(_feature_subset_matrix(snapshots['x'][test_idx], genome))
        selected_idx = test_idx[pred_test >= threshold]
        if len(selected_idx) < 20:
            continue
        selected_idx = _sample_evenly(selected_idx, TEST_SIM_CAP)
        _, _, test_results = _simulate_indices(selected_idx, snapshots, market, genome)
        st = _stats(test_results)
        if st['fills'] < 16:
            continue
        fold_stats.append(st); fold_thresholds.append(float(threshold))
        del m, x_fit, y_fit, x_cal, y_cal, pred_cal, pred_test
        gc.collect()
    if len(fold_stats) < 2:
        return None
    evs = [z['ev'] for z in fold_stats]; pfs = [z['pf'] for z in fold_stats]
    stability = _clamp(1.0 - .62 * statistics.pstdev(evs) - .025 * statistics.pstdev(pfs), 0.0, 1.0)
    profitable = sum(x > 0 for x in evs) / len(evs); worst = min(evs)
    fills = sum(z['fills'] for z in fold_stats); avg_ev = statistics.mean(evs); avg_pf = statistics.mean(pfs); avg_dd = statistics.mean(z['dd'] for z in fold_stats)
    score = avg_ev * 5.0 + math.log(max(avg_pf, 1e-6)) * .38 + stability * .32 + profitable * .24 - avg_dd * .006 + min(worst, .12)
    return {
        'score': float(score), 'ev': float(avg_ev), 'pf': float(avg_pf), 'dd': float(avg_dd),
        'stability': float(stability), 'profitable_folds': float(profitable), 'worst_fold_ev': float(worst),
        'development_fills': int(fills), 'threshold_hint': float(statistics.median(fold_thresholds)), 'folds': fold_stats,
    }


def _diversity_key(genome: dict[str, Any]) -> tuple[Any, ...]:
    hold = int(genome['max_hold_bars']); hold_bucket = 0 if hold <= 16 else 1 if hold <= 64 else 2 if hold <= 192 else 3
    gate_names = tuple(sorted(str(x.get('feature')) for x in genome.get('gate', []))[:3])
    entry_bucket = -1 if float(genome['entry_offset_atr']) < -.15 else 1 if float(genome['entry_offset_atr']) > .15 else 0
    return (genome['direction'], hold_bucket, entry_bucket, gate_names)


def _behavior_label(genome: dict[str, Any], gate_thresholds: list[dict[str, Any]]) -> str:
    hours = max(.25, float(genome['max_hold_bars']) * .25)
    gate = ', '.join(f"{x['feature']} {'≤' if x['op']=='LE' else '≥'} {x['value']:.3g}" for x in gate_thresholds[:3]) or 'all-state'
    return f"AI_STATE[{gate}] · {genome['direction']} · hold≤{hours:g}h"


def _bootstrap_ci05(pnls: list[float], seed: int, reps: int = 400, block: int = 8) -> float:
    if len(pnls) < 20:
        return -9.0
    rng = np.random.default_rng(seed); arr = np.asarray(pnls, dtype=float); vals = np.empty(reps, dtype=float)
    for i in range(reps):
        sample: list[float] = []
        while len(sample) < len(arr):
            start = int(rng.integers(0, max(1, len(arr) - block + 1)))
            sample.extend(arr[start:start + block].tolist())
        vals[i] = float(np.mean(sample[:len(arr)]))
    return float(np.quantile(vals, .05))


def _fit_and_audit_finalist(snapshots: dict[str, Any], market: dict[str, Any], genome: dict[str, Any], dev: dict[str, Any], seed: int) -> dict[str, Any]:
    ts = snapshots['ts']; x = snapshots['x']; n = len(ts); holdout_start = int(n * (1.0 - FINAL_HOLDOUT_PCT)); train_end = max(1000, holdout_start - 32)
    gate_thresholds = _gate_thresholds(x[:train_end], genome['gate']); stride = _decision_mask(ts, int(genome['decision_stride']))
    train_mask = _gate_mask(x[:train_end], gate_thresholds) & stride[:train_end]; hold_mask = _gate_mask(x[holdout_start:], gate_thresholds) & stride[holdout_start:]
    train_idx = np.where(train_mask)[0]; hold_idx = np.where(hold_mask)[0] + holdout_start
    if len(train_idx) < 300 or len(hold_idx) < 80:
        return {'status': 'REJECTED_INSUFFICIENT_STATE_SUPPORT', 'promoted': False, 'gate_thresholds': gate_thresholds, 'reason': 'autonomous state has insufficient chronological support'}
    cal_n = max(120, int(len(train_idx) * .18)); fit_idx = _sample_evenly(train_idx[:-cal_n], FINAL_REFIT_CAP); cal_idx = _sample_evenly(train_idx[-cal_n:], min(CAL_SIM_CAP * 2, 6000))
    x_fit, y_fit, _ = _simulate_indices(fit_idx, snapshots, market, genome); x_cal, y_cal, _ = _simulate_indices(cal_idx, snapshots, market, genome)
    if len(y_fit) < 180 or len(y_cal) < 50:
        return {'status': 'REJECTED_INSUFFICIENT_FILLED_TRAINING', 'promoted': False, 'gate_thresholds': gate_thresholds, 'reason': 'too few causal fills to estimate direct expected R'}
    model = _model(genome, seed); model.fit(_feature_subset_matrix(x_fit, genome), y_fit)
    pred_cal = model.predict(_feature_subset_matrix(x_cal, genome)); picked = _threshold_from_cal(pred_cal, y_cal)
    if picked is None:
        return {'status': 'REJECTED_NO_EV_THRESHOLD', 'promoted': False, 'gate_thresholds': gate_thresholds, 'reason': 'no viable direct-R selection threshold in development calibration'}
    threshold, cal_stats = picked
    pred_hold = model.predict(_feature_subset_matrix(snapshots['x'][hold_idx], genome)); selected_idx = hold_idx[pred_hold >= threshold]
    _, _, audit_results = _simulate_indices(selected_idx, snapshots, market, genome)
    invalid_paths = sum(1 for z in audit_results if not z.get('valid')); audit = _stats(audit_results)
    pnls = [float(z['pnl_r']) for z in audit_results if z.get('valid') and z.get('filled')]; ci05 = _bootstrap_ci05(pnls, seed + 99)
    promoted = bool(
        audit['fills'] >= MIN_OOS_FILLS and audit['pf'] >= MIN_OOS_PF and audit['ev'] >= MIN_OOS_EV_R and
        audit['dd'] <= MAX_OOS_DD_R and ci05 > MIN_BOOTSTRAP_CI05 and invalid_paths == 0 and
        float(dev['stability']) >= MIN_WF_STABILITY and float(dev['profitable_folds']) >= MIN_PROFITABLE_FOLDS and
        float(dev['worst_fold_ev']) >= MIN_WORST_FOLD_EV
    )
    metrics = {
        'schema': SCHEMA, 'validation_method': 'DIRECT_R_WALK_FORWARD_THEN_ONE_TIME_CHRONOLOGICAL_OOS',
        'historical_no_lookahead': True, 'legacy_success_label_used': False, 'legacy_strategy_family_used': False,
        'manual_regime_gate_used': False, 'manual_phase_gate_used': False,
        'oos_fills': int(audit['fills']), 'profit_factor': float(audit['pf']), 'expectancy_r': float(audit['ev']),
        'test_win': float(audit['win']), 'max_drawdown_r': float(audit['dd']), 'total_oos_r': float(audit['sum_r']),
        'bootstrap_ci05_r': float(ci05), 'invalid_future_paths': int(invalid_paths),
        'stability': float(dev['stability']), 'profitable_folds': float(dev['profitable_folds']),
        'worst_fold_ev': float(dev['worst_fold_ev']), 'development_pf': float(dev['pf']), 'development_ev': float(dev['ev']),
        'direct_r_threshold': float(threshold), 'calibration_stats': cal_stats,
        'gate_thresholds': gate_thresholds, 'feature_names': list(genome['feature_names']),
        'research_start_ts': RESEARCH_START_TS, 'research_end_exclusive_ts': RESEARCH_END_EXCLUSIVE_TS,
        'settlement_end_exclusive_ts': SETTLEMENT_END_EXCLUSIVE_TS,
        'paper_notional_usdt': PAPER_NOTIONAL_USDT, 'leverage_mode': 'MAX_AVAILABLE_AT_ORDER_TIME',
    }
    if not promoted:
        metrics['reason'] = f"OOS rejected: fills={int(audit['fills'])}, PF={audit['pf']:.2f}, EV={audit['ev']:+.3f}R, CI05={ci05:+.3f}R, DD={audit['dd']:.2f}R, invalid_paths={invalid_paths}"
        return {'status': 'REJECTED_AUTONOMOUS_OOS', 'promoted': False, 'metrics': metrics, 'genome': genome, 'gate_thresholds': gate_thresholds}
    full_mask = _gate_mask(x, gate_thresholds) & stride; full_idx = _sample_evenly(np.where(full_mask)[0], FINAL_REFIT_CAP)
    x_full, y_full, _ = _simulate_indices(full_idx, snapshots, market, genome); final_model = _model(genome, seed + 1)
    final_model.fit(_feature_subset_matrix(x_full, genome), y_full)
    feature_idx = [FEATURE_INDEX[n] for n in genome['feature_names']]; train_matrix = snapshots['x'][full_idx][:, feature_idx]
    metrics.update({
        'reason': 'complete autonomous package passed chronological OOS',
        'feature_median': np.median(train_matrix, axis=0).astype(float).tolist(),
        'feature_q1': np.quantile(train_matrix, .25, axis=0).astype(float).tolist(),
        'feature_q3': np.quantile(train_matrix, .75, axis=0).astype(float).tolist(),
    })
    return {'status': 'PROMOTED', 'promoted': True, 'metrics': metrics, 'genome': genome, 'gate_thresholds': gate_thresholds, 'model_blob': pickle.dumps(final_model, pickle.HIGHEST_PROTOCOL)}


def _save_audit(core: Any, finalist_id: str, genome: dict[str, Any], result: dict[str, Any]) -> None:
    _ensure_tables(core); con = core.db()
    try:
        con.execute(f'''INSERT OR REPLACE INTO {AUDIT_TABLE}(finalist_id,created_at,status,genome,metrics) VALUES(?,?,?,?,?)''', (
            finalist_id, int(time.time()), str(result.get('status') or 'UNKNOWN'),
            json.dumps(genome, separators=(',', ':'), default=_json_default),
            json.dumps(result.get('metrics') or {'reason': result.get('reason')}, separators=(',', ':'), ensure_ascii=False, default=_json_default),
        )); con.commit()
    finally:
        con.close()


def _save_champion(core: Any, rank: int, result: dict[str, Any]) -> dict[str, Any]:
    genome = dict(result['genome']); metrics = dict(result['metrics']); gate = list(result['gate_thresholds']); sid = _strategy_id(genome); label = _behavior_label(genome, gate)
    metrics['strategy_id'] = sid; metrics['behavior_label'] = label; metrics['rank'] = int(rank)
    _ensure_tables(core); con = core.db()
    try:
        con.execute(f'''INSERT OR REPLACE INTO {REGISTRY_TABLE}(strategy_id,created_at,status,direction,behavior_label,genome,metrics,model,active) VALUES(?,?,?,?,?,?,?,?,1)''', (
            sid, int(time.time()), 'CHAMPION', str(genome['direction']), label,
            json.dumps(genome, separators=(',', ':'), default=_json_default),
            json.dumps(metrics, ensure_ascii=False, separators=(',', ':'), default=_json_default), result['model_blob'],
        )); con.commit()
    finally:
        con.close()
    return {'strategy_id': sid, 'direction': genome['direction'], 'behavior_label': label, **metrics}


def _load_registry(core: Any, active_only: bool = True) -> list[dict[str, Any]]:
    _ensure_tables(core); con = core.db()
    try:
        sql = f'SELECT strategy_id,created_at,status,direction,behavior_label,genome,metrics,model,active FROM {REGISTRY_TABLE}'
        if active_only:
            sql += " WHERE active=1 AND status='CHAMPION'"
        sql += ' ORDER BY created_at, strategy_id'; rows = con.execute(sql).fetchall()
    finally:
        con.close()
    out = []
    for r in rows:
        try:
            genome = json.loads(r[5]); metrics = json.loads(r[6])
        except Exception:
            continue
        out.append({'strategy_id': str(r[0]), 'created_at': int(r[1]), 'status': str(r[2]), 'direction': str(r[3]), 'behavior_label': str(r[4]), 'genome': genome, 'metrics': metrics, 'model_blob': bytes(r[7]) if r[7] is not None else None, 'active': bool(r[8])})
    return out


def _load_audits(core: Any) -> list[dict[str, Any]]:
    _ensure_tables(core); con = core.db()
    try:
        rows = con.execute(f'SELECT finalist_id,created_at,status,genome,metrics FROM {AUDIT_TABLE} ORDER BY created_at,finalist_id').fetchall()
    finally:
        con.close()
    out = []
    for r in rows:
        try:
            out.append({'finalist_id': r[0], 'created_at': int(r[1]), 'status': r[2], 'genome': json.loads(r[3]), 'metrics': json.loads(r[4])})
        except Exception:
            continue
    return out


def _evolution(core: Any, snapshots: dict[str, Any], market: dict[str, Any]) -> list[tuple[float, dict[str, Any], dict[str, Any]]]:
    seed_base = int(hashlib.sha256(f'v30|{len(snapshots["ts"])}|{snapshots["ts"][-1]}'.encode()).hexdigest()[:12], 16)
    rng = random.Random(seed_base); population = [_new_genome(rng) for _ in range(POPULATION)]
    elites: list[tuple[float, dict[str, Any], dict[str, Any]]] = []; archive: dict[str, tuple[float, dict[str, Any], dict[str, Any]]] = {}
    checkpoint = core.get_state(CHECKPOINT_KEY, {}); start_generation = 0
    if isinstance(checkpoint, dict) and checkpoint.get('schema') == SCHEMA and checkpoint.get('status') == 'RUNNING':
        saved = checkpoint.get('elites') or []
        if saved:
            try:
                elites = [(float(x['score']), dict(x['genome']), dict(x['result'])) for x in saved]
                start_generation = max(0, min(GENERATIONS - 1, int(checkpoint.get('generation') or 0) + 1))
                rr = random.Random(seed_base + start_generation * 100003); population = []
                while len(population) < POPULATION:
                    population.append(_new_genome(rr, rr.choice(elites)[1]) if elites and len(population) < int(POPULATION * .75) else _new_genome(rr))
            except Exception:
                elites = []; start_generation = 0
    for generation in range(start_generation, GENERATIONS):
        scored: list[tuple[float, dict[str, Any], dict[str, Any]]] = []
        for ci, genome in enumerate(population):
            gid = _hash_payload(genome)
            core.state['autonomous_live_progress'] = {'stage': 'DIRECT_R_AUTONOMOUS_EVOLUTION', 'generation': generation + 1, 'generations': GENERATIONS, 'candidate': ci + 1, 'population': len(population), 'candidate_id': gid, 'direction': genome['direction'], 'max_hold_bars': genome['max_hold_bars'], 'gate_conditions': genome['gate'], 'updated_at': int(time.time())}
            result = _evaluate_candidate(snapshots, market, genome, seed_base + generation * 1000 + ci * 17)
            if result is None:
                continue
            item = (float(result['score']), genome, result); scored.append(item); prev = archive.get(gid)
            if prev is None or item[0] > prev[0]:
                archive[gid] = item
            if (ci + 1) % 6 == 0:
                gc.collect()
        dedup: dict[str, tuple[float, dict[str, Any], dict[str, Any]]] = {}
        for item in elites + scored:
            gid = _hash_payload(item[1])
            if gid not in dedup or item[0] > dedup[gid][0]:
                dedup[gid] = item
        elites = sorted(dedup.values(), key=lambda z: z[0], reverse=True)[:ELITES]
        core.set_state(CHECKPOINT_KEY, {'schema': SCHEMA, 'status': 'RUNNING', 'generation': generation, 'elites': [{'score': s, 'genome': g, 'result': r} for s, g, r in elites], 'updated_at': int(time.time())})
        if generation == GENERATIONS - 1 or not elites:
            break
        rr = random.Random(seed_base + (generation + 1) * 100003); population = []
        while len(population) < POPULATION:
            population.append(_new_genome(rr, rr.choice(elites)[1]) if len(population) < int(POPULATION * .72) else _new_genome(rr))
        gc.collect()
    ranked = sorted(archive.values(), key=lambda z: z[0], reverse=True); finalists = []; used: dict[tuple[Any, ...], int] = {}
    for item in ranked:
        key = _diversity_key(item[1])
        if used.get(key, 0) >= 2:
            continue
        used[key] = used.get(key, 0) + 1; finalists.append(item)
        if len(finalists) >= FINALISTS:
            break
    return finalists


def autonomous_certify(core: Any, force: bool = False) -> list[dict[str, Any]]:
    _ = force; replay = runtime_integrity.replay_progress(core)
    if not replay.get('complete'):
        core.state[STATE_KEY] = {'schema': SCHEMA, 'status': 'WAITING_FOR_REPLAY', 'replay': replay, 'updated_at': int(time.time())}; return []
    try:
        audit = final_system.final_audit(core, allow_auto_rebuild=False)
    except Exception as exc:
        core.state[STATE_KEY] = {'schema': SCHEMA, 'status': 'WAITING_DATA_AUDIT', 'error': f'{type(exc).__name__}: {exc}', 'updated_at': int(time.time())}; return []
    if not audit.get('valid'):
        core.state[STATE_KEY] = {'schema': SCHEMA, 'status': 'WAITING_DATA_AUDIT', 'audit': audit, 'updated_at': int(time.time())}; return []
    existing = _load_registry(core, active_only=True); checkpoint = core.get_state(CHECKPOINT_KEY, {})
    if existing and isinstance(checkpoint, dict) and checkpoint.get('status') == 'COMPLETE':
        core.state[STATE_KEY] = {'schema': SCHEMA, 'status': 'COMPLETE', 'champions': existing, 'updated_at': int(time.time())}; return existing
    snapshots = _load_feature_snapshots(core); market = _load_market(core)
    if not snapshots:
        core.state[STATE_KEY] = {'schema': SCHEMA, 'status': 'WAITING_CAUSAL_FEATURE_SNAPSHOTS', 'updated_at': int(time.time())}; return []
    if not market:
        core.state[STATE_KEY] = {'schema': SCHEMA, 'status': 'WAITING_MARKET_CACHE', 'updated_at': int(time.time())}; return []
    core.state[STATE_KEY] = {'schema': SCHEMA, 'status': 'AUTONOMOUS_EVOLUTION_RUNNING', 'decision_timestamps': int(len(snapshots['ts'])), 'population': POPULATION, 'generations': GENERATIONS, 'legacy_strategy_family_used': False, 'legacy_success_label_used': False, 'updated_at': int(time.time())}
    finalists = _evolution(core, snapshots, market)
    if not finalists:
        core.set_state(CHECKPOINT_KEY, {'schema': SCHEMA, 'status': 'COMPLETE', 'generation': GENERATIONS - 1, 'champions': 0, 'reason': 'no candidate survived direct-R development walk-forward', 'updated_at': int(time.time())})
        core.state[STATE_KEY] = {'schema': SCHEMA, 'status': 'COMPLETE_NO_CERTIFIED_PACKAGE', 'champions': [], 'updated_at': int(time.time())}; return []
    audited: list[tuple[float, dict[str, Any]]] = []; prior_audits = {x['finalist_id']: x for x in _load_audits(core)}
    for rank, (score, genome, dev) in enumerate(finalists, 1):
        fid = _hash_payload({'genome': genome, 'dev': dev}, 20)
        if fid in prior_audits:
            continue
        core.state['autonomous_live_progress'] = {'stage': 'ONE_TIME_COMPLETE_PACKAGE_OOS', 'finalist': rank, 'finalists': len(finalists), 'candidate_id': fid, 'development_score': score, 'updated_at': int(time.time())}
        result = _fit_and_audit_finalist(snapshots, market, genome, dev, 303000 + rank * 97); _save_audit(core, fid, genome, result)
        if result.get('promoted'):
            quality = float(result['metrics']['expectancy_r']) * 5.0 + math.log(max(float(result['metrics']['profit_factor']), 1e-6)) - float(result['metrics']['max_drawdown_r']) * .01
            audited.append((quality, result))
        gc.collect()
    champions: list[dict[str, Any]] = []
    if audited:
        audited.sort(key=lambda z: z[0], reverse=True); diversity: dict[tuple[Any, ...], int] = {}
        for _, result in audited:
            key = _diversity_key(result['genome'])
            if diversity.get(key, 0) >= 2:
                continue
            diversity[key] = diversity.get(key, 0) + 1; champions.append(_save_champion(core, len(champions) + 1, result))
            if len(champions) >= MAX_CHAMPIONS:
                break
    else:
        champions = [{'strategy_id': x['strategy_id'], 'direction': x['direction'], 'behavior_label': x['behavior_label'], **x['metrics']} for x in _load_registry(core, active_only=True)]
    core.set_state(CHECKPOINT_KEY, {'schema': SCHEMA, 'status': 'COMPLETE', 'generation': GENERATIONS - 1, 'finalists': len(finalists), 'champions': len(champions), 'updated_at': int(time.time())})
    core.state[STATE_KEY] = {'schema': SCHEMA, 'status': 'COMPLETE' if champions else 'COMPLETE_NO_CERTIFIED_PACKAGE', 'champions': champions, 'finalists': len(finalists), 'audits': len(_load_audits(core)), 'legacy_strategy_family_used': False, 'legacy_success_label_used': False, 'manual_regime_gate_used': False, 'manual_phase_gate_used': False, 'updated_at': int(time.time())}
    snapshots.clear(); market.clear(); gc.collect(); return champions


def _live_gate(features: dict[str, float], thresholds: list[dict[str, Any]]) -> bool:
    for c in thresholds:
        value = _finite(features.get(str(c['feature'])), 0.0)
        if c['op'] == 'LE' and not value <= float(c['value']): return False
        if c['op'] != 'LE' and not value >= float(c['value']): return False
    return True


def _ood_fraction(features: dict[str, float], metrics: dict[str, Any]) -> float:
    names = list(metrics.get('feature_names') or []); med = list(metrics.get('feature_median') or []); q1 = list(metrics.get('feature_q1') or []); q3 = list(metrics.get('feature_q3') or [])
    if not names or len(names) != len(med) or len(q1) != len(names) or len(q3) != len(names): return 0.0
    bad = 0
    for i, name in enumerate(names):
        iqr = max(abs(float(q3[i]) - float(q1[i])), 1e-6); value = _finite(features.get(name), float(med[i]))
        if abs(value - float(med[i])) > 4.0 * iqr: bad += 1
    return bad / len(names)


def _generic_plan_from_genome(price: float, atr_pct: float, genome: dict[str, Any]) -> dict[str, Any]:
    sign = 1.0 if genome['direction'] == 'LONG' else -1.0; atr_abs = max(float(price) * max(abs(float(atr_pct)), .00035), float(price) * .00035)
    entry = float(price + sign * float(genome['entry_offset_atr']) * atr_abs); risk = max(float(genome['stop_atr']) * atr_abs, price * .0008); stop = entry - sign * risk
    targets = [{'price': round(entry + sign * risk * float(rr), 4), 'rr': float(rr), 'allocation': float(alloc)} for rr, alloc in zip(genome['target_rr'], genome['allocations'])]
    return {'entry': round(entry, 4), 'stop': round(stop, 4), 'risk': risk, 'targets': targets, 'management': {'entry_market': bool(genome['entry_market']), 'expire_bars': int(genome['expire_bars']), 'max_hold_bars': int(genome['max_hold_bars']), 'breakeven_after_r': float(genome['breakeven_after_r']), 'trail_start_r': float(genome['trail_start_r']), 'trail_lock_r': float(genome['trail_lock_r']), 'cooldown_bars': int(genome['cooldown_bars']), 'never_widen_stop': True, 'initial_plan_immutable': True}}


def _autonomous_analysis(core: Any, bundle: dict[str, Any]) -> dict[str, Any]:
    d1, h4, h1, m15, btc = bundle['eth_1d'], bundle['eth_4h'], bundle['eth_1h'], bundle['eth_15m'], bundle['btc_1h']
    if min(map(len, (d1, h4, h1, m15, btc))) < 40: raise RuntimeError('insufficient closed candles')
    legacy_regime = core.detect_regime(d1, h4, h1) if hasattr(core, 'detect_regime') else adaptive_v5.detect_regime(d1, h4, h1)
    extras = core._raw_derivatives(bundle); features = core.build_features(m15, h1, btc, legacy_regime, extras); champions = _load_registry(core, active_only=True); choices = []
    for item in champions:
        genome = item['genome']; metrics = item['metrics']; gate = list(metrics.get('gate_thresholds') or [])
        if not _live_gate(features, gate):
            choices.append({'strategy': item['strategy_id'], 'direction': genome['direction'], 'tradeable': False, 'reason': 'AI state gate not active', 'behavior_label': item['behavior_label']}); continue
        ood = _ood_fraction(features, metrics)
        if ood > LIVE_MAX_OOD_FRACTION:
            choices.append({'strategy': item['strategy_id'], 'direction': genome['direction'], 'tradeable': False, 'reason': f'out-of-distribution {ood:.0%}', 'behavior_label': item['behavior_label']}); continue
        try:
            model = pickle.loads(item['model_blob']); vec = np.asarray([[_finite(features.get(n), 0.0) for n in genome['feature_names']]], dtype=np.float32); pred_ev = float(model.predict(vec)[0])
        except Exception as exc:
            choices.append({'strategy': item['strategy_id'], 'direction': genome['direction'], 'tradeable': False, 'reason': f'model load/predict error: {type(exc).__name__}', 'behavior_label': item['behavior_label']}); continue
        threshold = max(float(metrics.get('direct_r_threshold') or 0.0), LIVE_MIN_PREDICTED_EV_R); quality = _clamp(float(metrics.get('profit_factor') or 1.0) / 2.0, .4, 1.2) * _clamp(float(metrics.get('stability') or .5), .3, 1.0); score = pred_ev * quality
        choices.append({'strategy': item['strategy_id'], 'direction': genome['direction'], 'tradeable': bool(pred_ev >= threshold), 'predicted_ev_r': pred_ev, 'probability': _clamp(.5 + pred_ev / 4.0, .01, .99), 'threshold': threshold, 'score': score, 'behavior_label': item['behavior_label'], 'genome': genome, 'metrics': metrics, 'ood_fraction': ood, 'reason': 'autonomous direct-R package active' if pred_ev >= threshold else 'predicted R below learned/live safety threshold'})
    choices.sort(key=lambda z: float(z.get('score') or -999.0), reverse=True); tradeable = [z for z in choices if z.get('tradeable')]
    selected = tradeable[0] if tradeable else choices[0] if choices else {'strategy': 'AUTONOMOUS_RESEARCH_PENDING', 'direction': 'NONE', 'tradeable': False, 'probability': 0.0, 'threshold': LIVE_MIN_PREDICTED_EV_R, 'score': 0.0, 'reason': 'no autonomous package has passed chronological OOS yet'}
    price = float(bundle['ticker'].get('last') or m15[-1]['c']); state_label = selected.get('behavior_label') or 'AI_STATE_UNCERTIFIED'
    return {'snapshot_ts': int(time.time()), 'price': price, 'regime': {'regime': 'AI_DISCOVERED_STATE', 'phase': state_label}, 'features': features, 'selection': selected, 'data_quality': bundle['quality'], 'derivatives': extras, 'trade_label': 'AUTONOMOUS LEARNED TRADE' if selected.get('tradeable') else 'WAIT / AUTONOMOUS RESEARCH', 'rule': 'no hand-authored strategy/regime template; complete package selected by direct expected-R model', 'autonomous_candidates': choices[:12]}


def _autonomous_create_signal(core: Any, analysis: dict[str, Any], m15: list[dict[str, Any]]) -> dict[str, Any] | None:
    sel = analysis['selection']
    if not sel.get('tradeable') or not sel.get('genome'): return None
    current = core.latest_signal()
    if current: return current
    genome = dict(sel['genome']); plan = _generic_plan_from_genome(float(analysis['price']), _finite(analysis['features'].get('atr_pct'), .001), genome); now = int(time.time()); signal_id = f"{now}-AUTO-{str(sel['strategy'])[-6:]}-{str(genome['direction'])[0]}"
    payload = {'initial_plan': plan, 'selection': sel, 'regime': analysis['regime'], 'features': analysis.get('features') or {}, 'data_quality': float((analysis.get('data_quality') or {}).get('score', 0.0)), 'created_from_snapshot': analysis.get('snapshot_ts'), 'immutable': True, 'autonomous_schema': SCHEMA, 'paper_notional_usdt': PAPER_NOTIONAL_USDT, 'leverage_mode': 'MAX_AVAILABLE_AT_ORDER_TIME', 'management': {'hit_targets': [], 'mfe_r': 0.0, 'mae_r': 0.0, **plan['management']}}
    con = core.db()
    try:
        con.execute('''INSERT INTO signals(signal_id,created_at,updated_at,status,strategy,direction,regime,phase,probability,entry,initial_stop,current_stop,targets,payload) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)''', (signal_id, now, now, 'PLANNED', str(sel['strategy']), str(genome['direction']), 'AI_DISCOVERED_STATE', str(sel.get('behavior_label') or 'AI_STATE'), float(sel.get('probability') or .5), float(plan['entry']), float(plan['stop']), float(plan['stop']), json.dumps(plan['targets']), json.dumps(payload, ensure_ascii=False, default=_json_default))); con.commit()
    finally:
        con.close()
    return core.latest_signal()


def _autonomous_update_signal(core: Any, bar: dict[str, Any]) -> dict[str, Any] | None:
    row = core.latest_signal()
    if not row: return None
    payload = row['payload']; targets = row['targets']; mgmt = payload.get('management') or {}; ts = int(bar['ts']); low = float(bar['l']); high = float(bar['h']); close = float(bar['c'])
    entry = float(row['entry']); stop0 = float(row['initial_stop']); current_stop = float(row['current_stop']); sign = 1.0 if row['direction'] == 'LONG' else -1.0; risk = abs(entry - stop0) or 1e-9
    if row['status'] == 'PLANNED':
        market_entry = bool(mgmt.get('entry_market')); touched = market_entry or (low <= entry <= high); expire_seconds = int(mgmt.get('expire_bars', 8)) * 900
        if not touched:
            if ts - int(row['created_at']) > expire_seconds:
                con = core.db(); con.execute("UPDATE signals SET status='EXPIRED',updated_at=? WHERE signal_id=?", (ts, row['signal_id'])); con.commit(); con.close()
            return core.latest_signal()
        con = core.db(); con.execute("UPDATE signals SET status='OPEN',filled_at=?,updated_at=? WHERE signal_id=?", (ts, ts, row['signal_id'])); con.commit(); con.close(); row = core.latest_signal(('OPEN',)) or row
    stop_hit = low <= current_stop if sign > 0 else high >= current_stop
    if stop_hit:
        core._close_signal(row, current_stop, 'AUTONOMOUS_STOP', ts); return core.latest_signal()
    hit = set(mgmt.get('hit_targets') or []); changed = False
    for k, target in enumerate(targets):
        if k in hit: continue
        px = float(target['price'])
        if (high >= px if sign > 0 else low <= px): hit.add(k); changed = True
    fav_r = (high - entry) / risk if sign > 0 else (entry - low) / risk
    if fav_r >= float(mgmt.get('breakeven_after_r', 999.0)):
        current_stop = max(current_stop, entry) if sign > 0 else min(current_stop, entry); changed = True
    if fav_r >= float(mgmt.get('trail_start_r', 999.0)):
        lock = entry + sign * float(mgmt.get('trail_lock_r', 0.0)) * risk; current_stop = max(current_stop, lock) if sign > 0 else min(current_stop, lock); changed = True
    max_hold_seconds = int(mgmt.get('max_hold_bars', 64)) * 900; filled_at = int(row.get('filled_at') or row['created_at'])
    if ts - filled_at >= max_hold_seconds:
        core._close_signal(row, close, 'AUTONOMOUS_TIME_EXIT', ts); return core.latest_signal()
    if changed:
        payload['management']['hit_targets'] = sorted(hit); con = core.db(); con.execute('UPDATE signals SET current_stop=?,updated_at=?,payload=? WHERE signal_id=?', (float(current_stop), ts, json.dumps(payload, ensure_ascii=False, default=_json_default), row['signal_id'])); con.commit(); con.close()
    return core.latest_signal()


def _patch_live_runtime(core: Any) -> None:
    if getattr(core, '_analysis_from_bundle', None) is not None and not hasattr(core, '_v30_original_analysis'): core._v30_original_analysis = core._analysis_from_bundle
    core._analysis_from_bundle = lambda bundle: _autonomous_analysis(core, bundle)
    core.create_signal = lambda analysis, m15: _autonomous_create_signal(core, analysis, m15)
    core.update_signal_with_bar = lambda bar: _autonomous_update_signal(core, bar)
    def fixed_sizing(entry: float, stop: float) -> dict[str, Any]:
        stop_pct = abs(float(entry) - float(stop)) / max(float(entry), 1e-9)
        return {'notional_usdt': PAPER_NOTIONAL_USDT, 'stop_pct': stop_pct, 'leverage_mode': 'MAX_AVAILABLE_AT_ORDER_TIME', 'paper_only': True}
    core._notional_for_risk = fixed_sizing


def autonomous_status(core: Any) -> dict[str, Any]:
    champions = _load_registry(core, active_only=True); audits = _load_audits(core); checkpoint = core.get_state(CHECKPOINT_KEY, {}); checkpoint = dict(checkpoint) if isinstance(checkpoint, dict) else {}; active = core.state.get('autonomous_live_progress') or {}
    gen = int(active.get('generation') or 0); cand = int(active.get('candidate') or 0); pop = max(1, int(active.get('population') or POPULATION)); evo_pct = 0.0
    if checkpoint.get('status') == 'COMPLETE': evo_pct = 100.0
    elif active.get('stage') == 'DIRECT_R_AUTONOMOUS_EVOLUTION': evo_pct = 100.0 * min(1.0, ((max(0, gen - 1)) + min(1.0, cand / pop)) / max(1, GENERATIONS))
    elif active.get('stage') == 'ONE_TIME_COMPLETE_PACKAGE_OOS': evo_pct = 100.0
    finalist_count = int(checkpoint.get('finalists') or max(len(audits), FINALISTS if active.get('stage') == 'ONE_TIME_COMPLETE_PACKAGE_OOS' else 0)); oos_pct = 100.0 if checkpoint.get('status') == 'COMPLETE' else (100.0 * len(audits) / max(1, finalist_count) if active.get('stage') == 'ONE_TIME_COMPLETE_PACKAGE_OOS' else 0.0); research_complete = checkpoint.get('status') == 'COMPLETE'
    research_best = []
    for x in audits:
        m = x['metrics']; research_best.append({'finalist_id': x['finalist_id'], 'status': x['status'], 'direction': x['genome'].get('direction'), 'pf': m.get('profit_factor'), 'ev_r': m.get('expectancy_r'), 'fills': m.get('oos_fills'), 'dd_r': m.get('max_drawdown_r'), 'reason': m.get('reason')})
    research_best.sort(key=lambda z: (_finite(z.get('ev_r'), -99), _finite(z.get('pf'), 0)), reverse=True)
    return {'runtime': VERSION, 'schema': SCHEMA, 'status': (core.state.get(STATE_KEY) or {}).get('status', 'NOT_STARTED'), 'research_start_ts': RESEARCH_START_TS, 'research_end_exclusive_ts': RESEARCH_END_EXCLUSIVE_TS, 'settlement_end_exclusive_ts': SETTLEMENT_END_EXCLUSIVE_TS, 'no_strategy_templates': True, 'no_manual_regime_templates': True, 'legacy_success_label_used': False, 'direct_trade_outcome_fitness': True, 'walk_forward_causal': True, 'future_path_after_plan_freeze_only': True, 'active': active, 'champions': [{'strategy_id': x['strategy_id'], 'direction': x['direction'], 'behavior_label': x['behavior_label'], **x['metrics']} for x in champions], 'research_best': research_best[:12], 'research_complete': research_complete, 'live_ready': bool(research_complete and champions), 'progress': {'evolution_percent': round(evo_pct, 2), 'oos_percent': round(oos_pct, 2), 'audited': len(audits), 'finalists': finalist_count, 'candidates_evaluated': int(active.get('candidate') or 0) + max(0, gen - 1) * pop}, 'paper_notional_usdt': PAPER_NOTIONAL_USDT, 'leverage_mode': 'MAX_AVAILABLE_AT_ORDER_TIME'}


def _pipeline_status(core: Any) -> dict[str, Any]:
    base = dict(_ORIGINAL_PIPELINE_STATUS(core) or {}) if _ORIGINAL_PIPELINE_STATUS else {}; replay = runtime_integrity.replay_progress(core); status = autonomous_status(core); progress = status.get('progress') or {}; evo_pct = float(progress.get('evolution_percent') or 0.0); oos_pct = float(progress.get('oos_percent') or 0.0); cert_pct = 100.0 if status.get('research_complete') else oos_pct
    stages = list(base.get('stages') or [])[:5]
    def st(name: str, pct: float, s: str, detail: dict[str, Any]) -> dict[str, Any]:
        try: return pipeline._stage(name, pct, s, detail)
        except Exception: return {'name': name, 'percent': pct, 'status': s, **detail}
    running = bool(replay.get('complete'))
    stages.extend([
        st('6. AUTONOMOUS_DIRECT_R_STRATEGY_DISCOVERY', evo_pct, 'RUNNING' if running and evo_pct < 100 else 'COMPLETE' if evo_pct >= 100 else 'WAITING', {'no_strategy_templates': True, 'no_manual_regime_templates': True, 'legacy_success_label_used': False, 'active': status.get('active') or {}, 'candidates_evaluated': progress.get('candidates_evaluated', 0)}),
        st('7. COMPLETE_PACKAGE_CHRONOLOGICAL_OOS', oos_pct, 'RUNNING' if evo_pct >= 99 and oos_pct < 100 else 'COMPLETE' if oos_pct >= 100 else 'WAITING', {'finalists': progress.get('finalists', 0), 'audited': progress.get('audited', 0), 'rule': 'frozen Signal+direction+state gate+Entry+SL+TP+management is tested as one package'}),
        st('8. AUTONOMOUS_PACKAGE_CERTIFICATION', cert_pct, 'COMPLETE' if status.get('research_complete') else 'WAITING', {'champions': len(status.get('champions') or []), 'research_best': status.get('research_best') or []}),
        st('9. CURRENT_LIVE_HANDOFF', 100.0 if status.get('live_ready') else 0.0, 'COMPLETE' if status.get('live_ready') else 'WAITING', {'paper_notional_usdt': PAPER_NOTIONAL_USDT, 'leverage_mode': 'MAX_AVAILABLE_AT_ORDER_TIME'}),
    ])
    weights = [8, 8, 8, 8, 23, 25, 12, 5, 3]; overall = sum(float(z.get('percent') or 0.0) * weights[i] for i, z in enumerate(stages[:len(weights)])) / sum(weights[:len(stages)])
    base.update({'overall_percent': round(overall, 2), 'stages': stages, 'active_stage': next((z['name'] for z in stages if float(z.get('percent') or 0) < 99.5), stages[-1]['name']), 'operational': bool(status.get('live_ready')), 'autonomous_strategy_discovery': status, 'joint_signal_then_execution_separation': False}); core.state['hierarchical_pipeline'] = base; return base


def _install_dashboard(production: Any) -> None:
    from fastapi.responses import HTMLResponse
    app = production.core.app; root = next((r for r in app.router.routes if getattr(r, 'path', None) == '/'), None)
    if root is None or getattr(root, 'name', '') == 'autonomous_v30_dashboard': return
    original = root.endpoint; app.router.routes = [r for r in app.router.routes if getattr(r, 'path', None) != '/']
    @app.get('/', response_class=HTMLResponse, name='autonomous_v30_dashboard')
    def dashboard() -> str:
        raw = original(); html = raw.body.decode() if hasattr(raw, 'body') else str(raw)
        card = r'''<section class="card" id="auto30card"><h2>🧬 Autonomous Strategy Discovery / 無模板策略研發</h2><div id="auto30status" class="notice y">讀取自主研發狀態…</div><div id="auto30bars"></div><div id="auto30active" class="notice"></div><details open><summary>AI 已生成 / 驗證的策略</summary><div id="auto30strategies"></div></details><details><summary>研究最佳但未認證</summary><pre id="auto30research">—</pre></details></section>'''
        marker = '</div><div class="footer">'; html = html.replace(marker, card + marker, 1) if marker in html else html.replace('</body>', card + '</body>')
        script = r'''<script id="autonomous-v30-js">async function auto30json(url){const r=await fetch(url,{cache:'no-store'});const text=await r.text();if(!r.ok)throw new Error(url+' HTTP '+r.status);try{return JSON.parse(text)}catch(e){throw new Error(url+' non-JSON response')}}function auto30esc(x){return String(x??'—').replace(/[&<>"']/g,s=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[s]))}function auto30bar(name,pct,state){pct=Number(pct||0);return `<div style="margin:12px 0"><div style="display:flex;justify-content:space-between;gap:12px"><b>${auto30esc(name)}</b><span>${pct.toFixed(2)}% · ${auto30esc(state)}</span></div><div style="height:10px;border:1px solid #29466d;border-radius:999px;overflow:hidden;margin-top:6px"><div style="height:100%;width:${Math.max(0,Math.min(100,pct))}%;background:linear-gradient(90deg,#53dec2,#62aaff,#a17bff)"></div></div></div>`}async function refreshAuto30(){try{const z=await auto30json('/api/v30/autonomous');const p=z.progress||{},el=document.getElementById('auto30status');if(el){el.className='notice '+(z.live_ready?'g':'y');el.innerHTML=`<b>${auto30esc(z.status)}</b><br>決策歷史：2020/1/1 → 2026/8/1｜策略模板：<b>無</b>｜人工 Regime：<b>無</b>｜核心 fitness：<b>直接交易 R</b><br>已認證完整策略：<b>${(z.champions||[]).length}</b>｜Paper 名目：<b>${Number(z.paper_notional_usdt||0).toLocaleString()} USDT</b>`}const b=document.getElementById('auto30bars');if(b)b.innerHTML=auto30bar('6. 自主完整策略進化',p.evolution_percent,z.research_complete?'COMPLETE':'RUNNING')+auto30bar('7. 完整 Package 歷史泛化檢查',p.oos_percent,z.research_complete?'COMPLETE':'WAITING/RUNNING')+auto30bar('8. 正式策略認證',z.research_complete?100:0,z.live_ready?'CERTIFIED':z.research_complete?'NO_CERTIFIED_PACKAGE':'WAITING')+auto30bar('9. Current Paper Handoff',z.live_ready?100:0,z.live_ready?'READY':'WAITING');const a=z.active||{},ae=document.getElementById('auto30active');if(ae)ae.innerHTML=`目前：<b>${auto30esc(a.stage||'WAITING')}</b>${a.generation?`｜Generation ${a.generation}/${a.generations}`:''}${a.candidate?`｜Candidate ${a.candidate}/${a.population}`:''}${a.direction?`｜${auto30esc(a.direction)}`:''}${a.max_hold_bars?`｜最大持有 ${(Number(a.max_hold_bars)*.25).toFixed(1)}h`:''}`;const s=document.getElementById('auto30strategies');if(s){const rows=z.champions||[];s.innerHTML=rows.length?rows.map((x,i)=>`<div class="notice g" style="margin:8px 0"><b>#${i+1} ${auto30esc(x.strategy_id)} · ${auto30esc(x.direction)}</b><br>${auto30esc(x.behavior_label)}<br>OOS PF <b>${Number(x.profit_factor||0).toFixed(2)}</b>｜EV <b>${Number(x.expectancy_r||0).toFixed(3)}R</b>｜fills ${Number(x.oos_fills||0)}｜DD ${Number(x.max_drawdown_r||0).toFixed(2)}R</div>`).join(''):'<div class="notice y">目前尚無通過完整歷史泛化檢查的策略；研究中的候選不會冒充可用策略。</div>'}const r=document.getElementById('auto30research');if(r)r.textContent=JSON.stringify(z.research_best||[],null,2)}catch(e){const el=document.getElementById('auto30status');if(el){el.className='notice r';el.textContent='Autonomous endpoint 暫時不可用：'+String(e)}}}refreshAuto30();setInterval(refreshAuto30,5000);</script>'''
        html = html.replace('</body>', script + '</body>') if '</body>' in html else html + script; return html


def install(production: Any, joint: Any | None = None, fixes: Any | None = None) -> None:
    global _INSTALLED, _ORIGINAL_PIPELINE_STATUS
    if _INSTALLED: return
    _INSTALLED = True; core = production.core; _ensure_tables(core)
    if not core.get_state(RESET_MARKER, None):
        _clear_autonomous_products(core); core.set_state(RESET_MARKER, {'schema': SCHEMA, 'at': int(time.time()), 'raw_market_preserved': True, 'raw_derivatives_preserved': True, 'replay_must_restart': True, 'old_strategy_taxonomy_not_used_for_research': True})
    final_system.certify_and_execute = autonomous_certify; operational_guard.certify_and_execute = autonomous_certify; cert17.train_v17 = autonomous_certify; v5_runtime.train_v5 = autonomous_certify; core.train_if_due = lambda force=False: autonomous_certify(core, force); _patch_live_runtime(core)
    core.state['autonomous_research_contract'] = {'schema': SCHEMA, 'research_start_ts': RESEARCH_START_TS, 'research_end_exclusive_ts': RESEARCH_END_EXCLUSIVE_TS, 'settlement_end_exclusive_ts': SETTLEMENT_END_EXCLUSIVE_TS, 'raw_cache_reused': True, 'replay_derived_reset_required': True, 'legacy_strategy_family_used': False, 'legacy_success_label_used': False, 'manual_regime_gate_used': False, 'manual_phase_gate_used': False, 'direct_trade_outcome_fitness': True, 'strategy_controls': ['direction','feature subset','AI state gate','decision cadence','entry','stop','targets','allocations','expiry','holding horizon','breakeven','trailing','cooldown','model complexity'], 'holding_horizon_range': '1h..7d by default, evolved rather than hard-coded short-term', 'future_data_rule': 'forbidden before decision/plan freeze; sequential 5m settlement only afterwards', 'paper_notional_usdt': PAPER_NOTIONAL_USDT, 'leverage_mode': 'MAX_AVAILABLE_AT_ORDER_TIME'}
    if _ORIGINAL_PIPELINE_STATUS is None: _ORIGINAL_PIPELINE_STATUS = pipeline.pipeline_status
    pipeline.pipeline_status = lambda c: _pipeline_status(c); _install_dashboard(production)
    if not any(getattr(r, 'path', None) == '/api/v30/autonomous' for r in core.app.router.routes):
        @core.app.get('/api/v30/autonomous')
        def autonomous_api() -> dict[str, Any]: return autonomous_status(core)
    if not any(getattr(r, 'path', None) == '/api/v30/storage-lite' for r in core.app.router.routes):
        @core.app.get('/api/v30/storage-lite')
        def storage_lite() -> dict[str, Any]:
            path = str(getattr(core, 'DB_PATH', os.getenv('DATABASE_PATH', '/data/eth_adaptive.db'))); exists = os.path.exists(path); size = os.path.getsize(path) if exists else 0
            return {'ok': exists, 'path': path, 'bytes': size, 'persistent_expected': path.startswith('/data/'), 'schema': SCHEMA}
    runtime_identity.stamp(core)
