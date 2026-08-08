from __future__ import annotations

import bisect
import json
import math
import os
import sqlite3
import statistics
import time
from typing import Any

import numpy as np

import adaptive_v5 as signal

f = signal.f
clamp = signal.clamp
mean = signal.mean
atr = signal.atr
ema = signal.ema
pivots = signal.pivots

EXECUTION_SCHEMA = 2
ALL_IN_COST_BPS = float(os.getenv('EXECUTION_ALL_IN_COST_BPS', '8.0'))
MIN_STOP_PCT = float(os.getenv('EXECUTION_MIN_STOP_PCT', '0.0020'))
MAX_HOLD_BARS = max(12, int(os.getenv('EXECUTION_MAX_HOLD_BARS', '32')))
MIN_AUDIT_FILLS = max(40, int(os.getenv('EXECUTION_MIN_AUDIT_FILLS', '50')))


class ExecutionStore:
    def __init__(self, con: sqlite3.Connection) -> None:
        self.con = con
        con.execute('''CREATE TABLE IF NOT EXISTS execution_registry_v7(
            strategy TEXT NOT NULL,
            direction TEXT NOT NULL,
            model_version INTEGER NOT NULL,
            version INTEGER NOT NULL,
            status TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            metrics TEXT NOT NULL,
            policy TEXT NOT NULL,
            PRIMARY KEY(strategy,direction,model_version,version)
        )''')
        con.execute('CREATE INDEX IF NOT EXISTS ix_execution_v7_lookup ON execution_registry_v7(strategy,direction,model_version,status)')
        con.commit()

    def champion(self, strategy: str, direction: str, model_version: int) -> tuple[dict[str, Any] | None, dict[str, Any]]:
        row = self.con.execute(
            "SELECT policy,metrics,version FROM execution_registry_v7 WHERE strategy=? AND direction=? AND model_version=? AND status='CHAMPION' ORDER BY version DESC LIMIT 1",
            (strategy, direction, int(model_version)),
        ).fetchone()
        if not row:
            return None, {}
        return json.loads(row[0]), {**json.loads(row[1]), 'execution_version': int(row[2])}

    def save(self, strategy: str, direction: str, model_version: int, policy: dict[str, Any], metrics: dict[str, Any], promote: bool) -> int:
        row = self.con.execute(
            'SELECT MAX(version) FROM execution_registry_v7 WHERE strategy=? AND direction=? AND model_version=?',
            (strategy, direction, int(model_version)),
        ).fetchone()
        version = int(row[0] or 0) + 1
        if promote:
            self.con.execute(
                "UPDATE execution_registry_v7 SET status='ARCHIVED' WHERE strategy=? AND direction=? AND model_version=? AND status='CHAMPION'",
                (strategy, direction, int(model_version)),
            )
        self.con.execute(
            'INSERT INTO execution_registry_v7(strategy,direction,model_version,version,status,created_at,metrics,policy) VALUES(?,?,?,?,?,?,?,?)',
            (strategy, direction, int(model_version), version, 'CHAMPION' if promote else 'REJECTED', int(time.time()), json.dumps(metrics, ensure_ascii=False), json.dumps(policy, ensure_ascii=False)),
        )
        self.con.commit()
        return version


def _base_entry_factor(strategy: str) -> float:
    return {
        'TREND_PULLBACK': .11,
        'LIQUIDITY_SWEEP_REVERSAL': .08,
        'SQUEEZE_EXPANSION': .055,
        'BREAKOUT_RETEST': .06,
        'RANGE_MEAN_REVERSION': .10,
        'MOMENTUM_CONTINUATION': .04,
        'FAILED_BREAKOUT_REVERSAL': .075,
    }.get(strategy, .07)


TARGET_PROFILES = (
    (.75, 1.25, 1.90, 2.80),
    (.90, 1.45, 2.10, 3.10),
    (1.00, 1.55, 2.20, 3.20),
    (.85, 1.35, 2.00, 3.60),
)
ALLOCATIONS = (
    (20, 30, 30, 20),
    (25, 30, 25, 20),
    (30, 30, 25, 15),
)


def policy_candidates(strategy: str) -> list[dict[str, Any]]:
    base = _base_entry_factor(strategy)
    out: list[dict[str, Any]] = []
    for em in (.70, 1.0, 1.35):
        for sa in (.80, 1.00, 1.25, 1.50, 1.80, 2.20):
            for structure_mode in ('15m', '30m', '1h', 'balanced'):
                for rr in TARGET_PROFILES:
                    out.append({
                        'schema': EXECUTION_SCHEMA,
                        'entry_atr': round(base * em, 4),
                        'stop_atr': sa,
                        'structure_mode': structure_mode,
                        'target_rr': list(rr),
                        'allocations': [20, 30, 30, 20],
                        'lock_after_tp1_r': 0.0,
                        'lock_after_tp2_r': 0.55,
                        'lock_after_tp3_r': 1.05,
                        'expire_bars': 8 if strategy in ('BREAKOUT_RETEST', 'TREND_PULLBACK') else 6,
                        'max_hold_bars': MAX_HOLD_BARS,
                        'all_in_cost_bps': ALL_IN_COST_BPS,
                        'min_stop_pct': MIN_STOP_PCT,
                    })
    return out


def _nearest_structure(rows: list[dict[str, Any]] | None, entry: float, direction: str, radius: int, buffer: float) -> float | None:
    if not rows or len(rows) < radius * 2 + 5:
        return None
    hi, lo = pivots(rows[-160:], radius)
    if direction == 'LONG':
        xs = [x for _, x in lo if x < entry]
        return (max(xs) - buffer) if xs else None
    xs = [x for _, x in hi if x > entry]
    return (min(xs) + buffer) if xs else None


def _choose_structure(entry: float, direction: str, mode: str, levels: dict[str, float | None]) -> tuple[float | None, str]:
    available = [(tf, px) for tf, px in levels.items() if px is not None]
    if not available:
        return None, 'none'
    if mode in levels and levels.get(mode) is not None:
        return levels[mode], mode
    distances = sorted((abs(entry - float(px)), tf, float(px)) for tf, px in available)
    pick = distances[min(len(distances) - 1, max(0, len(distances) // 2))]
    return pick[2], pick[1]


def plan_from_policy(strategy: str, direction: str, live: float, m15: list[dict[str, Any]], policy: dict[str, Any], m30: list[dict[str, Any]] | None = None, h1: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    a15 = max(atr(m15), live * .001)
    a30 = max(atr(m30 or m15), live * .001)
    a1h = max(atr(h1 or m15), live * .001)
    e20 = ema([f(x['c']) for x in m15], 20)
    sign = 1 if direction == 'LONG' else -1
    off = max(.025, f(policy.get('entry_atr'), _base_entry_factor(strategy))) * a15
    if strategy in ('TREND_PULLBACK', 'RANGE_MEAN_REVERSION'):
        entry = min(live - off, e20) if direction == 'LONG' else max(live + off, e20)
    elif strategy == 'BREAKOUT_RETEST':
        w = m15[-28:-1] if len(m15) >= 29 else m15[:-1]
        ph = max((f(x['h']) for x in w), default=live)
        pl = min((f(x['l']) for x in w), default=live)
        entry = ph if direction == 'LONG' and ph < live else pl if direction == 'SHORT' and pl > live else live - sign * off
    else:
        entry = live - sign * off
    entry = min(entry, live - .02 * a15) if direction == 'LONG' else max(entry, live + .02 * a15)
    min_dist = max(f(policy.get('stop_atr'), 1.0) * a15, entry * f(policy.get('min_stop_pct'), MIN_STOP_PCT))
    levels = {
        '15m': _nearest_structure(m15, entry, direction, 2, .08 * a15),
        '30m': _nearest_structure(m30, entry, direction, 2, .10 * a30),
        '1h': _nearest_structure(h1, entry, direction, 2, .12 * a1h),
    }
    structural, used_tf = _choose_structure(entry, direction, str(policy.get('structure_mode') or 'balanced'), levels)
    if direction == 'LONG':
        stop = min(float(structural) if structural is not None else entry - min_dist, entry - min_dist)
    else:
        stop = max(float(structural) if structural is not None else entry + min_dist, entry + min_dist)
    risk = abs(entry - stop)
    rrs = [float(x) for x in policy['target_rr']]
    alloc = [int(x) for x in policy['allocations']]
    targets = [{'price': round(entry + sign * risk * rr, 2), 'rr': round(rr, 2), 'allocation': al} for rr, al in zip(rrs, alloc)]
    return {
        'entry': round(entry, 2), 'stop': round(stop, 2), 'risk': round(risk, 6), 'stop_pct': risk / max(entry, 1e-9), 'targets': targets,
        'profile': {'mode': 'POINT_IN_TIME_OOS_EXECUTION_CHAMPION', **policy, 'structure_used': used_tf, 'structure_levels': {k: (round(v, 2) if v is not None else None) for k, v in levels.items()}},
        'management': {'move_to_be_after_tp1': True, 'lock_after_tp2_r': f(policy.get('lock_after_tp2_r'), .55), 'lock_after_tp3_r': f(policy.get('lock_after_tp3_r'), 1.05), 'never_widen_stop': True, 'initial_plan_immutable': True},
    }


def _slice_to(rows: list[dict[str, Any]], timestamps: list[int], ts: int, max_bars: int) -> list[dict[str, Any]]:
    idx = bisect.bisect_right(timestamps, ts)
    return rows[max(0, idx - max_bars):idx]


def simulate_policy(data: dict[str, Any], opp: dict[str, Any], strategy: str, direction: str, policy: dict[str, Any]) -> dict[str, Any]:
    bars = data['m15']
    i = data['index15'].get(int(opp['ts']))
    if i is None or i < 100:
        return {'filled': False, 'pnl_r': 0.0}
    past15 = bars[max(0, i - 500):i + 1]
    past30 = _slice_to(data['m30'], data['ts30'], int(opp['ts']), 300)
    past1h = _slice_to(data['h1'], data['ts1h'], int(opp['ts']), 300)
    live = f(bars[i]['c'])
    plan = plan_from_policy(strategy, direction, live, past15, policy, past30, past1h)
    entry, stop0 = f(plan['entry']), f(plan['stop'])
    risk = abs(entry - stop0)
    if risk <= 1e-9:
        return {'filled': False, 'pnl_r': 0.0}
    sign = 1 if direction == 'LONG' else -1
    future = bars[i + 1:i + 1 + int(policy.get('max_hold_bars', MAX_HOLD_BARS))]
    expire = min(len(future), int(policy.get('expire_bars', 6)))
    fill_idx = next((j for j, b in enumerate(future[:expire]) if f(b['l']) <= entry <= f(b['h'])), None)
    if fill_idx is None:
        return {'filled': False, 'pnl_r': 0.0, 'entry': entry, 'stop': stop0}
    remaining, realized, current_stop = 1.0, 0.0, stop0
    hit: set[int] = set()
    mfe = mae = 0.0
    exit_reason, last = 'TIMEOUT', entry
    for b in future[fill_idx:]:
        low, high, close = f(b['l']), f(b['h']), f(b['c'])
        last = close
        favorable = (high - entry) / risk if direction == 'LONG' else (entry - low) / risk
        adverse = (entry - low) / risk if direction == 'LONG' else (high - entry) / risk
        mfe, mae = max(mfe, favorable), max(mae, adverse)
        stop_hit = low <= current_stop if direction == 'LONG' else high >= current_stop
        if stop_hit:
            exit_rr = (current_stop - entry) * sign / risk
            realized += remaining * exit_rr
            remaining = 0.0
            exit_reason = 'STOP_OR_TRAIL'
            break
        for idx, target in enumerate(plan['targets']):
            if idx in hit:
                continue
            px = f(target['price'])
            target_hit = high >= px if direction == 'LONG' else low <= px
            if not target_hit:
                continue
            frac = min(remaining, f(target['allocation']) / 100.0)
            realized += frac * f(target['rr'])
            remaining -= frac
            hit.add(idx)
        if 0 in hit:
            current_stop = max(current_stop, entry) if direction == 'LONG' else min(current_stop, entry)
        if 1 in hit:
            locked = entry + sign * f(policy.get('lock_after_tp2_r'), .55) * risk
            current_stop = max(current_stop, locked) if direction == 'LONG' else min(current_stop, locked)
        if 2 in hit:
            locked = entry + sign * f(policy.get('lock_after_tp3_r'), 1.05) * risk
            current_stop = max(current_stop, locked) if direction == 'LONG' else min(current_stop, locked)
        if remaining <= 1e-9:
            exit_reason = 'ALL_TARGETS'
            break
    if remaining > 1e-9:
        exit_rr = (last - entry) * sign / risk
        realized += remaining * exit_rr
    cost_r = (f(policy.get('all_in_cost_bps'), ALL_IN_COST_BPS) / 10000.0) * entry / risk
    net = realized - cost_r
    return {'filled': True, 'pnl_r': net, 'gross_r': realized, 'cost_r': cost_r, 'mfe_r': mfe, 'mae_r': mae, 'exit_reason': exit_reason, 'entry': entry, 'stop': stop0, 'stop_pct': risk / max(entry, 1e-9), 'hit_targets': sorted(hit), 'regime': opp.get('regime')}


def _stats(results: list[dict[str, Any]]) -> dict[str, float]:
    filled = [x for x in results if x.get('filled')]
    p = [f(x.get('pnl_r')) for x in filled]
    gains = sum(max(x, 0) for x in p)
    losses = sum(max(-x, 0) for x in p)
    eq = peak = dd = 0.0
    for x in p:
        eq += x; peak = max(peak, eq); dd = max(dd, peak - eq)
    return {'opportunities': len(results), 'fills': len(filled), 'fill_rate': len(filled) / max(len(results), 1), 'profit_factor': gains / max(losses, 1e-9), 'expectancy_r': mean(p), 'win_rate': mean(1.0 if x > 0 else 0.0 for x in p), 'max_drawdown_r': dd, 'avg_cost_r': mean(f(x.get('cost_r')) for x in filled), 'avg_stop_pct': mean(f(x.get('stop_pct')) for x in filled)}


def _utility(stats: dict[str, float], worst_segment_ev: float = 0.0) -> float:
    if stats['fills'] < 28 or stats['fill_rate'] < .12:
        return -999.0
    pf = max(stats['profit_factor'], 1e-6)
    return stats['expectancy_r'] * 3.0 + math.log(pf) * .24 - stats['max_drawdown_r'] * .008 + min(stats['fills'], 180) / 180 * .06 + min(worst_segment_ev, 0.0) * 2.0


def _block_bootstrap_ev(pnls: list[float], seed: int = 71, reps: int = 300, block: int = 8) -> tuple[float, float]:
    if len(pnls) < 20:
        return -9.0, 9.0
    rng = np.random.default_rng(seed)
    arr = np.array(pnls, dtype=float)
    n = len(arr)
    vals = []
    max_start = max(1, n - block + 1)
    for _ in range(reps):
        sample = []
        while len(sample) < n:
            s = int(rng.integers(0, max_start)); sample.extend(arr[s:s + block].tolist())
        vals.append(float(np.mean(sample[:n])))
    return float(np.quantile(vals, .05)), float(np.quantile(vals, .95))


def _signal_oof_opportunities(core: Any, strategy: str, direction: str) -> list[dict[str, Any]]:
    con = core.db(); store = signal.ModelStore(con); rows = [x for x in store.samples(strategy, direction=direction) if x['source_quality'] >= 55]; con.close()
    min_train, min_test, purge = 300, 120, 32
    if len(rows) < min_train + min_test + 80:
        return []
    n = len(rows); first = max(min_train + purge, int(n * .50)); remain = n - first
    folds = 4 if remain >= 4 * min_test else 3 if remain >= 3 * min_test else 2
    out: list[dict[str, Any]] = []
    for fold in range(folds):
        ts = first + fold * max(min_test, remain // folds); te = n if fold == folds - 1 else min(n, ts + max(min_test, remain // folds))
        train = rows[:max(0, ts - purge)]; test = rows[ts:te]
        if len(train) < min_train or len(test) < 60:
            continue
        cn = max(80, int(len(train) * .2)); fe = len(train) - cn - purge
        if fe < 220:
            continue
        fit = train[:fe]; cal = train[fe + purge:]
        yf = np.array([r['success'] for r in fit]); yc = np.array([r['success'] for r in cal]); yt = np.array([r['success'] for r in test])
        if min(len(set(yf)), len(set(yc)), len(set(yt))) < 2:
            continue
        mi = signal.Learner._model(7100 + fold)
        mi.fit(np.vstack([signal._vec(r['features']) for r in fit]), yf, sample_weight=signal._weights(fit, int(fit[-1]['ts'])))
        cp = mi.predict_proba(np.vstack([signal._vec(r['features']) for r in cal]))[:, 1]
        th, _ = signal._threshold(cal, cp, .035)
        mo = signal.Learner._model(7200 + fold); yo = np.array([r['success'] for r in train])
        mo.fit(np.vstack([signal._vec(r['features']) for r in train]), yo, sample_weight=signal._weights(train, int(train[-1]['ts'])))
        probs = mo.predict_proba(np.vstack([signal._vec(r['features']) for r in test]))[:, 1]
        for r, p in zip(test, probs):
            if float(p) >= th:
                out.append({'ts': int(r['ts']), 'regime': str(r['regime']), 'probability': float(p), 'threshold': float(th), 'fold': fold})
    return out


def _market_data(core: Any) -> dict[str, Any]:
    src15 = core._best_source('ETH', '15m'); src30 = core._best_source('ETH', '30m'); src1h = core._best_source('ETH', '1h')
    if not (src15 and src30 and src1h):
        return {}
    m15 = core.load_bars('ETH', '15m', src15); m30 = core.load_bars('ETH', '30m', src30); h1 = core.load_bars('ETH', '1h', src1h)
    return {'m15': m15, 'm30': m30, 'h1': h1, 'index15': {int(x['ts']): i for i, x in enumerate(m15)}, 'ts30': [int(x['ts']) for x in m30], 'ts1h': [int(x['ts']) for x in h1]}


def _segment_worst(data: dict[str, Any], opps: list[dict[str, Any]], strategy: str, direction: str, policy: dict[str, Any]) -> tuple[float, float]:
    if not opps:
        return -9.0, 0.0
    size = max(1, len(opps) // 3); evs = []
    for s in range(3):
        seg = opps[s * size: len(opps) if s == 2 else min(len(opps), (s + 1) * size)]
        st = _stats([simulate_policy(data, x, strategy, direction, policy) for x in seg])
        evs.append(st['expectancy_r'] if st['fills'] >= 10 else -1.0)
    return min(evs), sum(x > 0 for x in evs) / len(evs)


def optimize_pair(core: Any, strategy: str, direction: str, force: bool = False) -> dict[str, Any] | None:
    con = core.db(); signal_store = signal.ModelStore(con); model, signal_meta = signal_store.champion(strategy, direction)
    if model is None:
        con.close(); return None
    model_version = int(signal_meta.get('version') or 0); exec_store = ExecutionStore(con); existing, existing_meta = exec_store.champion(strategy, direction, model_version)
    if existing is not None and not force:
        con.close(); return {'strategy': strategy, 'direction': direction, 'model_version': model_version, 'status': 'UNCHANGED', **existing_meta}
    con.close()
    opps = _signal_oof_opportunities(core, strategy, direction); data = _market_data(core)
    if not data:
        return {'strategy': strategy, 'direction': direction, 'model_version': model_version, 'status': 'NO_MARKET_DATA'}
    opps = [x for x in opps if x['ts'] in data['index15'] and data['index15'][x['ts']] >= 100 and data['index15'][x['ts']] + MAX_HOLD_BARS + 2 < len(data['m15'])]
    if len(opps) < 260:
        return {'strategy': strategy, 'direction': direction, 'model_version': model_version, 'status': 'INSUFFICIENT_POINT_IN_TIME_OOS', 'opportunities': len(opps)}
    n = len(opps); purge = 12; dev_end = max(140, int(n * .56)); val_start = min(n, dev_end + purge); val_end = max(val_start + 45, int(n * .78)); audit_start = min(n, val_end + purge)
    dev, val, audit = opps[:dev_end], opps[val_start:val_end], opps[audit_start:]
    if len(val) < 45 or len(audit) < 55:
        return {'strategy': strategy, 'direction': direction, 'model_version': model_version, 'status': 'INSUFFICIENT_SPLITS', 'opportunities': n, 'validation': len(val), 'audit': len(audit)}
    ranked: list[tuple[float, dict[str, Any], dict[str, Any]]] = []
    for policy in policy_candidates(strategy):
        results = [simulate_policy(data, x, strategy, direction, policy) for x in dev]; st = _stats(results); worst, profitable = _segment_worst(data, dev, strategy, direction, policy); score = _utility(st, worst)
        ranked.append((score, policy, {**st, 'worst_segment_ev_r': worst, 'profitable_segment_ratio': profitable}))
    ranked.sort(key=lambda x: x[0], reverse=True); top = ranked[:8]
    val_ranked: list[tuple[float, dict[str, Any], dict[str, Any], dict[str, Any]]] = []
    for _, base_policy, dev_meta in top:
        for allocation in ALLOCATIONS:
            p = {**base_policy, 'allocations': list(allocation)}; vr = [simulate_policy(data, x, strategy, direction, p) for x in val]; vs = _stats(vr); score = _utility(vs, dev_meta.get('worst_segment_ev_r', -9.0)); val_ranked.append((score, p, dev_meta, vs))
    val_ranked.sort(key=lambda x: x[0], reverse=True); _, policy, dev_meta, val_meta = val_ranked[0]
    audit_results = [simulate_policy(data, x, strategy, direction, policy) for x in audit]; audit_meta = _stats(audit_results); pnls = [f(x['pnl_r']) for x in audit_results if x.get('filled')]
    ci_low, ci_high = _block_bootstrap_ev(pnls); shrunk_ev = audit_meta['expectancy_r'] * audit_meta['fills'] / max(audit_meta['fills'] + 80, 1)
    suspicious = bool((audit_meta['profit_factor'] > 5.0 and audit_meta['fills'] < 150) or (audit_meta['expectancy_r'] > .70 and audit_meta['fills'] < 150) or audit_meta['win_rate'] > .82)
    regime_metrics: dict[str, Any] = {}; blocked_regimes: list[str] = []
    for rg in sorted({x['regime'] for x in audit}):
        rr = [simulate_policy(data, x, strategy, direction, policy) for x in audit if x['regime'] == rg]; rs = _stats(rr); regime_metrics[rg] = rs
        if rs['fills'] >= 12 and (rs['expectancy_r'] <= 0 or rs['profit_factor'] < 1.0):
            blocked_regimes.append(rg)
    core_ok = bool(audit_meta['fills'] >= MIN_AUDIT_FILLS and audit_meta['profit_factor'] >= 1.20 and audit_meta['expectancy_r'] >= .08 and shrunk_ev >= .04 and ci_low > 0.0 and audit_meta['max_drawdown_r'] <= 10.0 and .15 <= audit_meta['fill_rate'] <= .95 and val_meta['fills'] >= 28 and val_meta['profit_factor'] >= 1.05 and val_meta['expectancy_r'] > .02 and dev_meta.get('worst_segment_ev_r', -9) >= -.08 and dev_meta.get('profitable_segment_ratio', 0) >= .66 and audit_meta['avg_stop_pct'] >= MIN_STOP_PCT * .95 and not suspicious)
    metrics = {
        'schema': EXECUTION_SCHEMA, 'validation_method': 'POINT_IN_TIME_SIGNAL_OOF -> DEV_TUNE -> CHRONO_VALIDATION -> UNTOUCHED_AUDIT', 'strategy': strategy, 'direction': direction, 'model_version': model_version, 'certified': core_ok, 'signal_oof_opportunities': n,
        'development': dev_meta, 'validation': val_meta, 'audit': audit_meta, 'profit_factor': audit_meta['profit_factor'], 'expectancy_r': audit_meta['expectancy_r'], 'win_rate': audit_meta['win_rate'], 'max_drawdown_r': audit_meta['max_drawdown_r'], 'fill_rate': audit_meta['fill_rate'], 'oos_fills': audit_meta['fills'], 'oos_opportunities': audit_meta['opportunities'], 'ev_bootstrap_05': ci_low, 'ev_bootstrap_95': ci_high, 'shrunk_ev_r': shrunk_ev, 'suspicious_metrics': suspicious, 'blocked_regimes': blocked_regimes, 'regime_metrics': regime_metrics, 'estimated_all_in_cost_bps': ALL_IN_COST_BPS,
        'reason': 'point-in-time signal OOF + exact execution audit passed' if core_ok else f"rejected v7 execution: PF={audit_meta['profit_factor']:.2f}, EV={audit_meta['expectancy_r']:.3f}R, CI05={ci_low:.3f}R, fills={audit_meta['fills']}, fill={audit_meta['fill_rate']:.0%}, DD={audit_meta['max_drawdown_r']:.1f}R, valEV={val_meta['expectancy_r']:.3f}R, suspicious={suspicious}",
    }
    con = core.db(); store = ExecutionStore(con); version = store.save(strategy, direction, model_version, policy, metrics, core_ok); con.close()
    return {'strategy': strategy, 'direction': direction, 'model_version': model_version, 'execution_version': version, 'status': 'CHAMPION' if core_ok else 'REJECTED', **metrics}


def optimize_all(core: Any, force: bool = False) -> list[dict[str, Any]]:
    out = []
    for strategy in signal.STRATEGIES:
        for direction in signal.DIRECTIONS:
            item = optimize_pair(core, strategy, direction, force=force)
            if item:
                out.append(item)
    return out


def execution_for_candidate(core: Any, candidate: dict[str, Any]) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    model_meta = (candidate.get('model') or {}).get('metrics') or {}
    model_version = int((candidate.get('model') or {}).get('model_version') or model_meta.get('version') or 0)
    if model_version <= 0:
        return None, {}
    con = core.db(); store = ExecutionStore(con); policy, meta = store.champion(str(candidate['strategy']), str(candidate['direction']), model_version); con.close(); return policy, meta
