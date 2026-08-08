from __future__ import annotations

import json
import math
import os
import pickle
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

EXECUTION_SCHEMA = 1
ALL_IN_COST_BPS = float(os.getenv('EXECUTION_ALL_IN_COST_BPS', '8.0'))
MIN_STOP_PCT = float(os.getenv('EXECUTION_MIN_STOP_PCT', '0.0020'))
MAX_HOLD_BARS = max(12, int(os.getenv('EXECUTION_MAX_HOLD_BARS', '32')))


class ExecutionStore:
    def __init__(self, con: sqlite3.Connection) -> None:
        self.con = con
        con.execute('''CREATE TABLE IF NOT EXISTS execution_registry(
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
        con.execute('CREATE INDEX IF NOT EXISTS ix_execution_registry_lookup ON execution_registry(strategy,direction,model_version,status)')
        con.commit()

    def champion(self, strategy: str, direction: str, model_version: int) -> tuple[dict[str, Any] | None, dict[str, Any]]:
        row = self.con.execute(
            "SELECT policy,metrics,version FROM execution_registry WHERE strategy=? AND direction=? AND model_version=? AND status='CHAMPION' ORDER BY version DESC LIMIT 1",
            (strategy, direction, int(model_version)),
        ).fetchone()
        if not row:
            return None, {}
        return json.loads(row[0]), {**json.loads(row[1]), 'execution_version': int(row[2])}

    def save(self, strategy: str, direction: str, model_version: int, policy: dict[str, Any], metrics: dict[str, Any], promote: bool) -> int:
        row = self.con.execute(
            'SELECT MAX(version) FROM execution_registry WHERE strategy=? AND direction=? AND model_version=?',
            (strategy, direction, int(model_version)),
        ).fetchone()
        version = int(row[0] or 0) + 1
        if promote:
            self.con.execute(
                "UPDATE execution_registry SET status='ARCHIVED' WHERE strategy=? AND direction=? AND model_version=? AND status='CHAMPION'",
                (strategy, direction, int(model_version)),
            )
        self.con.execute(
            'INSERT INTO execution_registry(strategy,direction,model_version,version,status,created_at,metrics,policy) VALUES(?,?,?,?,?,?,?,?)',
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


def policy_candidates(strategy: str) -> list[dict[str, Any]]:
    base = _base_entry_factor(strategy)
    entry_mult = (.70, 1.0, 1.35)
    stop_atr = (.80, 1.05, 1.30)
    profiles = (
        (.75, 1.25, 1.90, 2.80),
        (.90, 1.45, 2.10, 3.10),
        (1.00, 1.55, 2.20, 3.20),
        (.80, 1.20, 1.70, 2.50),
    )
    allocations = (
        (25, 30, 25, 20),
        (20, 30, 30, 20),
        (30, 30, 25, 15),
    )
    out: list[dict[str, Any]] = []
    # Keep the search broad enough to learn execution, but bounded enough for Zeabur.
    for em in entry_mult:
        for sa in stop_atr:
            for rr in profiles:
                # Allocation is optimized in a second dimension without exploding the grid.
                for alloc in allocations:
                    out.append({
                        'schema': EXECUTION_SCHEMA,
                        'entry_atr': round(base * em, 4),
                        'stop_atr': sa,
                        'target_rr': list(rr),
                        'allocations': list(alloc),
                        'lock_after_tp1_r': 0.0,
                        'lock_after_tp2_r': 0.55,
                        'lock_after_tp3_r': 1.05,
                        'expire_bars': 8 if strategy in ('BREAKOUT_RETEST', 'TREND_PULLBACK') else 6,
                        'max_hold_bars': MAX_HOLD_BARS,
                        'all_in_cost_bps': ALL_IN_COST_BPS,
                        'min_stop_pct': MIN_STOP_PCT,
                    })
    return out


def plan_from_policy(strategy: str, direction: str, live: float, past: list[dict[str, Any]], policy: dict[str, Any]) -> dict[str, Any]:
    a = max(atr(past), live * .001)
    e20 = ema([f(x['c']) for x in past], 20)
    sign = 1 if direction == 'LONG' else -1
    off = max(.025, f(policy.get('entry_atr'), _base_entry_factor(strategy))) * a
    if strategy in ('TREND_PULLBACK', 'RANGE_MEAN_REVERSION'):
        entry = min(live - off, e20) if direction == 'LONG' else max(live + off, e20)
    elif strategy == 'BREAKOUT_RETEST':
        w = past[-28:-1] if len(past) >= 29 else past[:-1]
        ph = max((f(x['h']) for x in w), default=live)
        pl = min((f(x['l']) for x in w), default=live)
        entry = ph if direction == 'LONG' and ph < live else pl if direction == 'SHORT' and pl > live else live - sign * off
    else:
        entry = live - sign * off
    entry = min(entry, live - .02 * a) if direction == 'LONG' else max(entry, live + .02 * a)

    min_dist = max(f(policy.get('stop_atr'), 1.0) * a, entry * f(policy.get('min_stop_pct'), MIN_STOP_PCT))
    hi, lo = pivots(past[-100:], 2)
    if direction == 'LONG':
        lows = [x for _, x in lo if x < entry]
        structural = (max(lows) - .08 * a) if lows else entry - min_dist
        stop = min(structural, entry - min_dist)
    else:
        highs = [x for _, x in hi if x > entry]
        structural = (min(highs) + .08 * a) if highs else entry + min_dist
        stop = max(structural, entry + min_dist)
    risk = abs(entry - stop)
    rrs = [float(x) for x in policy['target_rr']]
    alloc = [int(x) for x in policy['allocations']]
    targets = [
        {'price': round(entry + sign * risk * rr, 2), 'rr': round(rr, 2), 'allocation': al}
        for rr, al in zip(rrs, alloc)
    ]
    return {
        'entry': round(entry, 2),
        'stop': round(stop, 2),
        'risk': round(risk, 6),
        'targets': targets,
        'profile': {'mode': 'OOS_EXECUTION_CHAMPION', **policy},
        'management': {
            'move_to_be_after_tp1': True,
            'lock_after_tp2_r': f(policy.get('lock_after_tp2_r'), .55),
            'lock_after_tp3_r': f(policy.get('lock_after_tp3_r'), 1.05),
            'never_widen_stop': True,
            'initial_plan_immutable': True,
        },
    }


def simulate_policy(bars: list[dict[str, Any]], i: int, strategy: str, direction: str, policy: dict[str, Any]) -> dict[str, Any]:
    past = bars[max(0, i - 500):i + 1]
    if len(past) < 60:
        return {'filled': False, 'pnl_r': 0.0}
    live = f(bars[i]['c'])
    plan = plan_from_policy(strategy, direction, live, past, policy)
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

    remaining = 1.0
    realized = 0.0
    current_stop = stop0
    hit: set[int] = set()
    mfe = mae = 0.0
    exit_reason = 'TIMEOUT'
    exit_rr = 0.0
    last = entry
    for b in future[fill_idx:]:
        low, high, close = f(b['l']), f(b['h']), f(b['c'])
        last = close
        favorable = (high - entry) / risk if direction == 'LONG' else (entry - low) / risk
        adverse = (entry - low) / risk if direction == 'LONG' else (high - entry) / risk
        mfe, mae = max(mfe, favorable), max(mae, adverse)
        stop_hit = low <= current_stop if direction == 'LONG' else high >= current_stop
        # Conservative intrabar ordering: when stop and target are both inside one OHLC bar,
        # assume the stop happened first. This prevents optimistic backtest leakage.
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
            exit_rr = f(plan['targets'][-1]['rr'])
            break
    if remaining > 1e-9:
        exit_rr = (last - entry) * sign / risk
        realized += remaining * exit_rr
        remaining = 0.0

    cost_r = (f(policy.get('all_in_cost_bps'), ALL_IN_COST_BPS) / 10000.0) * entry / risk
    net = realized - cost_r
    return {
        'filled': True,
        'pnl_r': net,
        'gross_r': realized,
        'cost_r': cost_r,
        'mfe_r': mfe,
        'mae_r': mae,
        'exit_reason': exit_reason,
        'entry': entry,
        'stop': stop0,
        'stop_pct': risk / entry,
        'hit_targets': sorted(hit),
    }


def _stats(results: list[dict[str, Any]]) -> dict[str, float]:
    filled = [x for x in results if x.get('filled')]
    p = [f(x.get('pnl_r')) for x in filled]
    gains = sum(max(x, 0) for x in p)
    losses = sum(max(-x, 0) for x in p)
    eq = peak = dd = 0.0
    for x in p:
        eq += x
        peak = max(peak, eq)
        dd = max(dd, peak - eq)
    return {
        'opportunities': len(results),
        'fills': len(filled),
        'fill_rate': len(filled) / max(len(results), 1),
        'profit_factor': gains / max(losses, 1e-9),
        'expectancy_r': mean(p),
        'win_rate': mean(1.0 if x > 0 else 0.0 for x in p),
        'max_drawdown_r': dd,
        'avg_cost_r': mean(f(x.get('cost_r')) for x in filled),
        'avg_stop_pct': mean(f(x.get('stop_pct')) for x in filled),
    }


def _utility(stats: dict[str, float], worst_segment_ev: float = 0.0) -> float:
    if stats['fills'] < 28 or stats['fill_rate'] < .12:
        return -999.0
    pf = max(stats['profit_factor'], 1e-6)
    return stats['expectancy_r'] * 3.2 + math.log(pf) * .30 - stats['max_drawdown_r'] * .006 + min(stats['fills'], 180) / 180 * .08 + min(worst_segment_ev, 0.0) * 1.8


def _opportunities(core: Any, strategy: str, direction: str, model: Any, meta: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[int, int]]:
    con = core.db()
    rows = con.execute(
        'SELECT ts,regime,features,source_quality FROM learning_samples WHERE strategy=? AND direction=? AND source_quality>=55 ORDER BY ts',
        (strategy, direction),
    ).fetchall()
    con.close()
    threshold = f(meta.get('threshold'), .60)
    allowed = set(meta.get('allowed_regimes') or [])
    selected: list[dict[str, Any]] = []
    if rows:
        xs = np.vstack([np.array([f(json.loads(r[2]).get(n)) for n in signal.FEATURE_NAMES], dtype=np.float64) for r in rows])
        probs = model.predict_proba(xs)[:, 1]
        for r, p in zip(rows, probs):
            if float(p) < threshold:
                continue
            if allowed and str(r[1]) not in allowed:
                continue
            selected.append({'ts': int(r[0]), 'regime': str(r[1]), 'probability': float(p)})
    src = core._best_source('ETH', '15m')
    bars = core.load_bars('ETH', '15m', src) if src else []
    index = {int(x['ts']): i for i, x in enumerate(bars)}
    usable = [x for x in selected if x['ts'] in index and index[x['ts']] >= 100 and index[x['ts']] + MAX_HOLD_BARS + 2 < len(bars)]
    return usable, bars, index


def optimize_pair(core: Any, strategy: str, direction: str, force: bool = False) -> dict[str, Any] | None:
    con = core.db()
    signal_store = signal.ModelStore(con)
    model, meta = signal_store.champion(strategy, direction)
    if model is None:
        con.close()
        return None
    model_version = int(meta.get('version') or 0)
    exec_store = ExecutionStore(con)
    existing, existing_meta = exec_store.champion(strategy, direction, model_version)
    if existing is not None and not force:
        con.close()
        return {'strategy': strategy, 'direction': direction, 'model_version': model_version, 'status': 'UNCHANGED', **existing_meta}
    con.close()

    opps, bars, index = _opportunities(core, strategy, direction, model, meta)
    if len(opps) < 190:
        return {'strategy': strategy, 'direction': direction, 'model_version': model_version, 'status': 'INSUFFICIENT', 'opportunities': len(opps)}

    n = len(opps)
    dev_end = max(130, int(n * .72))
    purge = 16
    test = opps[min(n, dev_end + purge):]
    dev = opps[:dev_end]
    if len(test) < 50:
        return {'strategy': strategy, 'direction': direction, 'model_version': model_version, 'status': 'INSUFFICIENT_HOLDOUT', 'opportunities': n, 'holdout': len(test)}

    # Select a static execution policy on development data only. Three chronological
    # segments penalize policies that only work in one era/regime.
    candidates = policy_candidates(strategy)
    best_policy: dict[str, Any] | None = None
    best_score = -999.0
    best_dev: dict[str, Any] = {}
    seg_size = max(1, len(dev) // 3)
    for policy in candidates:
        all_results = [simulate_policy(bars, index[x['ts']], strategy, direction, policy) for x in dev]
        st = _stats(all_results)
        seg_evs = []
        for s in range(3):
            seg = dev[s * seg_size: len(dev) if s == 2 else min(len(dev), (s + 1) * seg_size)]
            seg_st = _stats([simulate_policy(bars, index[x['ts']], strategy, direction, policy) for x in seg])
            seg_evs.append(seg_st['expectancy_r'] if seg_st['fills'] >= 10 else -1.0)
        worst = min(seg_evs)
        score = _utility(st, worst)
        if score > best_score:
            best_score = score
            best_policy = policy
            best_dev = {**st, 'segment_evs': seg_evs, 'worst_segment_ev_r': worst, 'utility': score}
    if best_policy is None:
        return {'strategy': strategy, 'direction': direction, 'model_version': model_version, 'status': 'NO_POLICY'}

    # Exact deployed policy is now evaluated on a never-used chronological holdout.
    holdout_results = [simulate_policy(bars, index[x['ts']], strategy, direction, best_policy) for x in test]
    hs = _stats(holdout_results)
    regime_metrics: dict[str, Any] = {}
    blocked_regimes: list[str] = []
    for rg in sorted({x['regime'] for x in test}):
        rr = [simulate_policy(bars, index[x['ts']], strategy, direction, best_policy) for x in test if x['regime'] == rg]
        rs = _stats(rr)
        regime_metrics[rg] = rs
        if rs['fills'] >= 12 and (rs['expectancy_r'] <= 0 or rs['profit_factor'] < 1.0):
            blocked_regimes.append(rg)

    profitable_segments = sum(x > 0 for x in best_dev.get('segment_evs', [])) / max(len(best_dev.get('segment_evs', [])), 1)
    core_ok = (
        hs['fills'] >= 45
        and hs['profit_factor'] >= 1.20
        and hs['expectancy_r'] >= .10
        and hs['max_drawdown_r'] <= 10.0
        and .15 <= hs['fill_rate'] <= .95
        and best_dev.get('worst_segment_ev_r', -9) >= -.08
        and profitable_segments >= .66
        and hs['avg_stop_pct'] >= MIN_STOP_PCT * .95
    )
    metrics = {
        'schema': EXECUTION_SCHEMA,
        'strategy': strategy,
        'direction': direction,
        'model_version': model_version,
        'certified': bool(core_ok),
        'development': best_dev,
        'oos': hs,
        'profit_factor': hs['profit_factor'],
        'expectancy_r': hs['expectancy_r'],
        'win_rate': hs['win_rate'],
        'max_drawdown_r': hs['max_drawdown_r'],
        'fill_rate': hs['fill_rate'],
        'oos_fills': hs['fills'],
        'oos_opportunities': hs['opportunities'],
        'blocked_regimes': blocked_regimes,
        'regime_metrics': regime_metrics,
        'estimated_all_in_cost_bps': ALL_IN_COST_BPS,
        'reason': 'exact execution policy passed untouched OOS holdout' if core_ok else f"rejected execution OOS: PF={hs['profit_factor']:.2f}, EV={hs['expectancy_r']:.3f}R, fills={hs['fills']}, fill={hs['fill_rate']:.0%}, DD={hs['max_drawdown_r']:.1f}R, devWorst={best_dev.get('worst_segment_ev_r', -9):.3f}R",
    }
    con = core.db()
    store = ExecutionStore(con)
    version = store.save(strategy, direction, model_version, best_policy, metrics, bool(core_ok))
    con.close()
    return {'strategy': strategy, 'direction': direction, 'model_version': model_version, 'execution_version': version, 'status': 'CHAMPION' if core_ok else 'REJECTED', 'policy': best_policy, **metrics}


def optimize_all(core: Any, force: bool = False) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    con = core.db()
    rows = con.execute("SELECT strategy,direction,version FROM model_registry WHERE status='CHAMPION' AND direction IN ('LONG','SHORT') ORDER BY strategy,direction,version DESC").fetchall()
    con.close()
    seen: set[tuple[str, str]] = set()
    for row in rows:
        key = (str(row[0]), str(row[1]))
        if key in seen:
            continue
        seen.add(key)
        result = optimize_pair(core, key[0], key[1], force=force)
        if result:
            out.append(result)
    return out


def execution_for_candidate(core: Any, candidate: dict[str, Any]) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    meta = (candidate.get('model') or {}).get('metrics') or {}
    model_version = int((candidate.get('model') or {}).get('model_version') or meta.get('version') or 0)
    if model_version <= 0:
        return None, {}
    con = core.db()
    store = ExecutionStore(con)
    policy, emeta = store.champion(candidate['strategy'], candidate['direction'], model_version)
    con.close()
    return policy, emeta
