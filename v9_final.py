from __future__ import annotations

import bisect
import json
import math
import os
import random
import statistics
import time
from collections import Counter
from typing import Any

import adaptive_v5 as signal
import execution_v7 as execution
import v5_runtime
import v7_runtime
import v7_timesafe_learning
import v8_evolution
import v8_execution_walkforward as wf


FINAL_VERSION = '8.0.0-20260809'
STRICT_SCHEMA = 5
STRICT_REPLAY_SCHEMA = 1
DERIVATIVE_SAFETY_LAG_SECONDS = max(0, int(os.getenv('STRICT_DERIVATIVE_SAFETY_LAG_SECONDS', '14400')))
REPLAY_STRIDE_BARS = max(1, min(4, int(os.getenv('STRICT_REPLAY_STRIDE_BARS', '2'))))
EXEC_EVOLUTION_GENERATIONS = max(2, min(8, int(os.getenv('EXECUTION_EVOLUTION_GENERATIONS', '5'))))
EXEC_EVOLUTION_ELITES = max(4, min(16, int(os.getenv('EXECUTION_EVOLUTION_ELITES', '8'))))
EXEC_EVOLUTION_CHILDREN = max(2, min(10, int(os.getenv('EXECUTION_EVOLUTION_CHILDREN_PER_ELITE', '4'))))


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, float(x)))


def _closed_slice(rows: list[dict[str, Any]], tf_seconds: int, decision_close_ts: int, max_bars: int) -> list[dict[str, Any]]:
    """Bars are eligible only after their close timestamp is <= the decision clock."""
    if not rows:
        return []
    closes = [int(x['ts']) + int(tf_seconds) for x in rows]
    idx = bisect.bisect_right(closes, int(decision_close_ts))
    return rows[max(0, idx - max_bars):idx]


def _continuous(rows: list[dict[str, Any]], seconds: int) -> bool:
    return bool(rows) and all(int(rows[i]['ts']) - int(rows[i - 1]['ts']) == int(seconds) for i in range(1, len(rows)))


def _strict_derivative_extras(history: Any, decision_ts: int) -> dict[str, float]:
    """Use a conservative publication lag for aggregated 4h historical derivatives.

    This intentionally sacrifices some recency rather than risk treating an interval-open
    timestamp as if the interval's final value had already been known.
    """
    lagged = max(0, int(decision_ts) - DERIVATIVE_SAFETY_LAG_SECONDS)
    oi_rows = history._latest_values('oi_usd', lagged, 20 * 3600, 4) or history._latest_values('oi_coin', lagged, 20 * 3600, 4)
    funding_rows = history._latest_values('funding', int(decision_ts), 16 * 3600, 12)
    long_rows = history._latest_values('liq_long_usd', lagged, 12 * 3600, 2)
    short_rows = history._latest_values('liq_short_usd', lagged, 12 * 3600, 2)
    book_rows = history._latest_values('book_imbalance', lagged, 12 * 3600, 2)

    def fv(x: Any, default: float = 0.0) -> float:
        try:
            z = float(x)
            return z if math.isfinite(z) else default
        except Exception:
            return default

    oi_change = 0.0
    if len(oi_rows) >= 2 and fv(oi_rows[-1]['value']):
        newest, oldest = fv(oi_rows[0]['value']), fv(oi_rows[-1]['value'])
        oi_change = newest / oldest - 1 if oldest else 0.0
    funding = statistics.median([fv(x['value']) for x in funding_rows]) if funding_rows else 0.0
    long_liq = fv(long_rows[0]['value']) if long_rows else 0.0
    short_liq = fv(short_rows[0]['value']) if short_rows else 0.0
    total_liq = long_liq + short_liq
    liq_imbalance = (short_liq - long_liq) / max(total_liq, 1e-9) if total_liq else 0.0
    liq_intensity = math.log1p(total_liq) / 25.0 if total_liq else 0.0
    book = fv(book_rows[0]['value']) if book_rows else 0.0
    available_groups = sum((bool(oi_rows), bool(funding_rows), bool(long_rows and short_rows), bool(book_rows)))
    q = [float(x['quality']) for group in (oi_rows[:1], funding_rows[:1], long_rows[:1], short_rows[:1], book_rows[:1]) for x in group]
    return {
        'oi_change': oi_change,
        'funding': funding,
        'book_imbalance': book,
        'liquidation_imbalance': liq_imbalance,
        'liquidation_intensity': liq_intensity,
        'oi_available': float(bool(oi_rows)),
        'funding_available': float(bool(funding_rows)),
        'liquidation_available': float(bool(long_rows and short_rows)),
        'book_available': float(bool(book_rows)),
        'derivative_coverage': available_groups / 4.0,
        'derivative_quality': (statistics.mean(q) / 100.0) if q else 0.0,
        'historical_derivative_safety_lag_seconds': float(DERIVATIVE_SAFETY_LAG_SECONDS),
    }


def _reference_entry(strategy: str, direction: str, close: float, past: list[dict[str, Any]], a: float, scale: float) -> float:
    sign = 1 if direction == 'LONG' else -1
    e20 = signal.ema([signal.f(x['c']) for x in past], 20)
    base = execution._base_entry_factor(strategy) * scale
    if strategy in ('TREND_PULLBACK', 'RANGE_MEAN_REVERSION'):
        return min(close - base * a, e20) if direction == 'LONG' else max(close + base * a, e20)
    if strategy == 'BREAKOUT_RETEST':
        w = past[-28:-1] if len(past) >= 29 else past[:-1]
        ph = max((signal.f(x['h']) for x in w), default=close)
        pl = min((signal.f(x['l']) for x in w), default=close)
        if direction == 'LONG' and ph < close:
            return ph
        if direction == 'SHORT' and pl > close:
            return pl
    return close - sign * base * a


def _one_reference_outcome(cs: list[dict[str, Any]], i: int, strategy: str, direction: str, entry_scale: float, stop_atr: float, target_r: float, horizon: int) -> tuple[bool, float, float, float]:
    past = cs[:i + 1]
    close = signal.f(cs[i]['c'])
    a = max(signal.atr(past), close * .001)
    entry = _reference_entry(strategy, direction, close, past, a, entry_scale)
    sign = 1 if direction == 'LONG' else -1
    risk = max(stop_atr * a, entry * execution.MIN_STOP_PCT)
    stop = entry - sign * risk
    target = entry + sign * target_r * risk
    wait = {
        'MOMENTUM_CONTINUATION': 4, 'SQUEEZE_EXPANSION': 5,
        'FAILED_BREAKOUT_REVERSAL': 5, 'LIQUIDITY_SWEEP_REVERSAL': 6,
        'RANGE_MEAN_REVERSION': 6, 'TREND_PULLBACK': 8, 'BREAKOUT_RETEST': 8,
    }.get(strategy, 6)
    future = cs[i + 1:i + 1 + horizon]
    fill = next((j for j, b in enumerate(future[:wait]) if signal.f(b['l']) <= entry <= signal.f(b['h'])), None)
    if fill is None:
        return False, 0.0, 0.0, 0.0
    mfe = mae = 0.0
    last = entry
    for j, b in enumerate(future[fill:]):
        low, high, last = signal.f(b['l']), signal.f(b['h']), signal.f(b['c'])
        favorable = (high - entry) / risk if direction == 'LONG' else (entry - low) / risk
        adverse = (entry - low) / risk if direction == 'LONG' else (high - entry) / risk
        mfe, mae = max(mfe, favorable), max(mae, adverse)
        stop_hit = low <= stop if direction == 'LONG' else high >= stop
        if stop_hit:
            return True, -1.0, mfe, mae
        # The fill bar has unknown intra-bar ordering. Do not credit a favorable
        # target that may have happened before the entry was actually touched.
        if j == 0:
            continue
        target_hit = high >= target if direction == 'LONG' else low <= target
        if target_hit:
            return True, target_r, mfe, mae
    rr = _clamp((last - entry) * sign / max(risk, 1e-9), -1.0, target_r)
    return True, rr, mfe, mae


def strict_strategy_outcome(cs: list[dict[str, Any]], i: int, strategy: str, direction: str, horizon: int = 32) -> tuple[int, float, float, float]:
    """Execution-neutral-ish robust label built from several plans frozen before future bars.

    Future bars are labels only. No profile is changed after seeing its result.
    """
    profiles = (
        (.60, .90, 1.15),
        (.85, 1.15, 1.40),
        (1.00, 1.40, 1.70),
        (1.20, 1.75, 2.10),
        (1.45, 2.15, 2.65),
    )
    rows = [_one_reference_outcome(cs, i, strategy, direction, em, sa, tr, horizon) for em, sa, tr in profiles]
    filled = [x for x in rows if x[0]]
    if not filled:
        return 0, 0.0, 0.0, 0.0
    pnls = [x[1] for x in filled]
    robust = statistics.median(pnls)
    fill_conf = min(1.0, len(filled) / 3.0)
    pnl = robust * fill_conf
    positive_ratio = sum(x > .10 for x in pnls) / len(pnls)
    success = int(len(filled) >= 2 and pnl > .10 and positive_ratio >= .60)
    return success, pnl, statistics.median([x[2] for x in filled]), statistics.median([x[3] for x in filled])


def generate_strict_samples(core: Any, batch: int = 500) -> int:
    src15 = core._best_source('ETH', '15m'); src1h = core._best_source('ETH', '1h'); src4h = core._best_source('ETH', '4h'); src1d = core._best_source('ETH', '1d'); srcbtc = core._best_source('BTC', '1h')
    if not all((src15, src1h, src4h, src1d, srcbtc)):
        return 0
    m15 = core.load_bars('ETH', '15m', src15); h1 = core.load_bars('ETH', '1h', src1h); h4 = core.load_bars('ETH', '4h', src4h); d1 = core.load_bars('ETH', '1d', src1d); btc = core.load_bars('BTC', '1h', srcbtc)
    if min(map(len, (m15, h1, h4, d1, btc))) < 120:
        return 0
    ts15 = [int(x['ts']) for x in m15]
    last_ts = int(core.get_state(v5_runtime.REPLAY_STATE_KEY, core.START_TS) or core.START_TS)
    start_i = max(100, bisect.bisect_right(ts15, last_ts))
    con = core.db(); store = v5_runtime.ModelStore(con)
    created = examined = 0; newest = last_ts
    for i in range(start_i, len(m15) - 33):
        if i % REPLAY_STRIDE_BARS:
            continue
        sample_open_ts = ts15[i]
        decision_close_ts = sample_open_ts + core.TIMEFRAME_SECONDS['15m']
        examined += 1; newest = sample_open_ts
        d1s = _closed_slice(d1, core.TIMEFRAME_SECONDS['1d'], decision_close_ts, 420)
        h4s = _closed_slice(h4, core.TIMEFRAME_SECONDS['4h'], decision_close_ts, 900)
        h1s = _closed_slice(h1, core.TIMEFRAME_SECONDS['1h'], decision_close_ts, 1000)
        btcs = _closed_slice(btc, core.TIMEFRAME_SECONDS['1h'], decision_close_ts, 500)
        m15s = m15[max(0, i - 500):i + 1]
        if len(d1s) < 80 or len(h4s) < 100 or len(h1s) < 100 or len(btcs) < 50:
            if examined >= batch: break
            continue
        if not (
            _continuous(m15s[-min(160, len(m15s)):], core.TIMEFRAME_SECONDS['15m'])
            and _continuous(h1s[-min(120, len(h1s)):], core.TIMEFRAME_SECONDS['1h'])
            and _continuous(h4s[-min(60, len(h4s)):], core.TIMEFRAME_SECONDS['4h'])
            and _continuous(d1s[-min(30, len(d1s)):], core.TIMEFRAME_SECONDS['1d'])
            and _continuous(btcs[-min(120, len(btcs)):], core.TIMEFRAME_SECONDS['1h'])
        ):
            if examined >= batch: break
            continue
        regime = v5_runtime.detect_regime(d1s, h4s, h1s)
        extras = _strict_derivative_extras(core.derivative_history, decision_close_ts)
        extras.pop('source_agreement_bps', None)
        features = v7_timesafe_learning.model_safe_features(core.build_features, m15s, h1s, btcs, regime, extras)
        quality = max(60.0, (82.0 if src15 == 'gate' else 74.0) * (0.85 + 0.15 * float(extras.get('derivative_coverage', 0.0))))
        # No hand-written prior threshold filters the sample universe anymore.
        # Every strategy x direction sees the same decision clock and learns its own edge.
        for strategy in signal.STRATEGIES:
            for direction in signal.DIRECTIONS:
                success, pnl_r, mfe_r, mae_r = strict_strategy_outcome(m15, i, strategy, direction, 32)
                store.add_sample({
                    'ts': sample_open_ts, 'strategy': strategy, 'direction': direction,
                    'regime': regime['regime'], 'phase': regime['phase'], 'features': features,
                    'success': success, 'pnl_r': pnl_r, 'mfe_r': mfe_r, 'mae_r': mae_r,
                    'source_quality': quality,
                })
                created += 1
        if examined >= batch:
            break
    store.commit(); con.close()
    if newest > last_ts:
        core.set_state(v5_runtime.REPLAY_STATE_KEY, newest)
    core.state.setdefault('learning', {})['strict_replay_last_batch'] = {
        'decision_stride_15m_bars': REPLAY_STRIDE_BARS,
        'examined_decisions': examined,
        'created_strategy_direction_samples': created,
        'decision_time_semantics': 'sample ts is 15m open; decision clock is ts+900; only bars closed by decision clock are features',
        'future_usage': 'labels/outcomes only after the plan is frozen',
    }
    return created


def strict_simulate_policy(data: dict[str, Any], opp: dict[str, Any], strategy: str, direction: str, policy: dict[str, Any]) -> dict[str, Any]:
    m15 = data['m15']; i15 = data['index15'].get(int(opp['ts']))
    if i15 is None or i15 < 100:
        return {'invalid_data': True, 'filled': False, 'pnl_r': 0.0, 'strict_reason': 'missing decision bar'}
    decision_close = int(opp['ts']) + 900
    past15 = m15[max(0, i15 - 500):i15 + 1]
    if int(past15[-1]['ts']) + 900 > decision_close:
        return {'invalid_data': True, 'filled': False, 'pnl_r': 0.0, 'strict_reason': '15m close-time violation'}
    past30 = _closed_slice(data['m30'], 1800, decision_close, 300)
    past1h = _closed_slice(data['h1'], 3600, decision_close, 300)
    if len(past30) < 40 or len(past1h) < 24:
        return {'invalid_data': True, 'filled': False, 'pnl_r': 0.0, 'strict_reason': 'insufficient closed HTF context'}
    if int(past30[-1]['ts']) + 1800 > decision_close or int(past1h[-1]['ts']) + 3600 > decision_close:
        return {'invalid_data': True, 'filled': False, 'pnl_r': 0.0, 'strict_reason': 'HTF close-time violation'}
    live = execution.f(m15[i15]['c'])
    plan = execution.plan_from_policy(strategy, direction, live, past15, policy, past30, past1h)
    entry, stop0 = execution.f(plan['entry']), execution.f(plan['stop'])
    risk = abs(entry - stop0)
    if risk <= 1e-9:
        return {'invalid_data': True, 'filled': False, 'pnl_r': 0.0, 'strict_reason': 'zero risk distance'}

    m5 = data.get('m5') or []; ts5 = data.get('ts5') or []
    start5 = bisect.bisect_left(ts5, decision_close)
    max_5m = int(policy.get('max_hold_bars', execution.MAX_HOLD_BARS)) * 3
    future = m5[start5:start5 + max_5m]
    if len(future) < min(12, max_5m) or not _continuous(future, 300):
        return {'invalid_data': True, 'filled': False, 'pnl_r': 0.0, 'strict_reason': 'missing 5m execution path'}
    if any(int(b['ts']) < decision_close for b in future):
        return {'invalid_data': True, 'filled': False, 'pnl_r': 0.0, 'strict_reason': 'future path starts before decision clock'}

    expire_5m = min(len(future), int(policy.get('expire_bars', 6)) * 3)
    fill_idx = next((j for j, b in enumerate(future[:expire_5m]) if execution.f(b['l']) <= entry <= execution.f(b['h'])), None)
    if fill_idx is None:
        return {'filled': False, 'pnl_r': 0.0, 'entry': entry, 'stop': stop0, 'path_timeframe': '5m', 'strict_replay': True}

    sign = 1 if direction == 'LONG' else -1
    remaining, realized, current_stop = 1.0, 0.0, stop0
    hit: set[int] = set(); mfe = mae = 0.0; last = entry; exit_reason = 'TIMEOUT'
    for rel, b in enumerate(future[fill_idx:]):
        low, high, close = execution.f(b['l']), execution.f(b['h']), execution.f(b['c']); last = close
        favorable = (high - entry) / risk if direction == 'LONG' else (entry - low) / risk
        adverse = (entry - low) / risk if direction == 'LONG' else (high - entry) / risk
        mfe, mae = max(mfe, favorable), max(mae, adverse)
        stop_hit = low <= current_stop if direction == 'LONG' else high >= current_stop
        if stop_hit:
            exit_rr = (current_stop - entry) * sign / risk
            realized += remaining * exit_rr; remaining = 0.0; exit_reason = 'STOP_OR_TRAIL'; break
        # Unknown order inside the 5m fill candle: never award a target on that
        # same candle. This is deliberately conservative and prevents pre-fill highs/lows
        # from becoming post-fill profit.
        if rel == 0:
            continue
        for idx, target in enumerate(plan['targets']):
            if idx in hit:
                continue
            px = execution.f(target['price'])
            target_hit = high >= px if direction == 'LONG' else low <= px
            if not target_hit:
                continue
            frac = min(remaining, execution.f(target['allocation']) / 100.0)
            realized += frac * execution.f(target['rr']); remaining -= frac; hit.add(idx)
        if 0 in hit:
            current_stop = max(current_stop, entry) if direction == 'LONG' else min(current_stop, entry)
        if 1 in hit:
            locked = entry + sign * execution.f(policy.get('lock_after_tp2_r'), .55) * risk
            current_stop = max(current_stop, locked) if direction == 'LONG' else min(current_stop, locked)
        if 2 in hit:
            locked = entry + sign * execution.f(policy.get('lock_after_tp3_r'), 1.05) * risk
            current_stop = max(current_stop, locked) if direction == 'LONG' else min(current_stop, locked)
        if remaining <= 1e-9:
            exit_reason = 'ALL_TARGETS'; break
    if remaining > 1e-9:
        realized += remaining * (last - entry) * sign / risk
    cost_r = (execution.f(policy.get('all_in_cost_bps'), execution.ALL_IN_COST_BPS) / 10000.0) * entry / risk
    net = realized - cost_r
    return {
        'filled': True, 'pnl_r': net, 'gross_r': realized, 'cost_r': cost_r,
        'mfe_r': mfe, 'mae_r': mae, 'exit_reason': exit_reason, 'entry': entry,
        'stop': stop0, 'stop_pct': risk / max(entry, 1e-9), 'hit_targets': sorted(hit),
        'regime': opp.get('regime'), 'path_timeframe': '5m', 'strict_replay': True,
        'decision_close_ts': decision_close,
    }


def _policy_key(policy: dict[str, Any]) -> str:
    return json.dumps({
        'entry_atr': round(float(policy.get('entry_atr') or 0), 4),
        'stop_atr': round(float(policy.get('stop_atr') or 0), 2),
        'structure_mode': policy.get('structure_mode'),
        'target_rr': [round(float(x), 2) for x in policy.get('target_rr') or []],
        'allocations': list(policy.get('allocations') or []),
        'lock_after_tp2_r': round(float(policy.get('lock_after_tp2_r') or 0), 2),
        'lock_after_tp3_r': round(float(policy.get('lock_after_tp3_r') or 0), 2),
        'expire_bars': int(policy.get('expire_bars') or 0),
        'max_hold_bars': int(policy.get('max_hold_bars') or 0),
    }, sort_keys=True)


def _mutate_policy(parent: dict[str, Any], rng: random.Random, generation: int) -> dict[str, Any]:
    p = dict(parent)
    p['entry_atr'] = round(_clamp(float(p.get('entry_atr') or .05) * math.exp(rng.uniform(-.24, .24)), .015, .45), 4)
    p['stop_atr'] = round(_clamp(float(p.get('stop_atr') or 1.2) + rng.uniform(-.28, .28), .60, 3.20), 2)
    rr = [float(x) for x in p.get('target_rr') or (0.8, 1.4, 2.1, 3.2)]
    rr = [round(_clamp(x + rng.uniform(-.22, .22), .45, 5.50), 2) for x in rr]
    rr[1] = round(max(rr[1], rr[0] + .25), 2); rr[2] = round(max(rr[2], rr[1] + .30), 2); rr[3] = round(max(rr[3], rr[2] + .40), 2)
    p['target_rr'] = rr
    p['lock_after_tp2_r'] = round(_clamp(float(p.get('lock_after_tp2_r') or .55) + rng.uniform(-.18, .18), .0, 1.20), 2)
    p['lock_after_tp3_r'] = round(_clamp(max(float(p.get('lock_after_tp3_r') or 1.05), p['lock_after_tp2_r'] + .20) + rng.uniform(-.22, .22), .25, 2.00), 2)
    p['expire_bars'] = int(_clamp(int(p.get('expire_bars') or 6) + rng.choice((-2, -1, 0, 1, 2)), 3, 14))
    p['max_hold_bars'] = int(_clamp(int(p.get('max_hold_bars') or execution.MAX_HOLD_BARS) + rng.choice((-8, -4, 0, 4, 8)), 16, 64))
    if rng.random() < .18:
        p['structure_mode'] = rng.choice(('15m', '30m', '1h', 'balanced'))
    if rng.random() < .35:
        p['allocations'] = list(rng.choice(tuple(execution.ALLOCATIONS)))
    p['search_origin'] = f'STRICT_DEV_EVOLUTION_GEN_{generation}'
    p['evolution_generation'] = generation
    return p


def _evaluate_policy(data: dict[str, Any], rows: list[dict[str, Any]], strategy: str, direction: str, policy: dict[str, Any], validation: bool = False) -> tuple[float, dict[str, Any]]:
    results = [execution.simulate_policy(data, x, strategy, direction, policy) for x in rows]
    stats = execution._stats(results)
    worst, profitable = execution._segment_worst(data, rows, strategy, direction, policy)
    score = wf._selection_score(stats, worst, len(rows), validation)
    complexity = .0015 * (int(policy.get('max_hold_bars') or 0) / 16.0) + .001 * abs(float(policy.get('stop_atr') or 1.0) - 1.4)
    return score - complexity, {**stats, 'worst_segment_ev_r': worst, 'profitable_segment_ratio': profitable}


def evolved_select_policy(history: list[dict[str, Any]], data: dict[str, Any], strategy: str, direction: str):
    """Multi-generation search whose mutations see DEV only; validation is used once at the end.

    The outer walk-forward audit is never passed here, therefore it cannot influence mutation.
    """
    if len(history) < 72:
        return None
    purge = max(5, min(12, len(history) // 18))
    val_n = max(20, min(48, int(len(history) * .24)))
    dev_end = len(history) - val_n - purge
    if dev_end < 42:
        return None
    dev = history[:dev_end]; val = history[dev_end + purge:]
    if len(val) < 18:
        return None

    seed = 31000 + sum((i + 1) * ord(ch) for i, ch in enumerate(strategy + direction)) + int(history[-1]['ts'] // 900)
    rng = random.Random(seed)
    base_candidates = execution.policy_candidates(strategy)
    scored: list[tuple[float, dict[str, Any], dict[str, Any]]] = []
    seen: set[str] = set()
    for policy in base_candidates:
        k = _policy_key(policy)
        if k in seen: continue
        seen.add(k)
        score, meta = _evaluate_policy(data, dev, strategy, direction, policy, False)
        if score > -998.0:
            scored.append((score, dict(policy), meta))
    if not scored:
        return None
    scored.sort(key=lambda x: x[0], reverse=True)
    elites = scored[:EXEC_EVOLUTION_ELITES]
    generation_trace = [{'generation': 0, 'best_dev_score': elites[0][0], 'population': len(scored)}]

    for generation in range(1, EXEC_EVOLUTION_GENERATIONS + 1):
        children: list[tuple[float, dict[str, Any], dict[str, Any]]] = []
        for _, parent, _ in elites:
            for _ in range(EXEC_EVOLUTION_CHILDREN):
                child = _mutate_policy(parent, rng, generation)
                k = _policy_key(child)
                if k in seen: continue
                seen.add(k)
                score, meta = _evaluate_policy(data, dev, strategy, direction, child, False)
                if score > -998.0:
                    children.append((score, child, meta))
        pool = elites + children
        pool.sort(key=lambda x: x[0], reverse=True)
        elites = pool[:EXEC_EVOLUTION_ELITES]
        generation_trace.append({'generation': generation, 'best_dev_score': elites[0][0], 'new_children': len(children)})
        if not children:
            break

    # Validation is a selector, not a mutation oracle. Only the final small elite set sees it.
    val_ranked: list[tuple[float, dict[str, Any], dict[str, Any], dict[str, Any]]] = []
    for _, policy, dev_meta in elites:
        score, val_meta = _evaluate_policy(data, val, strategy, direction, policy, True)
        if score > -998.0:
            val_ranked.append((score, policy, dev_meta, val_meta))
    if not val_ranked:
        return None
    val_ranked.sort(key=lambda x: x[0], reverse=True)
    _, policy, dev_meta, val_meta = val_ranked[0]
    policy = {**policy, 'strict_replay': True, 'development_only_evolution': True}
    dev_meta = {**dev_meta, 'evolution_generations': generation_trace, 'validation_used_for_mutation': False, 'outer_audit_used_for_mutation': False}
    val_meta = {**val_meta, 'final_elites_tested': len(val_ranked)}
    return policy, dev_meta, val_meta


def _expanded_genomes() -> tuple[dict[str, Any], ...]:
    existing = {str(g['id']): dict(g) for g in v8_evolution.GENOMES}
    extras = (
        {'id': 'conservative_all_1460d', 'feature_mode': 'all', 'half_life_days': 1460, 'params': {'learning_rate': .028, 'max_iter': 250, 'max_leaf_nodes': 7, 'min_samples_leaf': 60, 'l2_regularization': 3.8}},
        {'id': 'price_structure_540d', 'feature_mode': 'price_action', 'half_life_days': 540, 'params': {'learning_rate': .034, 'max_iter': 220, 'max_leaf_nodes': 11, 'min_samples_leaf': 42, 'l2_regularization': 2.8}},
        {'id': 'momentum_regularized_540d', 'feature_mode': 'momentum_structure', 'half_life_days': 540, 'params': {'learning_rate': .036, 'max_iter': 210, 'max_leaf_nodes': 13, 'min_samples_leaf': 38, 'l2_regularization': 2.8}},
        {'id': 'flow_slow_900d', 'feature_mode': 'flow_structure', 'half_life_days': 900, 'params': {'learning_rate': .032, 'max_iter': 230, 'max_leaf_nodes': 9, 'min_samples_leaf': 48, 'l2_regularization': 3.2}},
    )
    for g in extras:
        existing[g['id']] = g
    return tuple(existing.values())


def _migrate(core: Any) -> None:
    if int(core.get_state('point_in_time_sample_schema', 0) or 0) >= STRICT_SCHEMA and int(core.get_state('strict_replay_schema', 0) or 0) >= STRICT_REPLAY_SCHEMA:
        return
    con = core.db(); now = int(time.time())
    # Preserve raw market/derivative caches. Retire only labels/models derived under
    # older replay semantics. Rename avoids duplicating a potentially large sample table.
    exists = con.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='learning_samples'").fetchone()
    if exists:
        con.execute('DROP TABLE IF EXISTS learning_samples_pre_strict_v5_archive')
        con.execute('ALTER TABLE learning_samples RENAME TO learning_samples_pre_strict_v5_archive')
        con.execute('DROP INDEX IF EXISTS ix_learning_samples_strategy_direction_ts')
    signal.ModelStore(con)
    con.execute("UPDATE model_registry SET status='ARCHIVED' WHERE status='CHAMPION'")
    if con.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='execution_registry_v7'").fetchone():
        con.execute("UPDATE execution_registry_v7 SET status='ARCHIVED' WHERE status IN ('CHAMPION','REJECTED')")
    for row in con.execute("SELECT signal_id,payload FROM signals WHERE status='PLANNED'").fetchall():
        payload = json.loads(row[1]); payload['superseded_reason'] = 'strict replay v8 invalidated pre-v8 model certification'
        con.execute("UPDATE signals SET status='EXPIRED',updated_at=?,payload=? WHERE signal_id=?", (now, json.dumps(payload, ensure_ascii=False), row[0]))
    for row in con.execute("SELECT signal_id,payload FROM signals WHERE status='OPEN'").fetchall():
        payload = json.loads(row[1]); payload['legacy_pre_strict_open_plan'] = True
        payload.setdefault('management', {})['legacy_note'] = 'original plan remains immutable; excluded from new v8 certification'
        con.execute('UPDATE signals SET payload=? WHERE signal_id=?', (json.dumps(payload, ensure_ascii=False), row[0]))
    con.commit(); con.close()
    core.set_state(v5_runtime.REPLAY_STATE_KEY, core.START_TS)
    core.set_state('v5_last_train_sample_total', 0)
    core.set_state('last_train_ts_v5', 0)
    core.set_state('v7_execution_signal_signature', [])
    core.set_state('v7_execution_last_attempt_ts', 0)
    core.set_state('point_in_time_sample_schema', STRICT_SCHEMA)
    core.set_state('strict_replay_schema', STRICT_REPLAY_SCHEMA)


async def final_boot_notice(core: Any) -> None:
    key = 'discord_boot_version_strict_final'
    if core.get_state(key) == FINAL_VERSION:
        return
    import v5_runtime as vr
    ok = await vr.robust_send_discord(
        core,
        '🛡️ ETH Adaptive AI 8.0 Strict Replay 已啟動',
        '歷史模擬規則已鎖死：時間 T 的 Signal / Entry / SL / TP 只能使用 T 當下已收線且已可取得的資料。30m/1H/4H/1D 未收線 K 棒禁止進入決策；歷史衍生品採保守 publication lag；未來價格只在計畫鎖定後依時間順序揭露。\n'
        'Signal label 改為多個預先鎖定 reference plans 的 robust 結果，不再被單一 1.2ATR/1.25R 綁死；每個策略×方向都建立樣本，不再由 hand-written prior 門檻先淘汰。\n'
        'Execution 使用多代 DEV-only evolution；Validation 只在最後 elite 中選擇，outer untouched audit 永遠不能回頭指導 mutation。新單仍只先進 deployment evidence，不能用單筆輸贏直接污染 Signal Model。',
        0x2ECC71,
    )
    if ok:
        core.set_state(key, FINAL_VERSION)


def install(core: Any) -> None:
    _migrate(core)
    v8_evolution.GENOMES = _expanded_genomes()
    # Strict historical derivative view and strict sample generation.
    core.derivative_history.extras_at = lambda ts: _strict_derivative_extras(core.derivative_history, int(ts))
    v5_runtime.generate_learning_samples_v5 = lambda c, batch=500: generate_strict_samples(c, batch)
    # Strict execution path supersedes the older 5m simulator and fixes HTF close-time alignment.
    execution.simulate_policy = strict_simulate_policy
    # Multi-generation evolution lives entirely inside the history passed to the inner selector.
    wf._select_policy = evolved_select_policy
    # Version/status surface.
    v8_evolution.EVOLUTION_VERSION = FINAL_VERSION
    core.state['runtime_version'] = FINAL_VERSION
    core.state['strict_replay'] = {
        'version': FINAL_VERSION,
        'schema': STRICT_REPLAY_SCHEMA,
        'sample_schema': STRICT_SCHEMA,
        'decision_stride_15m_bars': REPLAY_STRIDE_BARS,
        'htf_close_time_required': True,
        'derivative_safety_lag_seconds': DERIVATIVE_SAFETY_LAG_SECONDS,
        'fill_bar_favorable_credit': False,
        'execution_generations': EXEC_EVOLUTION_GENERATIONS,
        'execution_elites': EXEC_EVOLUTION_ELITES,
        'audit_can_mutate_policy': False,
        'live_trade_can_directly_mutate_signal_label': False,
    }
    core.state['execution_validation_method'] = 'STRICT_EVENT_TIME -> DEV_ONLY_MULTI_GENERATION_EVOLUTION -> FINAL_VALIDATION_SELECTOR -> NEXT_UNTOUCHED_WALK_FORWARD_AUDIT'
    core.app.version = '8.0.0'
    v7_runtime.maybe_boot_notice = final_boot_notice

    if not any(getattr(r, 'path', None) == '/api/v9/strict-replay' for r in core.app.router.routes):
        @core.app.get('/api/v9/strict-replay')
        def strict_replay_status() -> dict[str, Any]:
            return {
                **core.state.get('strict_replay', {}),
                'runtime': FINAL_VERSION,
                'validation_method': core.state.get('execution_validation_method'),
                'genomes': [g['id'] for g in v8_evolution.GENOMES],
                'safety_contract': [
                    'features at decision T use only information available by T',
                    'HTF candle requires open_ts + timeframe <= decision T',
                    'future bars are revealed only after plan freeze',
                    'unknown 5m fill-bar ordering is resolved conservatively',
                    'outer untouched audit never participates in policy mutation',
                    'new live/paper outcomes are deployment evidence, not direct Signal labels',
                ],
            }
