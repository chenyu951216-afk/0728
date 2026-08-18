from __future__ import annotations

"""V56 causal execution parity, multi-Champion arbitration and forward-only learning.

This is a semantic correction, not an OOS relaxation. Historical research still uses
closed decisions and future 5m bars only for outcome settlement. MARKET fills are now
anchored to the actual executable fill, impossible trailing locks are forbidden, and
historical/current-paper management share the same rules. After historical
certification, current observations live in separate append-only tables and can never
rewrite the fixed historical replay or its OOS verdict.
"""

import asyncio
import gc
import hashlib
import json
import math
import os
import pickle
import random
import statistics
import threading
import time
from typing import Any, Iterable

import numpy as np

import runtime_identity
import v36_bitget_execution_truth as leverage_truth
import v52_execution_authority as execution52

VERSION = 'V56_CAUSAL_MULTICHAMPION_ONLINE_LEARNING'
SCHEMA = 56
STATE_KEY = 'v56_causal_multichampion_learning'
RESET_MARKER = 'v56_canonical_execution_semantics_20260818'

FEATURE_TABLE = 'autonomous_live_feature_tape_v56'
OBS_TABLE = 'autonomous_forward_observation_v56'
CHALLENGER_TABLE = 'autonomous_online_challenger_v56'
ARCHIVE_TABLE = 'autonomous_champion_archive_v56'

CONFLICT_EDGE_R = max(.01, min(.50, float(os.getenv('AUTONOMOUS_V56_CONFLICT_EDGE_R', '.08'))))
LIVE_DECISION_MAX_AGE_SECONDS = max(30, min(900, int(os.getenv('AUTONOMOUS_V56_LIVE_DECISION_MAX_AGE_SECONDS', '120'))))
FORWARD_QUARANTINE_MIN_FILLS = max(24, int(os.getenv('AUTONOMOUS_V56_QUARANTINE_MIN_FILLS', '40')))
FORWARD_QUARANTINE_EV_R = min(0.0, float(os.getenv('AUTONOMOUS_V56_QUARANTINE_EV_R', '-.05')))
FORWARD_QUARANTINE_PF = max(.1, float(os.getenv('AUTONOMOUS_V56_QUARANTINE_PF', '.90')))
FORWARD_RECOVER_MIN_FILLS = max(FORWARD_QUARANTINE_MIN_FILLS, int(os.getenv('AUTONOMOUS_V56_RECOVER_MIN_FILLS', '60')))
FORWARD_RECOVER_EV_R = max(0.0, float(os.getenv('AUTONOMOUS_V56_RECOVER_EV_R', '.03')))
FORWARD_RECOVER_PF = max(1.0, float(os.getenv('AUTONOMOUS_V56_RECOVER_PF', '1.10')))
FORWARD_WINDOW = max(40, int(os.getenv('AUTONOMOUS_V56_FORWARD_WINDOW', '120')))
ONLINE_MIN_TAPE = max(480, int(os.getenv('AUTONOMOUS_V56_ONLINE_MIN_TAPE', '960')))
ONLINE_RESEARCH_INTERVAL_SECONDS = max(3600, int(os.getenv('AUTONOMOUS_V56_ONLINE_RESEARCH_INTERVAL_SECONDS', '21600')))
ONLINE_CANDIDATES_PER_CYCLE = max(1, min(12, int(os.getenv('AUTONOMOUS_V56_ONLINE_CANDIDATES_PER_CYCLE', '4'))))
ONLINE_VALIDATION_MIN_FILLS = max(30, int(os.getenv('AUTONOMOUS_V56_ONLINE_VALIDATION_MIN_FILLS', '60')))
ONLINE_MAX_ACTIVE_CHALLENGERS = max(1, min(12, int(os.getenv('AUTONOMOUS_V56_MAX_ACTIVE_CHALLENGERS', '4'))))
ONLINE_TRAIN_CAP = max(300, min(5000, int(os.getenv('AUTONOMOUS_V56_ONLINE_TRAIN_CAP', '1200'))))

_INSTALL_LOCK = threading.Lock()
_ONLINE_LOCK = threading.Lock()
_INSTALLED = False
_PREINSTALLED = False
_BASE_ANALYSIS: Any | None = None
_BASE_LEARNING_TICK: Any | None = None


def _now() -> int:
    return int(time.time())


def _f(x: Any, default: float = 0.0) -> float:
    try:
        value = float(x)
        return value if math.isfinite(value) else default
    except (TypeError, ValueError):
        return default


def _jd(value: Any) -> Any:
    if hasattr(value, 'item'):
        return value.item()
    if isinstance(value, (bytes, bytearray, memoryview)):
        return {'__binary_bytes__': len(value)}
    raise TypeError(type(value).__name__)


def _d(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _state(core: Any, **patch: Any) -> dict[str, Any]:
    old = core.state.get(STATE_KEY)
    out = dict(old) if isinstance(old, dict) else {}
    out.update(patch)
    out.update({'schema': SCHEMA, 'runtime': VERSION,
                'public_runtime': runtime_identity.RUNTIME_VERSION, 'updated_at': _now()})
    core.state[STATE_KEY] = out
    return out


def _normalize_allocations(values: Iterable[float]) -> list[float]:
    raw = [max(.0001, _f(x)) for x in values]
    total = sum(raw) or 1.0
    out = [round(100.0 * x / total, 6) for x in raw]
    if out:
        out[-1] = round(out[-1] + (100.0 - sum(out)), 6)
    return out


def _canonical_genome(genome: dict[str, Any] | None) -> dict[str, Any]:
    g = json.loads(json.dumps(genome or {}, default=_jd))
    g['direction'] = 'SHORT' if str(g.get('direction')).upper() == 'SHORT' else 'LONG'
    g['entry_market'] = bool(g.get('entry_market'))
    if g['entry_market']:
        # MARKET is an executable market price. Keeping a hidden ATR limit offset would
        # make research stop placement differ from current paper.
        g['entry_offset_atr'] = 0.0
    else:
        g['entry_offset_atr'] = round(max(-1.10, min(1.10, _f(g.get('entry_offset_atr')))), 5)
    g['stop_atr'] = round(max(.45, min(6.0, _f(g.get('stop_atr'), 1.5))), 5)

    rr = [_f(x) for x in (g.get('target_rr') or [])] or [1.0]
    alloc = [_f(x) for x in (g.get('allocations') or [])]
    if len(alloc) != len(rr):
        alloc = [1.0] * len(rr)
    pairs = sorted(zip(rr, alloc), key=lambda z: z[0])
    g['target_rr'] = [round(max(.20, min(12.0, x)), 5) for x, _ in pairs]
    g['allocations'] = _normalize_allocations([a for _, a in pairs])

    start = max(.10, min(8.0, _f(g.get('trail_start_r'), 2.0)))
    lock = max(0.0, _f(g.get('trail_lock_r'), 0.0))
    # A stop moved after bar N can only lock a price bar N has already reached.
    lock = min(lock, max(0.0, start - .05))
    g['trail_start_r'] = round(start, 5)
    g['trail_lock_r'] = round(lock, 5)
    g['breakeven_after_r'] = round(max(.10, min(8.0, _f(g.get('breakeven_after_r'), 1.0))), 5)
    g['expire_bars'] = max(1, int(g.get('expire_bars') or 1))
    g['max_hold_bars'] = max(1, int(g.get('max_hold_bars') or 1))
    g['cooldown_bars'] = max(0, int(g.get('cooldown_bars') or 0))
    return g


def _ensure_tables(core: Any) -> None:
    con = core.db()
    try:
        con.execute(f'''CREATE TABLE IF NOT EXISTS {FEATURE_TABLE}(
            ts INTEGER PRIMARY KEY, close REAL NOT NULL, features TEXT NOT NULL,
            quality REAL NOT NULL, created_at INTEGER NOT NULL)''')
        con.execute(f'''CREATE TABLE IF NOT EXISTS {OBS_TABLE}(
            strategy_id TEXT NOT NULL, source TEXT NOT NULL, genome_hash TEXT NOT NULL,
            decision_ts INTEGER NOT NULL, due_ts INTEGER NOT NULL,
            predicted_ev_r REAL, required_ev_r REAL, status TEXT NOT NULL,
            filled INTEGER, result_r REAL, reason TEXT, settled_at INTEGER,
            created_at INTEGER NOT NULL, PRIMARY KEY(strategy_id,source,decision_ts))''')
        con.execute(f'CREATE INDEX IF NOT EXISTS ix_{OBS_TABLE}_due ON {OBS_TABLE}(status,due_ts)')
        con.execute(f'''CREATE TABLE IF NOT EXISTS {CHALLENGER_TABLE}(
            challenger_id TEXT PRIMARY KEY, parent_id TEXT NOT NULL, created_at INTEGER NOT NULL,
            freeze_ts INTEGER NOT NULL, status TEXT NOT NULL, genome TEXT NOT NULL,
            gate_thresholds TEXT NOT NULL, threshold REAL NOT NULL, model BLOB NOT NULL,
            training_metrics TEXT NOT NULL, promoted_strategy_id TEXT)''')
        con.execute(f'CREATE INDEX IF NOT EXISTS ix_{CHALLENGER_TABLE}_status ON {CHALLENGER_TABLE}(status,created_at)')
        con.execute(f'''CREATE TABLE IF NOT EXISTS {ARCHIVE_TABLE}(
            strategy_id TEXT PRIMARY KEY, archived_at INTEGER NOT NULL, source TEXT NOT NULL,
            genome_hash TEXT NOT NULL, genome TEXT NOT NULL, metrics TEXT NOT NULL, model BLOB)''')
        con.commit()
    finally:
        con.close()


def _archive_champions(core: Any, autonomous: Any, source: str = 'HISTORICAL_CERTIFIED') -> int:
    _ensure_tables(core)
    champions = list(autonomous._load_registry(core, active_only=True) or [])
    con = core.db(); inserted = 0
    try:
        for item in champions:
            g = _canonical_genome(_d(item.get('genome')))
            before = con.total_changes
            con.execute(f'''INSERT OR IGNORE INTO {ARCHIVE_TABLE}
                (strategy_id,archived_at,source,genome_hash,genome,metrics,model)
                VALUES(?,?,?,?,?,?,?)''', (
                str(item.get('strategy_id')), _now(), source, autonomous._hash_payload(g, 20),
                json.dumps(g, sort_keys=True, separators=(',', ':'), default=_jd),
                json.dumps(_d(item.get('metrics')), sort_keys=True, separators=(',', ':'), default=_jd),
                item.get('model_blob')))
            inserted += int(con.total_changes > before)
        con.commit()
    finally:
        con.close()
    return inserted


def _continuous(ts: np.ndarray) -> bool:
    return len(ts) <= 1 or bool(np.all(np.diff(ts) == 300))


def _stop_safe_contract(core: Any, autonomous: Any, stop_fraction: float) -> tuple[dict[str, Any], float, float]:
    contract = dict(leverage_truth._frozen_contract(core, autonomous, create=False) or {})
    if not contract.get('ok'):
        return contract, 0.0, 0.0
    selected, headroom = execution52.safe_leverage(contract, float(stop_fraction))
    return contract, float(selected), float(headroom)


def canonical_simulate(core: Any, autonomous: Any, market: dict[str, Any], ts: int,
                       features: np.ndarray, genome: dict[str, Any]) -> dict[str, Any]:
    """One canonical simulator for development, OOS and forward paper settlement."""
    g = _canonical_genome(genome)
    close = (market.get('close15') or {}).get(int(ts))
    if close is None or float(close) <= 0:
        return {'valid': False, 'filled': False, 'pnl_r': 0.0, 'reason': 'missing_decision_close'}
    ts5 = np.asarray(market.get('ts5') if market.get('ts5') is not None else [], dtype=np.int64)
    if not len(ts5):
        return {'valid': False, 'filled': False, 'pnl_r': 0.0,
                'reason': 'incomplete_full_evolved_holding_horizon'}
    decision_close_ts = int(ts) + 900
    start = int(np.searchsorted(ts5, decision_close_ts, side='left'))
    max_hold_5 = int(g['max_hold_bars']) * 3
    required_end = start + max_hold_5
    if start >= len(ts5) or int(ts5[start]) != decision_close_ts:
        return {'valid': False, 'filled': False, 'pnl_r': 0.0, 'reason': 'missing_first_future_5m'}
    if required_end > len(ts5):
        return {'valid': False, 'filled': False, 'pnl_r': 0.0,
                'reason': 'incomplete_full_evolved_holding_horizon'}
    if not _continuous(ts5[start:required_end]):
        return {'valid': False, 'filled': False, 'pnl_r': 0.0, 'reason': 'future_5m_gap'}

    close = float(close)
    try:
        atr_pct = max(abs(float(features[autonomous.FEATURE_INDEX['atr_pct']])), .00035)
    except Exception:
        atr_pct = .00035
    atr_abs = max(close * atr_pct, close * .00035)
    sign = 1.0 if g['direction'] == 'LONG' else -1.0
    o5 = np.asarray(market['o5']); h5 = np.asarray(market['h5'])
    l5 = np.asarray(market['l5']); c5 = np.asarray(market['c5'])

    if g['entry_market']:
        fill_idx = start
        entry = float(o5[start])
    else:
        planned_entry = close + sign * float(g['entry_offset_atr']) * atr_abs
        expire_5 = min(max_hold_5, int(g['expire_bars']) * 3)
        where = np.flatnonzero((l5[start:start + expire_5] <= planned_entry) &
                               (h5[start:start + expire_5] >= planned_entry))
        if not len(where):
            return {'valid': True, 'filled': False, 'pnl_r': 0.0, 'reason': 'entry_not_filled'}
        fill_idx = start + int(where[0])
        entry = float(planned_entry)

    # Crucial fix: risk/stop are anchored to the ACTUAL fill, never a hypothetical
    # MARKET offset from the prior 15m close.
    risk = max(float(g['stop_atr']) * atr_abs, entry * .0008)
    if risk <= max(entry * 1e-6, 1e-9):
        return {'valid': False, 'filled': False, 'pnl_r': 0.0, 'reason': 'invalid_risk'}
    planned_stop = entry - sign * risk
    stop_fraction = risk / max(entry, 1e-9)
    contract, selected_leverage, headroom = _stop_safe_contract(core, autonomous, stop_fraction)
    if not contract.get('ok'):
        return {'valid': False, 'filled': False, 'pnl_r': 0.0,
                'reason': 'frozen_bitget_max_leverage_contract_unavailable'}
    if selected_leverage < float(getattr(execution52, 'MIN_LEVERAGE', 1.0)) or headroom <= stop_fraction:
        return {'valid': False, 'filled': False, 'pnl_r': 0.0,
                'reason': 'initial_stop_unsafe_even_after_safe_leverage_selection',
                'stop_fraction': stop_fraction, 'selected_leverage': selected_leverage,
                'safe_headroom_fraction': headroom}

    targets = [entry + sign * risk * float(rr) for rr in g['target_rr']]
    allocations = [x / 100.0 for x in _normalize_allocations(g['allocations'])]
    lows = np.asarray(l5[fill_idx:required_end], dtype=np.float64)
    highs = np.asarray(h5[fill_idx:required_end], dtype=np.float64)
    closes = np.asarray(c5[fill_idx:required_end], dtype=np.float64)

    remaining = 1.0; realized = 0.0; hit: set[int] = set()
    stop = float(planned_stop); max_fav_r = 0.0
    exit_reason = 'TIME_EXIT'; exit_rel = len(lows) - 1; exit_price = float(closes[-1])
    for rel in range(len(lows)):
        low = float(lows[rel]); high = float(highs[rel])
        # Stop was known before this bar. If stop and TP are both inside a 5m candle,
        # choose the conservative stop path instead of inventing favorable chronology.
        stop_hit = low <= stop if sign > 0 else high >= stop
        if stop_hit:
            realized += remaining * ((stop - entry) * sign / risk)
            remaining = 0.0; exit_reason = 'STOP_OR_TRAIL'; exit_rel = rel; exit_price = stop
            break

        fav = (high - entry) / risk if sign > 0 else (entry - low) / risk
        max_fav_r = max(max_fav_r, float(fav))
        # The fill bar never receives target credit because the touch/open can happen
        # after the candle extreme that appears in OHLC.
        if rel > 0:
            for k, px in enumerate(targets):
                if k in hit or remaining <= 1e-12:
                    continue
                target_hit = high >= px if sign > 0 else low <= px
                if target_hit:
                    frac = min(remaining, allocations[k])
                    realized += frac * float(g['target_rr'][k]); remaining -= frac; hit.add(k)
            if remaining <= 1e-12:
                exit_reason = 'ALL_TARGETS'; exit_rel = rel
                exit_price = float(targets[max(hit)] if hit else closes[rel]); break

        # Management discovered on this candle applies from the NEXT 5m candle.
        next_stop = stop
        if max_fav_r >= float(g['breakeven_after_r']):
            next_stop = max(next_stop, entry) if sign > 0 else min(next_stop, entry)
        if max_fav_r >= float(g['trail_start_r']):
            feasible = min(float(g['trail_lock_r']), max(0.0, max_fav_r))
            lock = entry + sign * feasible * risk
            next_stop = max(next_stop, lock) if sign > 0 else min(next_stop, lock)
        stop = next_stop

    if remaining > 1e-12:
        exit_price = float(closes[exit_rel])
        realized += remaining * ((exit_price - entry) * sign / risk)
    cost_r = (float(autonomous.ALL_IN_COST_BPS) / 10000.0) * entry / risk
    return {'valid': True, 'filled': True, 'pnl_r': float(realized - cost_r),
            'gross_r': float(realized), 'cost_r': float(cost_r),
            'fill_ts': int(ts5[fill_idx]), 'entry': float(entry), 'stop': float(planned_stop),
            'exit_ts': int(ts5[fill_idx + exit_rel]), 'exit_price': float(exit_price),
            'exit_reason': exit_reason, 'max_fav_r': float(max_fav_r),
            'selected_leverage': selected_leverage,
            'exchange_max_leverage': float(contract.get('effective_max_leverage') or 0.0),
            'safe_headroom_fraction': headroom, 'stop_fraction': stop_fraction,
            'leverage_mode': execution52.LEVERAGE_MODE, 'execution_semantics': VERSION}


def _generic_plan(autonomous: Any, price: float, atr_pct: float, genome: dict[str, Any],
                  decision_close: float | None = None) -> dict[str, Any]:
    g = _canonical_genome(genome); sign = 1.0 if g['direction'] == 'LONG' else -1.0
    reference = float(price if g['entry_market'] else (decision_close if decision_close is not None else price))
    atr_abs = max(reference * max(abs(float(atr_pct)), .00035), reference * .00035)
    entry = float(price) if g['entry_market'] else reference + sign * float(g['entry_offset_atr']) * atr_abs
    risk = max(float(g['stop_atr']) * atr_abs, entry * .0008); stop = entry - sign * risk
    targets = [{'price': round(entry + sign * risk * float(rr), 6), 'rr': float(rr), 'allocation': float(alloc)}
               for rr, alloc in zip(g['target_rr'], g['allocations'])]
    return {'entry': round(entry, 6), 'stop': round(stop, 6), 'risk': float(risk), 'targets': targets,
            'management': {'entry_market': bool(g['entry_market']),
                           'entry_offset_atr': float(g['entry_offset_atr']),
                           'expire_bars': int(g['expire_bars']), 'max_hold_bars': int(g['max_hold_bars']),
                           'breakeven_after_r': float(g['breakeven_after_r']),
                           'trail_start_r': float(g['trail_start_r']), 'trail_lock_r': float(g['trail_lock_r']),
                           'cooldown_bars': int(g['cooldown_bars']), 'never_widen_stop': True,
                           'initial_plan_immutable': True, 'execution_semantics': VERSION}}


def _record_feature(core: Any, decision_ts: int, close: float, features: dict[str, Any], quality: float) -> None:
    _ensure_tables(core); con = core.db()
    try:
        con.execute(f'''INSERT OR IGNORE INTO {FEATURE_TABLE}(ts,close,features,quality,created_at)
            VALUES(?,?,?,?,?)''', (int(decision_ts), float(close),
            json.dumps(features, sort_keys=True, separators=(',', ':'), default=_jd), float(quality), _now()))
        con.commit()
    finally:
        con.close()


def _record_observation(core: Any, autonomous: Any, strategy_id: str, source: str,
                        genome: dict[str, Any], decision_ts: int,
                        pred_ev: float | None, threshold: float | None) -> None:
    g = _canonical_genome(genome); due = int(decision_ts) + 900 + int(g['max_hold_bars']) * 900
    con = core.db()
    try:
        con.execute(f'''INSERT OR IGNORE INTO {OBS_TABLE}
            (strategy_id,source,genome_hash,decision_ts,due_ts,predicted_ev_r,required_ev_r,status,created_at)
            VALUES(?,?,?,?,?,?,?,?,?)''', (str(strategy_id), str(source), autonomous._hash_payload(g, 20),
            int(decision_ts), due, None if pred_ev is None else float(pred_ev),
            None if threshold is None else float(threshold), 'PENDING', _now()))
        con.commit()
    finally:
        con.close()


def _quarantine_state(core: Any) -> dict[str, Any]:
    return _d(core.get_state('v56_champion_quarantine', {}))


def _champion_forward_stats(core: Any, strategy_id: str, limit: int = FORWARD_WINDOW) -> dict[str, float]:
    con = core.db()
    try:
        rows = con.execute(f'''SELECT result_r FROM {OBS_TABLE}
            WHERE strategy_id=? AND source='CERTIFIED' AND status='SETTLED' AND filled=1
            ORDER BY decision_ts DESC LIMIT ?''', (str(strategy_id), int(limit))).fetchall()
    finally:
        con.close()
    pnls = [float(r[0]) for r in reversed(rows) if r[0] is not None]
    if not pnls:
        return {'fills': 0.0, 'ev': 0.0, 'pf': 0.0, 'win': 0.0, 'dd': 0.0}
    gains = sum(max(x, 0.0) for x in pnls); losses = sum(max(-x, 0.0) for x in pnls)
    eq = peak = dd = 0.0
    for p in pnls:
        eq += p; peak = max(peak, eq); dd = max(dd, peak - eq)
    return {'fills': float(len(pnls)), 'ev': float(statistics.mean(pnls)),
            'pf': float(gains / max(losses, 1e-9)),
            'win': float(sum(x > 0 for x in pnls) / len(pnls)), 'dd': float(dd)}


def _refresh_quarantine(core: Any, autonomous: Any) -> dict[str, Any]:
    q = _quarantine_state(core)
    for item in list(autonomous._load_registry(core, active_only=True) or []):
        sid = str(item.get('strategy_id')); st = _champion_forward_stats(core, sid); fills = int(st['fills'])
        existing = _d(q.get(sid))
        if existing.get('active'):
            if fills >= FORWARD_RECOVER_MIN_FILLS and st['ev'] >= FORWARD_RECOVER_EV_R and st['pf'] >= FORWARD_RECOVER_PF:
                q[sid] = {'active': False, 'at': _now(), 'reason': 'forward performance recovered', 'stats': st}
        elif fills >= FORWARD_QUARANTINE_MIN_FILLS and st['ev'] <= FORWARD_QUARANTINE_EV_R and st['pf'] <= FORWARD_QUARANTINE_PF:
            q[sid] = {'active': True, 'at': _now(), 'reason': 'forward degradation guard', 'stats': st}
    core.set_state('v56_champion_quarantine', q); return q


def _analysis_factory(core: Any, autonomous: Any, base_analysis: Any):
    def analysis(bundle: dict[str, Any]) -> dict[str, Any]:
        z = dict(base_analysis(core, bundle) or {}); m15 = bundle.get('eth_15m') or []
        if not m15:
            return z
        decision_ts = int(m15[-1]['ts']); decision_close = float(m15[-1]['c'])
        z.update({'decision_ts': decision_ts, 'decision_close_ts': decision_ts + 900,
                  'decision_close_price': decision_close})
        _record_feature(core, decision_ts, decision_close, _d(z.get('features')),
                        float((z.get('data_quality') or {}).get('score') or 0.0))
        q = _refresh_quarantine(core, autonomous); candidates: list[dict[str, Any]] = []
        for raw in list(z.get('autonomous_candidates') or []):
            x = dict(raw); pred = x.get('predicted_ev_r'); threshold = x.get('threshold')
            x['required_ev_r'] = threshold
            x['edge_r'] = (float(pred) - float(threshold)) if pred is not None and threshold is not None else None
            sid = str(x.get('strategy') or '')
            x['quarantined'] = bool(_d(q.get(sid)).get('active'))
            if x['quarantined']:
                x['tradeable'] = False; x['reason'] = 'forward degradation quarantine'
            if x.get('genome'):
                _record_observation(core, autonomous, sid, 'CERTIFIED', x['genome'], decision_ts,
                                    x.get('predicted_ev_r'), x.get('threshold'))
            candidates.append(x)

        tradeable = [x for x in candidates if x.get('tradeable') and not x.get('quarantined')]
        tradeable.sort(key=lambda x: (_f(x.get('edge_r'), -999.0), _f(x.get('score'), -999.0)), reverse=True)
        conflict = None; selected: dict[str, Any] | None = None
        if tradeable:
            selected = tradeable[0]
            opposite = next((x for x in tradeable[1:] if x.get('direction') != selected.get('direction')), None)
            if opposite is not None:
                gap = abs(_f(selected.get('edge_r')) - _f(opposite.get('edge_r')))
                if gap <= CONFLICT_EDGE_R:
                    conflict = {'status': 'WAIT_CONFLICT', 'edge_gap_r': gap,
                                'required_edge_separation_r': CONFLICT_EDGE_R,
                                'leaders': [{'strategy': selected.get('strategy'), 'direction': selected.get('direction'), 'edge_r': selected.get('edge_r')},
                                            {'strategy': opposite.get('strategy'), 'direction': opposite.get('direction'), 'edge_r': opposite.get('edge_r')}]}
                    selected = dict(selected); selected['tradeable'] = False
                    selected['reason'] = 'opposite certified Champions have statistically-close positive edges'
        if selected is None:
            selected = candidates[0] if candidates else _d(z.get('selection'))
        z['selection'] = selected; z['autonomous_candidates'] = candidates
        z['champion_arbitration'] = conflict or {'status': 'SELECTED' if selected.get('tradeable') else 'WAIT',
            'selected': selected.get('strategy'), 'direction': selected.get('direction'), 'edge_r': selected.get('edge_r')}
        z['trade_label'] = 'AUTONOMOUS LEARNED TRADE' if selected.get('tradeable') else 'WAIT / AUTONOMOUS RESEARCH'
        return z
    return analysis


def _signal_seen_decision(core: Any, strategy_id: str, decision_ts: int) -> bool:
    return int(core.get_state(f'v56_last_order_decision:{strategy_id}', 0) or 0) >= int(decision_ts)


def _cooldown_ok(core: Any, strategy_id: str, decision_ts: int, cooldown_bars: int) -> bool:
    con = core.db()
    try:
        row = con.execute('''SELECT MAX(COALESCE(exit_ts,filled_at,created_at)) FROM signals
            WHERE strategy=? AND status IN ('CLOSED','EXPIRED','CANCELLED')''', (str(strategy_id),)).fetchone()
    finally:
        con.close()
    last = int(row[0] or 0) if row else 0
    return not last or int(decision_ts) + 900 >= last + int(cooldown_bars) * 900


def _create_signal_factory(core: Any, autonomous: Any):
    def create(analysis: dict[str, Any], m15: list[dict[str, Any]]) -> dict[str, Any] | None:
        sel = _d((analysis or {}).get('selection'))
        if not sel.get('tradeable') or not sel.get('genome'):
            return None
        if core.latest_signal():
            return core.latest_signal()
        sid = str(sel.get('strategy') or '')
        registry = {str(x.get('strategy_id')): x for x in autonomous._load_registry(core, active_only=True)}
        item = registry.get(sid)
        if item is None:
            return None
        persisted = _canonical_genome(_d(item.get('genome'))); selected = _canonical_genome(_d(sel.get('genome')))
        if autonomous._hash_payload(persisted, 20) != autonomous._hash_payload(selected, 20):
            _state(core, live_fail_closed='selected genome differs from persisted Champion', strategy_id=sid); return None
        decision_ts = int((analysis or {}).get('decision_ts') or (m15[-1]['ts'] if m15 else 0))
        decision_close_ts = int((analysis or {}).get('decision_close_ts') or decision_ts + 900)
        if decision_ts <= 0:
            return None
        age = _now() - decision_close_ts
        if age < -5 or age > LIVE_DECISION_MAX_AGE_SECONDS:
            _state(core, live_wait_reason='stale_or_not_yet_closed_decision', decision_age_seconds=age); return None
        if _signal_seen_decision(core, sid, decision_ts):
            return None
        if not _cooldown_ok(core, sid, decision_ts, int(selected.get('cooldown_bars') or 0)):
            _state(core, live_wait_reason='learned_cooldown_active', strategy_id=sid); return None

        plan = _generic_plan(autonomous, float(analysis['price']),
                             _f(_d(analysis.get('features')).get('atr_pct'), .00035), selected,
                             decision_close=float(analysis.get('decision_close_price') or analysis['price']))
        stop_fraction = abs(float(plan['entry']) - float(plan['stop'])) / max(float(plan['entry']), 1e-9)
        live_contract = dict(leverage_truth._fetch_contract(float(autonomous.PAPER_NOTIONAL_USDT), force=True) or {})
        if not live_contract.get('ok'):
            return None
        selected_leverage, headroom = execution52.safe_leverage(live_contract, stop_fraction)
        if selected_leverage < float(getattr(execution52, 'MIN_LEVERAGE', 1.0)) or headroom <= stop_fraction:
            return None
        now = _now(); status = 'OPEN' if bool(selected['entry_market']) else 'PLANNED'
        filled_at = now if status == 'OPEN' else None
        risk = abs(float(plan['entry']) - float(plan['stop']))
        cost_r = (float(autonomous.ALL_IN_COST_BPS) / 10000.0) * float(plan['entry']) / max(risk, 1e-9)
        payload = {'initial_plan': plan, 'selection': sel, 'regime': analysis.get('regime') or {},
            'features': analysis.get('features') or {}, 'data_quality': float((analysis.get('data_quality') or {}).get('score', 0.0)),
            'created_from_snapshot': analysis.get('snapshot_ts'), 'immutable': True,
            'autonomous_schema': autonomous.SCHEMA, 'v56_schema': SCHEMA,
            'paper_notional_usdt': float(autonomous.PAPER_NOTIONAL_USDT), 'leverage_mode': execution52.LEVERAGE_MODE,
            'paper_only': True, 'bitget_execution_contract': live_contract,
            'exchange_max_leverage': float(live_contract.get('effective_max_leverage') or 0.0),
            'selected_leverage': float(selected_leverage), 'safe_headroom_fraction': float(headroom),
            'decision_ts': decision_ts, 'decision_close_ts': decision_close_ts,
            'last_processed_5m_ts': decision_close_ts - 300, 'initial_risk': risk, 'cost_r': cost_r,
            'management': {**plan['management'], 'hit_targets': [], 'remaining_fraction': 1.0,
                           'partial_realized_r': 0.0, 'mfe_r': 0.0, 'mae_r': 0.0, 'trail_reason': None}}
        signal_id = f"{now}-V56-{sid[-6:]}-{selected['direction'][0]}"
        con = core.db()
        try:
            con.execute('''INSERT INTO signals(signal_id,created_at,updated_at,status,strategy,direction,regime,phase,
                probability,entry,initial_stop,current_stop,targets,payload,filled_at)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''', (signal_id, now, now, status, sid, selected['direction'],
                'AI_DISCOVERED_STATE', str(sel.get('behavior_label') or 'AI_STATE'), _f(sel.get('probability'), .5),
                float(plan['entry']), float(plan['stop']), float(plan['stop']), json.dumps(plan['targets']),
                json.dumps(payload, ensure_ascii=False, default=_jd), filled_at)); con.commit()
        finally:
            con.close()
        core.set_state(f'v56_last_order_decision:{sid}', decision_ts); return core.latest_signal()
    return create


def _best_current_5m(core: Any, after_ts: int, max_ts: int) -> list[dict[str, Any]]:
    con = core.db()
    try:
        sources = [str(r[0]) for r in con.execute(
            "SELECT DISTINCT source FROM market_bars WHERE asset='ETH' AND tf='5m' AND ts>? AND ts<=?",
            (int(after_ts), int(max_ts))).fetchall()]
        priority = ['gate', 'bitget', 'binance', 'bybit', 'okx']
        sources.sort(key=lambda x: (priority.index(x) if x in priority else 99, x)); expected_start = int(after_ts) + 300
        for src in sources:
            rows = con.execute('''SELECT ts,o,h,l,c FROM market_bars WHERE source=? AND asset='ETH' AND tf='5m'
                AND ts>? AND ts<=? ORDER BY ts''', (src, int(after_ts), int(max_ts))).fetchall()
            if not rows:
                continue
            stamps = [int(r[0]) for r in rows]
            if stamps[0] != expected_start or any(b - a != 300 for a, b in zip(stamps, stamps[1:])):
                continue
            return [{'ts': int(r[0]), 'o': float(r[1]), 'h': float(r[2]), 'l': float(r[3]), 'c': float(r[4]), 'source': src}
                    for r in rows]
    finally:
        con.close()
    return []


def _finalize_signal(core: Any, row: dict[str, Any], exit_price: float, reason: str, ts: int,
                     mgmt: dict[str, Any], payload: dict[str, Any]) -> None:
    entry = float(row['entry']); stop0 = float(row['initial_stop']); risk = abs(entry - stop0) or 1e-9
    sign = 1.0 if row['direction'] == 'LONG' else -1.0
    remaining = max(0.0, min(1.0, _f(mgmt.get('remaining_fraction'), 1.0)))
    partial = _f(mgmt.get('partial_realized_r'), 0.0)
    gross = partial + remaining * ((float(exit_price) - entry) * sign / risk)
    net = gross - _f(payload.get('cost_r'), 0.0)
    mgmt['remaining_fraction'] = 0.0; mgmt['closed_reason'] = reason; payload['management'] = mgmt
    con = core.db()
    try:
        con.execute('''UPDATE signals SET status='CLOSED',updated_at=?,exit_ts=?,exit_price=?,exit_reason=?,
            realized_r=?,review_until=?,payload=? WHERE signal_id=?''', (int(ts), int(ts), float(exit_price), str(reason),
            float(net), int(ts) + int(getattr(core, 'POST_EXIT_BARS', 96)) * 900,
            json.dumps(payload, ensure_ascii=False, default=_jd), row['signal_id'])); con.commit()
    finally:
        con.close()


def _process_5m(core: Any, row: dict[str, Any], bar: dict[str, Any]) -> bool:
    payload = _d(row.get('payload')); mgmt = _d(payload.get('management')); ts = int(bar['ts'])
    if ts <= int(payload.get('last_processed_5m_ts') or -1) or ts < int(payload.get('decision_close_ts') or 0):
        return False
    entry = float(row['entry']); stop0 = float(row['initial_stop']); current_stop = float(row['current_stop'])
    risk = abs(entry - stop0) or 1e-9; sign = 1.0 if row['direction'] == 'LONG' else -1.0
    low, high, close = float(bar['l']), float(bar['h']), float(bar['c']); fill_bar = False
    if str(row['status']) == 'PLANNED':
        touched = low <= entry <= high
        expire_at = int(payload.get('decision_close_ts') or row['created_at']) + int(mgmt.get('expire_bars') or 1) * 900
        if not touched:
            payload['last_processed_5m_ts'] = ts; con = core.db()
            try:
                if ts >= expire_at:
                    con.execute("UPDATE signals SET status='EXPIRED',updated_at=?,payload=? WHERE signal_id=?",
                                (ts, json.dumps(payload, ensure_ascii=False, default=_jd), row['signal_id']))
                else:
                    con.execute('UPDATE signals SET updated_at=?,payload=? WHERE signal_id=?',
                                (ts, json.dumps(payload, ensure_ascii=False, default=_jd), row['signal_id']))
                con.commit()
            finally:
                con.close()
            return True
        fill_bar = True; row['status'] = 'OPEN'; row['filled_at'] = ts
        con = core.db()
        try:
            con.execute("UPDATE signals SET status='OPEN',filled_at=?,updated_at=? WHERE signal_id=?",
                        (ts, ts, row['signal_id'])); con.commit()
        finally:
            con.close()

    stop_hit = low <= current_stop if sign > 0 else high >= current_stop
    if stop_hit:
        payload['last_processed_5m_ts'] = ts
        _finalize_signal(core, row, current_stop, 'AUTONOMOUS_STOP_OR_TRAIL', ts, mgmt, payload); return True
    hit = set(int(x) for x in (mgmt.get('hit_targets') or [])); remaining = max(0.0, min(1.0, _f(mgmt.get('remaining_fraction'), 1.0)))
    partial = _f(mgmt.get('partial_realized_r'), 0.0); targets = list(row.get('targets') or [])
    if not fill_bar:
        for k, target in enumerate(targets):
            if k in hit or remaining <= 1e-12:
                continue
            px = float(target['price']); target_hit = high >= px if sign > 0 else low <= px
            if target_hit:
                frac = min(remaining, float(target.get('allocation') or 0.0) / 100.0)
                partial += frac * float(target.get('rr') or 0.0); remaining -= frac; hit.add(k)
    fav_r = (high - entry) / risk if sign > 0 else (entry - low) / risk
    adv_r = (entry - low) / risk if sign > 0 else (high - entry) / risk
    mfe = max(_f(mgmt.get('mfe_r')), fav_r); mae = max(_f(mgmt.get('mae_r')), adv_r)
    mgmt.update({'hit_targets': sorted(hit), 'remaining_fraction': remaining,
                 'partial_realized_r': partial, 'mfe_r': mfe, 'mae_r': mae})
    payload['last_processed_5m_ts'] = ts; payload['management'] = mgmt
    if remaining <= 1e-12:
        last_target = float(targets[max(hit)]['price']) if hit else close
        _finalize_signal(core, row, last_target, 'AUTONOMOUS_ALL_TARGETS', ts, mgmt, payload); return True

    next_stop = current_stop
    if mfe >= _f(mgmt.get('breakeven_after_r'), 999.0):
        next_stop = max(next_stop, entry) if sign > 0 else min(next_stop, entry)
    if mfe >= _f(mgmt.get('trail_start_r'), 999.0):
        feasible = min(_f(mgmt.get('trail_lock_r')), max(0.0, mfe)); lock = entry + sign * feasible * risk
        next_stop = max(next_stop, lock) if sign > 0 else min(next_stop, lock)
    filled_at = int(row.get('filled_at') or row['created_at']); max_hold = int(mgmt.get('max_hold_bars') or 1) * 900
    if ts + 300 >= filled_at + max_hold:
        _finalize_signal(core, row, close, 'AUTONOMOUS_TIME_EXIT', ts, mgmt, payload); return True
    con = core.db()
    try:
        con.execute('UPDATE signals SET current_stop=?,updated_at=?,payload=? WHERE signal_id=?',
                    (float(next_stop), ts, json.dumps(payload, ensure_ascii=False, default=_jd), row['signal_id'])); con.commit()
    finally:
        con.close()
    return True


def _update_signal_factory(core: Any):
    def update(_bar: dict[str, Any]) -> dict[str, Any] | None:
        row = core.latest_signal()
        if not row:
            return None
        payload = _d(row.get('payload')); last = int(payload.get('last_processed_5m_ts') or (int(payload.get('decision_close_ts') or 0) - 300))
        max_ts = (_now() // 300) * 300 - 300
        for b in _best_current_5m(core, last, max_ts):
            fresh = core.latest_signal()
            if not fresh or str(fresh.get('signal_id')) != str(row.get('signal_id')) or str(fresh.get('status')) not in ('PLANNED', 'OPEN'):
                break
            _process_5m(core, fresh, b)
        return core.latest_signal()
    return update


def _load_current_market(core: Any, decision_ts: int, max_hold_bars: int, decision_close: float) -> dict[str, Any]:
    start = int(decision_ts) + 900; end = start + int(max_hold_bars) * 900 - 300; expected = int(max_hold_bars) * 3
    con = core.db()
    try:
        sources = [str(r[0]) for r in con.execute("SELECT source FROM market_bars WHERE asset='ETH' AND tf='5m' AND ts>=? AND ts<=? GROUP BY source", (start, end)).fetchall()]
        priority = ['gate', 'bitget', 'binance', 'bybit', 'okx']; sources.sort(key=lambda x: (priority.index(x) if x in priority else 99, x))
        for src in sources:
            rows = con.execute("SELECT ts,o,h,l,c FROM market_bars WHERE source=? AND asset='ETH' AND tf='5m' AND ts>=? AND ts<=? ORDER BY ts", (src, start, end)).fetchall()
            if len(rows) != expected:
                continue
            stamps = np.asarray([int(r[0]) for r in rows], dtype=np.int64)
            if int(stamps[0]) != start or not _continuous(stamps):
                continue
            return {'source5': src, 'source15': 'V56_FORWARD_TAPE', 'ts5': stamps,
                    'o5': np.asarray([float(r[1]) for r in rows]), 'h5': np.asarray([float(r[2]) for r in rows]),
                    'l5': np.asarray([float(r[3]) for r in rows]), 'c5': np.asarray([float(r[4]) for r in rows]),
                    'close15': {int(decision_ts): float(decision_close)}}
    finally:
        con.close()
    return {}


def _load_challengers(core: Any) -> dict[str, dict[str, Any]]:
    _ensure_tables(core); con = core.db()
    try:
        rows = con.execute(f'''SELECT challenger_id,parent_id,created_at,freeze_ts,status,genome,gate_thresholds,
            threshold,model,training_metrics,promoted_strategy_id FROM {CHALLENGER_TABLE}
            WHERE status IN ('FORWARD_VALIDATING','PROMOTED')''').fetchall()
    finally:
        con.close()
    out: dict[str, dict[str, Any]] = {}
    for r in rows:
        try:
            out[str(r[0])] = {'strategy_id': str(r[0]), 'parent_id': str(r[1]), 'created_at': int(r[2]),
                'freeze_ts': int(r[3]), 'status': str(r[4]), 'genome': json.loads(r[5]),
                'gate_thresholds': json.loads(r[6]), 'threshold': float(r[7]), 'model_blob': bytes(r[8]),
                'training_metrics': json.loads(r[9]), 'promoted_strategy_id': r[10]}
        except Exception:
            continue
    return out


def _settle_forward(core: Any, autonomous: Any, limit: int = 128) -> dict[str, int]:
    _ensure_tables(core); con = core.db()
    try:
        rows = con.execute(f'''SELECT strategy_id,source,genome_hash,decision_ts,due_ts FROM {OBS_TABLE}
            WHERE status='PENDING' AND due_ts<=? ORDER BY due_ts LIMIT ?''', (_now(), int(limit))).fetchall()
    finally:
        con.close()
    registry = {str(x.get('strategy_id')): x for x in autonomous._load_registry(core, active_only=True)}
    challengers = _load_challengers(core); done = waiting = invalid = 0
    for sid, source, genome_hash, dts, _due in rows:
        item = registry.get(str(sid)) if str(source) == 'CERTIFIED' else challengers.get(str(sid))
        if not item:
            con = core.db()
            try:
                con.execute(f"UPDATE {OBS_TABLE} SET status='ORPHANED',reason=?,settled_at=? WHERE strategy_id=? AND source=? AND decision_ts=?",
                            ('strategy package unavailable', _now(), sid, source, dts)); con.commit()
            finally:
                con.close()
            continue
        g = _canonical_genome(_d(item.get('genome')))
        if autonomous._hash_payload(g, 20) != str(genome_hash):
            con = core.db()
            try:
                con.execute(f"UPDATE {OBS_TABLE} SET status='INVALID',reason=?,settled_at=? WHERE strategy_id=? AND source=? AND decision_ts=?",
                            ('genome hash changed after observation freeze', _now(), sid, source, dts)); con.commit()
            finally:
                con.close()
            invalid += 1; continue
        con = core.db()
        try:
            fr = con.execute(f'SELECT close,features FROM {FEATURE_TABLE} WHERE ts=?', (int(dts),)).fetchone()
        finally:
            con.close()
        if not fr:
            waiting += 1; continue
        try:
            fdict = json.loads(fr[1])
        except Exception:
            fdict = {}
        vec = np.asarray([_f(fdict.get(n), 0.0) for n in autonomous.FEATURE_NAMES], dtype=np.float32)
        market = _load_current_market(core, int(dts), int(g['max_hold_bars']), float(fr[0]))
        if not market:
            waiting += 1; continue
        res = canonical_simulate(core, autonomous, market, int(dts), vec, g)
        status = 'SETTLED' if res.get('valid') else 'INVALID'; con = core.db()
        try:
            con.execute(f'''UPDATE {OBS_TABLE} SET status=?,filled=?,result_r=?,reason=?,settled_at=?
                WHERE strategy_id=? AND source=? AND decision_ts=?''', (status, int(bool(res.get('filled'))),
                float(res.get('pnl_r') or 0.0), str(res.get('reason') or res.get('exit_reason') or ''),
                _now(), sid, source, int(dts))); con.commit()
        finally:
            con.close()
        done += int(status == 'SETTLED'); invalid += int(status == 'INVALID')
    return {'settled': done, 'waiting_market_data': waiting, 'invalid': invalid}


def _record_challenger_predictions(core: Any, autonomous: Any, decision_ts: int, features: dict[str, Any]) -> None:
    for cid, item in _load_challengers(core).items():
        if item.get('status') != 'FORWARD_VALIDATING' or decision_ts <= int(item.get('freeze_ts') or 0):
            continue
        g = _canonical_genome(_d(item.get('genome')))
        try:
            if not autonomous._live_gate(features, list(item.get('gate_thresholds') or [])):
                continue
            model = pickle.loads(item['model_blob']); vec = np.asarray([[_f(features.get(n), 0.0) for n in g['feature_names']]], dtype=np.float32)
            pred = float(model.predict(vec)[0]); threshold = float(item['threshold'])
        except Exception:
            continue
        if pred >= threshold:
            _record_observation(core, autonomous, cid, 'CHALLENGER', g, decision_ts, pred, threshold)


def _forward_rows_for_genome(core: Any, autonomous: Any, genome: dict[str, Any], before_ts: int) -> tuple[np.ndarray, np.ndarray]:
    g = _canonical_genome(genome); con = core.db()
    try:
        rows = con.execute(f'SELECT ts,close,features FROM {FEATURE_TABLE} WHERE ts<? ORDER BY ts DESC LIMIT ?',
                           (int(before_ts), int(ONLINE_TRAIN_CAP))).fetchall()
    finally:
        con.close()
    xs: list[list[float]] = []; ys: list[float] = []
    for r in reversed(rows):
        try:
            fdict = json.loads(r[2])
        except Exception:
            continue
        vec = np.asarray([_f(fdict.get(n), 0.0) for n in autonomous.FEATURE_NAMES], dtype=np.float32)
        market = _load_current_market(core, int(r[0]), int(g['max_hold_bars']), float(r[1]))
        if not market:
            continue
        res = canonical_simulate(core, autonomous, market, int(r[0]), vec, g)
        if res.get('valid') and res.get('filled'):
            xs.append(vec.tolist()); ys.append(float(res['pnl_r']))
    if not xs:
        return np.empty((0, len(autonomous.FEATURE_NAMES)), dtype=np.float32), np.empty(0, dtype=np.float32)
    return np.asarray(xs, dtype=np.float32), np.asarray(ys, dtype=np.float32)


def _challenger_precheck(core: Any, autonomous: Any, parent: dict[str, Any], genome: dict[str, Any], seed: int) -> dict[str, Any] | None:
    freeze = (_now() // 900) * 900; g = _canonical_genome(genome); x, y = _forward_rows_for_genome(core, autonomous, g, freeze)
    if len(y) < 120 or float(np.std(y)) < 1e-6:
        return None
    n = len(y); a = max(70, int(n * .65)); b = max(a + 30, int(n * .82))
    if b >= n - 20:
        return None
    thresholds = autonomous._gate_thresholds(x[:a], g.get('gate') or [])
    mt = autonomous._gate_mask(x[:a], thresholds); mc = autonomous._gate_mask(x[a:b], thresholds); mv = autonomous._gate_mask(x[b:], thresholds)
    xt, yt = x[:a][mt], y[:a][mt]; xc, yc = x[a:b][mc], y[a:b][mc]; xv, yv = x[b:][mv], y[b:][mv]
    if len(yt) < 60 or len(yc) < 20 or len(yv) < 20:
        return None
    model = autonomous._model(g, int(seed) & 0xFFFFFFFF); model.fit(autonomous._feature_subset_matrix(xt, g), yt)
    picked = autonomous._threshold_from_cal(model.predict(autonomous._feature_subset_matrix(xc, g)), yc)
    if picked is None:
        return None
    threshold, _ = picked; pred = model.predict(autonomous._feature_subset_matrix(xv, g)); selected = yv[pred >= threshold]
    if len(selected) < 12:
        return None
    st = autonomous._stats([{'valid': True, 'filled': True, 'pnl_r': float(v)} for v in selected])
    if st['ev'] <= 0.0 or st['pf'] < 1.05:
        return None
    mall = autonomous._gate_mask(x, thresholds); xf, yf = x[mall], y[mall]
    if len(yf) < 90:
        return None
    full_model = autonomous._model(g, (int(seed) + 1) & 0xFFFFFFFF); full_model.fit(autonomous._feature_subset_matrix(xf, g), yf)
    idx = [autonomous.FEATURE_INDEX[n] for n in g['feature_names'] if n in autonomous.FEATURE_INDEX]
    fm = xf[:, idx] if idx else xf
    cid = 'V56C_' + hashlib.sha256(json.dumps({'g': g, 'freeze': freeze, 'parent': parent.get('strategy_id')}, sort_keys=True, default=_jd).encode()).hexdigest()[:14].upper()
    return {'challenger_id': cid, 'parent_id': str(parent.get('strategy_id')), 'freeze_ts': freeze,
            'genome': g, 'gate_thresholds': thresholds, 'threshold': float(threshold),
            'model_blob': pickle.dumps(full_model, pickle.HIGHEST_PROTOCOL),
            'training_metrics': {'past_only': True, 'future_validation_not_seen': True,
                'precheck_fills': int(st['fills']), 'precheck_ev_r': float(st['ev']), 'precheck_pf': float(st['pf']),
                'precheck_dd_r': float(st['dd']), 'training_rows': int(len(yf)),
                'feature_median': np.median(fm, axis=0).astype(float).tolist(),
                'feature_q1': np.quantile(fm, .25, axis=0).astype(float).tolist(),
                'feature_q3': np.quantile(fm, .75, axis=0).astype(float).tolist()}}


def _maybe_create_challengers(core: Any, autonomous: Any) -> int:
    if not _ONLINE_LOCK.acquire(blocking=False):
        return 0
    try:
        last = int(core.get_state('v56_last_online_research_at', 0) or 0)
        if _now() - last < ONLINE_RESEARCH_INTERVAL_SECONDS:
            return 0
        con = core.db()
        try:
            tape_n = int(con.execute(f'SELECT COUNT(*) FROM {FEATURE_TABLE}').fetchone()[0] or 0)
            active_n = int(con.execute(f"SELECT COUNT(*) FROM {CHALLENGER_TABLE} WHERE status='FORWARD_VALIDATING'").fetchone()[0] or 0)
        finally:
            con.close()
        if tape_n < ONLINE_MIN_TAPE or active_n >= ONLINE_MAX_ACTIVE_CHALLENGERS:
            return 0
        champions = list(autonomous._load_registry(core, active_only=True) or [])
        if not champions:
            return 0
        rng = random.Random(int(hashlib.sha256(f'v56|{tape_n}|{last}'.encode()).hexdigest()[:12], 16))
        created = 0; attempts = 0; budget = min(ONLINE_CANDIDATES_PER_CYCLE, ONLINE_MAX_ACTIVE_CHALLENGERS - active_n)
        while created < budget and attempts < budget * 4:
            attempts += 1; parent = rng.choice(champions)
            g = _canonical_genome(autonomous._new_genome(rng, _d(parent.get('genome'))))
            result = _challenger_precheck(core, autonomous, parent, g, rng.randrange(0, 2**32 - 1))
            if not result:
                continue
            con = core.db()
            try:
                before = con.total_changes
                con.execute(f'''INSERT OR IGNORE INTO {CHALLENGER_TABLE}
                    (challenger_id,parent_id,created_at,freeze_ts,status,genome,gate_thresholds,threshold,model,training_metrics)
                    VALUES(?,?,?,?,?,?,?,?,?,?)''', (result['challenger_id'], result['parent_id'], _now(), result['freeze_ts'],
                    'FORWARD_VALIDATING', json.dumps(result['genome'], sort_keys=True, separators=(',', ':'), default=_jd),
                    json.dumps(result['gate_thresholds'], sort_keys=True, separators=(',', ':'), default=_jd),
                    result['threshold'], result['model_blob'],
                    json.dumps(result['training_metrics'], sort_keys=True, separators=(',', ':'), default=_jd))); con.commit()
                created += int(con.total_changes > before)
            finally:
                con.close()
        core.set_state('v56_last_online_research_at', _now()); return created
    finally:
        _ONLINE_LOCK.release()


def _challenger_stats(core: Any, cid: str) -> dict[str, float]:
    con = core.db()
    try:
        rows = con.execute(f'''SELECT result_r FROM {OBS_TABLE} WHERE strategy_id=? AND source='CHALLENGER'
            AND status='SETTLED' AND filled=1 ORDER BY decision_ts''', (str(cid),)).fetchall()
    finally:
        con.close()
    pnls = [float(r[0]) for r in rows if r[0] is not None]
    if not pnls:
        return {'fills': 0.0, 'ev': 0.0, 'pf': 0.0, 'dd': 0.0, 'profitable_halves': 0.0}
    gains = sum(max(x, 0.0) for x in pnls); losses = sum(max(-x, 0.0) for x in pnls); eq = peak = dd = 0.0
    for p in pnls:
        eq += p; peak = max(peak, eq); dd = max(dd, peak - eq)
    mid = len(pnls) // 2; halves = [pnls[:mid], pnls[mid:]] if mid else [pnls]
    ph = sum(bool(h) and statistics.mean(h) > 0 for h in halves) / len(halves)
    return {'fills': float(len(pnls)), 'ev': float(statistics.mean(pnls)), 'pf': float(gains / max(losses, 1e-9)),
            'dd': float(dd), 'profitable_halves': float(ph)}


def _promote_challengers(core: Any, autonomous: Any) -> int:
    promoted = 0
    for cid, item in _load_challengers(core).items():
        if item.get('status') != 'FORWARD_VALIDATING':
            continue
        st = _challenger_stats(core, cid)
        if int(st['fills']) < ONLINE_VALIDATION_MIN_FILLS:
            continue
        if not (st['pf'] >= float(autonomous.MIN_OOS_PF) and st['ev'] >= float(autonomous.MIN_OOS_EV_R) and
                st['dd'] <= float(autonomous.MAX_OOS_DD_R) and st['profitable_halves'] >= 1.0):
            continue
        g = _canonical_genome(_d(item.get('genome'))); sid = 'AUTO_ONLINE_' + autonomous._hash_payload(g, 12).upper()
        tm = _d(item.get('training_metrics'))
        metrics = {**tm, 'schema': SCHEMA, 'strategy_id': sid,
            'behavior_label': autonomous._behavior_label(g, list(item.get('gate_thresholds') or [])),
            'validation_method': 'CURRENT_TIME_PAST_ONLY_PRECHECK_THEN_FUTURE_ONLY_FORWARD_OOS',
            'historical_no_lookahead': True, 'online_future_only_validation': True, 'source': 'ONLINE_FORWARD_OOS',
            'oos_fills': int(st['fills']), 'profit_factor': float(st['pf']), 'expectancy_r': float(st['ev']),
            'max_drawdown_r': float(st['dd']), 'profitable_folds': float(st['profitable_halves']),
            'direct_r_threshold': float(item['threshold']), 'gate_thresholds': list(item.get('gate_thresholds') or []),
            'feature_names': list(g.get('feature_names') or []),
            'reason': 'online challenger passed future-only current-time validation', 'execution_semantics': VERSION}
        con = core.db()
        try:
            con.execute(f'''INSERT OR REPLACE INTO {autonomous.REGISTRY_TABLE}
                (strategy_id,created_at,status,direction,behavior_label,genome,metrics,model,active)
                VALUES(?,?,?,?,?,?,?,?,1)''', (sid, _now(), 'CHAMPION', g['direction'], metrics['behavior_label'],
                json.dumps(g, separators=(',', ':'), default=_jd),
                json.dumps(metrics, separators=(',', ':'), ensure_ascii=False, default=_jd), item['model_blob']))
            con.execute(f"UPDATE {CHALLENGER_TABLE} SET status='PROMOTED',promoted_strategy_id=? WHERE challenger_id=?", (sid, cid)); con.commit()
        finally:
            con.close()
        promoted += 1
    if promoted:
        _archive_champions(core, autonomous, 'ONLINE_FORWARD_OOS')
    return promoted


async def _current_learning_tick(core: Any, autonomous: Any, base_tick: Any) -> None:
    cp = _d(core.get_state(autonomous.CHECKPOINT_KEY, {})); champions = list(autonomous._load_registry(core, active_only=True) or [])
    if cp.get('status') != 'COMPLETE' or not champions:
        await base_tick(); return
    _archive_champions(core, autonomous); settled = await asyncio.to_thread(_settle_forward, core, autonomous)
    quarantine = _refresh_quarantine(core, autonomous)
    created = await asyncio.to_thread(_maybe_create_challengers, core, autonomous)
    promoted = await asyncio.to_thread(_promote_challengers, core, autonomous)
    # No historical Stage-6/replay restart after handoff. Current forward research owns
    # this lightweight tick and all new evidence remains time-separated.
    health = {'phase': 'CURRENT_PAPER_FORWARD_LEARNING', 'historical_oos_frozen': True,
        'historical_replay_mutated': False, 'forward_settlement': settled,
        'quarantined': sum(bool(_d(v).get('active')) for v in quarantine.values()),
        'online_challengers_created': int(created), 'online_champions_promoted': int(promoted),
        'continual_learning': True, 'future_only_challenger_validation': True, 'updated_at': _now()}
    core.state['learning'] = {**_d(core.state.get('learning')), **health}; _state(core, **health)


def _authority(core: Any, autonomous: Any) -> dict[str, Any]:
    _ensure_tables(core); champions = list(autonomous._load_registry(core, active_only=True) or []); _archive_champions(core, autonomous)
    con = core.db()
    try:
        tape = int(con.execute(f'SELECT COUNT(*) FROM {FEATURE_TABLE}').fetchone()[0] or 0)
        pending = int(con.execute(f"SELECT COUNT(*) FROM {OBS_TABLE} WHERE status='PENDING'").fetchone()[0] or 0)
        settled = int(con.execute(f"SELECT COUNT(*) FROM {OBS_TABLE} WHERE status='SETTLED'").fetchone()[0] or 0)
        chal = int(con.execute(f"SELECT COUNT(*) FROM {CHALLENGER_TABLE} WHERE status='FORWARD_VALIDATING'").fetchone()[0] or 0)
        online = int(con.execute(f"SELECT COUNT(*) FROM {CHALLENGER_TABLE} WHERE status='PROMOTED'").fetchone()[0] or 0)
        archived = int(con.execute(f'SELECT COUNT(*) FROM {ARCHIVE_TABLE}').fetchone()[0] or 0)
    finally:
        con.close()
    current = _d(core.state.get('analysis')); sel = _d(current.get('selection'))
    candidates = [{'strategy': x.get('strategy'), 'direction': x.get('direction'), 'tradeable': bool(x.get('tradeable')),
                   'predicted_ev_r': x.get('predicted_ev_r'), 'required_ev_r': x.get('required_ev_r', x.get('threshold')),
                   'edge_r': x.get('edge_r'), 'reason': x.get('reason'), 'quarantined': bool(x.get('quarantined'))}
                  for x in list(current.get('autonomous_candidates') or [])]
    cp = _d(core.get_state(autonomous.CHECKPOINT_KEY, {}))
    return {'schema': SCHEMA, 'runtime': VERSION,
        'historical': {'terminal': cp.get('status') == 'COMPLETE', 'fixed_start_ts': int(autonomous.RESEARCH_START_TS),
                       'fixed_end_exclusive_ts': int(autonomous.RESEARCH_END_EXCLUSIVE_TS),
                       'replay_mutated_by_v56_online_learning': False},
        'execution': {'semantics': VERSION, 'market_offset_forced_zero': True, 'stop_anchored_to_actual_fill': True,
                      'stop_before_target_same_bar': True, 'fill_bar_target_credit': False,
                      'trailing_next_bar_only': True, 'impossible_trail_lock_forbidden': True, 'live_5m_processing': True},
        'champions': {'active_count': len(champions), 'archived_count': archived,
                      'target_is_multiple_diverse_not_forced': True,
                      'ids': [str(x.get('strategy_id')) for x in champions], 'quarantine': _quarantine_state(core)},
        'arbiter': current.get('champion_arbitration') or {},
        'current_selection': {'strategy': sel.get('strategy'), 'direction': sel.get('direction'),
                              'predicted_ev_r': sel.get('predicted_ev_r'),
                              'required_ev_r': sel.get('required_ev_r', sel.get('threshold')),
                              'edge_r': sel.get('edge_r'), 'tradeable': bool(sel.get('tradeable')), 'reason': sel.get('reason')},
        'candidates': candidates,
        'forward_learning': {'feature_tape_rows': tape, 'pending_observations': pending,
                             'settled_observations': settled, 'active_challengers': chal,
                             'promoted_online_champions': online, 'historical_oos_frozen': True,
                             'future_only_validation': True},
        'rules': {'no_future_features': True, 'no_historical_oos_rewrite': True, 'weak_strategy_forced_to_pass': False,
                  'single_eth_position_arbiter': True, 'opposite_close_edges_wait_instead_of_fighting': True,
                  'current_mode_continues_learning': True}, 'updated_at': _now()}


def _install_routes_dashboard(core: Any, autonomous: Any) -> None:
    app = core.app; app.router.routes = [r for r in app.router.routes if getattr(r, 'path', None) != '/api/v56/authority']
    app.add_api_route('/api/v56/authority', lambda: _authority(core, autonomous), methods=['GET'], name='v56_authority')
    root = next((r for r in app.router.routes if getattr(r, 'path', None) == '/'), None); old = getattr(root, 'endpoint', None)
    if not callable(old):
        return
    from fastapi.responses import HTMLResponse
    app.router.routes = [r for r in app.router.routes if getattr(r, 'path', None) != '/']
    @app.get('/', response_class=HTMLResponse, name='v56_causal_multichampion_dashboard')
    def dashboard_v56() -> str:
        raw = old(); html = raw.body.decode() if hasattr(raw, 'body') else str(raw)
        card = '''<section class="card"><h2>🧠 V56 真實執行 / 多策略協調 / 現在式學習</h2>
<div id="v56authority" class="notice">讀取 V56 權威狀態…</div>
<details open><summary>查看所有已認證策略、Pred EV、門檻、衝突與 forward learning</summary><pre id="v56detail">—</pre></details></section>'''
        marker = '</div><div class="footer">'; html = html.replace(marker, card + marker, 1) if marker in html else html.replace('</body>', card + '</body>', 1)
        js = r'''<script id="v56-authority-ui">(function(){function E(x){return String(x??'—').replace(/[&<>\"']/g,s=>({'&':'&amp;','<':'&lt;','>':'&gt;','\"':'&quot;',"'":'&#39;'}[s]))}function R(x){let n=Number(x);return Number.isFinite(n)?((n>=0?'+':'')+n.toFixed(3)+'R'):'—'}async function T(){let b=document.getElementById('v56authority');if(!b)return;try{let r=await fetch('/api/v56/authority',{cache:'no-store'}),z=await r.json(),s=z.current_selection||{},f=z.forward_learning||{},a=z.arbiter||{},c=z.champions||{};b.className='notice '+(z.historical?.terminal?'g':'y');b.innerHTML='<b>'+(z.historical?.terminal?'HISTORICAL FROZEN → CURRENT FORWARD LEARNING':'HISTORICAL RESEARCH')+'</b><br>已認證策略：<b>'+Number(c.active_count||0)+'</b>｜Forward tape '+Number(f.feature_tape_rows||0).toLocaleString()+'｜已成熟觀測 '+Number(f.settled_observations||0).toLocaleString()+'｜Challengers '+Number(f.active_challengers||0)+'<br>目前：<b>'+E(s.strategy)+'</b> '+E(s.direction)+'｜Pred EV <b>'+R(s.predicted_ev_r)+'</b>｜Required <b>'+R(s.required_ev_r)+'</b>｜Edge <b>'+R(s.edge_r)+'</b><br>Arbiter：'+E(a.status||'WAIT')+(s.reason?'<br>原因：'+E(s.reason):'');let p=document.getElementById('probTitle'),pv=document.getElementById('prob'),th=document.getElementById('threshold');if(p)p.textContent='預測 EV / 真正入場門檻';if(pv)pv.textContent=R(s.predicted_ev_r);if(th)th.textContent='門檻 '+R(s.required_ev_r)+' · Edge '+R(s.edge_r);let d=document.getElementById('v56detail');if(d)d.textContent=JSON.stringify(z,null,2)}catch(e){b.className='notice r';b.textContent='V56 authority 讀取失敗：'+String(e)}}T();setInterval(T,1500)})();</script>'''
        return html.replace('</body>', js + '</body>', 1) if '</body>' in html else html + js


def preinstall(production: Any, autonomous: Any, integrity: Any, throughput: Any) -> None:
    global _PREINSTALLED
    with _INSTALL_LOCK:
        if _PREINSTALLED:
            return
        _PREINSTALLED = True
    core = production.core; _ensure_tables(core)
    mods = tuple(getattr(integrity, 'SEMANTIC_MODULES', ()))
    if 'v56_causal_multichampion_learning' not in mods:
        integrity.SEMANTIC_MODULES = mods + ('v56_causal_multichampion_learning',)
    # Old Stage-6 products used the broken pre-V56 execution semantics. They are not
    # allowed to survive this semantic change; raw history/replay rows are preserved.
    if not core.get_state(RESET_MARKER, None):
        autonomous._clear_autonomous_products(core); core.set_state(autonomous.CHECKPOINT_KEY, {})
        core.set_state('v49_stage6_outer_cursor', {})
        core.set_state(RESET_MARKER, {'schema': SCHEMA, 'at': _now(), 'raw_market_preserved': True,
            'raw_derivatives_preserved': True, 'historical_replay_rows_preserved': True,
            'old_stage6_products_invalidated': True,
            'reason': 'market-entry/stop/trailing/live-parity semantics corrected'})
    autonomous.RESET_MARKER = RESET_MARKER
    base_new = autonomous._new_genome
    def new_genome(rng: random.Random, parent: dict[str, Any] | None = None):
        return _canonical_genome(base_new(rng, _canonical_genome(parent) if parent else None))
    autonomous._new_genome = new_genome
    autonomous._simulate_trade = lambda market, ts, features, genome: canonical_simulate(core, autonomous, market, int(ts), features, genome)
    autonomous._generic_plan_from_genome = lambda price, atr_pct, genome: _generic_plan(autonomous, price, atr_pct, genome)
    try:
        throughput._install_parallel(core, autonomous)
    except Exception:
        pass
    _state(core, preinstalled=True, semantic_identity_includes_v56=True, old_stage6_products_invalidated=True,
           replay_reset=False, raw_history_deleted=False, future_peeking_enabled=False)


def install(production: Any, autonomous: Any, integrity: Any, throughput: Any) -> None:
    global _INSTALLED, _BASE_ANALYSIS, _BASE_LEARNING_TICK
    with _INSTALL_LOCK:
        if _INSTALLED:
            return
        _INSTALLED = True
    core = production.core; _ensure_tables(core)
    _BASE_ANALYSIS = autonomous._autonomous_analysis
    autonomous._autonomous_analysis = _analysis_factory(core, autonomous, _BASE_ANALYSIS)
    autonomous._autonomous_create_signal = _create_signal_factory(core, autonomous)
    autonomous._autonomous_update_signal = _update_signal_factory(core)
    wrapped_analysis = autonomous._autonomous_analysis
    def analysis_with_challengers(bundle: dict[str, Any]) -> dict[str, Any]:
        z = wrapped_analysis(bundle)
        if z.get('decision_ts'):
            _record_challenger_predictions(core, autonomous, int(z['decision_ts']), _d(z.get('features')))
        return z
    autonomous._autonomous_analysis = analysis_with_challengers
    _BASE_LEARNING_TICK = core.learning_tick
    async def learning_tick_v56():
        await _current_learning_tick(core, autonomous, _BASE_LEARNING_TICK)
    core.learning_tick = learning_tick_v56
    _archive_champions(core, autonomous); _install_routes_dashboard(core, autonomous)
    _state(core, installed=True, status='READY', execution_semantics=VERSION,
           multiple_certified_champions_supported=True, single_position_conflict_arbiter=True,
           current_forward_learning=True, historical_oos_frozen_after_certification=True,
           online_challengers_future_only=True, no_strategy_templates=True, future_peeking_enabled=False)
    role = core.state.get('bootstrap_replica_role')
    if isinstance(role, dict):
        role.update({'research_runtime': 'V56_CAUSAL_MULTICHAMPION_20260818',
            'v56_market_entry_actual_fill_anchor': True, 'v56_impossible_trailing_forbidden': True,
            'v56_historical_live_execution_parity': True, 'v56_multi_champion_single_position_arbiter': True,
            'v56_current_forward_learning': True, 'v56_historical_oos_immutable_after_handoff': True,
            'v56_online_challenger_future_only_validation': True, 'v47_exact_resume_identity_includes_v56': True})
    runtime_identity.stamp(core)
