from __future__ import annotations

"""Runtime hardening and live-parity bridge for V30 autonomous discovery.

This layer does not add trading templates. It restores the production safety gates
that an autonomous create-signal path must still obey, keeps live paper management
identical to the frozen package, isolates live outcomes from historical replay rows,
and throttles heavy discovery before cgroup memory pressure can become an OOM loop.
"""

import gc
import json
import os
import time
from pathlib import Path
from typing import Any

import v7_runtime
import v8_storage_guard
import v9_live_parity
import v12_clean_baseline
import v17_certification_orchestrator as cert17
import v18_final_system as final_system
import v18_operational_guard as operational_guard
import v5_runtime

SCHEMA = 31
STATE_KEY = 'v31_autonomous_runtime_hardening'
LIVE_TABLE = 'autonomous_live_outcomes_v30'
MEMORY_SOFT = max(.55, min(.82, float(os.getenv('AUTONOMOUS_MEMORY_SOFT_RATIO', '.70'))))
MEMORY_HARD = max(MEMORY_SOFT + .05, min(.94, float(os.getenv('AUTONOMOUS_MEMORY_HARD_RATIO', '.86'))))
_INSTALLED = False


def _memory() -> dict[str, Any]:
    current = limit = rss = None
    try:
        p = Path('/sys/fs/cgroup/memory.current'); q = Path('/sys/fs/cgroup/memory.max')
        if p.exists(): current = int(p.read_text().strip())
        if q.exists():
            raw = q.read_text().strip(); limit = None if raw == 'max' else int(raw)
    except Exception: pass
    try:
        pages = int(Path('/proc/self/statm').read_text().split()[1]); rss = pages * int(os.sysconf('SC_PAGE_SIZE'))
    except Exception: pass
    return {'current_bytes': current, 'limit_bytes': limit, 'rss_bytes': rss, 'ratio': (current / max(limit, 1)) if current is not None and limit else None, 'soft_ratio': MEMORY_SOFT, 'hard_ratio': MEMORY_HARD}


def _trim() -> dict[str, Any]:
    collected = int(gc.collect())
    try:
        import ctypes
        fn = getattr(ctypes.CDLL(None), 'malloc_trim', None)
        if fn is not None: fn(0)
    except Exception: pass
    return {'gc_collected': collected, 'memory': _memory()}


def _memory_guard(core: Any, stage: str) -> None:
    mem = _memory(); ratio = float(mem.get('ratio') or 0.0)
    if ratio >= MEMORY_SOFT:
        result = _trim(); mem = result['memory']; ratio = float(mem.get('ratio') or 0.0)
        core.state['autonomous_memory_guard'] = {'stage': stage, 'status': 'TRIMMED', **mem, 'updated_at': int(time.time())}
    if ratio >= MEMORY_HARD:
        # The V26 background authority catches MemoryError and backs off without
        # deleting replay/raw data. Failing closed is safer than an OOM restart loop.
        core.state['autonomous_memory_guard'] = {'stage': stage, 'status': 'HARD_LIMIT_BACKOFF', **mem, 'updated_at': int(time.time())}
        raise MemoryError(f'autonomous research memory ratio {ratio:.3f} >= hard limit {MEMORY_HARD:.3f}')


def _ensure_live_table(core: Any) -> None:
    con = core.db()
    try:
        con.execute(f'''CREATE TABLE IF NOT EXISTS {LIVE_TABLE}(
            signal_id TEXT PRIMARY KEY,
            created_at INTEGER NOT NULL,
            exit_ts INTEGER NOT NULL,
            strategy_id TEXT NOT NULL,
            direction TEXT NOT NULL,
            realized_r REAL NOT NULL,
            payload TEXT NOT NULL
        )'''); con.commit()
    finally: con.close()


def _persistent_ready(core: Any) -> tuple[bool, dict[str, Any]]:
    status = v8_storage_guard.storage_status(core, update_identity=False)
    return bool(status.get('healthy')), status


def _research_gate(core: Any) -> tuple[bool, str, dict[str, Any]]:
    ok, storage = _persistent_ready(core)
    if not ok: return False, f"persistent storage not healthy: {storage.get('reason')}", storage
    if not v12_clean_baseline._is_clean(core): return False, 'Final Clean Baseline is not CLEAN', storage
    return True, 'storage + dataset provenance ready', storage


def _autonomous_reentry_gate(core: Any, analysis: dict[str, Any], _m15: list[dict[str, Any]]) -> dict[str, Any]:
    sel = analysis.get('selection') or {}; genome = sel.get('genome') or {}
    if not sel.get('tradeable') or str(sel.get('direction') or '') not in ('LONG', 'SHORT'):
        return {'allowed': False, 'reason': 'no certified autonomous package is tradeable'}
    cooldown = max(0, int(genome.get('cooldown_bars') or 0)); now = int(time.time()); strategy = str(sel.get('strategy') or '')
    con = core.db()
    try:
        row = con.execute("SELECT exit_ts,realized_r FROM signals WHERE status='CLOSED' AND strategy=? AND direction=? ORDER BY exit_ts DESC LIMIT 1", (strategy, sel['direction'])).fetchone()
    finally: con.close()
    if row and cooldown > 0:
        elapsed = now - int(row[0] or 0); need = cooldown * 900
        if elapsed < need: return {'allowed': False, 'reason': f'AI-evolved cooldown: {cooldown}x15m', 'seconds_remaining': need - elapsed}
    return {'allowed': True, 'reason': 'autonomous package active; evolved cooldown satisfied', 'cooldown_bars': cooldown}


def _close(core: Any, row: dict[str, Any], price: float, reason: str, ts: int) -> None:
    payload = row['payload'] if isinstance(row.get('payload'), dict) else json.loads(row['payload']); targets = row['targets'] if isinstance(row.get('targets'), list) else json.loads(row['targets']); mgmt = payload.setdefault('management', {})
    entry = float(row['entry']); stop0 = float(row['initial_stop']); risk = abs(entry - stop0) or 1e-9; sign = 1.0 if row['direction'] == 'LONG' else -1.0
    remaining = float(mgmt.get('remaining_fraction', 1.0)); partial = float(mgmt.get('realized_partial_r', 0.0)); exit_r = (float(price) - entry) * sign / risk; gross = partial + remaining * exit_r; cost_r = float(mgmt.get('estimated_cost_r') or ((float(os.getenv('EXECUTION_ALL_IN_COST_BPS', '8.0')) / 10000.0) * entry / risk)); net = gross - cost_r
    mgmt.update({'closed_reason': reason, 'remaining_fraction': 0.0, 'realized_partial_r': partial, 'final_exit_rr': exit_r, 'gross_realized_r': gross, 'estimated_cost_r': cost_r, 'net_realized_r': net})
    con = core.db()
    try:
        con.execute("UPDATE signals SET status='CLOSED',updated_at=?,exit_ts=?,exit_price=?,exit_reason=?,realized_r=?,review_until=?,payload=? WHERE signal_id=?", (ts, ts, float(price), reason, float(net), ts + int(core.POST_EXIT_BARS) * 900, json.dumps(payload, ensure_ascii=False), row['signal_id'])); con.commit()
    finally: con.close()


def _event_update(core: Any, event: dict[str, Any]) -> dict[str, Any] | None:
    row = core.latest_signal()
    if not row: return None
    payload = row['payload']; mgmt = payload.setdefault('management', {}); targets = row['targets']; now = int(event.get('observed_at') or time.time()); start_ts = int(event.get('start_ts') or now); end_ts = int(event.get('end_ts') or now); last = float(event.get('last') or 0.0); low = float(event.get('low') or last); high = float(event.get('high') or last)
    decision_close = int(payload.get('strict_decision_close_ts') or row['created_at'])
    if end_ts <= decision_close: return row
    entry = float(row['entry']); stop0 = float(row['initial_stop']); current_stop = float(row['current_stop']); sign = 1.0 if row['direction'] == 'LONG' else -1.0; risk = abs(entry - stop0) or 1e-9; just_filled = False
    if row['status'] == 'PLANNED':
        expire = int(mgmt.get('expire_bars') or 8) * 900
        if now - decision_close > expire:
            con = core.db(); con.execute("UPDATE signals SET status='EXPIRED',updated_at=? WHERE signal_id=?", (now, row['signal_id'])); con.commit(); con.close(); return None
        market_entry = bool(mgmt.get('entry_market'))
        touched = market_entry or (low <= entry <= high)
        if not touched: return row
        fill_ts = max(decision_close, start_ts); fill_price = float(last if market_entry else entry)
        if market_entry and abs(fill_price - entry) > 1e-12:
            # Keep the historical/live semantics conservative: use the actual observed
            # event price, but preserve the frozen stop distance in R terms.
            frozen_risk = risk; entry = fill_price; stop0 = entry - sign * frozen_risk; current_stop = stop0
            targets = [{'price': entry + sign * frozen_risk * float(t.get('rr') or 0), 'rr': float(t.get('rr') or 0), 'allocation': float(t.get('allocation') or 0)} for t in targets]
        mgmt.setdefault('remaining_fraction', 1.0); mgmt.setdefault('realized_partial_r', 0.0); mgmt['fill_event_start_ts'] = start_ts
        con = core.db()
        try:
            con.execute("UPDATE signals SET status='OPEN',filled_at=?,updated_at=?,entry=?,initial_stop=?,current_stop=?,targets=?,payload=? WHERE signal_id=?", (fill_ts, now, entry, stop0, current_stop, json.dumps(targets), json.dumps(payload, ensure_ascii=False), row['signal_id'])); con.commit()
        finally: con.close()
        just_filled = True; row = core.latest_signal(('OPEN',)) or row; payload = row['payload']; mgmt = payload.setdefault('management', {}); targets = row['targets']; entry = float(row['entry']); stop0 = float(row['initial_stop']); current_stop = float(row['current_stop']); risk = abs(entry - stop0) or 1e-9
    stop_hit = low <= current_stop if sign > 0 else high >= current_stop
    if stop_hit:
        _close(core, row, current_stop, 'AUTONOMOUS_STOP_OR_TRAIL', now); return core.latest_signal()
    remaining = float(mgmt.get('remaining_fraction', 1.0)); partial = float(mgmt.get('realized_partial_r', 0.0)); hit = set(int(x) for x in (mgmt.get('hit_targets') or []))
    if not just_filled:
        for idx, target in enumerate(targets):
            if idx in hit or remaining <= 1e-12: continue
            px = float(target['price']); touched = high >= px if sign > 0 else low <= px
            if touched:
                frac = min(remaining, float(target.get('allocation') or 0.0) / 100.0); partial += frac * float(target.get('rr') or 0.0); remaining -= frac; hit.add(idx)
    fav = (high - entry) / risk if sign > 0 else (entry - low) / risk
    if fav >= float(mgmt.get('breakeven_after_r', 999.0)): current_stop = max(current_stop, entry) if sign > 0 else min(current_stop, entry)
    if fav >= float(mgmt.get('trail_start_r', 999.0)):
        lock = entry + sign * float(mgmt.get('trail_lock_r', 0.0)) * risk; current_stop = max(current_stop, lock) if sign > 0 else min(current_stop, lock)
    mgmt.update({'remaining_fraction': max(0.0, remaining), 'realized_partial_r': partial, 'hit_targets': sorted(hit)})
    if remaining <= 1e-12:
        payload['management'] = mgmt; row['payload'] = payload; row['targets'] = targets; _close(core, row, float(targets[max(hit)]['price']) if hit else last, 'AUTONOMOUS_TARGETS_COMPLETE', now); return core.latest_signal()
    filled_at = int(row.get('filled_at') or decision_close); max_hold = int(mgmt.get('max_hold_bars') or 64) * 900
    if end_ts - filled_at >= max_hold:
        payload['management'] = mgmt; row['payload'] = payload; row['targets'] = targets; _close(core, row, last, 'AUTONOMOUS_TIME_EXIT', now); return core.latest_signal()
    payload['management'] = mgmt; con = core.db()
    try:
        con.execute('UPDATE signals SET current_stop=?,updated_at=?,payload=? WHERE signal_id=?', (float(current_stop), now, json.dumps(payload, ensure_ascii=False), row['signal_id'])); con.commit()
    finally: con.close()
    return core.latest_signal()


def _ingest_live(core: Any) -> int:
    _ensure_live_table(core); con = core.db(); added = 0
    try:
        rows = con.execute("SELECT signal_id,created_at,exit_ts,strategy,direction,realized_r,payload FROM signals WHERE status='CLOSED' AND exit_ts IS NOT NULL ORDER BY exit_ts DESC LIMIT 2000").fetchall()
        for r in rows:
            try: payload = json.loads(r[6]) if isinstance(r[6], str) else dict(r[6] or {})
            except Exception: continue
            if int(payload.get('autonomous_schema') or 0) != 30 or payload.get('autonomous_learning_ingested'): continue
            before = con.total_changes; con.execute(f'INSERT OR IGNORE INTO {LIVE_TABLE}(signal_id,created_at,exit_ts,strategy_id,direction,realized_r,payload) VALUES(?,?,?,?,?,?,?)', (r[0], int(r[1]), int(r[2]), str(r[3]), str(r[4]), float(r[5] or 0.0), json.dumps(payload, ensure_ascii=False))); added += int(con.total_changes > before); payload['autonomous_learning_ingested'] = int(time.time()); con.execute('UPDATE signals SET payload=? WHERE signal_id=?', (json.dumps(payload, ensure_ascii=False), r[0]))
        con.commit()
    finally: con.close()
    return added


def install(production: Any, autonomous: Any) -> None:
    global _INSTALLED
    if _INSTALLED: return
    _INSTALLED = True; core = production.core; _ensure_live_table(core)

    # Broaden the data-supported horizon without creating future decisions. Historical
    # decisions still end 2026-08-01; later raw 5m bars are settlement-only.
    autonomous.HOLD_BARS_15M = (1, 2, 4, 8, 16, 32, 64, 96, 192, 384, 672, 960, 1152)
    autonomous.SETTLEMENT_END_EXCLUSIVE_TS = int(os.getenv('AUTONOMOUS_SETTLEMENT_END_TS', '1786723200'))  # 2026-08-15 00:00 Asia/Taipei

    original_certify = autonomous.autonomous_certify
    def guarded_certify(c: Any, force: bool = False):
        ok, reason, storage = _research_gate(c); c.state['autonomous_storage_gate'] = {'ready': ok, 'reason': reason, 'storage': storage, 'updated_at': int(time.time())}
        if not ok:
            c.state[autonomous.STATE_KEY] = {'schema': autonomous.SCHEMA, 'status': 'WAITING_STORAGE_OR_DATASET_PROVENANCE', 'reason': reason, 'updated_at': int(time.time())}; return []
        _memory_guard(c, 'PRE_AUTONOMOUS_RESEARCH'); return original_certify(c, force)
    autonomous.autonomous_certify = guarded_certify; final_system.certify_and_execute = guarded_certify; operational_guard.certify_and_execute = guarded_certify; cert17.train_v17 = guarded_certify; v5_runtime.train_v5 = guarded_certify; core.train_if_due = lambda force=False: guarded_certify(core, force)

    # Preserve storage + clean-baseline + freshly-closed-15m gates even though the
    # strategy package itself is template-free.
    base_create = autonomous._autonomous_create_signal
    def guarded_create(analysis: dict[str, Any], m15: list[dict[str, Any]]):
        ok, _, _ = _research_gate(core)
        if not ok or not m15: return None
        decision_close = int(m15[-1]['ts']) + 900; now = int(time.time()); age = now - decision_close
        if not (0 <= age <= int(v9_live_parity.MAX_DECISION_AGE_SECONDS)): return None
        aligned = dict(analysis); aligned['price'] = float(m15[-1]['c']); aligned['strict_decision_close_ts'] = decision_close; aligned['decision_bar_ts'] = int(m15[-1]['ts']); created = base_create(core, aligned, m15)
        if not created: return created
        payload = created['payload']; payload['strict_decision_close_ts'] = decision_close; payload['decision_bar_ts'] = int(m15[-1]['ts']); payload['execution_policy'] = dict((aligned.get('selection') or {}).get('genome') or {}); payload.setdefault('management', {}).setdefault('remaining_fraction', 1.0); payload['management'].setdefault('realized_partial_r', 0.0)
        con = core.db(); con.execute('UPDATE signals SET payload=? WHERE signal_id=?', (json.dumps(payload, ensure_ascii=False), created['signal_id'])); con.commit(); con.close(); return core.latest_signal()
    core.create_signal = guarded_create

    # V16's single signal entrance calls this module-level function dynamically.
    v7_runtime.reentry_gate = lambda c, analysis, m15: _autonomous_reentry_gate(c, analysis, m15)
    v7_runtime.update_signal_with_event_v7 = lambda c, event: _event_update(c, event)
    v7_runtime.ingest_completed_live_samples_v7 = lambda c: _ingest_live(c)
    core.ingest_completed_live_samples = lambda: _ingest_live(core)
    # Avoid duplicate 15m lifecycle mutation; the ordered public-trade monitor is the
    # authoritative paper execution clock.
    core.update_signal_with_bar = lambda bar: core.latest_signal()

    original_eval = autonomous._evaluate_candidate
    def stable_eval(*args: Any, **kwargs: Any):
        _memory_guard(core, 'DEVELOPMENT_CANDIDATE'); result = original_eval(*args, **kwargs); _trim(); return result
    autonomous._evaluate_candidate = stable_eval
    original_audit = autonomous._fit_and_audit_finalist
    def stable_audit(*args: Any, **kwargs: Any):
        _memory_guard(core, 'FINALIST_OOS'); result = original_audit(*args, **kwargs); _trim(); return result
    autonomous._fit_and_audit_finalist = stable_audit

    original_status = autonomous.autonomous_status
    def status(c: Any) -> dict[str, Any]:
        out = dict(original_status(c)); out['memory'] = _memory(); out['storage_gate'] = c.state.get('autonomous_storage_gate') or {}; out['live_outcomes_isolated_from_historical_replay'] = True; out['live_execution_clock'] = 'ORDERED_PUBLIC_TRADES'; return out
    autonomous.autonomous_status = status

    core.state[STATE_KEY] = {'schema': SCHEMA, 'installed': True, 'memory_soft_ratio': MEMORY_SOFT, 'memory_hard_ratio': MEMORY_HARD, 'storage_fail_closed': True, 'clean_baseline_required': True, 'fresh_closed_15m_required': True, 'ordered_public_trade_execution_monitor': True, 'partial_tp_live_parity': True, 'live_outcomes_separate_from_historical_replay': True, 'manual_reentry_structure_gate_removed': True, 'updated_at': int(time.time())}
    if not any(getattr(r, 'path', None) == '/api/v31/autonomous-integrity' for r in core.app.router.routes):
        @core.app.get('/api/v31/autonomous-integrity')
        def integrity() -> dict[str, Any]: return {'runtime': autonomous.VERSION, 'rules': core.state.get(STATE_KEY) or {}, 'memory': _memory(), 'storage_gate': core.state.get('autonomous_storage_gate') or {}, 'autonomous': autonomous.autonomous_status(core)}
