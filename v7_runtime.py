from __future__ import annotations

import asyncio
import json
import os
import time
from datetime import datetime
from typing import Any

import httpx

import adaptive_v5 as signal
import execution_v7 as execution
import v5_async_runtime
import v5_runtime

V7_VERSION = '7.0.0-20260809'
MONITOR_SECONDS = max(5, int(os.getenv('POSITION_MONITOR_SECONDS', '10')))
REENTRY_BASE_BARS = max(2, int(os.getenv('REENTRY_COOLDOWN_BARS', '6')))


def _signal_by_id(core: Any, signal_id: str) -> dict[str, Any] | None:
    con = core.db(); row = con.execute('SELECT * FROM signals WHERE signal_id=?', (signal_id,)).fetchone(); con.close()
    if not row: return None
    out = dict(row); out['targets'] = json.loads(out['targets']) if isinstance(out.get('targets'), str) else (out.get('targets') or []); out['payload'] = json.loads(out['payload']) if isinstance(out.get('payload'), str) else (out.get('payload') or {}); return out


def _execution_status(core: Any) -> list[dict[str, Any]]:
    con = core.db(); execution.ExecutionStore(con); rows = con.execute("SELECT strategy,direction,model_version,version,status,created_at,metrics,policy FROM execution_registry_v7 ORDER BY created_at DESC,version DESC").fetchall(); con.close()
    return [{'strategy': r[0], 'direction': r[1], 'model_version': r[2], 'execution_version': r[3], 'status': r[4], 'created_at': r[5], 'metrics': json.loads(r[6]), 'policy': json.loads(r[7])} for r in rows]


def _candidate_execution(core: Any, candidate: dict[str, Any], regime: str) -> dict[str, Any]:
    policy, meta = execution.execution_for_candidate(core, candidate); blocked = set(meta.get('blocked_regimes') or [])
    certified = bool(policy and meta.get('certified') and not meta.get('suspicious_metrics') and regime not in blocked and int(meta.get('schema') or 0) == execution.EXECUTION_SCHEMA)
    return {'certified': certified, 'policy': policy, 'metrics': meta, 'regime_ok': bool(policy and regime not in blocked), 'reason': meta.get('reason') if meta else 'no point-in-time execution Champion for this signal-model version'}


def choose_strategy_v7(core: Any, store: Any, learner: Any, features: dict[str, float], regime: dict[str, Any], data_quality: float) -> dict[str, Any]:
    base = signal.choose_strategy(store, learner, features, regime, data_quality); candidates = []
    for raw in base.get('candidates') or []:
        c = dict(raw); model_meta = (c.get('model') or {}).get('metrics') or {}; suspicious_signal = bool(float(model_meta.get('profit_factor') or 0) > 4.0 and int(model_meta.get('selected_n') or 0) < 150); signal_ok = bool(c.get('tradeable') and not suspicious_signal)
        ex = _candidate_execution(core, c, regime['regime']) if c.get('certified') else {'certified': False, 'policy': None, 'metrics': {}, 'regime_ok': False, 'reason': 'signal model not certified'}
        c['signal_tradeable'] = signal_ok; c['signal_suspicious'] = suspicious_signal; c['execution'] = ex; c['tradeable'] = bool(signal_ok and ex['certified']); em = ex.get('metrics') or {}
        if c['tradeable']:
            shrunk = float(em.get('shrunk_ev_r') or 0); pf = float(em.get('profit_factor') or 0); c['final_score'] = float(c.get('score') or 0) + min(.08, max(0.0, shrunk) * .10) + min(.04, max(0.0, pf - 1) * .02)
        else: c['final_score'] = float(c.get('score') or 0)
        candidates.append(c)
    candidates.sort(key=lambda x: x.get('final_score', 0), reverse=True); tradeable = [x for x in candidates if x.get('tradeable')]; certified_signal = [x for x in candidates if x.get('certified')]
    if tradeable: selected, reason = tradeable[0], 'signal point-in-time OOS + execution untouched audit both passed'
    elif certified_signal: selected, reason = {**certified_signal[0], 'tradeable': False}, 'signal Champion exists but no clean v7 execution audit is currently eligible'
    else: selected, reason = {**(candidates[0] if candidates else base), 'tradeable': False}, 'no direction-specific signal Champion is certified yet'
    return {**selected, 'tradeable': bool(selected.get('tradeable')), 'certified': bool(selected.get('certified')), 'research_best': candidates[0] if candidates else base.get('research_best'), 'tradeable_candidates': tradeable[:5], 'certified_candidates': certified_signal[:8], 'candidates': candidates, 'reason': reason, 'validation_stack': 'POINT_IN_TIME_SIGNAL_OOF + EXECUTION_DEV/VALIDATION/UNTOUCHED_AUDIT'}


def reentry_gate(core: Any, analysis: dict[str, Any], m15: list[dict[str, Any]]) -> dict[str, Any]:
    selection = analysis.get('selection') or {}; direction = str(selection.get('direction') or ''); strategy = str(selection.get('strategy') or '')
    if direction not in ('LONG', 'SHORT'): return {'allowed': False, 'reason': 'no valid direction'}
    now = int(time.time()); con = core.db(); rows = con.execute("SELECT signal_id,exit_ts,entry,initial_stop,realized_r,regime,strategy,direction,exit_reason FROM signals WHERE status='CLOSED' AND direction=? AND exit_ts>=? AND exit_reason='STOP_OR_TRAIL' ORDER BY exit_ts DESC LIMIT 8", (direction, now - 24 * 3600)).fetchall(); con.close(); losses = [dict(x) for x in rows if float(x['realized_r'] or 0) < 0]
    if not losses: return {'allowed': True, 'reason': 'no recent losing stop in this direction', 'consecutive_losses': 0}
    last = losses[0]; recent12 = [x for x in losses if int(x['exit_ts'] or 0) >= now - 12 * 3600]; consecutive = len(recent12); cooldown_bars = REENTRY_BASE_BARS if consecutive == 1 else REENTRY_BASE_BARS * 2 if consecutive == 2 else 96; elapsed = now - int(last['exit_ts'] or now)
    if elapsed < cooldown_bars * 900: return {'allowed': False, 'reason': f'losing-stop cooldown: {cooldown_bars}x15m bars required', 'seconds_remaining': cooldown_bars * 900 - elapsed, 'consecutive_losses': consecutive, 'last_stop_signal_id': last['signal_id']}
    if consecutive >= 3: return {'allowed': False, 'reason': 'three losing stops in same direction within 12h -> 24h quarantine', 'consecutive_losses': consecutive, 'last_stop_signal_id': last['signal_id']}
    features = analysis.get('features') or {}; current_regime = str((analysis.get('regime') or {}).get('regime') or ''); price = float(analysis.get('price') or 0); a = max(signal.atr(m15), price * .001) if m15 and price > 0 else 0.0; last_entry = float(last.get('entry') or 0)
    if direction == 'LONG': reset = bool(float(features.get('bos_up') or 0) > 0 or float(features.get('sweep_low') or 0) > 0 or current_regime != str(last.get('regime') or '') or (a > 0 and price >= last_entry + .50 * a))
    else: reset = bool(float(features.get('bos_down') or 0) > 0 or float(features.get('sweep_high') or 0) > 0 or current_regime != str(last.get('regime') or '') or (a > 0 and price <= last_entry - .50 * a))
    if not reset: return {'allowed': False, 'reason': 'cooldown elapsed but no new BOS/sweep/regime reset after the losing stop', 'consecutive_losses': consecutive, 'last_stop_signal_id': last['signal_id']}
    return {'allowed': True, 'reason': 'cooldown elapsed and a new structural reset is present', 'consecutive_losses': consecutive, 'last_stop_signal_id': last['signal_id'], 'strategy': strategy}


def _load_structure_context(core: Any) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    s30 = core._best_source('ETH', '30m'); s1h = core._best_source('ETH', '1h'); m30 = core.load_bars('ETH', '30m', s30, limit=300) if s30 else []; h1 = core.load_bars('ETH', '1h', s1h, limit=300) if s1h else []; return m30, h1


def create_signal_v7(core: Any, analysis: dict[str, Any], m15: list[dict[str, Any]]) -> dict[str, Any] | None:
    selection = analysis.get('selection') or {}
    if not selection.get('tradeable') or core.latest_signal(): return None
    gate = reentry_gate(core, analysis, m15); analysis['reentry_gate'] = gate
    if not gate.get('allowed'): return None
    ex = selection.get('execution') or {}; policy, metrics = ex.get('policy'), ex.get('metrics') or {}
    if not policy or not ex.get('certified') or int(metrics.get('schema') or 0) != execution.EXECUTION_SCHEMA: return None
    m30, h1 = _load_structure_context(core); plan = execution.plan_from_policy(selection['strategy'], selection['direction'], float(analysis['price']), m15, policy, m30, h1)
    plan['execution_validation'] = {'certified': True, 'schema': execution.EXECUTION_SCHEMA, 'execution_version': metrics.get('execution_version'), 'model_version': metrics.get('model_version'), 'audit_pf': metrics.get('profit_factor'), 'audit_ev_r': metrics.get('expectancy_r'), 'audit_ev_ci05_r': metrics.get('ev_bootstrap_05'), 'audit_win_rate': metrics.get('win_rate'), 'audit_fills': metrics.get('oos_fills'), 'fill_rate': metrics.get('fill_rate'), 'max_drawdown_r': metrics.get('max_drawdown_r'), 'estimated_all_in_cost_bps': metrics.get('estimated_all_in_cost_bps'), 'method': metrics.get('validation_method')}
    now = int(time.time()); signal_id = f"{now}-{selection['strategy'][:4]}-{selection['direction'][0]}"; payload = {'initial_plan': plan, 'selection': selection, 'regime': analysis.get('regime') or {}, 'features': analysis.get('features') or {}, 'data_quality': float((analysis.get('data_quality') or {}).get('score', 0)), 'created_from_snapshot': analysis.get('snapshot_ts'), 'immutable': True, 'model_schema_version': 2, 'execution_schema_version': execution.EXECUTION_SCHEMA, 'execution_policy': policy, 'execution_validation': plan['execution_validation'], 'reentry_gate': gate, 'management': {'hit_targets': [], 'mfe_r': 0.0, 'mae_r': 0.0, 'remaining_fraction': 1.0, 'realized_partial_r': 0.0, 'trail_reason': None, 'monitor_last_event_ts': 0}}
    con = core.db(); con.execute('INSERT INTO signals(signal_id,created_at,updated_at,status,strategy,direction,regime,phase,probability,entry,initial_stop,current_stop,targets,payload) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)', (signal_id, now, now, 'PLANNED', selection['strategy'], selection['direction'], (analysis.get('regime') or {}).get('regime'), (analysis.get('regime') or {}).get('phase'), selection['probability'], plan['entry'], plan['stop'], plan['stop'], json.dumps(plan['targets']), json.dumps(payload, ensure_ascii=False))); con.commit(); con.close(); return core.latest_signal()


def _infer_partial(row: dict[str, Any], payload: dict[str, Any]) -> tuple[float, float]:
    mgmt = payload.setdefault('management', {})
    if 'remaining_fraction' in mgmt and 'realized_partial_r' in mgmt: return float(mgmt['remaining_fraction']), float(mgmt['realized_partial_r'])
    hit = set(mgmt.get('hit_targets') or []); remaining, realized = 1.0, 0.0
    for idx, target in enumerate(row.get('targets') or []):
        if idx in hit:
            frac = min(remaining, float(target.get('allocation') or 0) / 100.0); realized += frac * float(target.get('rr') or 0); remaining -= frac
    mgmt['remaining_fraction'], mgmt['realized_partial_r'] = max(0.0, remaining), realized; return max(0.0, remaining), realized


def close_signal_v7(core: Any, row: dict[str, Any], price: float, reason: str, ts: int) -> None:
    entry, stop0 = float(row['entry']), float(row['initial_stop']); sign = 1 if row['direction'] == 'LONG' else -1; risk = abs(entry - stop0) or 1e-9; payload = row['payload'] if isinstance(row.get('payload'), dict) else json.loads(row['payload'])
    if isinstance(row.get('targets'), str): row['targets'] = json.loads(row['targets'])
    remaining, partial = _infer_partial(row, payload); exit_rr = (float(price) - entry) * sign / risk; gross = partial + remaining * exit_rr; policy = payload.get('execution_policy') or {}; cost_bps = float(policy.get('all_in_cost_bps') or execution.ALL_IN_COST_BPS); cost_r = (cost_bps / 10000.0) * entry / risk; net = gross - cost_r; mgmt = payload.setdefault('management', {}); mgmt.update({'closed_reason': reason, 'remaining_fraction': 0.0, 'realized_partial_r': partial, 'final_exit_rr': exit_rr, 'gross_realized_r': gross, 'estimated_cost_r': cost_r, 'net_realized_r': net})
    con = core.db(); con.execute("UPDATE signals SET status='CLOSED',updated_at=?,exit_ts=?,exit_price=?,exit_reason=?,realized_r=?,review_until=?,payload=? WHERE signal_id=?", (ts, ts, float(price), reason, net, ts + core.POST_EXIT_BARS * 900, json.dumps(payload, ensure_ascii=False), row['signal_id'])); con.commit(); con.close()


def update_signal_with_event_v7(core: Any, event: dict[str, Any]) -> dict[str, Any] | None:
    row = core.latest_signal()
    if not row: return None
    payload = row['payload']; policy = payload.get('execution_policy') or {}; now = int(event.get('observed_at') or time.time()); start_ts, end_ts = int(event.get('start_ts') or now), int(event.get('end_ts') or now); last = float(event.get('last') or 0); low, high = float(event.get('low') or last), float(event.get('high') or last); entry, stop0, current_stop = float(row['entry']), float(row['initial_stop']), float(row['current_stop']); direction = row['direction']; sign = 1 if direction == 'LONG' else -1
    eligible_since = int(row['created_at']) if row['status'] == 'PLANNED' else int(row.get('filled_at') or row['created_at'])
    if end_ts <= eligible_since: return row
    if start_ts < eligible_since: low = high = last
    if row['status'] == 'PLANNED':
        expire_bars = int(policy.get('expire_bars') or 6)
        if now - int(row['created_at']) > expire_bars * 900:
            con = core.db(); con.execute("UPDATE signals SET status='EXPIRED',updated_at=? WHERE signal_id=?", (now, row['signal_id'])); con.commit(); con.close(); return None
        if not (low <= entry <= high): return row
        row['status'] = 'OPEN'; row['filled_at'] = max(eligible_since, start_ts); con = core.db(); con.execute("UPDATE signals SET status='OPEN',filled_at=?,updated_at=? WHERE signal_id=?", (row['filled_at'], now, row['signal_id'])); con.commit(); con.close()
    row = core.latest_signal(('OPEN',)) or row; payload = row['payload']; targets = row['targets']; entry, stop0, current_stop = float(row['entry']), float(row['initial_stop']), float(row['current_stop']); risk = abs(entry - stop0) or 1e-9; favorable = (high - entry) / risk if direction == 'LONG' else (entry - low) / risk; adverse = (entry - low) / risk if direction == 'LONG' else (high - entry) / risk; mgmt = payload.setdefault('management', {}); mgmt['mfe_r'] = max(float(mgmt.get('mfe_r', 0)), favorable); mgmt['mae_r'] = max(float(mgmt.get('mae_r', 0)), adverse); mgmt['monitor_last_event_ts'] = max(int(mgmt.get('monitor_last_event_ts') or 0), start_ts); mgmt['monitor_source'] = event.get('source'); hit_targets = set(mgmt.get('hit_targets') or []); remaining, partial = _infer_partial(row, payload)
    stop_hit = low <= current_stop if direction == 'LONG' else high >= current_stop
    if stop_hit: close_signal_v7(core, row, current_stop, 'STOP_OR_TRAIL', now); return None
    for idx, target in enumerate(targets):
        if idx in hit_targets: continue
        px = float(target['price']); target_hit = high >= px if direction == 'LONG' else low <= px
        if not target_hit: continue
        frac = min(remaining, float(target.get('allocation') or 0) / 100.0); partial += frac * float(target.get('rr') or 0); remaining -= frac; hit_targets.add(idx); mgmt.setdefault('realized_legs', []).append({'target': idx + 1, 'fraction': frac, 'rr': float(target.get('rr') or 0), 'ts': now})
    mgmt['hit_targets'] = sorted(hit_targets); mgmt['remaining_fraction'] = max(0.0, remaining); mgmt['realized_partial_r'] = partial; new_stop = current_stop
    if 0 in hit_targets: new_stop = max(new_stop, entry) if direction == 'LONG' else min(new_stop, entry); mgmt['trail_reason'] = 'TP1 -> breakeven'
    if 1 in hit_targets:
        lock2 = float(policy.get('lock_after_tp2_r') or .55); locked = entry + sign * lock2 * risk; new_stop = max(new_stop, locked) if direction == 'LONG' else min(new_stop, locked); mgmt['trail_reason'] = f'TP2 -> lock {lock2:.2f}R'
    if 2 in hit_targets:
        lock3 = float(policy.get('lock_after_tp3_r') or 1.05); locked = entry + sign * lock3 * risk; new_stop = max(new_stop, locked) if direction == 'LONG' else min(new_stop, locked); mgmt['trail_reason'] = f'TP3 -> lock {lock3:.2f}R'
    new_stop = max(new_stop, current_stop) if direction == 'LONG' else min(new_stop, current_stop)
    if remaining <= 1e-9: close_signal_v7(core, row, float(targets[-1]['price']), 'ALL_TARGETS', now); return None
    con = core.db(); con.execute('UPDATE signals SET updated_at=?,current_stop=?,payload=? WHERE signal_id=?', (now, new_stop, json.dumps(payload, ensure_ascii=False), row['signal_id'])); con.commit(); con.close(); return core.latest_signal()


async def fetch_risk_events(core: Any) -> list[dict[str, Any]]:
    now = int(time.time())
    async with httpx.AsyncClient(timeout=12) as client:
        try:
            raw, ticker = await asyncio.gather(core.hub._json(client, core.hub.GATE + '/futures/usdt/candlesticks', {'contract': 'ETH_USDT', 'interval': '10s', 'limit': 8}), core.hub._json(client, core.hub.GATE + '/futures/usdt/tickers', {'contract': 'ETH_USDT'}))
            if isinstance(ticker, list): ticker = ticker[0] if ticker else {}
            last_now = float((ticker or {}).get('last') or 0); events = []
            for x in raw or []:
                if isinstance(x, dict):
                    ts = int(float(x.get('t') or 0)); events.append({'start_ts': ts, 'end_ts': ts + 10, 'low': float(x.get('l') or x.get('c') or 0), 'high': float(x.get('h') or x.get('c') or 0), 'last': float(x.get('c') or last_now or 0), 'observed_at': now, 'source': 'gate-10s'})
            events.sort(key=lambda x: x['start_ts'])
            if last_now > 0: events.append({'start_ts': now, 'end_ts': now + 1, 'low': last_now, 'high': last_now, 'last': last_now, 'observed_at': now, 'source': 'gate-ticker'})
            return events
        except Exception as gate_exc: core.state['risk_monitor'] = {'ok': False, 'error': f'gate risk feed: {gate_exc}', 'updated_at': now}
        try:
            data = await core.hub._json(client, core.hub.BYBIT + '/v5/market/kline', {'category': 'linear', 'symbol': 'ETHUSDT', 'interval': '1', 'limit': 3}); rows = ((data or {}).get('result') or {}).get('list') or []; events = []
            for x in rows:
                ts = int(float(x[0]) / 1000); events.append({'start_ts': ts, 'end_ts': ts + 60, 'low': float(x[3]), 'high': float(x[2]), 'last': float(x[4]), 'observed_at': now, 'source': 'bybit-1m-fallback'})
            return sorted(events, key=lambda x: x['start_ts'])
        except Exception as exc: core.state['risk_monitor'] = {'ok': False, 'error': f'all risk feeds failed: {exc}', 'updated_at': now}; return []


def _summary(core: Any, row: dict[str, Any]) -> str:
    targets = row.get('targets') or []; t = '｜'.join(f"TP{i+1} {float(x.get('price') or 0):,.2f} ({float(x.get('rr') or 0):.2f}R/{int(x.get('allocation') or 0)}%)" for i, x in enumerate(targets)); payload = row.get('payload') or {}; val = payload.get('execution_validation') or {}
    return f"`{row['direction']}`｜`{row['strategy']}`｜{row['regime']}/{row['phase']}\n機率 `{float(row['probability']):.1%}`｜Entry `{float(row['entry']):,.2f}`｜初始SL `{float(row['initial_stop']):,.2f}`｜目前SL `{float(row['current_stop']):,.2f}`\n{t}\nExecution audit PF `{float(val.get('audit_pf') or 0):.2f}`｜EV `{float(val.get('audit_ev_r') or 0):+.3f}R`｜CI05 `{float(val.get('audit_ev_ci05_r') or 0):+.3f}R`"


async def monitor_active_signal(core: Any) -> None:
    before = core.latest_signal()
    if not before: core.state['risk_monitor'] = {'ok': True, 'active': False, 'updated_at': int(time.time()), 'interval_seconds': MONITOR_SECONDS}; return
    before_copy = json.loads(json.dumps(before, ensure_ascii=False)); events = await fetch_risk_events(core)
    if not events: return
    for event in events:
        if not core.latest_signal(): break
        update_signal_with_event_v7(core, event)
    current = _signal_by_id(core, before_copy['signal_id']); core.state['risk_monitor'] = {'ok': True, 'active': bool(current and current['status'] in ('PLANNED', 'OPEN')), 'signal_id': before_copy['signal_id'], 'updated_at': int(time.time()), 'interval_seconds': MONITOR_SECONDS, 'source': events[-1].get('source')}
    if not current: return
    if before_copy['status'] == 'PLANNED' and current['status'] == 'OPEN': await v5_runtime.robust_send_discord(core, '📥 ETH v7 訊號已成交 / 即時監控啟動', _summary(core, current), 0x3498DB)
    if current['status'] == 'CLOSED' and before_copy['status'] in ('PLANNED', 'OPEN'):
        await v5_runtime.robust_send_discord(core, f"🛑 ETH v7 持倉已結束｜{current.get('exit_reason')}", _summary(core, current) + f"\nNet `{float(current.get('realized_r') or 0):+.2f}R`｜出場 `{float(current.get('exit_price') or 0):,.2f}`\n同方向再入場會先進入 cooldown + 結構 reset 檢查。", 0x2ECC71 if float(current.get('realized_r') or 0) >= 0 else 0xE74C3C)
    if current['status'] == 'OPEN':
        old_hits = set((before_copy.get('payload') or {}).get('management', {}).get('hit_targets', [])); new_hits = set((current.get('payload') or {}).get('management', {}).get('hit_targets', []))
        for idx in sorted(new_hits - old_hits):
            mg = (current.get('payload') or {}).get('management', {}); await v5_runtime.robust_send_discord(core, f"🎯 ETH v7 TP{idx+1} 已觸及", _summary(core, current) + f"\n已實現 `{float(mg.get('realized_partial_r') or 0):+.2f}R`｜剩餘 `{float(mg.get('remaining_fraction') or 0):.0%}`。", 0x2ECC71)


async def scan_v7(core: Any) -> dict[str, Any]:
    bundle = await core.hub.live_bundle(); core.upsert_live_gate(bundle); analysis = core._analysis_from_bundle(bundle); now = int(time.time()); m15 = bundle['eth_15m']; gate = reentry_gate(core, analysis, m15) if analysis.get('selection') else {'allowed': False, 'reason': 'no selection'}; analysis['reentry_gate'] = gate; analysis['runtime_version'] = V7_VERSION
    con = core.db(); con.execute('INSERT INTO snapshots(ts,payload) VALUES(?,?)', (now, json.dumps(analysis, ensure_ascii=False))); con.execute('DELETE FROM snapshots WHERE ts<?', (now - 120 * 86400,)); con.commit(); con.close(); active = core.latest_signal()
    if active is None and analysis.get('selection', {}).get('tradeable') and gate.get('allowed'):
        created = create_signal_v7(core, analysis, m15)
        if created:
            val = (created.get('payload') or {}).get('execution_validation') or {}; await v5_runtime.robust_send_discord(core, '🆕 ETH v7 Point-in-Time 雙認證掛單', _summary(core, created) + f"\nAudit fills `{int(val.get('audit_fills') or 0)}`｜方法 `{val.get('method')}`\n不追價；舊 v6 PF 不再具交易資格。", 0x4C8BF5)
    core.state.update(service='OK', updated_at=datetime.now(core.timezone.utc).isoformat(), error=None, scan_count=core.state['scan_count'] + 1, analysis=analysis, active_signal=core.latest_signal()); return analysis


def ingest_completed_live_samples_v7(core: Any) -> int:
    con = core.db(); con.execute('''CREATE TABLE IF NOT EXISTS live_execution_samples(signal_id TEXT PRIMARY KEY, ts INTEGER NOT NULL, strategy TEXT NOT NULL, direction TEXT NOT NULL, regime TEXT NOT NULL, model_version INTEGER, execution_version INTEGER, probability REAL NOT NULL, realized_r REAL NOT NULL, mfe_r REAL NOT NULL, mae_r REAL NOT NULL, review_label TEXT, payload TEXT NOT NULL)'''); rows = con.execute("SELECT * FROM signals WHERE status='CLOSED' ORDER BY exit_ts").fetchall(); added = 0
    for raw in rows:
        row = dict(raw); payload = json.loads(row['payload']) if isinstance(row['payload'], str) else row['payload']
        if payload.get('v7_live_ingested'): continue
        selection = payload.get('selection') or {}; model = selection.get('model') or {}; val = payload.get('execution_validation') or {}; mg = payload.get('management') or {}
        con.execute('INSERT OR IGNORE INTO live_execution_samples(signal_id,ts,strategy,direction,regime,model_version,execution_version,probability,realized_r,mfe_r,mae_r,review_label,payload) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)', (row['signal_id'], int(row.get('created_at') or 0), row['strategy'], row['direction'], row['regime'], model.get('model_version'), val.get('execution_version'), float(row.get('probability') or 0), float(row.get('realized_r') or 0), max(float(mg.get('mfe_r') or 0), float(row.get('post_mfe_r') or 0)), max(float(mg.get('mae_r') or 0), float(row.get('post_mae_r') or 0)), row.get('review_label'), json.dumps({'execution_validation': val, 'review_label': row.get('review_label')}, ensure_ascii=False)))
        payload['v7_live_ingested'] = int(time.time()); payload['learning_ingested'] = 'V7_SEPARATE_EXECUTION_SAMPLE'; con.execute('UPDATE signals SET payload=? WHERE signal_id=?', (json.dumps(payload, ensure_ascii=False), row['signal_id'])); added += 1
    con.commit(); con.close(); return added


def _champion_signature(core: Any) -> list[tuple[str, str, int]]:
    con = core.db(); rows = con.execute("SELECT strategy,direction,MAX(version) FROM model_registry WHERE status='CHAMPION' AND direction IN ('LONG','SHORT') GROUP BY strategy,direction ORDER BY strategy,direction").fetchall(); con.close(); return [(str(x[0]), str(x[1]), int(x[2])) for x in rows]


async def _notify_execution_results(core: Any, results: list[dict[str, Any]]) -> None:
    for x in results:
        if x.get('status') != 'CHAMPION': continue
        await v5_runtime.robust_send_discord(core, f"🧭 v7 Execution Champion｜{x['strategy']} {x['direction']}", f"Signal 機會全部由 point-in-time OOF 產生；final model 不可回頭預測自己的歷史。\nAudit PF `{float(x.get('profit_factor') or 0):.2f}`｜EV `{float(x.get('expectancy_r') or 0):+.3f}R`｜CI05 `{float(x.get('ev_bootstrap_05') or 0):+.3f}R`\nAudit fills `{int(x.get('oos_fills') or 0)}`｜DD `{float(x.get('max_drawdown_r') or 0):.1f}R`｜成本 `{float(x.get('estimated_all_in_cost_bps') or 0):.1f}bps`。", 0x2ECC71)


async def learning_tick_v7(core: Any) -> None:
    live_added = await asyncio.to_thread(ingest_completed_live_samples_v7, core); replay = v5_runtime._replay_progress(core); now = int(time.time()); last_heavy = int(core.get_state('v7_last_heavy_learning_ts', 0) or 0); need_new_label = int(replay.get('latest_market_ts') or 0) - int(replay.get('cursor_ts') or 0) >= 29 * 900; heavy = not replay.get('complete') or need_new_label or now - last_heavy >= 900
    if heavy:
        await v5_async_runtime.learning_tick_v5_async(core); core.set_state('v7_last_heavy_learning_ts', now)
    else:
        core.state.setdefault('learning', {})['v7_live_execution_samples_added'] = live_added; core.state['learning']['v7_heavy_learning_skipped'] = True; core.state['learning']['v7_next_check_seconds'] = max(0, 900 - (now - last_heavy))
    signature = _champion_signature(core); old_signature = core.get_state('v7_execution_signal_signature', []); registry = _execution_status(core); have_current = {(x['strategy'], x['direction'], int(x['model_version'])) for x in registry if x['status'] == 'CHAMPION'}; need_exec = signature != old_signature or any((s, d, v) not in have_current for s, d, v in signature)
    if signature and need_exec:
        results = await asyncio.to_thread(execution.optimize_all, core, False); core.state['execution_learning'] = {'version': V7_VERSION, 'results': results, 'registry': _execution_status(core)[:50], 'updated_at': datetime.now(core.timezone.utc).isoformat()}; core.set_state('v7_execution_signal_signature', signature); await _notify_execution_results(core, results)


async def maybe_boot_notice(core: Any) -> None:
    if core.get_state('discord_boot_version_v7') == V7_VERSION: return
    ok = await v5_runtime.robust_send_discord(core, '✅ ETH Adaptive AI v7 已啟動', '已停用有洩漏風險的 v6 Execution Champion。v7 使用 point-in-time Signal OOF、獨立 validation/audit、10 秒級持倉監控、多週期結構止損、止損後 cooldown + 結構 reset，且實盤 execution 樣本不再污染 Signal Model。', 0x3498DB)
    if ok: core.set_state('discord_boot_version_v7', V7_VERSION)


async def scan_worker_v7(core: Any) -> None:
    next_scan = 0.0; await maybe_boot_notice(core)
    while True:
        try:
            now = time.time()
            if now >= next_scan: await scan_v7(core); next_scan = now + core.SCAN_SECONDS
            await monitor_active_signal(core); await v5_runtime.poll_discord_commands(core)
        except Exception as exc: core.LOG.exception('v7 live worker failed'); core.state.update(service='DEGRADED', error=str(exc))
        await asyncio.sleep(MONITOR_SECONDS)


def migrate(core: Any) -> None:
    con = core.db(); execution.ExecutionStore(con); con.execute('''CREATE TABLE IF NOT EXISTS live_execution_samples(signal_id TEXT PRIMARY KEY, ts INTEGER NOT NULL, strategy TEXT NOT NULL, direction TEXT NOT NULL, regime TEXT NOT NULL, model_version INTEGER, execution_version INTEGER, probability REAL NOT NULL, realized_r REAL NOT NULL, mfe_r REAL NOT NULL, mae_r REAL NOT NULL, review_label TEXT, payload TEXT NOT NULL)''')
    if core.get_state('v7_migration') != execution.EXECUTION_SCHEMA:
        con.execute('DROP TABLE IF EXISTS learning_samples_mixed_live_archive_v7'); con.execute('''CREATE TABLE learning_samples_mixed_live_archive_v7 AS SELECT ls.* FROM learning_samples ls JOIN signals s ON ls.ts=s.created_at AND ls.strategy=s.strategy AND ls.direction=s.direction'''); con.execute('''DELETE FROM learning_samples WHERE EXISTS (SELECT 1 FROM signals s WHERE learning_samples.ts=s.created_at AND learning_samples.strategy=s.strategy AND learning_samples.direction=s.direction)''')
        if con.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='execution_registry'").fetchone(): con.execute("UPDATE execution_registry SET status='ARCHIVED' WHERE status='CHAMPION'")
        planned = con.execute("SELECT signal_id,payload FROM signals WHERE status='PLANNED'").fetchall()
        for r in planned:
            payload = json.loads(r[1]); payload['superseded_reason'] = 'v7 requires point-in-time execution audit and reentry gate'; con.execute("UPDATE signals SET status='EXPIRED',updated_at=?,payload=? WHERE signal_id=?", (int(time.time()), json.dumps(payload, ensure_ascii=False), r[0]))
        opened = con.execute("SELECT signal_id,payload FROM signals WHERE status='OPEN'").fetchall()
        for r in opened:
            payload = json.loads(r[1]); payload['legacy_v6_open_plan'] = True; payload.setdefault('management', {})['legacy_note'] = 'kept immutable; monitored by v7 but excluded from new signal/execution certification'; con.execute('UPDATE signals SET payload=? WHERE signal_id=?', (json.dumps(payload, ensure_ascii=False), r[0]))
        con.commit(); core.set_state('v7_migration', execution.EXECUTION_SCHEMA); core.set_state('v7_execution_signal_signature', [])
    con.close()


def install(core: Any) -> None:
    migrate(core)
    def chooser(store: Any, learner: Any, features: dict[str, float], regime: dict[str, Any], data_quality: float) -> dict[str, Any]: return choose_strategy_v7(core, store, learner, features, regime, data_quality)
    core.choose_strategy = chooser; core.create_signal = lambda analysis, m15: create_signal_v7(core, analysis, m15); core.update_signal_with_bar = lambda bar: core.latest_signal(); core._close_signal = lambda row, price, reason, ts: close_signal_v7(core, row, price, reason, ts); core.ingest_completed_live_samples = lambda: ingest_completed_live_samples_v7(core)
    async def scan_wrapper() -> dict[str, Any]: return await scan_v7(core)
    async def scan_worker_wrapper() -> None: await scan_worker_v7(core)
    async def learning_tick_wrapper() -> None: await learning_tick_v7(core)
    core.scan = scan_wrapper; core.scan_worker = scan_worker_wrapper; core.learning_tick = learning_tick_wrapper; core.app.version = '7.0.0'; core.state['runtime_version'] = V7_VERSION
    if not any(getattr(r, 'path', None) == '/api/v7/audit' for r in core.app.router.routes):
        @core.app.get('/api/v7/audit')
        def api_v7_audit() -> dict[str, Any]: return {'runtime': V7_VERSION, 'risk_monitor': core.state.get('risk_monitor'), 'execution_learning': core.state.get('execution_learning'), 'validation_rules': {'signal_history': 'purged chronological OOS', 'execution_history': 'point-in-time signal OOF -> dev -> validation -> untouched audit', 'live_monitor': f'Gate 10s/ticker every ~{MONITOR_SECONDS}s; Bybit current 1m fallback', 'reentry': f'{REENTRY_BASE_BARS} bars minimum after losing stop; longer after consecutive losses; new structure reset required', 'live_learning': 'separate live_execution_samples; never mixed into signal labels'}}
    if not any(getattr(r, 'path', None) == '/api/v7/execution' for r in core.app.router.routes):
        @core.app.get('/api/v7/execution')
        def api_v7_execution() -> dict[str, Any]: return {'runtime': V7_VERSION, 'registry': _execution_status(core), 'state': core.state.get('execution_learning', {})}
    if not any(getattr(r, 'path', None) == '/api/v7/execution/train' for r in core.app.router.routes):
        @core.app.post('/api/v7/execution/train')
        async def api_v7_execution_train() -> dict[str, Any]:
            results = await asyncio.to_thread(execution.optimize_all, core, True); core.state['execution_learning'] = {'version': V7_VERSION, 'results': results, 'registry': _execution_status(core)[:50], 'updated_at': datetime.now(core.timezone.utc).isoformat()}; await _notify_execution_results(core, results); return {'runtime': V7_VERSION, 'results': results}
