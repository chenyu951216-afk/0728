from __future__ import annotations

import asyncio
import json
import os
import time
from typing import Any

import httpx

import v5_runtime
import v7_runtime

TRADE_MONITOR_SECONDS = max(3, int(os.getenv('POSITION_MONITOR_SECONDS', '5')))


def _trade_time_seconds(row: dict[str, Any]) -> float:
    raw = row.get('create_time_ms')
    if raw is None: raw = row.get('create_time')
    try: x = float(raw)
    except (TypeError, ValueError): return 0.0
    if x > 1e12: x /= 1000.0
    elif x > 1e10: x /= 1000.0
    return x


def _monitor_start(core: Any, now: int) -> int:
    row = core.latest_signal()
    if not row:
        return now - max(15, TRADE_MONITOR_SECONDS * 3)
    mg = (row.get('payload') or {}).get('management', {})
    last = float(mg.get('monitor_last_trade_time') or 0)
    if last > 0:
        return max(int(last) - 1, now - 120)
    eligible = int(row.get('filled_at') or row.get('created_at') or now)
    return max(eligible, now - 120)


async def fetch_trade_events(core: Any) -> list[dict[str, Any]]:
    now = int(time.time()); start = _monitor_start(core, now)
    async with httpx.AsyncClient(timeout=12) as client:
        try:
            raw, ticker = await asyncio.gather(
                core.hub._json(client, core.hub.GATE + '/futures/usdt/trades', {'contract': 'ETH_USDT', 'from': start, 'to': now, 'limit': 1000}),
                core.hub._json(client, core.hub.GATE + '/futures/usdt/tickers', {'contract': 'ETH_USDT'}),
            )
            events = []
            for x in raw or []:
                if not isinstance(x, dict): continue
                px = float(x.get('price') or 0)
                if px <= 0: continue
                events.append({'kind': 'trade', 'trade_id': int(x.get('id') or 0), 'time': _trade_time_seconds(x), 'price': px, 'source': 'gate-trades'})
            events.sort(key=lambda x: (x['time'], x['trade_id']))
            if isinstance(ticker, list): ticker = ticker[0] if ticker else {}
            last = float((ticker or {}).get('last') or 0)
            if last > 0: events.append({'kind': 'ticker', 'trade_id': 0, 'time': float(time.time()), 'price': last, 'source': 'gate-ticker'})
            return events
        except Exception as exc:
            core.state['risk_monitor'] = {'ok': False, 'error': f'gate trades feed: {exc}', 'updated_at': now}
        try:
            data = await core.hub._json(client, core.hub.BYBIT + '/v5/market/kline', {'category': 'linear', 'symbol': 'ETHUSDT', 'interval': '1', 'limit': 1}); rows = ((data or {}).get('result') or {}).get('list') or []
            if rows:
                px = float(rows[0][4]); return [{'kind': 'ticker', 'trade_id': 0, 'time': float(time.time()), 'price': px, 'source': 'bybit-last-fallback'}]
        except Exception as exc:
            core.state['risk_monitor'] = {'ok': False, 'error': f'all trade feeds failed: {exc}', 'updated_at': now}
    return []


def _persist_payload(core: Any, row: dict[str, Any], payload: dict[str, Any], current_stop: float | None = None) -> None:
    con = core.db()
    if current_stop is None: con.execute('UPDATE signals SET updated_at=?,payload=? WHERE signal_id=?', (int(time.time()), json.dumps(payload, ensure_ascii=False), row['signal_id']))
    else: con.execute('UPDATE signals SET updated_at=?,current_stop=?,payload=? WHERE signal_id=?', (int(time.time()), current_stop, json.dumps(payload, ensure_ascii=False), row['signal_id']))
    con.commit(); con.close()


def process_trade_event(core: Any, event: dict[str, Any]) -> dict[str, Any] | None:
    row = core.latest_signal()
    if not row: return None
    payload = row['payload']; mgmt = payload.setdefault('management', {}); event_time = float(event.get('time') or time.time()); price = float(event.get('price') or 0)
    if price <= 0: return row
    if event.get('kind') == 'trade':
        last_time = float(mgmt.get('monitor_last_trade_time') or 0); last_id = int(mgmt.get('monitor_last_trade_id') or 0); trade_id = int(event.get('trade_id') or 0)
        if event_time < last_time or (event_time == last_time and trade_id <= last_id): return row
    eligible_since = float(row['created_at']) if row['status'] == 'PLANNED' else float(row.get('filled_at') or row['created_at'])
    if event_time < eligible_since: return row
    entry = float(row['entry']); direction = row['direction']; policy = payload.get('execution_policy') or {}
    if row['status'] == 'PLANNED':
        expire_bars = int(policy.get('expire_bars') or 6)
        if time.time() - float(row['created_at']) > expire_bars * 900:
            con = core.db(); con.execute("UPDATE signals SET status='EXPIRED',updated_at=? WHERE signal_id=?", (int(time.time()), row['signal_id'])); con.commit(); con.close(); return None
        touched = price <= entry if direction == 'LONG' else price >= entry
        if not touched:
            if event.get('kind') == 'trade': mgmt['monitor_last_trade_time'] = event_time; mgmt['monitor_last_trade_id'] = int(event.get('trade_id') or 0); _persist_payload(core, row, payload)
            return row
        fill_ts = int(event_time); con = core.db(); con.execute("UPDATE signals SET status='OPEN',filled_at=?,updated_at=? WHERE signal_id=?", (fill_ts, int(time.time()), row['signal_id'])); con.commit(); con.close(); row = core.latest_signal(('OPEN',)) or row; payload = row['payload']; mgmt = payload.setdefault('management', {})
    entry = float(row['entry']); stop0 = float(row['initial_stop']); current_stop = float(row['current_stop']); targets = row['targets']; risk = abs(entry - stop0) or 1e-9; sign = 1 if direction == 'LONG' else -1
    favorable = (price - entry) * sign / risk; adverse = (entry - price) * sign / risk; mgmt['mfe_r'] = max(float(mgmt.get('mfe_r', 0)), favorable); mgmt['mae_r'] = max(float(mgmt.get('mae_r', 0)), adverse)
    if event.get('kind') == 'trade': mgmt['monitor_last_trade_time'] = event_time; mgmt['monitor_last_trade_id'] = int(event.get('trade_id') or 0)
    mgmt['monitor_source'] = event.get('source')
    stop_hit = price <= current_stop if direction == 'LONG' else price >= current_stop
    if stop_hit: v7_runtime.close_signal_v7(core, row, current_stop, 'STOP_OR_TRAIL', int(event_time)); return None
    hit_targets = set(mgmt.get('hit_targets') or []); remaining, partial = v7_runtime._infer_partial(row, payload)
    for idx, target in enumerate(targets):
        if idx in hit_targets: continue
        tp = float(target['price']); hit = price >= tp if direction == 'LONG' else price <= tp
        if not hit: continue
        frac = min(remaining, float(target.get('allocation') or 0) / 100.0); partial += frac * float(target.get('rr') or 0); remaining -= frac; hit_targets.add(idx); mgmt.setdefault('realized_legs', []).append({'target': idx + 1, 'fraction': frac, 'rr': float(target.get('rr') or 0), 'ts': int(event_time), 'trade_id': event.get('trade_id')})
    mgmt['hit_targets'] = sorted(hit_targets); mgmt['remaining_fraction'] = max(0.0, remaining); mgmt['realized_partial_r'] = partial; new_stop = current_stop
    if 0 in hit_targets: new_stop = max(new_stop, entry) if direction == 'LONG' else min(new_stop, entry); mgmt['trail_reason'] = 'TP1 -> breakeven'
    if 1 in hit_targets:
        lock2 = float(policy.get('lock_after_tp2_r') or .55); locked = entry + sign * lock2 * risk; new_stop = max(new_stop, locked) if direction == 'LONG' else min(new_stop, locked); mgmt['trail_reason'] = f'TP2 -> lock {lock2:.2f}R'
    if 2 in hit_targets:
        lock3 = float(policy.get('lock_after_tp3_r') or 1.05); locked = entry + sign * lock3 * risk; new_stop = max(new_stop, locked) if direction == 'LONG' else min(new_stop, locked); mgmt['trail_reason'] = f'TP3 -> lock {lock3:.2f}R'
    new_stop = max(new_stop, current_stop) if direction == 'LONG' else min(new_stop, current_stop)
    if remaining <= 1e-9: v7_runtime.close_signal_v7(core, row, float(targets[-1]['price']), 'ALL_TARGETS', int(event_time)); return None
    _persist_payload(core, row, payload, new_stop); return core.latest_signal()


async def _notify_transition(core: Any, previous: dict[str, Any], current: dict[str, Any] | None) -> bool:
    signal_id = previous['signal_id']; current = current or v7_runtime._signal_by_id(core, signal_id)
    if not current: return False
    if previous['status'] == 'PLANNED' and current['status'] == 'OPEN':
        await v5_runtime.robust_send_discord(core, '📥 ETH v7 逐筆成交確認入場', v7_runtime._summary(core, current), 0x3498DB)
    old_hits = set((previous.get('payload') or {}).get('management', {}).get('hit_targets', [])); new_hits = set((current.get('payload') or {}).get('management', {}).get('hit_targets', []))
    if current['status'] == 'OPEN':
        for idx in sorted(new_hits - old_hits):
            mg = (current.get('payload') or {}).get('management', {}); await v5_runtime.robust_send_discord(core, f"🎯 ETH v7 TP{idx+1} 逐筆成交確認", v7_runtime._summary(core, current) + f"\n已實現 `{float(mg.get('realized_partial_r') or 0):+.2f}R`｜剩餘 `{float(mg.get('remaining_fraction') or 0):.0%}`。", 0x2ECC71)
    if current['status'] == 'CLOSED' and previous['status'] in ('PLANNED', 'OPEN'):
        prefix = '入場後立即觸發風控｜' if previous['status'] == 'PLANNED' else ''
        await v5_runtime.robust_send_discord(core, f"🛑 ETH v7 {prefix}{current.get('exit_reason')}", v7_runtime._summary(core, current) + f"\nNet `{float(current.get('realized_r') or 0):+.2f}R`｜出場 `{float(current.get('exit_price') or 0):,.2f}`\n同方向再入場進入 cooldown + 新結構 reset。", 0x2ECC71 if float(current.get('realized_r') or 0) >= 0 else 0xE74C3C)
        return True
    return False


async def monitor_trades(core: Any) -> None:
    initial = core.latest_signal()
    if not initial: core.state['risk_monitor'] = {'ok': True, 'active': False, 'updated_at': int(time.time()), 'interval_seconds': TRADE_MONITOR_SECONDS, 'source': 'gate-trades'}; return
    signal_id = initial['signal_id']; events = await fetch_trade_events(core); closed = False
    for event in events:
        previous = v7_runtime._signal_by_id(core, signal_id)
        if not previous or previous['status'] not in ('PLANNED', 'OPEN'): break
        process_trade_event(core, event)
        current = v7_runtime._signal_by_id(core, signal_id)
        if await _notify_transition(core, previous, current):
            closed = True; break
    current = v7_runtime._signal_by_id(core, signal_id); core.state['risk_monitor'] = {'ok': bool(events), 'active': bool(current and current['status'] in ('PLANNED','OPEN')), 'signal_id': signal_id, 'updated_at': int(time.time()), 'interval_seconds': TRADE_MONITOR_SECONDS, 'source': events[-1].get('source') if events else None, 'closed_this_cycle': closed}


async def scan_worker_trade_monitor(core: Any) -> None:
    next_scan = 0.0; await v7_runtime.maybe_boot_notice(core)
    while True:
        try:
            now = time.time()
            if now >= next_scan: await v7_runtime.scan_v7(core); next_scan = now + core.SCAN_SECONDS
            await monitor_trades(core); await v5_runtime.poll_discord_commands(core)
        except Exception as exc: core.LOG.exception('v7 trade monitor worker failed'); core.state.update(service='DEGRADED', error=str(exc))
        await asyncio.sleep(TRADE_MONITOR_SECONDS)


def install(core: Any) -> None:
    async def worker() -> None: await scan_worker_trade_monitor(core)
    core.scan_worker = worker; core.state['risk_monitor_mode'] = 'GATE_PUBLIC_TRADES_ORDERED'
    if not any(getattr(r,'path',None)=='/api/v7/trade-monitor' for r in core.app.router.routes):
        @core.app.get('/api/v7/trade-monitor')
        def trade_monitor_status() -> dict[str, Any]: return {'mode':'GATE_PUBLIC_TRADES_ORDERED','interval_seconds':TRADE_MONITOR_SECONDS,'state':core.state.get('risk_monitor')}
