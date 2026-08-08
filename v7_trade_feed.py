from __future__ import annotations

import time
from typing import Any

import httpx

import v7_trade_monitor as tm

MAX_PAGES_PER_CHUNK = 20
PAGE_SIZE = 1000
CHUNK_SECONDS = 300
MAX_RECONCILE_SECONDS = 12 * 3600


def monitor_start_full(core: Any, now: int) -> int:
    row = core.latest_signal()
    if not row:
        return now - max(15, tm.TRADE_MONITOR_SECONDS * 3)
    mg = (row.get('payload') or {}).get('management', {})
    last = float(mg.get('monitor_last_trade_time') or 0)
    if last > 0:
        return max(0, int(last) - 1)
    return max(0, int(row.get('filled_at') or row.get('created_at') or now))


async def fetch_trade_events_paginated(core: Any) -> list[dict[str, Any]]:
    now = int(time.time()); start = monitor_start_full(core, now); events = []; seen = set(); total_pages = 0
    if now - start > MAX_RECONCILE_SECONDS:
        core.state['risk_feed_probe'] = {'gate_trades_ok': False, 'coverage_complete': False, 'error': f'unreconciled monitor gap exceeds {MAX_RECONCILE_SECONDS // 3600}h', 'from': start, 'to': now, 'checked_at': now}
        return []
    async with httpx.AsyncClient(timeout=15) as client:
        try:
            cursor = start
            while cursor <= now:
                chunk_to = min(now, cursor + CHUNK_SECONDS); complete_chunk = False
                for page in range(MAX_PAGES_PER_CHUNK):
                    raw = await core.hub._json(client, core.hub.GATE + '/futures/usdt/trades', {'contract': 'ETH_USDT', 'from': cursor, 'to': chunk_to, 'limit': PAGE_SIZE, 'offset': page * PAGE_SIZE}); rows = raw or []; total_pages += 1
                    for x in rows:
                        if not isinstance(x, dict): continue
                        trade_id = int(x.get('id') or 0); px = float(x.get('price') or 0); ts = tm._trade_time_seconds(x); key = (trade_id, ts, px)
                        if px <= 0 or key in seen: continue
                        seen.add(key); events.append({'kind': 'trade', 'trade_id': trade_id, 'time': ts, 'price': px, 'source': 'gate-trades'})
                    if len(rows) < PAGE_SIZE:
                        complete_chunk = True; break
                if not complete_chunk:
                    core.state['risk_feed_probe'] = {'gate_trades_ok': False, 'coverage_complete': False, 'error': f'pagination exceeded {MAX_PAGES_PER_CHUNK * PAGE_SIZE} trades in {CHUNK_SECONDS}s chunk', 'from': cursor, 'to': chunk_to, 'checked_at': now}
                    return []
                if chunk_to >= now: break
                cursor = chunk_to + 1
            ticker = await core.hub._json(client, core.hub.GATE + '/futures/usdt/tickers', {'contract': 'ETH_USDT'})
            if isinstance(ticker, list): ticker = ticker[0] if ticker else {}
            last = float((ticker or {}).get('last') or 0); events.sort(key=lambda x: (x['time'], x['trade_id']))
            if last > 0: events.append({'kind': 'ticker', 'trade_id': 0, 'time': float(time.time()), 'price': last, 'source': 'gate-ticker'})
            core.state['risk_feed_probe'] = {'gate_trades_ok': True, 'coverage_complete': True, 'pages': total_pages, 'trades': len(seen), 'from': start, 'to': now, 'reconciled_seconds': now - start, 'checked_at': now}
            return events
        except Exception as exc:
            core.state['risk_feed_probe'] = {'gate_trades_ok': False, 'coverage_complete': False, 'error': f'Gate public trades: {exc}', 'from': start, 'to': now, 'checked_at': now}
        # Fallback last price is visibility only. It may help close an obviously
        # breached active position, but it never authorizes a new signal because
        # risk_feed_probe stays false.
        try:
            data = await core.hub._json(client, core.hub.BYBIT + '/v5/market/kline', {'category': 'linear', 'symbol': 'ETHUSDT', 'interval': '1', 'limit': 1}); rows = ((data or {}).get('result') or {}).get('list') or []
            if rows:
                px = float(rows[0][4]); return [{'kind': 'ticker', 'trade_id': 0, 'time': float(time.time()), 'price': px, 'source': 'bybit-last-fallback'}]
        except Exception as exc:
            core.state.setdefault('risk_feed_probe', {})['fallback_error'] = str(exc)
    return []


async def monitor_with_idle_probe(core: Any) -> None:
    if not core.latest_signal():
        events = await fetch_trade_events_paginated(core); probe = core.state.get('risk_feed_probe') or {}; core.state['risk_monitor'] = {'ok': bool(probe.get('gate_trades_ok') and probe.get('coverage_complete')), 'active': False, 'updated_at': int(time.time()), 'interval_seconds': tm.TRADE_MONITOR_SECONDS, 'source': 'gate-trades' if probe.get('gate_trades_ok') else (events[-1].get('source') if events else None), 'coverage_complete': bool(probe.get('coverage_complete'))}; return
    await _ORIGINAL_MONITOR(core)


_ORIGINAL_MONITOR = tm.monitor_trades


def install(core: Any) -> None:
    tm.fetch_trade_events = fetch_trade_events_paginated
    tm.monitor_trades = monitor_with_idle_probe
    core.state['risk_feed_probe'] = {'gate_trades_ok': False, 'coverage_complete': False, 'checked_at': None, 'reason': 'awaiting first complete ordered-trade probe'}
