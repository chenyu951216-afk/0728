from __future__ import annotations

import time
from typing import Any

import httpx

import v7_trade_monitor as tm

MAX_PAGES = 20
PAGE_SIZE = 1000


async def fetch_trade_events_paginated(core: Any) -> list[dict[str, Any]]:
    now = int(time.time()); start = tm._monitor_start(core, now); events = []; seen = set(); complete = False
    async with httpx.AsyncClient(timeout=12) as client:
        try:
            for page in range(MAX_PAGES):
                raw = await core.hub._json(client, core.hub.GATE + '/futures/usdt/trades', {'contract': 'ETH_USDT', 'from': start, 'to': now, 'limit': PAGE_SIZE, 'offset': page * PAGE_SIZE})
                rows = raw or []
                for x in rows:
                    if not isinstance(x, dict): continue
                    trade_id = int(x.get('id') or 0); px = float(x.get('price') or 0); ts = tm._trade_time_seconds(x)
                    key = (trade_id, ts, px)
                    if px <= 0 or key in seen: continue
                    seen.add(key); events.append({'kind': 'trade', 'trade_id': trade_id, 'time': ts, 'price': px, 'source': 'gate-trades'})
                if len(rows) < PAGE_SIZE:
                    complete = True; break
            if not complete:
                core.state['risk_feed_probe'] = {'gate_trades_ok': False, 'coverage_complete': False, 'error': f'pagination exceeded {MAX_PAGES * PAGE_SIZE} trades in requested window', 'checked_at': now}
                return []
            ticker = await core.hub._json(client, core.hub.GATE + '/futures/usdt/tickers', {'contract': 'ETH_USDT'})
            if isinstance(ticker, list): ticker = ticker[0] if ticker else {}
            last = float((ticker or {}).get('last') or 0)
            events.sort(key=lambda x: (x['time'], x['trade_id']))
            if last > 0: events.append({'kind': 'ticker', 'trade_id': 0, 'time': float(time.time()), 'price': last, 'source': 'gate-ticker'})
            core.state['risk_feed_probe'] = {'gate_trades_ok': True, 'coverage_complete': True, 'pages': page + 1, 'trades': len(seen), 'from': start, 'to': now, 'checked_at': now}
            return events
        except Exception as exc:
            core.state['risk_feed_probe'] = {'gate_trades_ok': False, 'coverage_complete': False, 'error': f'Gate public trades: {exc}', 'checked_at': now}
        # Fallback last price can help visibility, but is deliberately NOT treated
        # as an ordered-trade feed and therefore cannot authorize a new signal.
        try:
            data = await core.hub._json(client, core.hub.BYBIT + '/v5/market/kline', {'category': 'linear', 'symbol': 'ETHUSDT', 'interval': '1', 'limit': 1}); rows = ((data or {}).get('result') or {}).get('list') or []
            if rows:
                px = float(rows[0][4]); return [{'kind': 'ticker', 'trade_id': 0, 'time': float(time.time()), 'price': px, 'source': 'bybit-last-fallback'}]
        except Exception as exc:
            core.state['risk_feed_probe']['fallback_error'] = str(exc)
    return []


async def monitor_with_idle_probe(core: Any) -> None:
    if not core.latest_signal():
        events = await fetch_trade_events_paginated(core)
        probe = core.state.get('risk_feed_probe') or {}
        core.state['risk_monitor'] = {'ok': bool(probe.get('gate_trades_ok') and probe.get('coverage_complete')), 'active': False, 'updated_at': int(time.time()), 'interval_seconds': tm.TRADE_MONITOR_SECONDS, 'source': 'gate-trades' if probe.get('gate_trades_ok') else (events[-1].get('source') if events else None), 'coverage_complete': bool(probe.get('coverage_complete'))}
        return
    await _ORIGINAL_MONITOR(core)


_ORIGINAL_MONITOR = tm.monitor_trades


def install(core: Any) -> None:
    tm.fetch_trade_events = fetch_trade_events_paginated
    tm.monitor_trades = monitor_with_idle_probe
    core.state['risk_feed_probe'] = {'gate_trades_ok': False, 'coverage_complete': False, 'checked_at': None, 'reason': 'awaiting first probe'}
