from __future__ import annotations

import asyncio
import os
import time
from typing import Any

import httpx

import v7_trade_monitor as tm

MAX_PAGES_PER_CHUNK = 20
PAGE_SIZE = 1000
CHUNK_SECONDS = 300
MAX_RECONCILE_SECONDS = 12 * 3600
IDLE_PROBE_SECONDS = max(20, min(300, int(os.getenv('RISK_IDLE_PROBE_SECONDS', '60'))))
IDLE_RETRY_SECONDS = max(5, min(IDLE_PROBE_SECONDS, int(os.getenv('RISK_IDLE_RETRY_SECONDS', '15'))))
REQUEST_RETRIES = max(1, min(4, int(os.getenv('RISK_FEED_REQUEST_RETRIES', '3'))))
REQUEST_TIMEOUT_SECONDS = max(6, min(30, float(os.getenv('RISK_FEED_REQUEST_TIMEOUT_SECONDS', '12'))))


def _exc_text(exc: BaseException) -> str:
    text = str(exc).strip()
    return f'{type(exc).__name__}: {text}' if text else repr(exc)


def monitor_start_full(core: Any, now: int) -> int:
    row = core.latest_signal()
    if not row:
        return now - max(15, tm.TRADE_MONITOR_SECONDS * 3)
    mg = (row.get('payload') or {}).get('management', {})
    last = float(mg.get('monitor_last_trade_time') or 0)
    if last > 0:
        return max(0, int(last) - 1)
    return max(0, int(row.get('filled_at') or row.get('created_at') or now))


async def _json_retry(core: Any, client: httpx.AsyncClient, url: str, params: dict[str, Any]) -> Any:
    last: BaseException | None = None
    for attempt in range(REQUEST_RETRIES):
        try:
            return await core.hub._json(client, url, params)
        except Exception as exc:
            last = exc
            if attempt + 1 < REQUEST_RETRIES:
                await asyncio.sleep(0.35 * (attempt + 1))
    assert last is not None
    raise last


async def fetch_trade_events_paginated(core: Any) -> list[dict[str, Any]]:
    """Fetch a complete ordered public-trade path for risk management.

    Trade-history coverage is authoritative. The optional current ticker is fetched in
    a separate best-effort step so a ticker timeout cannot invalidate an otherwise
    complete ordered trade reconciliation. A cross-exchange fallback may supply a
    display price, but never marks Gate ordered coverage complete.
    """
    now = int(time.time())
    start = monitor_start_full(core, now)
    events: list[dict[str, Any]] = []
    seen: set[tuple[int, float, float]] = set()
    total_pages = 0
    if now - start > MAX_RECONCILE_SECONDS:
        core.state['risk_feed_probe'] = {
            'gate_trades_ok': False, 'coverage_complete': False,
            'error': f'unreconciled monitor gap exceeds {MAX_RECONCILE_SECONDS // 3600}h',
            'from': start, 'to': now, 'checked_at': now,
        }
        return []

    timeout = httpx.Timeout(REQUEST_TIMEOUT_SECONDS, connect=min(5.0, REQUEST_TIMEOUT_SECONDS))
    limits = httpx.Limits(max_connections=4, max_keepalive_connections=2)
    async with httpx.AsyncClient(timeout=timeout, limits=limits) as client:
        try:
            cursor = start
            while cursor <= now:
                chunk_to = min(now, cursor + CHUNK_SECONDS)
                complete_chunk = False
                for page in range(MAX_PAGES_PER_CHUNK):
                    raw = await _json_retry(
                        core, client, core.hub.GATE + '/futures/usdt/trades',
                        {'contract': 'ETH_USDT', 'from': cursor, 'to': chunk_to,
                         'limit': PAGE_SIZE, 'offset': page * PAGE_SIZE},
                    )
                    rows = raw if isinstance(raw, list) else []
                    total_pages += 1
                    for x in rows:
                        if not isinstance(x, dict):
                            continue
                        try:
                            trade_id = int(x.get('id') or 0)
                            px = float(x.get('price') or 0)
                        except (TypeError, ValueError):
                            continue
                        ts = tm._trade_time_seconds(x)
                        key = (trade_id, ts, px)
                        if px <= 0 or ts <= 0 or key in seen:
                            continue
                        seen.add(key)
                        events.append({'kind': 'trade', 'trade_id': trade_id, 'time': ts,
                                       'price': px, 'source': 'gate-trades'})
                    if len(rows) < PAGE_SIZE:
                        complete_chunk = True
                        break
                if not complete_chunk:
                    core.state['risk_feed_probe'] = {
                        'gate_trades_ok': False, 'coverage_complete': False,
                        'error': f'pagination exceeded {MAX_PAGES_PER_CHUNK * PAGE_SIZE} trades in {CHUNK_SECONDS}s chunk',
                        'from': cursor, 'to': chunk_to, 'checked_at': now,
                    }
                    return []
                if chunk_to >= now:
                    break
                # Keep a one-second boundary overlap and deduplicate by identity.
                cursor = chunk_to

            events.sort(key=lambda x: (x['time'], x['trade_id']))
            ticker_error = None
            try:
                ticker = await _json_retry(
                    core, client, core.hub.GATE + '/futures/usdt/tickers',
                    {'contract': 'ETH_USDT'},
                )
                if isinstance(ticker, list):
                    ticker = ticker[0] if ticker else {}
                last = float((ticker or {}).get('last') or 0)
                if last > 0:
                    events.append({'kind': 'ticker', 'trade_id': 0, 'time': float(time.time()),
                                   'price': last, 'source': 'gate-ticker'})
            except Exception as exc:
                ticker_error = _exc_text(exc)

            core.state['risk_feed_probe'] = {
                'gate_trades_ok': True, 'coverage_complete': True,
                'pages': total_pages, 'trades': len(seen), 'from': start, 'to': now,
                'reconciled_seconds': now - start, 'checked_at': now,
                'ticker_optional_ok': ticker_error is None,
                'ticker_error': ticker_error,
                'request_retries': REQUEST_RETRIES,
            }
            return events
        except Exception as exc:
            core.state['risk_feed_probe'] = {
                'gate_trades_ok': False, 'coverage_complete': False,
                'error': f'Gate public trades: {_exc_text(exc)}',
                'from': start, 'to': now, 'checked_at': now,
                'request_retries': REQUEST_RETRIES,
            }

        # Price fallback is observational only. It must not silently satisfy the
        # ordered-trade safety gate because it cannot prove the missing trade path.
        try:
            data = await _json_retry(
                core, client, core.hub.BYBIT + '/v5/market/kline',
                {'category': 'linear', 'symbol': 'ETHUSDT', 'interval': '1', 'limit': 1},
            )
            rows = ((data or {}).get('result') or {}).get('list') or []
            if rows:
                px = float(rows[0][4])
                core.state.setdefault('risk_feed_probe', {})['fallback_price_source'] = 'bybit-last'
                return [{'kind': 'ticker', 'trade_id': 0, 'time': float(time.time()),
                         'price': px, 'source': 'bybit-last-fallback'}]
        except Exception as exc:
            core.state.setdefault('risk_feed_probe', {})['fallback_error'] = _exc_text(exc)
    return []


async def monitor_with_idle_probe(core: Any) -> None:
    active = core.latest_signal()
    if not active:
        now = int(time.time())
        prior = core.state.get('risk_feed_probe') or {}
        last_checked = int(prior.get('checked_at') or 0)
        prior_ok = bool(prior.get('gate_trades_ok') and prior.get('coverage_complete'))
        interval = IDLE_PROBE_SECONDS if prior_ok else IDLE_RETRY_SECONDS
        if last_checked and now - last_checked < interval:
            core.state['risk_monitor'] = {
                'ok': prior_ok, 'active': False, 'updated_at': now,
                'interval_seconds': tm.TRADE_MONITOR_SECONDS,
                'source': 'gate-trades' if prior_ok else prior.get('fallback_price_source'),
                'coverage_complete': bool(prior.get('coverage_complete')),
                'idle_probe_cached': True,
                'next_probe_in_seconds': max(0, interval - (now - last_checked)),
            }
            return
        events = await fetch_trade_events_paginated(core)
        probe = core.state.get('risk_feed_probe') or {}
        core.state['risk_monitor'] = {
            'ok': bool(probe.get('gate_trades_ok') and probe.get('coverage_complete')),
            'active': False, 'updated_at': int(time.time()),
            'interval_seconds': tm.TRADE_MONITOR_SECONDS,
            'source': 'gate-trades' if probe.get('gate_trades_ok') else (events[-1].get('source') if events else None),
            'coverage_complete': bool(probe.get('coverage_complete')),
            'idle_probe_cached': False,
        }
        return
    await _ORIGINAL_MONITOR(core)


_ORIGINAL_MONITOR = tm.monitor_trades


def install(core: Any) -> None:
    tm.fetch_trade_events = fetch_trade_events_paginated
    tm.monitor_trades = monitor_with_idle_probe
    core.state['risk_feed_probe'] = {
        'gate_trades_ok': False, 'coverage_complete': False, 'checked_at': None,
        'reason': 'awaiting first complete ordered-trade probe',
        'idle_probe_seconds': IDLE_PROBE_SECONDS,
        'request_retries': REQUEST_RETRIES,
    }
