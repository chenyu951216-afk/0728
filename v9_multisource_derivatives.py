from __future__ import annotations

import asyncio
import math
import os
import statistics
import time
from typing import Any

import httpx

import derivative_data
import v5_runtime
import v9_derivative_gate
import v9_final
import v9_live_parity
import v9_readiness
import v9_training_store
import runtime_identity


VERSION = runtime_identity.RUNTIME_VERSION
STATE_KEY = 'strict_multisource_derivatives_v1'
INTERVAL = 4 * 3600
PERSISTENT_FAILURE_LIMIT = max(2, min(6, int(os.getenv('STRICT_SOURCE_DISABLE_AFTER', '2'))))
DISABLED_PROBE_SECONDS = max(900, int(os.getenv('STRICT_DISABLED_SOURCE_PROBE_SECONDS', '3600')))

# These are independent historical sources. A source is disabled only after repeated
# deterministic provider rejection. Network/rate-limit errors keep retrying and never
# silently downgrade the training set.
SOURCE_KEYS = ('cg_oi', 'cg_liq', 'cg_book', 'gate_stats', 'bybit_oi')


def _default_state() -> dict[str, Any]:
    return {'version': VERSION, 'sources': {}, 'updated_at': None, 'source_generation': 1}


def _load(core: Any) -> dict[str, Any]:
    raw = core.get_state(STATE_KEY, None)
    state = _default_state()
    if isinstance(raw, dict):
        state.update(raw)
        state['sources'] = dict(raw.get('sources') or {})
    return state


def _save(core: Any, state: dict[str, Any]) -> None:
    state['version'] = VERSION
    state['updated_at'] = int(time.time())
    core.set_state(STATE_KEY, state)
    core.state.setdefault('strict_replay', {})['multisource_derivatives'] = state


def _persistent(message: str) -> bool:
    return v9_derivative_gate._is_persistent_provider_rejection(message)


def _transient(message: str) -> bool:
    return v9_derivative_gate._is_transient(message)


def _record(core: Any, source: str, *, ok: bool, cursor: int | None = None,
            error: str | None = None, added: int = 0) -> dict[str, Any]:
    state = _load(core)
    rec = dict((state.get('sources') or {}).get(source) or {})
    now = int(time.time())
    if cursor is not None:
        rec['cursor'] = max(int(core.START_TS), int(cursor))
    rec['last_attempt_at'] = now
    rec['last_added'] = int(added)
    if ok:
        rec['last_success_at'] = now
        rec['last_error'] = None
        rec['consecutive_errors'] = 0
        rec['same_error_count'] = 0
        rec['success_streak'] = int(rec.get('success_streak') or 0) + 1
        # A source excluded because of a provider rejection is kept out of the current
        # replay generation. It may still be probed/backfilled, but re-introducing it
        # mid-history would change feature semantics and is therefore not automatic.
    else:
        same = rec.get('last_error') == str(error)
        rec['last_error'] = str(error or 'unknown error')
        rec['last_error_at'] = now
        rec['consecutive_errors'] = int(rec.get('consecutive_errors') or 0) + 1
        rec['same_error_count'] = int(rec.get('same_error_count') or 0) + 1 if same else 1
        rec['success_streak'] = 0
        if _persistent(rec['last_error']) and rec['same_error_count'] >= PERSISTENT_FAILURE_LIMIT:
            rec['disabled'] = True
            rec['disabled_at'] = rec.get('disabled_at') or now
            rec['disabled_reason'] = rec['last_error']
            rec['mode'] = 'explicit_missingness_current_replay_generation'
    state.setdefault('sources', {})[source] = rec
    _save(core, state)
    return rec


def _cursor(core: Any, source: str) -> int:
    rec = (_load(core).get('sources') or {}).get(source) or {}
    return max(int(core.START_TS), int(rec.get('cursor') or core.START_TS))


def _disabled(core: Any, source: str) -> bool:
    return bool(((_load(core).get('sources') or {}).get(source) or {}).get('disabled'))


def _should_probe(core: Any, source: str) -> bool:
    rec = ((_load(core).get('sources') or {}).get(source) or {})
    if not rec.get('disabled'):
        return True
    return int(time.time()) - int(rec.get('last_attempt_at') or 0) >= DISABLED_PROBE_SECONDS


async def _cg_forward(core: Any, metric: str, source_key: str, fn: Any, pages: int) -> dict[str, Any]:
    history = core.derivative_history
    cursor = _cursor(core, source_key)
    added = 0
    if not getattr(history, 'coinglass_key', ''):
        rec = _record(core, source_key, ok=False, cursor=cursor, error='CoinGlass key not configured')
        rec['disabled'] = True
        state = _load(core); state['sources'][source_key] = rec; _save(core, state)
        return {'source': source_key, 'added': 0, 'cursor': cursor, 'disabled': True}
    if not _should_probe(core, source_key):
        return {'source': source_key, 'added': 0, 'cursor': cursor, 'disabled': True, 'probe_deferred': True}
    try:
        for _ in range(max(1, pages)):
            if cursor >= int(time.time()):
                break
            window_end = min(int(time.time()), cursor + 999 * INTERVAL)
            n = await fn(cursor, window_end)
            added += int(n)
            latest = history._latest(metric)
            if latest is None or latest < cursor:
                cursor = window_end + INTERVAL
            else:
                cursor = max(window_end + INTERVAL, int(latest) + INTERVAL)
        _record(core, source_key, ok=True, cursor=cursor, added=added)
        return {'source': source_key, 'added': added, 'cursor': cursor, 'disabled': _disabled(core, source_key)}
    except Exception as exc:
        _record(core, source_key, ok=False, cursor=cursor, error=str(exc), added=added)
        return {'source': source_key, 'added': added, 'cursor': cursor, 'error': str(exc), 'disabled': _disabled(core, source_key)}


async def _gate_stats_forward(core: Any, pages: int) -> dict[str, Any]:
    if not _should_probe(core, 'gate_stats'):
        return {'source': 'gate_stats', 'added': 0, 'cursor': _cursor(core, 'gate_stats'), 'disabled': True, 'probe_deferred': True}
    history = core.derivative_history
    cursor = _cursor(core, 'gate_stats')
    added = 0
    try:
        async with httpx.AsyncClient(timeout=getattr(core.hub, 'timeout', 18.0)) as client:
            for _ in range(max(1, pages)):
                if cursor >= int(time.time()):
                    break
                raw = await core.hub._json(client, core.hub.GATE + '/futures/usdt/contract_stats', {
                    'contract': 'ETH_USDT', 'from': cursor, 'interval': '4h', 'limit': 1000,
                })
                rows = list(raw or [])
                oi_rows = []
                long_rows = []
                short_rows = []
                for x in rows:
                    ts = int(derivative_data._f(x.get('time')))
                    if ts < cursor:
                        continue
                    oi = derivative_data._f(x.get('open_interest_usd'))
                    long_liq = derivative_data._f(x.get('long_liq_usd_new'), derivative_data._f(x.get('long_liq_usd')))
                    short_liq = derivative_data._f(x.get('short_liq_usd_new'), derivative_data._f(x.get('short_liq_usd')))
                    if oi > 0:
                        oi_rows.append((ts, oi, 90.0, {'kind': 'gate_contract_stats'}))
                    long_rows.append((ts, long_liq, 86.0, {'kind': 'gate_contract_stats'}))
                    short_rows.append((ts, short_liq, 86.0, {'kind': 'gate_contract_stats'}))
                added += history._insert('gate', 'oi_usd', oi_rows)
                added += history._insert('gate', 'liq_long_usd', long_rows)
                added += history._insert('gate', 'liq_short_usd', short_rows)
                valid_ts = [int(derivative_data._f(x.get('time'))) for x in rows if int(derivative_data._f(x.get('time'))) >= cursor]
                if not valid_ts:
                    # No market coverage in this old window: advance conservatively by
                    # one 1000-bar window rather than restarting from 2020 forever.
                    cursor += 1000 * INTERVAL
                else:
                    cursor = max(valid_ts) + INTERVAL
        _record(core, 'gate_stats', ok=True, cursor=cursor, added=added)
        return {'source': 'gate_stats', 'added': added, 'cursor': cursor, 'disabled': _disabled(core, 'gate_stats')}
    except Exception as exc:
        _record(core, 'gate_stats', ok=False, cursor=cursor, error=str(exc), added=added)
        return {'source': 'gate_stats', 'added': added, 'cursor': cursor, 'error': str(exc), 'disabled': _disabled(core, 'gate_stats')}


async def _bybit_oi_forward(core: Any, pages: int) -> dict[str, Any]:
    if not _should_probe(core, 'bybit_oi'):
        return {'source': 'bybit_oi', 'added': 0, 'cursor': _cursor(core, 'bybit_oi'), 'disabled': True, 'probe_deferred': True}
    history = core.derivative_history
    cursor = _cursor(core, 'bybit_oi')
    added = 0
    try:
        for _ in range(max(1, pages)):
            if cursor >= int(time.time()):
                break
            end = min(int(time.time()), cursor + 199 * INTERVAL)
            rows = await core.hub.fetch_bybit_oi_history('ETH', '4h', end_ts=end, limit=200)
            filtered = [x for x in rows if cursor <= int(x['ts']) <= end]
            added += history._insert('bybit', 'oi_coin', [
                (int(x['ts']), derivative_data._f(x['oi']), 84.0, {'kind': 'bybit_oi_4h'}) for x in filtered
            ])
            cursor = end + INTERVAL
        _record(core, 'bybit_oi', ok=True, cursor=cursor, added=added)
        return {'source': 'bybit_oi', 'added': added, 'cursor': cursor, 'disabled': _disabled(core, 'bybit_oi')}
    except Exception as exc:
        _record(core, 'bybit_oi', ok=False, cursor=cursor, error=str(exc), added=added)
        return {'source': 'bybit_oi', 'added': added, 'cursor': cursor, 'error': str(exc), 'disabled': _disabled(core, 'bybit_oi')}


async def _funding_backfill(core: Any, source: str, pages: int) -> dict[str, Any]:
    history = core.derivative_history
    key = f'funding:{source}'
    earliest = history._earliest('funding')
    # Per-source earliest, not the global funding earliest.
    con = history._con()
    row = con.execute('SELECT MIN(ts) FROM derivative_history WHERE metric=? AND source=?', ('funding', source)).fetchone()
    con.close()
    earliest = int(row[0]) if row and row[0] is not None else None
    end = (earliest - 1) if earliest else int(time.time())
    added = 0
    try:
        for _ in range(max(1, min(pages, 3))):
            rows = await core.hub.fetch_funding_history(source, 'ETH', end_ts=end, limit=200 if source == 'bybit' else 1000)
            added += history._insert(source, 'funding', [
                (int(x['ts']), derivative_data._f(x['funding']), 84.0 if source != 'gate' else 80.0, {}) for x in rows
            ])
            if not rows:
                break
            oldest = min(int(x['ts']) for x in rows)
            if oldest <= int(core.START_TS) or oldest >= end:
                break
            end = oldest - 1
        return {'source': key, 'added': added}
    except Exception as exc:
        return {'source': key, 'added': added, 'error': str(exc)}


def _active_sources(core: Any) -> dict[str, list[str]]:
    state = _load(core)
    src = state.get('sources') or {}
    active = lambda k: not bool((src.get(k) or {}).get('disabled'))
    return {
        'oi': [k for k in ('cg_oi', 'gate_stats', 'bybit_oi') if active(k)],
        'liquidation': [k for k in ('cg_liq', 'gate_stats') if active(k)],
        'book': [k for k in ('cg_book',) if active(k)],
    }


def _ready_through(core: Any) -> int | None:
    if not getattr(core.derivative_history, 'coinglass_key', '') and not _active_sources(core)['oi']:
        return None
    groups = _active_sources(core)
    required = list(dict.fromkeys(groups['oi'] + groups['liquidation'] + groups['book']))
    # If every optional provider was deterministically unavailable, replay proceeds
    # with explicit missingness. OI should normally retain Gate and/or Bybit.
    if not required:
        return None
    cursors = [_cursor(core, key) for key in required]
    return min(cursors) if cursors else None


def _source_series(history: Any, metric: str, ts: int, max_age: int, limit_each: int = 4) -> dict[str, list[Any]]:
    con = history._con()
    rows = con.execute(
        'SELECT source,ts,value,quality FROM derivative_history WHERE metric=? AND ts<=? AND ts>=? ORDER BY source,ts DESC',
        (metric, int(ts), int(ts) - int(max_age)),
    ).fetchall()
    con.close()
    out: dict[str, list[Any]] = {}
    for row in rows:
        bucket = out.setdefault(str(row['source']), [])
        if len(bucket) < limit_each:
            bucket.append(row)
    return out


def _multisource_extras(core: Any, history: Any, decision_ts: int) -> dict[str, float]:
    lagged = max(0, int(decision_ts) - int(v9_final.DERIVATIVE_SAFETY_LAG_SECONDS))
    active = _active_sources(core)
    disabled = {k for k in SOURCE_KEYS if _disabled(core, k)}

    oi_changes = []
    oi_quality = []
    # Percent change is comparable across USD OI and coin OI as long as each change is
    # computed within one source; never divide a Gate value by a Bybit value.
    for metric in ('oi_usd', 'oi_coin'):
        series = _source_series(history, metric, lagged, 24 * 3600, 6)
        for source, rows in series.items():
            if source == 'coinglass' and 'cg_oi' in disabled:
                continue
            if source == 'gate' and 'gate_stats' in disabled:
                continue
            if source == 'bybit' and 'bybit_oi' in disabled:
                continue
            if len(rows) >= 2:
                newest, oldest = float(rows[0]['value']), float(rows[-1]['value'])
                if oldest:
                    change = newest / oldest - 1
                    if math.isfinite(change):
                        oi_changes.append(change)
                        oi_quality.append(float(rows[0]['quality']))

    funding_series = _source_series(history, 'funding', int(decision_ts), 20 * 3600, 3)
    funding_values = [float(rows[0]['value']) for rows in funding_series.values() if rows]
    funding_quality = [float(rows[0]['quality']) for rows in funding_series.values() if rows]

    long_series = _source_series(history, 'liq_long_usd', lagged, 12 * 3600, 2)
    short_series = _source_series(history, 'liq_short_usd', lagged, 12 * 3600, 2)
    liq_imbalances = []
    liq_totals = []
    liq_quality = []
    for source in set(long_series) & set(short_series):
        if source == 'coinglass' and 'cg_liq' in disabled:
            continue
        if source == 'gate' and 'gate_stats' in disabled:
            continue
        lv = float(long_series[source][0]['value'])
        sv = float(short_series[source][0]['value'])
        total = max(0.0, lv) + max(0.0, sv)
        if total > 0:
            liq_imbalances.append((sv - lv) / total)
            liq_totals.append(total)
            liq_quality.append(min(float(long_series[source][0]['quality']), float(short_series[source][0]['quality'])))

    book_series = _source_series(history, 'book_imbalance', lagged, 12 * 3600, 2)
    book_values = []
    book_quality = []
    if 'cg_book' not in disabled:
        for rows in book_series.values():
            if rows:
                book_values.append(float(rows[0]['value']))
                book_quality.append(float(rows[0]['quality']))

    quality_values = oi_quality + funding_quality + liq_quality + book_quality
    available = (bool(oi_changes), bool(funding_values), bool(liq_imbalances), bool(book_values))
    return {
        'oi_change': statistics.median(oi_changes) if oi_changes else 0.0,
        'funding': statistics.median(funding_values) if funding_values else 0.0,
        'liquidation_imbalance': statistics.median(liq_imbalances) if liq_imbalances else 0.0,
        'liquidation_intensity': math.log1p(statistics.median(liq_totals)) / 25.0 if liq_totals else 0.0,
        'book_imbalance': statistics.median(book_values) if book_values else 0.0,
        'oi_available': float(bool(oi_changes)),
        'funding_available': float(bool(funding_values)),
        'liquidation_available': float(bool(liq_imbalances)),
        'book_available': float(bool(book_values)),
        'derivative_coverage': sum(available) / 4.0,
        'derivative_quality': statistics.mean(quality_values) / 100.0 if quality_values else 0.0,
        'historical_derivative_safety_lag_seconds': float(v9_final.DERIVATIVE_SAFETY_LAG_SECONDS),
        'oi_source_count': float(len(oi_changes)),
        'funding_source_count': float(len(funding_values)),
        'liquidation_source_count': float(len(liq_imbalances)),
    }


async def _parallel_backfill(core: Any, start_ts: int, pages: int = 2) -> dict[str, Any]:
    history = core.derivative_history
    history.ensure_schema()
    pages = max(1, min(8, int(pages)))
    tasks = [
        _cg_forward(core, 'oi_usd', 'cg_oi', history._backfill_coinglass_oi, pages),
        _cg_forward(core, 'liq_long_usd', 'cg_liq', history._backfill_coinglass_liquidation, pages),
        _cg_forward(core, 'book_imbalance', 'cg_book', history._backfill_coinglass_book, pages),
        _gate_stats_forward(core, pages),
        _bybit_oi_forward(core, pages),
        _funding_backfill(core, 'bybit', pages),
        _funding_backfill(core, 'binance', pages),
        _funding_backfill(core, 'gate', 1),
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    normalized = []
    errors = []
    for result in results:
        if isinstance(result, Exception):
            errors.append(str(result))
        else:
            normalized.append(result)
            if result.get('error'):
                errors.append(f"{result.get('source')}: {result.get('error')}")
    payload = {
        'mode': 'parallel_multisource',
        'sources': normalized,
        'errors': errors,
        'active': _active_sources(core),
        'ready_through': _ready_through(core),
        'updated_at': int(time.time()),
    }
    history._set_state('last_tick', payload)
    core.state['derivative_multisource'] = payload
    return payload


def install(core: Any) -> None:
    history = core.derivative_history
    history.backfill_tick = lambda hub, start_ts, pages=2: _parallel_backfill(core, start_ts, pages)
    v9_readiness._coinglass_ready_through = _ready_through
    v9_final._strict_derivative_extras = lambda h, ts: _multisource_extras(core, h, ts)

    original_status = history.status
    def status() -> dict[str, Any]:
        result = original_status()
        result['multisource'] = {
            'version': VERSION,
            'state': _load(core),
            'active': _active_sources(core),
            'ready_through': _ready_through(core),
            'last_tick': core.state.get('derivative_multisource', {}),
        }
        return result
    history.status = status

    # Keep final runtime identity coherent across all last-layer endpoints/notices.
    v9_final.FINAL_VERSION = VERSION
    v9_readiness.READINESS_VERSION = VERSION
    v9_training_store.STORE_VERSION = VERSION
    v9_live_parity.PARITY_VERSION = VERSION
    v9_derivative_gate.GATE_VERSION = VERSION
    core.state['runtime_version'] = VERSION
    core.state.setdefault('strict_replay', {})['runtime'] = VERSION
    core.state['strict_replay']['multisource_policy'] = {
        'parallel_backfill': True,
        'oi_sources': ['CoinGlass', 'Gate contract_stats', 'Bybit open-interest'],
        'liquidation_sources': ['CoinGlass', 'Gate contract_stats'],
        'funding_sources': ['Bybit', 'Binance', 'Gate'],
        'same_source_oi_change_only': True,
        'robust_cross_source_aggregation': 'median',
        'persistent_provider_rejection_can_disable_only_that_source': True,
        'transient_failure_blocks_and_retries_without_source_downgrade': True,
        'future_data_backfill_forbidden': True,
    }
    runtime_identity.stamp(core)

    if not any(getattr(r, 'path', None) == '/api/v9/multisource-derivatives' for r in core.app.router.routes):
        @core.app.get('/api/v9/multisource-derivatives')
        def multisource_status() -> dict[str, Any]:
            return {
                'runtime': VERSION,
                'active': _active_sources(core),
                'ready_through': _ready_through(core),
                'state': _load(core),
                'last_tick': core.state.get('derivative_multisource', {}),
                'rule': 'fetch every historically valid source first; only explicit all-source absence becomes missingness',
            }
