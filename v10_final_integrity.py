from __future__ import annotations

import asyncio
import bisect
import json
import math
import os
import statistics
import time
from typing import Any

import httpx

import adaptive_v5 as signal
import derivative_data
import execution_v7
import v5_runtime
import v7_timesafe_learning
import v9_derivative_gate
import v9_final
import v9_live_parity
import v9_readiness


VERSION = '8.1.0-20260809'
SAMPLE_SCHEMA = 6
STATE_KEY = 'final_derivative_coverage_v2'
INTERVAL = 4 * 3600
READY_SAFETY_SECONDS = 2 * INTERVAL
GATE_STATS_PUBLIC_START = 1604448000  # endpoint introduced 2020-11-04; earlier values are explicitly unavailable
SOURCE_PRIORITY = ('gate', 'bybit', 'binance', 'okx', 'bitget')
PERSISTENT_LIMIT = max(2, min(6, int(os.getenv('STRICT_SOURCE_DISABLE_AFTER', '2'))))
BACKFILL_PAGES = max(1, min(8, int(os.getenv('STRICT_CORE_DERIVATIVE_PAGES_PER_TICK', '4'))))


def _default_state(core: Any) -> dict[str, Any]:
    return {
        'version': VERSION,
        'sources': {},
        'updated_at': None,
        'generation': int(core.get_state('final_data_generation', 1) or 1),
        'frozen_enrichment': list(core.get_state('final_frozen_enrichment', []) or []),
    }


def _load(core: Any) -> dict[str, Any]:
    raw = core.get_state(STATE_KEY, None)
    out = _default_state(core)
    if isinstance(raw, dict):
        out.update(raw)
        out['sources'] = dict(raw.get('sources') or {})
        out['frozen_enrichment'] = list(raw.get('frozen_enrichment') or [])
    return out


def _save(core: Any, state: dict[str, Any]) -> None:
    state['version'] = VERSION
    state['updated_at'] = int(time.time())
    core.set_state(STATE_KEY, state)
    core.state.setdefault('strict_replay', {})['final_derivative_coverage'] = state


def _src(core: Any, key: str) -> dict[str, Any]:
    return dict((_load(core).get('sources') or {}).get(key) or {})


def _cursor(core: Any, key: str, start: int | None = None) -> int:
    rec = _src(core, key)
    return max(int(start if start is not None else core.START_TS), int(rec.get('processed_through') or rec.get('cursor') or start or core.START_TS))


def _persistent(msg: str) -> bool:
    return bool(v9_derivative_gate._is_persistent_provider_rejection(str(msg)))


def _transient(msg: str) -> bool:
    return bool(v9_derivative_gate._is_transient(str(msg)))


def _record(core: Any, key: str, *, ok: bool, processed_through: int | None = None,
            added: int = 0, error: str | None = None, detail: dict[str, Any] | None = None) -> dict[str, Any]:
    state = _load(core)
    rec = dict((state.get('sources') or {}).get(key) or {})
    now = int(time.time())
    rec['last_attempt_at'] = now
    rec['last_added'] = int(added)
    if processed_through is not None:
        rec['processed_through'] = max(int(core.START_TS), int(processed_through))
    if detail:
        rec['detail'] = detail
    if ok:
        rec['last_success_at'] = now
        rec['last_error'] = None
        rec['consecutive_errors'] = 0
        rec['same_error_count'] = 0
        rec['success_streak'] = int(rec.get('success_streak') or 0) + 1
    else:
        msg = str(error or 'unknown error')
        same = rec.get('last_error') == msg
        rec['last_error'] = msg
        rec['last_error_at'] = now
        rec['consecutive_errors'] = int(rec.get('consecutive_errors') or 0) + 1
        rec['same_error_count'] = int(rec.get('same_error_count') or 0) + 1 if same else 1
        rec['success_streak'] = 0
        # Never disable a source on timeout/429/5xx. Only deterministic provider
        # rejection may exclude that source, and only after repeated identical proof.
        if _persistent(msg) and not _transient(msg) and rec['same_error_count'] >= PERSISTENT_LIMIT:
            rec['disabled'] = True
            rec['disabled_at'] = rec.get('disabled_at') or now
            rec['disabled_reason'] = msg
    state.setdefault('sources', {})[key] = rec
    _save(core, state)
    return rec


def _disabled(core: Any, key: str) -> bool:
    return bool(_src(core, key).get('disabled'))


def _row_count(history: Any, source: str, metric: str) -> int:
    con = history._con()
    row = con.execute('SELECT COUNT(*) FROM derivative_history WHERE source=? AND metric=?', (source, metric)).fetchone()
    con.close()
    return int(row[0] or 0) if row else 0


async def _gate_stats(core: Any, pages: int) -> dict[str, Any]:
    key = 'gate_stats'
    if _disabled(core, key):
        return {'source': key, 'disabled': True, 'processed_through': _cursor(core, key, GATE_STATS_PUBLIC_START)}
    history = core.derivative_history
    cursor = max(GATE_STATS_PUBLIC_START, _cursor(core, key, GATE_STATS_PUBLIC_START))
    added = 0
    try:
        async with httpx.AsyncClient(timeout=getattr(core.hub, 'timeout', 18.0)) as client:
            for _ in range(max(1, pages)):
                now = int(time.time())
                if cursor >= now - INTERVAL:
                    break
                # Gate contract_stats is a public forward-history endpoint. Use a
                # conservative page size; huge limits have produced provider-side 4xx
                # on some deployments and must never freeze the whole replay.
                raw = await core.hub._json(client, core.hub.GATE + '/futures/usdt/contract_stats', {
                    'contract': 'ETH_USDT', 'from': int(cursor), 'interval': '4h', 'limit': 100,
                })
                rows = list(raw or [])
                oi_rows, long_rows, short_rows = [], [], []
                valid_ts: list[int] = []
                for x in rows:
                    ts = int(derivative_data._f(x.get('time')))
                    if ts < cursor:
                        continue
                    valid_ts.append(ts)
                    oi = derivative_data._f(x.get('open_interest_usd'))
                    long_liq = derivative_data._f(x.get('long_liq_usd_new'), derivative_data._f(x.get('long_liq_usd')))
                    short_liq = derivative_data._f(x.get('short_liq_usd_new'), derivative_data._f(x.get('short_liq_usd')))
                    if oi > 0:
                        oi_rows.append((ts, oi, 92.0, {'kind': 'gate_contract_stats_4h'}))
                    long_rows.append((ts, max(0.0, long_liq), 88.0, {'kind': 'gate_contract_stats_4h'}))
                    short_rows.append((ts, max(0.0, short_liq), 88.0, {'kind': 'gate_contract_stats_4h'}))
                added += history._insert('gate', 'oi_usd', oi_rows)
                added += history._insert('gate', 'liq_long_usd', long_rows)
                added += history._insert('gate', 'liq_short_usd', short_rows)
                if valid_ts:
                    nxt = max(valid_ts) + INTERVAL
                    cursor = nxt if nxt > cursor else cursor + 100 * INTERVAL
                else:
                    # An empty historical window is still evidence that this provider
                    # has no rows there. Advance the processed watermark, not a fake value.
                    cursor += 100 * INTERVAL
        rec = _record(core, key, ok=True, processed_through=min(cursor, int(time.time())), added=added,
                      detail={'oi_rows': _row_count(history, 'gate', 'oi_usd'), 'public_start': GATE_STATS_PUBLIC_START})
        return {'source': key, 'added': added, **rec}
    except Exception as exc:
        rec = _record(core, key, ok=False, processed_through=cursor, added=added, error=str(exc))
        return {'source': key, 'added': added, **rec}


async def _bybit_oi(core: Any, pages: int) -> dict[str, Any]:
    key = 'bybit_oi'
    if _disabled(core, key):
        return {'source': key, 'disabled': True, 'processed_through': _cursor(core, key)}
    history = core.derivative_history
    cursor = _cursor(core, key)
    added = 0
    try:
        async with httpx.AsyncClient(timeout=getattr(core.hub, 'timeout', 18.0)) as client:
            for _ in range(max(1, pages)):
                now = int(time.time())
                if cursor >= now - INTERVAL:
                    break
                end = min(now, cursor + 199 * INTERVAL)
                data = await core.hub._json(client, core.hub.BYBIT + '/v5/market/open-interest', {
                    'category': 'linear', 'symbol': 'ETHUSDT', 'intervalTime': '4h',
                    'startTime': int(cursor * 1000), 'endTime': int(end * 1000), 'limit': 200,
                })
                rows = ((data or {}).get('result') or {}).get('list') or []
                parsed = []
                for x in rows:
                    ts = int(derivative_data._f(x.get('timestamp')) / 1000)
                    val = derivative_data._f(x.get('openInterest'))
                    if cursor <= ts <= end and val > 0:
                        parsed.append((ts, val, 86.0, {'kind': 'bybit_oi_4h'}))
                added += history._insert('bybit', 'oi_coin', parsed)
                # Both startTime and endTime were explicitly processed. No returned
                # row means known missingness for this provider in that interval.
                cursor = end + INTERVAL
        rec = _record(core, key, ok=True, processed_through=min(cursor, int(time.time())), added=added,
                      detail={'oi_rows': _row_count(history, 'bybit', 'oi_coin')})
        return {'source': key, 'added': added, **rec}
    except Exception as exc:
        rec = _record(core, key, ok=False, processed_through=cursor, added=added, error=str(exc))
        return {'source': key, 'added': added, **rec}


async def _funding_bybit(core: Any, pages: int) -> dict[str, Any]:
    key = 'funding_bybit'
    if _disabled(core, key):
        return {'source': key, 'disabled': True, 'processed_through': _cursor(core, key)}
    history = core.derivative_history
    cursor = _cursor(core, key)
    added = 0
    try:
        async with httpx.AsyncClient(timeout=getattr(core.hub, 'timeout', 18.0)) as client:
            for _ in range(max(1, pages)):
                now = int(time.time())
                if cursor >= now - 3600:
                    break
                # Seven-day windows remain below the 200-record ceiling even if the
                # funding interval becomes as short as one hour.
                end = min(now, cursor + 7 * 86400)
                data = await core.hub._json(client, core.hub.BYBIT + '/v5/market/funding/history', {
                    'category': 'linear', 'symbol': 'ETHUSDT',
                    'startTime': int(cursor * 1000), 'endTime': int(end * 1000), 'limit': 200,
                })
                rows = ((data or {}).get('result') or {}).get('list') or []
                parsed = []
                for x in rows:
                    ts = int(derivative_data._f(x.get('fundingRateTimestamp')) / 1000)
                    if cursor <= ts <= end:
                        parsed.append((ts, derivative_data._f(x.get('fundingRate')), 86.0, {'kind': 'bybit_funding'}))
                added += history._insert('bybit', 'funding', parsed)
                cursor = end + 1
        rec = _record(core, key, ok=True, processed_through=min(cursor, int(time.time())), added=added,
                      detail={'funding_rows': _row_count(history, 'bybit', 'funding')})
        return {'source': key, 'added': added, **rec}
    except Exception as exc:
        rec = _record(core, key, ok=False, processed_through=cursor, added=added, error=str(exc))
        return {'source': key, 'added': added, **rec}


async def _funding_binance(core: Any, pages: int) -> dict[str, Any]:
    key = 'funding_binance'
    if _disabled(core, key):
        return {'source': key, 'disabled': True, 'processed_through': _cursor(core, key)}
    history = core.derivative_history
    cursor = _cursor(core, key)
    added = 0
    try:
        async with httpx.AsyncClient(timeout=getattr(core.hub, 'timeout', 18.0)) as client:
            for _ in range(max(1, pages)):
                now = int(time.time())
                if cursor >= now - 3600:
                    break
                end = min(now, cursor + 30 * 86400)
                rows = await core.hub._json(client, core.hub.BINANCE_FUT + '/fapi/v1/fundingRate', {
                    'symbol': 'ETHUSDT', 'startTime': int(cursor * 1000),
                    'endTime': int(end * 1000), 'limit': 1000,
                })
                parsed = []
                for x in rows or []:
                    ts = int(derivative_data._f(x.get('fundingTime')) / 1000)
                    if cursor <= ts <= end:
                        parsed.append((ts, derivative_data._f(x.get('fundingRate')), 86.0, {'kind': 'binance_funding'}))
                added += history._insert('binance', 'funding', parsed)
                cursor = end + 1
        rec = _record(core, key, ok=True, processed_through=min(cursor, int(time.time())), added=added,
                      detail={'funding_rows': _row_count(history, 'binance', 'funding')})
        return {'source': key, 'added': added, **rec}
    except Exception as exc:
        rec = _record(core, key, ok=False, processed_through=cursor, added=added, error=str(exc))
        return {'source': key, 'added': added, **rec}


async def _coinglass_optional(core: Any, key: str, metric: str, fn: Any) -> dict[str, Any]:
    if _disabled(core, key):
        return {'source': key, 'disabled': True, 'processed_through': _cursor(core, key)}
    if not getattr(core.derivative_history, 'coinglass_key', ''):
        rec = _record(core, key, ok=False, processed_through=_cursor(core, key), error='CoinGlass key not configured')
        rec['disabled'] = True
        state = _load(core); state.setdefault('sources', {})[key] = rec; _save(core, state)
        return {'source': key, **rec}
    cursor = _cursor(core, key)
    added = 0
    try:
        now = int(time.time())
        if cursor < now - INTERVAL:
            end = min(now, cursor + 999 * INTERVAL)
            added = int(await fn(cursor, end) or 0)
            cursor = end + INTERVAL
        rec = _record(core, key, ok=True, processed_through=min(cursor, now), added=added,
                      detail={'metric': metric, 'rows': _row_count(core.derivative_history, 'coinglass', metric)})
        return {'source': key, 'added': added, **rec}
    except Exception as exc:
        rec = _record(core, key, ok=False, processed_through=cursor, added=added, error=str(exc), detail={'metric': metric})
        return {'source': key, 'added': added, **rec}


def _group_ready(core: Any, keys: tuple[str, ...]) -> int | None:
    active = []
    for key in keys:
        rec = _src(core, key)
        if rec.get('disabled'):
            continue
        if rec.get('last_success_at') and rec.get('processed_through') is not None:
            active.append(int(rec['processed_through']))
    if active:
        # A feature group is usable once at least one independent provider has
        # processed that interval. Other providers continue backfilling for robustness.
        return max(active)
    if all(_src(core, k).get('disabled') for k in keys if _src(core, k)) and any(_src(core, k) for k in keys):
        return None
    return int(core.START_TS)


def core_ready_through(core: Any) -> int | None:
    oi_ready = _group_ready(core, ('gate_stats', 'bybit_oi', 'cg_oi'))
    funding_ready = _group_ready(core, ('funding_bybit', 'funding_binance'))
    required = [x for x in (oi_ready, funding_ready) if x is not None]
    if not required:
        return None
    return min(required)


def _complete(core: Any, key: str) -> bool:
    rec = _src(core, key)
    return bool(not rec.get('disabled') and int(rec.get('processed_through') or 0) >= int(time.time()) - READY_SAFETY_SECONDS)


def _maybe_freeze_enrichment(core: Any) -> None:
    state = _load(core)
    frozen = set(state.get('frozen_enrichment') or [])
    candidates = {k for k in ('cg_oi', 'cg_liq', 'cg_book') if _complete(core, k)}
    con = core.db(); sample_count = int(con.execute('SELECT COUNT(*) FROM learning_samples').fetchone()[0]); con.close()
    if not frozen and sample_count == 0:
        frozen = candidates
        state['frozen_enrichment'] = sorted(frozen)
        core.set_state('final_frozen_enrichment', sorted(frozen))
        _save(core, state)
        return
    # Once a replay generation has started, never change feature-source semantics in
    # the middle. Wait until every CoinGlass enrichment is either complete or proven
    # unavailable, then rebuild labels once with the richer frozen source set.
    cg_settled = all(_complete(core, k) or _src(core, k).get('disabled') for k in ('cg_oi', 'cg_liq', 'cg_book'))
    if cg_settled and candidates - frozen and sample_count > 0 and not state.get('richer_generation_applied'):
        _reset_labels_only(core, 'CoinGlass enrichment completed after replay start; rebuilding one richer, source-consistent generation')
        state = _load(core)
        state['frozen_enrichment'] = sorted(candidates)
        state['richer_generation_applied'] = True
        state['generation'] = int(state.get('generation') or 1) + 1
        core.set_state('final_frozen_enrichment', sorted(candidates))
        core.set_state('final_data_generation', int(state['generation']))
        _save(core, state)


async def parallel_backfill(core: Any, _hub: Any, _start_ts: int, pages: int = 4) -> dict[str, Any]:
    core.derivative_history.ensure_schema()
    page_budget = max(1, min(BACKFILL_PAGES, int(pages or BACKFILL_PAGES)))
    tasks = [
        _gate_stats(core, page_budget),
        _bybit_oi(core, page_budget),
        _funding_bybit(core, page_budget),
        _funding_binance(core, page_budget),
        _coinglass_optional(core, 'cg_oi', 'oi_usd', core.derivative_history._backfill_coinglass_oi),
        _coinglass_optional(core, 'cg_liq', 'liq_long_usd', core.derivative_history._backfill_coinglass_liquidation),
        _coinglass_optional(core, 'cg_book', 'book_imbalance', core.derivative_history._backfill_coinglass_book),
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    normalized, errors = [], []
    for x in results:
        if isinstance(x, Exception):
            errors.append(str(x))
        else:
            normalized.append(x)
            if x.get('last_error'):
                errors.append(f"{x.get('source')}: {x.get('last_error')}")
    _maybe_freeze_enrichment(core)
    payload = {
        'version': VERSION,
        'mode': 'core-quorum-plus-background-enrichment',
        'sources': normalized,
        'core_ready_through': core_ready_through(core),
        'frozen_enrichment': list((_load(core).get('frozen_enrichment') or [])),
        'errors': errors,
        'updated_at': int(time.time()),
    }
    core.state['derivative_multisource'] = payload
    return payload


def _coverage_allows(core: Any, key: str, ts: int) -> bool:
    rec = _src(core, key)
    return bool(not rec.get('disabled') and rec.get('last_success_at') and int(rec.get('processed_through') or 0) >= int(ts))


def _series(history: Any, metric: str, ts: int, max_age: int, limit_each: int = 6) -> dict[str, list[Any]]:
    con = history._con()
    rows = con.execute(
        'SELECT source,ts,value,quality FROM derivative_history WHERE metric=? AND ts<=? AND ts>=? ORDER BY source,ts DESC',
        (metric, int(ts), int(ts) - int(max_age)),
    ).fetchall(); con.close()
    out: dict[str, list[Any]] = {}
    for row in rows:
        b = out.setdefault(str(row['source']), [])
        if len(b) < limit_each:
            b.append(row)
    return out


def strict_derivative_extras(core: Any, history: Any, decision_ts: int) -> dict[str, float]:
    lagged = max(0, int(decision_ts) - int(v9_final.DERIVATIVE_SAFETY_LAG_SECONDS))
    frozen = set((_load(core).get('frozen_enrichment') or []))
    oi_changes, oi_q = [], []
    for metric in ('oi_usd', 'oi_coin'):
        for source, rows in _series(history, metric, lagged, 24 * 3600, 6).items():
            key = 'gate_stats' if source == 'gate' else 'bybit_oi' if source == 'bybit' else 'cg_oi' if source == 'coinglass' else None
            if not key or not _coverage_allows(core, key, lagged):
                continue
            if source == 'coinglass' and 'cg_oi' not in frozen:
                continue
            if len(rows) >= 2:
                newest, oldest = float(rows[0]['value']), float(rows[-1]['value'])
                if oldest and math.isfinite(newest) and math.isfinite(oldest):
                    oi_changes.append(newest / oldest - 1.0)
                    oi_q.append(float(rows[0]['quality']))

    funding_vals, funding_q = [], []
    for source, rows in _series(history, 'funding', int(decision_ts), 20 * 3600, 6).items():
        key = 'funding_bybit' if source == 'bybit' else 'funding_binance' if source == 'binance' else None
        if key and _coverage_allows(core, key, int(decision_ts)) and rows:
            funding_vals.append(float(rows[0]['value'])); funding_q.append(float(rows[0]['quality']))

    liq_vals, liq_totals, liq_q = [], [], []
    longs = _series(history, 'liq_long_usd', lagged, 12 * 3600, 3)
    shorts = _series(history, 'liq_short_usd', lagged, 12 * 3600, 3)
    for source in set(longs) & set(shorts):
        key = 'gate_stats' if source == 'gate' else 'cg_liq' if source == 'coinglass' else None
        if not key or not _coverage_allows(core, key, lagged):
            continue
        if source == 'coinglass' and 'cg_liq' not in frozen:
            continue
        lv, sv = max(0.0, float(longs[source][0]['value'])), max(0.0, float(shorts[source][0]['value']))
        total = lv + sv
        if total > 0:
            liq_vals.append((sv - lv) / total); liq_totals.append(total)
            liq_q.append(min(float(longs[source][0]['quality']), float(shorts[source][0]['quality'])))

    book_vals, book_q = [], []
    if 'cg_book' in frozen and _coverage_allows(core, 'cg_book', lagged):
        for source, rows in _series(history, 'book_imbalance', lagged, 12 * 3600, 3).items():
            if source == 'coinglass' and rows:
                book_vals.append(float(rows[0]['value'])); book_q.append(float(rows[0]['quality']))

    available = (bool(oi_changes), bool(funding_vals), bool(liq_vals), bool(book_vals))
    qs = oi_q + funding_q + liq_q + book_q
    return {
        'oi_change': statistics.median(oi_changes) if oi_changes else 0.0,
        'funding': statistics.median(funding_vals) if funding_vals else 0.0,
        'liquidation_imbalance': statistics.median(liq_vals) if liq_vals else 0.0,
        'liquidation_intensity': math.log1p(statistics.median(liq_totals)) / 25.0 if liq_totals else 0.0,
        'book_imbalance': statistics.median(book_vals) if book_vals else 0.0,
        'oi_available': float(bool(oi_changes)), 'funding_available': float(bool(funding_vals)),
        'liquidation_available': float(bool(liq_vals)), 'book_available': float(bool(book_vals)),
        'derivative_coverage': sum(available) / 4.0,
        'derivative_quality': statistics.mean(qs) / 100.0 if qs else 0.0,
        'historical_derivative_safety_lag_seconds': float(v9_final.DERIVATIVE_SAFETY_LAG_SECONDS),
    }


def deterministic_best_source(core: Any, asset: str, tf: str) -> str | None:
    # Fixed source priority is independent of what happens later in history. We only
    # ask whether the source exists near the configured historical start; we never rank
    # it by future row count/performance.
    con = core.db()
    candidates = []
    for rank, source in enumerate(SOURCE_PRIORITY):
        row = con.execute('SELECT MIN(ts) FROM market_bars WHERE source=? AND asset=? AND tf=?', (source, asset, tf)).fetchone()
        if row and row[0] is not None:
            candidates.append((rank, int(row[0]), source))
    con.close()
    if not candidates:
        return None
    early = [x for x in candidates if x[1] <= int(core.START_TS) + 7 * int(core.TIMEFRAME_SECONDS[tf])]
    if early:
        return sorted(early)[0][2]
    # If no exchange covers the exact configured start, select the objectively earliest
    # available source; ties still use the fixed priority.
    candidates.sort(key=lambda x: (x[1], x[0]))
    return candidates[0][2]


def _upsert_live_without_spot_contamination(core: Any, bundle: dict[str, Any]) -> None:
    for key, tf in (('eth_1d','1d'),('eth_4h','4h'),('eth_1h','1h'),('eth_30m','30m'),('eth_15m','15m'),('eth_5m','5m'),('btc_1h','1h')):
        asset = 'BTC' if key == 'btc_1h' else 'ETH'
        grouped: dict[str, list[dict[str, Any]]] = {}
        for row in bundle.get(key, []):
            grouped.setdefault(str(row.get('source') or 'gate'), []).append(row)
        for source, rows in grouped.items():
            core.insert_bars(source, asset, tf, rows)
    # Spot is valuable telemetry but must never share the futures primary key/source.
    spot_grouped: dict[str, list[dict[str, Any]]] = {}
    for row in bundle.get('eth_spot_15m', []):
        spot_grouped.setdefault(str(row.get('source') or 'gate') + '_spot', []).append(row)
    for source, rows in spot_grouped.items():
        core.insert_bars(source, 'ETH_SPOT', '15m', rows)
    for source, rows in bundle.get('validators', {}).items():
        core.insert_bars(source, 'ETH', '15m', rows)


def _one_outcome_5m(m15: list[dict[str, Any]], i15: int, future5: list[dict[str, Any]], strategy: str,
                    direction: str, entry_scale: float, stop_atr: float, target_r: float) -> tuple[bool, float, float, float]:
    past = m15[:i15 + 1]
    close = signal.f(m15[i15]['c'])
    a = max(signal.atr(past), close * .001)
    entry = v9_final._reference_entry(strategy, direction, close, past, a, entry_scale)
    sign = 1 if direction == 'LONG' else -1
    risk = max(stop_atr * a, entry * execution_v7.MIN_STOP_PCT)
    stop = entry - sign * risk
    target = entry + sign * target_r * risk
    wait15 = {'MOMENTUM_CONTINUATION':4,'SQUEEZE_EXPANSION':5,'FAILED_BREAKOUT_REVERSAL':5,
              'LIQUIDITY_SWEEP_REVERSAL':6,'RANGE_MEAN_REVERSION':6,'TREND_PULLBACK':8,'BREAKOUT_RETEST':8}.get(strategy,6)
    wait5 = min(len(future5), wait15 * 3)
    fill = next((j for j, b in enumerate(future5[:wait5]) if signal.f(b['l']) <= entry <= signal.f(b['h'])), None)
    if fill is None:
        return False, 0.0, 0.0, 0.0
    mfe = mae = 0.0
    last = entry
    for j, b in enumerate(future5[fill:]):
        low, high, last = signal.f(b['l']), signal.f(b['h']), signal.f(b['c'])
        favorable = (high-entry)/risk if direction == 'LONG' else (entry-low)/risk
        adverse = (entry-low)/risk if direction == 'LONG' else (high-entry)/risk
        mfe, mae = max(mfe, favorable), max(mae, adverse)
        stop_hit = low <= stop if direction == 'LONG' else high >= stop
        if stop_hit:
            return True, -1.0, mfe, mae
        # Entry bar ordering is unknown: never credit a target on the same 5m bar.
        if j == 0:
            continue
        target_hit = high >= target if direction == 'LONG' else low <= target
        if target_hit:
            return True, target_r, mfe, mae
    rr = max(-1.0, min(target_r, (last-entry)*sign/max(risk,1e-9)))
    return True, rr, mfe, mae


def strategy_outcome_5m(m15: list[dict[str, Any]], i15: int, future5: list[dict[str, Any]], strategy: str, direction: str) -> tuple[int,float,float,float]:
    profiles = ((.60,.90,1.15),(.85,1.15,1.40),(1.00,1.40,1.70),(1.20,1.75,2.10),(1.45,2.15,2.65))
    rows = [_one_outcome_5m(m15, i15, future5, strategy, direction, a, b, c) for a,b,c in profiles]
    filled = [x for x in rows if x[0]]
    if not filled:
        return 0, 0.0, 0.0, 0.0
    pnls = [x[1] for x in filled]
    robust = statistics.median(pnls)
    pnl = robust * min(1.0, len(filled)/3.0)
    positive_ratio = sum(x > .10 for x in pnls)/len(pnls)
    success = int(len(filled) >= 2 and pnl > .10 and positive_ratio >= .60)
    return success, pnl, statistics.median([x[2] for x in filled]), statistics.median([x[3] for x in filled])


def generate_samples(core: Any, batch: int = 500) -> int:
    src15 = deterministic_best_source(core,'ETH','15m'); src5 = deterministic_best_source(core,'ETH','5m')
    src1h = deterministic_best_source(core,'ETH','1h'); src4h = deterministic_best_source(core,'ETH','4h')
    src1d = deterministic_best_source(core,'ETH','1d'); srcbtc = deterministic_best_source(core,'BTC','1h')
    if not all((src15,src5,src1h,src4h,src1d,srcbtc)):
        return 0
    m15=core.load_bars('ETH','15m',src15); m5=core.load_bars('ETH','5m',src5); h1=core.load_bars('ETH','1h',src1h)
    h4=core.load_bars('ETH','4h',src4h); d1=core.load_bars('ETH','1d',src1d); btc=core.load_bars('BTC','1h',srcbtc)
    if min(map(len,(m15,m5,h1,h4,d1,btc))) < 120:
        return 0
    ts15=[int(x['ts']) for x in m15]; ts5=[int(x['ts']) for x in m5]
    last_ts=int(core.get_state(v5_runtime.REPLAY_STATE_KEY,core.START_TS) or core.START_TS)
    start_i=max(100,bisect.bisect_right(ts15,last_ts)); con=core.db(); store=v5_runtime.ModelStore(con)
    created=examined=0; newest=last_ts
    for i in range(start_i,len(m15)-33):
        if i % v9_final.REPLAY_STRIDE_BARS: continue
        sample_open=ts15[i]; decision_close=sample_open+900; examined+=1; newest=sample_open
        d1s=v9_final._closed_slice(d1,86400,decision_close,420); h4s=v9_final._closed_slice(h4,14400,decision_close,900)
        h1s=v9_final._closed_slice(h1,3600,decision_close,1000); btcs=v9_final._closed_slice(btc,3600,decision_close,500)
        m15s=m15[max(0,i-500):i+1]
        j5=bisect.bisect_left(ts5,decision_close); future5=m5[j5:j5+96]
        valid_future=bool(len(future5)>=96 and int(future5[0]['ts'])==decision_close and v9_final._continuous(future5,300))
        if len(d1s)<80 or len(h4s)<100 or len(h1s)<100 or len(btcs)<50 or not valid_future:
            if examined>=batch: break
            continue
        if not (v9_final._continuous(m15s[-min(160,len(m15s)):],900) and v9_final._continuous(h1s[-min(120,len(h1s)):],3600)
                and v9_final._continuous(h4s[-min(60,len(h4s)):],14400) and v9_final._continuous(d1s[-min(30,len(d1s)):],86400)
                and v9_final._continuous(btcs[-min(120,len(btcs)):],3600)):
            if examined>=batch: break
            continue
        regime=v5_runtime.detect_regime(d1s,h4s,h1s); extras=strict_derivative_extras(core,core.derivative_history,decision_close)
        features=v7_timesafe_learning.model_safe_features(core.build_features,m15s,h1s,btcs,regime,extras)
        quality=max(58.0,78.0*(.84+.16*float(extras.get('derivative_coverage',0.0))))
        for strategy in signal.STRATEGIES:
            for direction in signal.DIRECTIONS:
                success,pnl,mfe,mae=strategy_outcome_5m(m15,i,future5,strategy,direction)
                store.add_sample({'ts':sample_open,'strategy':strategy,'direction':direction,'regime':regime['regime'],'phase':regime['phase'],
                                  'features':features,'success':success,'pnl_r':pnl,'mfe_r':mfe,'mae_r':mae,'source_quality':quality})
                created+=1
        if examined>=batch: break
    store.commit(); con.close()
    if newest>last_ts: core.set_state(v5_runtime.REPLAY_STATE_KEY,newest)
    core.state.setdefault('learning',{})['strict_replay_last_batch']={'schema':SAMPLE_SCHEMA,'event_path':'5m_after_frozen_15m_decision',
        'examined_decisions':examined,'created_strategy_direction_samples':created,'future_usage':'labels only; never features or parameter selection'}
    return created


def watermarked_generate(core: Any, batch: int = 500) -> int:
    ready=core_ready_through(core); current=max(int(core.START_TS),int(core.get_state(v5_runtime.REPLAY_STATE_KEY,core.START_TS) or core.START_TS))
    if ready is None:
        core.state.setdefault('learning',{})['derivative_replay_watermark']={'mode':'explicit_missingness_no_core_derivative_source','blocked':False,'ready_through':None,'cursor':current}
        return int(generate_samples(core,batch) or 0)
    stride=int(v9_final.REPLAY_STRIDE_BARS)*900; room=int(ready)-current-2*stride; allowed=max(0,room//max(stride,1))
    if allowed<=0:
        core.state.setdefault('learning',{})['derivative_replay_watermark']={'mode':'core_derivative_quorum','blocked':True,'ready_through':int(ready),'cursor':current,
            'reason':'waiting for at least one OI source and one funding source to process the next historical decision interval'}
        return 0
    n=int(generate_samples(core,min(int(batch),int(allowed))) or 0)
    after=max(int(core.START_TS),int(core.get_state(v5_runtime.REPLAY_STATE_KEY,current) or current))
    if after>int(ready): raise RuntimeError(f'final strict replay exceeded core derivative readiness: cursor={after} ready={ready}')
    core.state.setdefault('learning',{})['derivative_replay_watermark']={'mode':'core_derivative_quorum','blocked':False,'ready_through':int(ready),'cursor':after,'batch_limit_from_watermark':min(int(batch),int(allowed))}
    return n


def _reset_labels_only(core: Any, reason: str) -> None:
    con=core.db(); tables={str(r[0]) for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
    if 'learning_samples' in tables: con.execute('DELETE FROM learning_samples')
    if 'learning_feature_snapshots' in tables: con.execute('DELETE FROM learning_feature_snapshots')
    if 'model_registry' in tables: con.execute("UPDATE model_registry SET status='ARCHIVED' WHERE status='CHAMPION'")
    if 'execution_registry_v7' in tables: con.execute("UPDATE execution_registry_v7 SET status='ARCHIVED' WHERE status='CHAMPION'")
    con.commit(); con.close(); core.set_state(v5_runtime.REPLAY_STATE_KEY,int(core.START_TS)); core.set_state('v5_last_train_sample_total',0)
    core.set_state('last_train_ts_v5',0); core.set_state('v7_execution_signal_signature',[]); core.set_state('v7_execution_last_attempt_ts',0)
    core.state.setdefault('learning',{})['final_generation_reset']={'at':int(time.time()),'reason':reason,'raw_market_preserved':True,'raw_derivatives_preserved':True}


def _migrate(core: Any) -> None:
    current=int(core.get_state('point_in_time_sample_schema',0) or 0)
    if current>=SAMPLE_SCHEMA:
        return
    _reset_labels_only(core,'schema 6: fixed-source priority, core derivative quorum, 5m event-time Signal labels')
    core.set_state('point_in_time_sample_schema',SAMPLE_SCHEMA)
    core.set_state('strict_replay_schema',max(2,int(core.get_state('strict_replay_schema',0) or 0)))


def _training_ready(core: Any) -> tuple[bool,str]:
    replay=v5_runtime._replay_progress(core); ready=core_ready_through(core); now=int(time.time())
    if not replay.get('complete'):
        return False,'full strict replay not complete'
    if ready is not None and int(ready)<now-READY_SAFETY_SECONDS:
        return False,'core derivative history has not caught up through the latest safe interval'
    return True,'full-span strict replay + core derivatives ready'


def install(core: Any) -> None:
    _migrate(core)
    # Deterministic source priority replaces future-aware row-count ranking everywhere,
    # including Execution replay data selection.
    core._best_source=lambda asset,tf: deterministic_best_source(core,asset,tf)
    core.upsert_live_gate=lambda bundle: _upsert_live_without_spot_contamination(core,bundle)
    core.derivative_history.backfill_tick=lambda hub,start_ts,pages=4: parallel_backfill(core,hub,start_ts,pages)
    v9_readiness._coinglass_ready_through=lambda c: core_ready_through(c)
    v9_final._strict_derivative_extras=lambda history,ts: strict_derivative_extras(core,history,ts)
    v5_runtime.generate_learning_samples_v5=lambda c,batch=500: watermarked_generate(c,batch)

    original_train=v5_runtime.train_v5
    def guarded_train(c: Any):
        ok,reason=_training_ready(c)
        c.state.setdefault('learning',{})['model_certification_gate']={'ready':ok,'reason':reason}
        if not ok: return []
        return original_train(c)
    v5_runtime.train_v5=guarded_train

    core.state['runtime_version']=VERSION; core.app.version='8.1.0'
    strict=core.state.setdefault('strict_replay',{}); strict['runtime']=VERSION
    strict['final_integrity']={
        'sample_schema':SAMPLE_SCHEMA,'fixed_price_source_priority':list(SOURCE_PRIORITY),'future_row_count_source_selection_forbidden':True,
        'signal_outcome_path':'5m sequential bars after frozen 15m decision','core_derivative_readiness':'OI quorum + funding quorum',
        'optional_derivatives_cannot_deadlock_replay':True,'unprocessed_source_is_missing_not_zero':True,
        'full_replay_required_before_signal_certification':True,'future_data_in_features_forbidden':True,
    }
    v9_final.FINAL_VERSION=VERSION; v9_readiness.READINESS_VERSION=VERSION; v9_live_parity.PARITY_VERSION=VERSION

    if not any(getattr(r,'path',None)=='/api/v10/final-integrity' for r in core.app.router.routes):
        @core.app.get('/api/v10/final-integrity')
        def final_integrity_status() -> dict[str,Any]:
            ok,reason=_training_ready(core); state=_load(core)
            return {'runtime':VERSION,'core_ready_through':core_ready_through(core),'training_certification_ready':ok,'training_gate_reason':reason,
                    'sources':state.get('sources',{}),'frozen_enrichment':state.get('frozen_enrichment',[]),'generation':state.get('generation',1),
                    'rules':strict.get('final_integrity',{})}
