from __future__ import annotations

import bisect
import math
import os
import re
import statistics
import time
from typing import Any

import v5_runtime
import v9_derivative_gate
import v9_final
import v9_readiness
import v10_final_integrity as fin
import v10_source_freeze as source_freeze
import v13_replay_cursor_integrity as cursor_guard

VERSION = '8.3.0-20260810'
SCHEMA = 1
SCHEMA_KEY = 'final_data_resilience_schema'
STATE_KEY = 'final_data_resilience_v1'
GAP_KEY = 'final_price_gap_registry_v1'

PRICE_PRIORITY = ('gate', 'bybit', 'binance', 'okx')
OI_PRIORITY = ('bybit_oi', 'gate_stats', 'cg_oi')
FUNDING_PRIORITY = ('funding_binance', 'funding_bybit')
GAP_REPAIR_ROUNDS = max(1, min(5, int(os.getenv('STRICT_GAP_REPAIR_ROUNDS', '2'))))
MAX_QUARANTINED_GAPS = max(0, min(200, int(os.getenv('STRICT_MAX_QUARANTINED_GAPS', '24'))))
CG_RANGE_RE = re.compile(r'earliest allowed start_time is\s*(\d+)', re.I)
WARMUP_SECONDS = 80 * 86400

_CANON_CACHE: dict[tuple[int, str, str, int, int], list[dict[str, Any]]] = {}


def parse_provider_earliest(message: str) -> int | None:
    m = CG_RANGE_RE.search(str(message))
    if not m:
        return None
    raw = int(m.group(1))
    return raw // 1000 if raw > 10_000_000_000 else raw


def _load(core: Any) -> dict[str, Any]:
    raw = core.get_state(STATE_KEY, None)
    out = {
        'version': VERSION, 'source_set_frozen': False,
        'model_oi_sources': [], 'model_funding_sources': [], 'model_enrichment_sources': [],
        'oi_mode': 'PENDING', 'funding_mode': 'PENDING', 'enrichment_mode': 'MODEL_MASKED',
        'effective_model_start': int(core.START_TS) + WARMUP_SECONDS,
        'provider_capabilities': {}, 'updated_at': None,
    }
    if isinstance(raw, dict):
        out.update(raw)
        for k in ('model_oi_sources', 'model_funding_sources', 'model_enrichment_sources'):
            out[k] = list(raw.get(k) or [])
        out['provider_capabilities'] = dict(raw.get('provider_capabilities') or {})
    return out


def _save(core: Any, state: dict[str, Any]) -> None:
    state['version'] = VERSION
    state['updated_at'] = int(time.time())
    core.set_state(STATE_KEY, state)
    core.state.setdefault('strict_replay', {})['data_resilience'] = state


def _gaps(core: Any) -> dict[str, Any]:
    raw = core.get_state(GAP_KEY, None)
    return {'version': 1, 'gaps': dict(raw.get('gaps') or {}), 'updated_at': raw.get('updated_at')} if isinstance(raw, dict) else {'version': 1, 'gaps': {}, 'updated_at': None}


def _save_gaps(core: Any, state: dict[str, Any]) -> None:
    state['version'] = 1
    state['updated_at'] = int(time.time())
    core.set_state(GAP_KEY, state)
    core.state.setdefault('strict_replay', {})['price_gap_registry'] = state


def _metric_for_key(key: str) -> tuple[str, str] | None:
    return {
        'bybit_oi': ('bybit', 'oi_coin'), 'gate_stats': ('gate', 'oi_usd'), 'cg_oi': ('coinglass', 'oi_usd'),
        'funding_binance': ('binance', 'funding'), 'funding_bybit': ('bybit', 'funding'),
        'cg_liq': ('coinglass', 'liq_long_usd'), 'cg_book': ('coinglass', 'book_imbalance'),
    }.get(key)


def _span(core: Any, key: str) -> dict[str, Any]:
    pair = _metric_for_key(key)
    if not pair:
        return {'rows': 0, 'first_ts': None, 'last_ts': None}
    con = core.derivative_history._con()
    try:
        row = con.execute('SELECT COUNT(*),MIN(ts),MAX(ts) FROM derivative_history WHERE source=? AND metric=?', pair).fetchone()
    finally:
        con.close()
    return {'rows': int(row[0] or 0), 'first_ts': int(row[1]) if row[1] is not None else None, 'last_ts': int(row[2]) if row[2] is not None else None}


def _complete(core: Any, key: str) -> bool:
    rec = fin._src(core, key)
    return bool(not rec.get('disabled') and rec.get('last_success_at') and int(rec.get('processed_through') or 0) >= int(time.time()) - fin.READY_SAFETY_SECONDS)


def _range_limited(core: Any, key: str) -> bool:
    rec = fin._src(core, key); detail = dict(rec.get('detail') or {})
    return bool(rec.get('range_limited') or detail.get('range_limited'))


def _full_span(core: Any, key: str, model_start: int) -> bool:
    if _range_limited(core, key):
        return False
    span = _span(core, key)
    lookback = 24 * 3600 if key in OI_PRIORITY else 20 * 3600 if key in FUNDING_PRIORITY else 12 * 3600
    return bool(span['rows'] >= 2 and span['first_ts'] is not None and int(span['first_ts']) <= int(model_start) - lookback)


def _settled(core: Any, keys: tuple[str, ...]) -> bool:
    return all(_complete(core, k) or fin._src(core, k).get('disabled') or _range_limited(core, k) for k in keys)


def _freeze_sources(core: Any) -> dict[str, Any]:
    state = _load(core)
    if state.get('source_set_frozen'):
        return state

    base = int(core.START_TS) + WARMUP_SECONDS
    oi = [k for k in OI_PRIORITY if _complete(core, k) and _full_span(core, k, base)]
    funding = [k for k in FUNDING_PRIORITY if _complete(core, k) and _full_span(core, k, base)]
    if not ((oi or _settled(core, OI_PRIORITY)) and (funding or _settled(core, FUNDING_PRIORITY))):
        state['oi_mode'] = 'FULL_SPAN_MODEL_ELIGIBLE' if oi else 'PENDING'
        state['funding_mode'] = 'FULL_SPAN_MODEL_ELIGIBLE' if funding else 'PENDING'
        _save(core, state)
        return state

    # Freeze one fixed-priority source per core group. If no provider can cover the
    # full model span, mask that group for the whole generation instead of letting an
    # availability transition become a fake market regime.
    chosen_oi = oi[:1]
    chosen_funding = funding[:1]
    state['model_oi_sources'] = chosen_oi
    state['model_funding_sources'] = chosen_funding
    state['oi_mode'] = 'FULL_SPAN_MODEL_ELIGIBLE' if chosen_oi else 'EXPLICIT_MISSINGNESS_MODEL_MASKED'
    state['funding_mode'] = 'FULL_SPAN_MODEL_ELIGIBLE' if chosen_funding else 'EXPLICIT_MISSINGNESS_MODEL_MASKED'

    starts = [base]
    for key in chosen_oi + chosen_funding:
        span = _span(core, key)
        if span['first_ts'] is not None:
            starts.append(int(span['first_ts']) + (24 * 3600 if key in OI_PRIORITY else 20 * 3600))
    state['effective_model_start'] = max(starts)

    # Recent-retention optional sources are never historical model features.
    enrichment: list[str] = []
    for key in ('cg_liq', 'cg_book'):
        if _complete(core, key) and _full_span(core, key, int(state['effective_model_start'])):
            enrichment.append(key)
    # Gate liquidation is admitted only if both sides genuinely cover the full span.
    con = core.derivative_history._con()
    try:
        a = con.execute("SELECT COUNT(*),MIN(ts) FROM derivative_history WHERE source='gate' AND metric='liq_long_usd'").fetchone()
        b = con.execute("SELECT COUNT(*),MIN(ts) FROM derivative_history WHERE source='gate' AND metric='liq_short_usd'").fetchone()
    finally:
        con.close()
    if (_complete(core, 'gate_stats') and a and b and int(a[0] or 0) >= 2 and int(b[0] or 0) >= 2 and
            a[1] is not None and b[1] is not None and
            min(int(a[1]), int(b[1])) <= int(state['effective_model_start']) - 12 * 3600):
        enrichment.insert(0, 'gate_stats')

    state['model_enrichment_sources'] = enrichment
    state['enrichment_mode'] = 'FULL_SPAN_MODEL_ELIGIBLE' if enrichment else 'MODEL_MASKED'
    state['source_set_frozen'] = True
    state['frozen_at'] = int(time.time())
    state['freeze_reason'] = 'full-span sources only; range-limited providers stay live/background and cannot block replay'
    _save(core, state)

    coverage = fin._load(core)
    coverage.update({
        'core_frozen': True, 'frozen_core_oi': chosen_oi, 'frozen_core_funding': chosen_funding,
        'frozen_enrichment': enrichment, 'all_sources_upgrade_applied': True,
        'freeze_reason': state['freeze_reason'],
    })
    fin._save(core, coverage)
    core.set_state('final_frozen_core_oi', chosen_oi)
    core.set_state('final_frozen_core_funding', chosen_funding)
    core.set_state('final_frozen_enrichment', enrichment)

    # Derived replay has just been reset on the resilience schema migration, so it is
    # safe to skip the pre-feature derivative warmup without creating labels.
    con = core.db()
    try:
        sample_count = int(con.execute('SELECT COUNT(*) FROM learning_samples').fetchone()[0] or 0)
    finally:
        con.close()
    if sample_count == 0:
        current = int(core.get_state(v5_runtime.REPLAY_STATE_KEY, core.START_TS) or core.START_TS)
        target = int(state['effective_model_start']) - 900
        if target > current:
            core.set_state(v5_runtime.REPLAY_STATE_KEY, target)
    return state


def ready_through(core: Any) -> int | None:
    state = _freeze_sources(core)
    if not state.get('source_set_frozen'):
        return int(core.START_TS)
    keys = list(state.get('model_oi_sources') or []) + list(state.get('model_funding_sources') or [])
    if not keys:
        return None
    return min(int(fin._src(core, k).get('processed_through') or core.START_TS) for k in keys)


def _capability(core: Any, key: str, **updates: Any) -> None:
    state = _load(core); caps = state.setdefault('provider_capabilities', {})
    cap = dict(caps.get(key) or {}); cap.update(updates); cap['updated_at'] = int(time.time())
    caps[key] = cap; _save(core, state)


async def range_safe_cg(core: Any, key: str, metric: str, fn: Any) -> dict[str, Any]:
    if not getattr(core.derivative_history, 'coinglass_key', ''):
        _capability(core, key, mode='DISABLED', model_full_span_eligible=False, reason='key not configured')
        rec = fin._src(core, key)
        rec.update({'disabled': True, 'disabled_reason': 'CoinGlass key not configured', 'mode': 'DISABLED'})
        state = fin._load(core); state.setdefault('sources', {})[key] = rec; fin._save(core, state)
        return {'source': key, **rec}

    old = fin._src(core, key); detail = dict(old.get('detail') or {})
    available_from = detail.get('provider_available_from')
    cursor = max(int(available_from or core.START_TS), int(detail.get('background_cursor') or available_from or core.START_TS))
    now = int(time.time())

    async def one(start: int) -> tuple[int, int]:
        end = min(now, int(start) + 999 * fin.INTERVAL)
        if end <= start:
            return 0, end
        return int(await fn(start, end) or 0), end

    try:
        added = 0
        if cursor < now - fin.INTERVAL:
            added, end = await one(cursor); cursor = end + fin.INTERVAL
        span = _span(core, key)
        detail.update({'metric': metric, 'rows': span['rows'], 'first_data_ts': span['first_ts'], 'last_data_ts': span['last_ts'],
                       'background_cursor': cursor, 'provider_available_from': available_from,
                       'range_limited': bool(available_from and int(available_from) > int(core.START_TS))})
        rec = fin._record(core, key, ok=True, processed_through=min(cursor, now), added=added, detail=detail)
        if detail['range_limited']:
            rec.update({'mode': 'RANGE_LIMITED_LIVE_ONLY', 'range_limited': True, 'model_full_span_eligible': False})
            state = fin._load(core); state.setdefault('sources', {})[key] = rec; fin._save(core, state)
            _capability(core, key, mode='RANGE_LIMITED_LIVE_ONLY', provider_available_from=available_from, model_full_span_eligible=False)
        return {'source': key, 'added': added, **rec}
    except Exception as exc:
        msg = str(exc)
        earliest = parse_provider_earliest(msg)
        if earliest is None:
            rec = fin._record(core, key, ok=False, processed_through=cursor, error=msg, detail={'metric': metric})
            _capability(core, key, mode='TRANSIENT_RETRY' if v9_derivative_gate._is_transient(msg) else 'RETRY', notice=msg)
            return {'source': key, **rec}

        # Provider proved it cannot serve the old interval. This is a capability,
        # not a replay error. Jump background collection into the valid range.
        added = 0; retry_error = None; bg = int(earliest)
        try:
            if bg < now - fin.INTERVAL:
                added, end = await one(bg); bg = end + fin.INTERVAL
        except Exception as retry_exc:
            retry_error = str(retry_exc)
        span = _span(core, key)
        detail = {'metric': metric, 'rows': span['rows'], 'first_data_ts': span['first_ts'], 'last_data_ts': span['last_ts'],
                  'provider_available_from': int(earliest), 'background_cursor': bg, 'range_limited': True,
                  'model_full_span_eligible': False, 'provider_notice': msg, 'background_retry_error': retry_error}
        state = fin._load(core); rec = dict((state.get('sources') or {}).get(key) or {})
        rec.update({'last_attempt_at': now, 'last_error': None, 'consecutive_errors': 0, 'same_error_count': 0, 'disabled': False,
                    'mode': 'RANGE_LIMITED_LIVE_ONLY', 'range_limited': True, 'model_full_span_eligible': False,
                    'processed_through': max(int(rec.get('processed_through') or core.START_TS), min(bg, now)),
                    'last_added': added, 'detail': detail})
        if retry_error is None: rec['last_success_at'] = now
        state.setdefault('sources', {})[key] = rec; fin._save(core, state)
        _capability(core, key, mode='RANGE_LIMITED_LIVE_ONLY', provider_available_from=int(earliest),
                    model_full_span_eligible=False, notice=msg, background_retry_error=retry_error)
        return {'source': key, 'added': added, **rec,
                'nonblocking_notice': f'{key} range-limited; excluded from historical model and replay blocking'}


def strict_extras(core: Any, history: Any, decision_ts: int) -> dict[str, float]:
    state = _freeze_sources(core)
    oi_allowed = set(state.get('model_oi_sources') or [])
    funding_allowed = set(state.get('model_funding_sources') or [])
    enrichment = set(state.get('model_enrichment_sources') or [])
    decision_ts = int(decision_ts)
    lagged = max(0, decision_ts - int(v9_final.DERIVATIVE_SAFETY_LAG_SECONDS))

    def series(metric: str, upper: int, age: int) -> dict[str, list[Any]]:
        con = history._con()
        try:
            rows = con.execute('SELECT source,ts,value,quality FROM derivative_history WHERE metric=? AND ts<=? AND ts>=? ORDER BY source,ts DESC',
                               (metric, upper, upper - age)).fetchall()
        finally:
            con.close()
        out: dict[str, list[Any]] = {}
        for row in rows:
            bucket = out.setdefault(str(row['source']), [])
            if len(bucket) < 6: bucket.append(row)
        return out

    oi: list[float] = []; oiq: list[float] = []
    for metric in ('oi_usd', 'oi_coin'):
        for src, rows in series(metric, lagged, 24 * 3600).items():
            key = 'gate_stats' if src == 'gate' else 'bybit_oi' if src == 'bybit' else 'cg_oi' if src == 'coinglass' else None
            if key not in oi_allowed or len(rows) < 2 or not fin._coverage_allows(core, key, lagged): continue
            newest, oldest = float(rows[0]['value']), float(rows[-1]['value'])
            if oldest: oi.append(newest / oldest - 1.0); oiq.append(float(rows[0]['quality']))

    funding: list[float] = []; fq: list[float] = []
    for src, rows in series('funding', decision_ts, 20 * 3600).items():
        key = 'funding_bybit' if src == 'bybit' else 'funding_binance' if src == 'binance' else None
        if key in funding_allowed and rows and fin._coverage_allows(core, key, decision_ts):
            funding.append(float(rows[0]['value'])); fq.append(float(rows[0]['quality']))

    longs, shorts = series('liq_long_usd', lagged, 12 * 3600), series('liq_short_usd', lagged, 12 * 3600)
    liq: list[float] = []; totals: list[float] = []; lq: list[float] = []
    for src in set(longs) & set(shorts):
        key = 'gate_stats' if src == 'gate' else 'cg_liq' if src == 'coinglass' else None
        if key not in enrichment or not fin._coverage_allows(core, key, lagged): continue
        lv, sv = max(0.0, float(longs[src][0]['value'])), max(0.0, float(shorts[src][0]['value']))
        total = lv + sv
        if total > 0: liq.append((sv - lv) / total); totals.append(total); lq.append(min(float(longs[src][0]['quality']), float(shorts[src][0]['quality'])))

    book: list[float] = []; bq: list[float] = []
    if 'cg_book' in enrichment and fin._coverage_allows(core, 'cg_book', lagged):
        for src, rows in series('book_imbalance', lagged, 12 * 3600).items():
            if src == 'coinglass' and rows: book.append(float(rows[0]['value'])); bq.append(float(rows[0]['quality']))

    avail = (bool(oi), bool(funding), bool(liq), bool(book)); qs = oiq + fq + lq + bq
    return {'oi_change': statistics.median(oi) if oi else 0.0, 'funding': statistics.median(funding) if funding else 0.0,
            'liquidation_imbalance': statistics.median(liq) if liq else 0.0,
            'liquidation_intensity': math.log1p(statistics.median(totals)) / 25.0 if totals else 0.0,
            'book_imbalance': statistics.median(book) if book else 0.0, 'oi_available': float(bool(oi)),
            'funding_available': float(bool(funding)), 'liquidation_available': float(bool(liq)), 'book_available': float(bool(book)),
            'derivative_coverage': sum(avail) / 4.0, 'derivative_quality': statistics.mean(qs) / 100.0 if qs else 0.0,
            'historical_derivative_safety_lag_seconds': float(v9_final.DERIVATIVE_SAFETY_LAG_SECONDS)}


def canonical_bars(core: Any, asset: str, tf: str) -> list[dict[str, Any]]:
    con = core.db()
    try:
        sig = con.execute("SELECT COUNT(*),COALESCE(MAX(ts),0) FROM market_bars WHERE asset=? AND tf=? AND source IN ('gate','bybit','binance','okx')",
                          (asset, tf)).fetchone()
        key = (id(core), asset, tf, int(sig[0] or 0), int(sig[1] or 0))
        if key in _CANON_CACHE:
            return _CANON_CACHE[key]
        rows = con.execute("SELECT source,ts,o,h,l,c,v,qv FROM market_bars WHERE asset=? AND tf=? AND source IN ('gate','bybit','binance','okx') ORDER BY ts",
                           (asset, tf)).fetchall()
    finally:
        con.close()
    rank = {s: i for i, s in enumerate(PRICE_PRIORITY)}
    by_ts: dict[int, tuple[int, dict[str, Any]]] = {}
    for r in rows:
        src, ts = str(r['source']), int(r['ts']); pr = rank.get(src, 999)
        old = by_ts.get(ts)
        if old is None or pr < old[0]:
            by_ts[ts] = (pr, {'ts': ts, 'o': float(r['o']), 'h': float(r['h']), 'l': float(r['l']), 'c': float(r['c']),
                              'v': float(r['v']), 'qv': float(r['qv']), '_source': src})
    out = [by_ts[t][1] for t in sorted(by_ts)]
    for old in list(_CANON_CACHE):
        if old[:3] == (id(core), asset, tf) and old != key: _CANON_CACHE.pop(old, None)
    _CANON_CACHE[key] = out
    return out


def _gap_id(asset: str, tf: str, ts: int) -> str:
    return f'{asset}:{tf}:{int(ts)}'


def _first_missing(rows: list[dict[str, Any]], start: int, sec: int, count: int) -> int | None:
    present = {int(x['ts']) for x in rows}
    for i in range(count):
        ts = start + i * sec
        if ts not in present: return ts
    return None


def _tail_gap(rows: list[dict[str, Any]], sec: int, bars: int) -> int | None:
    tail = rows[-bars:] if len(rows) >= bars else rows
    for i in range(1, len(tail)):
        if int(tail[i]['ts']) - int(tail[i-1]['ts']) != sec: return int(tail[i-1]['ts']) + sec
    return None


def detect_gap_near_cursor(core: Any) -> dict[str, Any] | None:
    m15, m5 = canonical_bars(core, 'ETH', '15m'), canonical_bars(core, 'ETH', '5m')
    h1, h4, d1, btc = canonical_bars(core, 'ETH', '1h'), canonical_bars(core, 'ETH', '4h'), canonical_bars(core, 'ETH', '1d'), canonical_bars(core, 'BTC', '1h')
    if min(map(len, (m15, m5, h1, h4, d1, btc))) < 120: return None
    ts15, ts5 = [int(x['ts']) for x in m15], [int(x['ts']) for x in m5]
    last = int(core.get_state(v5_runtime.REPLAY_STATE_KEY, core.START_TS) or core.START_TS)
    start_i = max(100, bisect.bisect_right(ts15, last))
    for i in range(start_i, min(len(m15)-33, start_i+12)):
        if i % v9_final.REPLAY_STRIDE_BARS: continue
        open_ts, close_ts = ts15[i], ts15[i] + 900
        d1s = v9_final._closed_slice(d1, 86400, close_ts, 420); h4s = v9_final._closed_slice(h4, 14400, close_ts, 900)
        h1s = v9_final._closed_slice(h1, 3600, close_ts, 1000); btcs = v9_final._closed_slice(btc, 3600, close_ts, 500)
        if len(d1s)<80 or len(h4s)<100 or len(h1s)<100 or len(btcs)<50: continue
        j5 = bisect.bisect_left(ts5, close_ts); future5 = m5[j5:j5+96]
        missing = _first_missing(future5, close_ts, 300, 96)
        if missing is not None: return {'asset':'ETH','tf':'5m','missing_ts':missing,'at_ts':open_ts,'reason':'5m future path gap'}
        checks = [('ETH','15m',m15[max(0,i-500):i+1],900,160),('ETH','1h',h1s,3600,120),('ETH','4h',h4s,14400,60),
                  ('ETH','1d',d1s,86400,30),('BTC','1h',btcs,3600,120)]
        for asset, tf, rows, sec, bars in checks:
            missing = _tail_gap(rows, sec, min(bars,len(rows)))
            if missing is not None: return {'asset':asset,'tf':tf,'missing_ts':missing,'at_ts':open_ts,'reason':'feature continuity gap'}
        return None
    return None


async def repair_gap(core: Any, target: dict[str, Any]) -> dict[str, Any]:
    asset, tf, ts = str(target['asset']), str(target['tf']), int(target['missing_ts']); gid = _gap_id(asset,tf,ts)
    state = _gaps(core); gaps = state.setdefault('gaps', {})
    rec = dict(gaps.get(gid) or {'gap_id':gid,'asset':asset,'tf':tf,'missing_ts':ts,'status':'PENDING_REPAIR','attempts':0,'settled_rounds':0,'created_at':int(time.time())})
    if rec.get('status') in ('REPAIRED','QUARANTINED_UNRECOVERABLE'): return rec
    rec['attempts'] = int(rec.get('attempts') or 0)+1; rec['last_attempt_at']=int(time.time())
    sec=int(core.TIMEFRAME_SECONDS[tf]); any_exact=False; all_settled=True; results={}
    for source in PRICE_PRIORITY:
        try:
            rows=await core.hub.fetch_history(source,asset,tf,end_ts=ts+2*sec,limit=30)
            payload=[]; exact=False
            for c in rows or []:
                cts=int(c.ts)
                if ts-2*sec<=cts<=ts+2*sec: payload.append(c.dict())
                if cts==ts: exact=True
            added=int(core.insert_bars(source,asset,tf,payload) or 0) if payload else 0
            results[source]={'ok':True,'rows':len(rows or []),'added':added,'exact':exact}; any_exact=any_exact or exact
        except Exception as exc:
            msg=str(exc); transient=v9_derivative_gate._is_transient(msg)
            results[source]={'ok':False,'transient':transient,'error':msg[-500:]}; all_settled=all_settled and not transient
    _CANON_CACHE.clear()
    if any_exact and ts in {int(x['ts']) for x in canonical_bars(core,asset,tf)}:
        rec.update({'status':'REPAIRED','repaired_at':int(time.time()),'results':results})
    else:
        if all_settled: rec['settled_rounds']=int(rec.get('settled_rounds') or 0)+1
        rec['results']=results
        if int(rec.get('settled_rounds') or 0)>=GAP_REPAIR_ROUNDS:
            rec.update({'status':'QUARANTINED_UNRECOVERABLE','quarantined_at':int(time.time()),
                        'rule':'all fixed-priority exchanges queried repeatedly; no synthetic candle or interpolation created'})
        else: rec['status']='PENDING_REPAIR'
    gaps[gid]=rec; _save_gaps(core,state); return rec


def _safe_resume_after_gap(target: dict[str, Any]) -> int:
    ts, tf = int(target['missing_ts']), str(target['tf'])
    # Skip every decision whose feature or 8h label window could cross the audited gap.
    return ts + {'5m':8*3600,'15m':160*900,'1h':120*3600,'4h':60*14400,'1d':30*86400}.get(tf,8*3600)


def _gap_summary(core: Any) -> dict[str, Any]:
    vals=list((_gaps(core).get('gaps') or {}).values()); counts={}
    for r in vals: counts[str(r.get('status') or 'UNKNOWN')]=counts.get(str(r.get('status') or 'UNKNOWN'),0)+1
    return {'total':len(vals),'counts':counts,'recent':sorted(vals,key=lambda x:int(x.get('last_attempt_at') or x.get('created_at') or 0),reverse=True)[:10]}


def _ensure_migration(core: Any) -> None:
    if int(core.get_state(SCHEMA_KEY,0) or 0)>=SCHEMA: return
    cursor_guard._reset_derived_replay(core,'8.3.0 final data resilience: provider capability ranges + canonical multi-exchange price gaps')
    core.set_state(GAP_KEY,{'version':1,'gaps':{},'updated_at':int(time.time())})
    coverage=fin._load(core); coverage.update({'core_frozen':False,'frozen_core_oi':[],'frozen_core_funding':[],'frozen_enrichment':[],'all_sources_upgrade_applied':True}); fin._save(core,coverage)
    _save(core,{**_load(core),'source_set_frozen':False,'model_oi_sources':[],'model_funding_sources':[],'model_enrichment_sources':[],
                'migration_at':int(time.time()),'migration_reason':'derived only; raw market/derivative cache and CLEAN Dataset ID preserved'})
    core.set_state(SCHEMA_KEY,SCHEMA)


def install(core: Any) -> None:
    _ensure_migration(core)

    # One final source-generation authority. Older source-freeze hooks remain downloaders only.
    source_freeze._freeze_if_ready=lambda c:None
    source_freeze._upgrade_if_all_settled=lambda c:None
    fin._coinglass_optional=range_safe_cg

    original_load=core.load_bars
    def resilient_load(asset:str,tf:str,source:str='gate',limit:int|None=None):
        if source=='canonical':
            rows=canonical_bars(core,asset,tf)
            return rows[-int(limit):] if limit else rows
        return original_load(asset,tf,source,limit)
    core.load_bars=resilient_load
    fin.deterministic_best_source=lambda c,asset,tf:'canonical' if canonical_bars(c,asset,tf) else None
    core._best_source=lambda asset,tf:'canonical' if canonical_bars(core,asset,tf) else None

    original_backfill=core.derivative_history.backfill_tick
    async def backfill(hub:Any,start_ts:int,pages:int=4):
        result=dict(await original_backfill(hub,start_ts,pages) or {})
        st=_freeze_sources(core); notices=[]; errors=[]
        for e in result.get('errors') or []:
            (notices if parse_provider_earliest(str(e)) is not None else errors).append(str(e))
        for key,cap in (st.get('provider_capabilities') or {}).items():
            if cap.get('mode')=='RANGE_LIMITED_LIVE_ONLY': notices.append(f'{key}: range-limited live/background only; excluded from historical model')
        result.update({'version':VERSION,'core_frozen':bool(st.get('source_set_frozen')),'frozen_core_oi':st.get('model_oi_sources',[]),
                       'frozen_core_funding':st.get('model_funding_sources',[]),'frozen_enrichment':st.get('model_enrichment_sources',[]),
                       'core_ready_through':ready_through(core),'effective_model_start':st.get('effective_model_start'),
                       'errors':errors,'nonblocking_provider_notices':notices[-12:],'provider_capabilities':st.get('provider_capabilities',{})})
        core.state['derivative_multisource']=result; return result
    core.derivative_history.backfill_tick=backfill

    fin.core_ready_through=lambda c:ready_through(c)
    v9_readiness._coinglass_ready_through=lambda c:ready_through(c)
    fin.strict_derivative_extras=lambda c,h,ts:strict_extras(c,h,ts)
    v9_final._strict_derivative_extras=lambda h,ts:strict_extras(core,h,ts)

    # Capture the true replay blocker before the older public-state formatter overwrites it.
    original_generate=fin.generate_samples
    def captured_generate(c:Any,batch:int=500):
        n=int(original_generate(c,batch) or 0)
        c.state['strict_replay_gap_blocker']=dict((c.state.get('learning') or {}).get('replay_price_blocker') or {})
        return n
    fin.generate_samples=captured_generate

    original_train=v5_runtime.train_v5
    def guarded_train(c:Any,*args:Any,**kwargs:Any):
        st=_freeze_sources(c); summary=_gap_summary(c); pending=int(summary['counts'].get('PENDING_REPAIR',0)); quarantined=int(summary['counts'].get('QUARANTINED_UNRECOVERABLE',0))
        ready=bool(st.get('source_set_frozen') and pending==0 and quarantined<=MAX_QUARANTINED_GAPS)
        c.state.setdefault('learning',{})['resilience_certification_gate']={'ready':ready,'pending_gaps':pending,'quarantined_gaps':quarantined,'max_quarantined_gaps':MAX_QUARANTINED_GAPS}
        return original_train(c,*args,**kwargs) if ready else []
    v5_runtime.train_v5=guarded_train

    original_learning=core.learning_tick
    async def learning():
        target=detect_gap_near_cursor(core)
        if target: await repair_gap(core,target)
        await original_learning()
        public=dict(core.state.get('strict_replay_gap_blocker') or {})
        target=detect_gap_near_cursor(core) if public.get('blocked') else None
        repair=None
        if target:
            repair=await repair_gap(core,target)
            if repair.get('status')=='QUARANTINED_UNRECOVERABLE':
                current=int(core.get_state(v5_runtime.REPLAY_STATE_KEY,core.START_TS) or core.START_TS)
                resume=_safe_resume_after_gap(target)
                if resume>current: core.set_state(v5_runtime.REPLAY_STATE_KEY,resume)
                core.state['strict_replay_gap_blocker']={'blocked':False,'quarantined_gap':repair}
        lr=core.state.setdefault('learning',{}); st=_load(core)
        lr['data_resilience']=st; lr['price_gap_summary']=_gap_summary(core); lr['price_gap_repair']=repair
        lr['replay_price_blocker']=core.state.get('strict_replay_gap_blocker') or {}
        db=lr.get('derivative_backfill') or {}; lr['provider_notices']=list(db.get('nonblocking_provider_notices') or []); lr['derivative_errors']=list(db.get('errors') or [])
        if repair and repair.get('status')=='PENDING_REPAIR':
            lr['phase']='WAITING_PRICE_GAP_REPAIR'; lr['blocker']=f"repairing {target['asset']} {target['tf']} gap across Gate/Bybit/Binance/OKX; no interpolation"
        elif repair and repair.get('status')=='REPAIRED':
            lr['phase']='PRICE_GAP_REPAIRED_RESUMING'; lr['blocker']=None
        elif repair and repair.get('status')=='QUARANTINED_UNRECOVERABLE':
            lr['phase']='PRICE_GAP_QUARANTINED_RESUMING'; lr['blocker']=None
        elif not (lr.get('replay_price_blocker') or {}).get('blocked'):
            lr['blocker']=None
    core.learning_tick=learning

    webhook=bool(os.getenv('DISCORD_WEBHOOK_URL','') or getattr(core,'DISCORD_WEBHOOK_URL','')); bot=bool(os.getenv('DISCORD_BOT_TOKEN','') or getattr(core,'DISCORD_BOT_TOKEN','')); channel=bool(os.getenv('DISCORD_CHANNEL_ID','') or getattr(core,'DISCORD_CHANNEL_ID',''))
    if not isinstance(core.state.get('discord'),dict) or not (core.state.get('discord') or {}).get('ok'):
        core.state['discord']={'configured':bool(webhook or (bot and channel)),'ok':False,'route':'pending-first-send' if (webhook or (bot and channel)) else None,'last_success':None,'error':None if (webhook or (bot and channel)) else 'Discord not configured'}

    strict=core.state.setdefault('strict_replay',{})
    strict['data_resilience']={**_load(core),'runtime':VERSION,'schema':SCHEMA,'fixed_price_priority':list(PRICE_PRIORITY),
        'provider_retention_is_capability_not_replay_error':True,'range_limited_derivatives_excluded_from_historical_model':True,
        'internal_price_gaps_auto_repaired_across_exchanges':True,'unrecoverable_price_gaps_never_interpolated':True,
        'unrecoverable_contaminated_windows_omitted_with_audit':True,'max_quarantined_gaps_for_certification':MAX_QUARANTINED_GAPS,
        'future_prices_never_used_as_features':True}
    core.state['runtime_version']=VERSION; core.app.version='8.3.0'

    if not any(getattr(r,'path',None)=='/api/v15/resilience' for r in core.app.router.routes):
        @core.app.get('/api/v15/resilience')
        def status()->dict[str,Any]:
            return {'runtime':VERSION,'source_state':_load(core),'gap_summary':_gap_summary(core),'current_gap':detect_gap_near_cursor(core),
                    'core_ready_through':ready_through(core),'rules':{'fixed_price_priority':list(PRICE_PRIORITY),'future_peeking':False,
                    'synthetic_gap_fill':False,'range_limited_provider_can_block_replay':False,'pending_real_gap_can_block_replay':True,
                    'confirmed_unrecoverable_gap_is_quarantined_not_fabricated':True}}
