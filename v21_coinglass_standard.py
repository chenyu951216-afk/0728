from __future__ import annotations

import json
import math
import os
import statistics
import time
from typing import Any

import execution_v7 as execution
import v7_runtime
import v10_final_integrity as final_integrity
import v13_replay_cursor_integrity as cursor_guard
import v15_data_resilience as resilience


VERSION = '9.2.0-20260812'
FEATURE_SCHEMA = 7
FEATURE_SCHEMA_KEY = 'coinglass_standard_feature_schema'
STATE_KEY = 'coinglass_standard_v1'
HEATMAP_TTL_SECONDS = max(120, min(1800, int(os.getenv('COINGLASS_HEATMAP_TTL_SECONDS', '600'))))
HEATMAP_STALE_SECONDS = max(HEATMAP_TTL_SECONDS, int(os.getenv('COINGLASS_HEATMAP_STALE_SECONDS', '1200')))
HEATMAP_STOP_BAND_R = max(.04, min(.30, float(os.getenv('COINGLASS_HEATMAP_STOP_BAND_R', '.12'))))
COINGLASS_PLAN = str(os.getenv('COINGLASS_PLAN', 'STANDARD')).strip().upper()

# Official API V4 Standard-plan datasets used by this runtime. Historical series may
# still have provider-specific retention limits; range-limited data is collected but
# excluded from historical models until it can satisfy the fixed full-span contract.
CAPABILITIES: dict[str, dict[str, Any]] = {
    'open_interest': {'endpoint': '/api/futures/open-interest/aggregated-history', 'use': 'historical_signal'},
    'liquidation_history': {'endpoint': '/api/futures/liquidation/aggregated-history', 'use': 'historical_signal'},
    'orderbook_depth': {'endpoint': '/api/futures/orderbook/aggregated-ask-bids-history', 'use': 'historical_signal'},
    'oi_weighted_funding': {'endpoint': '/api/futures/funding-rate/oi-weight-history', 'use': 'historical_signal'},
    'taker_buy_sell': {'endpoint': '/api/futures/aggregated-taker-buy-sell-volume/history', 'use': 'historical_signal'},
    'global_long_short': {'endpoint': '/api/futures/global-long-short-account-ratio/history', 'use': 'historical_signal'},
    'top_position_ratio': {'endpoint': '/api/futures/top-long-short-position-ratio/history', 'use': 'historical_signal'},
    'liquidation_heatmap': {
        'endpoint': '/api/futures/liquidation/heatmap/model1',
        'use': 'live_veto_and_snapshot_only', 'standard_available': False,
        'minimum_plan': 'PROFESSIONAL',
    },
}

STANDARD_SERIES = (
    ('cg_oi_funding', 'oi_weighted_funding', '_backfill_coinglass_oi_weighted_funding'),
    ('cg_taker', 'taker_imbalance', '_backfill_coinglass_taker'),
    ('cg_crowd', 'crowd_skew', '_backfill_coinglass_crowd_ratio'),
    ('cg_top_position', 'top_position_skew', '_backfill_coinglass_top_position_ratio'),
)


def _state(core: Any) -> dict[str, Any]:
    raw = core.get_state(STATE_KEY, None)
    return dict(raw) if isinstance(raw, dict) else {
        'runtime': VERSION, 'feature_schema': FEATURE_SCHEMA, 'capabilities': CAPABILITIES,
        'historical_series': {}, 'heatmap': {}, 'execution_gate': {},
    }


def _save(core: Any, state: dict[str, Any]) -> None:
    state.update({'runtime': VERSION, 'feature_schema': FEATURE_SCHEMA, 'updated_at': int(time.time())})
    core.set_state(STATE_KEY, state)
    core.state['coinglass_standard'] = state


def _reset_feature_generation(core: Any) -> None:
    if int(core.get_state(FEATURE_SCHEMA_KEY, 0) or 0) >= FEATURE_SCHEMA:
        return
    cursor_guard._reset_derived_replay(
        core,
        'schema 7: CoinGlass Standard OI-weighted funding, taker flow and crowd positioning features',
    )
    source = resilience._load(core)
    source.update({
        'source_set_frozen': False, 'model_oi_sources': [], 'model_funding_sources': [],
        'model_enrichment_sources': [], 'enrichment_mode': 'PENDING_STANDARD_CAPABILITY_AUDIT',
        'migration_at': int(time.time()),
        'migration_reason': 'derived samples only; raw market/derivative caches and CLEAN dataset identity preserved',
    })
    resilience._save(core, source)
    coverage = final_integrity._load(core)
    coverage.update({'core_frozen': False, 'frozen_enrichment': [], 'all_sources_upgrade_applied': True})
    final_integrity._save(core, coverage)
    core.set_state(FEATURE_SCHEMA_KEY, FEATURE_SCHEMA)


def _heatmap_zones(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {'zones': [], 'reference_price': None, 'reason': 'invalid heatmap response'}
    y_axis: list[float | None] = []
    for value in payload.get('y_axis') or []:
        try:
            parsed_value = float(value)
            y_axis.append(parsed_value if math.isfinite(parsed_value) else None)
        except (TypeError, ValueError):
            y_axis.append(None)
    points = payload.get('liquidation_leverage_data') or []
    candles = payload.get('price_candlesticks') or []
    reference = None
    if candles and isinstance(candles[-1], (list, tuple)) and len(candles[-1]) >= 5:
        try:
            reference = float(candles[-1][4])
        except (TypeError, ValueError):
            reference = None
    parsed: list[tuple[int, int, float]] = []
    for row in points:
        try:
            x_idx, y_idx, intensity = int(row[0]), int(row[1]), float(row[2])
            if 0 <= y_idx < len(y_axis) and y_axis[y_idx] is not None and intensity > 0 and math.isfinite(intensity):
                parsed.append((x_idx, y_idx, intensity))
        except (TypeError, ValueError, IndexError):
            continue
    if not parsed:
        return {'zones': [], 'reference_price': reference, 'reason': 'no usable heatmap levels'}
    max_x = max(x[0] for x in parsed)
    recent_cut = max(0, max_x - max(8, int((max_x + 1) * .35)))
    by_level: dict[int, float] = {}
    for x_idx, y_idx, intensity in parsed:
        if x_idx >= recent_cut:
            by_level[y_idx] = by_level.get(y_idx, 0.0) + intensity
    ranked = sorted(by_level.items(), key=lambda item: item[1], reverse=True)[:16]
    peak = max((x[1] for x in ranked), default=1.0)
    zones = [
        {'price': round(float(y_axis[idx]), 4), 'intensity': value, 'relative_intensity': value / peak}
        for idx, value in ranked
    ]
    zones.sort(key=lambda x: x['price'])
    return {
        'zones': zones, 'reference_price': reference, 'source_range': '3d',
        'x_cutoff': recent_cut, 'point_count': len(parsed),
        'historical_backtest_eligible': False,
        'rule': 'current heatmap is snapshotted for prospective evidence and may veto, never rewrite, an audited plan',
    }


def _store_heatmap_snapshot(core: Any, heatmap: dict[str, Any], observed_at: int) -> None:
    con = core.db()
    try:
        con.execute('''CREATE TABLE IF NOT EXISTS coinglass_structure_snapshots(
            ts INTEGER PRIMARY KEY, reference_price REAL, payload TEXT NOT NULL
        )''')
        con.execute(
            'INSERT OR REPLACE INTO coinglass_structure_snapshots(ts,reference_price,payload) VALUES(?,?,?)',
            (observed_at, heatmap.get('reference_price'), json.dumps(heatmap, ensure_ascii=False, separators=(',', ':'))),
        )
        con.commit()
    finally:
        con.close()


async def _refresh_heatmap(core: Any, force: bool = False) -> dict[str, Any]:
    state = _state(core)
    previous = dict(state.get('heatmap') or {})
    now = int(time.time())
    if not force and now - int(previous.get('observed_at') or 0) < HEATMAP_TTL_SECONDS:
        return previous
    if COINGLASS_PLAN not in ('PROFESSIONAL', 'ENTERPRISE'):
        result = {
            'available': False, 'observed_at': now, 'mode': 'PLAN_UNAVAILABLE',
            'reason': 'CoinGlass liquidation heatmap requires Professional or Enterprise; Standard is not queried',
        }
    elif not getattr(core.derivative_history, 'coinglass_key', ''):
        result = {'available': False, 'observed_at': now, 'reason': 'CoinGlass key not configured'}
    else:
        try:
            raw = await core.derivative_history.coinglass_liquidation_heatmap('3d')
            result = {**_heatmap_zones(raw), 'available': True, 'observed_at': now}
            _store_heatmap_snapshot(core, result, now)
        except Exception as exc:
            result = {'available': False, 'observed_at': now, 'reason': f'{type(exc).__name__}: {exc}'}
    state['heatmap'] = result
    _save(core, state)
    return result


def liquidation_stop_gate(core: Any, plan: dict[str, Any]) -> dict[str, Any]:
    heatmap = dict((_state(core).get('heatmap') or {}))
    now = int(time.time())
    if not heatmap.get('available') or now - int(heatmap.get('observed_at') or 0) > HEATMAP_STALE_SECONDS:
        return {'allowed': True, 'mode': 'OPTIONAL_UNAVAILABLE', 'reason': 'fresh CoinGlass heatmap unavailable; audited plan remains unchanged'}
    entry = float(plan.get('entry') or 0); stop = float(plan.get('stop') or 0)
    risk = abs(entry - stop)
    if entry <= 0 or risk <= 0:
        return {'allowed': False, 'mode': 'INVALID_PLAN', 'reason': 'execution plan has invalid entry/stop distance'}
    reference = float(heatmap.get('reference_price') or entry)
    if abs(reference - entry) / entry > .03:
        return {'allowed': True, 'mode': 'REFERENCE_MISMATCH', 'reason': 'heatmap reference is too far from the audited plan; optional veto skipped'}
    adverse = [
        z for z in heatmap.get('zones') or []
        if (stop <= float(z['price']) <= entry or entry <= float(z['price']) <= stop)
        and float(z.get('relative_intensity') or 0) >= .55
    ]
    nearest = min(adverse, key=lambda z: abs(float(z['price']) - stop), default=None)
    distance_r = abs(float(nearest['price']) - stop) / risk if nearest else None
    blocked = bool(nearest and distance_r is not None and distance_r <= HEATMAP_STOP_BAND_R)
    return {
        'allowed': not blocked, 'mode': 'LIVE_VETO_ONLY', 'observed_at': heatmap.get('observed_at'),
        'nearest_dense_zone': nearest, 'distance_from_stop_r': distance_r,
        'stop_band_r': HEATMAP_STOP_BAND_R,
        'reason': 'audited stop overlaps a dense current liquidation band; skip this order rather than mutate SL' if blocked else 'audited plan does not overlap a dense current liquidation band',
        'plan_mutated': False, 'historical_model_feature': False,
    }


def install(core: Any) -> None:
    _reset_feature_generation(core)
    original_backfill = core.derivative_history.backfill_tick

    async def standard_backfill(hub: Any, start_ts: int, pages: int = 4) -> dict[str, Any]:
        base = dict(await original_backfill(hub, start_ts, pages) or {})
        normalized: list[dict[str, Any]] = []
        # Keep Standard-plan requests serialized. This consumes the same useful data
        # without a four-request burst that can amplify 429s near a plan limit.
        for key, metric, method in STANDARD_SERIES:
            try:
                result = await resilience.range_safe_cg(
                    core, key, metric, getattr(core.derivative_history, method),
                )
                normalized.append(dict(result))
            except Exception as exc:
                normalized.append({'source': key, 'error': f'{type(exc).__name__}: {exc}'})
        heatmap = await _refresh_heatmap(core)
        state = _state(core)
        state.update({'historical_series': {str(x.get('source') or 'error'): x for x in normalized}, 'heatmap': heatmap})
        _save(core, state)
        base['coinglass_standard'] = {'series': normalized, 'heatmap': heatmap, 'capabilities': CAPABILITIES}
        return base

    core.derivative_history.backfill_tick = standard_backfill

    original_create_v7 = v7_runtime.create_signal_v7

    def coinglass_guarded_create(core_obj: Any, analysis: dict[str, Any], m15: list[dict[str, Any]]):
        selection = analysis.get('selection') or {}
        ex = selection.get('execution') or {}
        policy = ex.get('policy')
        if selection.get('tradeable') and policy and m15:
            m30, h1 = v7_runtime._load_structure_context(core_obj)
            plan = execution.plan_from_policy(
                selection['strategy'], selection['direction'], float(analysis['price']),
                m15, policy, m30, h1,
            )
            gate = liquidation_stop_gate(core_obj, plan)
            analysis['coinglass_execution_gate'] = gate
            state = _state(core_obj); state['execution_gate'] = {**gate, 'checked_at': int(time.time())}; _save(core_obj, state)
            if not gate.get('allowed'):
                return None
        return original_create_v7(core_obj, analysis, m15)

    v7_runtime.create_signal_v7 = coinglass_guarded_create
    state = _state(core)
    state.update({
        'capabilities': CAPABILITIES, 'feature_schema': FEATURE_SCHEMA,
        'rules': {
            'range_limited_history_can_train': False,
            'current_heatmap_can_rewrite_historical_features': False,
            'current_heatmap_can_mutate_audited_stop': False,
            'current_heatmap_can_veto_new_order': COINGLASS_PLAN in ('PROFESSIONAL', 'ENTERPRISE'),
            'snapshots_collected_for_future_untouched_execution_learning': COINGLASS_PLAN in ('PROFESSIONAL', 'ENTERPRISE'),
            'configured_plan': COINGLASS_PLAN,
            'standard_stop_learning_inputs': 'historical MAE/MFE + liquidation history + orderbook + taker flow + positioning; heatmap is unavailable on Standard',
        },
    })
    _save(core, state)
    core.state.setdefault('strict_replay', {})['coinglass_standard'] = state['rules']

    if not any(getattr(route, 'path', None) == '/api/v21/coinglass-standard' for route in core.app.router.routes):
        @core.app.get('/api/v21/coinglass-standard')
        def coinglass_standard_status() -> dict[str, Any]:
            return _state(core)
