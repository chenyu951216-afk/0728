from __future__ import annotations

import math
import os
import statistics
import time
from typing import Any

import derivative_data
import v9_final
import v9_live_parity
import v9_readiness
import v9_training_store


GATE_VERSION = '8.0.3-20260809'
STATE_KEY = 'strict_cg_gate_v1'
ENRICHMENT_FAILURE_LIMIT = max(2, min(6, int(os.getenv('STRICT_CG_ENRICHMENT_DISABLE_AFTER', '2'))))
CORE_METRIC = 'oi_usd'
ENRICHMENT_METRICS = ('liq_long_usd', 'book_imbalance')
ALL_METRICS = (CORE_METRIC,) + ENRICHMENT_METRICS


def _default_state() -> dict[str, Any]:
    return {
        'version': GATE_VERSION,
        'global_disabled': False,
        'global_reason': None,
        'disabled_metrics': {},
        'metrics': {},
        'updated_at': None,
    }


def _load_state(core: Any) -> dict[str, Any]:
    raw = core.get_state(STATE_KEY, None)
    state = _default_state()
    if isinstance(raw, dict):
        state.update(raw)
        state['disabled_metrics'] = dict(raw.get('disabled_metrics') or {})
        state['metrics'] = dict(raw.get('metrics') or {})
    return state


def _save_state(core: Any, state: dict[str, Any]) -> None:
    state['version'] = GATE_VERSION
    state['updated_at'] = int(time.time())
    core.set_state(STATE_KEY, state)
    core.state.setdefault('strict_replay', {})['coinglass_gate'] = state


def _error_for(result: dict[str, Any], metric: str) -> str | None:
    prefix = metric + ':'
    for item in result.get('errors') or []:
        text = str(item)
        if text.startswith(prefix):
            return text[len(prefix):].strip()
    return None


def _is_transient(message: str) -> bool:
    m = message.lower()
    return any(x in m for x in (
        '429', 'rate limit', 'too many requests', 'timeout', 'timed out',
        'connection', 'connecterror', 'readerror', '502', '503', '504',
        'temporarily unavailable', 'service unavailable',
    ))


def _is_auth_or_plan(message: str) -> bool:
    m = message.lower()
    return any(x in m for x in (
        '401', '403', 'unauthorized', 'forbidden', 'invalid api', 'api key',
        'permission', 'subscription', 'upgrade', 'plan', 'access denied',
    ))


def _is_persistent_provider_rejection(message: str) -> bool:
    if _is_transient(message):
        return False
    m = message.lower()
    return _is_auth_or_plan(message) or any(x in m for x in (
        '400', '404', '422', 'invalid parameter', 'parameter error',
        'not supported', 'unsupported', 'does not support',
    ))


def _update_gate_state(core: Any, result: dict[str, Any]) -> dict[str, Any]:
    state = _load_state(core)
    metrics = state.setdefault('metrics', {})
    disabled = state.setdefault('disabled_metrics', {})
    now = int(time.time())

    for metric in ALL_METRICS:
        rec = dict(metrics.get(metric) or {})
        err = _error_for(result, metric)
        if err:
            same = rec.get('last_error') == err
            rec['consecutive_errors'] = int(rec.get('consecutive_errors') or 0) + 1
            rec['same_error_count'] = int(rec.get('same_error_count') or 0) + 1 if same else 1
            rec['last_error'] = err
            rec['last_error_at'] = now
            rec['last_result'] = 'ERROR'
            persistent = _is_persistent_provider_rejection(err)
            rec['persistent_rejection'] = persistent

            if metric == CORE_METRIC and _is_auth_or_plan(err) and rec['same_error_count'] >= ENRICHMENT_FAILURE_LIMIT:
                # A bad/unauthorized CoinGlass key must not freeze six years of learning.
                # In this mode the whole CoinGlass feature family is consistently omitted
                # and the strict model uses exchange-native OI/funding availability instead.
                state['global_disabled'] = True
                state['global_reason'] = f'{metric}: {err}'
                state['global_disabled_at'] = now
            elif metric in ENRICHMENT_METRICS and persistent and rec['same_error_count'] >= ENRICHMENT_FAILURE_LIMIT:
                # Optional enrichment cannot veto the complete replay forever. Once a
                # deterministic provider rejection repeats, omit that feature family for
                # this replay generation and keep its availability flag false everywhere.
                disabled[metric] = {
                    'reason': err,
                    'disabled_at': now,
                    'mode': 'explicit_missingness_for_this_replay',
                }
        else:
            rec['consecutive_errors'] = 0
            rec['same_error_count'] = 0
            rec['last_error'] = None
            rec['last_result'] = 'OK_OR_EMPTY'
            rec['last_success_at'] = now
            rec['persistent_rejection'] = False
        metrics[metric] = rec

    _save_state(core, state)
    return state


def _ready_through(core: Any) -> int | None:
    history = core.derivative_history
    if not getattr(history, 'coinglass_key', ''):
        return None
    state = _load_state(core)
    if state.get('global_disabled'):
        return None

    disabled = state.get('disabled_metrics') or {}
    keys = [('oi_usd', 'cg_cursor:oi_usd')]
    if 'liq_long_usd' not in disabled:
        keys.append(('liq_long_usd', 'cg_cursor:liq_long_usd'))
    if 'book_imbalance' not in disabled:
        keys.append(('book_imbalance', 'cg_cursor:book_imbalance'))

    cursors = []
    for _metric, key in keys:
        value = int(history._get_state(key, core.START_TS) or core.START_TS)
        cursors.append(max(int(core.START_TS), value))
    return min(cursors) if cursors else None


def _gated_extras(core: Any, history: Any, decision_ts: int) -> dict[str, float]:
    state = _load_state(core)
    disabled = state.get('disabled_metrics') or {}
    global_off = bool(state.get('global_disabled'))
    lagged = max(0, int(decision_ts) - int(v9_final.DERIVATIVE_SAFETY_LAG_SECONDS))

    if global_off:
        oi_rows = history._latest_values('oi_coin', lagged, 20 * 3600, 4)
    else:
        oi_rows = history._latest_values('oi_usd', lagged, 20 * 3600, 4) or history._latest_values('oi_coin', lagged, 20 * 3600, 4)
    funding_rows = history._latest_values('funding', int(decision_ts), 16 * 3600, 12)

    liq_enabled = (not global_off) and ('liq_long_usd' not in disabled)
    book_enabled = (not global_off) and ('book_imbalance' not in disabled)
    long_rows = history._latest_values('liq_long_usd', lagged, 12 * 3600, 2) if liq_enabled else []
    short_rows = history._latest_values('liq_short_usd', lagged, 12 * 3600, 2) if liq_enabled else []
    book_rows = history._latest_values('book_imbalance', lagged, 12 * 3600, 2) if book_enabled else []

    def fv(value: Any, default: float = 0.0) -> float:
        try:
            x = float(value)
            return x if math.isfinite(x) else default
        except Exception:
            return default

    oi_change = 0.0
    if len(oi_rows) >= 2:
        newest, oldest = fv(oi_rows[0]['value']), fv(oi_rows[-1]['value'])
        if oldest:
            oi_change = newest / oldest - 1
    funding = statistics.median([fv(x['value']) for x in funding_rows]) if funding_rows else 0.0
    long_liq = fv(long_rows[0]['value']) if long_rows else 0.0
    short_liq = fv(short_rows[0]['value']) if short_rows else 0.0
    total_liq = long_liq + short_liq
    liq_imbalance = (short_liq - long_liq) / max(total_liq, 1e-9) if total_liq else 0.0
    liq_intensity = math.log1p(total_liq) / 25.0 if total_liq else 0.0
    book = fv(book_rows[0]['value']) if book_rows else 0.0

    availability = (
        bool(oi_rows), bool(funding_rows), bool(long_rows and short_rows), bool(book_rows)
    )
    quality_values = [
        float(x['quality'])
        for group in (oi_rows[:1], funding_rows[:1], long_rows[:1], short_rows[:1], book_rows[:1])
        for x in group
    ]
    return {
        'oi_change': oi_change,
        'funding': funding,
        'book_imbalance': book,
        'liquidation_imbalance': liq_imbalance,
        'liquidation_intensity': liq_intensity,
        'oi_available': float(bool(oi_rows)),
        'funding_available': float(bool(funding_rows)),
        'liquidation_available': float(bool(long_rows and short_rows)),
        'book_available': float(bool(book_rows)),
        'derivative_coverage': sum(availability) / 4.0,
        'derivative_quality': (statistics.mean(quality_values) / 100.0) if quality_values else 0.0,
        'historical_derivative_safety_lag_seconds': float(v9_final.DERIVATIVE_SAFETY_LAG_SECONDS),
    }


def install(core: Any) -> None:
    history = core.derivative_history
    original_backfill = history.backfill_tick
    original_status = history.status

    # The official examples use Binance/OKX/Bybit. Avoid a single exchange-name
    # mismatch making the entire aggregated liquidation request fail.
    async def safe_liquidation(start_ts: int, end_ts: int) -> int:
        data = await history._cg('/futures/liquidation/aggregated-history', {
            'exchange_list': 'Binance,OKX,Bybit',
            'symbol': 'ETH', 'interval': '4h', 'limit': 1000,
            'start_time': start_ts * 1000, 'end_time': end_ts * 1000,
        })
        long_rows, short_rows = [], []
        for x in data or []:
            ts = derivative_data._ts_seconds(x.get('time'))
            long_rows.append((ts, derivative_data._f(x.get('aggregated_long_liquidation_usd')), 92.0, {}))
            short_rows.append((ts, derivative_data._f(x.get('aggregated_short_liquidation_usd')), 92.0, {}))
        return history._insert('coinglass', 'liq_long_usd', long_rows) + history._insert('coinglass', 'liq_short_usd', short_rows)

    history._backfill_coinglass_liquidation = safe_liquidation

    async def guarded_backfill(hub: Any, start_ts: int, pages: int = 2) -> dict[str, Any]:
        result = await original_backfill(hub, start_ts, pages)
        gate = _update_gate_state(core, result)
        result['strict_gate'] = gate
        return result

    def guarded_status() -> dict[str, Any]:
        result = original_status()
        result['strict_gate'] = _load_state(core)
        result['strict_ready_through'] = _ready_through(core)
        return result

    history.backfill_tick = guarded_backfill
    history.status = guarded_status

    # v9_readiness' wrapper resolves this module global at call time, so replacing it
    # here changes the production gate without weakening the event-time replay rules.
    v9_readiness._coinglass_ready_through = _ready_through
    v9_final._strict_derivative_extras = lambda h, ts: _gated_extras(core, h, ts)

    # Keep every public runtime/component version aligned with the hotfix.
    v9_final.FINAL_VERSION = GATE_VERSION
    v9_readiness.READINESS_VERSION = GATE_VERSION
    v9_training_store.STORE_VERSION = GATE_VERSION
    v9_live_parity.PARITY_VERSION = GATE_VERSION
    core.state['runtime_version'] = GATE_VERSION
    core.state.setdefault('strict_replay', {})['runtime'] = GATE_VERSION
    core.state['strict_replay']['derivative_gate_policy'] = {
        'core_metric': CORE_METRIC,
        'enrichment_metrics': list(ENRICHMENT_METRICS),
        'persistent_rejection_limit': ENRICHMENT_FAILURE_LIMIT,
        'transient_errors_never_auto_disable': True,
        'global_auth_failure_uses_explicit_missingness_native_fallback': True,
        'rule': 'optional CoinGlass enrichment cannot permanently deadlock strict replay; missing features remain explicit and consistent',
    }
    core.app.version = '8.0.3'

    if not any(getattr(r, 'path', None) == '/api/v9/derivative-gate' for r in core.app.router.routes):
        @core.app.get('/api/v9/derivative-gate')
        def derivative_gate_status() -> dict[str, Any]:
            state = _load_state(core)
            return {
                'runtime': GATE_VERSION,
                'coinglass_enabled': bool(getattr(history, 'coinglass_key', '')),
                'ready_through': _ready_through(core),
                'gate': state,
                'cursors': {
                    metric: max(int(core.START_TS), int(history._get_state(f'cg_cursor:{metric}', core.START_TS) or core.START_TS))
                    for metric in ALL_METRICS
                },
            }
