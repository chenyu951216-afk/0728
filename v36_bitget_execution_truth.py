from __future__ import annotations

"""Bitget ETHUSDT execution-contract truth for fixed-notional paper research.

The requested leverage mode is MAX_AVAILABLE_AT_ORDER_TIME. With a fixed 20,000-USDT
notional, leverage does not multiply position PnL; it changes required margin and
liquidation headroom. Autonomous research therefore rejects packages whose learned
initial stop would sit beyond a conservative margin headroom at the real Bitget tier,
rather than pretending liquidation cannot happen.

The Bitget contract/tier configuration is frozen once for a research run. It is an
execution constraint, never a market feature, and cannot leak future prices into a
historical decision. Live paper creation rechecks the current Bitget tier and fails
closed if the previously certified stop is no longer safe under maximum leverage.
"""

import json
import math
import time
from typing import Any

import httpx

import v5_runtime
import v17_certification_orchestrator as cert17
import v18_final_system as final_system
import v18_operational_guard as operational_guard

SCHEMA = 36
STATE_KEY = 'v36_bitget_execution_truth'
FROZEN_KEY = 'v36_frozen_research_execution_contract'
CACHE_SECONDS = 900
REQUEST_TIMEOUT_SECONDS = 6.0
EXTRA_MARGIN_BUFFER = 0.0015  # fee/slippage/mark-price cushion
_INSTALLED = False
_CACHE: dict[str, Any] = {}


def _f(x: Any, default: float = 0.0) -> float:
    try:
        v = float(x)
        return v if math.isfinite(v) else default
    except (TypeError, ValueError):
        return default


def _fetch_contract(notional: float, *, force: bool = False) -> dict[str, Any]:
    now = int(time.time())
    if not force and _CACHE and now - int(_CACHE.get('fetched_at') or 0) < CACHE_SECONDS:
        return dict(_CACHE)
    try:
        with httpx.Client(timeout=REQUEST_TIMEOUT_SECONDS, headers={'User-Agent': 'eth-adaptive-autonomous/36'}) as client:
            cfg = client.get('https://api.bitget.com/api/v2/mix/market/contracts', params={'productType': 'USDT-FUTURES', 'symbol': 'ETHUSDT'})
            cfg.raise_for_status(); cfgj = cfg.json()
            tiers = client.get('https://api.bitget.com/api/v2/mix/market/query-position-lever', params={'productType': 'USDT-FUTURES', 'symbol': 'ETHUSDT'})
            tiers.raise_for_status(); tierj = tiers.json()
        if str(cfgj.get('code')) != '00000' or not cfgj.get('data'):
            raise RuntimeError(f"contract config code={cfgj.get('code')} msg={cfgj.get('msg')}")
        if str(tierj.get('code')) != '00000' or not tierj.get('data'):
            raise RuntimeError(f"position tier code={tierj.get('code')} msg={tierj.get('msg')}")
        contract = cfgj['data'][0]; contract_max = _f(contract.get('maxLever'))
        chosen = None
        for row in tierj['data']:
            start = _f(row.get('startUnit')); end = _f(row.get('endUnit'))
            if float(notional) >= start and (end <= 0 or float(notional) < end):
                chosen = row; break
        if chosen is None:
            chosen = tierj['data'][-1]
        tier_max = _f(chosen.get('leverage')); mmr = max(0.0, _f(chosen.get('keepMarginRate')))
        positive = [x for x in (contract_max, tier_max) if x > 0]
        if not positive:
            raise RuntimeError('Bitget returned no positive leverage bound')
        leverage = min(positive)
        raw_headroom = max(0.0, 1.0 / leverage - mmr)
        conservative_headroom = max(0.0, raw_headroom - EXTRA_MARGIN_BUFFER)
        if conservative_headroom <= 0:
            raise RuntimeError('maximum leverage leaves no conservative liquidation headroom')
        result = {
            'schema': SCHEMA, 'ok': True, 'symbol': 'ETHUSDT', 'product_type': 'USDT-FUTURES',
            'notional_usdt': float(notional), 'contract_max_leverage': contract_max,
            'tier_max_leverage': tier_max, 'effective_max_leverage': leverage,
            'maintenance_margin_rate': mmr, 'tier_start_notional': _f(chosen.get('startUnit')),
            'tier_end_notional': _f(chosen.get('endUnit')), 'raw_margin_headroom_fraction': raw_headroom,
            'conservative_stop_headroom_fraction': conservative_headroom,
            'headroom_model': '1/maxLeverage - maintenanceMarginRate - 0.15% safety cushion; conservative research guard, not an exchange liquidation-price quote',
            'source': 'BITGET_PUBLIC_CONTRACT_CONFIG_AND_POSITION_TIER', 'fetched_at': now,
        }
    except Exception as exc:
        result = {'schema': SCHEMA, 'ok': False, 'symbol': 'ETHUSDT', 'notional_usdt': float(notional), 'error': f'{type(exc).__name__}: {exc}', 'fetched_at': now}
    _CACHE.clear(); _CACHE.update(result)
    return dict(result)


def _frozen_contract(core: Any, autonomous: Any, *, create: bool = False) -> dict[str, Any]:
    raw = core.get_state(FROZEN_KEY, None)
    if isinstance(raw, dict) and raw.get('ok') and float(raw.get('notional_usdt') or 0.0) == float(autonomous.PAPER_NOTIONAL_USDT):
        return dict(raw)
    if not create:
        return {}
    fresh = _fetch_contract(float(autonomous.PAPER_NOTIONAL_USDT), force=True)
    if not fresh.get('ok'):
        return fresh
    frozen = {
        **fresh,
        'frozen_at': int(time.time()),
        'research_start_ts': int(autonomous.RESEARCH_START_TS),
        'research_end_exclusive_ts': int(autonomous.RESEARCH_END_EXCLUSIVE_TS),
        'purpose': 'CURRENT_BITGET_MAX_LEVERAGE_EXECUTION_CONTRACT_APPLIED_TO_HISTORICAL_PRICE_RESEARCH',
        'not_a_market_feature': True,
    }
    core.set_state(FROZEN_KEY, frozen)
    return frozen


def install(production: Any, autonomous: Any) -> None:
    global _INSTALLED
    if _INSTALLED:
        return
    _INSTALLED = True
    core = production.core

    base_simulate = autonomous._simulate_trade
    def leverage_safe_simulate(market: dict[str, Any], ts: int, features: Any, genome: dict[str, Any]):
        contract = _frozen_contract(core, autonomous, create=False)
        if not contract.get('ok'):
            return {'valid': False, 'filled': False, 'pnl_r': 0.0, 'reason': 'frozen_bitget_max_leverage_contract_unavailable'}
        close = (market.get('close15') or {}).get(int(ts))
        if close is None or float(close) <= 0:
            return {'valid': False, 'filled': False, 'pnl_r': 0.0, 'reason': 'missing_decision_close'}
        try:
            atr_idx = autonomous.FEATURE_INDEX['atr_pct']; atr_pct = max(abs(float(features[atr_idx])), .00035)
        except Exception:
            atr_pct = .00035
        stop_fraction = max(float(genome.get('stop_atr') or 0.0) * atr_pct, .0008)
        headroom = float(contract.get('conservative_stop_headroom_fraction') or 0.0)
        if headroom <= 0 or stop_fraction >= headroom:
            return {
                'valid': False, 'filled': False, 'pnl_r': 0.0,
                'reason': 'initial_stop_outside_conservative_max_leverage_headroom',
                'stop_fraction': stop_fraction, 'max_safe_fraction': headroom,
            }
        out = base_simulate(market, ts, features, genome)
        if isinstance(out, dict):
            out = dict(out); out['max_leverage'] = float(contract['effective_max_leverage']); out['maintenance_margin_rate'] = float(contract['maintenance_margin_rate'])
        return out
    autonomous._simulate_trade = leverage_safe_simulate

    # Freeze Bitget execution truth before expensive research. A transient external
    # outage causes WAITING, not a fake zero-strategy research result.
    base_certify = final_system.certify_and_execute
    def leverage_truth_certify(c: Any, force: bool = False):
        contract = _frozen_contract(c, autonomous, create=True); c.state[STATE_KEY] = {**contract, 'research_contract_frozen': bool(contract.get('ok')), 'updated_at': int(time.time())}
        if not contract.get('ok'):
            c.state[autonomous.STATE_KEY] = {'schema': autonomous.SCHEMA, 'status': 'WAITING_BITGET_EXECUTION_CONTRACT', 'reason': contract.get('error'), 'updated_at': int(time.time())}
            return []
        return base_certify(c, force)
    final_system.certify_and_execute = leverage_truth_certify
    operational_guard.certify_and_execute = leverage_truth_certify
    cert17.train_v17 = leverage_truth_certify
    v5_runtime.train_v5 = leverage_truth_certify
    core.train_if_due = lambda force=False: leverage_truth_certify(core, force)

    base_create = core.create_signal
    def leverage_safe_create(analysis: dict[str, Any], m15: list[dict[str, Any]]):
        # Live order time always rechecks the public contract. If Bitget changes its
        # tier/max leverage, a strategy is withheld rather than assuming old headroom.
        contract = _fetch_contract(float(autonomous.PAPER_NOTIONAL_USDT), force=True); core.state[STATE_KEY] = {**contract, 'research_contract_frozen': bool(_frozen_contract(core, autonomous)), 'updated_at': int(time.time())}
        if not contract.get('ok'):
            return None
        sel = analysis.get('selection') or {}; genome = sel.get('genome') or {}
        if genome and m15:
            atr_pct = max(abs(_f((analysis.get('features') or {}).get('atr_pct'), .00035)), .00035)
            stop_fraction = max(float(genome.get('stop_atr') or 0.0) * atr_pct, .0008)
            if stop_fraction >= float(contract.get('conservative_stop_headroom_fraction') or 0.0):
                return None
        created = base_create(analysis, m15)
        if not created:
            return created
        payload = created['payload']; payload['bitget_execution_contract'] = contract; payload['leverage'] = float(contract['effective_max_leverage']); payload['leverage_mode'] = 'MAX_AVAILABLE_AT_ORDER_TIME'
        con = core.db()
        try:
            con.execute('UPDATE signals SET payload=? WHERE signal_id=?', (json.dumps(payload, ensure_ascii=False), created['signal_id'])); con.commit()
        finally:
            con.close()
        return core.latest_signal()
    core.create_signal = leverage_safe_create

    core.state[STATE_KEY] = {'schema': SCHEMA, 'ok': False, 'status': 'WAITING_FIRST_BITGET_PUBLIC_CONTRACT_PROBE', 'notional_usdt': float(autonomous.PAPER_NOTIONAL_USDT), 'leverage_mode': 'MAX_AVAILABLE_AT_ORDER_TIME', 'updated_at': int(time.time())}
    core.state['autonomous_leverage_contract'] = {
        'schema': SCHEMA, 'fixed_notional_usdt': float(autonomous.PAPER_NOTIONAL_USDT),
        'leverage_mode': 'MAX_AVAILABLE_AT_ORDER_TIME', 'leverage_does_not_multiply_fixed_notional_pnl': True,
        'max_leverage_changes_margin_and_liquidation_headroom': True,
        'research_rejects_stop_outside_conservative_headroom': True,
        'research_contract_frozen_for_reproducibility': True,
        'live_contract_rechecked_at_order_time': True,
        'bitget_public_contract_truth_required': True,
        'updated_at': int(time.time()),
    }

    if not any(getattr(r, 'path', None) == '/api/v36/bitget-execution' for r in core.app.router.routes):
        @core.app.get('/api/v36/bitget-execution')
        def bitget_execution() -> dict[str, Any]:
            live = _fetch_contract(float(autonomous.PAPER_NOTIONAL_USDT), force=True); frozen = _frozen_contract(core, autonomous, create=False); core.state[STATE_KEY] = {**live, 'frozen_research_contract': frozen, 'updated_at': int(time.time())}; return {'live': live, 'frozen_research': frozen, 'schema': SCHEMA}
