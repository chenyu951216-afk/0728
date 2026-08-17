from __future__ import annotations

"""Execution fix for Stage 6/current paper.

A stop unsafe at exchange maximum leverage is a leverage-selection problem, not corrupt
historical data. Fixed notional PnL is leverage-independent, so V52 selects the highest
leverage that still keeps the frozen stop inside conservative liquidation headroom.
Only genuine missing/gapped settlement paths remain fail-closed data invalidity.
"""

import json
import os
import time
from types import SimpleNamespace
from typing import Any

import numpy as np

import runtime_identity
import v36_bitget_execution_truth as leverage_truth
import v43_unified_performance_authority as performance
import v51_evolution_survivability_authority as survivability

VERSION = 'V52_SAFE_LEVERAGE_EXECUTION_AUTHORITY'
SCHEMA = 52
STATE_KEY = 'v52_safe_leverage_execution_authority'
LEVERAGE_MODE = 'MAX_SAFE_WITH_STOP_HEADROOM_AT_ORDER_TIME'
SAFE_CUSHION = max(.90, min(.999, float(os.getenv('AUTONOMOUS_V52_SAFE_LEVERAGE_CUSHION', '.98'))))
MIN_LEVERAGE = max(1.0, float(os.getenv('AUTONOMOUS_V52_MIN_EXECUTION_LEVERAGE', '1.0')))
DATA_PATH_REASONS = {
    'missing_decision_close', 'missing_first_future_5m', 'future_5m_gap',
    'no_future_path', 'incomplete_full_evolved_holding_horizon',
}
_INSTALLED = False
_BASE_FAST = None
_BASE_CREATE = None


def _now() -> int:
    return int(time.time())


def _state(core: Any, **patch: Any) -> None:
    raw = core.state.get(STATE_KEY)
    out = dict(raw) if isinstance(raw, dict) else {}
    out.update(patch)
    out.update({'schema': SCHEMA, 'runtime': VERSION,
                'public_runtime': runtime_identity.RUNTIME_VERSION,
                'updated_at': _now()})
    core.state[STATE_KEY] = out


def safe_leverage(contract: dict[str, Any], stop_fraction: float) -> tuple[float, float]:
    exchange_max = float(contract.get('effective_max_leverage') or 0.0)
    mmr = max(0.0, float(contract.get('maintenance_margin_rate') or 0.0))
    buffer = float(getattr(leverage_truth, 'EXTRA_MARGIN_BUFFER', .0015))
    denom = max(float(stop_fraction) + mmr + buffer, 1e-9)
    selected = min(exchange_max, SAFE_CUSHION / denom) if exchange_max > 0 else SAFE_CUSHION / denom
    selected = max(0.0, float(selected))
    headroom = max(0.0, 1.0 / max(selected, 1e-12) - mmr - buffer) if selected > 0 else 0.0
    return selected, headroom


def _install_reason_classifier() -> None:
    def reason_counts(results, counter):
        attempted = invalid_data = 0
        for item in results:
            attempted += 1
            reason = str(item.get('reason') or ('filled' if item.get('filled') else 'unknown'))
            counter[reason] += 1
            if reason in DATA_PATH_REASONS:
                invalid_data += 1
        return attempted, invalid_data
    survivability._reason_counts = reason_counts


def install(production: Any, autonomous: Any, throughput: Any, integrity: Any) -> None:
    global _INSTALLED, _BASE_FAST, _BASE_CREATE
    if _INSTALLED:
        return
    _INSTALLED = True
    core = production.core

    mods = tuple(getattr(integrity, 'SEMANTIC_MODULES', ()))
    if 'v52_execution_authority' not in mods:
        integrity.SEMANTIC_MODULES = mods + ('v52_execution_authority',)
    _install_reason_classifier()

    _BASE_FAST = performance._fast_trade

    def safe_trade(market: dict[str, Any], ts: int, features: np.ndarray,
                   genome: dict[str, Any]) -> dict[str, Any]:
        contract = dict(leverage_truth._frozen_contract(core, autonomous, create=False) or {})
        if not contract.get('ok'):
            return {'valid': False, 'filled': False, 'pnl_r': 0.0,
                    'reason': 'frozen_bitget_max_leverage_contract_unavailable'}
        try:
            atr_pct = max(abs(float(features[autonomous.FEATURE_INDEX['atr_pct']])), .00035)
        except Exception:
            atr_pct = .00035
        stop_fraction = max(float(genome.get('stop_atr') or 0.0) * atr_pct, .0008)
        selected, headroom = safe_leverage(contract, stop_fraction)
        if selected < MIN_LEVERAGE or headroom <= stop_fraction:
            return {
                'valid': False, 'filled': False, 'pnl_r': 0.0,
                'reason': 'initial_stop_unsafe_even_after_safe_leverage_selection',
                'stop_fraction': stop_fraction, 'selected_leverage': selected,
                'safe_headroom_fraction': headroom,
            }

        frozen = dict(contract)
        frozen['exchange_max_leverage'] = float(contract.get('effective_max_leverage') or 0.0)
        frozen['effective_max_leverage'] = selected
        frozen['conservative_stop_headroom_fraction'] = headroom
        proxy = SimpleNamespace(_frozen_contract=lambda _c, _a, create=False: dict(frozen))
        out = dict(_BASE_FAST(core, autonomous, proxy, market, int(ts), features, genome) or {})
        out.update({
            'exchange_max_leverage': float(contract.get('effective_max_leverage') or 0.0),
            'selected_leverage': selected, 'safe_headroom_fraction': headroom,
            'stop_fraction': stop_fraction, 'leverage_mode': LEVERAGE_MODE,
        })
        return out

    autonomous._simulate_trade = safe_trade
    # V46 captured the old scalar simulator. Rebuild only dispatch so the new exact run
    # uses V52 semantics; history/features/folds/population are unchanged.
    throughput._install_parallel(core, autonomous)

    current_create = core.create_signal
    base_create = None
    try:
        code = current_create.__code__
        cells = current_create.__closure__ or ()
        base_create = {n: c.cell_contents for n, c in zip(code.co_freevars, cells)}.get('base_create')
    except Exception:
        pass
    if not callable(base_create):
        base_create = lambda analysis, m15: autonomous._autonomous_create_signal(core, analysis, m15)
    _BASE_CREATE = base_create

    def safe_create(analysis: dict[str, Any], m15: list[dict[str, Any]]):
        contract = dict(leverage_truth._fetch_contract(float(autonomous.PAPER_NOTIONAL_USDT), force=True) or {})
        if not contract.get('ok'):
            return None
        genome = dict((analysis.get('selection') or {}).get('genome') or {})
        selected = float(contract.get('effective_max_leverage') or 0.0)
        headroom = float(contract.get('conservative_stop_headroom_fraction') or 0.0)
        stop_fraction = None
        if genome:
            atr_pct = max(abs(float((analysis.get('features') or {}).get('atr_pct') or .00035)), .00035)
            stop_fraction = max(float(genome.get('stop_atr') or 0.0) * atr_pct, .0008)
            selected, headroom = safe_leverage(contract, stop_fraction)
            if selected < MIN_LEVERAGE or headroom <= stop_fraction:
                return None
        created = _BASE_CREATE(analysis, m15)
        if not created:
            return created
        payload = dict(created.get('payload') or {})
        payload.update({
            'bitget_execution_contract': contract,
            'exchange_max_leverage': float(contract.get('effective_max_leverage') or 0.0),
            'selected_leverage': selected, 'leverage': selected,
            'safe_headroom_fraction': headroom, 'stop_fraction': stop_fraction,
            'leverage_mode': LEVERAGE_MODE, 'paper_only': True,
        })
        con = core.db()
        try:
            con.execute('UPDATE signals SET payload=? WHERE signal_id=?',
                        (json.dumps(payload, ensure_ascii=False), created['signal_id']))
            con.commit()
        finally:
            con.close()
        return core.latest_signal()

    core.create_signal = safe_create
    core._notional_for_risk = lambda entry, stop: {
        'notional_usdt': float(autonomous.PAPER_NOTIONAL_USDT),
        'stop_pct': abs(float(entry)-float(stop))/max(float(entry),1e-9),
        'leverage_mode': LEVERAGE_MODE, 'paper_only': True,
    }

    core.state.setdefault('strict_replay', {})['v52_safe_leverage_execution'] = {
        'schema': SCHEMA,
        'fixed_notional_pnl_independent_of_leverage': True,
        'exchange_max_leverage_forced_when_stop_would_liquidate_first': False,
        'highest_stop_safe_leverage_selected': True,
        'future_price_used_for_leverage_selection': False,
        'genuine_missing_settlement_paths_fail_closed': True,
        'final_oos_thresholds_relaxed': False,
    }
    _state(core, installed=True, leverage_mode=LEVERAGE_MODE,
           final_oos_thresholds_relaxed=False, future_peeking_enabled=False)
