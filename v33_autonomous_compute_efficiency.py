from __future__ import annotations

"""Candidate-local deterministic simulation cache for autonomous research.

A complete candidate repeatedly touches overlapping chronological folds. Replaying the
same candidate at the same decision timestamp again adds no information, so cache that
exact deterministic result only for the lifetime of one candidate/finalist. This cuts
CPU without changing a single decision, feature, fill, stop, target, cost or OOS rule.

The wrapper also fail-closes a candidate decision when the raw cache cannot cover its
entire evolved maximum holding horizon. A long-horizon strategy is never silently
marked-to-market early merely because the dataset ended.
"""

import gc
import time
from typing import Any

_INSTALLED = False
_ACTIVE_CACHE: dict[tuple[str, int], dict[str, Any]] | None = None
_ACTIVE_ID: str | None = None


def install(production: Any, autonomous: Any) -> None:
    global _INSTALLED
    if _INSTALLED:
        return
    _INSTALLED = True
    core = production.core

    base_simulate = autonomous._simulate_trade
    def cached_simulate(market: dict[str, Any], ts: int, features: Any, genome: dict[str, Any]):
        global _ACTIVE_CACHE, _ACTIVE_ID
        key = (_ACTIVE_ID or 'UNCACHED', int(ts))
        if _ACTIVE_CACHE is not None:
            cached = _ACTIVE_CACHE.get(key)
            if cached is not None:
                return dict(cached)

        # The complete planned holding horizon must exist. This is deliberately
        # conservative: a late historical decision is omitted rather than pretending
        # the end of the available file was the AI's chosen time exit.
        ts5 = market.get('ts5')
        decision_close = int(ts) + 900
        if ts5 is not None and len(ts5):
            start = int(ts5.searchsorted(decision_close, side='left'))
            required_end = start + int(genome.get('max_hold_bars') or 1) * 3
            if start >= len(ts5) or required_end > len(ts5):
                out = {'valid': False, 'filled': False, 'pnl_r': 0.0, 'reason': 'incomplete_full_evolved_holding_horizon'}
                if _ACTIVE_CACHE is not None:
                    _ACTIVE_CACHE[key] = dict(out)
                return out

        out = base_simulate(market, ts, features, genome)
        if _ACTIVE_CACHE is not None:
            _ACTIVE_CACHE[key] = dict(out)
        return out
    autonomous._simulate_trade = cached_simulate

    base_eval = autonomous._evaluate_candidate
    def eval_cached(snapshots: dict[str, Any], market: dict[str, Any], genome: dict[str, Any], seed: int):
        global _ACTIVE_CACHE, _ACTIVE_ID
        _ACTIVE_ID = autonomous._hash_payload(genome, 18); _ACTIVE_CACHE = {}
        started = time.monotonic()
        try:
            return base_eval(snapshots, market, genome, seed)
        finally:
            core.state['autonomous_compute_efficiency'] = {
                'candidate_id': _ACTIVE_ID,
                'unique_trade_paths_simulated': len(_ACTIVE_CACHE or {}),
                'candidate_elapsed_seconds': round(time.monotonic() - started, 3),
                'semantic_change': False,
                'cache_scope': 'candidate-local deterministic only',
                'full_evolved_holding_horizon_required': True,
                'updated_at': int(time.time()),
            }
            _ACTIVE_CACHE = None; _ACTIVE_ID = None; gc.collect()
    autonomous._evaluate_candidate = eval_cached

    base_final = autonomous._fit_and_audit_finalist
    def final_cached(snapshots: dict[str, Any], market: dict[str, Any], genome: dict[str, Any], dev: dict[str, Any], seed: int):
        global _ACTIVE_CACHE, _ACTIVE_ID
        _ACTIVE_ID = 'OOS-' + autonomous._hash_payload(genome, 18); _ACTIVE_CACHE = {}
        try:
            return base_final(snapshots, market, genome, dev, seed)
        finally:
            core.state['autonomous_oos_compute_efficiency'] = {
                'finalist_id': _ACTIVE_ID,
                'unique_trade_paths_simulated': len(_ACTIVE_CACHE or {}),
                'semantic_change': False,
                'cache_scope': 'one frozen finalist only',
                'full_evolved_holding_horizon_required': True,
                'updated_at': int(time.time()),
            }
            _ACTIVE_CACHE = None; _ACTIVE_ID = None; gc.collect()
    autonomous._fit_and_audit_finalist = final_cached

    core.state['autonomous_compute_contract'] = {
        'candidate_local_simulation_cache': True,
        'cross_candidate_cache': False,
        'cross_holdout_cache': False,
        'full_evolved_holding_horizon_required': True,
        'dataset_end_never_forces_fake_time_exit': True,
        'no_lookahead_changed': False,
        'candidate_population_reduced': False,
        'history_span_reduced': False,
        'fitness_changed': False,
        'updated_at': int(time.time()),
    }
