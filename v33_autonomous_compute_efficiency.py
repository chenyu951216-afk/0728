from __future__ import annotations

"""Candidate-local deterministic simulation cache for autonomous research.

A complete candidate repeatedly touches overlapping chronological folds. Replaying the
same candidate at the same decision timestamp again adds no information, so cache that
exact deterministic result only for the lifetime of one candidate/finalist. This cuts
CPU without changing a single decision, feature, fill, stop, target, cost or OOS rule.
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
        if _ACTIVE_CACHE is None or _ACTIVE_ID is None:
            return base_simulate(market, ts, features, genome)
        key = (_ACTIVE_ID, int(ts))
        cached = _ACTIVE_CACHE.get(key)
        if cached is not None:
            return dict(cached)
        out = base_simulate(market, ts, features, genome)
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
                'updated_at': int(time.time()),
            }
            _ACTIVE_CACHE = None; _ACTIVE_ID = None; gc.collect()
    autonomous._fit_and_audit_finalist = final_cached

    core.state['autonomous_compute_contract'] = {
        'candidate_local_simulation_cache': True,
        'cross_candidate_cache': False,
        'cross_holdout_cache': False,
        'no_lookahead_changed': False,
        'candidate_population_reduced': False,
        'history_span_reduced': False,
        'fitness_changed': False,
        'updated_at': int(time.time()),
    }
