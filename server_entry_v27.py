from __future__ import annotations

import os

# Set conservative native math thread defaults before importing the application stack.
# Autonomous research already has one authoritative background worker; uncontrolled
# nested BLAS/OpenMP fan-out only burns CPU and makes the web process unresponsive.
for _name in ('OMP_NUM_THREADS', 'OPENBLAS_NUM_THREADS', 'MKL_NUM_THREADS', 'NUMEXPR_NUM_THREADS', 'VECLIB_MAXIMUM_THREADS', 'BLIS_NUM_THREADS'):
    os.environ.setdefault(_name, '1')

import importlib
from typing import Any

import server_entry as base


def _import_production_blocking_joint() -> tuple[Any, Any]:
    role = base._claim_bootstrap_role()
    os.environ['ETH_RUNTIME_BOOTSTRAP_ROLE'] = role

    research_start = os.getenv('AUTONOMOUS_RESEARCH_START_TS', '1577808000')
    os.environ['LEARNING_START_TS'] = research_start
    os.environ['HISTORICAL_RESEARCH_START_TS'] = research_start
    os.environ['STRICT_REPLAY_STRIDE_BARS'] = '1'
    os.environ.setdefault('AUTONOMOUS_RESEARCH_START_TS', research_start)
    os.environ.setdefault('AUTONOMOUS_RESEARCH_END_TS', '1785600000')
    os.environ.setdefault('AUTONOMOUS_SETTLEMENT_END_TS', '1786723200')

    joint = importlib.import_module('v28_joint_strategy_research')
    fixes = importlib.import_module('v29_joint_runtime_fixes')
    autonomous = importlib.import_module('v30_autonomous_strategy_discovery')
    hardening = importlib.import_module('v31_autonomous_runtime_hardening')
    ui_compat = importlib.import_module('v32_autonomous_ui_compat')
    compute = importlib.import_module('v33_autonomous_compute_efficiency')
    recovery = importlib.import_module('v34_autonomous_checkpoint_recovery')
    features = importlib.import_module('v35_autonomous_feature_integrity')
    leverage = importlib.import_module('v36_bitget_execution_truth')
    fresh_bootstrap = importlib.import_module('v37_fresh_price_bootstrap')
    timeframe_alignment = importlib.import_module('v38_timeframe_aligned_bootstrap')
    replay_liveness = importlib.import_module('v39_replay_liveness_grid_integrity')
    handoff_integrity = importlib.import_module('v40_autonomous_handoff_integrity')
    post_replay_scheduler = importlib.import_module('v41_post_replay_autonomous_scheduler')
    resource_authority = importlib.import_module('v42_post_replay_resource_authority')
    performance_authority = importlib.import_module('v43_unified_performance_authority')
    horizon_authority = importlib.import_module('v44_fixed_research_horizon_authority')

    autonomous.RESET_MARKER = 'v35_autonomous_direct_r_reset_20260801_final'
    prebase = importlib.import_module('server_v17')
    joint.RESET_MARKER = autonomous.RESET_MARKER
    fixes.RESET_MARKER = autonomous.RESET_MARKER
    joint._reset_derived_once(prebase.core)
    fixes.prepare_before_server_v19(prebase.core)

    production = importlib.import_module('server_v19')
    fixes.install(production, joint)
    autonomous.install(production, joint, fixes)
    features.install(production, autonomous)
    hardening.install(production, autonomous)
    compute.install(production, autonomous)
    ui_compat.install(production, autonomous)
    recovery.install(production, autonomous)
    leverage.install(production, autonomous)
    fresh_bootstrap.install(production.core)
    timeframe_alignment.install(production.core)
    replay_liveness.install(production.core)
    handoff_integrity.install(production, autonomous)

    # V26 owns the single background research executor and replica leader fence.
    # V43 pre-installs the semantic-equivalent hot-path/memory guards before V42 is
    # allowed to boot-kick Stage 6. V44 then reconciles every historical/replay layer
    # to the same immutable autonomous research end before the final V43 governor runs.
    transition = importlib.import_module('v26_replay_transition_stability')
    transition.install(production.core)
    post_replay_scheduler.install(production, autonomous, transition)
    performance_authority.preinstall(production, autonomous, transition, leverage)
    resource_authority.install(production, autonomous, transition, post_replay_scheduler)
    horizon_authority.install(
        production, autonomous, transition, post_replay_scheduler, resource_authority,
    )
    performance_authority.install(
        production, autonomous, transition, resource_authority, leverage,
        scheduler=post_replay_scheduler,
    )

    production.core.state['bootstrap_replica_role'] = {
        'role': role, 'pid': os.getpid(),
        'import_preflight_allowed': not role.startswith('FOLLOWER'),
        'research_runtime': 'V44_FIXED_RESEARCH_HORIZON_AUTHORITY_20260817',
        'no_strategy_templates': True, 'no_manual_regime_templates': True,
        'legacy_success_label_used': False, 'authoritative_feature_snapshots': True,
        'replay_decision_stride_15m_bars': 1, 'candidate_local_simulation_cache': True,
        'crash_safe_oos_checkpointing': True, 'bitget_max_leverage_truth': True,
        'fresh_database_bulk_price_bootstrap': True,
        'timeframe_aligned_collection_start': True,
        'phantom_off_grid_daily_gap_forbidden': True,
        'off_grid_candle_rows_excluded_from_replay': True,
        'stale_replay_blockers_self_heal': True,
        'stalled_replay_watchdog': True,
        'autonomous_virtual_canonical_sql_deadlock_fixed': True,
        'completed_replay_terminal_future_probe_nonblocking': True,
        'autonomous_market_cache_grid_validated': True,
        'post_replay_autonomous_boot_kick': True,
        'post_replay_autonomous_learning_tick_retry': True,
        'post_replay_autonomous_scan_fallback_retry': True,
        'post_replay_legacy_maintenance_quiesced': True,
        'completed_replay_status_o1': True,
        'persistent_autonomous_feature_memmap': True,
        'persistent_autonomous_market_memmap': True,
        'stale_certification_queue_future_reconciled': True,
        'native_math_threads_default': 1,
        'frozen_execution_contract_memory_cached': True,
        'adaptive_gc_cadence': True,
        'decision_mask_semantic_cache': True,
        'vectorized_trade_simulator_runtime_parity_guard': True,
        'cgroup_pagecache_reclaim': True,
        'adaptive_candidate_resource_governor': True,
        'memory_emergency_fails_closed': True,
        'single_fixed_research_horizon_authority': True,
        'live_market_cannot_expand_historical_replay': True,
        'post_horizon_raw_data_role': 'SETTLEMENT_OR_CURRENT_PAPER_ONLY',
        'paper_notional_usdt': 20000, 'leverage_mode': 'MAX_AVAILABLE_AT_ORDER_TIME',
    }
    base._prepare_100_generation(production)
    return production, production.app


base._import_production_blocking = _import_production_blocking_joint
app = base.app


if __name__ == '__main__':
    # Keep the public boot-mode token stable for existing deployment smoke checks;
    # V44 is the final fixed-horizon/data-alignment authority.
    base.LOG.info('UVICORN_BIND host=0.0.0.0 port=%s mode=AUTONOMOUS_V36', base.PORT)
    base.uvicorn.run(app, host='0.0.0.0', port=base.PORT, access_log=True, log_level='info')
