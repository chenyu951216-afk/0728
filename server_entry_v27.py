from __future__ import annotations

import importlib
import os
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

    # Install replica/near-finish stability last so followers cannot execute the
    # V39 repair/liveness authority and the leader alone owns all mutating loops.
    transition = importlib.import_module('v26_replay_transition_stability')
    transition.install(production.core)
    production.core.state['bootstrap_replica_role'] = {
        'role': role, 'pid': os.getpid(),
        'import_preflight_allowed': not role.startswith('FOLLOWER'),
        'research_runtime': 'V39_REPLAY_LIVENESS_GRID_INTEGRITY_20260816',
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
        'paper_notional_usdt': 20000, 'leverage_mode': 'MAX_AVAILABLE_AT_ORDER_TIME',
    }
    base._prepare_100_generation(production)
    return production, production.app


base._import_production_blocking = _import_production_blocking_joint
app = base.app


if __name__ == '__main__':
    # Keep the public boot-mode token stable for existing deployment smoke checks;
    # V39 is a replay-liveness/grid-integrity layer on top of V36/V37/V38.
    base.LOG.info('UVICORN_BIND host=0.0.0.0 port=%s mode=AUTONOMOUS_V36', base.PORT)
    base.uvicorn.run(app, host='0.0.0.0', port=base.PORT, access_log=True, log_level='info')
