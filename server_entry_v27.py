from __future__ import annotations

import importlib
import os
from typing import Any

import server_entry as base


def _import_production_blocking_joint() -> tuple[Any, Any]:
    role = base._claim_bootstrap_role()
    os.environ['ETH_RUNTIME_BOOTSTRAP_ROLE'] = role

    # Exact Taiwan research calendar: 2020-01-01 00:00 through 2026-08-01.
    # Reuse raw cache; only a truly missing edge gap may be fetched.
    research_start = os.getenv('AUTONOMOUS_RESEARCH_START_TS', '1577808000')
    os.environ['LEARNING_START_TS'] = research_start
    os.environ['HISTORICAL_RESEARCH_START_TS'] = research_start
    os.environ['STRICT_REPLAY_STRIDE_BARS'] = '1'  # every 15m decision is available to the autonomous learner
    os.environ.setdefault('AUTONOMOUS_RESEARCH_START_TS', research_start)
    os.environ.setdefault('AUTONOMOUS_RESEARCH_END_TS', '1785600000')
    os.environ.setdefault('AUTONOMOUS_SETTLEMENT_END_TS', '1786723200')

    # Keep v28/v29 only as compatibility/data-window bridges. V30+ replaces their
    # hand-authored strategy taxonomy and legacy success-label objective.
    joint = importlib.import_module('v28_joint_strategy_research')
    fixes = importlib.import_module('v29_joint_runtime_fixes')
    autonomous = importlib.import_module('v30_autonomous_strategy_discovery')
    hardening = importlib.import_module('v31_autonomous_runtime_hardening')
    ui_compat = importlib.import_module('v32_autonomous_ui_compat')
    compute = importlib.import_module('v33_autonomous_compute_efficiency')
    recovery = importlib.import_module('v34_autonomous_checkpoint_recovery')
    features = importlib.import_module('v35_autonomous_feature_integrity')

    # A new final architecture marker intentionally resets replay-derived samples,
    # models and policies once. Raw market_bars / derivative_history stay intact.
    autonomous.RESET_MARKER = 'v35_autonomous_direct_r_reset_20260801_final'
    prebase = importlib.import_module('server_v17')
    joint.RESET_MARKER = autonomous.RESET_MARKER
    fixes.RESET_MARKER = autonomous.RESET_MARKER
    joint._reset_derived_once(prebase.core)
    fixes.prepare_before_server_v19(prebase.core)

    # Compose the validated fixed-horizon/data-integrity stack first, then install
    # autonomous strategy discovery and all safety/performance/durability overlays
    # before v26 captures the heavy research authority.
    production = importlib.import_module('server_v19')
    fixes.install(production, joint)
    autonomous.install(production, joint, fixes)
    features.install(production, autonomous)
    hardening.install(production, autonomous)
    compute.install(production, autonomous)
    ui_compat.install(production, autonomous)
    recovery.install(production, autonomous)

    # Single mutating replica, background training thread, cgroup memory watermarks,
    # WAL checkpoints and heap trimming remain authoritative for the new engine.
    transition = importlib.import_module('v26_replay_transition_stability')
    transition.install(production.core)
    production.core.state['bootstrap_replica_role'] = {
        'role': role,
        'pid': os.getpid(),
        'import_preflight_allowed': not role.startswith('FOLLOWER'),
        'research_runtime': 'V35_AUTONOMOUS_DIRECT_R_FIXED_20260801',
        'no_strategy_templates': True,
        'no_manual_regime_templates': True,
        'legacy_success_label_used': False,
        'authoritative_feature_snapshots': True,
        'replay_decision_stride_15m_bars': 1,
        'candidate_local_simulation_cache': True,
        'crash_safe_oos_checkpointing': True,
        'paper_notional_usdt': 20000,
        'leverage_mode': 'MAX_AVAILABLE_AT_ORDER_TIME',
    }
    base._prepare_100_generation(production)
    return production, production.app


# Keep this filename because existing Zeabur services may already point at it.
base._import_production_blocking = _import_production_blocking_joint
app = base.app


if __name__ == '__main__':
    base.LOG.info('UVICORN_BIND host=0.0.0.0 port=%s mode=AUTONOMOUS_V35', base.PORT)
    base.uvicorn.run(app, host='0.0.0.0', port=base.PORT, access_log=True, log_level='info')
