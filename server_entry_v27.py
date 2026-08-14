from __future__ import annotations

import importlib
import os
from typing import Any

import server_entry as base


def _import_production_blocking_joint() -> tuple[Any, Any]:
    role = base._claim_bootstrap_role()
    os.environ['ETH_RUNTIME_BOOTSTRAP_ROLE'] = role

    # Keep v28/v29 only as compatibility/data-window bridges. V30 replaces their
    # hand-authored strategy taxonomy and success-label objective with autonomous
    # direct-R discovery.
    joint = importlib.import_module('v28_joint_strategy_research')
    fixes = importlib.import_module('v29_joint_runtime_fixes')
    autonomous = importlib.import_module('v30_autonomous_strategy_discovery')

    # Reset only replay-derived products once for the V30 architecture. Raw
    # market_bars and derivative history remain on the persistent volume so the
    # expensive historical download is reused.
    prebase = importlib.import_module('server_v17')
    joint.RESET_MARKER = autonomous.RESET_MARKER
    fixes.RESET_MARKER = autonomous.RESET_MARKER
    joint._reset_derived_once(prebase.core)
    fixes.prepare_before_server_v19(prebase.core)

    # Compose the validated fixed-horizon/data-integrity stack first, then install
    # autonomous strategy discovery before v26 captures the heavy certification
    # function. This keeps V30 training off the FastAPI event loop.
    production = importlib.import_module('server_v19')
    fixes.install(production, joint)
    autonomous.install(production, joint, fixes)

    # Single mutating replica, background training thread, cgroup memory watermarks,
    # WAL checkpoints and heap trimming remain authoritative for the new engine.
    transition = importlib.import_module('v26_replay_transition_stability')
    transition.install(production.core)
    production.core.state['bootstrap_replica_role'] = {
        'role': role,
        'pid': os.getpid(),
        'import_preflight_allowed': not role.startswith('FOLLOWER'),
        'research_runtime': 'V30_AUTONOMOUS_DIRECT_R_FIXED_20260801',
        'no_strategy_templates': True,
        'no_manual_regime_templates': True,
        'legacy_success_label_used': False,
    }
    base._prepare_100_generation(production)
    return production, production.app


# Keep this filename because existing Zeabur services may already point at it.
base._import_production_blocking = _import_production_blocking_joint
app = base.app


if __name__ == '__main__':
    base.LOG.info('UVICORN_BIND host=0.0.0.0 port=%s mode=AUTONOMOUS_V30', base.PORT)
    base.uvicorn.run(app, host='0.0.0.0', port=base.PORT, access_log=True, log_level='info')
