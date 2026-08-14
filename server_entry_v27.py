from __future__ import annotations

import importlib
import os
from typing import Any

import server_entry as base


def _import_production_blocking_joint() -> tuple[Any, Any]:
    role = base._claim_bootstrap_role()
    os.environ['ETH_RUNTIME_BOOTSTRAP_ROLE'] = role

    joint = importlib.import_module('v28_joint_strategy_research')
    fixes = importlib.import_module('v29_joint_runtime_fixes')

    # Load the already-validated v17/core stack first, but do the one-time derived
    # replay reset before server_v19 starts its source-provenance preflight thread.
    # Raw market_bars and derivative_history are deliberately preserved.
    prebase = importlib.import_module('server_v17')
    joint.RESET_MARKER = fixes.RESET_MARKER
    joint._reset_derived_once(prebase.core)
    fixes.prepare_before_server_v19(prebase.core)

    # server_v19/v25 now capture the final fixed label-support horizon from the first
    # instant. Historical decisions themselves still stop at 2026-08-01 23:45 Taipei.
    production = importlib.import_module('server_v19')
    fixes.install(production, joint)

    # Install memory/replica stability last so its one background worker captures the
    # complete joint Signal+Entry+SL+TP certification authority.
    transition = importlib.import_module('v26_replay_transition_stability')
    transition.install(production.core)
    production.core.state['bootstrap_replica_role'] = {
        'role': role,
        'pid': os.getpid(),
        'import_preflight_allowed': not role.startswith('FOLLOWER'),
        'research_runtime': 'V29_JOINT_SIGNAL_ENTRY_SL_TP_FIXED_20260801',
    }
    base._prepare_100_generation(production)
    return production, production.app


# Keep this filename because existing Zeabur services may already point at it.
base._import_production_blocking = _import_production_blocking_joint
app = base.app


if __name__ == '__main__':
    # Keep the stable bind prefix for Docker/Zeabur diagnostics; append the research mode.
    base.LOG.info('UVICORN_BIND host=0.0.0.0 port=%s mode=JOINT_V29', base.PORT)
    base.uvicorn.run(app, host='0.0.0.0', port=base.PORT, access_log=True, log_level='info')
