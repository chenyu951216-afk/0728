from __future__ import annotations

import importlib
import os
from typing import Any

import server_entry as base


def _import_production_blocking_v27() -> tuple[Any, Any]:
    role = base._claim_bootstrap_role()
    os.environ['ETH_RUNTIME_BOOTSTRAP_ROLE'] = role
    production = importlib.import_module('server_v19')

    # Install live lineage/generation progress plus the single-matrix candidate scorer
    # before v26 wraps certification with its single-worker memory guard.
    certification_progress = importlib.import_module('v27_certification_progress')
    certification_progress.install(
        production.core,
        production.signal_evolution,
        production.fixed_horizon_runtime,
    )

    transition = importlib.import_module('v26_replay_transition_stability')
    transition.install(production.core)
    production.core.state['bootstrap_replica_role'] = {
        'role': role,
        'pid': os.getpid(),
        'import_preflight_allowed': not role.startswith('FOLLOWER'),
    }
    base._prepare_100_generation(production)
    return production, production.app


# server_entry has not entered its FastAPI lifespan yet, so replacing this loader here
# guarantees the v27 layer is active before any production background worker starts.
base._import_production_blocking = _import_production_blocking_v27
app = base.app


if __name__ == '__main__':
    base.LOG.info('UVICORN_BIND_V27 host=0.0.0.0 port=%s', base.PORT)
    base.uvicorn.run(app, host='0.0.0.0', port=base.PORT, access_log=True, log_level='info')
