from __future__ import annotations

import importlib
import os
from typing import Any

import server_entry as base


def _import_production_blocking_joint() -> tuple[Any, Any]:
    role = base._claim_bootstrap_role()
    os.environ['ETH_RUNTIME_BOOTSTRAP_ROLE'] = role
    production = importlib.import_module('server_v19')

    # v28 must be installed before v26. v26 then captures the complete joint
    # Signal+Entry+SL+TP authority and moves it to the single background worker.
    joint = importlib.import_module('v28_joint_strategy_research')
    joint.install(production)

    transition = importlib.import_module('v26_replay_transition_stability')
    transition.install(production.core)
    production.core.state['bootstrap_replica_role'] = {
        'role': role,
        'pid': os.getpid(),
        'import_preflight_allowed': not role.startswith('FOLLOWER'),
        'research_runtime': 'V28_JOINT_SIGNAL_ENTRY_SL_TP',
    }
    base._prepare_100_generation(production)
    return production, production.app


# Keep the historical filename because some Zeabur services already override the
# start command to server_entry_v27.py. It now boots the v28 joint research runtime.
base._import_production_blocking = _import_production_blocking_joint
app = base.app


if __name__ == '__main__':
    base.LOG.info('UVICORN_BIND_JOINT_V28 host=0.0.0.0 port=%s', base.PORT)
    base.uvicorn.run(app, host='0.0.0.0', port=base.PORT, access_log=True, log_level='info')
