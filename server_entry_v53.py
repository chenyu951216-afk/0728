from __future__ import annotations

"""Production entry: add status/tail recovery on top of the exact V52 research stack."""

import importlib
import logging

import server_entry_v52 as v52_entry

LOG = logging.getLogger('eth-adaptive.v53-entry')
v27 = v52_entry.v27
_ORIGINAL_V52_IMPORT = v27.base._import_production_blocking


def _import_production_blocking_v53():
    production, app = _ORIGINAL_V52_IMPORT()
    autonomous = importlib.import_module('v30_autonomous_strategy_discovery')
    throughput = importlib.import_module('v46_stage6_throughput_liveness')
    transition = importlib.import_module('v26_replay_transition_stability')
    pipeline_module = importlib.import_module('v22_hierarchical_pipeline')
    pipeline52 = importlib.import_module('v52_pipeline_authority')
    recovery53 = importlib.import_module('v53_terminal_handoff_recovery')

    # Intentionally do NOT add V53 to V47 SEMANTIC_MODULES: this overlay changes no
    # research/evaluation semantics and must not throw away the completed V52 run.
    recovery53.install(production, autonomous, throughput, pipeline52,
                       transition, pipeline_module)

    role = production.core.state.get('bootstrap_replica_role')
    if isinstance(role, dict):
        role.update({
            'terminal_handoff_runtime': 'V53_TERMINAL_HANDOFF_RECOVERY',
            'research_semantics_changed_by_v53': False,
            'v47_exact_identity_changed_by_v53': False,
            'stale_oos_progress_can_block_terminal_status': False,
            'durable_oos_tail_commit_recovery': True,
        })
    return production, app


v27.base._import_production_blocking = _import_production_blocking_v53
app = v27.base.app

if __name__ == '__main__':
    LOG.info('UVICORN_BIND host=0.0.0.0 port=%s mode=AUTONOMOUS_V36 overlay=V53_TERMINAL_HANDOFF', v27.base.PORT)
    v27.base.uvicorn.run(app, host='0.0.0.0', port=v27.base.PORT,
                        access_log=True, log_level='info')
