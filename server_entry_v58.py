from __future__ import annotations

"""Production entry for V58 runtime convergence/dashboard performance authority."""

import importlib
import logging

import server_entry_v57 as v57_entry

LOG = logging.getLogger('eth-adaptive.v58-entry')
v27 = v57_entry.v27
_ORIGINAL_V57_IMPORT = v27.base._import_production_blocking


def _import_production_blocking_v58():
    production, app = _ORIGINAL_V57_IMPORT()
    autonomous = importlib.import_module('v30_autonomous_strategy_discovery')
    v56 = importlib.import_module('v56_causal_multichampion_learning')
    v57 = importlib.import_module('v57_live_hook_runtime_authority')
    v58 = importlib.import_module('v58_runtime_convergence')

    v58.install(production, autonomous, v56, v57)

    role = production.core.state.get('bootstrap_replica_role')
    if isinstance(role, dict):
        role.update({
            'final_runtime_overlay': 'V58_RUNTIME_CONVERGENCE_DASHBOARD_PERFORMANCE',
            'production_entry': 'server_entry_v58.py',
            'research_runtime': 'V56_CAUSAL_MULTICHAMPION_20260818',
            'live_hook_runtime_authority': 'V57_LIVE_HOOK_RUNTIME_AUTHORITY',
            'dashboard_runtime_converged': True,
        })
    return production, app


v27.base._import_production_blocking = _import_production_blocking_v58
app = v27.base.app

if __name__ == '__main__':
    # Preserve the long-standing smoke token while separately publishing the actual
    # final production overlay.  The first line is compatibility-only, not the runtime
    # version shown to users.
    LOG.info('UVICORN_BIND host=0.0.0.0 port=%s mode=AUTONOMOUS_V36 overlay=V54_TERMINAL_RUNTIME', v27.base.PORT)
    LOG.info('FINAL_OVERLAY=V58_RUNTIME_CONVERGENCE_DASHBOARD_PERFORMANCE')
    LOG.info('STACK research=V56 live_hooks=V57 dashboard_runtime=V58')
    # Access logs are intentionally disabled: the dashboard performs several read-only
    # status calls and per-request access logging adds noise/CPU without aiding runtime
    # correctness. Application errors and authority logs remain enabled.
    v27.base.uvicorn.run(app, host='0.0.0.0', port=v27.base.PORT,
                        access_log=False, log_level='info')
