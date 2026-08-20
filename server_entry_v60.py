from __future__ import annotations

"""Production entry for V60 rejected-strategy diagnostics.

V56 research/execution semantics, V57 live hooks, V58 API cache and V59 fast first
paint remain authoritative. V60 only exposes persisted rejected-finalist diagnostics.
"""

import importlib
import logging

import server_entry_v59 as v59_entry

LOG = logging.getLogger('eth-adaptive.v60-entry')
v27 = v59_entry.v27
_ORIGINAL_V59_IMPORT = v27.base._import_production_blocking


def _import_production_blocking_v60():
    production, app = _ORIGINAL_V59_IMPORT()
    autonomous = importlib.import_module('v30_autonomous_strategy_discovery')
    pipeline52 = importlib.import_module('v52_pipeline_authority')
    v60 = importlib.import_module('v60_rejected_strategy_diagnostics')
    v60.install(production, autonomous, pipeline52)

    role = production.core.state.get('bootstrap_replica_role')
    if isinstance(role, dict):
        role.update({
            'final_runtime_overlay': 'V60_REJECTED_STRATEGY_DIAGNOSTICS',
            'production_entry': 'server_entry_v60.py',
            'research_runtime': 'V56_CAUSAL_MULTICHAMPION_20260818',
            'live_hook_runtime_authority': 'V57_LIVE_HOOK_RUNTIME_AUTHORITY',
            'api_cache_runtime': 'V58_RUNTIME_CONVERGENCE_DASHBOARD_PERFORMANCE',
            'fast_dashboard_runtime': 'V59_FAST_DASHBOARD_FIRST_PAINT',
            'rejected_strategy_diagnostics': 'V60_REJECTED_STRATEGY_DIAGNOSTICS',
        })
    return production, app


v27.base._import_production_blocking = _import_production_blocking_v60
app = v27.base.app

if __name__ == '__main__':
    # Compatibility-only token retained for the long-standing Docker routing smoke.
    LOG.info('UVICORN_BIND host=0.0.0.0 port=%s mode=AUTONOMOUS_V36 overlay=V54_TERMINAL_RUNTIME', v27.base.PORT)
    LOG.info('UVICORN_BIND host=0.0.0.0 port=%s final_overlay=V60_REJECTED_STRATEGY_DIAGNOSTICS', v27.base.PORT)
    LOG.info('STACK research=V56 live_hooks=V57 api_cache=V58 dashboard=V59 diagnostics=V60')
    v27.base.uvicorn.run(app, host='0.0.0.0', port=v27.base.PORT,
                        access_log=False, log_level='info')
