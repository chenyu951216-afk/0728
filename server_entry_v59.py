from __future__ import annotations

"""Production entry for V59 fast first-paint dashboard.

V56 research/execution semantics, V57 live hook compatibility and V58 read-only API
cache remain authoritative. V59 only replaces the root-page delivery path so mobile
clients always receive a tiny HTML shell immediately, even while research/SQLite is busy.
"""

import importlib
import logging

import server_entry_v58 as v58_entry

LOG = logging.getLogger('eth-adaptive.v59-entry')
v27 = v58_entry.v27
_ORIGINAL_V58_IMPORT = v27.base._import_production_blocking


def _import_production_blocking_v59():
    production, app = _ORIGINAL_V58_IMPORT()
    autonomous = importlib.import_module('v30_autonomous_strategy_discovery')
    v56 = importlib.import_module('v56_causal_multichampion_learning')
    v57 = importlib.import_module('v57_live_hook_runtime_authority')
    v58 = importlib.import_module('v58_runtime_convergence')
    v59 = importlib.import_module('v59_fast_dashboard')

    v59.install(production, autonomous, v56, v57, v58)

    role = production.core.state.get('bootstrap_replica_role')
    if isinstance(role, dict):
        role.update({
            'final_runtime_overlay': 'V59_FAST_DASHBOARD_FIRST_PAINT',
            'production_entry': 'server_entry_v59.py',
            'research_runtime': 'V56_CAUSAL_MULTICHAMPION_20260818',
            'live_hook_runtime_authority': 'V57_LIVE_HOOK_RUNTIME_AUTHORITY',
            'api_cache_runtime': 'V58_RUNTIME_CONVERGENCE_DASHBOARD_PERFORMANCE',
            'fast_dashboard_runtime': 'V59_FAST_DASHBOARD_FIRST_PAINT',
        })
    return production, app


v27.base._import_production_blocking = _import_production_blocking_v59
app = v27.base.app

if __name__ == '__main__':
    LOG.info('UVICORN_BIND host=0.0.0.0 port=%s final_overlay=V59_FAST_DASHBOARD_FIRST_PAINT', v27.base.PORT)
    LOG.info('STACK research=V56 live_hooks=V57 api_cache=V58 dashboard=V59')
    v27.base.uvicorn.run(app, host='0.0.0.0', port=v27.base.PORT,
                        access_log=False, log_level='info')
