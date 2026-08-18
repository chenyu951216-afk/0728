from __future__ import annotations

"""Production entry for the V57 live-hook runtime hotfix.

V56 remains the research/execution semantic authority.  V57 is installed immediately
after the complete V56 stack and before FastAPI lifespan workers start, so Market Scan
can only see the corrected dual-signature live hooks and the Core live surfaces are
explicitly bound to V56 canonical analysis/create/update implementations.
"""

import importlib
import logging

import server_entry_v56 as v56_entry

LOG = logging.getLogger('eth-adaptive.v57-entry')
v27 = v56_entry.v27
_ORIGINAL_V56_IMPORT = v27.base._import_production_blocking


def _import_production_blocking_v57():
    production, app = _ORIGINAL_V56_IMPORT()
    autonomous = importlib.import_module('v30_autonomous_strategy_discovery')
    v56 = importlib.import_module('v56_causal_multichampion_learning')
    v57 = importlib.import_module('v57_live_hook_runtime_authority')

    v57.install(production, autonomous, v56)

    role = production.core.state.get('bootstrap_replica_role')
    if isinstance(role, dict):
        role.update({
            'research_runtime': 'V56_CAUSAL_MULTICHAMPION_20260818',
            'final_runtime_overlay': 'V57_LIVE_HOOK_RUNTIME_AUTHORITY',
            'market_scan_signature_compat_fixed': True,
            'v56_live_hooks_explicitly_bound_to_core': True,
        })
    return production, app


v27.base._import_production_blocking = _import_production_blocking_v57
app = v27.base.app

if __name__ == '__main__':
    LOG.info('UVICORN_BIND host=0.0.0.0 port=%s mode=AUTONOMOUS_V36 overlay=V54_TERMINAL_RUNTIME', v27.base.PORT)
    LOG.info('FINAL_OVERLAY=V57_LIVE_HOOK_RUNTIME_AUTHORITY underlying=V56_CAUSAL_MULTICHAMPION_ONLINE_LEARNING')
    v27.base.uvicorn.run(app, host='0.0.0.0', port=v27.base.PORT,
                        access_log=True, log_level='info')
