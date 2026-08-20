from __future__ import annotations

"""Production entry for V61 risk-adjusted provisional Current Paper.

V56 research/execution semantics, V57 live hook compatibility, V58 read-only API cache,
V59 fast first paint and V60 rejected-strategy diagnostics remain in place. V61 adds one
paper-only provisional path for a frozen finalist that failed strict historical
certification solely on absolute R drawdown while meeting stronger quality guards.
"""

import importlib
import logging

import server_entry_v60 as v60_entry

LOG = logging.getLogger('eth-adaptive.v61-entry')
v27 = v60_entry.v27
_ORIGINAL_V60_IMPORT = v27.base._import_production_blocking


def _import_production_blocking_v61():
    production, app = _ORIGINAL_V60_IMPORT()
    autonomous = importlib.import_module('v30_autonomous_strategy_discovery')
    pipeline52 = importlib.import_module('v52_pipeline_authority')
    pipeline = importlib.import_module('v22_hierarchical_pipeline')
    v56 = importlib.import_module('v56_causal_multichampion_learning')
    v61 = importlib.import_module('v61_risk_adjusted_provisional')
    v61.install(production, autonomous, pipeline52, pipeline, v56)

    role = production.core.state.get('bootstrap_replica_role')
    if isinstance(role, dict):
        role.update({
            'final_runtime_overlay': 'V61_RISK_ADJUSTED_PROVISIONAL_PAPER',
            'production_entry': 'server_entry_v61.py',
            'research_runtime': 'V56_CAUSAL_MULTICHAMPION_20260818',
            'live_hook_runtime_authority': 'V57_LIVE_HOOK_RUNTIME_AUTHORITY',
            'api_cache_runtime': 'V58_RUNTIME_CONVERGENCE_DASHBOARD_PERFORMANCE',
            'fast_dashboard_runtime': 'V59_FAST_DASHBOARD_FIRST_PAINT',
            'rejected_strategy_diagnostics': 'V60_REJECTED_STRATEGY_DIAGNOSTICS',
            'provisional_paper_authority': 'V61_RISK_ADJUSTED_PROVISIONAL_PAPER',
            'strict_historical_oos_rewritten_by_v61': False,
        })
    return production, app


v27.base._import_production_blocking = _import_production_blocking_v61
app = v27.base.app

if __name__ == '__main__':
    LOG.info('UVICORN_BIND host=0.0.0.0 port=%s mode=AUTONOMOUS_V36 overlay=V54_TERMINAL_RUNTIME', v27.base.PORT)
    LOG.info('UVICORN_BIND host=0.0.0.0 port=%s final_overlay=V61_RISK_ADJUSTED_PROVISIONAL_PAPER', v27.base.PORT)
    LOG.info('STACK research=V56 live_hooks=V57 api_cache=V58 dashboard=V59 diagnostics=V60 provisional=V61')
    v27.base.uvicorn.run(app, host='0.0.0.0', port=v27.base.PORT,
                        access_log=False, log_level='info')
