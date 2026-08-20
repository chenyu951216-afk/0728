from __future__ import annotations

"""Production entry for V64 score/current-paper UX lifecycle authority."""

import importlib
import logging

import server_entry_v63 as v63_entry

LOG = logging.getLogger('eth-adaptive.v64-entry')
v27 = v63_entry.v27
_ORIGINAL_V63_IMPORT = v27.base._import_production_blocking


def _import_production_blocking_v64():
    production, app = _ORIGINAL_V63_IMPORT()
    autonomous = importlib.import_module('v30_autonomous_strategy_discovery')
    v56 = importlib.import_module('v56_causal_multichampion_learning')
    v63 = importlib.import_module('v63_score_arbiter_notifications')
    v64 = importlib.import_module('v64_current_paper_ux_authority')
    v64.install(production, autonomous, v56, v63)

    role = production.core.state.get('bootstrap_replica_role')
    if isinstance(role, dict):
        role.update({
            'final_runtime_overlay': 'V64_CURRENT_PAPER_CHINESE_UX_LIFECYCLE',
            'production_entry': 'server_entry_v64.py',
            'research_runtime': 'V56_CAUSAL_MULTICHAMPION_20260818',
            'live_hook_runtime_authority': 'V57_LIVE_HOOK_RUNTIME_AUTHORITY',
            'fast_dashboard_runtime': 'V59_FAST_DASHBOARD_FIRST_PAINT',
            'rejected_strategy_diagnostics': 'V60_REJECTED_STRATEGY_DIAGNOSTICS',
            'score_authority': 'V63_CAPPED_SCORE_ARBITER_DISCORD_LIFECYCLE',
            'current_paper_ux_authority': 'V64_CURRENT_PAPER_CHINESE_UX_LIFECYCLE',
        })
    return production, app


v27.base._import_production_blocking = _import_production_blocking_v64
app = v27.base.app

if __name__ == '__main__':
    LOG.info('UVICORN_BIND host=0.0.0.0 port=%s mode=AUTONOMOUS_V36 overlay=V54_TERMINAL_RUNTIME', v27.base.PORT)
    LOG.info('UVICORN_BIND host=0.0.0.0 port=%s final_overlay=V64_CURRENT_PAPER_CHINESE_UX_LIFECYCLE', v27.base.PORT)
    LOG.info('STACK research=V56 live_hooks=V57 dashboard=V59 diagnostics=V60 score=V63 current_paper_ux=V64')
    v27.base.uvicorn.run(app, host='0.0.0.0', port=v27.base.PORT,
                        access_log=False, log_level='info')
