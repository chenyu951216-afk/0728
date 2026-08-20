from __future__ import annotations

"""Production entry for V65 score/multi-strategy/current-position authority."""

import importlib
import logging

import server_entry_v60 as v60_entry

LOG = logging.getLogger('eth-adaptive.v65-entry')
v27 = v60_entry.v27
_ORIGINAL_V60_IMPORT = v27.base._import_production_blocking


def _import_production_blocking_v65():
    production, app = _ORIGINAL_V60_IMPORT()
    autonomous = importlib.import_module('v30_autonomous_strategy_discovery')
    pipeline52 = importlib.import_module('v52_pipeline_authority')
    pipeline = importlib.import_module('v22_hierarchical_pipeline')
    v56 = importlib.import_module('v56_causal_multichampion_learning')
    v61 = importlib.import_module('v61_risk_adjusted_provisional')
    v62 = importlib.import_module('v62_relaxed_multistrategy_provisional')
    v63 = importlib.import_module('v63_score_arbiter_notifications')
    v64 = importlib.import_module('v64_current_paper_ux_authority')
    v65 = importlib.import_module('v65_multistrategy_position_authority')

    # This must happen before V63 -> V62 -> V61 starts the provisional daemon.
    v65.configure_worker(v61)
    v63.install(production, autonomous, pipeline52, pipeline, v56, v61, v62)
    v64.install(production, autonomous, v56, v63)
    v65.install(production, v63, v64)

    role = production.core.state.get('bootstrap_replica_role')
    if isinstance(role, dict):
        role.update({
            'final_runtime_overlay': 'V65_MULTISTRATEGY_POSITION_AUTHORITY',
            'production_entry': 'server_entry_v65.py',
            'research_runtime': 'V56_CAUSAL_MULTICHAMPION_20260818',
            'live_hook_runtime_authority': 'V57_LIVE_HOOK_RUNTIME_AUTHORITY',
            'fast_dashboard_runtime': 'V59_FAST_DASHBOARD_FIRST_PAINT',
            'rejected_strategy_diagnostics': 'V60_REJECTED_STRATEGY_DIAGNOSTICS',
            'score_authority': 'V63_CAPPED_SCORE_ARBITER_DISCORD_LIFECYCLE',
            'current_paper_ux_authority': 'V64_CURRENT_PAPER_CHINESE_UX_LIFECYCLE',
            'multi_strategy_position_authority': 'V65_MULTISTRATEGY_POSITION_AUTHORITY',
        })
    return production, app


v27.base._import_production_blocking = _import_production_blocking_v65
app = v27.base.app

if __name__ == '__main__':
    LOG.info('UVICORN_BIND host=0.0.0.0 port=%s mode=AUTONOMOUS_V36 overlay=V54_TERMINAL_RUNTIME', v27.base.PORT)
    LOG.info('UVICORN_BIND host=0.0.0.0 port=%s final_overlay=V65_MULTISTRATEGY_POSITION_AUTHORITY', v27.base.PORT)
    LOG.info('STACK research=V56 live_hooks=V57 dashboard=V59 diagnostics=V60 score=V63 ux=V64 position=V65')
    v27.base.uvicorn.run(app, host='0.0.0.0', port=v27.base.PORT,
                        access_log=False, log_level='info')
