from __future__ import annotations

"""Production entry for V66 forced Current-Time + strategy explainer authority."""

import importlib
import logging

import server_entry_v65 as v65_entry

LOG = logging.getLogger("eth-adaptive.v66-entry")
v27 = v65_entry.v27
_ORIGINAL_V65_IMPORT = v27.base._import_production_blocking


def _import_production_blocking_v66():
    production, app = _ORIGINAL_V65_IMPORT()
    autonomous = importlib.import_module("v30_autonomous_strategy_discovery")
    v63 = importlib.import_module("v63_score_arbiter_notifications")
    v64 = importlib.import_module("v64_current_paper_ux_authority")
    v66 = importlib.import_module("v66_current_mode_strategy_explainer")
    v66.install(production, autonomous, v63, v64)

    role = production.core.state.get("bootstrap_replica_role")
    if isinstance(role, dict):
        role.update({
            "final_runtime_overlay": "V66_FORCED_CURRENT_TIME_STRATEGY_EXPLAINER",
            "production_entry": "server_entry_v66.py",
            "score_authority": "V63_CAPPED_SCORE_ARBITER_DISCORD_LIFECYCLE",
            "current_paper_ux_authority": "V64_CURRENT_PAPER_CHINESE_UX_LIFECYCLE",
            "multi_strategy_position_authority": "V65_MULTISTRATEGY_POSITION_AUTHORITY",
            "forced_current_time_authority": "V66_FORCED_CURRENT_TIME_STRATEGY_EXPLAINER",
        })
    return production, app


v27.base._import_production_blocking = _import_production_blocking_v66
app = v27.base.app

if __name__ == "__main__":
    LOG.info("UVICORN_BIND host=0.0.0.0 port=%s mode=AUTONOMOUS_V36 overlay=V54_TERMINAL_RUNTIME", v27.base.PORT)
    LOG.info("UVICORN_BIND host=0.0.0.0 port=%s final_overlay=V66_FORCED_CURRENT_TIME_STRATEGY_EXPLAINER", v27.base.PORT)
    LOG.info("STACK research=V56 live_hooks=V57 score=V63 ux=V64 position=V65 current_mode=V66")
    v27.base.uvicorn.run(app, host="0.0.0.0", port=v27.base.PORT,
                        access_log=False, log_level="info")
