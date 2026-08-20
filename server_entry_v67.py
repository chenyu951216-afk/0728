from __future__ import annotations

"""Production entry for V67 parallel Current-Time roster/UI authority."""

import importlib
import logging

import server_entry_v66 as v66_entry

LOG = logging.getLogger("eth-adaptive.v67-entry")
v27 = v66_entry.v27
_ORIGINAL_V66_IMPORT = v27.base._import_production_blocking


def _import_production_blocking_v67():
    production, app = _ORIGINAL_V66_IMPORT()
    v67 = importlib.import_module("v67_current_parallel_ui")
    v67.install(production)

    role = production.core.state.get("bootstrap_replica_role")
    if isinstance(role, dict):
        role.update({
            "final_runtime_overlay": "V67_PARALLEL_CURRENT_UI_PLACEMENT",
            "production_entry": "server_entry_v67.py",
            "forced_current_time_authority": "V66_FORCED_CURRENT_TIME_STRATEGY_EXPLAINER",
            "parallel_current_ui_authority": "V67_PARALLEL_CURRENT_UI_PLACEMENT",
        })
    return production, app


v27.base._import_production_blocking = _import_production_blocking_v67
app = v27.base.app

if __name__ == "__main__":
    LOG.info("UVICORN_BIND host=0.0.0.0 port=%s mode=AUTONOMOUS_V36 overlay=V54_TERMINAL_RUNTIME", v27.base.PORT)
    LOG.info("UVICORN_BIND host=0.0.0.0 port=%s final_overlay=V67_PARALLEL_CURRENT_UI_PLACEMENT", v27.base.PORT)
    LOG.info("STACK research=V56 score=V63 roster=V65 current_mode=V66 ui=V67")
    v27.base.uvicorn.run(app, host="0.0.0.0", port=v27.base.PORT,
                        access_log=False, log_level="info")
