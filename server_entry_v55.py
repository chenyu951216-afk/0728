from __future__ import annotations

"""Production entry for V55 autonomous Champion/UI/execution convergence."""

import importlib
import logging

import server_entry_v54 as v54_entry

LOG = logging.getLogger('eth-adaptive.v55-entry')
v27 = v54_entry.v27
_ORIGINAL_V54_IMPORT = v27.base._import_production_blocking


def _import_production_blocking_v55():
    production, app = _ORIGINAL_V54_IMPORT()
    autonomous = importlib.import_module('v30_autonomous_strategy_discovery')
    execution52 = importlib.import_module('v52_execution_authority')
    authority55 = importlib.import_module('v55_autonomous_champion_authority')

    authority55.install(production, autonomous, execution52)

    role = production.core.state.get('bootstrap_replica_role')
    if isinstance(role, dict):
        role.update({
            'autonomous_champion_authority': 'V55_AUTONOMOUS_CHAMPION_AUTHORITY',
            'canonical_champion_source': 'AUTONOMOUS_COMPLETE_PACKAGE',
            'legacy_signal_execution_ui_projection': True,
            'stage9_exact_champion_genome_guard': True,
            'champion_logic_exposed_from_persisted_genome': True,
            'research_semantics_changed_by_v55': False,
            'v47_exact_identity_changed_by_v55': False,
        })
    return production, app


v27.base._import_production_blocking = _import_production_blocking_v55
app = v27.base.app

if __name__ == '__main__':
    # Keep the established V54 bind diagnostic so existing Docker smoke assertions
    # continue proving that the complete V54 runtime is underneath this post-terminal layer.
    LOG.info('UVICORN_BIND host=0.0.0.0 port=%s mode=AUTONOMOUS_V36 overlay=V54_TERMINAL_RUNTIME', v27.base.PORT)
    LOG.info('V55_CHAMPION_AUTHORITY enabled canonical_source=AUTONOMOUS_COMPLETE_PACKAGE')
    v27.base.uvicorn.run(app, host='0.0.0.0', port=v27.base.PORT,
                        access_log=True, log_level='info')
