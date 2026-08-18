from __future__ import annotations

"""Production entry: terminal-aware Stage 1-9 convergence on top of V53.

The pre-import scheduler fence prevents a completed durable autonomous run from being
re-kicked during a redeploy.  V54 itself changes no research semantics and therefore is
intentionally excluded from V47 semantic identity.
"""

import importlib
import logging

import v41_post_replay_autonomous_scheduler as scheduler_module
import v42_post_replay_resource_authority as resource_module

LOG = logging.getLogger('eth-adaptive.v54-entry')
_PRE_V54_SCHEDULER_KICK = scheduler_module._kick
_PRE_V54_RESOURCE_KICK = resource_module._scheduler_kick


def _already_terminal(core, autonomous, source: str):
    try:
        cp = core.get_state(autonomous.CHECKPOINT_KEY, {})
        cp = dict(cp) if isinstance(cp, dict) else {}
        champions = list(autonomous._load_registry(core, active_only=True) or [])
    except Exception:
        return None
    if str(cp.get('status') or '') != 'COMPLETE' or not champions:
        return None
    gate = {
        'schema': 54,
        'runtime': 'V54_TERMINAL_RUNTIME_AUTHORITY',
        'suppressed': True,
        'source': str(source),
        'reason': 'durable historical checkpoint + certified autonomous Champion already complete; Stage 6 boot kick suppressed',
        'champions': len(champions),
    }
    core.state['v54_terminal_boot_gate'] = gate
    return gate


def _pre_gate_scheduler(core, autonomous, transition, *, source: str, force_interval: bool = False):
    terminal = _already_terminal(core, autonomous, source)
    if terminal is not None:
        return terminal
    return _PRE_V54_SCHEDULER_KICK(
        core, autonomous, transition, source=source, force_interval=force_interval,
    )


def _pre_gate_resource(core, autonomous, transition, authoritative_request, *, source: str,
                       force_interval: bool = False):
    terminal = _already_terminal(core, autonomous, source)
    if terminal is not None:
        return terminal
    return _PRE_V54_RESOURCE_KICK(
        core, autonomous, transition, authoritative_request,
        source=source, force_interval=force_interval,
    )


# This fence must exist before the V49/V52 entry modules capture their downstream kick
# functions.  A fresh/incomplete run passes straight through unchanged.
scheduler_module._kick = _pre_gate_scheduler
resource_module._scheduler_kick = _pre_gate_resource

import server_entry_v53 as v53_entry

v27 = v53_entry.v27
_ORIGINAL_V53_IMPORT = v27.base._import_production_blocking


def _import_production_blocking_v54():
    production, app = _ORIGINAL_V53_IMPORT()
    autonomous = importlib.import_module('v30_autonomous_strategy_discovery')
    throughput = importlib.import_module('v46_stage6_throughput_liveness')
    integrity = importlib.import_module('v47_dataset_integrity_authority')
    orchestration = importlib.import_module('v49_stage6_atomic_orchestration')
    pipeline52 = importlib.import_module('v52_pipeline_authority')
    performance = importlib.import_module('v43_unified_performance_authority')
    authority54 = importlib.import_module('v54_terminal_runtime_authority')

    # Do NOT add V54/server_entry_v54 to V47 SEMANTIC_MODULES.  This layer controls
    # runtime convergence, boot scheduling, UI truth and resources only.
    authority54.install(
        production, autonomous, throughput, integrity, orchestration, pipeline52, performance,
    )

    role = production.core.state.get('bootstrap_replica_role')
    if isinstance(role, dict):
        role.update({
            'terminal_runtime_authority': 'V54_TERMINAL_RUNTIME_AUTHORITY',
            'research_semantics_changed_by_v54': False,
            'v47_exact_identity_changed_by_v54': False,
            'terminal_stage6_rekick_suppressed': True,
            'terminal_stage6_executor_release': True,
            'post_restart_v47_v49_state_restoration': True,
            'autonomous_current_paper_health_convergence': True,
            'memory_bounded_same_candidate_parallelism': True,
        })
    return production, app


v27.base._import_production_blocking = _import_production_blocking_v54
app = v27.base.app

if __name__ == '__main__':
    LOG.info('UVICORN_BIND host=0.0.0.0 port=%s mode=AUTONOMOUS_V36 overlay=V54_TERMINAL_RUNTIME', v27.base.PORT)
    v27.base.uvicorn.run(app, host='0.0.0.0', port=v27.base.PORT,
                        access_log=True, log_level='info')
