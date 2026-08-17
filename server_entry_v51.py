from __future__ import annotations

"""Production entry that atomically installs V51 before Stage 6 may start."""

import importlib
import logging

import server_entry_v50 as v50_entry
import v41_post_replay_autonomous_scheduler as scheduler_module
import v42_post_replay_resource_authority as resource_module

LOG = logging.getLogger('eth-adaptive.v51-entry')
v27 = v50_entry.v27
_ORIGINAL_V50_IMPORT = v27.base._import_production_blocking
_ORIGINAL_SCHEDULER_KICK = scheduler_module._kick
_ORIGINAL_RESOURCE_KICK = resource_module._scheduler_kick
_STACK_READY = False


def _closed(core, source: str):
    state = core.state.setdefault('v51_evolution_survivability_authority', {})
    state.update({'schema': 51, 'runtime': 'V51_EVOLUTION_SURVIVABILITY_AUTHORITY',
                  'startup_barrier_open': False,
                  'startup_barrier_reason': 'V51 evaluator/evolution survivability must be installed before Stage 6 starts',
                  'suppressed_pre_v51_kick_source': str(source)})
    return dict(state)


def _gated_scheduler_kick(core, autonomous, transition, *, source: str, force_interval: bool = False):
    if not _STACK_READY:
        return _closed(core, source)
    return _ORIGINAL_SCHEDULER_KICK(core, autonomous, transition,
                                    source=source, force_interval=force_interval)


def _gated_resource_kick(core, autonomous, transition, authoritative_request, *, source: str,
                         force_interval: bool = False):
    if not _STACK_READY:
        return _closed(core, source)
    return _ORIGINAL_RESOURCE_KICK(core, autonomous, transition, authoritative_request,
                                   source=source, force_interval=force_interval)


# V50's final kick resolves scheduler_module._kick dynamically, so closing this gate
# here also suppresses V50's kick until V51 has rebuilt the candidate/evolution stack.
scheduler_module._kick = _gated_scheduler_kick
resource_module._scheduler_kick = _gated_resource_kick


def _import_production_blocking_v51():
    global _STACK_READY
    production, app = _ORIGINAL_V50_IMPORT()

    autonomous = importlib.import_module('v30_autonomous_strategy_discovery')
    throughput = importlib.import_module('v46_stage6_throughput_liveness')
    integrity = importlib.import_module('v47_dataset_integrity_authority')
    continuity = importlib.import_module('v48_runtime_continuity')
    orchestration = importlib.import_module('v49_stage6_atomic_orchestration')
    transition = importlib.import_module('v26_replay_transition_stability')
    scheduler = importlib.import_module('v41_post_replay_autonomous_scheduler')
    v51 = importlib.import_module('v51_evolution_survivability_authority')

    v51.install(production, autonomous, throughput, integrity, continuity, orchestration)
    _STACK_READY = True

    state = production.core.state.setdefault(v51.STATE_KEY, {})
    state['startup_barrier_open'] = True
    state['startup_barrier_reason'] = (
        'V30-V51 stack complete; search-only evolution guidance is isolated from final OOS'
    )
    orchestration.mark_startup_barrier(
        production.core, True,
        'V30-V51 production stack fully installed; exact Stage-6 research may start',
    )

    try:
        scheduler._kick(production.core, autonomous, transition,
                        source='v51_survivability_ready', force_interval=True)
    except Exception as exc:
        state = production.core.state.setdefault(v51.STATE_KEY, {})
        state['authoritative_kick_error'] = f'{type(exc).__name__}: {exc}'
        LOG.exception('V51 authoritative Stage-6 kick failed')

    role = production.core.state.get('bootstrap_replica_role')
    if isinstance(role, dict):
        role.update({
            'research_runtime': 'V51_EVOLUTION_SURVIVABILITY_20260818',
            'stage6_search_only_parent_selection': True,
            'stage6_search_only_finalist_forbidden': True,
            'stage6_all_generations_survivable': True,
            'stage6_causal_rejection_diagnostics': True,
            'v47_exact_resume_identity_includes_v51': True,
            'stage6_start_waits_for_v51': True,
            'research_data_changed_by_v51': False,
            'final_oos_thresholds_changed_by_v51': False,
            'no_lookahead_changed_by_v51': False,
        })
    return production, app


v27.base._import_production_blocking = _import_production_blocking_v51
app = v27.base.app

if __name__ == '__main__':
    LOG.info(
        'UVICORN_BIND host=0.0.0.0 port=%s mode=AUTONOMOUS_V36 overlay=V51_EVOLUTION_SURVIVABILITY',
        v27.base.PORT,
    )
    v27.base.uvicorn.run(app, host='0.0.0.0', port=v27.base.PORT,
                        access_log=True, log_level='info')
