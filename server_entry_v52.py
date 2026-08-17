from __future__ import annotations

"""Production entry: install V52 execution + Stage 1-9 authority before Stage 6 starts."""

import importlib
import logging

import server_entry_v51 as v51_entry
import v41_post_replay_autonomous_scheduler as scheduler_module
import v42_post_replay_resource_authority as resource_module

LOG = logging.getLogger('eth-adaptive.v52-entry')
v27 = v51_entry.v27
_ORIGINAL_V51_IMPORT = v27.base._import_production_blocking
_ORIGINAL_SCHEDULER_KICK = scheduler_module._kick
_ORIGINAL_RESOURCE_KICK = resource_module._scheduler_kick
_STACK_READY = False


def _closed(core, source: str):
    state = core.state.setdefault('v52_stage1_9_pipeline_authority', {})
    state.update({'schema': 52, 'runtime': 'V52_STAGE1_9_PIPELINE_AUTHORITY',
                  'startup_barrier_open': False,
                  'startup_barrier_reason': 'V52 execution/persistence/progress authority must be installed before Stage 6 starts',
                  'suppressed_pre_v52_kick_source': str(source)})
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


scheduler_module._kick = _gated_scheduler_kick
resource_module._scheduler_kick = _gated_resource_kick


def _import_production_blocking_v52():
    global _STACK_READY
    production, app = _ORIGINAL_V51_IMPORT()
    autonomous = importlib.import_module('v30_autonomous_strategy_discovery')
    throughput = importlib.import_module('v46_stage6_throughput_liveness')
    integrity = importlib.import_module('v47_dataset_integrity_authority')
    orchestration = importlib.import_module('v49_stage6_atomic_orchestration')
    transition = importlib.import_module('v26_replay_transition_stability')
    scheduler = importlib.import_module('v41_post_replay_autonomous_scheduler')
    execution52 = importlib.import_module('v52_execution_authority')
    pipeline52 = importlib.import_module('v52_pipeline_authority')

    # A promoted OOS result contains the fitted sklearn model as bytes. The vault stores
    # that model in its BLOB column; JSON audit metadata keeps only a byte-count marker.
    # _attach_audit resolves this global serializer at call time, so this is installed
    # before any Stage-6/OOS work can start.
    base_json_default = pipeline52._jd
    def v52_json_default(value):
        if isinstance(value, (bytes, bytearray, memoryview)):
            return {'__binary_model_bytes__': len(value)}
        return base_json_default(value)
    pipeline52._jd = v52_json_default

    mods = tuple(getattr(integrity, 'SEMANTIC_MODULES', ()))
    if 'server_entry_v52' not in mods:
        integrity.SEMANTIC_MODULES = mods + ('server_entry_v52',)

    execution52.install(production, autonomous, throughput, integrity)
    pipeline52.install(production, autonomous, throughput, integrity, orchestration)
    _STACK_READY = True

    state = production.core.state.setdefault(pipeline52.STATE_KEY, {})
    state['startup_barrier_open'] = True
    state['startup_barrier_reason'] = 'V30-V52 complete; exact Stage 1-9 run may start'
    orchestration.mark_startup_barrier(
        production.core, True,
        'V30-V52 production stack complete; safe-leverage Stage 6 and strategy-first persistence ready',
    )
    try:
        scheduler._kick(production.core, autonomous, transition,
                        source='v52_stage1_9_ready', force_interval=True)
    except Exception as exc:
        state['authoritative_kick_error'] = f'{type(exc).__name__}: {exc}'
        LOG.exception('V52 authoritative Stage-6 kick failed')

    role = production.core.state.get('bootstrap_replica_role')
    if isinstance(role, dict):
        role.update({
            'research_runtime': 'V52_STAGE1_9_PIPELINE_20260818',
            'stage6_safe_stop_leverage_selection': True,
            'stage6_strategy_saved_before_oos': True,
            'stage6_stale_terminal_checkpoint_fixed': True,
            'stage1_9_truthful_progress': True,
            'current_paper_after_certified_history': True,
            'v47_exact_resume_identity_includes_v52': True,
            'research_data_changed_by_v52': False,
            'final_oos_thresholds_changed_by_v52': False,
            'future_peeking_changed_by_v52': False,
        })
    return production, app


v27.base._import_production_blocking = _import_production_blocking_v52
app = v27.base.app

if __name__ == '__main__':
    LOG.info('UVICORN_BIND host=0.0.0.0 port=%s mode=AUTONOMOUS_V36 overlay=V52_STAGE1_9_PIPELINE', v27.base.PORT)
    v27.base.uvicorn.run(app, host='0.0.0.0', port=v27.base.PORT,
                        access_log=True, log_level='info')
