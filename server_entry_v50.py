from __future__ import annotations

"""Final production entry with V50 sklearn seed compatibility installed atomically.

V49 already prevents Stage 6 from starting before the V30-V49 stack is complete. V50
adds one more outer startup fence so V49's authoritative kick cannot fire until the
sklearn random-state boundary has also been installed and added to V47 exact identity.
"""

import importlib
import logging

import server_entry_v49 as v49_entry
import v41_post_replay_autonomous_scheduler as scheduler_module
import v42_post_replay_resource_authority as resource_module

LOG = logging.getLogger('eth-adaptive.v50-entry')
v27 = v49_entry.v27
_ORIGINAL_V49_IMPORT = v27.base._import_production_blocking
_ORIGINAL_SCHEDULER_KICK = scheduler_module._kick
_ORIGINAL_RESOURCE_KICK = resource_module._scheduler_kick
_STACK_READY = False


def _publish_closed(core, source: str):
    state = core.state.setdefault('v50_sklearn_seed_authority', {})
    state.update({
        'schema': 50,
        'runtime': 'V50_SKLEARN_SEED_AUTHORITY',
        'startup_barrier_open': False,
        'startup_barrier_reason': 'V50 sklearn seed authority must be installed before Stage 6 may start',
        'suppressed_pre_v50_kick_source': str(source),
    })
    return dict(state)


def _gated_scheduler_kick(core, autonomous, transition, *, source: str, force_interval: bool = False):
    if not _STACK_READY:
        return _publish_closed(core, source)
    return _ORIGINAL_SCHEDULER_KICK(
        core, autonomous, transition, source=source, force_interval=force_interval,
    )


def _gated_resource_kick(core, autonomous, transition, authoritative_request, *, source: str,
                         force_interval: bool = False):
    if not _STACK_READY:
        return _publish_closed(core, source)
    return _ORIGINAL_RESOURCE_KICK(
        core, autonomous, transition, authoritative_request,
        source=source, force_interval=force_interval,
    )


# Close both known scheduler entrances before server_entry's async production loader can
# begin. V42 may later replace scheduler_module._kick, but its wrapper resolves
# resource_module._scheduler_kick dynamically, so the resource fence remains authoritative.
scheduler_module._kick = _gated_scheduler_kick
resource_module._scheduler_kick = _gated_resource_kick


def _import_production_blocking_v50():
    global _STACK_READY
    production, app = _ORIGINAL_V49_IMPORT()

    autonomous = importlib.import_module('v30_autonomous_strategy_discovery')
    integrity = importlib.import_module('v47_dataset_integrity_authority')
    transition = importlib.import_module('v26_replay_transition_stability')
    scheduler = importlib.import_module('v41_post_replay_autonomous_scheduler')
    seed_authority = importlib.import_module('v50_sklearn_seed_authority')

    # V49's own kick was suppressed by the V50 resource fence. Install the model seed
    # boundary and exact-identity participation before opening the final barrier.
    seed_authority.install(production, autonomous, integrity, transition)
    _STACK_READY = True

    state = production.core.state.setdefault(seed_authority.STATE_KEY, {})
    state['startup_barrier_open'] = True
    state['startup_barrier_reason'] = (
        'V30-V50 stack complete; sklearn random_state boundary verified; Stage 6 may start'
    )

    # Clear V49's public barrier wording only after V50 is really installed.
    orchestration = importlib.import_module('v49_stage6_atomic_orchestration')
    orchestration.mark_startup_barrier(
        production.core,
        True,
        'V30-V50 production overlays fully installed; exact Stage-6 run may start',
    )

    try:
        scheduler._kick(
            production.core,
            autonomous,
            transition,
            source='v50_seed_authority_ready',
            force_interval=True,
        )
    except Exception as exc:
        state = production.core.state.setdefault(seed_authority.STATE_KEY, {})
        state['authoritative_kick_error'] = f'{type(exc).__name__}: {exc}'
        LOG.exception('V50 authoritative Stage-6 kick failed')

    role = production.core.state.get('bootstrap_replica_role')
    if isinstance(role, dict):
        role.update({
            'research_runtime': 'V50_SKLEARN_SEED_AUTHORITY_20260818',
            'stage6_v50_seed_boundary': True,
            'stage6_arbitrary_python_seed_to_uint32': True,
            'stage6_valid_seed_identity_preserved': True,
            'v47_exact_resume_identity_includes_v50': True,
            'stage6_start_waits_for_v50': True,
            'research_data_changed_by_v50': False,
            'research_semantics_changed_except_invalid_seed_compatibility': False,
            'no_lookahead_changed_by_v50': False,
        })
    return production, app


v27.base._import_production_blocking = _import_production_blocking_v50
app = v27.base.app


if __name__ == '__main__':
    LOG.info(
        'UVICORN_BIND host=0.0.0.0 port=%s mode=AUTONOMOUS_V36 overlay=V50_SKLEARN_SEED',
        v27.base.PORT,
    )
    v27.base.uvicorn.run(
        app,
        host='0.0.0.0',
        port=v27.base.PORT,
        access_log=True,
        log_level='info',
    )
