from __future__ import annotations

"""Final production entry with an atomic Stage-6 startup barrier.

Older entries could boot-kick autonomous research from V41/V42/V44 while later
V43-V48 monkey-patches were still being installed.  A candidate could therefore begin
under a partially composed runtime and finish under another one.  V49 closes every
post-replay scheduler gate before the production import starts, installs the complete
V30-V49 stack, then opens the gate and issues one authoritative kick.
"""

import importlib
import logging

import server_entry_v48 as v48_entry
import v41_post_replay_autonomous_scheduler as scheduler_module
import v42_post_replay_resource_authority as resource_module

LOG = logging.getLogger('eth-adaptive.v49-entry')
v27 = v48_entry.v27
_ORIGINAL_V48_IMPORT = v27.base._import_production_blocking
_ORIGINAL_V41_KICK = scheduler_module._kick
_ORIGINAL_V42_KICK = resource_module._scheduler_kick
_STACK_READY = False


def _publish_closed(core, source: str):
    state = core.state.setdefault('v49_stage6_atomic_orchestration', {})
    state.update({
        'schema': 49,
        'runtime': 'V49_STAGE6_ATOMIC_ORCHESTRATION',
        'startup_barrier_open': False,
        'startup_barrier_reason': 'all V30-V49 overlays must be installed before Stage 6 may start',
        'suppressed_pre_stack_kick_source': str(source),
    })
    return dict(state)


def _gated_v41_kick(core, autonomous, transition, *, source: str, force_interval: bool = False):
    if not _STACK_READY:
        return _publish_closed(core, source)
    return _ORIGINAL_V41_KICK(
        core, autonomous, transition, source=source, force_interval=force_interval,
    )


def _gated_v42_kick(core, autonomous, transition, authoritative_request, *, source: str,
                     force_interval: bool = False):
    if not _STACK_READY:
        return _publish_closed(core, source)
    return _ORIGINAL_V42_KICK(
        core, autonomous, transition, authoritative_request,
        source=source, force_interval=force_interval,
    )


# Install the gate at module import time. server_entry's async production loader has not
# begun yet, so V41's boot kick and all V42/V44 follow-up kicks are fenced deterministically.
scheduler_module._kick = _gated_v41_kick
resource_module._scheduler_kick = _gated_v42_kick


def _install_production_health_routes(production) -> None:
    """Keep liveness endpoints valid before and after DynamicProductionApp swaps apps."""
    paths = {getattr(r, 'path', None) for r in production.app.router.routes}
    if '/healthz' not in paths:
        @production.app.get('/healthz')
        def production_healthz():
            return {
                'ok': True, 'alive': True, 'ready': True,
                'startup_status': 'PRODUCTION_READY',
                'startup_error_type': None, 'port': v27.base.PORT,
                'bootstrap_replica_role': getattr(v27.base, '_BOOTSTRAP_ROLE', 'UNKNOWN'),
                'stage6_atomic_barrier_open': bool(_STACK_READY),
            }
    if '/readyz' not in paths:
        @production.app.get('/readyz')
        def production_readyz():
            return {
                'ok': True, 'ready': True,
                'startup_status': 'PRODUCTION_READY',
                'startup_error_type': None, 'port': v27.base.PORT,
                'bootstrap_replica_role': getattr(v27.base, '_BOOTSTRAP_ROLE', 'UNKNOWN'),
                'stage6_atomic_barrier_open': bool(_STACK_READY),
            }


def _import_production_blocking_v49():
    global _STACK_READY
    production, app = _ORIGINAL_V48_IMPORT()
    autonomous = importlib.import_module('v30_autonomous_strategy_discovery')
    throughput = importlib.import_module('v46_stage6_throughput_liveness')
    integrity = importlib.import_module('v47_dataset_integrity_authority')
    transition = importlib.import_module('v26_replay_transition_stability')
    scheduler = importlib.import_module('v41_post_replay_autonomous_scheduler')
    orchestration = importlib.import_module('v49_stage6_atomic_orchestration')

    # V48 has already wrapped candidate evaluation/resource liveness at this point.
    # V49 now owns only the outer Stage-6 orchestration and exception visibility.
    orchestration.install(
        production, autonomous, throughput, integrity, transition, scheduler,
    )

    _STACK_READY = True
    orchestration.mark_startup_barrier(
        production.core, True,
        'V30-V49 production overlays fully installed; one authoritative Stage-6 kick is now allowed',
    )
    _install_production_health_routes(production)

    # scheduler._kick is V42's installed wrapper by now. Its call reaches the gated
    # resource-module function above, which delegates because _STACK_READY is true.
    try:
        scheduler._kick(
            production.core, autonomous, transition,
            source='v49_atomic_stack_ready', force_interval=True,
        )
    except Exception as exc:
        state = production.core.state.setdefault('v49_stage6_atomic_orchestration', {})
        state['atomic_kick_error'] = f'{type(exc).__name__}: {exc}'
        LOG.exception('V49 authoritative Stage-6 kick failed')

    role = production.core.state.get('bootstrap_replica_role')
    if isinstance(role, dict):
        role.update({
            'research_runtime': 'V49_STAGE6_ATOMIC_ORCHESTRATION_20260818',
            'stage6_atomic_startup_barrier': True,
            'stage6_no_pre_overlay_boot_kick': True,
            'stage6_outer_candidate_cursor_durable': True,
            'stage6_background_general_exception_recovery': True,
            'v47_exact_resume_identity_includes_v49': True,
            'research_semantics_changed_by_v49': False,
            'no_lookahead_changed_by_v49': False,
            'health_routes_survive_dynamic_app_swap': True,
        })
    return production, app


v27.base._import_production_blocking = _import_production_blocking_v49
app = v27.base.app


if __name__ == '__main__':
    # Preserve the established smoke-token while adding the final authority marker.
    LOG.info(
        'UVICORN_BIND host=0.0.0.0 port=%s mode=AUTONOMOUS_V36 overlay=V49_STAGE6_ATOMIC',
        v27.base.PORT,
    )
    v27.base.uvicorn.run(
        app, host='0.0.0.0', port=v27.base.PORT,
        access_log=True, log_level='info',
    )
