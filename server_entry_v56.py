from __future__ import annotations

"""Production entry for V56 causal execution parity and current forward learning.

Stage 6 is fenced before importing the V55 stack. This matters because V56 changes
execution semantics (MARKET fill anchoring / trailing feasibility), so not one Stage-6
candidate may start under the previous simulator and later resume under V56.
"""

import importlib
import logging

import v41_post_replay_autonomous_scheduler as scheduler_module
import v42_post_replay_resource_authority as resource_module

LOG = logging.getLogger('eth-adaptive.v56-entry')

# Install the deepest gate BEFORE importing V55/V54/V53/... . Inner entries may wrap
# this function, but every kick ultimately reaches this closed gate until V56 is ready.
_PRE_V56_SCHEDULER_KICK = scheduler_module._kick
_PRE_V56_RESOURCE_KICK = resource_module._scheduler_kick
_V56_READY = False


def _closed(core, source: str):
    state = core.state.setdefault('v56_causal_multichampion_learning', {})
    state.update({
        'schema': 56,
        'runtime': 'V56_CAUSAL_MULTICHAMPION_ONLINE_LEARNING',
        'startup_barrier_open': False,
        'startup_barrier_reason': 'V56 canonical execution semantics must be installed before Stage 6 starts',
        'suppressed_pre_v56_kick_source': str(source),
    })
    return dict(state)


def _v56_scheduler_gate(core, autonomous, transition, *, source: str, force_interval: bool = False):
    if not _V56_READY:
        return _closed(core, source)
    return _PRE_V56_SCHEDULER_KICK(core, autonomous, transition,
                                   source=source, force_interval=force_interval)


def _v56_resource_gate(core, autonomous, transition, authoritative_request, *, source: str,
                       force_interval: bool = False):
    if not _V56_READY:
        return _closed(core, source)
    return _PRE_V56_RESOURCE_KICK(core, autonomous, transition, authoritative_request,
                                  source=source, force_interval=force_interval)


scheduler_module._kick = _v56_scheduler_gate
resource_module._scheduler_kick = _v56_resource_gate

# Import the complete existing production stack only after the deepest gate is closed.
import server_entry_v55 as v55_entry

v27 = v55_entry.v27
_ORIGINAL_V55_IMPORT = v27.base._import_production_blocking


def _import_production_blocking_v56():
    global _V56_READY
    production, app = _ORIGINAL_V55_IMPORT()

    autonomous = importlib.import_module('v30_autonomous_strategy_discovery')
    throughput = importlib.import_module('v46_stage6_throughput_liveness')
    integrity = importlib.import_module('v47_dataset_integrity_authority')
    orchestration = importlib.import_module('v49_stage6_atomic_orchestration')
    transition = importlib.import_module('v26_replay_transition_stability')
    scheduler = importlib.import_module('v41_post_replay_autonomous_scheduler')
    v56 = importlib.import_module('v56_causal_multichampion_learning')
    parallel = importlib.import_module('v56_parallel_authority')

    # All semantic modules are installed while the deepest scheduler gate is closed.
    # V47 therefore hashes the exact V56 simulator + dispatcher before any candidate
    # can be resumed or computed.
    v56.preinstall(production, autonomous, integrity, throughput)
    parallel.install(production, autonomous, integrity)
    v56.install(production, autonomous, integrity, throughput)

    _V56_READY = True
    state = production.core.state.setdefault(v56.STATE_KEY, {})
    state['startup_barrier_open'] = True
    state['startup_barrier_reason'] = (
        'V30-V56 stack complete; corrected causal execution semantics are authoritative'
    )
    orchestration.mark_startup_barrier(
        production.core, True,
        'V56 canonical execution + bounded parallel + multi-Champion current-learning stack is fully installed',
    )

    try:
        scheduler._kick(production.core, autonomous, transition,
                        source='v56_causal_runtime_ready', force_interval=True)
    except Exception as exc:
        state['authoritative_kick_error'] = f'{type(exc).__name__}: {exc}'
        LOG.exception('V56 authoritative Stage-6 kick failed')

    role = production.core.state.get('bootstrap_replica_role')
    if isinstance(role, dict):
        role.update({
            'research_runtime': 'V56_CAUSAL_MULTICHAMPION_20260818',
            'stage6_start_waits_for_v56': True,
            'market_entry_stop_anchored_to_actual_fill': True,
            'impossible_trailing_lock_forbidden': True,
            'historical_live_execution_semantics_unified': True,
            'multiple_champions_supported': True,
            'single_eth_position_conflict_arbiter': True,
            'current_forward_learning_after_handoff': True,
            'historical_oos_immutable_after_handoff': True,
            'online_challenger_future_only_validation': True,
            'v56_bounded_path_workers': True,
            'v47_exact_resume_identity_includes_v56': True,
        })
    return production, app


v27.base._import_production_blocking = _import_production_blocking_v56
app = v27.base.app

if __name__ == '__main__':
    # Keep the legacy V54 token for the existing deployment smoke diagnostic, while
    # publishing the actual final overlay on the following line.
    LOG.info('UVICORN_BIND host=0.0.0.0 port=%s mode=AUTONOMOUS_V36 overlay=V54_TERMINAL_RUNTIME', v27.base.PORT)
    LOG.info('FINAL_OVERLAY=V56_CAUSAL_MULTICHAMPION_ONLINE_LEARNING')
    v27.base.uvicorn.run(app, host='0.0.0.0', port=v27.base.PORT,
                        access_log=True, log_level='info')
