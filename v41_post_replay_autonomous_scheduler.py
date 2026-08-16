from __future__ import annotations

"""Deterministic Strict-Replay -> autonomous-research scheduler.

V40 fixes the market-cache handoff itself. This layer fixes the remaining orchestration
ambiguity visible when the replay is already 100% and the public learning card says
READY_FOR_SIGNAL_CERTIFICATION while autonomous discovery is still at 0%.

The V26 request path remains the only certification authority: this module never calls
training directly and never bypasses storage, memory, provenance, OOS or no-lookahead
gates. It simply guarantees that the authoritative request function is re-invoked after
completed replay (at boot, after every learning tick, and as a scan-loop fallback) until
it is queued/running/complete, and it exposes the real transition state instead of a
misleading generic READY label.
"""

import os
import time
from typing import Any

import v16_runtime_integrity as runtime_integrity

VERSION = 'V41_POST_REPLAY_AUTONOMOUS_SCHEDULER'
SCHEMA = 41
STATE_KEY = 'v41_post_replay_autonomous_scheduler'
MIN_KICK_SECONDS = max(3, min(60, int(os.getenv('AUTONOMOUS_POST_REPLAY_KICK_SECONDS', '8'))))

_RUNNING_AUTO_STATES = {
    'AUTONOMOUS_EVOLUTION_RUNNING',
    'COMPLETE',
    'COMPLETE_NO_CERTIFIED_PACKAGE',
}
_TRANSITION_ACTIVE = {
    'CERTIFICATION_QUEUED_BACKGROUND',
    'CERTIFICATION_RUNNING',
    'REPLAY_COMPLETE_COOLDOWN',
    'CERTIFICATION_DEFERRED_MEMORY_PRESSURE',
    'CERTIFICATION_MEMORY_ERROR',
    'RECOVERED_AFTER_INTERRUPTED_CERTIFICATION',
}


def _dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _transition_state(core: Any, transition: Any) -> dict[str, Any]:
    try:
        return _dict(core.get_state(transition.STATE_KEY, {}))
    except Exception:
        return _dict(core.state.get('replay_transition_stability'))


def _autonomous_state(core: Any, autonomous: Any) -> dict[str, Any]:
    try:
        return _dict(autonomous.autonomous_status(core))
    except Exception as exc:
        return {'status': 'STATUS_ERROR', 'error': f'{type(exc).__name__}: {exc}'}


def _replay(core: Any) -> dict[str, Any]:
    try:
        return _dict(runtime_integrity.replay_progress(core))
    except Exception as exc:
        return {'complete': False, 'error': f'{type(exc).__name__}: {exc}'}


def _role(core: Any) -> str:
    role = _dict(core.state.get('runtime_role')).get('role')
    if role:
        return str(role)
    role = _dict(core.state.get('bootstrap_replica_role')).get('role')
    return str(role or 'UNKNOWN')


def _phase_from_truth(core: Any, transition: Any, auto: dict[str, Any], trans: dict[str, Any]) -> tuple[str, str | None]:
    astatus = str(auto.get('status') or '')
    active = _dict(auto.get('active'))
    stage = str(active.get('stage') or '')
    tstatus = str(trans.get('status') or '')
    now = int(time.time())
    ready_after = int(trans.get('ready_after') or 0)

    if auto.get('research_complete'):
        return ('AUTONOMOUS_RESEARCH_COMPLETE' if auto.get('champions') else 'AUTONOMOUS_RESEARCH_COMPLETE_NO_CERTIFIED_PACKAGE', None)
    if astatus == 'AUTONOMOUS_EVOLUTION_RUNNING' or stage == 'DIRECT_R_AUTONOMOUS_EVOLUTION':
        return 'AUTONOMOUS_DIRECT_R_EVOLUTION_RUNNING', None
    if stage == 'ONE_TIME_COMPLETE_PACKAGE_OOS':
        return 'AUTONOMOUS_COMPLETE_PACKAGE_OOS_RUNNING', None
    if tstatus == 'CERTIFICATION_RUNNING':
        return 'AUTONOMOUS_RESEARCH_BOOTSTRAP_RUNNING', None
    if tstatus == 'CERTIFICATION_QUEUED_BACKGROUND':
        return 'AUTONOMOUS_RESEARCH_QUEUED', None
    if ready_after > now:
        remain = ready_after - now
        return 'AUTONOMOUS_RESEARCH_SAFETY_BACKOFF', f'authoritative certification retry in {remain}s ({tstatus or "backoff"})'
    if astatus == 'WAITING_MARKET_CACHE':
        market = _dict(auto.get('market_cache_integrity'))
        if market.get('status') == 'VALID':
            return 'AUTONOMOUS_RESEARCH_READY_TO_QUEUE', 'market cache valid; authoritative certification request will be retried'
        return 'WAITING_AUTONOMOUS_MARKET_CACHE_INTEGRITY', str(market.get('status') or 'market cache has not been validated')
    if astatus in _RUNNING_AUTO_STATES:
        return astatus, None
    return 'AUTONOMOUS_RESEARCH_READY_TO_QUEUE', None


def _publish(core: Any, transition: Any, auto: dict[str, Any], trans: dict[str, Any], patch: dict[str, Any] | None = None) -> dict[str, Any]:
    replay = _replay(core)
    phase, blocker = _phase_from_truth(core, transition, auto, trans)
    state = _dict(core.state.get(STATE_KEY))
    state.update({
        'schema': SCHEMA,
        'runtime': VERSION,
        'replay_complete': bool(replay.get('complete')),
        'replay_percent': float(replay.get('percent') or 0.0),
        'autonomous_status': auto.get('status'),
        'autonomous_research_complete': bool(auto.get('research_complete')),
        'autonomous_active': _dict(auto.get('active')),
        'transition_status': trans.get('status'),
        'transition_ready_after': int(trans.get('ready_after') or 0),
        'replica_role': _role(core),
        'authoritative_request': 'core.train_if_due -> V26 background certification authority',
        'direct_training_bypass': False,
        'raw_data_reset': False,
        'replay_reset': False,
        'future_peeking': False,
        'updated_at': int(time.time()),
    })
    if patch:
        state.update(patch)
    core.state[STATE_KEY] = state

    # The Point-in-Time card is a public status view. Once replay is complete it must
    # not keep saying generic READY when the real autonomous handoff is queued/running
    # or deliberately in a safety backoff.
    if replay.get('complete'):
        lr = core.state.setdefault('learning', {})
        lr['phase'] = phase
        lr['blocker'] = blocker
    return state


def _kick(core: Any, autonomous: Any, transition: Any, *, source: str, force_interval: bool = False) -> dict[str, Any]:
    replay = _replay(core)
    auto = _autonomous_state(core, autonomous)
    trans = _transition_state(core, transition)
    now = int(time.time())
    previous = _dict(core.state.get(STATE_KEY))

    if not replay.get('complete'):
        return _publish(core, transition, auto, trans, {'last_source': source, 'decision': 'WAIT_REPLAY'})
    if auto.get('research_complete'):
        return _publish(core, transition, auto, trans, {'last_source': source, 'decision': 'RESEARCH_COMPLETE'})

    role = _role(core)
    if role.startswith('FOLLOWER'):
        return _publish(core, transition, auto, trans, {'last_source': source, 'decision': 'FOLLOWER_READ_ONLY'})

    ready_after = int(trans.get('ready_after') or 0)
    if ready_after > now:
        return _publish(core, transition, auto, trans, {
            'last_source': source,
            'decision': 'WAIT_AUTHORITATIVE_BACKOFF',
            'retry_in_seconds': ready_after - now,
        })

    # If V26 already owns an active request, do not create noise. The V26 executor and
    # heavy lock remain authoritative; this scheduler only supplies missing requests.
    tstatus = str(trans.get('status') or '')
    if tstatus in ('CERTIFICATION_QUEUED_BACKGROUND', 'CERTIFICATION_RUNNING'):
        return _publish(core, transition, auto, trans, {'last_source': source, 'decision': 'AUTHORITY_ALREADY_ACTIVE'})

    last_kick = int(previous.get('last_kick_at') or 0)
    if not force_interval and now - last_kick < MIN_KICK_SECONDS:
        return _publish(core, transition, auto, trans, {
            'last_source': source,
            'decision': 'THROTTLED',
            'next_kick_in_seconds': MIN_KICK_SECONDS - (now - last_kick),
        })

    requested = False
    error = None
    try:
        # After V26 is installed this is its idempotent request_certification wrapper.
        # Calling it does not execute sklearn on the web event loop.
        core.train_if_due(False)
        requested = True
    except Exception as exc:
        error = f'{type(exc).__name__}: {exc}'

    trans_after = _transition_state(core, transition)
    auto_after = _autonomous_state(core, autonomous)
    patch = {
        'last_source': source,
        'last_kick_at': now,
        'kick_count': int(previous.get('kick_count') or 0) + 1,
        'request_called': requested,
        'request_error': error,
        'decision': 'REQUESTED_AUTHORITATIVE_CERTIFICATION' if requested else 'REQUEST_ERROR',
        'transition_before': tstatus,
        'transition_after': trans_after.get('status'),
    }
    return _publish(core, transition, auto_after, trans_after, patch)


def install(production: Any, autonomous: Any, transition: Any) -> None:
    core = production.core
    if getattr(core, '_v41_post_replay_scheduler_installed', False):
        return
    core._v41_post_replay_scheduler_installed = True

    original_learning_tick = core.learning_tick
    original_scan = core.scan

    async def scheduled_learning_tick() -> None:
        await original_learning_tick()
        _kick(core, autonomous, transition, source='learning_tick')

    async def scheduled_scan() -> Any:
        result = await original_scan()
        _kick(core, autonomous, transition, source='scan_fallback')
        return result

    core.learning_tick = scheduled_learning_tick
    core.scan = scheduled_scan

    core.state.setdefault('strict_replay', {})['post_replay_autonomous_scheduler_v41'] = {
        'runtime': VERSION,
        'boot_kick': True,
        'learning_tick_kick': True,
        'scan_fallback_kick': True,
        'minimum_kick_seconds': MIN_KICK_SECONDS,
        'v26_background_authority_preserved': True,
        'storage_memory_provenance_gates_bypassed': False,
        'replay_or_raw_data_reset': False,
        'future_peeking': False,
    }

    # Important for persisted deployments where replay finished before this runtime was
    # installed: do not wait for another historical sample or another code path to
    # notice completion. V26's request function is non-blocking and idempotent.
    try:
        _kick(core, autonomous, transition, source='boot', force_interval=True)
    except Exception as exc:
        core.state[STATE_KEY] = {
            'schema': SCHEMA,
            'runtime': VERSION,
            'decision': 'BOOT_KICK_ERROR',
            'request_error': f'{type(exc).__name__}: {exc}',
            'updated_at': int(time.time()),
        }

    app = getattr(core, 'app', None)
    routes = list(getattr(getattr(app, 'router', None), 'routes', []) or []) if app is not None else []
    if app is not None and not any(getattr(r, 'path', None) == '/api/v41/autonomous-scheduler' for r in routes):
        @app.get('/api/v41/autonomous-scheduler')
        def scheduler_status() -> dict[str, Any]:
            auto = _autonomous_state(core, autonomous)
            trans = _transition_state(core, transition)
            return _publish(core, transition, auto, trans)
