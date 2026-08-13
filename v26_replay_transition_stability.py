from __future__ import annotations

import ctypes
import gc
import os
import threading
import time
from typing import Any

import execution_v7
import runtime_identity
import v5_runtime
import v16_runtime_integrity as runtime_integrity
import v17_certification_orchestrator as cert17
import v18_final_system as final_system
import v18_operational_guard as operational_guard
import v20_historical_signal_evolution as signal_evolution


VERSION = runtime_identity.RUNTIME_VERSION
STATE_KEY = 'v26_replay_transition_stability'
NEAR_END_PCT = max(90.0, min(99.9, float(os.getenv('REPLAY_STABILITY_NEAR_END_PCT', '95.0'))))
FINAL_PCT = max(NEAR_END_PCT, min(99.99, float(os.getenv('REPLAY_STABILITY_FINAL_PCT', '99.5'))))
NEAR_END_BATCH = max(80, min(500, int(os.getenv('REPLAY_STABILITY_NEAR_END_BATCH', '240'))))
FINAL_BATCH = max(20, min(200, int(os.getenv('REPLAY_STABILITY_FINAL_BATCH', '64'))))
COMPLETION_COOLDOWN_SECONDS = max(60, min(900, int(os.getenv('REPLAY_CERT_COOLDOWN_SECONDS', '180'))))
INTERRUPTED_BACKOFF_SECONDS = max(180, min(1800, int(os.getenv('REPLAY_CERT_CRASH_BACKOFF_SECONDS', '600'))))
AUDIT_CACHE_SECONDS = max(30, min(300, int(os.getenv('REPLAY_STABILITY_AUDIT_CACHE_SECONDS', '90'))))

_HEAVY_LOCK = threading.RLock()
_INSTALL_LOCK = threading.Lock()
_INSTALLED = False


def _persist(core: Any, patch: dict[str, Any]) -> dict[str, Any]:
    raw = core.get_state(STATE_KEY, {})
    state = dict(raw) if isinstance(raw, dict) else {}
    state.update(patch)
    state['runtime'] = VERSION
    state['updated_at'] = int(time.time())
    core.set_state(STATE_KEY, state)
    core.state['replay_transition_stability'] = state
    return state


def _trim_heap() -> dict[str, Any]:
    collected = 0
    malloc_trimmed = False
    try:
        collected = int(gc.collect())
    except Exception:
        collected = 0
    try:
        libc = ctypes.CDLL(None)
        malloc_trim = getattr(libc, 'malloc_trim', None)
        if malloc_trim is not None:
            malloc_trim.argtypes = [ctypes.c_size_t]
            malloc_trim.restype = ctypes.c_int
            malloc_trimmed = bool(malloc_trim(0))
    except Exception:
        malloc_trimmed = False
    return {'gc_collected': collected, 'malloc_trimmed': malloc_trimmed}


def _checkpoint(core: Any) -> dict[str, Any]:
    out: dict[str, Any] = {'ok': False}
    con = None
    try:
        con = core.db()
        row = con.execute('PRAGMA wal_checkpoint(PASSIVE)').fetchone()
        out = {
            'ok': True,
            'busy': int(row[0]) if row else None,
            'wal_pages': int(row[1]) if row else None,
            'checkpointed_pages': int(row[2]) if row else None,
        }
        try:
            con.execute('PRAGMA optimize')
        except Exception:
            pass
    except Exception as exc:
        out = {'ok': False, 'error': f'{type(exc).__name__}: {exc}'}
    finally:
        if con is not None:
            try:
                con.close()
            except Exception:
                pass
    return out


def _release_memory(core: Any | None = None, *, checkpoint: bool = False) -> dict[str, Any]:
    result: dict[str, Any] = {}
    if checkpoint and core is not None:
        result['sqlite_checkpoint'] = _checkpoint(core)
    result.update(_trim_heap())
    return result


def _known_replay(core: Any, *, refresh_near_finish: bool = False) -> dict[str, Any]:
    learning = core.state.get('learning') if isinstance(core.state.get('learning'), dict) else {}
    replay = learning.get('replay_learning_progress') if isinstance(learning.get('replay_learning_progress'), dict) else None
    if not replay:
        view = core.state.get('final_system_view') if isinstance(core.state.get('final_system_view'), dict) else {}
        replay = view.get('replay') if isinstance(view.get('replay'), dict) else None
    if replay:
        pct = float(replay.get('percent') or 0.0)
        if replay.get('complete') or not refresh_near_finish or pct < FINAL_PCT:
            return dict(replay)
    try:
        return dict(runtime_integrity.replay_progress(core) or {})
    except Exception:
        return dict(replay or {})


def _batch_cap(replay: dict[str, Any], requested: int) -> int:
    pct = float(replay.get('percent') or 0.0)
    pending_raw = replay.get('pending_eligible_decisions')
    pending = int(pending_raw) if pending_raw is not None else None
    cap = requested
    if pct >= FINAL_PCT:
        cap = min(cap, FINAL_BATCH)
    elif pct >= NEAR_END_PCT:
        cap = min(cap, NEAR_END_BATCH)
    if pending is not None and pending > 0:
        cap = min(cap, pending)
    return max(1, int(cap))


def _recover_interrupted_attempt(core: Any) -> None:
    raw = core.get_state(STATE_KEY, {})
    state = dict(raw) if isinstance(raw, dict) else {}
    if state.get('status') != 'CERTIFICATION_RUNNING':
        core.state['replay_transition_stability'] = state
        return
    started = int(state.get('certification_started_at') or 0)
    finished = int(state.get('certification_finished_at') or 0)
    if started <= finished:
        return
    now = int(time.time())
    interrupted = int(state.get('interrupted_certification_attempts') or 0) + 1
    _persist(core, {
        'status': 'RECOVERED_AFTER_INTERRUPTED_CERTIFICATION',
        'interrupted_certification_attempts': interrupted,
        'previous_certification_started_at': started,
        'ready_after': now + INTERRUPTED_BACKOFF_SECONDS,
        'reason': 'previous process ended while final certification was marked RUNNING; backing off before another memory-heavy attempt',
        'raw_market_preserved': True,
        'raw_derivatives_preserved': True,
        'learning_samples_preserved': True,
        'replay_cursor_preserved': True,
    })


def install(core: Any) -> None:
    global _INSTALLED
    with _INSTALL_LOCK:
        if _INSTALLED:
            return
        _INSTALLED = True

    _recover_interrupted_attempt(core)

    # Dashboard refreshes must not repeatedly execute whole-dataset audits while the
    # replay/certification transition is under memory pressure.
    operational_guard.VIEW_TTL_SECONDS = max(int(operational_guard.VIEW_TTL_SECONDS), AUDIT_CACHE_SECONDS)

    original_generate = v5_runtime.generate_learning_samples_v5

    def stable_generate(c: Any, batch: int | None = None) -> int:
        requested = 500 if batch is None else max(1, int(batch))
        replay = _known_replay(c, refresh_near_finish=True)
        target = _batch_cap(replay, requested)
        pct = float(replay.get('percent') or 0.0)
        _persist(c, {
            'status': 'REPLAY_RUNNING' if not replay.get('complete') else 'REPLAY_COMPLETE',
            'replay_percent_before_batch': pct,
            'requested_replay_batch': requested,
            'effective_replay_batch': target,
            'pending_before_batch': replay.get('pending_eligible_decisions'),
            'memory_guard_active': pct >= NEAR_END_PCT,
        })
        try:
            return int(original_generate(c, target) or 0)
        finally:
            # At the end of replay the next legacy call is training/certification.
            # Explicitly release the large full-history candle/list allocations first.
            if pct >= NEAR_END_PCT:
                mem = _release_memory(c, checkpoint=True)
                _persist(c, {'last_post_replay_memory_release': mem})

    v5_runtime.generate_learning_samples_v5 = stable_generate
    core.generate_learning_samples = lambda batch=None: stable_generate(core, batch)

    base_certify = final_system.certify_and_execute

    def stable_certify(c: Any, force: bool = False):
        replay = _known_replay(c, refresh_near_finish=True)
        if not replay.get('complete'):
            # This is intentionally cheaper than the old path, which performed full
            # dataset snapshots/audits on every 3-second replay tick before returning.
            if float(replay.get('percent') or 0.0) >= NEAR_END_PCT:
                _persist(c, {
                    'status': 'WAITING_FOR_REPLAY_COMPLETION',
                    'replay_percent': float(replay.get('percent') or 0.0),
                    'pending_eligible_decisions': replay.get('pending_eligible_decisions'),
                    'reason': 'final certification is deferred until strict replay is fully committed',
                })
            return []

        now = int(time.time())
        raw = c.get_state(STATE_KEY, {})
        state = dict(raw) if isinstance(raw, dict) else {}
        detected = int(state.get('replay_complete_detected_at') or 0)
        if detected <= 0:
            detected = now
            state = _persist(c, {
                'status': 'REPLAY_COMPLETE_COOLDOWN',
                'replay_complete_detected_at': detected,
                'ready_after': detected + COMPLETION_COOLDOWN_SECONDS,
                'replay_percent': 100.0,
                'reason': 'replay completed; release candle/feature heap and checkpoint WAL before Signal certification',
                'last_transition_memory_release': _release_memory(c, checkpoint=True),
            })

        ready_after = int(state.get('ready_after') or (detected + COMPLETION_COOLDOWN_SECONDS))
        if now < ready_after:
            lr = c.state.setdefault('learning', {})
            lr['phase'] = 'REPLAY_COMPLETE_COOLDOWN'
            lr['blocker'] = f'final certification starts after memory cooldown ({max(0, ready_after - now)}s remaining)'
            _persist(c, {
                'status': 'REPLAY_COMPLETE_COOLDOWN',
                'ready_after': ready_after,
                'cooldown_remaining_seconds': max(0, ready_after - now),
            })
            return []

        with _HEAVY_LOCK:
            # Re-check after waiting on a concurrent manual/worker attempt.
            current = c.get_state(STATE_KEY, {})
            current = dict(current) if isinstance(current, dict) else {}
            running_since = int(current.get('certification_started_at') or 0)
            finished_at = int(current.get('certification_finished_at') or 0)
            if current.get('status') == 'CERTIFICATION_RUNNING' and running_since > finished_at and now - running_since < INTERRUPTED_BACKOFF_SECONDS:
                return []

            attempt = int(current.get('certification_attempts') or 0) + 1
            _persist(c, {
                'status': 'CERTIFICATION_RUNNING',
                'certification_attempts': attempt,
                'certification_started_at': int(time.time()),
                'certification_finished_at': 0,
                'force_requested': bool(force),
                'reason': 'Signal certification running only after replay memory has been released',
                'pre_certification_memory_release': _release_memory(c, checkpoint=True),
            })
            lr = c.state.setdefault('learning', {})
            lr['phase'] = 'SIGNAL_CERTIFICATION_RUNNING_STABLE'
            lr['blocker'] = None
            try:
                result = base_certify(c, force)
                _persist(c, {
                    'status': 'CERTIFICATION_FINISHED',
                    'certification_finished_at': int(time.time()),
                    'last_certification_result_count': len(result or []),
                    'reason': 'certification call completed without overlapping the final replay batch',
                })
                return result
            except MemoryError as exc:
                # A Python-level allocation failure should degrade the learning phase,
                # not kill/restart the whole HTTP service. OS-level OOM is handled by
                # the persistent RUNNING marker and restart backoff above.
                _persist(c, {
                    'status': 'CERTIFICATION_MEMORY_ERROR',
                    'certification_finished_at': int(time.time()),
                    'ready_after': int(time.time()) + INTERRUPTED_BACKOFF_SECONDS,
                    'error': f'{type(exc).__name__}: {exc}',
                    'reason': 'certification allocation failed; backing off without deleting replay data',
                })
                c.state.setdefault('learning', {})['error'] = f'{type(exc).__name__}: {exc}'
                return []
            except Exception as exc:
                _persist(c, {
                    'status': 'CERTIFICATION_ERROR',
                    'certification_finished_at': int(time.time()),
                    'ready_after': int(time.time()) + COMPLETION_COOLDOWN_SECONDS,
                    'error': f'{type(exc).__name__}: {exc}',
                })
                raise
            finally:
                mem = _release_memory(c, checkpoint=True)
                latest = c.get_state(STATE_KEY, {})
                latest = dict(latest) if isinstance(latest, dict) else {}
                latest['post_certification_memory_release'] = mem
                latest['runtime'] = VERSION
                latest['updated_at'] = int(time.time())
                c.set_state(STATE_KEY, latest)
                c.state['replay_transition_stability'] = latest

    # Every automatic/manual training route is pointed at the same transition guard.
    # base_certify already includes the existing source-provenance preflight wrapper.
    final_system.certify_and_execute = stable_certify
    operational_guard.certify_and_execute = stable_certify
    cert17.train_v17 = lambda c, force=False: stable_certify(c, force)
    v5_runtime.train_v5 = lambda c, force=False: stable_certify(c, force)
    core.train_if_due = lambda force=False: stable_certify(core, force)

    # The historical learner frees one lineage's decoded rows/matrices before the next
    # lineage starts. This changes memory lifetime only, never samples, labels, folds,
    # thresholds, genomes, holdouts, or no-lookahead semantics.
    original_lineage = signal_evolution.HistoricalEvolutionLearner.train_strategy_direction

    def stable_lineage(self: Any, *args: Any, **kwargs: Any):
        _trim_heap()
        try:
            return original_lineage(self, *args, **kwargs)
        finally:
            _trim_heap()

    signal_evolution.HistoricalEvolutionLearner.train_strategy_direction = stable_lineage

    # Execution audit may run immediately after Signal certification inside the legacy
    # authority. Trim sklearn/numpy allocations at that boundary and serialize any
    # manual execution request against certification.
    original_optimize_all = execution_v7.optimize_all

    def stable_optimize_all(*args: Any, **kwargs: Any):
        with _HEAVY_LOCK:
            _trim_heap()
            try:
                return original_optimize_all(*args, **kwargs)
            finally:
                _trim_heap()

    execution_v7.optimize_all = stable_optimize_all

    strict = core.state.setdefault('strict_replay', {})
    strict['transition_stability'] = {
        'runtime': VERSION,
        'near_end_percent': NEAR_END_PCT,
        'final_percent': FINAL_PCT,
        'near_end_batch': NEAR_END_BATCH,
        'final_batch': FINAL_BATCH,
        'replay_completion_cooldown_seconds': COMPLETION_COOLDOWN_SECONDS,
        'interrupted_certification_backoff_seconds': INTERRUPTED_BACKOFF_SECONDS,
        'audit_cache_seconds': operational_guard.VIEW_TTL_SECONDS,
        'same_tick_replay_to_certification': False,
        'heap_trim_between_replay_and_certification': True,
        'sqlite_passive_checkpoint_between_heavy_phases': True,
        'historical_samples_deleted': False,
        'replay_cursor_reset': False,
        'raw_market_deleted': False,
        'no_lookahead_semantics_changed': False,
        'strategy_search_semantics_changed': False,
    }
    runtime_identity.stamp(core)
    _persist(core, {
        'status': (core.get_state(STATE_KEY, {}) or {}).get('status') or 'INSTALLED',
        'installed_at': int(time.time()),
        'raw_market_preserved': True,
        'raw_derivatives_preserved': True,
        'learning_samples_preserved': True,
        'replay_cursor_preserved': True,
    })

    if not any(getattr(route, 'path', None) == '/api/v26/stability' for route in core.app.router.routes):
        @core.app.get('/api/v26/stability')
        def stability_status() -> dict[str, Any]:
            return {
                'runtime': VERSION,
                'state': core.get_state(STATE_KEY, {}) or {},
                'rules': strict.get('transition_stability', {}),
                'replay': _known_replay(core, refresh_near_finish=False),
            }
