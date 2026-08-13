from __future__ import annotations

import asyncio
import ctypes
import gc
import os
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor
from pathlib import Path
from typing import Any

import execution_v7
import runtime_identity
import v5_runtime
import v16_runtime_integrity as runtime_integrity
import v17_certification_orchestrator as cert17
import v18_final_system as final_system
import v18_operational_guard as operational_guard
import v20_historical_signal_evolution as signal_evolution

try:
    import fcntl
except Exception:  # pragma: no cover - production is Linux
    fcntl = None  # type: ignore[assignment]

try:
    from threadpoolctl import threadpool_limits
except Exception:  # pragma: no cover
    threadpool_limits = None  # type: ignore[assignment]


VERSION = runtime_identity.RUNTIME_VERSION
STATE_KEY = 'v26_replay_transition_stability'
NEAR_END_PCT = max(90.0, min(99.9, float(os.getenv('REPLAY_STABILITY_NEAR_END_PCT', '95.0'))))
FINAL_PCT = max(NEAR_END_PCT, min(99.99, float(os.getenv('REPLAY_STABILITY_FINAL_PCT', '99.5'))))
NEAR_END_BATCH = max(80, min(500, int(os.getenv('REPLAY_STABILITY_NEAR_END_BATCH', '240'))))
FINAL_BATCH = max(20, min(200, int(os.getenv('REPLAY_STABILITY_FINAL_BATCH', '64'))))
COMPLETION_COOLDOWN_SECONDS = max(60, min(900, int(os.getenv('REPLAY_CERT_COOLDOWN_SECONDS', '180'))))
INTERRUPTED_BACKOFF_SECONDS = max(180, min(1800, int(os.getenv('REPLAY_CERT_CRASH_BACKOFF_SECONDS', '600'))))
AUDIT_CACHE_SECONDS = max(30, min(300, int(os.getenv('REPLAY_STABILITY_AUDIT_CACHE_SECONDS', '90'))))
REPLAY_VIEW_CACHE_SECONDS = max(1, min(10, int(os.getenv('REPLAY_VIEW_CACHE_SECONDS', '3'))))
MEMORY_SOFT_RATIO = max(.50, min(.84, float(os.getenv('CERT_MEMORY_SOFT_RATIO', '.68'))))
MEMORY_HARD_RATIO = max(MEMORY_SOFT_RATIO + .05, min(.94, float(os.getenv('CERT_MEMORY_HARD_RATIO', '.82'))))
MEMORY_BACKOFF_SECONDS = max(60, min(1200, int(os.getenv('CERT_MEMORY_BACKOFF_SECONDS', '180'))))
CANDIDATE_TRIM_EVERY = max(2, min(32, int(os.getenv('CERT_CANDIDATE_TRIM_EVERY', '8'))))
FOLLOWER_RETRY_SECONDS = max(2, min(30, int(os.getenv('RUNTIME_FOLLOWER_RETRY_SECONDS', '5'))))

_HEAVY_LOCK = threading.RLock()
_INSTALL_LOCK = threading.Lock()
_ROLE_LOCK = threading.Lock()
_CACHE_LOCK = threading.RLock()
_CERT_REQUEST_LOCK = threading.Lock()
_EXECUTOR = ThreadPoolExecutor(max_workers=1, thread_name_prefix='eth-signal-certification')
_INSTALLED = False
_LEADER_FH: Any | None = None
_CERT_FUTURE: Future[Any] | None = None
_REPLAY_CACHE: tuple[float, dict[str, Any]] | None = None
_AUDIT_CACHE: tuple[float, dict[str, Any]] | None = None
_CANDIDATE_COUNTER = 0
_LEADER_TOKEN = f'{os.getpid()}-{int(time.time())}'


def _persist(core: Any, patch: dict[str, Any]) -> dict[str, Any]:
    raw = core.get_state(STATE_KEY, {})
    state = dict(raw) if isinstance(raw, dict) else {}
    state.update(patch)
    state['runtime'] = VERSION
    state['updated_at'] = int(time.time())
    core.set_state(STATE_KEY, state)
    core.state['replay_transition_stability'] = state
    return state


def _memory_status() -> dict[str, Any]:
    current = limit = None
    try:
        cur = Path('/sys/fs/cgroup/memory.current')
        lim = Path('/sys/fs/cgroup/memory.max')
        if cur.exists():
            current = int(cur.read_text().strip())
        if lim.exists():
            raw = lim.read_text().strip()
            if raw != 'max':
                limit = int(raw)
    except Exception:
        pass
    rss = None
    try:
        pages = int(Path('/proc/self/statm').read_text().split()[1])
        rss = pages * int(os.sysconf('SC_PAGE_SIZE'))
    except Exception:
        pass
    ratio = current / max(limit, 1) if current is not None and limit else None
    return {
        'cgroup_current_bytes': current,
        'cgroup_limit_bytes': limit,
        'rss_bytes': rss,
        'ratio': ratio,
        'soft_ratio': MEMORY_SOFT_RATIO,
        'hard_ratio': MEMORY_HARD_RATIO,
    }


def _trim_heap() -> dict[str, Any]:
    collected = 0
    malloc_trimmed = False
    try:
        collected = int(gc.collect())
    except Exception:
        pass
    try:
        libc = ctypes.CDLL(None)
        malloc_trim = getattr(libc, 'malloc_trim', None)
        if malloc_trim is not None:
            malloc_trim.argtypes = [ctypes.c_size_t]
            malloc_trim.restype = ctypes.c_int
            malloc_trimmed = bool(malloc_trim(0))
    except Exception:
        pass
    return {'gc_collected': collected, 'malloc_trimmed': malloc_trimmed, 'memory': _memory_status()}


def _checkpoint(core: Any) -> dict[str, Any]:
    con = None
    try:
        con = core.db()
        row = con.execute('PRAGMA wal_checkpoint(PASSIVE)').fetchone()
        try:
            con.execute('PRAGMA optimize')
        except Exception:
            pass
        return {
            'ok': True,
            'busy': int(row[0]) if row else None,
            'wal_pages': int(row[1]) if row else None,
            'checkpointed_pages': int(row[2]) if row else None,
        }
    except Exception as exc:
        return {'ok': False, 'error': f'{type(exc).__name__}: {exc}'}
    finally:
        if con is not None:
            try:
                con.close()
            except Exception:
                pass


def _release_memory(core: Any | None = None, *, checkpoint: bool = False) -> dict[str, Any]:
    result: dict[str, Any] = {}
    if checkpoint and core is not None:
        result['sqlite_checkpoint'] = _checkpoint(core)
    result.update(_trim_heap())
    return result


def _leader_path(core: Any) -> Path:
    path = Path(str(getattr(core, 'DB_PATH', os.getenv('DATABASE_PATH', '/data/eth_adaptive.db'))))
    path.parent.mkdir(parents=True, exist_ok=True)
    return Path(str(path) + '.runtime-leader.lock')


def _try_become_leader(core: Any) -> bool:
    global _LEADER_FH
    if _LEADER_FH is not None:
        return True
    if fcntl is None:
        core.state['runtime_role'] = {'role': 'LEADER_NO_FCNTL', 'token': _LEADER_TOKEN}
        return True
    with _ROLE_LOCK:
        if _LEADER_FH is not None:
            return True
        path = _leader_path(core)
        fh = open(path, 'a+', encoding='utf-8')
        try:
            fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            fh.close()
            core.state['runtime_role'] = {'role': 'FOLLOWER_READ_ONLY', 'token': _LEADER_TOKEN, 'lock': str(path)}
            return False
        except Exception:
            fh.close()
            return False
        _LEADER_FH = fh
        try:
            fh.seek(0)
            fh.truncate(0)
            fh.write(f'{_LEADER_TOKEN}\n')
            fh.flush()
        except Exception:
            pass
        role = {'role': 'LEADER', 'token': _LEADER_TOKEN, 'pid': os.getpid(), 'lock': str(path), 'acquired_at': int(time.time())}
        core.state['runtime_role'] = role
        _persist(core, role)
        return True


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
    _persist(core, {
        'status': 'RECOVERED_AFTER_INTERRUPTED_CERTIFICATION',
        'interrupted_certification_attempts': int(state.get('interrupted_certification_attempts') or 0) + 1,
        'previous_certification_started_at': started,
        'ready_after': now + INTERRUPTED_BACKOFF_SECONDS,
        'reason': 'previous process ended during certification; back off before retry without deleting replay data',
        'raw_market_preserved': True,
        'raw_derivatives_preserved': True,
        'learning_samples_preserved': True,
        'replay_cursor_preserved': True,
    })


def install(core: Any) -> None:
    global _INSTALLED, _CERT_FUTURE, _REPLAY_CACHE, _AUDIT_CACHE, _CANDIDATE_COUNTER
    with _INSTALL_LOCK:
        if _INSTALLED:
            return
        _INSTALLED = True

    _recover_interrupted_attempt(core)
    _try_become_leader(core)

    # The existing final view cache is retained and lengthened so dashboard polling
    # cannot repeatedly scan the whole derived dataset while training is active.
    operational_guard.VIEW_TTL_SECONDS = max(int(operational_guard.VIEW_TTL_SECONDS), AUDIT_CACHE_SECONDS)

    # /api/latest/progress-detail and /api/v17/certification are polled frequently.
    # Cache only read-only status calls; repair/certification paths still force fresh audits.
    original_replay_progress = runtime_integrity.replay_progress
    def cached_replay_progress(c: Any) -> dict[str, Any]:
        global _REPLAY_CACHE
        now = time.monotonic()
        with _CACHE_LOCK:
            if _REPLAY_CACHE and now - _REPLAY_CACHE[0] < REPLAY_VIEW_CACHE_SECONDS:
                return dict(_REPLAY_CACHE[1])
        result = dict(original_replay_progress(c) or {})
        with _CACHE_LOCK:
            _REPLAY_CACHE = (now, result)
        return dict(result)
    runtime_integrity.replay_progress = cached_replay_progress

    original_audit = cert17.audit_derived_dataset
    def cached_audit(c: Any, allow_auto_rebuild: bool = True):
        global _AUDIT_CACHE
        if allow_auto_rebuild:
            return original_audit(c, allow_auto_rebuild=True)
        now = time.monotonic()
        with _CACHE_LOCK:
            if _AUDIT_CACHE and now - _AUDIT_CACHE[0] < AUDIT_CACHE_SECONDS:
                return dict(_AUDIT_CACHE[1])
        result = dict(original_audit(c, allow_auto_rebuild=False) or {})
        with _CACHE_LOCK:
            _AUDIT_CACHE = (now, result)
        return dict(result)
    cert17.audit_derived_dataset = cached_audit

    # During a Zeabur rolling replacement multiple containers can briefly coexist.
    # Only the container holding the volume-level flock may run mutating background
    # loops; followers remain read-only and automatically take over after leader exit.
    original_scan_worker = core.scan_worker
    original_learning_worker = core.learning_worker
    original_scan = core.scan
    original_learning_tick = core.learning_tick

    async def leader_scan() -> Any:
        if not _try_become_leader(core):
            return core.state
        return await original_scan()

    async def leader_learning_tick() -> None:
        if not _try_become_leader(core):
            core.state.setdefault('learning', {})['runtime_role'] = 'FOLLOWER_READ_ONLY'
            return
        await original_learning_tick()

    async def leader_scan_worker() -> None:
        while not _try_become_leader(core):
            await asyncio.sleep(FOLLOWER_RETRY_SECONDS)
        await original_scan_worker()

    async def leader_learning_worker() -> None:
        while not _try_become_leader(core):
            await asyncio.sleep(FOLLOWER_RETRY_SECONDS)
        await original_learning_worker()

    core.scan = leader_scan
    core.learning_tick = leader_learning_tick
    core.scan_worker = leader_scan_worker
    core.learning_worker = leader_learning_worker

    original_generate = v5_runtime.generate_learning_samples_v5
    def stable_generate(c: Any, batch: int | None = None) -> int:
        if not _try_become_leader(c):
            return 0
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
            if pct >= NEAR_END_PCT:
                _persist(c, {'last_post_replay_memory_release': _release_memory(c, checkpoint=True)})

    v5_runtime.generate_learning_samples_v5 = stable_generate
    core.generate_learning_samples = lambda batch=None: stable_generate(core, batch)

    # Capture the fully composed fixed-horizon + source-preflight authority. The public
    # entry points below only enqueue this blocking function in one dedicated thread.
    base_certify = final_system.certify_and_execute

    def blocking_certify(c: Any, force: bool = False):
        if not _try_become_leader(c):
            return []
        replay = _known_replay(c, refresh_near_finish=True)
        if not replay.get('complete'):
            if float(replay.get('percent') or 0.0) >= NEAR_END_PCT:
                _persist(c, {
                    'status': 'WAITING_FOR_REPLAY_COMPLETION',
                    'replay_percent': float(replay.get('percent') or 0.0),
                    'pending_eligible_decisions': replay.get('pending_eligible_decisions'),
                    'reason': 'certification deferred until fixed replay is fully committed',
                })
            return []

        now = int(time.time())
        state = c.get_state(STATE_KEY, {})
        state = dict(state) if isinstance(state, dict) else {}
        detected = int(state.get('replay_complete_detected_at') or 0)
        if detected <= 0:
            detected = now
            state = _persist(c, {
                'status': 'REPLAY_COMPLETE_COOLDOWN',
                'replay_complete_detected_at': detected,
                'ready_after': detected + COMPLETION_COOLDOWN_SECONDS,
                'replay_percent': 100.0,
                'reason': 'replay complete; release historical heap and checkpoint WAL before certification',
                'last_transition_memory_release': _release_memory(c, checkpoint=True),
            })
        ready_after = int(state.get('ready_after') or detected + COMPLETION_COOLDOWN_SECONDS)
        if now < ready_after:
            c.state.setdefault('learning', {})['phase'] = 'REPLAY_COMPLETE_COOLDOWN'
            c.state['learning']['blocker'] = f'certification memory cooldown {max(0, ready_after - now)}s'
            _persist(c, {'status': 'REPLAY_COMPLETE_COOLDOWN', 'ready_after': ready_after,
                         'cooldown_remaining_seconds': max(0, ready_after - now)})
            return []

        mem = _memory_status()
        if mem.get('ratio') is not None and float(mem['ratio']) >= MEMORY_SOFT_RATIO:
            _release_memory(c, checkpoint=True)
            mem = _memory_status()
        if mem.get('ratio') is not None and float(mem['ratio']) >= MEMORY_HARD_RATIO:
            _persist(c, {
                'status': 'CERTIFICATION_DEFERRED_MEMORY_PRESSURE',
                'ready_after': int(time.time()) + MEMORY_BACKOFF_SECONDS,
                'memory': mem,
                'reason': 'memory stayed above hard watermark after cleanup; defer instead of OOM/restart',
            })
            return []

        with _HEAVY_LOCK:
            current = c.get_state(STATE_KEY, {})
            current = dict(current) if isinstance(current, dict) else {}
            running_since = int(current.get('certification_started_at') or 0)
            finished_at = int(current.get('certification_finished_at') or 0)
            if current.get('status') == 'CERTIFICATION_RUNNING' and running_since > finished_at and now - running_since < INTERRUPTED_BACKOFF_SECONDS:
                return []
            _persist(c, {
                'status': 'CERTIFICATION_RUNNING',
                'certification_attempts': int(current.get('certification_attempts') or 0) + 1,
                'certification_started_at': int(time.time()),
                'certification_finished_at': 0,
                'force_requested': bool(force),
                'memory_before_certification': _memory_status(),
                'pre_certification_memory_release': _release_memory(c, checkpoint=True),
                'reason': 'Signal certification runs off the web event loop after replay memory release',
            })
            c.state.setdefault('learning', {})['phase'] = 'SIGNAL_CERTIFICATION_RUNNING_STABLE'
            c.state['learning']['blocker'] = None
            try:
                if threadpool_limits is not None:
                    with threadpool_limits(limits=1):
                        result = base_certify(c, force)
                else:
                    result = base_certify(c, force)
                _persist(c, {
                    'status': 'CERTIFICATION_FINISHED',
                    'certification_finished_at': int(time.time()),
                    'last_certification_result_count': len(result or []),
                })
                return result
            except MemoryError as exc:
                _persist(c, {
                    'status': 'CERTIFICATION_MEMORY_ERROR',
                    'certification_finished_at': int(time.time()),
                    'ready_after': int(time.time()) + INTERRUPTED_BACKOFF_SECONDS,
                    'error': f'{type(exc).__name__}: {exc}',
                    'reason': 'allocation failed; back off without deleting raw/replay data',
                })
                c.state.setdefault('learning', {})['error'] = f'{type(exc).__name__}: {exc}'
                return []
            finally:
                _persist(c, {'post_certification_memory_release': _release_memory(c, checkpoint=True)})

    def request_certification(c: Any, force: bool = False):
        global _CERT_FUTURE
        if not _try_become_leader(c):
            return []
        replay = _known_replay(c, refresh_near_finish=True)
        if not replay.get('complete'):
            return []
        state = c.get_state(STATE_KEY, {})
        state = dict(state) if isinstance(state, dict) else {}
        if int(state.get('ready_after') or 0) > int(time.time()) and state.get('status') not in ('REPLAY_COMPLETE_COOLDOWN',):
            return []
        with _CERT_REQUEST_LOCK:
            if _CERT_FUTURE is not None and not _CERT_FUTURE.done():
                return []
            _CERT_FUTURE = _EXECUTOR.submit(blocking_certify, c, bool(force))
            _persist(c, {'status': 'CERTIFICATION_QUEUED_BACKGROUND', 'certification_queued_at': int(time.time())})
        return []

    # All automatic/manual paths enqueue the same authority. This prevents sklearn
    # training from blocking FastAPI and causing 502 -> health restart -> many replicas.
    final_system.certify_and_execute = request_certification
    operational_guard.certify_and_execute = request_certification
    cert17.train_v17 = request_certification
    v5_runtime.train_v5 = request_certification
    core.train_if_due = lambda force=False: request_certification(core, force)

    # Release candidate-local matrices/models during a long lineage, not only after the
    # entire lineage. Candidate population, feature set, folds and thresholds stay intact.
    original_dev_score = signal_evolution._development_score
    def stable_dev_score(*args: Any, **kwargs: Any):
        global _CANDIDATE_COUNTER
        try:
            return original_dev_score(*args, **kwargs)
        finally:
            _CANDIDATE_COUNTER += 1
            ratio = float(_memory_status().get('ratio') or 0.0)
            if _CANDIDATE_COUNTER % CANDIDATE_TRIM_EVERY == 0 or ratio >= MEMORY_SOFT_RATIO:
                core.state['certification_resource_progress'] = {
                    'candidates_completed_in_process': _CANDIDATE_COUNTER,
                    'last_heap_release': _trim_heap(),
                    'updated_at': int(time.time()),
                }
    signal_evolution._development_score = stable_dev_score

    original_lineage = signal_evolution.HistoricalEvolutionLearner.train_strategy_direction
    def stable_lineage(self: Any, *args: Any, **kwargs: Any):
        _trim_heap()
        try:
            return original_lineage(self, *args, **kwargs)
        finally:
            _trim_heap()
    signal_evolution.HistoricalEvolutionLearner.train_strategy_direction = stable_lineage

    # Execution audit is serialized with Signal certification and receives the same
    # heap cleanup. Search space and untouched-audit rules are not reduced.
    original_optimize_all = execution_v7.optimize_all
    def stable_optimize_all(*args: Any, **kwargs: Any):
        with _HEAVY_LOCK:
            _release_memory(core, checkpoint=True)
            try:
                if threadpool_limits is not None:
                    with threadpool_limits(limits=1):
                        return original_optimize_all(*args, **kwargs)
                return original_optimize_all(*args, **kwargs)
            finally:
                _release_memory(core, checkpoint=True)
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
        'audit_cache_seconds': AUDIT_CACHE_SECONDS,
        'replay_view_cache_seconds': REPLAY_VIEW_CACHE_SECONDS,
        'memory_soft_ratio': MEMORY_SOFT_RATIO,
        'memory_hard_ratio': MEMORY_HARD_RATIO,
        'candidate_heap_trim_every': CANDIDATE_TRIM_EVERY,
        'same_tick_replay_to_certification': False,
        'certification_runs_off_event_loop': True,
        'single_mutating_replica_per_persistent_volume': True,
        'follower_auto_takeover_after_leader_exit': True,
        'native_math_threads_during_certification': 1,
        'sqlite_passive_checkpoint_between_heavy_phases': True,
        'historical_samples_deleted': False,
        'replay_cursor_reset': False,
        'raw_market_deleted': False,
        'features_reduced': False,
        'strategy_population_reduced': False,
        'execution_policy_search_reduced': False,
        'no_lookahead_semantics_changed': False,
        'sealed_holdout_semantics_changed': False,
    }
    runtime_identity.stamp(core)
    _persist(core, {
        'status': (core.get_state(STATE_KEY, {}) or {}).get('status') or 'INSTALLED',
        'installed_at': int(time.time()),
        'raw_market_preserved': True,
        'raw_derivatives_preserved': True,
        'learning_samples_preserved': True,
        'replay_cursor_preserved': True,
        'memory': _memory_status(),
    })

    if not any(getattr(route, 'path', None) == '/api/v26/stability' for route in core.app.router.routes):
        @core.app.get('/api/v26/stability')
        def stability_status() -> dict[str, Any]:
            future = _CERT_FUTURE
            return {
                'runtime': VERSION,
                'state': core.get_state(STATE_KEY, {}) or {},
                'rules': strict.get('transition_stability', {}),
                'replay': _known_replay(core, refresh_near_finish=False),
                'runtime_role': core.state.get('runtime_role') or {},
                'memory': _memory_status(),
                'certification_future': {
                    'present': future is not None,
                    'running': bool(future is not None and not future.done()),
                    'done': bool(future is not None and future.done()),
                },
                'resource_progress': core.state.get('certification_resource_progress') or {},
            }
