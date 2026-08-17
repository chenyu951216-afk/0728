from __future__ import annotations

"""Exact Stage-6 throughput/liveness overlay.

Performance is improved only by running independent historical decision paths of the
same frozen candidate concurrently after V43's scalar simulator has passed its legacy
parity guard.  Features, candidate search space, chronological folds, costs, stops,
targets and OOS gates are unchanged.  Future 5m bars remain outcome-settlement only.

Every completed development candidate (including NO_RESULT) is persisted under an
immutable run fingerprint.  A restart therefore resumes expensive candidate work
instead of recomputing it, while finalist ranking is reconstructed from the complete
persisted candidate archive.
"""

import hashlib
import json
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import numpy as np

import runtime_identity
import v16_runtime_integrity as runtime_integrity
import v43_unified_performance_authority as v43

VERSION = 'V46_STAGE6_THROUGHPUT_LIVENESS'
SCHEMA = 46
STATE_KEY = 'v46_stage6_throughput_liveness'
TABLE = 'autonomous_evolution_candidate_v46'
CPU_COUNT = max(1, int(os.cpu_count() or 1))
MAX_WORKERS = max(1, min(4, int(os.getenv('AUTONOMOUS_V46_MAX_SIM_WORKERS', str(max(1, min(3, CPU_COUNT - 1)))))))
CHUNK = max(8, min(192, int(os.getenv('AUTONOMOUS_V46_SIM_CHUNK', '48'))))
SERIAL_MEMORY_RATIO = max(.60, min(.94, float(os.getenv('AUTONOMOUS_V46_SERIAL_MEMORY_RATIO', '.82'))))
TWO_WORKER_MEMORY_RATIO = max(.50, min(SERIAL_MEMORY_RATIO, float(os.getenv('AUTONOMOUS_V46_TWO_WORKER_MEMORY_RATIO', '.70'))))

_INSTALL_LOCK = threading.Lock()
_STATE_LOCK = threading.Lock()
_EXECUTOR_LOCK = threading.Lock()
_INSTALLED = False
_EXECUTOR: ThreadPoolExecutor | None = None
_RUN_ID: str | None = None
_RESUMED = 0
_COMPUTED = 0
_PATHS = 0


def _jd(v: Any) -> Any:
    if hasattr(v, 'item'):
        return v.item()
    raise TypeError(type(v).__name__)


def _memory() -> dict[str, Any]:
    try:
        return dict(v43._memory() or {})
    except Exception:
        return {'ratio': None}


def _state(core: Any, **patch: Any) -> dict[str, Any]:
    with _STATE_LOCK:
        old = core.state.get(STATE_KEY)
        out = dict(old) if isinstance(old, dict) else {}
        out.update(patch)
        out.update({'schema': SCHEMA, 'runtime': VERSION, 'public_runtime': runtime_identity.RUNTIME_VERSION, 'updated_at': int(time.time())})
        core.state[STATE_KEY] = out
        return out


def _ensure(core: Any) -> None:
    con = core.db()
    try:
        con.execute(f'''CREATE TABLE IF NOT EXISTS {TABLE}(
            run_id TEXT NOT NULL,generation INTEGER NOT NULL,candidate INTEGER NOT NULL,
            candidate_id TEXT NOT NULL,seed INTEGER NOT NULL,status TEXT NOT NULL,
            score REAL,genome TEXT NOT NULL,result TEXT,completed_at INTEGER NOT NULL,
            PRIMARY KEY(run_id,generation,candidate))''')
        con.execute(f'CREATE INDEX IF NOT EXISTS ix_{TABLE}_gid ON {TABLE}(run_id,candidate_id)')
        con.commit()
    finally:
        con.close()


def _sample(arr: Any, n: int = 32) -> bytes:
    try:
        x = np.asarray(arr)
        if x.size == 0:
            return b''
        if x.ndim == 0:
            return x.reshape(1).tobytes()
        idx = np.linspace(0, x.shape[0] - 1, min(n, x.shape[0]), dtype=np.int64)
        return np.ascontiguousarray(x[idx]).tobytes()
    except Exception:
        return b''


def _run_fingerprint(core: Any, a: Any, s: dict[str, Any], m: dict[str, Any]) -> str:
    baseline = core.get_state('final_dataset_baseline_v1', {})
    baseline = dict(baseline) if isinstance(baseline, dict) else {}
    ts = np.asarray(s.get('ts') if s.get('ts') is not None else [], dtype=np.int64)
    ts5 = np.asarray(m.get('ts5') if m.get('ts5') is not None else [], dtype=np.int64)
    payload = {
        'schema': SCHEMA, 'dataset_id': baseline.get('dataset_id'), 'reset': str(getattr(a, 'RESET_MARKER', '')),
        'research': [int(a.RESEARCH_START_TS), int(a.RESEARCH_END_EXCLUSIVE_TS), int(a.SETTLEMENT_END_EXCLUSIVE_TS)],
        'snapshots': [len(ts), int(ts[0]) if len(ts) else None, int(ts[-1]) if len(ts) else None],
        'market5': [len(ts5), int(ts5[0]) if len(ts5) else None, int(ts5[-1]) if len(ts5) else None, str(m.get('source5') or '')],
        'search': [int(a.POPULATION), int(a.GENERATIONS), int(a.ELITES), int(a.FINALISTS), int(a.MAX_CHAMPIONS)],
        'caps': [int(a.TRAIN_SIM_CAP), int(a.CAL_SIM_CAP), int(a.TEST_SIM_CAP), int(a.FINAL_REFIT_CAP)],
        'hold': list(map(int, a.HOLD_BARS_15M)), 'expire': list(map(int, a.EXPIRE_BARS_15M)), 'stride': list(map(int, a.DECISION_STRIDES)),
        'oos': [float(a.FINAL_HOLDOUT_PCT), int(a.MIN_OOS_FILLS), float(a.MIN_OOS_PF), float(a.MIN_OOS_EV_R), float(a.MAX_OOS_DD_R), float(a.MIN_WF_STABILITY), float(a.MIN_PROFITABLE_FOLDS), float(a.MIN_WORST_FOLD_EV), float(a.MIN_BOOTSTRAP_CI05)],
        'cost_bps': float(a.ALL_IN_COST_BPS), 'features': list(a.FEATURE_NAMES),
    }
    h = hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(',', ':')).encode())
    h.update(_sample(ts, 48)); h.update(_sample(s.get('x'), 48)); h.update(_sample(ts5, 48))
    for key in ('o5', 'h5', 'l5', 'c5'):
        h.update(_sample(m.get(key), 24))
    return h.hexdigest()[:28]


def _load(core: Any, run: str, generation: int, candidate: int) -> dict[str, Any] | None:
    con = core.db()
    try:
        row = con.execute(f'SELECT candidate_id,seed,status,score,genome,result FROM {TABLE} WHERE run_id=? AND generation=? AND candidate=?', (run, generation, candidate)).fetchone()
    finally:
        con.close()
    if not row:
        return None
    try:
        return {'candidate_id': str(row[0]), 'seed': int(row[1]), 'status': str(row[2]), 'score': float(row[3]) if row[3] is not None else None, 'genome': json.loads(row[4]), 'result': json.loads(row[5]) if row[5] else None}
    except Exception:
        return None


def _save(core: Any, run: str, generation: int, candidate: int, gid: str, seed: int, genome: dict[str, Any], result: dict[str, Any] | None) -> None:
    score = float(result['score']) if result is not None and result.get('score') is not None else None
    con = core.db()
    try:
        con.execute(f'''INSERT OR REPLACE INTO {TABLE}(run_id,generation,candidate,candidate_id,seed,status,score,genome,result,completed_at)
            VALUES(?,?,?,?,?,?,?,?,?,?)''', (run, generation, candidate, gid, seed, 'SCORED' if result is not None else 'NO_RESULT', score, json.dumps(genome, sort_keys=True, separators=(',', ':'), default=_jd), json.dumps(result, separators=(',', ':'), default=_jd) if result is not None else None, int(time.time())))
        con.commit()
    finally:
        con.close()


def _counts(core: Any, run: str) -> dict[str, int]:
    con = core.db()
    try:
        row = con.execute(f"SELECT COUNT(*),SUM(status='SCORED'),SUM(status='NO_RESULT') FROM {TABLE} WHERE run_id=?", (run,)).fetchone()
    finally:
        con.close()
    row = row or (0, 0, 0)
    return {'persisted': int(row[0] or 0), 'scored': int(row[1] or 0), 'no_result': int(row[2] or 0)}


def _finalists(core: Any, a: Any, run: str) -> list[tuple[float, dict[str, Any], dict[str, Any]]]:
    con = core.db()
    try:
        rows = con.execute(f"SELECT candidate_id,score,genome,result FROM {TABLE} WHERE run_id=? AND status='SCORED' AND result IS NOT NULL ORDER BY generation,candidate", (run,)).fetchall()
    finally:
        con.close()
    archive: dict[str, tuple[float, dict[str, Any], dict[str, Any]]] = {}
    for gid, score, genome_raw, result_raw in rows:
        try:
            item = (float(score), json.loads(genome_raw), json.loads(result_raw))
        except Exception:
            continue
        if str(gid) not in archive or item[0] > archive[str(gid)][0]:
            archive[str(gid)] = item
    ranked = sorted(archive.values(), key=lambda z: z[0], reverse=True)
    out: list[tuple[float, dict[str, Any], dict[str, Any]]] = []
    used: dict[tuple[Any, ...], int] = {}
    for item in ranked:
        key = a._diversity_key(item[1])
        if used.get(key, 0) >= 2:
            continue
        used[key] = used.get(key, 0) + 1; out.append(item)
        if len(out) >= int(a.FINALISTS):
            break
    return out


def _executor() -> ThreadPoolExecutor:
    global _EXECUTOR
    with _EXECUTOR_LOCK:
        if _EXECUTOR is None:
            _EXECUTOR = ThreadPoolExecutor(max_workers=MAX_WORKERS, thread_name_prefix='stage6-sim')
        return _EXECUTOR


def _workers() -> int:
    if MAX_WORKERS <= 1:
        return 1
    ratio = float(_memory().get('ratio') or 0.0)
    if ratio >= SERIAL_MEMORY_RATIO:
        return 1
    if ratio >= TWO_WORKER_MEMORY_RATIO:
        return min(2, MAX_WORKERS)
    return MAX_WORKERS


def _sync_phase(core: Any, a: Any) -> None:
    try:
        replay = dict(runtime_integrity.replay_progress(core) or {})
        status = str((a.autonomous_status(core) or {}).get('status') or '')
    except Exception:
        return
    if not replay.get('complete'):
        return
    phase = 'AUTONOMOUS_DIRECT_R_EVOLUTION_RUNNING' if status in ('AUTONOMOUS_EVOLUTION_RUNNING', 'AUTONOMOUS_OOS_RUNNING', 'CERTIFICATION_RUNNING') else ('WAITING_AUTONOMOUS_MARKET_CACHE_INTEGRITY' if status == 'WAITING_MARKET_CACHE' else ('AUTONOMOUS_RESEARCH_COMPLETE' if status in ('COMPLETE', 'COMPLETE_NO_CERTIFIED_PACKAGE') else 'AUTONOMOUS_RESEARCH_QUEUED'))
    learning = core.state.setdefault('learning', {}); learning['phase'] = phase
    for key in ('formal_stage', 'official_stage', 'certification_stage', 'stage'):
        if str(learning.get(key) or '') == 'STRICT_REPLAY_ADVANCING':
            learning[key] = phase


def _install_parallel(core: Any, a: Any) -> None:
    scalar = a._simulate_trade
    calls = {'n': 0}

    def simulate_indices(indices: np.ndarray, snapshots: dict[str, Any], market: dict[str, Any], genome: dict[str, Any]):
        global _PATHS
        idxs = np.asarray(indices, dtype=np.int64); results: list[dict[str, Any]] = []; xs: list[np.ndarray] = []; ys: list[float] = []
        if not len(idxs):
            return np.empty((0, snapshots['x'].shape[1]), dtype=np.float32), np.empty(0, dtype=np.float32), results
        calls['n'] += 1; started = time.monotonic(); done = 0
        for off in range(0, len(idxs), CHUNK):
            chunk = idxs[off:off + CHUNK]
            workers = _workers() if bool(getattr(v43, '_FAST_VERIFIED', False)) and bool(getattr(v43, '_FAST_ENABLED', True)) else 1
            def one(raw: int):
                i = int(raw); return i, dict(scalar(market, int(snapshots['ts'][i]), snapshots['x'][i], genome))
            pairs = [one(int(i)) for i in chunk.tolist()] if workers <= 1 else list(_executor().map(one, chunk.tolist()))
            for i, res in pairs:
                results.append(res)
                if res.get('valid') and res.get('filled'):
                    xs.append(snapshots['x'][i]); ys.append(float(res['pnl_r']))
            done += len(pairs); _PATHS += len(pairs)
            active = dict(core.state.get('autonomous_live_progress') or {})
            active.update({'heartbeat_at': int(time.time()), 'substage': 'CAUSAL_TRADE_PATH_SIMULATION', 'simulation_call': calls['n'], 'paths_completed_current_call': done, 'paths_total_current_call': int(len(idxs)), 'paths_per_second_current_call': round(done / max(time.monotonic() - started, 1e-9), 3), 'simulation_workers': workers, 'future_prices_as_features': False, 'future_5m_role': 'OUTCOME_SETTLEMENT_AFTER_PLAN_FREEZE_ONLY'})
            core.state['autonomous_live_progress'] = active; _sync_phase(core, a)
            _state(core, status='RUNNING', run_id=_RUN_ID, heartbeat=active, paths_completed=_PATHS, memory=_memory(), resumed_candidates=_RESUMED, computed_candidates=_COMPUTED)
        if not xs:
            return np.empty((0, snapshots['x'].shape[1]), dtype=np.float32), np.empty(0, dtype=np.float32), results
        return np.asarray(xs, dtype=np.float32), np.asarray(ys, dtype=np.float32), results

    a._simulate_indices = simulate_indices


def _install_resume(core: Any, a: Any) -> None:
    global _RUN_ID
    base_eval = a._evaluate_candidate; base_evolution = a._evolution

    def eval_resume(snapshots: dict[str, Any], market: dict[str, Any], genome: dict[str, Any], seed: int):
        global _RESUMED, _COMPUTED
        active = dict(core.state.get('autonomous_live_progress') or {})
        generation = int(active.get('generation') or 0); candidate = int(active.get('candidate') or 0); run = _RUN_ID
        if not run or generation <= 0 or candidate <= 0:
            return base_eval(snapshots, market, genome, seed)
        gid = a._hash_payload(genome, 18); saved = _load(core, run, generation, candidate)
        if saved and saved['candidate_id'] == gid and saved['seed'] == int(seed):
            _RESUMED += 1; active.update({'candidate_checkpoint': 'RESUMED_EXACT', 'heartbeat_at': int(time.time())}); core.state['autonomous_live_progress'] = active
            _state(core, status='RUNNING', run_id=run, resumed_candidates=_RESUMED, computed_candidates=_COMPUTED, checkpoint_counts=_counts(core, run))
            return saved['result'] if saved['status'] == 'SCORED' else None
        result = base_eval(snapshots, market, genome, seed); _save(core, run, generation, candidate, gid, int(seed), genome, result); _COMPUTED += 1
        _state(core, status='RUNNING', run_id=run, resumed_candidates=_RESUMED, computed_candidates=_COMPUTED, checkpoint_counts=_counts(core, run), memory=_memory())
        return result

    def evolution_resume(c: Any, snapshots: dict[str, Any], market: dict[str, Any]):
        global _RUN_ID
        run = _run_fingerprint(c, a, snapshots, market); _RUN_ID = run; counts = _counts(c, run)
        checkpoint = c.get_state(a.CHECKPOINT_KEY, {})
        if isinstance(checkpoint, dict) and checkpoint.get('status') == 'RUNNING' and not checkpoint.get('v46_run_id') and counts['persisted'] == 0:
            c.set_state(a.CHECKPOINT_KEY, {})
            _state(c, legacy_generation_checkpoint_discarded=True, legacy_generation_checkpoint_reason='pre-V46 checkpoint had no complete candidate archive; only Stage-6 evolution restarts, historical replay remains untouched')
        _state(c, status='RUNNING', run_id=run, checkpoint_counts=counts, exact_candidate_resume=True, full_archive_reconstruction=True)
        base = base_evolution(c, snapshots, market)
        rebuilt = _finalists(c, a, run); out = rebuilt if rebuilt else base
        checkpoint = c.get_state(a.CHECKPOINT_KEY, {})
        if isinstance(checkpoint, dict):
            checkpoint = dict(checkpoint); checkpoint['v46_run_id'] = run; checkpoint['v46_candidate_rows'] = _counts(c, run)['persisted']; c.set_state(a.CHECKPOINT_KEY, checkpoint)
        _state(c, status='DEVELOPMENT_EVOLUTION_COMPLETE', run_id=run, checkpoint_counts=_counts(c, run), finalists_reconstructed=len(out)); _sync_phase(c, a)
        return out

    a._evaluate_candidate = eval_resume; a._evolution = evolution_resume


def _normalize(v: Any, phase: str) -> Any:
    if isinstance(v, dict): return {k: _normalize(x, phase) for k, x in v.items()}
    if isinstance(v, list): return [_normalize(x, phase) for x in v]
    if v == 'STRICT_REPLAY_ADVANCING': return phase
    if v == 'full price history contract is complete; point-in-time replay is advancing': return 'fixed historical replay is complete; autonomous strategy research is active'
    return v


def _patch_routes(core: Any, a: Any) -> None:
    app = getattr(core, 'app', None)
    if app is None: return
    for path in ('/api/latest/progress-detail', '/api/latest/pipeline'):
        route = next((r for r in list(app.router.routes) if getattr(r, 'path', None) == path), None); old = getattr(route, 'endpoint', None)
        if not callable(old): continue
        app.router.routes = [r for r in app.router.routes if getattr(r, 'path', None) != path]
        def make(original: Any):
            def wrapped():
                raw = original(); payload = dict(raw) if isinstance(raw, dict) else {}; _sync_phase(core, a); replay = runtime_integrity.replay_progress(core); phase = str((core.state.get('learning') or {}).get('phase') or 'AUTONOMOUS_RESEARCH_QUEUED')
                if replay.get('complete'): payload = _normalize(payload, phase)
                payload['stage6_throughput'] = dict(core.state.get(STATE_KEY) or {}); payload['historical_replay_complete_is_terminal'] = bool(replay.get('complete')); payload['learning_phase'] = phase
                return payload
            return wrapped
        app.add_api_route(path, make(old), methods=['GET'])
    if not any(getattr(r, 'path', None) == '/api/v46/stage6-throughput' for r in app.router.routes):
        @app.get('/api/v46/stage6-throughput')
        def status():
            return {'schema': SCHEMA, 'runtime': VERSION, 'state': dict(core.state.get(STATE_KEY) or {}), 'run_id': _RUN_ID, 'checkpoint_counts': _counts(core, _RUN_ID) if _RUN_ID else {}, 'memory': _memory(), 'v43_scalar': {'enabled': bool(getattr(v43, '_FAST_ENABLED', False)), 'verified': bool(getattr(v43, '_FAST_VERIFIED', False)), 'parity_done': int(getattr(v43, '_FAST_PARITY_DONE', 0)), 'mismatches': int(getattr(v43, '_FAST_PARITY_MISMATCHES', 0))}, 'rules': {'history_reduced': False, 'features_reduced': False, 'population_reduced': False, 'generations_reduced': False, 'holding_horizons_reduced': False, 'oos_relaxed': False, 'fitness_changed': False, 'future_features_enabled': False, 'future_5m_outcome_only_after_plan_freeze': True, 'slow_candidate_timeout': False, 'cross_candidate_result_cache': False, 'resume_requires_exact_run_candidate_and_seed': True}}


def install(production: Any, autonomous: Any) -> None:
    global _INSTALLED
    with _INSTALL_LOCK:
        if _INSTALLED: return
        _INSTALLED = True
    core = production.core; _ensure(core); _install_parallel(core, autonomous); _install_resume(core, autonomous); _patch_routes(core, autonomous); _sync_phase(core, autonomous)
    rules = {'history_reduced': False, 'features_reduced': False, 'population_reduced': False, 'generations_reduced': False, 'holding_horizons_reduced': False, 'oos_rules_changed': False, 'fitness_changed': False, 'cost_model_changed': False, 'stop_target_semantics_changed': False, 'future_peeking_enabled': False, 'parallel_same_frozen_candidate_only': True, 'v43_scalar_parity_required_before_parallelism': True, 'candidate_results_persisted': True, 'no_result_candidates_persisted': True, 'full_archive_reconstructed_after_restart': True, 'memory_pressure_reduces_workers_not_research': True, 'slow_candidate_timeout_rejection': False}
    core.state.setdefault('strict_replay', {})['stage6_throughput_liveness_v46'] = dict(rules)
    _state(core, installed=True, status='READY', rules=rules, max_workers=MAX_WORKERS, simulation_chunk=CHUNK, memory=_memory())
