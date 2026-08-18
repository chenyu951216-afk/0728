from __future__ import annotations

"""Bounded parallel dispatcher for the V56 canonical causal simulator.

V46 intentionally waited for V43 legacy-parity before using multiple workers. V56 is a
new semantic authority and must not claim parity with the now-rejected legacy execution
rules. This dispatcher therefore parallelizes only independent decision paths of the
same frozen V56 candidate, preserves input/result order, and drops to one worker under
memory pressure. It never parallelizes candidates, OOS packages, or model fits.
"""

import gc
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import numpy as np

import runtime_identity
import v43_unified_performance_authority as v43

VERSION = 'V56_BOUNDED_CAUSAL_PARALLEL_AUTHORITY'
SCHEMA = 56
STATE_KEY = 'v56_bounded_causal_parallel_authority'
MAX_WORKERS = max(1, min(2, int(os.getenv('AUTONOMOUS_V56_MAX_SIM_WORKERS', '2'))))
CHUNK = max(8, min(64, int(os.getenv('AUTONOMOUS_V56_SIM_CHUNK', '32'))))
SERIAL_MEMORY_RATIO = max(.55, min(.88, float(os.getenv('AUTONOMOUS_V56_SERIAL_MEMORY_RATIO', '.72'))))
GC_MEMORY_RATIO = max(.50, min(SERIAL_MEMORY_RATIO, float(os.getenv('AUTONOMOUS_V56_GC_MEMORY_RATIO', '.66'))))

_LOCK = threading.Lock()
_EXECUTOR_LOCK = threading.Lock()
_EXECUTOR: ThreadPoolExecutor | None = None
_INSTALLED = False
_PATHS = 0


def _memory() -> dict[str, Any]:
    try:
        return dict(v43._memory() or {})
    except Exception:
        return {'ratio': None}


def _workers() -> int:
    ratio = float(_memory().get('ratio') or 0.0)
    if ratio >= SERIAL_MEMORY_RATIO:
        return 1
    return MAX_WORKERS


def _executor() -> ThreadPoolExecutor:
    global _EXECUTOR
    with _EXECUTOR_LOCK:
        if _EXECUTOR is None:
            _EXECUTOR = ThreadPoolExecutor(max_workers=MAX_WORKERS, thread_name_prefix='v56-path')
        return _EXECUTOR


def _state(core: Any, **patch: Any) -> dict[str, Any]:
    old = core.state.get(STATE_KEY)
    out = dict(old) if isinstance(old, dict) else {}
    out.update(patch)
    out.update({'schema': SCHEMA, 'runtime': VERSION,
                'public_runtime': runtime_identity.RUNTIME_VERSION, 'updated_at': int(time.time())})
    core.state[STATE_KEY] = out
    return out


def install(production: Any, autonomous: Any, integrity: Any) -> None:
    global _INSTALLED
    with _LOCK:
        if _INSTALLED:
            return
        _INSTALLED = True
    core = production.core
    mods = tuple(getattr(integrity, 'SEMANTIC_MODULES', ()))
    if 'v56_parallel_authority' not in mods:
        integrity.SEMANTIC_MODULES = mods + ('v56_parallel_authority',)

    scalar = autonomous._simulate_trade

    def simulate_indices(indices: np.ndarray, snapshots: dict[str, Any], market: dict[str, Any], genome: dict[str, Any]):
        global _PATHS
        idxs = np.asarray(indices, dtype=np.int64)
        results: list[dict[str, Any]] = []
        xs: list[np.ndarray] = []
        ys: list[float] = []
        if not len(idxs):
            return (np.empty((0, snapshots['x'].shape[1]), dtype=np.float32),
                    np.empty(0, dtype=np.float32), results)
        started = time.monotonic(); done = 0
        for off in range(0, len(idxs), CHUNK):
            chunk = idxs[off:off + CHUNK]; workers = _workers()

            def one(raw: int):
                i = int(raw)
                return i, dict(scalar(market, int(snapshots['ts'][i]), snapshots['x'][i], genome))

            if workers <= 1:
                pairs = [one(int(i)) for i in chunk.tolist()]
            else:
                # executor.map preserves the exact chronological input order.
                pairs = list(_executor().map(one, chunk.tolist()))
            for i, res in pairs:
                results.append(res)
                if res.get('valid') and res.get('filled'):
                    xs.append(snapshots['x'][i]); ys.append(float(res['pnl_r']))
            done += len(pairs); _PATHS += len(pairs)
            mem = _memory(); ratio = float(mem.get('ratio') or 0.0)
            if ratio >= GC_MEMORY_RATIO:
                gc.collect(0)
            active = dict(core.state.get('autonomous_live_progress') or {})
            active.update({'heartbeat_at': int(time.time()), 'substage': 'V56_CAUSAL_TRADE_PATH_SIMULATION',
                'paths_completed_current_call': done, 'paths_total_current_call': int(len(idxs)),
                'paths_per_second_current_call': round(done / max(time.monotonic() - started, 1e-9), 3),
                'simulation_workers': workers, 'simulation_worker_cap': MAX_WORKERS,
                'simulation_chunk': CHUNK, 'memory_ratio': mem.get('ratio'),
                'future_prices_as_features': False,
                'future_5m_role': 'OUTCOME_SETTLEMENT_AFTER_PLAN_FREEZE_ONLY'})
            core.state['autonomous_live_progress'] = active
            _state(core, status='RUNNING', paths_completed=_PATHS, heartbeat=active, memory=mem)
        if not xs:
            return (np.empty((0, snapshots['x'].shape[1]), dtype=np.float32),
                    np.empty(0, dtype=np.float32), results)
        return np.asarray(xs, dtype=np.float32), np.asarray(ys, dtype=np.float32), results

    autonomous._simulate_indices = simulate_indices
    _state(core, installed=True, status='READY', max_workers=MAX_WORKERS, chunk=CHUNK,
           memory_pressure_forces_serial=True, candidate_parallelism=False,
           oos_package_parallelism=False, ordered_results=True,
           history_reduced=False, features_reduced=False, future_peeking_enabled=False)
