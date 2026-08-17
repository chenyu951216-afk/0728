from __future__ import annotations

"""Unified post-replay performance authority for the autonomous research runtime.

This layer is deliberately semantic-neutral: it does not reduce history, features,
population, generations, holding horizons, OOS gates, costs, stops, targets, or
no-lookahead rules. It removes hot-path overhead that does not add research
information and adds runtime parity checks before enabling a vectorized simulator.
"""

import gc
import math
import mmap
import os
import threading
import time
from pathlib import Path
from typing import Any

import numpy as np

import v15_data_resilience as resilience
import v31_autonomous_runtime_hardening as hardening
import v33_autonomous_compute_efficiency as compute

VERSION = 'V43_UNIFIED_PERFORMANCE_AUTHORITY'
SCHEMA = 43
STATE_KEY = 'v43_unified_performance_authority'

MEMORY_SOFT = max(.55, min(.85, float(os.getenv('AUTONOMOUS_V43_MEMORY_SOFT_RATIO', '.72'))))
MEMORY_HARD = max(MEMORY_SOFT + .04, min(.91, float(os.getenv('AUTONOMOUS_V43_MEMORY_HARD_RATIO', '.84'))))
MEMORY_EMERGENCY = max(MEMORY_HARD + .03, min(.96, float(os.getenv('AUTONOMOUS_V43_MEMORY_EMERGENCY_RATIO', '.90'))))
DB_PAGECACHE_DROP = max(.50, min(.90, float(os.getenv('AUTONOMOUS_V43_DB_PAGECACHE_DROP_RATIO', '.70'))))
MMAP_DROP = max(DB_PAGECACHE_DROP, min(.94, float(os.getenv('AUTONOMOUS_V43_MMAP_DROP_RATIO', '.82'))))
FAST_PARITY_TRADES = max(8, min(256, int(os.getenv('AUTONOMOUS_V43_FAST_SIM_PARITY_TRADES', '48'))))
MIN_YIELD_MS = max(0, min(50, int(os.getenv('AUTONOMOUS_V43_MIN_YIELD_MS', '2'))))
MAX_YIELD_MS = max(MIN_YIELD_MS, min(500, int(os.getenv('AUTONOMOUS_V43_MAX_YIELD_MS', '160'))))
GC_EVERY = max(4, min(64, int(os.getenv('AUTONOMOUS_V43_GC_EVERY', '12'))))

_INSTALL_LOCK = threading.Lock()
_INSTALLED = False
_PREINSTALLED = False
_STATE_LOCK = threading.Lock()
_FROZEN_CONTRACT: dict[str, Any] = {}
_DECISION_MASK_CACHE: dict[int, np.ndarray] = {}
_DECISION_MASK_TOKEN: tuple[Any, ...] | None = None
_FAST_LOCK = threading.Lock()
_FAST_ENABLED = True
_FAST_VERIFIED = False
_FAST_PARITY_DONE = 0
_FAST_PARITY_MISMATCHES = 0
_FAST_MISMATCH: dict[str, Any] | None = None
_CANDIDATES_DONE = 0
_PAGECACHE_RECLAIMS = 0
_MMAP_RECLAIMS = 0


def _memory() -> dict[str, Any]:
    current = limit = rss = None
    try:
        p = Path('/sys/fs/cgroup/memory.current')
        q = Path('/sys/fs/cgroup/memory.max')
        if p.exists():
            current = int(p.read_text().strip())
        if q.exists():
            raw = q.read_text().strip()
            limit = None if raw == 'max' else int(raw)
    except Exception:
        pass
    try:
        pages = int(Path('/proc/self/statm').read_text().split()[1])
        rss = pages * int(os.sysconf('SC_PAGE_SIZE'))
    except Exception:
        pass
    ratio = current / max(limit, 1) if current is not None and limit else None
    return {
        'current_bytes': current,
        'limit_bytes': limit,
        'rss_bytes': rss,
        'ratio': ratio,
        'soft_ratio': MEMORY_SOFT,
        'hard_ratio': MEMORY_HARD,
        'emergency_ratio': MEMORY_EMERGENCY,
    }


def _state(core: Any, **patch: Any) -> dict[str, Any]:
    with _STATE_LOCK:
        raw = core.state.get(STATE_KEY)
        out = dict(raw) if isinstance(raw, dict) else {}
        out.update(patch)
        out.update({'runtime': VERSION, 'schema': SCHEMA, 'updated_at': int(time.time())})
        core.state[STATE_KEY] = out
        return out


def _db_files(core: Any) -> list[Path]:
    base = Path(str(getattr(core, 'DB_PATH', os.getenv('DATABASE_PATH', '/data/eth_adaptive.db'))))
    return [base, Path(str(base) + '-wal'), Path(str(base) + '-shm')]


def _advise_drop(path: Path) -> bool:
    if not path.exists() or not hasattr(os, 'posix_fadvise') or not hasattr(os, 'POSIX_FADV_DONTNEED'):
        return False
    fd = None
    try:
        fd = os.open(path, os.O_RDONLY)
        os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED)
        return True
    except Exception:
        return False
    finally:
        if fd is not None:
            try:
                os.close(fd)
            except Exception:
                pass


def _madvise_array(arr: Any) -> bool:
    mm = getattr(arr, '_mmap', None)
    if mm is None or not hasattr(mm, 'madvise') or not hasattr(mmap, 'MADV_DONTNEED'):
        return False
    try:
        mm.madvise(mmap.MADV_DONTNEED)
        return True
    except Exception:
        return False


def _drop_memmaps(snapshots: dict[str, Any] | None, market: dict[str, Any] | None) -> int:
    seen: set[int] = set()
    dropped = 0
    for container in (snapshots or {}, market or {}):
        for value in container.values():
            if isinstance(value, np.ndarray) and id(value) not in seen:
                seen.add(id(value))
                dropped += int(_madvise_array(value))
            elif hasattr(value, 'ts') and isinstance(getattr(value, 'ts', None), np.ndarray):
                value = getattr(value, 'ts')
                if id(value) not in seen:
                    seen.add(id(value))
                    dropped += int(_madvise_array(value))
    return dropped


def _reclaim(core: Any, snapshots: dict[str, Any] | None = None, market: dict[str, Any] | None = None, *, aggressive: bool = False) -> dict[str, Any]:
    global _PAGECACHE_RECLAIMS, _MMAP_RECLAIMS
    before = _memory()
    ratio = float(before.get('ratio') or 0.0)
    collected = int(gc.collect()) if aggressive or ratio >= MEMORY_SOFT else 0
    malloc_trimmed = False
    try:
        import ctypes
        fn = getattr(ctypes.CDLL(None), 'malloc_trim', None)
        if fn is not None:
            malloc_trimmed = bool(fn(0))
    except Exception:
        pass
    checkpoint = None
    dropped_files = 0
    if ratio >= DB_PAGECACHE_DROP or aggressive:
        try:
            con = core.db()
            try:
                row = con.execute('PRAGMA wal_checkpoint(PASSIVE)').fetchone()
                checkpoint = list(row) if row else None
            finally:
                con.close()
        except Exception:
            checkpoint = None
        try:
            resilience._CANON_CACHE.clear()
        except Exception:
            pass
        for p in _db_files(core):
            dropped_files += int(_advise_drop(p))
        _PAGECACHE_RECLAIMS += int(dropped_files > 0)
    dropped_maps = 0
    mid = _memory()
    if float(mid.get('ratio') or 0.0) >= MMAP_DROP or aggressive:
        dropped_maps = _drop_memmaps(snapshots, market)
        _MMAP_RECLAIMS += int(dropped_maps > 0)
    after = _memory()
    return {
        'before': before,
        'after': after,
        'gc_collected': collected,
        'malloc_trimmed': malloc_trimmed,
        'sqlite_checkpoint': checkpoint,
        'file_cache_advice_count': dropped_files,
        'memmap_advice_count': dropped_maps,
    }


class _AdaptiveGCProxy:
    """Avoid full-generation GC on every fold/candidate; collect on cadence/pressure."""
    def __init__(self) -> None:
        self.calls = 0
        self.executed = 0

    def collect(self, *args: Any, **kwargs: Any) -> int:
        self.calls += 1
        ratio = float(_memory().get('ratio') or 0.0)
        if ratio >= MEMORY_SOFT or self.calls % GC_EVERY == 0:
            self.executed += 1
            return int(gc.collect(*args, **kwargs))
        return 0


def _install_gc_policy(core: Any, autonomous: Any) -> None:
    proxy = _AdaptiveGCProxy()
    autonomous.gc = proxy
    compute.gc = proxy
    _state(core, adaptive_gc_policy=True, explicit_gc_every=GC_EVERY)


def _install_frozen_contract_cache(core: Any, autonomous: Any, leverage: Any) -> None:
    original = leverage._frozen_contract

    def cached_frozen_contract(c: Any, a: Any, *, create: bool = False) -> dict[str, Any]:
        global _FROZEN_CONTRACT
        if create:
            result = dict(original(c, a, create=True) or {})
            if result.get('ok'):
                _FROZEN_CONTRACT = result
            return result
        cached = _FROZEN_CONTRACT
        if cached.get('ok') and float(cached.get('notional_usdt') or 0.0) == float(a.PAPER_NOTIONAL_USDT):
            return dict(cached)
        result = dict(original(c, a, create=False) or {})
        if result.get('ok'):
            _FROZEN_CONTRACT = result
        return result

    leverage._frozen_contract = cached_frozen_contract
    _state(core, frozen_execution_contract_memory_cached=True)


def _install_decision_mask_cache(core: Any, autonomous: Any) -> None:
    original = autonomous._decision_mask

    def cached_decision_mask(ts: np.ndarray, stride: int) -> np.ndarray:
        global _DECISION_MASK_TOKEN
        stride = int(stride)
        if stride <= 1:
            return np.ones(len(ts), dtype=bool)
        token = (id(ts), len(ts), int(ts[0]) if len(ts) else None, int(ts[-1]) if len(ts) else None)
        if token != _DECISION_MASK_TOKEN:
            _DECISION_MASK_CACHE.clear()
            _DECISION_MASK_TOKEN = token
        cached = _DECISION_MASK_CACHE.get(stride)
        if cached is None:
            cached = original(ts, stride)
            _DECISION_MASK_CACHE[stride] = cached
        return cached

    autonomous._decision_mask = cached_decision_mask
    _state(core, decision_mask_cache=True)


def _start_index(ts5: np.ndarray, decision_close: int, continuous: bool) -> int:
    if continuous and len(ts5):
        delta = int(decision_close) - int(ts5[0])
        if delta >= 0 and delta % 300 == 0:
            idx = delta // 300
            if idx < len(ts5) and int(ts5[idx]) == int(decision_close):
                return int(idx)
    return int(np.searchsorted(ts5, int(decision_close), side='left'))


def _fast_trade(core: Any, autonomous: Any, leverage: Any, market: dict[str, Any], ts: int, features: np.ndarray, genome: dict[str, Any]) -> dict[str, Any]:
    contract = leverage._frozen_contract(core, autonomous, create=False)
    if not contract.get('ok'):
        return {'valid': False, 'filled': False, 'pnl_r': 0.0, 'reason': 'frozen_bitget_max_leverage_contract_unavailable'}
    close = market['close15'].get(int(ts)) if market.get('close15') is not None else None
    if close is None or float(close) <= 0:
        return {'valid': False, 'filled': False, 'pnl_r': 0.0, 'reason': 'missing_decision_close'}
    try:
        atr_idx = autonomous.FEATURE_INDEX['atr_pct']
        atr_pct = max(abs(float(features[atr_idx])), .00035)
    except Exception:
        atr_pct = .00035
    stop_fraction = max(float(genome.get('stop_atr') or 0.0) * atr_pct, .0008)
    headroom = float(contract.get('conservative_stop_headroom_fraction') or 0.0)
    if headroom <= 0 or stop_fraction >= headroom:
        return {
            'valid': False, 'filled': False, 'pnl_r': 0.0,
            'reason': 'initial_stop_outside_conservative_max_leverage_headroom',
            'stop_fraction': stop_fraction, 'max_safe_fraction': headroom,
        }

    ts5 = market.get('ts5')
    if ts5 is None or not len(ts5):
        out = {'valid': False, 'filled': False, 'pnl_r': 0.0, 'reason': 'incomplete_full_evolved_holding_horizon'}
        out['max_leverage'] = float(contract['effective_max_leverage'])
        out['maintenance_margin_rate'] = float(contract['maintenance_margin_rate'])
        return out
    decision_close = int(ts) + 900
    continuous = str(market.get('source5') or '').startswith('canonical-sql-fixed-priority-v42')
    start = _start_index(ts5, decision_close, continuous)
    max_hold_5 = int(genome.get('max_hold_bars') or 1) * 3
    required_end = start + max_hold_5
    if start >= len(ts5) or required_end > len(ts5):
        out = {'valid': False, 'filled': False, 'pnl_r': 0.0, 'reason': 'incomplete_full_evolved_holding_horizon'}
        out['max_leverage'] = float(contract['effective_max_leverage'])
        out['maintenance_margin_rate'] = float(contract['maintenance_margin_rate'])
        return out
    if int(ts5[start]) != decision_close:
        out = {'valid': False, 'filled': False, 'pnl_r': 0.0, 'reason': 'missing_first_future_5m'}
        out['max_leverage'] = float(contract['effective_max_leverage'])
        out['maintenance_margin_rate'] = float(contract['maintenance_margin_rate'])
        return out
    end = required_end
    if not continuous:
        segment = ts5[start:end]
        if len(segment) > 1 and bool(np.any(np.diff(segment) != 300)):
            out = {'valid': False, 'filled': False, 'pnl_r': 0.0, 'reason': 'future_5m_gap'}
            out['max_leverage'] = float(contract['effective_max_leverage'])
            out['maintenance_margin_rate'] = float(contract['maintenance_margin_rate'])
            return out

    close = float(close)
    atr_abs = max(close * atr_pct, close * .00035)
    sign = 1.0 if genome['direction'] == 'LONG' else -1.0
    planned_entry = float(close + sign * float(genome['entry_offset_atr']) * atr_abs)
    stop_distance = max(float(genome['stop_atr']) * atr_abs, close * .0008)
    planned_stop = planned_entry - sign * stop_distance
    expire_5 = min(max_hold_5, int(genome['expire_bars']) * 3)

    if genome.get('entry_market'):
        fill_idx = start
        entry = float(market['o5'][start])
    else:
        lows = np.asarray(market['l5'][start:start + expire_5])
        highs = np.asarray(market['h5'][start:start + expire_5])
        touched = (lows <= planned_entry) & (highs >= planned_entry)
        where = np.flatnonzero(touched)
        if not len(where):
            out = {'valid': True, 'filled': False, 'pnl_r': 0.0, 'reason': 'entry_not_filled'}
            out['max_leverage'] = float(contract['effective_max_leverage'])
            out['maintenance_margin_rate'] = float(contract['maintenance_margin_rate'])
            return out
        fill_idx = start + int(where[0])
        entry = planned_entry

    risk = abs(entry - planned_stop)
    if risk <= max(entry * 1e-6, 1e-9):
        out = {'valid': False, 'filled': False, 'pnl_r': 0.0, 'reason': 'invalid_risk'}
        out['max_leverage'] = float(contract['effective_max_leverage'])
        out['maintenance_margin_rate'] = float(contract['maintenance_margin_rate'])
        return out

    lows = np.asarray(market['l5'][fill_idx:end], dtype=np.float64)
    highs = np.asarray(market['h5'][fill_idx:end], dtype=np.float64)
    closes = np.asarray(market['c5'][fill_idx:end], dtype=np.float64)
    length = len(lows)
    if length <= 0:
        out = {'valid': False, 'filled': False, 'pnl_r': 0.0, 'reason': 'no_future_path'}
        out['max_leverage'] = float(contract['effective_max_leverage'])
        out['maintenance_margin_rate'] = float(contract['maintenance_margin_rate'])
        return out

    fav = (highs - entry) / risk if sign > 0 else (entry - lows) / risk
    prior = np.empty(length, dtype=np.float64)
    prior[0] = -np.inf
    if length > 1:
        prior[1:] = np.maximum.accumulate(fav[:-1])
    stops = np.full(length, planned_stop, dtype=np.float64)
    be = float(genome['breakeven_after_r'])
    trail_start = float(genome['trail_start_r'])
    lock = entry + sign * float(genome['trail_lock_r']) * risk
    if sign > 0:
        stops[prior >= be] = np.maximum(stops[prior >= be], entry)
        stops[prior >= trail_start] = np.maximum(stops[prior >= trail_start], lock)
        stop_hits = lows <= stops
    else:
        stops[prior >= be] = np.minimum(stops[prior >= be], entry)
        stops[prior >= trail_start] = np.minimum(stops[prior >= trail_start], lock)
        stop_hits = highs >= stops
    hit_stop = np.flatnonzero(stop_hits)
    stop_rel = int(hit_stop[0]) if len(hit_stop) else None
    target_end = stop_rel if stop_rel is not None else length

    remaining = 1.0
    realized = 0.0
    allocations = [float(x) / 100.0 for x in genome['allocations']]
    targets = [entry + sign * risk * float(rr) for rr in genome['target_rr']]
    # The fill bar never receives target credit. A stop on a bar always has priority,
    # therefore target searches end strictly before the first stop bar.
    if target_end > 1:
        th = highs[1:target_end]
        tl = lows[1:target_end]
        for k, px in enumerate(targets):
            target_hit = bool(np.any(th >= px)) if sign > 0 else bool(np.any(tl <= px))
            if target_hit and remaining > 1e-12:
                frac = min(remaining, allocations[k])
                realized += frac * float(genome['target_rr'][k])
                remaining -= frac

    if remaining > 1e-12 and stop_rel is not None:
        realized += remaining * ((float(stops[stop_rel]) - entry) * sign / risk)
        remaining = 0.0
    if remaining > 1e-12:
        realized += remaining * ((float(closes[-1]) - entry) * sign / risk)

    cost_r = (float(autonomous.ALL_IN_COST_BPS) / 10000.0) * entry / risk
    out = {
        'valid': True, 'filled': True, 'pnl_r': float(realized - cost_r),
        'gross_r': float(realized), 'cost_r': float(cost_r),
        'fill_ts': int(ts5[fill_idx]), 'entry': float(entry), 'stop': float(planned_stop),
        'max_leverage': float(contract['effective_max_leverage']),
        'maintenance_margin_rate': float(contract['maintenance_margin_rate']),
    }
    return out


def _equivalent(a: dict[str, Any], b: dict[str, Any]) -> tuple[bool, str | None]:
    for key in ('valid', 'filled', 'reason'):
        if a.get(key) != b.get(key):
            return False, f'{key}: legacy={a.get(key)!r} fast={b.get(key)!r}'
    keys = ('pnl_r', 'gross_r', 'cost_r', 'entry', 'stop', 'fill_ts', 'stop_fraction', 'max_safe_fraction', 'max_leverage', 'maintenance_margin_rate')
    for key in keys:
        if key not in a and key not in b:
            continue
        av, bv = a.get(key), b.get(key)
        if av is None or bv is None:
            if av != bv:
                return False, f'{key}: legacy={av!r} fast={bv!r}'
            continue
        try:
            if not math.isclose(float(av), float(bv), rel_tol=1e-10, abs_tol=1e-10):
                return False, f'{key}: legacy={av!r} fast={bv!r}'
        except Exception:
            if av != bv:
                return False, f'{key}: legacy={av!r} fast={bv!r}'
    return True, None


def _install_fast_simulator(core: Any, autonomous: Any, leverage: Any) -> None:
    global _FAST_ENABLED, _FAST_VERIFIED, _FAST_PARITY_DONE, _FAST_PARITY_MISMATCHES, _FAST_MISMATCH
    legacy_simulate = autonomous._simulate_trade

    def fast_simulate(market: dict[str, Any], ts: int, features: np.ndarray, genome: dict[str, Any]) -> dict[str, Any]:
        global _FAST_ENABLED, _FAST_VERIFIED, _FAST_PARITY_DONE, _FAST_PARITY_MISMATCHES, _FAST_MISMATCH
        active_cache = getattr(compute, '_ACTIVE_CACHE', None)
        active_id = getattr(compute, '_ACTIVE_ID', None) or 'UNCACHED'
        key = (active_id, int(ts))
        if active_cache is not None:
            cached = active_cache.get(key)
            if cached is not None:
                return dict(cached)
        if not _FAST_ENABLED:
            return legacy_simulate(market, ts, features, genome)

        fast = _fast_trade(core, autonomous, leverage, market, ts, features, genome)
        need_parity = False
        with _FAST_LOCK:
            need_parity = _FAST_PARITY_DONE < FAST_PARITY_TRADES
        if need_parity:
            saved_cache = getattr(compute, '_ACTIVE_CACHE', None)
            try:
                compute._ACTIVE_CACHE = None
                legacy = legacy_simulate(market, ts, features, genome)
            finally:
                compute._ACTIVE_CACHE = saved_cache
            ok, reason = _equivalent(dict(legacy), dict(fast))
            with _FAST_LOCK:
                _FAST_PARITY_DONE += 1
                if not ok:
                    _FAST_PARITY_MISMATCHES += 1
                    _FAST_ENABLED = False
                    _FAST_VERIFIED = False
                    _FAST_MISMATCH = {'ts': int(ts), 'reason': reason, 'legacy': legacy, 'fast': fast}
                elif _FAST_PARITY_DONE >= FAST_PARITY_TRADES:
                    _FAST_VERIFIED = True
            if not ok:
                _state(core, fast_simulator='DISABLED_PARITY_MISMATCH', fast_parity_done=_FAST_PARITY_DONE,
                       fast_parity_mismatches=_FAST_PARITY_MISMATCHES, fast_mismatch=_FAST_MISMATCH)
                out = dict(legacy)
            else:
                out = dict(fast)
                if _FAST_VERIFIED:
                    _state(core, fast_simulator='VERIFIED_ACTIVE', fast_parity_done=_FAST_PARITY_DONE,
                           fast_parity_mismatches=0)
        else:
            out = dict(fast)
        if active_cache is not None:
            active_cache[key] = dict(out)
        return out

    autonomous._simulate_trade = fast_simulate
    _state(core, fast_simulator='PARITY_WARMUP', fast_parity_target=FAST_PARITY_TRADES,
           fast_simulator_auto_fallback=True)


def _install_resource_release(core: Any, transition: Any) -> None:
    original_release = transition._release_memory

    def release_memory(c: Any | None = None, *, checkpoint: bool = False) -> dict[str, Any]:
        result = dict(original_release(c, checkpoint=checkpoint) or {})
        target = c or core
        mem = _memory()
        if float(mem.get('ratio') or 0.0) >= DB_PAGECACHE_DROP:
            result['v43_reclaim'] = _reclaim(target, aggressive=float(mem.get('ratio') or 0.0) >= MEMORY_HARD)
        return result

    transition._release_memory = release_memory

    original_trim = hardening._trim
    def trim() -> dict[str, Any]:
        result = dict(original_trim() or {})
        mem = _memory()
        if float(mem.get('ratio') or 0.0) >= DB_PAGECACHE_DROP:
            result['v43_reclaim'] = _reclaim(core, aggressive=float(mem.get('ratio') or 0.0) >= MEMORY_HARD)
        return result
    hardening._trim = trim
    _state(core, cgroup_pagecache_reclaim=True)


def _install_candidate_governor(core: Any, autonomous: Any, resource_authority: Any) -> None:
    global _CANDIDATES_DONE
    resource_authority.CANDIDATE_YIELD_MS = 0
    base_eval = autonomous._evaluate_candidate

    def governed_eval(snapshots: dict[str, Any], market: dict[str, Any], genome: dict[str, Any], seed: int):
        global _CANDIDATES_DONE
        wall0 = time.monotonic()
        cpu0 = time.process_time()
        result = base_eval(snapshots, market, genome, seed)
        wall = max(time.monotonic() - wall0, 1e-9)
        cpu = max(time.process_time() - cpu0, 0.0)
        _CANDIDATES_DONE += 1
        mem = _memory()
        ratio = float(mem.get('ratio') or 0.0)
        reclaim = None
        if ratio >= MEMORY_SOFT:
            reclaim = _reclaim(core, snapshots, market, aggressive=ratio >= MEMORY_HARD)
            mem = _memory()
            ratio = float(mem.get('ratio') or 0.0)
        elif _CANDIDATES_DONE % GC_EVERY == 0:
            gc.collect(0)

        if ratio >= MEMORY_EMERGENCY:
            reclaim = _reclaim(core, snapshots, market, aggressive=True)
            mem = _memory()
            ratio = float(mem.get('ratio') or 0.0)
            if ratio >= MEMORY_EMERGENCY:
                _state(core, governor='EMERGENCY_FAIL_CLOSED', candidate_count=_CANDIDATES_DONE,
                       memory=mem, last_candidate_seconds=round(wall, 3))
                raise MemoryError(f'V43 emergency memory ratio {ratio:.3f} >= {MEMORY_EMERGENCY:.3f}')

        if ratio < MEMORY_SOFT:
            yield_ms = MIN_YIELD_MS
        elif ratio < MEMORY_HARD:
            span = max(MEMORY_HARD - MEMORY_SOFT, 1e-6)
            yield_ms = int(MIN_YIELD_MS + (MAX_YIELD_MS * .35) * (ratio - MEMORY_SOFT) / span)
        else:
            yield_ms = MAX_YIELD_MS
        if yield_ms:
            time.sleep(yield_ms / 1000.0)
        _state(core, governor='RUNNING', candidate_count=_CANDIDATES_DONE,
               last_candidate_seconds=round(wall, 3), last_candidate_process_cpu_seconds=round(cpu, 3),
               cpu_core_equivalent=round(cpu / wall, 3), candidate_yield_ms=yield_ms,
               memory=mem, last_reclaim=reclaim, pagecache_reclaims=_PAGECACHE_RECLAIMS,
               memmap_reclaims=_MMAP_RECLAIMS)
        return result

    autonomous._evaluate_candidate = governed_eval
    _state(core, adaptive_candidate_resource_governor=True, fixed_candidate_sleep_removed=True)


def preinstall(production: Any, autonomous: Any, transition: Any, leverage: Any) -> None:
    """Install guards that must exist before V42 is allowed to enqueue stage 6."""
    global _PREINSTALLED
    with _INSTALL_LOCK:
        if _PREINSTALLED:
            return
        _PREINSTALLED = True
    core = production.core
    _install_gc_policy(core, autonomous)
    _install_frozen_contract_cache(core, autonomous, leverage)
    _install_decision_mask_cache(core, autonomous)
    _install_fast_simulator(core, autonomous, leverage)
    _install_resource_release(core, transition)
    _state(core, preinstalled=True)


def install(production: Any, autonomous: Any, transition: Any, resource_authority: Any, leverage: Any, scheduler: Any | None = None) -> None:
    global _INSTALLED
    preinstall(production, autonomous, transition, leverage)
    with _INSTALL_LOCK:
        if _INSTALLED:
            return
        _INSTALLED = True
    core = production.core
    _install_candidate_governor(core, autonomous, resource_authority)

    # Replay/data/model semantics remain untouched. Performance changes are permitted
    # only when they can be proven equivalent; the fast simulator disables itself on
    # the first runtime parity mismatch and falls back to the legacy path.
    rules = {
        'history_reduced': False,
        'feature_set_reduced': False,
        'population_reduced': False,
        'generations_reduced': False,
        'holding_horizons_reduced': False,
        'oos_rules_changed': False,
        'fitness_changed': False,
        'cost_model_changed': False,
        'stop_target_semantics_changed': False,
        'future_peeking_enabled': False,
        'cross_candidate_result_cache': False,
        'frozen_execution_contract_memory_cached': True,
        'decision_mask_semantic_cache': True,
        'vectorized_simulator_runtime_parity_guard': True,
        'pagecache_reclaim_is_advisory_only': True,
        'memory_emergency_fails_closed': True,
    }
    core.state.setdefault('strict_replay', {})['unified_performance_authority_v43'] = dict(rules)
    _state(core, installed=True, rules=rules, memory=_memory())

    if not any(getattr(r, 'path', None) == '/api/v43/performance' for r in core.app.router.routes):
        @core.app.get('/api/v43/performance')
        def performance_status() -> dict[str, Any]:
            return {
                'runtime': VERSION, 'schema': SCHEMA,
                'state': dict(core.state.get(STATE_KEY) or {}),
                'memory': _memory(),
                'fast_simulator': {
                    'enabled': _FAST_ENABLED,
                    'verified': _FAST_VERIFIED,
                    'parity_done': _FAST_PARITY_DONE,
                    'parity_target': FAST_PARITY_TRADES,
                    'mismatches': _FAST_PARITY_MISMATCHES,
                    'last_mismatch': _FAST_MISMATCH,
                },
                'frozen_contract_cached': bool(_FROZEN_CONTRACT.get('ok')),
                'decision_mask_cache_entries': len(_DECISION_MASK_CACHE),
                'candidates_completed': _CANDIDATES_DONE,
                'pagecache_reclaims': _PAGECACHE_RECLAIMS,
                'memmap_reclaims': _MMAP_RECLAIMS,
                'rules': rules,
            }

    # V42 boot scheduling can be intentionally deferred until this final authority is
    # installed so the first heavy candidate already has all resource/simulator guards.
    if scheduler is not None:
        try:
            scheduler._kick(core, autonomous, transition, source='v43_boot', force_interval=True)
            _state(core, post_install_scheduler_kick='REQUESTED')
        except Exception as exc:
            _state(core, post_install_scheduler_kick='ERROR', scheduler_error=f'{type(exc).__name__}: {exc}')
