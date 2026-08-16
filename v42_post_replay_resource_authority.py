from __future__ import annotations

"""Post-replay scheduling + resource authority for autonomous research.

The fixed historical replay is immutable once complete.  Older replay/gap/watchdog
layers were still re-auditing millions of rows every learning tick and V41 could also
mistake a completed V26 future for an active queued certification forever.  On an
8 GB deployment that combination can leave stage 6 at 0% while SQLite/canonical
history scans consume CPU and retain large Python candle caches.

V42 makes the completed replay a cheap immutable view, stops replay-maintenance loops
once that replay is formally complete, repairs stale V26 queue state by checking the
actual background Future, and builds the autonomous feature/market research caches
through bounded streaming/mmap paths.  It does not change history, strategy fitness,
OOS rules, fills, stops, targets, or no-lookahead semantics.
"""

import gc
import hashlib
import json
import os
import threading
import time
from pathlib import Path
from typing import Any

import numpy as np

import v5_runtime
import v15_data_resilience as resilience
import v16_runtime_integrity as runtime_integrity


VERSION = 'V42_POST_REPLAY_RESOURCE_AUTHORITY'
SCHEMA = 42
STATE_KEY = 'v42_post_replay_resource_authority'
CACHE_DIR_NAME = 'autonomous_v42_cache'
POST_REPLAY_KICK_SECONDS = max(10, min(120, int(os.getenv('AUTONOMOUS_POST_REPLAY_KICK_SECONDS', '20'))))
CANDIDATE_YIELD_MS = max(0, min(1000, int(os.getenv('AUTONOMOUS_CANDIDATE_YIELD_MS', '100'))))
FEATURE_CHUNK = max(128, min(4096, int(os.getenv('AUTONOMOUS_FEATURE_CACHE_CHUNK', '768'))))
MARKET_CHUNK = max(256, min(8192, int(os.getenv('AUTONOMOUS_MARKET_CACHE_CHUNK', '2048'))))

_INSTALL_LOCK = threading.Lock()
_CACHE_LOCK = threading.RLock()
_SCHED_LOCK = threading.Lock()
_INSTALLED = False
_LAST_KICK_AT = 0.0
_REPLAY_COMPLETE = False
_REPLAY_SNAPSHOT: dict[str, Any] = {}


class _CloseLookup:
    """Array-backed exact 15m close lookup without a 200k-entry Python dict."""

    def __init__(self, ts: np.ndarray, close: np.ndarray):
        self.ts = ts
        self.close = close
        self.start = int(ts[0]) if len(ts) else 0
        self.sec = 900

    def get(self, key: int, default: Any = None) -> Any:
        if not len(self.ts):
            return default
        k = int(key)
        delta = k - self.start
        if delta < 0 or delta % self.sec:
            return default
        idx = delta // self.sec
        if idx < 0 or idx >= len(self.ts) or int(self.ts[idx]) != k:
            return default
        value = float(self.close[idx])
        return value if np.isfinite(value) else default


def _state(core: Any, **patch: Any) -> dict[str, Any]:
    raw = core.state.get(STATE_KEY)
    out = dict(raw) if isinstance(raw, dict) else {}
    out.update(patch)
    out['runtime'] = VERSION
    out['schema'] = SCHEMA
    out['updated_at'] = int(time.time())
    core.state[STATE_KEY] = out
    return out


def _db_path(core: Any) -> Path:
    return Path(str(getattr(core, 'DB_PATH', os.getenv('DATABASE_PATH', '/data/eth_adaptive.db'))))


def _cache_root(core: Any) -> Path:
    root = _db_path(core).parent / CACHE_DIR_NAME
    root.mkdir(parents=True, exist_ok=True)
    return root


def _atomic_json(path: Path, payload: dict[str, Any]) -> None:
    tmp = path.with_suffix(path.suffix + '.tmp')
    tmp.write_text(json.dumps(payload, sort_keys=True, separators=(',', ':'), ensure_ascii=False), encoding='utf-8')
    os.replace(tmp, path)


def _fast_replay_complete(core: Any, autonomous: Any) -> bool:
    global _REPLAY_COMPLETE, _REPLAY_SNAPSHOT
    if _REPLAY_COMPLETE:
        return True
    learning = core.state.get('learning') if isinstance(core.state.get('learning'), dict) else {}
    known = learning.get('replay_learning_progress') if isinstance(learning.get('replay_learning_progress'), dict) else {}
    if known.get('complete'):
        _REPLAY_COMPLETE = True
        _REPLAY_SNAPSHOT = dict(known)
        return True
    try:
        cursor = int(core.get_state(v5_runtime.REPLAY_STATE_KEY, core.START_TS) or core.START_TS)
        last_decision = int(autonomous.RESEARCH_END_EXCLUSIVE_TS) - int(core.TIMEFRAME_SECONDS.get('15m', 900))
        if cursor >= last_decision:
            snap = dict(runtime_integrity.replay_progress(core) or {})
            if snap.get('complete'):
                _REPLAY_COMPLETE = True
                _REPLAY_SNAPSHOT = snap
                return True
    except Exception:
        pass
    return False


def _freeze_completed_replay_view(core: Any, autonomous: Any) -> None:
    """Replay progress is immutable after completion; dashboard polling must be O(1)."""
    global _REPLAY_COMPLETE, _REPLAY_SNAPSHOT
    base = runtime_integrity.replay_progress
    try:
        snap = dict(base(core) or {})
    except Exception as exc:
        _state(core, replay_snapshot_error=f'{type(exc).__name__}: {exc}')
        return
    if not snap.get('complete'):
        return
    _REPLAY_COMPLETE = True
    _REPLAY_SNAPSHOT = snap

    def completed_replay_view(c: Any) -> dict[str, Any]:
        # A fixed replay cannot become incomplete without a process-level dataset reset.
        return dict(_REPLAY_SNAPSHOT)

    runtime_integrity.replay_progress = completed_replay_view
    _state(core, replay_view='IMMUTABLE_O1_AFTER_COMPLETE', replay_percent=100.0,
           replay_cursor=snap.get('cursor_ts') or snap.get('replay_cursor_ts'))


def _transition_snapshot(core: Any, transition: Any) -> dict[str, Any]:
    try:
        raw = core.get_state(transition.STATE_KEY, {})
    except Exception:
        raw = core.state.get('replay_transition_stability')
    return dict(raw) if isinstance(raw, dict) else {}


def _future_snapshot(transition: Any) -> tuple[bool, bool, str | None]:
    future = getattr(transition, '_CERT_FUTURE', None)
    if future is None:
        return False, True, None
    try:
        done = bool(future.done())
        if not done:
            return True, False, None
        exc = future.exception()
        return False, True, f'{type(exc).__name__}: {exc}' if exc else None
    except Exception as exc:
        return False, True, f'{type(exc).__name__}: {exc}'


def _reconcile_transition(core: Any, autonomous: Any, transition: Any) -> dict[str, Any]:
    if not _fast_replay_complete(core, autonomous):
        return _transition_snapshot(core, transition)
    now = int(time.time())
    trans = _transition_snapshot(core, transition)
    active, done, future_error = _future_snapshot(transition)
    status = str(trans.get('status') or '')
    detected = int(trans.get('replay_complete_detected_at') or 0)
    patch: dict[str, Any] = {}

    # A fresh process cannot still be carrying the heap from the replay that completed
    # in a previous process.  If no completion timestamp was ever persisted, mark the
    # replay cooldown as already satisfied rather than sleeping forever before stage 6.
    if detected <= 0:
        cooldown = int(getattr(transition, 'COMPLETION_COOLDOWN_SECONDS', 180))
        patch.update({'replay_complete_detected_at': now - cooldown - 1, 'ready_after': 0})

    # V26 can finish a very short cooldown/defer Future before the request thread writes
    # QUEUED_BACKGROUND.  Persisted status then says queued although no Future exists.
    # Truth comes from the Future, not the stale label.
    if status in ('CERTIFICATION_QUEUED_BACKGROUND', 'CERTIFICATION_RUNNING') and not active and done:
        ready_after = int(trans.get('ready_after') or patch.get('ready_after') or 0)
        patch.update({
            'status': 'CERTIFICATION_RETRY_READY' if ready_after <= now else 'REPLAY_COMPLETE_COOLDOWN',
            'stale_queue_reconciled_at': now,
            'stale_queue_previous_status': status,
            'stale_queue_future_error': future_error,
        })

    if status == 'REPLAY_COMPLETE_COOLDOWN' and int(trans.get('ready_after') or 0) <= now:
        patch.update({'status': 'CERTIFICATION_RETRY_READY', 'ready_after': 0})

    if patch:
        try:
            trans = dict(transition._persist(core, patch))
        except Exception:
            trans.update(patch)
            core.state['replay_transition_stability'] = trans
    return trans


def _scheduler_kick(core: Any, autonomous: Any, transition: Any, authoritative_request: Any, *, source: str,
                    force_interval: bool = False) -> dict[str, Any]:
    global _LAST_KICK_AT
    if not _fast_replay_complete(core, autonomous):
        return _state(core, scheduler='WAIT_REPLAY', scheduler_source=source)
    role = str((core.state.get('runtime_role') or {}).get('role') or (core.state.get('bootstrap_replica_role') or {}).get('role') or '')
    if role.startswith('FOLLOWER'):
        return _state(core, scheduler='FOLLOWER_READ_ONLY', scheduler_source=source)
    with _SCHED_LOCK:
        now_m = time.monotonic()
        if not force_interval and now_m - _LAST_KICK_AT < POST_REPLAY_KICK_SECONDS:
            return _state(core, scheduler='THROTTLED', scheduler_source=source,
                          next_scheduler_seconds=round(POST_REPLAY_KICK_SECONDS - (now_m - _LAST_KICK_AT), 2))
        _LAST_KICK_AT = now_m
        trans = _reconcile_transition(core, autonomous, transition)
        active, _, future_error = _future_snapshot(transition)
        now = int(time.time())
        ready_after = int(trans.get('ready_after') or 0)
        if active:
            return _state(core, scheduler='BACKGROUND_RESEARCH_ACTIVE', scheduler_source=source,
                          transition_status=trans.get('status'), future_error=future_error)
        if ready_after > now:
            return _state(core, scheduler='SAFETY_BACKOFF', scheduler_source=source,
                          transition_status=trans.get('status'), retry_in_seconds=ready_after - now)
        auto_raw = core.state.get(getattr(autonomous, 'STATE_KEY', 'v30_autonomous_strategy_discovery'))
        auto_raw = dict(auto_raw) if isinstance(auto_raw, dict) else {}
        if str(auto_raw.get('status') or '') in ('COMPLETE', 'COMPLETE_NO_CERTIFIED_PACKAGE'):
            return _state(core, scheduler='RESEARCH_COMPLETE', scheduler_source=source)
        error = None
        try:
            authoritative_request(False)
        except Exception as exc:
            error = f'{type(exc).__name__}: {exc}'
        after = _transition_snapshot(core, transition)
        return _state(core, scheduler='REQUESTED' if error is None else 'REQUEST_ERROR', scheduler_source=source,
                      transition_status=after.get('status'), request_error=error,
                      future_active=_future_snapshot(transition)[0])


def _feature_cache_key(core: Any, autonomous: Any) -> tuple[str, dict[str, Any]]:
    con = core.db()
    try:
        fs = con.execute('''SELECT COUNT(*),MIN(ts),MAX(ts) FROM learning_feature_snapshots
                            WHERE ts>=? AND ts<?''',
                         (int(autonomous.RESEARCH_START_TS), int(autonomous.RESEARCH_END_EXCLUSIVE_TS))).fetchone()
        ls = con.execute('''SELECT COUNT(*),MIN(ts),MAX(ts) FROM learning_samples
                            WHERE ts>=? AND ts<?''',
                         (int(autonomous.RESEARCH_START_TS), int(autonomous.RESEARCH_END_EXCLUSIVE_TS))).fetchone()
    finally:
        con.close()
    detail = {
        'snapshots': int(fs[0] or 0), 'snapshot_first_ts': int(fs[1]) if fs and fs[1] is not None else None,
        'snapshot_last_ts': int(fs[2]) if fs and fs[2] is not None else None,
        'sample_rows': int(ls[0] or 0), 'sample_first_ts': int(ls[1]) if ls and ls[1] is not None else None,
        'sample_last_ts': int(ls[2]) if ls and ls[2] is not None else None,
        'feature_count': len(autonomous.FEATURE_NAMES),
        'research_start_ts': int(autonomous.RESEARCH_START_TS),
        'research_end_exclusive_ts': int(autonomous.RESEARCH_END_EXCLUSIVE_TS),
    }
    raw = json.dumps({**detail, 'features': list(autonomous.FEATURE_NAMES)}, sort_keys=True, separators=(',', ':'))
    return hashlib.sha256(raw.encode()).hexdigest()[:20], detail


def _install_feature_loader(core: Any, autonomous: Any) -> None:
    def load_feature_snapshots(c: Any) -> dict[str, Any]:
        with _CACHE_LOCK:
            key, detail = _feature_cache_key(c, autonomous)
            count = int(detail['snapshots'])
            sample_rows = int(detail['sample_rows'])
            if count < 5000:
                c.state['v35_autonomous_feature_integrity'] = {'schema': SCHEMA, 'status': 'WAITING_ENOUGH_CAUSAL_SNAPSHOTS', **detail, 'updated_at': int(time.time())}
                return {}
            # Schema-6 creates exactly 14 unique strategy/direction rows per timestamp.
            # Equality is a cheap C-level integrity proof here and avoids a 3.1M-row
            # JOIN/GROUP BY on every deployment.
            if sample_rows != count * 14 or detail['sample_first_ts'] != detail['snapshot_first_ts'] or detail['sample_last_ts'] != detail['snapshot_last_ts']:
                c.state['v35_autonomous_feature_integrity'] = {
                    'schema': SCHEMA, 'status': 'FAILED_SAMPLE_SNAPSHOT_CARDINALITY', **detail,
                    'expected_sample_rows': count * 14, 'updated_at': int(time.time()),
                }
                return {}
            root = _cache_root(c) / ('features-' + key)
            root.mkdir(parents=True, exist_ok=True)
            meta_path = root / 'meta.json'; ts_path = root / 'ts.npy'; x_path = root / 'x.npy'
            valid_cache = False
            if meta_path.exists() and ts_path.exists() and x_path.exists():
                try:
                    meta = json.loads(meta_path.read_text(encoding='utf-8'))
                    valid_cache = meta.get('key') == key and int(meta.get('snapshots') or 0) == count
                except Exception:
                    valid_cache = False
            if not valid_cache:
                c.state['v35_autonomous_feature_integrity'] = {'schema': SCHEMA, 'status': 'BUILDING_STREAMED_FEATURE_CACHE', **detail, 'percent': 0.0, 'updated_at': int(time.time())}
                ts_tmp = root / 'ts.tmp.npy'; x_tmp = root / 'x.tmp.npy'
                for p in (ts_tmp, x_tmp):
                    try: p.unlink()
                    except FileNotFoundError: pass
                ts_mm = np.lib.format.open_memmap(ts_tmp, mode='w+', dtype=np.int64, shape=(count,))
                x_mm = np.lib.format.open_memmap(x_tmp, mode='w+', dtype=np.float32, shape=(count, len(autonomous.FEATURE_NAMES)))
                con = c.db(); idx = 0; invalid_json = 0
                try:
                    cur = con.execute('''SELECT ts,features FROM learning_feature_snapshots
                                         WHERE ts>=? AND ts<? ORDER BY ts''',
                                      (int(autonomous.RESEARCH_START_TS), int(autonomous.RESEARCH_END_EXCLUSIVE_TS)))
                    while True:
                        rows = cur.fetchmany(FEATURE_CHUNK)
                        if not rows: break
                        for row in rows:
                            raw = row[1].decode('utf-8') if isinstance(row[1], (bytes, bytearray)) else str(row[1])
                            try:
                                feat = json.loads(raw)
                                if not isinstance(feat, dict): raise ValueError('snapshot is not dict')
                            except Exception:
                                invalid_json += 1; feat = {}
                            ts_mm[idx] = int(row[0])
                            x_mm[idx] = np.asarray([autonomous._finite(feat.get(name), 0.0) for name in autonomous.FEATURE_NAMES], dtype=np.float32)
                            idx += 1
                        c.state['v35_autonomous_feature_integrity'] = {
                            'schema': SCHEMA, 'status': 'BUILDING_STREAMED_FEATURE_CACHE', **detail,
                            'loaded': idx, 'percent': round(idx / max(count, 1) * 100.0, 2),
                            'invalid_json': invalid_json, 'updated_at': int(time.time()),
                        }
                        time.sleep(0.002)
                finally:
                    con.close()
                ts_mm.flush(); x_mm.flush(); del ts_mm, x_mm
                if idx != count or invalid_json:
                    c.state['v35_autonomous_feature_integrity'] = {'schema': SCHEMA, 'status': 'FAILED_STREAMED_FEATURE_CACHE', **detail, 'loaded': idx, 'invalid_json': invalid_json, 'updated_at': int(time.time())}
                    return {}
                ts_check = np.load(ts_tmp, mmap_mode='r'); x_check = np.load(x_tmp, mmap_mode='r')
                sample_idx = np.linspace(0, count - 1, min(count, 20000), dtype=np.int64)
                sample_x = np.asarray(x_check[sample_idx], dtype=np.float64)
                finite = bool(np.isfinite(sample_x).all())
                variances = np.nanvar(sample_x, axis=0)
                varying = int(np.sum(variances > 1e-14)); nonzero = int(np.sum(np.any(np.abs(sample_x) > 1e-12, axis=0)))
                del ts_check, x_check, sample_x
                if not finite or varying < 6 or nonzero < 6:
                    c.state['v35_autonomous_feature_integrity'] = {'schema': SCHEMA, 'status': 'FAILED_DEGENERATE_FEATURE_CACHE', **detail, 'varying_features': varying, 'nonzero_features': nonzero, 'updated_at': int(time.time())}
                    return {}
                os.replace(ts_tmp, ts_path); os.replace(x_tmp, x_path)
                _atomic_json(meta_path, {'key': key, **detail, 'varying_features': varying, 'nonzero_features': nonzero, 'schema': SCHEMA})
            ts = np.load(ts_path, mmap_mode='r'); x = np.load(x_path, mmap_mode='r')
            c.state['v35_autonomous_feature_integrity'] = {
                'schema': SCHEMA, 'status': 'VALID', **detail,
                'cache': 'PERSISTENT_NUMPY_MEMMAP', 'cache_key': key,
                'legacy_3m_row_join_removed': True, 'updated_at': int(time.time()),
            }
            _state(c, stage6_preflight='FEATURE_CACHE_READY', feature_snapshots=count)
            return {'ts': ts, 'x': x, 'quality': np.ones(len(ts), dtype=np.float32)}
    autonomous._load_feature_snapshots = load_feature_snapshots


def _canonical_expr(column: str) -> str:
    parts = [f"MAX(CASE WHEN source='{src}' THEN {column} END)" for src in resilience.PRICE_PRIORITY]
    return 'COALESCE(' + ','.join(parts) + ')'


def _series_signature(core: Any, tf: str, start: int, end: int, sec: int) -> tuple[dict[str, Any], int]:
    placeholders = ','.join('?' for _ in resilience.PRICE_PRIORITY)
    con = core.db()
    try:
        raw = con.execute(
            f'''SELECT COUNT(*),COALESCE(MAX(rowid),0),MIN(ts),MAX(ts)
                FROM market_bars WHERE asset='ETH' AND tf=? AND ts>=? AND ts<?
                  AND source IN ({placeholders}) AND (ts % ?) = 0''',
            (tf, start, end, *resilience.PRICE_PRIORITY, sec),
        ).fetchone()
        distinct = int(con.execute(
            f'''SELECT COUNT(DISTINCT ts) FROM market_bars WHERE asset='ETH' AND tf=? AND ts>=? AND ts<?
                AND source IN ({placeholders}) AND (ts % ?) = 0''',
            (tf, start, end, *resilience.PRICE_PRIORITY, sec),
        ).fetchone()[0] or 0)
    finally:
        con.close()
    sig = {'tf': tf, 'start': start, 'end': end, 'sec': sec, 'raw_rows': int(raw[0] or 0), 'max_rowid': int(raw[1] or 0),
           'first_ts': int(raw[2]) if raw and raw[2] is not None else None, 'last_ts': int(raw[3]) if raw and raw[3] is not None else None,
           'canonical_timestamps': distinct}
    return sig, distinct


def _build_market_series(core: Any, tf: str, start_ts: int, end_exclusive_ts: int, full_ohlc: bool) -> tuple[Path, dict[str, Any]] | tuple[None, dict[str, Any]]:
    sec = int(core.TIMEFRAME_SECONDS[tf]); start = ((int(start_ts) + sec - 1) // sec) * sec; end = (int(end_exclusive_ts) // sec) * sec
    expected = max(0, (end - start) // sec)
    sig, distinct = _series_signature(core, tf, start, end, sec)
    detail = {**sig, 'expected_bars': expected, 'continuous': False}
    if expected <= 0 or distinct != expected or sig['first_ts'] != start or sig['last_ts'] != end - sec:
        return None, detail
    raw_key = json.dumps(sig, sort_keys=True, separators=(',', ':'))
    key = hashlib.sha256(raw_key.encode()).hexdigest()[:20]
    root = _cache_root(core) / f'market-{tf}-{key}'; root.mkdir(parents=True, exist_ok=True)
    meta_path = root / 'meta.json'; names = ['ts', 'c'] if not full_ohlc else ['ts', 'o', 'h', 'l', 'c']
    paths = {name: root / f'{name}.npy' for name in names}
    valid = False
    if meta_path.exists() and all(p.exists() for p in paths.values()):
        try:
            meta = json.loads(meta_path.read_text(encoding='utf-8')); valid = meta.get('key') == key and int(meta.get('expected_bars') or 0) == expected
        except Exception: valid = False
    if not valid:
        tmps = {name: root / f'{name}.tmp.npy' for name in names}
        for p in tmps.values():
            try: p.unlink()
            except FileNotFoundError: pass
        arrays: dict[str, Any] = {'ts': np.lib.format.open_memmap(tmps['ts'], mode='w+', dtype=np.int64, shape=(expected,)),
                                  'c': np.lib.format.open_memmap(tmps['c'], mode='w+', dtype=np.float64, shape=(expected,))}
        if full_ohlc:
            for name in ('o', 'h', 'l'):
                arrays[name] = np.lib.format.open_memmap(tmps[name], mode='w+', dtype=np.float64, shape=(expected,))
        columns = ['ts'] + (['o', 'h', 'l', 'c'] if full_ohlc else ['c'])
        select = ','.join(['ts'] + [_canonical_expr(col) + f' AS {col}' for col in columns[1:]])
        placeholders = ','.join('?' for _ in resilience.PRICE_PRIORITY)
        con = core.db(); idx = 0; bad_ts = None
        try:
            cur = con.execute(
                f'''SELECT {select} FROM market_bars
                    WHERE asset='ETH' AND tf=? AND ts>=? AND ts<? AND source IN ({placeholders}) AND (ts % ?) = 0
                    GROUP BY ts ORDER BY ts''',
                (tf, start, end, *resilience.PRICE_PRIORITY, sec),
            )
            while True:
                rows = cur.fetchmany(MARKET_CHUNK)
                if not rows: break
                for row in rows:
                    ts = int(row[0]); wanted = start + idx * sec
                    if ts != wanted:
                        bad_ts = {'expected': wanted, 'actual': ts}; break
                    arrays['ts'][idx] = ts
                    offset = 1
                    if full_ohlc:
                        for name in ('o', 'h', 'l', 'c'):
                            arrays[name][idx] = float(row[offset]); offset += 1
                    else:
                        arrays['c'][idx] = float(row[offset])
                    idx += 1
                _state(core, stage6_preflight=f'BUILDING_MARKET_CACHE_{tf}', market_cache_loaded=idx,
                       market_cache_expected=expected, market_cache_percent=round(idx / max(expected, 1) * 100.0, 2))
                if bad_ts: break
                time.sleep(0.001)
        finally:
            con.close()
        for arr in arrays.values(): arr.flush()
        arrays.clear(); gc.collect()
        if bad_ts or idx != expected:
            detail.update({'bad_ts': bad_ts, 'loaded': idx})
            return None, detail
        for name in names: os.replace(tmps[name], paths[name])
        _atomic_json(meta_path, {'key': key, **detail, 'expected_bars': expected, 'continuous': True, 'schema': SCHEMA})
    detail.update({'continuous': True, 'cache_key': key, 'cache_dir': str(root)})
    return root, detail


def _install_market_loader(core: Any, autonomous: Any) -> None:
    def load_market(c: Any) -> dict[str, Any]:
        with _CACHE_LOCK:
            _state(c, stage6_preflight='BUILDING_OR_LOADING_MARKET_CACHE')
            root5, d5 = _build_market_series(c, '5m', int(autonomous.RESEARCH_START_TS), int(autonomous.SETTLEMENT_END_EXCLUSIVE_TS), True)
            root15, d15 = _build_market_series(c, '15m', int(autonomous.RESEARCH_START_TS), int(autonomous.RESEARCH_END_EXCLUSIVE_TS), False)
            ready = bool(root5 is not None and root15 is not None and d5.get('continuous') and d15.get('continuous'))
            c.state['autonomous_market_cache_integrity_v40'] = {
                'schema': SCHEMA, 'runtime': VERSION, 'status': 'VALID' if ready else 'WAITING_REAL_CANONICAL_PRICE_WINDOW',
                'series': {'ETH:5m': d5, 'ETH:15m': d15}, 'sql_fixed_priority_canonical': True,
                'python_full_history_canonical_materialization': False, 'synthetic_gap_fill': False,
                'future_peeking': False, 'updated_at': int(time.time()),
            }
            if not ready:
                return {}
            ts5 = np.load(root5 / 'ts.npy', mmap_mode='r'); o5 = np.load(root5 / 'o.npy', mmap_mode='r')
            h5 = np.load(root5 / 'h.npy', mmap_mode='r'); l5 = np.load(root5 / 'l.npy', mmap_mode='r'); c5 = np.load(root5 / 'c.npy', mmap_mode='r')
            ts15 = np.load(root15 / 'ts.npy', mmap_mode='r'); c15 = np.load(root15 / 'c.npy', mmap_mode='r')
            _state(c, stage6_preflight='MARKET_CACHE_READY', market_5m_bars=len(ts5), market_15m_bars=len(ts15))
            return {'source5': 'canonical-sql-fixed-priority-v42', 'source15': 'canonical-sql-fixed-priority-v42',
                    'ts5': ts5, 'o5': o5, 'h5': h5, 'l5': l5, 'c5': c5, 'close15': _CloseLookup(ts15, c15)}
    autonomous._load_market = load_market


def _recent_canonical(core: Any, asset: str, tf: str, limit: int) -> list[dict[str, Any]]:
    sec = int(core.TIMEFRAME_SECONDS[tf]); lim = max(1, int(limit)); placeholders = ','.join('?' for _ in resilience.PRICE_PRIORITY)
    con = core.db()
    try:
        latest = con.execute(
            f'''SELECT MAX(ts) FROM market_bars WHERE asset=? AND tf=? AND source IN ({placeholders}) AND (ts % ?) = 0''',
            (asset, tf, *resilience.PRICE_PRIORITY, sec),
        ).fetchone()[0]
        if latest is None: return []
        start = int(latest) - (lim + 64) * sec
        select = ','.join(['ts'] + [_canonical_expr(col) + f' AS {col}' for col in ('o','h','l','c','v','qv')])
        rows = con.execute(
            f'''SELECT {select} FROM market_bars WHERE asset=? AND tf=? AND ts>=? AND source IN ({placeholders}) AND (ts % ?) = 0
                GROUP BY ts ORDER BY ts DESC LIMIT ?''',
            (asset, tf, start, *resilience.PRICE_PRIORITY, sec, lim),
        ).fetchall()
    finally:
        con.close()
    out = [{'ts': int(r[0]), 'o': float(r[1]), 'h': float(r[2]), 'l': float(r[3]), 'c': float(r[4]), 'v': float(r[5]), 'qv': float(r[6]), '_source': 'canonical-v42'} for r in reversed(rows)]
    return out


def _install_recent_canonical_loader(core: Any, autonomous: Any) -> None:
    original = core.load_bars
    def load_bars(asset: str, tf: str, source: str = 'gate', limit: int | None = None):
        if source == 'canonical' and limit is not None and int(limit) <= 5000 and _fast_replay_complete(core, autonomous):
            return _recent_canonical(core, asset, tf, int(limit))
        return original(asset, tf, source, limit)
    core.load_bars = load_bars


def _install_candidate_yield(core: Any, autonomous: Any) -> None:
    base_eval = autonomous._evaluate_candidate
    def controlled_eval(*args: Any, **kwargs: Any):
        started = time.monotonic()
        try:
            return base_eval(*args, **kwargs)
        finally:
            elapsed = time.monotonic() - started
            _state(core, stage6_compute='CANDIDATE_COMPLETE', last_candidate_seconds=round(elapsed, 3),
                   candidate_yield_ms=CANDIDATE_YIELD_MS)
            if CANDIDATE_YIELD_MS:
                time.sleep(CANDIDATE_YIELD_MS / 1000.0)
    autonomous._evaluate_candidate = controlled_eval


def install(production: Any, autonomous: Any, transition: Any, scheduler: Any) -> None:
    global _INSTALLED
    with _INSTALL_LOCK:
        if _INSTALLED: return
        _INSTALLED = True
    core = production.core

    # Freeze the finished replay before any post-replay wrapper gets another chance to
    # rescan all canonical history.  This alone removes the main dashboard/learning-tick
    # CPU loop after the 3.1M derived rows are committed.
    _freeze_completed_replay_view(core, autonomous)
    if _REPLAY_COMPLETE:
        try:
            resilience._CANON_CACHE.clear(); gc.collect()
        except Exception:
            pass

    _install_feature_loader(core, autonomous)
    _install_market_loader(core, autonomous)
    _install_recent_canonical_loader(core, autonomous)
    _install_candidate_yield(core, autonomous)

    authoritative_request = core.train_if_due

    # Replace V41's global kick implementation.  Existing V41 scan wrapper now calls
    # this resource-aware scheduler without another wrapper stack.
    def v42_kick(c: Any, a: Any, t: Any, *, source: str, force_interval: bool = False):
        return _scheduler_kick(c, a, t, authoritative_request, source=source, force_interval=force_interval)
    scheduler._kick = v42_kick

    # Critical CPU fix: after formal replay completion, legacy V15/V39/V40 learning
    # maintenance has no historical work left.  It was still running COUNT DISTINCT,
    # gap scans and canonical materialisation every ~3s.  Preserve that chain before
    # completion; after completion only the autonomous scheduler remains on this tick.
    previous_learning_tick = core.learning_tick
    async def post_replay_learning_tick() -> None:
        if _fast_replay_complete(core, autonomous):
            _scheduler_kick(core, autonomous, transition, authoritative_request, source='lean_learning_tick')
            core.state.setdefault('learning', {})['post_replay_maintenance'] = 'QUIESCED_V42_FIXED_REPLAY_IMMUTABLE'
            return
        await previous_learning_tick()
    core.learning_tick = post_replay_learning_tick

    # Reconcile the exact V41 failure mode immediately: persisted QUEUED/RUNNING is not
    # active unless the actual V26 Future is still running.
    transition_state = _reconcile_transition(core, autonomous, transition)
    _state(core, installed=True, replay_complete=_REPLAY_COMPLETE,
           transition_status=transition_state.get('status'),
           post_replay_legacy_learning_quiesced=True,
           persistent_feature_memmap=True, persistent_market_memmap=True,
           canonical_recent_sql_loader=True,
           full_history_python_canonical_cache_released=bool(_REPLAY_COMPLETE),
           native_thread_budget=os.getenv('OMP_NUM_THREADS', '1'))
    try:
        _scheduler_kick(core, autonomous, transition, authoritative_request, source='v42_boot', force_interval=True)
    except Exception as exc:
        _state(core, boot_kick_error=f'{type(exc).__name__}: {exc}')

    core.state.setdefault('strict_replay', {})['post_replay_resource_authority_v42'] = {
        'fixed_replay_mutated': False, 'learning_samples_reset': False, 'raw_market_reset': False,
        'oos_rules_changed': False, 'fitness_changed': False, 'future_peeking': False,
        'post_replay_gap_watchdog_polling_quiesced': True,
        'completed_replay_dashboard_rescan_forbidden': True,
        'stale_background_queue_uses_future_truth': True,
        'autonomous_cache_streamed_and_persistent': True,
    }

    if not any(getattr(r, 'path', None) == '/api/v42/resource-authority' for r in core.app.router.routes):
        @core.app.get('/api/v42/resource-authority')
        def resource_authority_status() -> dict[str, Any]:
            trans = _reconcile_transition(core, autonomous, transition)
            active, done, future_error = _future_snapshot(transition)
            return {
                'runtime': VERSION, 'schema': SCHEMA,
                'state': dict(core.state.get(STATE_KEY) or {}),
                'replay_complete': _fast_replay_complete(core, autonomous),
                'transition': trans,
                'background_future': {'active': active, 'done_or_absent': done, 'error': future_error},
                'autonomous_state': dict(core.state.get(getattr(autonomous, 'STATE_KEY', 'v30_autonomous_strategy_discovery')) or {}),
                'feature_integrity': dict(core.state.get('v35_autonomous_feature_integrity') or {}),
                'market_cache_integrity': dict(core.state.get('autonomous_market_cache_integrity_v40') or {}),
                'rules': dict(core.state.get('strict_replay', {}).get('post_replay_resource_authority_v42') or {}),
            }
