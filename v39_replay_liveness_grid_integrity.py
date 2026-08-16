from __future__ import annotations

import asyncio
import bisect
import os
import time
from typing import Any

import v5_runtime
import v9_final
import v15_data_resilience as resilience
import v16_runtime_integrity as runtime_integrity
import v22_hierarchical_pipeline as hierarchical
import v38_timeframe_aligned_bootstrap as alignment


VERSION = 'V39_REPLAY_LIVENESS_GRID_INTEGRITY'
STATE_KEY = 'v39_replay_liveness_grid_integrity'
STALL_SECONDS = max(45, min(900, int(os.getenv('REPLAY_LIVENESS_STALL_SECONDS', '120'))))
RESUME_BATCH = max(16, min(256, int(os.getenv('REPLAY_LIVENESS_RESUME_BATCH', '64'))))
FORCE_COOLDOWN_SECONDS = max(30, min(600, int(os.getenv('REPLAY_LIVENESS_FORCE_COOLDOWN_SECONDS', '90'))))


def _grid_aligned(ts: int, sec: int) -> bool:
    return int(ts) % max(1, int(sec)) == 0


def _canonical_bars_grid_safe(core: Any, asset: str, tf: str) -> list[dict[str, Any]]:
    """Canonical price series containing only real exchange timeframe-grid opens.

    A stray/off-grid provider timestamp must never be allowed to sit between two
    valid candles and make strict continuity look broken forever.  We retain raw
    rows in SQLite for audit, but only epoch-aligned candle opens are model/replay
    eligible.
    """
    sec = int(core.TIMEFRAME_SECONDS[tf])
    placeholders = ','.join('?' for _ in resilience.PRICE_PRIORITY)
    con = core.db()
    try:
        sig = con.execute(
            f'''SELECT COUNT(*),COALESCE(MAX(ts),0),COALESCE(MIN(ts),0)
                FROM market_bars
                WHERE asset=? AND tf=? AND source IN ({placeholders})
                  AND (ts % ?) = 0''',
            (asset, tf, *resilience.PRICE_PRIORITY, sec),
        ).fetchone()
        key = (id(core), asset, tf, int(sig[0] or 0), int(sig[1] or 0), int(sig[2] or 0), sec)
        cache = resilience._CANON_CACHE
        if key in cache:
            return cache[key]
        rows = con.execute(
            f'''SELECT source,ts,o,h,l,c,v,qv FROM market_bars
                WHERE asset=? AND tf=? AND source IN ({placeholders})
                  AND (ts % ?) = 0
                ORDER BY ts''',
            (asset, tf, *resilience.PRICE_PRIORITY, sec),
        ).fetchall()
    finally:
        con.close()

    rank = {source: i for i, source in enumerate(resilience.PRICE_PRIORITY)}
    by_ts: dict[int, tuple[int, dict[str, Any]]] = {}
    for row in rows:
        source = str(row['source'])
        ts = int(row['ts'])
        priority = rank.get(source, 999)
        old = by_ts.get(ts)
        if old is None or priority < old[0]:
            by_ts[ts] = (
                priority,
                {
                    'ts': ts,
                    'o': float(row['o']), 'h': float(row['h']),
                    'l': float(row['l']), 'c': float(row['c']),
                    'v': float(row['v']), 'qv': float(row['qv']),
                    '_source': source,
                },
            )
    out = [by_ts[ts][1] for ts in sorted(by_ts)]
    for old_key in list(resilience._CANON_CACHE):
        if len(old_key) >= 3 and old_key[:3] == (id(core), asset, tf) and old_key != key:
            resilience._CANON_CACHE.pop(old_key, None)
    resilience._CANON_CACHE[key] = out
    return out


def _series_progress_grid_safe(core: Any, asset: str, tf: str) -> dict[str, Any]:
    sec, requested_start, start, target_end = alignment._aligned_series_window(core, tf)
    expected = max(0, (target_end - start) // sec + 1) if target_end >= start else 0
    placeholders = ','.join('?' for _ in resilience.PRICE_PRIORITY)
    con = core.db()
    try:
        if expected <= 0:
            row = (0, None, None, 0)
        else:
            row = con.execute(
                f'''SELECT
                        COUNT(DISTINCT CASE WHEN (ts % ?) = 0 THEN ts END),
                        MIN(CASE WHEN (ts % ?) = 0 THEN ts END),
                        MAX(CASE WHEN (ts % ?) = 0 THEN ts END),
                        COUNT(DISTINCT CASE WHEN (ts % ?) != 0 THEN ts END)
                    FROM market_bars
                    WHERE asset=? AND tf=? AND ts BETWEEN ? AND ?
                      AND source IN ({placeholders})''',
                (sec, sec, sec, sec, asset, tf, start, target_end, *resilience.PRICE_PRIORITY),
            ).fetchone()
    finally:
        con.close()

    unique = int(row[0] or 0) if row else 0
    earliest = int(row[1]) if row and row[1] is not None else None
    latest = int(row[2]) if row and row[2] is not None else None
    off_grid = int(row[3] or 0) if row else 0
    base = {
        'asset': asset,
        'timeframe': tf,
        'requested_from': requested_start,
        'target_from': start,
        'target_to': target_end,
        'alignment_shift_seconds': max(0, start - requested_start),
        'expected_bars': expected,
        'required_coverage_pct': hierarchical.PRICE_MIN_COVERAGE_PCT,
        'maximum_missing_bars_before_replay': hierarchical.PRICE_MAX_MISSING_BARS,
        'timestamp_grid_seconds': sec,
        'off_grid_distinct_timestamps_ignored': off_grid,
        'timestamp_grid_contract': 'only epoch-aligned candle opens are eligible for coverage/canonical replay',
    }
    if expected <= 0 or not unique or earliest is None or latest is None:
        return {
            **base,
            'percent': 0.0, 'bars': 0, 'from': None, 'to': None,
            'gaps_estimate': expected, 'density': 0.0,
            'start_ready': False, 'tail_ready': False,
            'coverage_ready': False, 'history_ready': False,
        }

    raw_percent = unique / max(expected, 1) * 100.0
    missing_bars = max(0, expected - unique)
    start_ready = earliest <= start + hierarchical.PRICE_START_TOLERANCE_BARS * sec
    tail_ready = latest >= target_end - hierarchical.PRICE_TAIL_TOLERANCE_BARS * sec
    coverage_ready = bool(
        raw_percent >= hierarchical.PRICE_MIN_COVERAGE_PCT
        and missing_bars <= hierarchical.PRICE_MAX_MISSING_BARS
    )
    return {
        **base,
        'percent': hierarchical._pct(raw_percent),
        'bars': unique, 'from': earliest, 'to': latest,
        'gaps_estimate': missing_bars,
        'density': round(unique / max(expected, 1), 8),
        'start_ready': start_ready, 'tail_ready': tail_ready,
        'coverage_ready': coverage_ready,
        'history_ready': bool(start_ready and tail_ready and coverage_ready),
    }


def _first_collection_gap_grid_safe(core: Any) -> dict[str, Any] | None:
    placeholders = ','.join('?' for _ in resilience.PRICE_PRIORITY)
    for _group, specs in hierarchical.PRICE_GROUPS:
        for asset, tf in specs:
            progress = _series_progress_grid_safe(core, asset, tf)
            if bool(progress.get('history_ready')):
                continue
            sec = int(core.TIMEFRAME_SECONDS[tf])
            start = int(progress['target_from'])
            target_end = int(progress['target_to'])
            if target_end < start:
                continue

            con = core.db()
            try:
                first_last = con.execute(
                    f'''SELECT MIN(ts),MAX(ts) FROM market_bars
                        WHERE asset=? AND tf=? AND ts BETWEEN ? AND ?
                          AND source IN ({placeholders}) AND (ts % ?) = 0''',
                    (asset, tf, start, target_end, *resilience.PRICE_PRIORITY, sec),
                ).fetchone()
                earliest = int(first_last[0]) if first_last and first_last[0] is not None else None
                latest = int(first_last[1]) if first_last and first_last[1] is not None else None
                if earliest is None or earliest > start:
                    missing = start
                else:
                    row = con.execute(
                        f'''WITH unique_ts AS (
                                SELECT DISTINCT ts FROM market_bars
                                WHERE asset=? AND tf=? AND ts BETWEEN ? AND ?
                                  AND source IN ({placeholders}) AND (ts % ?) = 0
                            ), ordered AS (
                                SELECT ts,LAG(ts) OVER (ORDER BY ts) AS previous_ts FROM unique_ts
                            )
                            SELECT previous_ts+? FROM ordered
                            WHERE previous_ts IS NOT NULL AND ts-previous_ts>?
                            ORDER BY ts LIMIT 1''',
                        (asset, tf, start, target_end, *resilience.PRICE_PRIORITY, sec, sec, sec),
                    ).fetchone()
                    missing = int(row[0]) if row and row[0] is not None else (
                        latest + sec if latest is not None and latest < target_end else None
                    )
            finally:
                con.close()
            if missing is not None and missing <= target_end:
                return {
                    'asset': asset, 'timeframe': tf, 'missing_ts': int(missing),
                    'requested_from': int(progress['requested_from']),
                    'target_from': start, 'target_to': target_end,
                    'alignment_shift_seconds': int(progress.get('alignment_shift_seconds') or 0),
                    'timestamp_grid_seconds': sec,
                    'off_grid_rows_are_not_gaps': True,
                }
    return None


def _exact_real_bar_exists(core: Any, asset: str, tf: str, ts: int) -> bool:
    sec = int(core.TIMEFRAME_SECONDS[tf])
    if not _grid_aligned(ts, sec):
        return False
    placeholders = ','.join('?' for _ in resilience.PRICE_PRIORITY)
    con = core.db()
    try:
        row = con.execute(
            f'''SELECT 1 FROM market_bars
                WHERE asset=? AND tf=? AND ts=? AND source IN ({placeholders})
                  AND (ts % ?) = 0 LIMIT 1''',
            (asset, tf, int(ts), *resilience.PRICE_PRIORITY, sec),
        ).fetchone()
        return bool(row)
    finally:
        con.close()


def _reconcile_gap_registry(core: Any) -> dict[str, Any]:
    state = resilience._gaps(core)
    gaps = state.setdefault('gaps', {})
    recovered: list[str] = []
    for gid, raw in list(gaps.items()):
        rec = dict(raw or {})
        if str(rec.get('status') or '') not in ('PENDING_REPAIR', 'QUARANTINED_UNRECOVERABLE'):
            continue
        asset = str(rec.get('asset') or '')
        tf = str(rec.get('tf') or '')
        ts = int(rec.get('missing_ts') or 0)
        if not asset or tf not in core.TIMEFRAME_SECONDS or ts <= 0:
            continue
        if _exact_real_bar_exists(core, asset, tf, ts):
            previous = str(rec.get('status') or '')
            rec.update({
                'status': 'REPAIRED',
                'repaired_at': int(time.time()),
                'reconciled_by': VERSION,
                'previous_status': previous,
                'rule': 'real grid-aligned candle now exists; stale repair/quarantine state cannot block replay/certification',
            })
            gaps[gid] = rec
            recovered.append(gid)
    if recovered:
        resilience._save_gaps(core, state)
    return {'recovered': recovered, 'count': len(recovered)}


def _blocker_stale(core: Any, blocker: dict[str, Any], gate: dict[str, Any], current_gap: dict[str, Any] | None) -> bool:
    if not blocker or not blocker.get('blocked'):
        return False
    state = str(blocker.get('state') or '').upper()
    reason = str(blocker.get('reason') or '').lower()
    if gate.get('ready') and (
        state == 'WAITING_FOR_FULL_HISTORY'
        or 'full_history' in state
        or 'full history' in reason
        or 'every required price timeframe' in reason
    ):
        return True
    if current_gap is None and (
        state == 'BLOCK_PRICE_GAP'
        or 'continuity gap' in reason
        or 'price gap' in reason
    ):
        return True
    if current_gap is None and state == 'BLOCK_FUTURE_PATH':
        frontier = runtime_integrity._legal_frontier(core)
        at_ts = int(blocker.get('at_ts') or 0)
        legal = int(frontier.get('legal_frontier_ts') or 0)
        return bool(frontier.get('ready') and at_ts > 0 and at_ts <= legal)
    return False


def _normalize_authority_state(core: Any, current_gap: dict[str, Any] | None = None) -> dict[str, Any]:
    gate = hierarchical.price_collection_gate(core)
    current_gap = resilience.detect_gap_near_cursor(core) if current_gap is None else current_gap
    lr = core.state.setdefault('learning', {})
    cleared: list[str] = []

    local_blocker = dict(lr.get('replay_price_blocker') or {})
    strict_blocker = dict(core.state.get('strict_replay_gap_blocker') or {})
    if _blocker_stale(core, local_blocker, gate, current_gap):
        cleared.append('learning.replay_price_blocker')
    if _blocker_stale(core, strict_blocker, gate, current_gap):
        cleared.append('strict_replay_gap_blocker')

    if cleared:
        cleared_state = {
            'blocked': False,
            'cleared_at': int(time.time()),
            'cleared_by': VERSION,
            'reason': 'authoritative price/full-history recheck proved the previous blocker is no longer true',
        }
        lr['replay_price_blocker'] = dict(cleared_state)
        core.state['strict_replay_gap_blocker'] = dict(cleared_state)
        lr['blocker'] = None
        lr['replay_blocker_cleared_v39'] = {
            'at': int(time.time()), 'cleared': cleared,
            'previous_learning_blocker': local_blocker,
            'previous_strict_blocker': strict_blocker,
        }

    replay = runtime_integrity.replay_progress(core)
    if gate.get('ready') and not current_gap and not replay.get('complete'):
        if str(lr.get('phase') or '') in (
            'WAITING_FOR_FULL_HISTORY', 'COLLECTING_FULL_HISTORY_BEFORE_REPLAY',
            'WAITING_PRICE_GAP_REPAIR', 'PRICE_GAP_REPAIRED_RESUMING',
        ):
            lr['phase'] = 'STRICT_REPLAY_ADVANCING'
        final_state = core.get_state('v18_final_system_state', {})
        final_state = dict(final_state) if isinstance(final_state, dict) else {}
        if str(final_state.get('status') or '') == 'WAITING_FOR_FULL_HISTORY':
            final_state.update({
                'status': 'STRICT_REPLAY_ADVANCING',
                'reason': 'full price history contract is complete; point-in-time replay is advancing',
                'updated_at': int(time.time()),
            })
            core.set_state('v18_final_system_state', final_state)

    watermark = dict(lr.get('derivative_replay_watermark') or {})
    if watermark.get('blocked'):
        cursor = int(core.get_state(v5_runtime.REPLAY_STATE_KEY, core.START_TS) or core.START_TS)
        ready = resilience.ready_through(core)
        stride = int(v9_final.REPLAY_STRIDE_BARS) * 900
        if ready is None or int(ready) - cursor - 2 * stride > 0:
            watermark.update({
                'blocked': False,
                'cleared_at': int(time.time()),
                'cleared_by': VERSION,
                'reason': 'authoritative derivative watermark now has room for the next causal decision',
            })
            lr['derivative_replay_watermark'] = watermark
            cleared.append('derivative_replay_watermark')

    return {
        'gate': gate,
        'replay': replay,
        'current_gap': current_gap,
        'cleared': cleared,
        'learning_blocker': dict(lr.get('replay_price_blocker') or {}),
        'strict_blocker': dict(core.state.get('strict_replay_gap_blocker') or {}),
        'derivative_watermark': dict(lr.get('derivative_replay_watermark') or {}),
    }


def _resume_eligible(status: dict[str, Any]) -> bool:
    return bool(
        (status.get('gate') or {}).get('ready')
        and not (status.get('replay') or {}).get('complete')
        and status.get('current_gap') is None
        and not (status.get('learning_blocker') or {}).get('blocked')
        and not (status.get('strict_blocker') or {}).get('blocked')
        and not (status.get('derivative_watermark') or {}).get('blocked')
    )


def _persist_watch(core: Any, patch: dict[str, Any]) -> dict[str, Any]:
    raw = core.get_state(STATE_KEY, {})
    state = dict(raw) if isinstance(raw, dict) else {}
    state.update(patch)
    state['runtime'] = VERSION
    state['updated_at'] = int(time.time())
    core.set_state(STATE_KEY, state)
    core.state['replay_liveness_v39'] = state
    return state


def _watch_status(core: Any, authority: dict[str, Any]) -> dict[str, Any]:
    now = int(time.time())
    cursor = int(core.get_state(v5_runtime.REPLAY_STATE_KEY, core.START_TS) or core.START_TS)
    raw = core.get_state(STATE_KEY, {})
    old = dict(raw) if isinstance(raw, dict) else {}
    previous_cursor = int(old.get('cursor_ts') or cursor)
    if cursor != previous_cursor:
        last_advanced_at = now
        stalled_ticks = 0
    else:
        last_advanced_at = int(old.get('last_advanced_at') or now)
        stalled_ticks = int(old.get('stalled_ticks') or 0) + 1
    stalled_seconds = max(0, now - last_advanced_at)
    eligible = _resume_eligible(authority)
    last_forced_at = int(old.get('last_forced_at') or 0)
    force_due = bool(
        eligible
        and stalled_seconds >= STALL_SECONDS
        and now - last_forced_at >= FORCE_COOLDOWN_SECONDS
    )
    return _persist_watch(core, {
        'cursor_ts': cursor,
        'last_advanced_at': last_advanced_at,
        'stalled_seconds': stalled_seconds,
        'stalled_ticks': stalled_ticks,
        'resume_eligible': eligible,
        'force_due': force_due,
        'last_forced_at': last_forced_at,
        'current_gap': authority.get('current_gap'),
        'cleared_blockers': list(authority.get('cleared') or []),
    })


def _grid_diagnostics(core: Any) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for group, specs in hierarchical.PRICE_GROUPS:
        for asset, tf in specs:
            p = _series_progress_grid_safe(core, asset, tf)
            rows.append({
                'group': group, 'asset': asset, 'timeframe': tf,
                'percent': p.get('percent'), 'bars': p.get('bars'),
                'expected_bars': p.get('expected_bars'),
                'missing_bars': p.get('gaps_estimate'),
                'off_grid_distinct_timestamps_ignored': p.get('off_grid_distinct_timestamps_ignored'),
                'history_ready': p.get('history_ready'),
                'target_from': p.get('target_from'), 'target_to': p.get('target_to'),
            })
    return rows


def install(core: Any) -> None:
    if getattr(hierarchical, '_v39_replay_liveness_installed', False):
        return

    # Correct the two source-of-truth readers first.  Raw off-grid rows remain in
    # SQLite for audit but can no longer poison coverage or replay continuity.
    resilience._CANON_CACHE.clear()
    resilience.canonical_bars = _canonical_bars_grid_safe
    hierarchical._series_progress = _series_progress_grid_safe
    hierarchical._first_collection_gap = _first_collection_gap_grid_safe
    hierarchical._v39_replay_liveness_installed = True

    original_learning = core.learning_tick

    async def liveness_learning_tick() -> None:
        before_cursor = int(core.get_state(v5_runtime.REPLAY_STATE_KEY, core.START_TS) or core.START_TS)
        await original_learning()
        registry = _reconcile_gap_registry(core)
        current_gap = resilience.detect_gap_near_cursor(core)
        authority = _normalize_authority_state(core, current_gap=current_gap)
        watch = _watch_status(core, authority)

        # A just-repaired stale blocker should resume in the same scheduler tick.
        # A genuinely unexplained stall gets one small bounded kick after the
        # watchdog threshold.  Real gaps/full-history/derivative blockers remain
        # fail-closed and can never be bypassed.
        immediate_resume = bool(authority.get('cleared')) and _resume_eligible(authority)
        force_due = bool(watch.get('force_due'))
        generated = 0
        attempted = False
        if immediate_resume or force_due:
            attempted = True
            try:
                generated = int(await asyncio.to_thread(core.generate_learning_samples, RESUME_BATCH) or 0)
            except Exception as exc:
                _persist_watch(core, {
                    'last_force_error': f'{type(exc).__name__}: {exc}',
                    'last_force_error_at': int(time.time()),
                })
            else:
                after_force = int(core.get_state(v5_runtime.REPLAY_STATE_KEY, core.START_TS) or core.START_TS)
                patch = {
                    'last_forced_at': int(time.time()),
                    'last_force_reason': 'stale_blocker_cleared' if immediate_resume else 'stall_watchdog',
                    'last_force_generated_samples': generated,
                    'last_force_cursor_before': before_cursor,
                    'last_force_cursor_after': after_force,
                    'last_force_error': None,
                }
                if after_force > before_cursor:
                    patch['last_advanced_at'] = int(time.time())
                    patch['stalled_seconds'] = 0
                    patch['stalled_ticks'] = 0
                _persist_watch(core, patch)

        lr = core.state.setdefault('learning', {})
        lr['replay_liveness_v39'] = {
            **dict(core.state.get('replay_liveness_v39') or {}),
            'registry_reconciliation': registry,
            'resume_attempted_this_tick': attempted,
            'resume_generated_this_tick': generated,
        }

    core.learning_tick = liveness_learning_tick

    core.state.setdefault('strict_replay', {})['replay_liveness_v39'] = {
        'runtime': VERSION,
        'off_grid_raw_rows_are_audit_only': True,
        'off_grid_rows_can_poison_canonical_continuity': False,
        'coverage_counts_only_valid_timeframe_grid_opens': True,
        'resolved_full_history_blocker_can_persist': False,
        'resolved_price_gap_blocker_can_persist': False,
        'stale_derivative_watermark_can_persist': False,
        'stalled_replay_watchdog_seconds': STALL_SECONDS,
        'bounded_resume_batch': RESUME_BATCH,
        'real_gap_can_be_bypassed': False,
        'future_path_can_be_bypassed': False,
        'synthetic_gap_fill': False,
        'future_peeking': False,
    }

    # Normalize stale persisted state immediately at boot; the learning loop will
    # perform the same check after every subsequent tick.
    try:
        _reconcile_gap_registry(core)
        _normalize_authority_state(core)
    except Exception as exc:
        _persist_watch(core, {'boot_reconciliation_error': f'{type(exc).__name__}: {exc}'})

    if not any(getattr(route, 'path', None) == '/api/v39/liveness' for route in core.app.router.routes):
        @core.app.get('/api/v39/liveness')
        def liveness_status() -> dict[str, Any]:
            current_gap = resilience.detect_gap_near_cursor(core)
            authority = _normalize_authority_state(core, current_gap=current_gap)
            return {
                'runtime': VERSION,
                'watch': dict(core.state.get('replay_liveness_v39') or core.get_state(STATE_KEY, {}) or {}),
                'authority': authority,
                'gap_registry': resilience._gap_summary(core),
                'grid_series': _grid_diagnostics(core),
                'next_collection_gap': _first_collection_gap_grid_safe(core),
                'rules': core.state.get('strict_replay', {}).get('replay_liveness_v39', {}),
            }
