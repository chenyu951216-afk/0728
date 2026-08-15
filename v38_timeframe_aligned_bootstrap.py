from __future__ import annotations

import time
from typing import Any

import v22_hierarchical_pipeline as hierarchical

VERSION = 'V38_TIMEFRAME_ALIGNED_BOOTSTRAP'


def _aligned_series_window(core: Any, tf: str) -> tuple[int, int, int, int]:
    """Return (sec, requested_start, first_valid_open, frozen_target_end).

    The autonomous research start is expressed in Taipei wall-clock midnight.  Raw
    exchange candles, however, are timestamped on UTC/epoch timeframe boundaries.
    For example production START_TS=1577808000 is 2019-12-31 16:00 UTC, while a
    canonical 1d candle opens at 00:00 UTC.  A strict collector must therefore ask
    for the first *valid candle open on or after* the requested start rather than an
    impossible off-grid timestamp.
    """
    sec = int(core.TIMEFRAME_SECONDS[tf])
    requested_start = int(core.START_TS)
    first_valid_open = ((requested_start + sec - 1) // sec) * sec
    cutoff = int(hierarchical._collection_cutoff(core))
    target_end = (cutoff // sec) * sec - sec
    return sec, requested_start, first_valid_open, target_end


def _series_progress_aligned(core: Any, asset: str, tf: str) -> dict[str, Any]:
    sec, requested_start, start, target_end = _aligned_series_window(core, tf)
    expected = max(0, (target_end - start) // sec + 1) if target_end >= start else 0
    placeholders = ','.join('?' for _ in hierarchical.resilience.PRICE_PRIORITY)
    con = core.db()
    try:
        if expected <= 0:
            row = (0, None, None)
        else:
            row = con.execute(
                f'''SELECT COUNT(DISTINCT ts),MIN(ts),MAX(ts) FROM market_bars
                    WHERE asset=? AND tf=? AND ts BETWEEN ? AND ?
                      AND source IN ({placeholders})''',
                (asset, tf, start, target_end, *hierarchical.resilience.PRICE_PRIORITY),
            ).fetchone()
    finally:
        con.close()

    unique = int(row[0] or 0) if row else 0
    earliest = int(row[1]) if row and row[1] is not None else None
    latest = int(row[2]) if row and row[2] is not None else None
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
        'timestamp_grid_contract': 'first valid epoch-aligned candle open on/after requested research start',
    }
    if expected <= 0 or not unique or earliest is None or latest is None:
        return {
            **base,
            'percent': 0.0,
            'bars': 0,
            'from': None,
            'to': None,
            'gaps_estimate': expected,
            'density': 0.0,
            'start_ready': False,
            'tail_ready': False,
            'coverage_ready': False,
            'history_ready': False,
        }

    raw_percent = unique / max(expected, 1) * 100.0
    percent = hierarchical._pct(raw_percent)
    missing_bars = max(0, expected - unique)
    start_ready = earliest <= start + hierarchical.PRICE_START_TOLERANCE_BARS * sec
    tail_ready = latest >= target_end - hierarchical.PRICE_TAIL_TOLERANCE_BARS * sec
    coverage_ready = (
        raw_percent >= hierarchical.PRICE_MIN_COVERAGE_PCT
        and missing_bars <= hierarchical.PRICE_MAX_MISSING_BARS
    )
    history_ready = bool(start_ready and tail_ready and coverage_ready)
    return {
        **base,
        'percent': percent,
        'bars': unique,
        'from': earliest,
        'to': latest,
        'gaps_estimate': missing_bars,
        'density': round(unique / max(expected, 1), 6),
        'start_ready': start_ready,
        'tail_ready': tail_ready,
        'coverage_ready': coverage_ready,
        'history_ready': history_ready,
    }


def _first_collection_gap_aligned(core: Any) -> dict[str, Any] | None:
    placeholders = ','.join('?' for _ in hierarchical.resilience.PRICE_PRIORITY)
    for _group, specs in hierarchical.PRICE_GROUPS:
        for asset, tf in specs:
            progress = _series_progress_aligned(core, asset, tf)
            # Never let a series that already satisfies the strict frozen-horizon
            # contract steal the repair cursor because of an impossible off-grid
            # requested timestamp.
            if bool(progress.get('history_ready')):
                continue

            sec = int(core.TIMEFRAME_SECONDS[tf])
            requested_start = int(progress['requested_from'])
            start = int(progress['target_from'])
            target_end = int(progress['target_to'])
            if target_end < start:
                continue

            con = core.db()
            try:
                first_last = con.execute(
                    f'''SELECT MIN(ts),MAX(ts) FROM market_bars
                        WHERE asset=? AND tf=? AND ts BETWEEN ? AND ?
                          AND source IN ({placeholders})''',
                    (asset, tf, start, target_end, *hierarchical.resilience.PRICE_PRIORITY),
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
                                  AND source IN ({placeholders})
                            ), ordered AS (
                                SELECT ts,LAG(ts) OVER (ORDER BY ts) AS previous_ts FROM unique_ts
                            )
                            SELECT previous_ts+? FROM ordered
                            WHERE previous_ts IS NOT NULL AND ts-previous_ts>?
                            ORDER BY ts LIMIT 1''',
                        (asset, tf, start, target_end, *hierarchical.resilience.PRICE_PRIORITY, sec, sec),
                    ).fetchone()
                    missing = int(row[0]) if row and row[0] is not None else (
                        latest + sec if latest is not None and latest < target_end else None
                    )
            finally:
                con.close()

            if missing is not None and missing <= target_end:
                return {
                    'asset': asset,
                    'timeframe': tf,
                    'missing_ts': int(missing),
                    'requested_from': requested_start,
                    'target_from': start,
                    'target_to': target_end,
                    'alignment_shift_seconds': max(0, start - requested_start),
                    'timestamp_grid_seconds': sec,
                }
    return None


def _diagnostics(core: Any) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    for group, specs in hierarchical.PRICE_GROUPS:
        for asset, tf in specs:
            item = _series_progress_aligned(core, asset, tf)
            rows.append({
                'group': group,
                'asset': asset,
                'timeframe': tf,
                'requested_from': item.get('requested_from'),
                'target_from': item.get('target_from'),
                'alignment_shift_seconds': item.get('alignment_shift_seconds'),
                'percent': item.get('percent'),
                'bars': item.get('bars'),
                'expected_bars': item.get('expected_bars'),
                'history_ready': item.get('history_ready'),
            })
    return {
        'runtime': VERSION,
        'requested_research_start_ts': int(core.START_TS),
        'series': rows,
        'next_real_gap': _first_collection_gap_aligned(core),
        'phantom_off_grid_start_gap_forbidden': True,
        'strict_coverage_requirement_unchanged': True,
        'no_interpolation': True,
        'updated_at': int(time.time()),
    }


def install(core: Any) -> None:
    if getattr(hierarchical, '_v38_timeframe_alignment_installed', False):
        return

    hierarchical._series_progress = _series_progress_aligned
    hierarchical._first_collection_gap = _first_collection_gap_aligned
    hierarchical._v38_timeframe_alignment_installed = True

    state = _diagnostics(core)
    core.state['price_time_alignment_v38'] = state
    core.state.setdefault('strict_replay', {})['timeframe_alignment_v38'] = {
        'runtime': VERSION,
        'requested_research_start_ts': int(core.START_TS),
        'first_valid_candle_open_is_epoch_aligned': True,
        'phantom_off_grid_start_gap_forbidden': True,
        'strict_coverage_requirement_unchanged': True,
        'future_peeking': False,
        'synthetic_gap_fill': False,
    }

    if not any(getattr(route, 'path', None) == '/api/v38/time-alignment' for route in core.app.router.routes):
        @core.app.get('/api/v38/time-alignment')
        def timeframe_alignment_status() -> dict[str, Any]:
            status = _diagnostics(core)
            core.state['price_time_alignment_v38'] = status
            return status
