from __future__ import annotations

"""Final replay -> autonomous research handoff integrity.

This overlay fixes a production-only deadlock that appears after Strict Replay reaches
100%: V30 asks the deterministic price selector for a source name, receives the
virtual source ``canonical``, and then queries ``market_bars WHERE source='canonical'``.
Canonical is intentionally not a physical SQLite source, so the query returns zero
rows and autonomous research remains in WAITING_MARKET_CACHE forever.

V40 makes the already-audited grid-safe canonical series authoritative for autonomous
trade-path simulation, validates the complete fixed research/settlement windows, and
clears only terminal future-path blockers that belong *after* a replay that is already
formally complete. No raw data is deleted, no gap is interpolated, and no future price
is exposed to a historical decision before its plan is frozen.
"""

import time
from typing import Any

import numpy as np

import v15_data_resilience as resilience
import v16_runtime_integrity as runtime_integrity
import v39_replay_liveness_grid_integrity as replay_liveness


VERSION = 'V40_AUTONOMOUS_HANDOFF_INTEGRITY'
SCHEMA = 40
STATE_KEY = 'v40_autonomous_handoff_integrity'
MARKET_STATE_KEY = 'autonomous_market_cache_integrity_v40'


def _window_rows(core: Any, asset: str, tf: str, start_ts: int, end_exclusive_ts: int) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    sec = int(core.TIMEFRAME_SECONDS[tf])
    start = ((int(start_ts) + sec - 1) // sec) * sec
    end = (int(end_exclusive_ts) // sec) * sec
    expected = max(0, (end - start) // sec)
    canonical = resilience.canonical_bars(core, asset, tf)
    selected: list[dict[str, Any]] = []
    for row in canonical:
        ts = int(row['ts'])
        if ts < start:
            continue
        if ts >= end:
            break
        if ts % sec == 0:
            selected.append(row)

    first_gap = None
    if expected > 0:
        if not selected or int(selected[0]['ts']) != start:
            first_gap = start
        else:
            previous = int(selected[0]['ts'])
            for row in selected[1:]:
                ts = int(row['ts'])
                if ts - previous != sec:
                    first_gap = previous + sec
                    break
                previous = ts
            if first_gap is None and (len(selected) != expected or int(selected[-1]['ts']) != end - sec):
                first_gap = int(selected[-1]['ts']) + sec if selected else start

    diag = {
        'asset': asset,
        'timeframe': tf,
        'requested_start_ts': int(start_ts),
        'start_ts': start,
        'end_exclusive_ts': end,
        'expected_bars': expected,
        'actual_bars': len(selected),
        'first_ts': int(selected[0]['ts']) if selected else None,
        'last_ts': int(selected[-1]['ts']) if selected else None,
        'first_gap_ts': first_gap,
        'continuous': bool(expected > 0 and first_gap is None and len(selected) == expected),
        'source_semantics': 'grid-safe fixed-priority canonical virtual series',
        'physical_sql_source_named_canonical_required': False,
        'off_grid_rows_eligible': False,
    }
    return selected, diag


def _load_market_grid_safe(core: Any, autonomous: Any) -> dict[str, Any]:
    rows5, d5 = _window_rows(
        core, 'ETH', '5m',
        int(autonomous.RESEARCH_START_TS),
        int(autonomous.SETTLEMENT_END_EXCLUSIVE_TS),
    )
    rows15, d15 = _window_rows(
        core, 'ETH', '15m',
        int(autonomous.RESEARCH_START_TS),
        int(autonomous.RESEARCH_END_EXCLUSIVE_TS),
    )
    ready = bool(d5['continuous'] and d15['continuous'])
    diag = {
        'schema': SCHEMA,
        'runtime': VERSION,
        'status': 'VALID' if ready else 'WAITING_REAL_CANONICAL_PRICE_WINDOW',
        'research_start_ts': int(autonomous.RESEARCH_START_TS),
        'research_end_exclusive_ts': int(autonomous.RESEARCH_END_EXCLUSIVE_TS),
        'settlement_end_exclusive_ts': int(autonomous.SETTLEMENT_END_EXCLUSIVE_TS),
        'series': {'ETH:5m': d5, 'ETH:15m': d15},
        'virtual_canonical_sql_deadlock_fixed': True,
        'canonical_rows_are_read_through_resilience_authority': True,
        'complete_evolved_holding_horizon_required': True,
        'synthetic_gap_fill': False,
        'future_peeking': False,
        'updated_at': int(time.time()),
    }
    core.state[MARKET_STATE_KEY] = diag
    lr = core.state.setdefault('learning', {})
    if not ready:
        gap = d5.get('first_gap_ts') or d15.get('first_gap_ts')
        lr['phase'] = 'WAITING_AUTONOMOUS_MARKET_CACHE_INTEGRITY'
        lr['blocker'] = f'autonomous canonical market window incomplete at {gap}' if gap else 'autonomous canonical market window incomplete'
        return {}

    lr['phase'] = 'AUTONOMOUS_DIRECT_R_EVOLUTION_RUNNING'
    lr['blocker'] = None
    ts5 = np.asarray([int(r['ts']) for r in rows5], dtype=np.int64)
    market = {
        'source5': 'canonical-grid-fixed-priority',
        'source15': 'canonical-grid-fixed-priority',
        'ts5': ts5,
        'o5': np.asarray([float(r['o']) for r in rows5], dtype=np.float64),
        'h5': np.asarray([float(r['h']) for r in rows5], dtype=np.float64),
        'l5': np.asarray([float(r['l']) for r in rows5], dtype=np.float64),
        'c5': np.asarray([float(r['c']) for r in rows5], dtype=np.float64),
        'close15': {int(r['ts']): float(r['c']) for r in rows15},
    }
    core.state[MARKET_STATE_KEY] = {
        **diag,
        'status': 'VALID',
        'loaded_5m_bars': int(len(rows5)),
        'loaded_15m_bars': int(len(rows15)),
        'first_5m_ts': int(ts5[0]) if len(ts5) else None,
        'last_5m_ts': int(ts5[-1]) if len(ts5) else None,
        'updated_at': int(time.time()),
    }
    return market


def _terminal_future_path_blocker_is_stale(core: Any, blocker: dict[str, Any], current_gap: dict[str, Any] | None = None) -> bool:
    if not blocker or not blocker.get('blocked') or current_gap is not None:
        return False
    state = str(blocker.get('state') or '').upper()
    reason = str(blocker.get('reason') or '').lower()
    if state != 'BLOCK_FUTURE_PATH' and 'future path' not in reason:
        return False
    replay = runtime_integrity.replay_progress(core)
    if not replay.get('complete'):
        return False
    legal = int(replay.get('legal_frontier_ts') or 0)
    at_ts = int(blocker.get('at_ts') or 0)
    decision_close = int(blocker.get('decision_close_ts') or 0)
    # V39 already handles an obsolete blocker at/before the legal frontier. V40
    # additionally handles the terminal probe immediately after the fixed research
    # horizon. Clearing it cannot expose future data because replay is already complete
    # and this decision is outside the certified decision set.
    return bool(
        legal > 0
        and ((at_ts > legal) or (decision_close > legal + int(core.TIMEFRAME_SECONDS['15m'])))
    )


def _reconcile_terminal_blocker(core: Any) -> dict[str, Any]:
    current_gap = resilience.detect_gap_near_cursor(core)
    replay = runtime_integrity.replay_progress(core)
    lr = core.state.setdefault('learning', {})
    local = dict(lr.get('replay_price_blocker') or {})
    strict = dict(core.state.get('strict_replay_gap_blocker') or {})
    cleared: list[str] = []
    if _terminal_future_path_blocker_is_stale(core, local, current_gap):
        cleared.append('learning.replay_price_blocker')
    if _terminal_future_path_blocker_is_stale(core, strict, current_gap):
        cleared.append('strict_replay_gap_blocker')
    if cleared:
        resolved = {
            'blocked': False,
            'state': 'TERMINAL_AFTER_FIXED_RESEARCH_HORIZON',
            'reason': 'replay is already complete; the previous future-path probe belongs after the certified fixed research horizon',
            'cleared_by': VERSION,
            'cleared_at': int(time.time()),
        }
        lr['replay_price_blocker'] = dict(resolved)
        core.state['strict_replay_gap_blocker'] = dict(resolved)
        if replay.get('complete') and str(lr.get('phase') or '').startswith(('WAITING_', 'STRICT_REPLAY_')):
            lr['phase'] = 'REPLAY_COMPLETE_READY_FOR_AUTONOMOUS_RESEARCH'
        lr['blocker'] = None
    return {
        'cleared': cleared,
        'replay_complete': bool(replay.get('complete')),
        'legal_frontier_ts': replay.get('legal_frontier_ts'),
        'current_gap': current_gap,
    }


def install(production: Any, autonomous: Any) -> None:
    core = production.core
    if getattr(core, '_v40_autonomous_handoff_installed', False):
        return
    core._v40_autonomous_handoff_installed = True

    # Fix the root cause: V30 must consume the virtual canonical series via the
    # resilience authority, never by querying a nonexistent physical source='canonical'.
    autonomous._load_market = lambda c: _load_market_grid_safe(c, autonomous)

    # Extend V39 stale-blocker semantics only for a completed replay's terminal probe.
    base_blocker_stale = replay_liveness._blocker_stale
    def blocker_stale(c: Any, blocker: dict[str, Any], gate: dict[str, Any], current_gap: dict[str, Any] | None) -> bool:
        return bool(base_blocker_stale(c, blocker, gate, current_gap) or _terminal_future_path_blocker_is_stale(c, blocker, current_gap))
    replay_liveness._blocker_stale = blocker_stale

    # Surface the exact handoff state without rescanning the full market on dashboard
    # polls. The heavy canonical validation runs only when autonomous certification
    # actually requests its immutable research cache.
    base_status = autonomous.autonomous_status
    def autonomous_status(c: Any) -> dict[str, Any]:
        out = dict(base_status(c))
        market = dict(c.state.get(MARKET_STATE_KEY) or {})
        out['market_cache_integrity'] = market
        if out.get('status') == 'WAITING_MARKET_CACHE':
            out['handoff_reason'] = (
                'legacy virtual-source SQL deadlock is fixed; waiting for a certification retry'
                if market.get('status') == 'VALID'
                else market.get('status') or 'market cache has not been validated yet'
            )
        return out
    autonomous.autonomous_status = autonomous_status

    base_pipeline = autonomous._pipeline_status
    def pipeline_status(c: Any) -> dict[str, Any]:
        out = dict(base_pipeline(c))
        auto_state = autonomous.autonomous_status(c)
        market = dict(auto_state.get('market_cache_integrity') or {})
        for stage in out.get('stages') or []:
            if str(stage.get('name') or '').startswith('6. AUTONOMOUS_DIRECT_R'):
                if auto_state.get('status') == 'WAITING_MARKET_CACHE' and market.get('status') not in ('', None, 'VALID'):
                    stage['status'] = 'WAITING'
                    stage['blocker'] = market.get('status')
                    stage.setdefault('evidence', {})['market_cache_integrity'] = market
                elif auto_state.get('status') == 'WAITING_MARKET_CACHE' and market.get('status') == 'VALID':
                    stage['status'] = 'QUEUED'
                    stage['blocker'] = 'market cache validated; awaiting/retrying autonomous certification worker'
        return out
    autonomous._pipeline_status = pipeline_status

    original_learning = core.learning_tick
    async def handoff_learning_tick() -> None:
        await original_learning()
        terminal = _reconcile_terminal_blocker(core)
        core.state[STATE_KEY] = {
            'schema': SCHEMA,
            'runtime': VERSION,
            'installed': True,
            'terminal_blocker_reconciliation': terminal,
            'market_cache_integrity': dict(core.state.get(MARKET_STATE_KEY) or {}),
            'raw_market_preserved': True,
            'learning_samples_preserved': True,
            'autonomous_checkpoint_preserved': True,
            'future_peeking': False,
            'synthetic_gap_fill': False,
            'updated_at': int(time.time()),
        }
    core.learning_tick = handoff_learning_tick

    # Reconcile the exact state shown in the user's completed-replay screenshot now,
    # rather than waiting for the next learning scheduler interval.
    try:
        terminal = _reconcile_terminal_blocker(core)
    except Exception as exc:
        terminal = {'error': f'{type(exc).__name__}: {exc}'}

    core.state[STATE_KEY] = {
        'schema': SCHEMA,
        'runtime': VERSION,
        'installed': True,
        'root_cause_fixed': 'virtual canonical source was queried as a physical SQLite source',
        'terminal_blocker_reconciliation': terminal,
        'raw_market_preserved': True,
        'learning_samples_preserved': True,
        'autonomous_checkpoint_preserved': True,
        'future_peeking': False,
        'synthetic_gap_fill': False,
        'updated_at': int(time.time()),
    }
    core.state.setdefault('strict_replay', {})['autonomous_handoff_v40'] = {
        'virtual_canonical_sql_deadlock': False,
        'autonomous_market_uses_grid_safe_canonical_series': True,
        'research_window_continuity_required': True,
        'settlement_window_continuity_required': True,
        'terminal_future_probe_can_block_completed_replay': False,
        'real_in_range_future_gap_can_be_bypassed': False,
        'raw_data_reset_required': False,
        'replay_reset_required': False,
        'future_peeking': False,
    }

    if not any(getattr(route, 'path', None) == '/api/v40/handoff' for route in core.app.router.routes):
        @core.app.get('/api/v40/handoff')
        def handoff_status() -> dict[str, Any]:
            replay = runtime_integrity.replay_progress(core)
            auto = autonomous.autonomous_status(core)
            market = dict(core.state.get(MARKET_STATE_KEY) or {})
            if replay.get('complete') and market.get('status') == 'VALID' and not auto.get('research_complete'):
                next_action = 'AUTONOMOUS_EVOLUTION_RUNNING_OR_QUEUED'
            elif not replay.get('complete'):
                next_action = 'WAIT_REPLAY'
            elif market and market.get('status') != 'VALID':
                next_action = 'REPAIR_REAL_CANONICAL_MARKET_WINDOW'
            else:
                next_action = 'WAIT_AUTONOMOUS_CERTIFICATION_PREFLIGHT'
            return {
                'runtime': VERSION,
                'schema': SCHEMA,
                'replay': replay,
                'autonomous': auto,
                'market_cache_integrity': market,
                'feature_integrity': dict(core.state.get('v35_autonomous_feature_integrity') or {}),
                'terminal_blocker': _reconcile_terminal_blocker(core),
                'next_action': next_action,
                'rules': core.state.get('strict_replay', {}).get('autonomous_handoff_v40', {}),
            }
