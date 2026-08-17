from __future__ import annotations

"""Single fixed-horizon authority for replay, collection and autonomous handoff.

The historical decision universe is immutable: 2020 research start through the
configured AUTONOMOUS_RESEARCH_END_TS (exclusive). Live candles after that point may
exist in raw storage and may be used only for outcome settlement/current-paper logic;
they must never enlarge Strict Replay, make a completed replay fall below 100%, or
create new historical decisions.

This overlay deliberately reconciles the older deployment-horizon compatibility layer,
full-history collection, replay progress, autonomous market cache and dashboard/API to
one research boundary. Raw data is preserved. Only derived historical rows beyond the
fixed decision universe may be removed. No-lookahead, real-candle-only and fail-closed
contracts remain unchanged.
"""

import bisect
import os
import time
from typing import Any

from fastapi.responses import HTMLResponse

import runtime_identity
import v5_runtime
import v16_runtime_integrity as runtime_integrity
import v22_hierarchical_pipeline as hierarchical
import v25_fixed_horizon_runtime as fixed_horizon
import v38_timeframe_aligned_bootstrap as alignment


VERSION = 'V44_FIXED_RESEARCH_HORIZON_AUTHORITY'
SCHEMA = 44
STATE_KEY = 'v44_fixed_research_horizon_authority'


def _research_start(autonomous: Any) -> int:
    return int(autonomous.RESEARCH_START_TS)


def _research_end(autonomous: Any) -> int:
    return int(autonomous.RESEARCH_END_EXCLUSIVE_TS)


def _settlement_end(autonomous: Any) -> int:
    return max(_research_end(autonomous), int(autonomous.SETTLEMENT_END_EXCLUSIVE_TS))


def _decision_last_open(core: Any, autonomous: Any) -> int:
    sec = int(core.TIMEFRAME_SECONDS['15m'])
    return ((_research_end(autonomous) // sec) * sec) - sec


def _series_end_exclusive(core: Any, autonomous: Any, tf: str) -> int:
    """Required raw window by role, never by wall clock.

    * 5m continues through the configured settlement horizon so frozen historical
      trades can be resolved honestly, including long-hold autonomous candidates.
    * 15m keeps exactly the generator's 33-bar maturity tail after the final decision.
    * higher timeframes stop at the research end; later candles are not historical
      features and therefore must not be required for replay completion.
    """
    research_end = _research_end(autonomous)
    if tf == '5m':
        return _settlement_end(autonomous)
    if tf == '15m':
        return research_end + 33 * int(core.TIMEFRAME_SECONDS['15m'])
    return research_end


def _aligned_series_window(core: Any, autonomous: Any, tf: str) -> tuple[int, int, int, int]:
    sec = int(core.TIMEFRAME_SECONDS[tf])
    requested_start = int(core.START_TS)
    first_valid_open = ((requested_start + sec - 1) // sec) * sec
    end_exclusive = int(_series_end_exclusive(core, autonomous, tf))
    target_end = (end_exclusive // sec) * sec - sec
    return sec, requested_start, first_valid_open, target_end


def _fixed_legal_frontier(core: Any, autonomous: Any) -> dict[str, Any]:
    """Newest legal historical decision, capped by the configured research end."""
    m15 = hierarchical.resilience.canonical_bars(core, 'ETH', '15m')
    m5 = hierarchical.resilience.canonical_bars(core, 'ETH', '5m')
    desired = _decision_last_open(core, autonomous)
    if len(m15) < 134 or len(m5) < 96:
        latest = int(m15[-1]['ts']) if m15 else int(core.START_TS)
        return {
            'ready': False,
            'latest_market_ts': latest,
            'legal_frontier_ts': int(core.START_TS),
            'legal_frontier_index': None,
            'fixed_research_start_ts': _research_start(autonomous),
            'fixed_research_end_exclusive_ts': _research_end(autonomous),
            'fixed_research_last_decision_ts': desired,
            'moving_frontier': False,
            'reason': 'fixed research window does not yet contain enough real 15m/5m history',
        }

    ts15 = [int(x['ts']) for x in m15]
    ts5 = [int(x['ts']) for x in m5]

    # Maturity evidence itself is bounded by the configured research/settlement
    # contract. Current/live candles later than settlement are irrelevant here and
    # cannot accidentally become part of the historical authority.
    settlement_last = _settlement_end(autonomous) - int(core.TIMEFRAME_SECONDS['5m'])
    settlement_i = bisect.bisect_right(ts5, settlement_last) - 1
    last5 = int(ts5[settlement_i]) if settlement_i >= 0 else 0
    maturity_15m_last = _research_end(autonomous) + 32 * int(core.TIMEFRAME_SECONDS['15m'])
    maturity_15m_i = bisect.bisect_right(ts15, maturity_15m_last) - 1

    # Preserve the original causal maturity rules: a decision needs 96 future 5m
    # candles after its close and the generator requires a 33x15m maturity tail.
    max_open_from_5m = last5 - 900 - 95 * 300
    max_i_from_5m = bisect.bisect_right(ts15, max_open_from_5m) - 1
    max_i_from_15m = maturity_15m_i - 33
    desired_i = bisect.bisect_right(ts15, desired) - 1
    i = min(desired_i, max_i_from_5m, max_i_from_15m)
    stride = max(1, int(os.getenv('STRICT_REPLAY_STRIDE_BARS', '1')))
    while i >= 100 and i % stride:
        i -= 1
    if i < 100:
        return {
            'ready': False,
            'latest_market_ts': int(ts15[-1]),
            'legal_frontier_ts': int(core.START_TS),
            'legal_frontier_index': None,
            'latest_5m_ts': last5 or None,
            'fixed_research_start_ts': _research_start(autonomous),
            'fixed_research_end_exclusive_ts': _research_end(autonomous),
            'fixed_research_last_decision_ts': desired,
            'moving_frontier': False,
            'reason': 'no label-matured decision exists inside the immutable research window yet',
        }

    legal = int(ts15[i])
    return {
        'ready': True,
        'latest_market_ts': int(ts15[-1]),
        'legal_frontier_ts': legal,
        'legal_frontier_index': int(i),
        'latest_5m_ts': last5,
        'stride_bars': stride,
        'fixed_research_start_ts': _research_start(autonomous),
        'fixed_research_end_exclusive_ts': _research_end(autonomous),
        'fixed_research_last_decision_ts': desired,
        'fixed_settlement_end_exclusive_ts': _settlement_end(autonomous),
        'moving_frontier': False,
        'research_horizon_reached': legal >= desired,
        'reason': 'immutable autonomous research horizon; live/post-cutoff candles cannot enlarge historical replay',
    }


def _prune_derived_after_horizon(core: Any, legal_ts: int) -> dict[str, int]:
    """Remove only derived historical products outside the fixed decision window."""
    con = core.db()
    deleted: dict[str, int] = {}
    try:
        tables = {str(r[0]) for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
        for table in ('learning_samples', 'learning_feature_snapshots'):
            if table not in tables:
                deleted[table] = 0
                continue
            count = int(con.execute(f'SELECT COUNT(*) FROM {table} WHERE ts>?', (int(legal_ts),)).fetchone()[0] or 0)
            if count:
                con.execute(f'DELETE FROM {table} WHERE ts>?', (int(legal_ts),))
            deleted[table] = count
        con.commit()
    finally:
        con.close()
    cursor = int(core.get_state(v5_runtime.REPLAY_STATE_KEY, core.START_TS) or core.START_TS)
    if cursor > int(legal_ts):
        core.set_state(v5_runtime.REPLAY_STATE_KEY, int(legal_ts))
    return deleted


def _replace_progress_detail_route(core: Any, autonomous: Any) -> None:
    """Make the public/API progress card consume the same V44 replay authority."""
    app = getattr(core, 'app', None)
    if app is None:
        return
    routes = list(getattr(app.router, 'routes', []) or [])
    old = next((r for r in routes if getattr(r, 'path', None) == '/api/latest/progress-detail'), None)
    old_endpoint = getattr(old, 'endpoint', None)
    if old is not None:
        app.router.routes = [r for r in app.router.routes if getattr(r, 'path', None) != '/api/latest/progress-detail']

    @app.get('/api/latest/progress-detail')
    def progress_detail_v44() -> dict[str, Any]:
        payload: dict[str, Any] = {}
        if callable(old_endpoint):
            try:
                raw = old_endpoint()
                payload = dict(raw) if isinstance(raw, dict) else {}
            except Exception as exc:
                payload = {'compatibility_view_error': f'{type(exc).__name__}: {exc}'}
        replay = dict(runtime_integrity.replay_progress(core) or {})
        payload.update({
            'schema': SCHEMA,
            'runtime': runtime_identity.RUNTIME_VERSION,
            'fixed_replay_cutoff_ts': _research_end(autonomous),
            'fixed_replay_cutoff_is_immutable': True,
            'fixed_research_start_ts': _research_start(autonomous),
            'fixed_research_end_exclusive_ts': _research_end(autonomous),
            'last_historical_decision_ts': _decision_last_open(core, autonomous),
            'settlement_end_exclusive_ts': _settlement_end(autonomous),
            'replay': replay,
            'post_research_live_candles_replayed_as_history': False,
            'post_research_data_role': 'SETTLEMENT_OR_CURRENT_PAPER_ONLY',
        })
        return payload


def _replace_dashboard_route(core: Any) -> None:
    """Remove stale 10.2/full-history labels from the actual served dashboard."""
    app = getattr(core, 'app', None)
    if app is None:
        return
    routes = list(getattr(app.router, 'routes', []) or [])
    old = next((r for r in routes if getattr(r, 'path', None) == '/'), None)
    old_endpoint = getattr(old, 'endpoint', None)
    if not callable(old_endpoint):
        return
    app.router.routes = [r for r in app.router.routes if getattr(r, 'path', None) != '/']

    @app.get('/', response_class=HTMLResponse)
    def dashboard_v44() -> str:
        html = str(old_endpoint())
        html = html.replace('ETH Adaptive AI 10.2', f'{runtime_identity.PRODUCT_NAME} {runtime_identity.DISPLAY_VERSION}')
        html = html.replace('Causal Full-History Learning', runtime_identity.PUBLIC_PIPELINE_NAME)
        html = html.replace('2020→現在 K 線覆蓋（直接查 DB）', '固定研究資料覆蓋（直接查 DB）')
        html = html.replace('2020→現在 K 線覆蓋', '固定研究資料覆蓋')
        return html


def install(
    production: Any,
    autonomous: Any,
    transition: Any,
    scheduler: Any,
    resource_authority: Any,
) -> None:
    core = production.core
    if getattr(core, '_v44_fixed_research_horizon_installed', False):
        return
    core._v44_fixed_research_horizon_installed = True

    # One source of truth for collection windows. V39's grid-safe progress/gap code
    # calls this helper dynamically, so the role-specific horizons propagate without
    # duplicating its SQL integrity checks.
    alignment._aligned_series_window = lambda c, tf: _aligned_series_window(c, autonomous, tf)

    # The legacy deployment-time cutoff must no longer define the replay decision set.
    fixed_horizon._cutoff = lambda _c: _research_end(autonomous)
    fixed_horizon._fixed_legal_frontier = lambda c: _fixed_legal_frontier(c, autonomous)
    runtime_integrity._legal_frontier = lambda c: _fixed_legal_frontier(c, autonomous)

    base_progress = runtime_integrity.replay_progress

    def fixed_progress(c: Any) -> dict[str, Any]:
        out = dict(base_progress(c) or {})
        frontier = _fixed_legal_frontier(c, autonomous)
        legal = int(frontier.get('legal_frontier_ts') or c.START_TS)
        desired = int(frontier.get('fixed_research_last_decision_ts') or _decision_last_open(c, autonomous))
        cursor = int(c.get_state(v5_runtime.REPLAY_STATE_KEY, c.START_TS) or c.START_TS)
        # Completion is allowed only when real maturity data actually reaches the fixed
        # last decision. A partial dataset therefore cannot be painted green.
        horizon_ready = bool(frontier.get('ready') and legal >= desired)
        complete = bool(horizon_ready and cursor >= desired)
        out.update({
            'cursor_ts': min(cursor, desired) if complete else cursor,
            'legal_frontier_ts': desired if horizon_ready else legal,
            'fixed_research_start_ts': _research_start(autonomous),
            'fixed_research_end_exclusive_ts': _research_end(autonomous),
            'fixed_research_last_decision_ts': desired,
            'fixed_replay_cutoff_ts': _research_end(autonomous),
            'moving_frontier': False,
            'frontier_reason': frontier.get('reason'),
            'completion_basis': 'IMMUTABLE_AUTONOMOUS_RESEARCH_HORIZON',
        })
        if complete:
            out.update({
                'percent': 100.0,
                'complete': True,
                'pending_eligible_decisions': 0,
                'remaining_to_legal_frontier_seconds': 0,
                'status': 'COMPLETE_FIXED_RESEARCH_HORIZON',
            })
        return out

    runtime_integrity.replay_progress = fixed_progress

    frontier = _fixed_legal_frontier(core, autonomous)
    desired = int(frontier.get('fixed_research_last_decision_ts') or _decision_last_open(core, autonomous))
    pruned = _prune_derived_after_horizon(core, desired)
    cursor = int(core.get_state(v5_runtime.REPLAY_STATE_KEY, core.START_TS) or core.START_TS)
    if cursor > desired:
        core.set_state(v5_runtime.REPLAY_STATE_KEY, desired)
        cursor = desired

    snap = dict(runtime_integrity.replay_progress(core) or {})
    lr = core.state.setdefault('learning', {})
    lr['replay_learning_progress'] = dict(snap)
    if snap.get('complete'):
        lr['phase'] = 'AUTONOMOUS_RESEARCH_READY_TO_QUEUE'
        lr['blocker'] = None
        core.state['strict_replay_gap_blocker'] = {
            'blocked': False,
            'state': 'FIXED_RESEARCH_HORIZON_COMPLETE',
            'reason': 'historical replay ended at the immutable autonomous research boundary',
            'cleared_by': VERSION,
            'cleared_at': int(time.time()),
        }

    core.state[STATE_KEY] = {
        'schema': SCHEMA,
        'runtime': VERSION,
        'public_runtime': runtime_identity.RUNTIME_VERSION,
        'research_start_ts': _research_start(autonomous),
        'research_end_exclusive_ts': _research_end(autonomous),
        'last_historical_decision_ts': desired,
        'settlement_end_exclusive_ts': _settlement_end(autonomous),
        'replay_cursor_ts': cursor,
        'replay_complete': bool(snap.get('complete')),
        'replay_percent': float(snap.get('percent') or 0.0),
        'pending_historical_decisions': int(snap.get('pending_eligible_decisions') or 0),
        'derived_rows_pruned_after_horizon': pruned,
        'live_market_can_extend_historical_frontier': False,
        'post_research_raw_data_preserved': True,
        'post_research_data_role': 'SETTLEMENT_OR_CURRENT_PAPER_ONLY',
        'future_peeking': False,
        'synthetic_gap_fill': False,
        'updated_at': int(time.time()),
    }
    core.state.setdefault('strict_replay', {})['fixed_research_horizon_v44'] = {
        'runtime': VERSION,
        'single_horizon_authority': True,
        'research_end_exclusive_ts': _research_end(autonomous),
        'last_historical_decision_ts': desired,
        'live_frontier_growth_forbidden': True,
        'raw_data_deleted': False,
        'derived_data_outside_horizon_pruned': True,
        'no_lookahead_unchanged': True,
    }

    _replace_progress_detail_route(core, autonomous)
    _replace_dashboard_route(core)

    # Prime V42's immutable O(1) completed view and immediately wake the existing
    # V26/V41 scheduler. No direct model-training bypass is introduced here.
    if snap.get('complete'):
        try:
            resource_authority._freeze_completed_replay_view(core, autonomous)
        except Exception as exc:
            core.state[STATE_KEY]['freeze_view_error'] = f'{type(exc).__name__}: {exc}'
        try:
            scheduler._kick(core, autonomous, transition, source='v44_horizon_alignment', force_interval=True)
        except Exception as exc:
            core.state[STATE_KEY]['scheduler_kick_error'] = f'{type(exc).__name__}: {exc}'

    app = getattr(core, 'app', None)
    routes = list(getattr(getattr(app, 'router', None), 'routes', []) or []) if app is not None else []
    if app is not None and not any(getattr(r, 'path', None) == '/api/v44/horizon' for r in routes):
        @app.get('/api/v44/horizon')
        def horizon_status() -> dict[str, Any]:
            replay = dict(runtime_integrity.replay_progress(core) or {})
            state = dict(core.state.get(STATE_KEY) or {})
            state.update({
                'replay': replay,
                'collection_gate': dict(core.price_collection_gate() or {}) if callable(getattr(core, 'price_collection_gate', None)) else {},
                'updated_at': int(time.time()),
            })
            return state
