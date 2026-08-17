from __future__ import annotations

"""Post-replay autonomous market-cache alignment and priority repair.

V44 intentionally fixes the historical decision universe at the configured research
end, while autonomous trade-path evaluation still needs real 5m settlement candles
after that decision horizon. Those two facts must not be shown as one moving
"history" percentage.

This layer keeps the historical replay immutable, reports the Stage-6 market-cache
window separately, prioritizes repair of the exact ETH 5m/15m gaps that block
autonomous research, and normalizes public/runtime phase truth so stale running labels
cannot override WAITING_MARKET_CACHE.

No raw rows are deleted. Missing candles are fetched from the existing real-exchange
history providers only; interpolation/synthetic fills remain forbidden.
"""

import asyncio
import os
import threading
import time
from typing import Any

import runtime_identity
import v15_data_resilience as resilience
import v16_runtime_integrity as runtime_integrity
import v22_hierarchical_pipeline as hierarchical


VERSION = 'V45_AUTONOMOUS_MARKET_CACHE_ALIGNMENT'
SCHEMA = 45
STATE_KEY = 'v45_autonomous_market_cache_alignment'
REPAIR_PAGES_PER_PASS = max(1, min(12, int(os.getenv('AUTONOMOUS_STAGE6_REPAIR_PAGES_PER_PASS', '4'))))
REPAIR_RETRY_SECONDS = max(10, min(300, int(os.getenv('AUTONOMOUS_STAGE6_REPAIR_RETRY_SECONDS', '30'))))
TRUTH_TTL_SECONDS = max(2, min(60, int(os.getenv('AUTONOMOUS_STAGE6_TRUTH_TTL_SECONDS', '10'))))

_REPAIR_LOCK = threading.Lock()
_TRUTH_LOCK = threading.Lock()
_LAST_REPAIR_AT = 0.0
_TRUTH_CACHE: dict[tuple[Any, ...], tuple[float, dict[str, Any]]] = {}
_TASKS: set[asyncio.Task[Any]] = set()


def _role(core: Any) -> str:
    role = (core.state.get('runtime_role') or {}).get('role') if isinstance(core.state.get('runtime_role'), dict) else None
    if role:
        return str(role)
    boot = core.state.get('bootstrap_replica_role')
    return str((boot or {}).get('role') or 'UNKNOWN') if isinstance(boot, dict) else 'UNKNOWN'


def _replay(core: Any) -> dict[str, Any]:
    try:
        return dict(runtime_integrity.replay_progress(core) or {})
    except Exception as exc:
        return {'complete': False, 'percent': 0.0, 'error': f'{type(exc).__name__}: {exc}'}


def _window(core: Any, autonomous: Any, tf: str) -> tuple[int, int, int]:
    sec = int(core.TIMEFRAME_SECONDS[tf])
    start = ((int(autonomous.RESEARCH_START_TS) + sec - 1) // sec) * sec
    if tf == '5m':
        end_exclusive = int(autonomous.SETTLEMENT_END_EXCLUSIVE_TS)
    elif tf == '15m':
        end_exclusive = int(autonomous.RESEARCH_END_EXCLUSIVE_TS)
    else:
        raise ValueError(f'unsupported Stage-6 timeframe: {tf}')
    end = (end_exclusive // sec) * sec
    return sec, start, end


def _series_truth(core: Any, autonomous: Any, tf: str) -> dict[str, Any]:
    sec, start, end = _window(core, autonomous, tf)
    expected = max(0, (end - start) // sec)
    placeholders = ','.join('?' for _ in resilience.PRICE_PRIORITY)
    con = core.db()
    try:
        row = con.execute(
            f"""SELECT COUNT(DISTINCT ts),MIN(ts),MAX(ts)
                FROM market_bars
                WHERE asset='ETH' AND tf=? AND ts>=? AND ts<?
                  AND source IN ({placeholders})
                  AND ((ts-?) % ?)=0""",
            (tf, start, end, *resilience.PRICE_PRIORITY, start, sec),
        ).fetchone()
        count = int(row[0] or 0) if row else 0
        first = int(row[1]) if row and row[1] is not None else None
        last = int(row[2]) if row and row[2] is not None else None
        missing = None
        if expected > 0 and (count != expected or first != start or last != end - sec):
            if first is None or first > start:
                missing = start
            else:
                gap = con.execute(
                    f"""WITH unique_ts AS (
                            SELECT DISTINCT ts
                            FROM market_bars
                            WHERE asset='ETH' AND tf=? AND ts>=? AND ts<?
                              AND source IN ({placeholders})
                              AND ((ts-?) % ?)=0
                        ), ordered AS (
                            SELECT ts,LAG(ts) OVER (ORDER BY ts) AS previous_ts
                            FROM unique_ts
                        )
                        SELECT previous_ts+?
                        FROM ordered
                        WHERE previous_ts IS NOT NULL AND ts-previous_ts>?
                        ORDER BY ts LIMIT 1""",
                    (tf, start, end, *resilience.PRICE_PRIORITY, start, sec, sec, sec),
                ).fetchone()
                if gap and gap[0] is not None:
                    missing = int(gap[0])
                elif last is None or last < end - sec:
                    missing = int((last + sec) if last is not None else start)
    finally:
        con.close()
    percent = 100.0 if expected == 0 else min(100.0, max(0.0, count / expected * 100.0))
    return {
        'asset': 'ETH',
        'timeframe': tf,
        'start_ts': start,
        'end_exclusive_ts': end,
        'target_last_ts': end - sec if expected else None,
        'expected_bars': expected,
        'bars': count,
        'percent': round(percent, 4),
        'first_ts': first,
        'last_ts': last,
        'first_missing_ts': missing,
        'complete': bool(expected > 0 and count == expected and first == start and last == end - sec and missing is None),
        'real_exchange_rows_only': True,
        'synthetic_gap_fill': False,
    }


def market_truth(core: Any, autonomous: Any, *, refresh: bool = False) -> dict[str, Any]:
    key = (
        id(core),
        int(autonomous.RESEARCH_START_TS),
        int(autonomous.RESEARCH_END_EXCLUSIVE_TS),
        int(autonomous.SETTLEMENT_END_EXCLUSIVE_TS),
    )
    now_m = time.monotonic()
    if not refresh:
        with _TRUTH_LOCK:
            cached = _TRUTH_CACHE.get(key)
        if cached and now_m - cached[0] <= TRUTH_TTL_SECONDS:
            return cached[1]

    series = [_series_truth(core, autonomous, tf) for tf in ('5m', '15m')]
    ready = bool(series and all(bool(x.get('complete')) for x in series))
    first_gap = next((x for x in series if x.get('first_missing_ts') is not None), None)
    out = {
        'schema': SCHEMA,
        'runtime': VERSION,
        'public_runtime': runtime_identity.RUNTIME_VERSION,
        'ready': ready,
        'percent': round(min((float(x.get('percent') or 0.0) for x in series), default=0.0), 4),
        'series': {f"ETH:{x['timeframe']}": x for x in series},
        'first_blocking_gap': ({
            'asset': 'ETH',
            'timeframe': first_gap['timeframe'],
            'missing_ts': first_gap['first_missing_ts'],
            'target_from': first_gap['start_ts'],
            'target_to': first_gap['target_last_ts'],
        } if first_gap else None),
        'historical_replay_is_separate': True,
        'historical_replay_percent': float(_replay(core).get('percent') or 0.0),
        'research_end_exclusive_ts': int(autonomous.RESEARCH_END_EXCLUSIVE_TS),
        'settlement_end_exclusive_ts': int(autonomous.SETTLEMENT_END_EXCLUSIVE_TS),
        'future_prices_as_features': False,
        'post_research_5m_role': 'OUTCOME_SETTLEMENT_ONLY',
        'updated_at': int(time.time()),
    }
    with _TRUTH_LOCK:
        _TRUTH_CACHE[key] = (now_m, out)
    return out


def _state(core: Any, **patch: Any) -> dict[str, Any]:
    raw = core.state.get(STATE_KEY)
    out = dict(raw) if isinstance(raw, dict) else {}
    out.update(patch)
    out.update({'schema': SCHEMA, 'runtime': VERSION, 'public_runtime': runtime_identity.RUNTIME_VERSION, 'updated_at': int(time.time())})
    core.state[STATE_KEY] = out
    return out


def _normalize_phase(core: Any, autonomous: Any, truth: dict[str, Any] | None = None) -> None:
    replay = _replay(core)
    if not replay.get('complete'):
        return
    try:
        auto = dict(autonomous.autonomous_status(core) or {})
    except Exception:
        auto = {}
    status = str(auto.get('status') or '')
    market = dict(auto.get('market_cache_integrity') or {})
    truth = truth or market_truth(core, autonomous)
    lr = core.state.setdefault('learning', {})

    if auto.get('research_complete'):
        lr['phase'] = 'AUTONOMOUS_RESEARCH_COMPLETE' if auto.get('champions') else 'AUTONOMOUS_RESEARCH_COMPLETE_NO_CERTIFIED_PACKAGE'
        lr['blocker'] = None
        return
    if status == 'WAITING_MARKET_CACHE':
        if market.get('status') == 'VALID':
            lr['phase'] = 'AUTONOMOUS_RESEARCH_READY_TO_QUEUE'
            lr['blocker'] = None
        elif truth.get('ready'):
            lr['phase'] = 'AUTONOMOUS_MARKET_CACHE_REBUILD_PENDING'
            lr['blocker'] = 'all real Stage-6 price windows are complete; rebuild/retry autonomous market cache'
        else:
            gap = truth.get('first_blocking_gap') or {}
            lr['phase'] = 'WAITING_AUTONOMOUS_MARKET_CACHE_INTEGRITY'
            lr['blocker'] = (
                f"missing real ETH {gap.get('timeframe') or '?'} candle at {gap.get('missing_ts')}"
                if gap else 'Stage-6 real market cache window is not complete'
            )
        return
    if status in ('AUTONOMOUS_EVOLUTION_RUNNING', 'AUTONOMOUS_OOS_RUNNING', 'CERTIFICATION_RUNNING'):
        lr['phase'] = 'AUTONOMOUS_DIRECT_R_EVOLUTION_RUNNING'
        lr['blocker'] = None
        return

    if str(lr.get('phase') or '').startswith(('STRICT_REPLAY', 'COLLECTING_FULL_HISTORY')):
        lr['phase'] = 'AUTONOMOUS_RESEARCH_QUEUED'
        lr['blocker'] = None


def _replace_progress_detail_route(core: Any, autonomous: Any) -> None:
    app = getattr(core, 'app', None)
    if app is None:
        return
    routes = list(getattr(app.router, 'routes', []) or [])
    old = next((r for r in routes if getattr(r, 'path', None) == '/api/latest/progress-detail'), None)
    old_endpoint = getattr(old, 'endpoint', None)
    if not callable(old_endpoint):
        return
    app.router.routes = [r for r in app.router.routes if getattr(r, 'path', None) != '/api/latest/progress-detail']

    def rewrite(value: Any, phase: str) -> Any:
        if isinstance(value, dict):
            return {k: rewrite(v, phase) for k, v in value.items()}
        if isinstance(value, list):
            return [rewrite(v, phase) for v in value]
        if value == 'STRICT_REPLAY_ADVANCING':
            return phase
        if value == 'full price history contract is complete; point-in-time replay is advancing':
            return 'fixed historical replay is complete; Stage-6 settlement market cache is being validated/repaired'
        return value

    @app.get('/api/latest/progress-detail')
    def progress_detail_v45() -> dict[str, Any]:
        raw = old_endpoint()
        payload = dict(raw) if isinstance(raw, dict) else {}
        truth = market_truth(core, autonomous)
        _normalize_phase(core, autonomous, truth)
        phase = str((core.state.get('learning') or {}).get('phase') or 'AUTONOMOUS_RESEARCH_QUEUED')
        payload = rewrite(payload, phase)
        payload.update({
            'schema': SCHEMA,
            'runtime': runtime_identity.RUNTIME_VERSION,
            'historical_replay': _replay(core),
            'historical_replay_complete_is_terminal': True,
            'stage6_market_requirements': truth,
            'stage6_market_percent': truth.get('percent'),
            'stage6_first_blocking_gap': truth.get('first_blocking_gap'),
            'stage6_settlement_window_is_not_historical_replay': True,
            'learning_phase': phase,
        })
        return payload


def _replace_dashboard_labels(core: Any) -> None:
    app = getattr(core, 'app', None)
    if app is None:
        return
    routes = list(getattr(app.router, 'routes', []) or [])
    old = next((r for r in routes if getattr(r, 'path', None) == '/'), None)
    old_endpoint = getattr(old, 'endpoint', None)
    if not callable(old_endpoint):
        return
    app.router.routes = [r for r in app.router.routes if getattr(r, 'path', None) != '/']

    from fastapi.responses import HTMLResponse

    @app.get('/', response_class=HTMLResponse)
    def dashboard_v45() -> str:
        raw = old_endpoint()
        html = raw.body.decode() if hasattr(raw, 'body') else str(raw)
        html = html.replace(
            '原始價格資料覆蓋（必要時框）',
            '研究＋Stage 6 結算必要資料覆蓋',
        )
        html = html.replace(
            '固定研究資料覆蓋（直接查 DB）',
            '研究＋Stage 6 結算必要資料覆蓋（直接查 DB）',
        )
        html = html.replace(
            '原始價格資料覆蓋',
            '研究＋Stage 6 結算必要資料覆蓋',
        )
        return html


async def repair_stage6_market(core: Any, autonomous: Any, *, force: bool = False) -> dict[str, Any]:
    global _LAST_REPAIR_AT
    replay = _replay(core)
    if not replay.get('complete'):
        return _state(core, status='WAIT_REPLAY', replay=replay, market=market_truth(core, autonomous))
    if _role(core).startswith('FOLLOWER'):
        return _state(core, status='FOLLOWER_READ_ONLY', replay=replay, market=market_truth(core, autonomous))

    now_m = time.monotonic()
    if not force and now_m - _LAST_REPAIR_AT < REPAIR_RETRY_SECONDS:
        truth = market_truth(core, autonomous)
        _normalize_phase(core, autonomous, truth)
        return _state(core, status='THROTTLED' if not truth.get('ready') else 'READY', replay=replay, market=truth)

    if not _REPAIR_LOCK.acquire(blocking=False):
        truth = market_truth(core, autonomous)
        _normalize_phase(core, autonomous, truth)
        return _state(core, status='REPAIR_ALREADY_RUNNING', replay=replay, market=truth)

    _LAST_REPAIR_AT = now_m
    attempts: list[dict[str, Any]] = []
    try:
        truth = market_truth(core, autonomous, refresh=True)
        if truth.get('ready'):
            _normalize_phase(core, autonomous, truth)
            return _state(core, status='READY', replay=replay, market=truth, repair_attempts=[])

        for _ in range(REPAIR_PAGES_PER_PASS):
            gap = truth.get('first_blocking_gap')
            if not gap:
                break
            tf = str(gap['timeframe'])
            before = dict(truth['series'].get(f'ETH:{tf}') or {})
            try:
                result = await hierarchical._repair_collection_gap(core, dict(gap))
            except Exception as exc:
                result = {'status': 'REPAIR_ERROR', 'error': f'{type(exc).__name__}: {exc}'}
            after_series = _series_truth(core, autonomous, tf)
            attempts.append({
                'target': dict(gap),
                'provider_result': result,
                'bars_before': int(before.get('bars') or 0),
                'bars_after': int(after_series.get('bars') or 0),
                'percent_after': float(after_series.get('percent') or 0.0),
            })
            truth = market_truth(core, autonomous, refresh=True)
            if truth.get('ready'):
                break
            if int(after_series.get('bars') or 0) <= int(before.get('bars') or 0):
                break

        _normalize_phase(core, autonomous, truth)
        return _state(
            core,
            status='READY' if truth.get('ready') else 'WAITING_REAL_STAGE6_PRICE_WINDOW',
            replay=replay,
            market=truth,
            repair_attempts=attempts[-REPAIR_PAGES_PER_PASS:],
            repair_pages_per_pass=REPAIR_PAGES_PER_PASS,
        )
    finally:
        _REPAIR_LOCK.release()


def install(production: Any, autonomous: Any, transition: Any, scheduler: Any) -> None:
    core = production.core
    if getattr(core, '_v45_autonomous_market_cache_alignment_installed', False):
        return
    core._v45_autonomous_market_cache_alignment_installed = True

    _replace_progress_detail_route(core, autonomous)
    _replace_dashboard_labels(core)

    base_status = autonomous.autonomous_status

    def aligned_autonomous_status(c: Any) -> dict[str, Any]:
        out = dict(base_status(c) or {})
        truth = market_truth(c, autonomous)
        out['stage6_market_requirements'] = truth
        out['stage6_market_percent'] = truth.get('percent')
        out['stage6_first_blocking_gap'] = truth.get('first_blocking_gap')
        if out.get('status') == 'WAITING_MARKET_CACHE':
            out['handoff_reason'] = (
                'all real Stage-6 price windows are complete; autonomous cache rebuild/retry pending'
                if truth.get('ready')
                else f"Stage-6 real market window {float(truth.get('percent') or 0.0):.4f}% complete"
            )
        return out

    autonomous.autonomous_status = aligned_autonomous_status

    base_pipeline = autonomous._pipeline_status

    def aligned_pipeline(c: Any) -> dict[str, Any]:
        out = dict(base_pipeline(c) or {})
        auto = autonomous.autonomous_status(c)
        truth = dict(auto.get('stage6_market_requirements') or {})
        for stage in out.get('stages') or []:
            if str(stage.get('name') or '').startswith('6. AUTONOMOUS_DIRECT_R'):
                if auto.get('status') == 'WAITING_MARKET_CACHE':
                    if truth.get('ready'):
                        stage['status'] = 'QUEUED'
                        stage['blocker'] = 'real market window complete; rebuilding/retrying market cache'
                    else:
                        stage['status'] = 'WAITING'
                        stage['blocker'] = (
                            f"real Stage-6 market window {float(truth.get('percent') or 0.0):.4f}%"
                        )
                    evidence = stage.setdefault('evidence', {})
                    evidence['stage6_market_requirements'] = truth
        return out

    autonomous._pipeline_status = aligned_pipeline

    original_learning_tick = core.learning_tick

    async def learning_tick_aligned(*args: Any, **kwargs: Any) -> Any:
        await repair_stage6_market(core, autonomous)
        result = await original_learning_tick(*args, **kwargs)
        truth = market_truth(core, autonomous)
        _normalize_phase(core, autonomous, truth)
        try:
            auto = dict(autonomous.autonomous_status(core) or {})
        except Exception:
            auto = {}
        if truth.get('ready') and auto.get('status') == 'WAITING_MARKET_CACHE':
            try:
                scheduler._kick(core, autonomous, transition, source='v45_market_window_ready', force_interval=True)
            except Exception as exc:
                _state(core, scheduler_kick_error=f'{type(exc).__name__}: {exc}')
        return result

    core.learning_tick = learning_tick_aligned

    original_scan = getattr(core, 'scan', None)
    if callable(original_scan):
        async def scan_aligned(*args: Any, **kwargs: Any) -> Any:
            await repair_stage6_market(core, autonomous)
            result = await original_scan(*args, **kwargs)
            _normalize_phase(core, autonomous)
            return result
        core.scan = scan_aligned

    app = getattr(core, 'app', None)
    if app is not None:
        @app.get('/api/v45/market-cache-alignment')
        def market_cache_alignment() -> dict[str, Any]:
            truth = market_truth(core, autonomous)
            replay = _replay(core)
            state = dict(core.state.get(STATE_KEY) or {})
            state.update({
                'schema': SCHEMA,
                'runtime': VERSION,
                'public_runtime': runtime_identity.RUNTIME_VERSION,
                'replay': replay,
                'market': truth,
                'meaning': {
                    'replay_100_percent': 'fixed historical decision universe complete',
                    'stage6_market_percent': 'real 5m settlement + 15m research windows required for autonomous trade-path simulation',
                    'lower_stage6_percent_does_not_mean_historical_replay_regressed': True,
                },
                'updated_at': int(time.time()),
            })
            return state

        @app.on_event('startup')
        async def _v45_startup_repair() -> None:
            async def boot() -> None:
                try:
                    result = await repair_stage6_market(core, autonomous, force=True)
                    if (result.get('market') or {}).get('ready'):
                        scheduler._kick(core, autonomous, transition, source='v45_startup_market_ready', force_interval=True)
                    _normalize_phase(core, autonomous)
                except Exception as exc:
                    _state(core, startup_repair_error=f'{type(exc).__name__}: {exc}')
            task = asyncio.create_task(boot())
            _TASKS.add(task)
            task.add_done_callback(_TASKS.discard)

    truth = market_truth(core, autonomous)
    _normalize_phase(core, autonomous, truth)
    _state(
        core,
        status='READY' if truth.get('ready') else 'WAITING_REAL_STAGE6_PRICE_WINDOW',
        replay=_replay(core),
        market=truth,
        rules={
            'historical_replay_can_regress_due_to_live_market_growth': False,
            'stage6_settlement_window_reported_separately': True,
            'stage6_gap_repair_priority': ['ETH:5m', 'ETH:15m'],
            'real_exchange_rows_only': True,
            'synthetic_gap_fill': False,
            'future_prices_as_features': False,
            'raw_data_deleted': False,
        },
    )
