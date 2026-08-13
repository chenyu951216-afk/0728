from __future__ import annotations

import asyncio
import time
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable

import execution_v7
import v5_runtime
import runtime_identity
import v7_trade_monitor as trade_monitor


STABILITY_VERSION = runtime_identity.RUNTIME_VERSION
SCAN_RETRIES = 2
SCAN_STALE_SECONDS = 180
SCAN_DEGRADE_AFTER = 3
LEARNING_RETRY_SECONDS = 2


def _health(core: Any, name: str) -> dict[str, Any]:
    root = core.state.setdefault('subsystem_health', {})
    return root.setdefault(name, {
        'status': 'BOOTING',
        'consecutive_errors': 0,
        'last_success_at': None,
        'last_error_at': None,
        'last_error': None,
    })


def _ok(core: Any, name: str, **extra: Any) -> None:
    h = _health(core, name)
    h.update({
        'status': 'OK',
        'consecutive_errors': 0,
        'last_success_at': int(time.time()),
        'last_error': None,
        **extra,
    })


def _err(core: Any, name: str, exc: Exception | str, status: str = 'DEGRADED', **extra: Any) -> dict[str, Any]:
    h = _health(core, name)
    h['status'] = status
    h['consecutive_errors'] = int(h.get('consecutive_errors') or 0) + 1
    h['last_error_at'] = int(time.time())
    h['last_error'] = str(exc)[-1600:]
    h.update(extra)
    return h


def _age(last_success: Any) -> int | None:
    try:
        if last_success is None:
            return None
        return max(0, int(time.time()) - int(last_success))
    except Exception:
        return None


def _refresh_service(core: Any) -> None:
    storage = core.state.get('storage') or {}
    market = _health(core, 'market_scan')
    risk = _health(core, 'risk_monitor')
    active = core.latest_signal() if storage.get('healthy', True) else None

    critical: list[str] = []
    if storage and not storage.get('healthy', False):
        critical.append(f"storage: {storage.get('reason') or 'unhealthy'}")

    market_age = _age(market.get('last_success_at'))
    if market.get('consecutive_errors', 0) >= SCAN_DEGRADE_AFTER or (market_age is not None and market_age > SCAN_STALE_SECONDS):
        critical.append(f"market scan stale/error: {market.get('last_error') or market_age}")

    if active and active.get('status') in ('PLANNED', 'OPEN'):
        probe = core.state.get('risk_feed_probe') or {}
        if not (probe.get('gate_trades_ok') and probe.get('coverage_complete')):
            critical.append(f"ordered risk feed incomplete: {probe.get('error') or 'coverage not confirmed'}")
        if risk.get('status') == 'DEGRADED':
            critical.append(f"risk monitor: {risk.get('last_error') or 'degraded'}")

    if critical:
        core.state['service'] = 'DEGRADED'
        core.state['error'] = ' | '.join(critical)[:1800]
    else:
        if market.get('last_success_at') is not None:
            core.state['service'] = 'OK'
            core.state['error'] = None


async def _safe_scan(core: Any, original_scan: Callable[[], Awaitable[dict[str, Any]]]) -> dict[str, Any]:
    errors: list[str] = []
    for attempt in range(SCAN_RETRIES + 1):
        try:
            result = await original_scan()
            _ok(core, 'market_scan', attempts=attempt + 1, snapshot_ts=int((result or {}).get('snapshot_ts') or time.time()))
            core.state['analysis_stale'] = False
            _refresh_service(core)
            return result
        except Exception as exc:
            errors.append(f'#{attempt + 1} {type(exc).__name__}: {exc}')
            if attempt < SCAN_RETRIES:
                await asyncio.sleep(0.7 * (attempt + 1))

    h = _err(core, 'market_scan', ' ; '.join(errors), status='RETRY_EXHAUSTED', attempts=SCAN_RETRIES + 1)
    core.state['analysis_stale'] = True
    analysis = core.state.get('analysis') or {}
    if analysis:
        selection = dict(analysis.get('selection') or {})
        selection['tradeable'] = False
        selection['reason'] = 'market scan stale/retry exhausted; fail-closed until a fresh scan succeeds'
        analysis = dict(analysis)
        analysis['selection'] = selection
        analysis['stale'] = True
        analysis['stale_reason'] = h.get('last_error')
        core.state['analysis'] = analysis
    _refresh_service(core)
    return core.state.get('analysis') or {}


async def _safe_learning(core: Any, original_learning: Callable[[], Awaitable[None]]) -> None:
    try:
        await original_learning()
        _ok(core, 'learning')
    except Exception as first:
        _err(core, 'learning', first, status='RETRYING')
        await asyncio.sleep(LEARNING_RETRY_SECONDS)
        try:
            await original_learning()
            _ok(core, 'learning', recovered_after_retry=True)
        except Exception as second:
            _err(core, 'learning', second, status='DEGRADED')
            core.state.setdefault('learning', {})['error'] = str(second)[-1600:]
    _refresh_service(core)


async def _safe_monitor(core: Any) -> None:
    """Keep idle feed hiccups visible without falsely declaring active risk protection healthy.

    No open/planned position: a transient ordered-feed failure is RETRYING and new
    entries remain blocked by the separate risk-feed gate. With a planned/open
    position, the exact same failure is DEGRADED because stop/target monitoring is
    safety-critical.
    """
    active = core.latest_signal()
    active_risk = bool(active and active.get('status') in ('PLANNED', 'OPEN'))
    try:
        await trade_monitor.monitor_trades(core)
        probe = core.state.get('risk_feed_probe') or {}
        if probe.get('gate_trades_ok') and probe.get('coverage_complete'):
            _ok(core, 'risk_monitor', source='gate-trades', active_position=active_risk)
        else:
            status = 'DEGRADED' if active_risk else 'RETRYING'
            _err(
                core, 'risk_monitor', probe.get('error') or 'ordered feed coverage incomplete',
                status=status, active_position=active_risk,
                new_entries_fail_closed=True,
            )
    except Exception as exc:
        status = 'DEGRADED' if active_risk else 'RETRYING'
        _err(core, 'risk_monitor', exc, status=status, active_position=active_risk, new_entries_fail_closed=True)
        probe = core.state.setdefault('risk_feed_probe', {})
        text = str(exc).strip() or repr(exc)
        probe.update({
            'gate_trades_ok': False, 'coverage_complete': False,
            'error': f'monitor exception: {type(exc).__name__}: {text}',
            'checked_at': int(time.time()),
        })
    _refresh_service(core)


async def _safe_discord_poll(core: Any) -> None:
    try:
        await v5_runtime.poll_discord_commands(core)
        _ok(core, 'discord_poll')
    except Exception as exc:
        _err(core, 'discord_poll', exc, status='DEGRADED')


def install(core: Any) -> None:
    original_scan = core.scan
    original_learning = core.learning_tick
    original_optimize_all = execution_v7.optimize_all

    async def stable_scan() -> dict[str, Any]:
        return await _safe_scan(core, original_scan)

    async def stable_learning() -> None:
        await _safe_learning(core, original_learning)

    def stable_optimize_all(c: Any, force: bool = False):
        started = time.time()
        try:
            result = original_optimize_all(c, force)
            _ok(core, 'execution_audit', duration_seconds=round(time.time() - started, 3), results=len(result or []))
            return result
        except Exception as exc:
            _err(core, 'execution_audit', exc, status='DEGRADED', duration_seconds=round(time.time() - started, 3))
            raise

    async def stable_live_worker() -> None:
        next_scan = 0.0
        try:
            runtime = str(core.state.get('runtime_version') or STABILITY_VERSION)
            strict = core.state.get('strict_replay') or {}
            strict_text = (
                'Strict Replay 啟用：HTF 僅收線後可用、未來路徑只能在決策鎖定後揭露、Execution outer audit 不得回頭改參數。'
                if strict.get('htf_close_time_required') else
                'Point-in-time / fail-closed 安全模式啟用。'
            )
            await v5_runtime.robust_send_discord(
                core,
                f'✅ ETH Adaptive AI {runtime.replace("-20260809", "")} 已啟動',
                'Market scan、ordered-trade risk monitor、learning、Execution Audit、Discord polling 已拆成獨立健康域。單次外部 API timeout 會重試且禁止新開單；持久資料庫禁止 silent /tmp fallback。\n' + strict_text,
                0x3498DB,
            )
        except Exception as exc:
            _err(core, 'discord_send', exc, status='DEGRADED')

        while True:
            now = time.time()
            if now >= next_scan:
                await stable_scan()
                next_scan = now + core.SCAN_SECONDS
            await _safe_monitor(core)
            await _safe_discord_poll(core)
            await asyncio.sleep(trade_monitor.TRADE_MONITOR_SECONDS)

    core.scan = stable_scan
    core.learning_tick = stable_learning
    execution_v7.optimize_all = stable_optimize_all
    core.scan_worker = stable_live_worker
    core.state['runtime_version'] = STABILITY_VERSION
    runtime_identity.stamp(core)
    core.state['stability_mode'] = 'SUBSYSTEM_ISOLATED_FAIL_CLOSED'
    core.state['subsystem_health'] = {
        'market_scan': _health(core, 'market_scan'),
        'risk_monitor': _health(core, 'risk_monitor'),
        'learning': _health(core, 'learning'),
        'execution_audit': _health(core, 'execution_audit'),
        'discord_poll': _health(core, 'discord_poll'),
    }

    if not any(getattr(r, 'path', None) == '/api/stability' for r in core.app.router.routes):
        @core.app.get('/api/stability')
        def stability_status() -> dict[str, Any]:
            _refresh_service(core)
            return {
                'runtime': str(core.state.get('runtime_version') or STABILITY_VERSION),
                'stability_component_version': STABILITY_VERSION,
                'mode': core.state.get('stability_mode'),
                'service': core.state.get('service'),
                'global_error': core.state.get('error'),
                'analysis_stale': core.state.get('analysis_stale', False),
                'subsystems': core.state.get('subsystem_health', {}),
                'storage': core.state.get('storage', {}),
                'risk_feed_probe': core.state.get('risk_feed_probe', {}),
            }
