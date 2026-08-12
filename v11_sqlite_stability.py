from __future__ import annotations

import asyncio
import sqlite3
import time
from pathlib import Path
from types import MethodType
from typing import Any

import adaptive_v5
import v5_runtime
import v7_runtime
import v8_stability
import v7_trade_monitor as trade_monitor
import runtime_identity


VERSION = runtime_identity.RUNTIME_VERSION
BUSY_TIMEOUT_MS = 5000
SAMPLE_COMMIT_EVERY = 14  # one complete strategy×direction decision snapshot
DISCORD_POLL_TIMEOUT_SECONDS = 8


def _configure_connection(con: sqlite3.Connection) -> sqlite3.Connection:
    con.row_factory = sqlite3.Row
    con.execute(f'PRAGMA busy_timeout={BUSY_TIMEOUT_MS}')
    con.execute('PRAGMA synchronous=NORMAL')
    con.execute('PRAGMA temp_store=MEMORY')
    return con


def _install_light_connections(core: Any) -> None:
    # All schema/WAL migrations have already run before this final layer. Runtime
    # connections must be lightweight: repeatedly executing journal_mode/DDL on every
    # read or state write can itself contend with the replay writer.
    core.db().close()
    core.derivative_history.ensure_schema()

    def light_db() -> sqlite3.Connection:
        path = Path(core.DB_PATH)
        path.parent.mkdir(parents=True, exist_ok=True)
        con = sqlite3.connect(path, timeout=BUSY_TIMEOUT_MS / 1000.0, check_same_thread=False)
        return _configure_connection(con)

    def derivative_con(self: Any) -> sqlite3.Connection:
        path = Path(self.db_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        con = sqlite3.connect(path, timeout=BUSY_TIMEOUT_MS / 1000.0, check_same_thread=False)
        return _configure_connection(con)

    core.db = light_db
    core.derivative_history._con = MethodType(derivative_con, core.derivative_history)


def _install_short_sample_transactions() -> None:
    if getattr(adaptive_v5.ModelStore.add_sample, '_v11_short_txn', False):
        return
    original = adaptive_v5.ModelStore.add_sample

    def add_sample_short_txn(self: Any, row: dict[str, Any]) -> None:
        original(self, row)
        n = int(getattr(self, '_v11_pending_samples', 0)) + 1
        if n >= SAMPLE_COMMIT_EVERY:
            self.con.commit()
            n = 0
        self._v11_pending_samples = n

    add_sample_short_txn._v11_short_txn = True  # type: ignore[attr-defined]
    adaptive_v5.ModelStore.add_sample = add_sample_short_txn


def _set_running_health(core: Any) -> None:
    h = v8_stability._health(core, 'learning')
    now = int(time.time())
    h.update({
        'status': 'RUNNING',
        'started_at': h.get('started_at') or now,
        'heartbeat_at': now,
        'last_error': None if not h.get('consecutive_errors') else h.get('last_error'),
    })
    core.state.setdefault('learning', {})['runtime_status'] = 'RUNNING'
    core.state['learning']['runtime_heartbeat_at'] = now


def _install_learning_health(core: Any) -> None:
    original_learning = core.learning_tick

    async def visible_learning() -> None:
        _set_running_health(core)
        try:
            await original_learning()
        finally:
            h = v8_stability._health(core, 'learning')
            h['heartbeat_at'] = int(time.time())
            core.state.setdefault('learning', {})['runtime_status'] = h.get('status', 'RUNNING')

    core.learning_tick = visible_learning


def _install_independent_live_loops(core: Any) -> None:
    async def scan_loop() -> None:
        while True:
            started = time.monotonic()
            try:
                await core.scan()
            except Exception as exc:
                v8_stability._err(core, 'market_scan', exc, status='DEGRADED')
            elapsed = time.monotonic() - started
            await asyncio.sleep(max(1.0, float(core.SCAN_SECONDS) - elapsed))

    async def risk_loop() -> None:
        while True:
            await v8_stability._safe_monitor(core)
            await asyncio.sleep(max(1.0, float(trade_monitor.TRADE_MONITOR_SECONDS)))

    async def discord_loop() -> None:
        while True:
            try:
                await asyncio.wait_for(v8_stability._safe_discord_poll(core), timeout=DISCORD_POLL_TIMEOUT_SECONDS)
            except asyncio.TimeoutError:
                v8_stability._err(core, 'discord_poll', 'Discord poll timeout; isolated from market/risk loops', status='RETRYING')
            except Exception as exc:
                v8_stability._err(core, 'discord_poll', exc, status='DEGRADED')
            await asyncio.sleep(max(1.0, float(trade_monitor.TRADE_MONITOR_SECONDS)))

    async def independent_live_worker() -> None:
        try:
            await v7_runtime.maybe_boot_notice(core)
        except Exception as exc:
            v8_stability._err(core, 'discord_send', exc, status='DEGRADED')
        tasks = [
            asyncio.create_task(scan_loop(), name='market-scan-loop'),
            asyncio.create_task(risk_loop(), name='ordered-risk-loop'),
            asyncio.create_task(discord_loop(), name='discord-poll-loop'),
        ]
        try:
            await asyncio.gather(*tasks)
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    core.scan_worker = independent_live_worker


def install(core: Any) -> None:
    _install_light_connections(core)
    _install_short_sample_transactions()
    _install_learning_health(core)
    _install_independent_live_loops(core)

    execution = v8_stability._health(core, 'execution_audit')
    if execution.get('status') == 'BOOTING':
        execution['status'] = 'WAITING_FOR_SIGNAL_CHAMPION'

    strict = core.state.setdefault('strict_replay', {})
    strict['sqlite_stability'] = {
        'runtime': VERSION,
        'wal_schema_initialized_once': True,
        'runtime_connections_do_not_repeat_wal_or_ddl': True,
        'busy_timeout_ms': BUSY_TIMEOUT_MS,
        'sample_commit_every': SAMPLE_COMMIT_EVERY,
        'discord_poll_isolated_from_market_and_risk_loops': True,
        'learning_health_reports_running': True,
        'single_modern_boot_notice_preserved': True,
    }
    core.state['runtime_version'] = VERSION
    runtime_identity.stamp(core)
