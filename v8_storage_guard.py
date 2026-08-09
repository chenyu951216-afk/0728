from __future__ import annotations

import os
from pathlib import Path
from typing import Any


STORAGE_SCHEMA = 1


def _is_real_mount(path: Path) -> bool:
    try:
        resolved = path.resolve()
        if os.path.ismount(resolved):
            return True
        mountinfo = Path('/proc/self/mountinfo')
        if mountinfo.exists():
            target = str(resolved)
            for line in mountinfo.read_text(encoding='utf-8', errors='ignore').splitlines():
                parts = line.split()
                if len(parts) > 4 and parts[4] == target:
                    return True
    except Exception:
        pass
    return False


def storage_status(core: Any) -> dict[str, Any]:
    db_path = Path(str(core.DB_PATH)).expanduser()
    parent = db_path.parent if str(db_path.parent) not in ('', '.') else Path.cwd()
    exists = db_path.exists()
    writable = os.access(parent, os.W_OK) if parent.exists() else False
    persistent_mount = _is_real_mount(parent)
    configured_for_data = str(db_path).startswith('/data/')
    ephemeral_path = str(db_path).startswith('/tmp/') or not db_path.is_absolute()

    bars = samples = champions = executions = 0
    coverage = 0.0
    oldest = newest = None
    db_error = None
    try:
        con = core.db()
        bars = int(con.execute('SELECT COUNT(*) FROM market_bars').fetchone()[0])
        row = con.execute("SELECT MIN(ts),MAX(ts) FROM market_bars WHERE asset='ETH'").fetchone()
        oldest, newest = row[0], row[1]
        samples = int(con.execute('SELECT COUNT(*) FROM learning_samples').fetchone()[0])
        champions = int(con.execute("SELECT COUNT(*) FROM model_registry WHERE status='CHAMPION'").fetchone()[0])
        if con.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='execution_registry_v7'").fetchone():
            executions = int(con.execute("SELECT COUNT(*) FROM execution_registry_v7 WHERE status='CHAMPION'").fetchone()[0])
        coverage = float(core.bootstrap_progress(con).get('overall', 0.0))
        con.close()
    except Exception as exc:
        db_error = str(exc)

    required = str(os.getenv('REQUIRE_PERSISTENT_STORAGE', 'true')).lower() not in ('0', 'false', 'no')
    # CI/local test databases are allowed outside /data. Production /data must be an actual mounted volume.
    ci_or_test = str(db_path).startswith('/tmp/')
    persistent_ok = bool(ci_or_test or (configured_for_data and persistent_mount and writable and not db_error))
    healthy = bool((persistent_ok or not required) and not db_error)
    reason = 'persistent volume mounted' if persistent_ok and not ci_or_test else 'test/local database' if ci_or_test else 'persistent /data volume is not mounted' if configured_for_data and not persistent_mount else 'DATABASE_PATH must point to /data/eth_adaptive.db' if ephemeral_path or not configured_for_data else db_error or 'storage unavailable'

    return {
        'schema': STORAGE_SCHEMA,
        'healthy': healthy,
        'persistent_ok': persistent_ok,
        'required': required,
        'database_path': str(db_path),
        'database_exists': exists,
        'database_size_bytes': int(db_path.stat().st_size) if exists else 0,
        'parent': str(parent),
        'parent_writable': writable,
        'parent_is_mount': persistent_mount,
        'market_bars': bars,
        'learning_samples': samples,
        'signal_champions': champions,
        'execution_champions': executions,
        'historical_price_coverage': coverage,
        'oldest_market_ts': oldest,
        'newest_market_ts': newest,
        'point_in_time_sample_schema': core.get_state('point_in_time_sample_schema', None),
        'v5_sample_schema': core.get_state('v5_sample_schema', None),
        'db_error': db_error,
        'reason': reason,
        'recovery': 'Mount a Zeabur Volume at /data and keep DATABASE_PATH=/data/eth_adaptive.db. Historical candles will then rebuild once and persist across redeploys.' if not persistent_ok and not ci_or_test else None,
    }


def install(core: Any) -> None:
    def refresh() -> dict[str, Any]:
        status = storage_status(core)
        core.state['storage'] = status
        return status

    refresh()

    original_learning_tick = core.learning_tick
    async def guarded_learning_tick() -> None:
        status = refresh()
        if status['required'] and not status['persistent_ok']:
            core.state.setdefault('learning', {})['storage_blocked'] = True
            core.state['learning']['storage_reason'] = status['reason']
            return
        await original_learning_tick()
        refresh()
    core.learning_tick = guarded_learning_tick

    original_create_signal = core.create_signal
    def guarded_create_signal(analysis, m15):
        status = refresh()
        if status['required'] and not status['persistent_ok']:
            return None
        return original_create_signal(analysis, m15)
    core.create_signal = guarded_create_signal

    if not any(getattr(r, 'path', None) == '/api/storage/status' for r in core.app.router.routes):
        @core.app.get('/api/storage/status')
        def api_storage_status() -> dict[str, Any]:
            return refresh()
