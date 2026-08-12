from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any


STORAGE_SCHEMA = 2
IDENTITY_FILE = '.eth_adaptive_storage_identity.json'


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


def _candidate_databases(parent: Path, current: Path) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    if not parent.exists():
        return out
    try:
        files = list(parent.iterdir())
    except Exception:
        return out
    for p in files:
        if not p.is_file():
            continue
        name = p.name.lower()
        if not (name.endswith('.db') or name.endswith('.sqlite') or name.endswith('.sqlite3')):
            continue
        try:
            out.append({
                'path': str(p),
                'name': p.name,
                'size_bytes': int(p.stat().st_size),
                'current': p.resolve() == current.resolve(),
            })
        except Exception:
            continue
    out.sort(key=lambda x: (not x['current'], -x['size_bytes'], x['name']))
    return out[:20]


def install_early(core: Any) -> None:
    """Prevent app.db() from silently switching production storage to /tmp."""
    if getattr(core, '_strict_db_guard_installed', False):
        return
    original_db = core.db

    def strict_db():
        expected = str(core.DB_PATH)
        production = expected.startswith('/data/')
        parent = Path(expected).expanduser().parent
        if production and (not parent.exists() or not os.access(parent, os.W_OK)):
            raise RuntimeError(f'persistent database path is unavailable or not writable: {expected}')
        con = original_db()
        actual = str(core.DB_PATH)
        if production and actual != expected:
            try:
                con.close()
            finally:
                core.DB_PATH = expected
            raise RuntimeError(f'database fallback blocked: expected {expected}, app attempted {actual}')
        return con

    core.db = strict_db
    core._strict_db_guard_installed = True


def storage_status(core: Any, update_identity: bool = True) -> dict[str, Any]:
    db_path = Path(str(core.DB_PATH)).expanduser()
    parent = db_path.parent if str(db_path.parent) not in ('', '.') else Path.cwd()
    exists = db_path.exists()
    writable = os.access(parent, os.W_OK) if parent.exists() else False
    persistent_mount = _is_real_mount(parent)
    configured_for_data = str(db_path).startswith('/data/')
    ephemeral_path = str(db_path).startswith('/tmp/') or not db_path.is_absolute()
    candidates = _candidate_databases(parent, db_path)

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
    ci_or_test = str(db_path).startswith('/tmp/')
    persistent_ok = bool(ci_or_test or (configured_for_data and persistent_mount and writable and not db_error))

    identity_path = parent / IDENTITY_FILE
    previous: dict[str, Any] = {}
    if identity_path.exists():
        try:
            previous = json.loads(identity_path.read_text(encoding='utf-8'))
        except Exception:
            previous = {}
    previous_bars = int(previous.get('market_bars') or 0)
    unexpected_reset = bool(previous_bars >= 5000 and bars < max(100, int(previous_bars * .10)))
    larger_alternative = next((x for x in candidates if not x['current'] and x['size_bytes'] > max(int(db_path.stat().st_size) if exists else 0, 0) * 3 and x['size_bytes'] > 5_000_000), None)
    possible_db_mismatch = bool(bars < 100 and larger_alternative)

    healthy = bool((persistent_ok or not required) and not db_error and not unexpected_reset and not possible_db_mismatch)
    if db_error:
        reason = db_error
    elif unexpected_reset:
        reason = f'current database unexpectedly lost most historical rows: previous bars={previous_bars}, current bars={bars}'
    elif possible_db_mismatch:
        reason = f'current database is nearly empty but a larger SQLite file exists at {larger_alternative["path"]}'
    elif persistent_ok and not ci_or_test:
        reason = 'persistent volume mounted and current database is readable'
    elif ci_or_test:
        reason = 'test/local database'
    elif configured_for_data and not persistent_mount:
        reason = 'configured /data path is not detected as a mounted persistent volume'
    elif ephemeral_path or not configured_for_data:
        reason = 'DATABASE_PATH is not configured under /data'
    else:
        reason = 'storage unavailable'

    if update_identity and healthy and persistent_ok and not ci_or_test:
        try:
            identity_path.write_text(json.dumps({
                'database_path': str(db_path),
                'market_bars': bars,
                'learning_samples': samples,
                'oldest_market_ts': oldest,
                'newest_market_ts': newest,
                'database_size_bytes': int(db_path.stat().st_size) if exists else 0,
            }, ensure_ascii=False), encoding='utf-8')
        except Exception:
            pass

    recovery = None
    if possible_db_mismatch:
        recovery = f'Do not delete or redownload. Check DATABASE_PATH against the larger existing file: {larger_alternative["path"]}'
    elif unexpected_reset:
        recovery = 'Do not redownload yet. The storage guard stopped learning because the persistent DB row count dropped unexpectedly.'
    elif not persistent_ok and not ci_or_test:
        recovery = 'Verify the Zeabur Volume mount path and keep DATABASE_PATH pointed to the mounted SQLite file.'

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
        'causal_sample_schema': core.get_state('point_in_time_sample_schema', None),
        # Persistence-only compatibility marker. New UI/API consumers use
        # causal_sample_schema and never present this as an active runtime version.
        'legacy_compatibility_schema': core.get_state('v5_sample_schema', None),
        'db_error': db_error,
        'unexpected_reset': unexpected_reset,
        'possible_db_mismatch': possible_db_mismatch,
        'candidate_databases': candidates,
        'identity_previous_market_bars': previous_bars or None,
        'reason': reason,
        'recovery': recovery,
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
        if not status['healthy']:
            core.state.setdefault('learning', {})['storage_blocked'] = True
            core.state['learning']['storage_reason'] = status['reason']
            return
        await original_learning_tick()
        refresh()
    core.learning_tick = guarded_learning_tick

    original_create_signal = core.create_signal
    def guarded_create_signal(analysis, m15):
        status = refresh()
        if not status['healthy']:
            return None
        return original_create_signal(analysis, m15)
    core.create_signal = guarded_create_signal

    if not any(getattr(r, 'path', None) == '/api/storage/status' for r in core.app.router.routes):
        @core.app.get('/api/storage/status')
        def api_storage_status() -> dict[str, Any]:
            return refresh()
