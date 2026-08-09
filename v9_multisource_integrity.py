from __future__ import annotations

import time
from typing import Any

import v5_runtime
import v9_multisource_derivatives as ms


def _latest_for_source(history: Any, source: str, metric: str) -> int | None:
    con = history._con()
    row = con.execute(
        'SELECT MAX(ts) FROM derivative_history WHERE source=? AND metric=?',
        (source, metric),
    ).fetchone()
    con.close()
    return int(row[0]) if row and row[0] is not None else None


def _reset_learning_generation(core: Any, source: str, reason: str) -> None:
    """Restart labels/models only when the historical source set changes.

    Market bars and already downloaded derivative caches are preserved. This prevents
    the first half of history using one feature-source set and the second half using a
    different one, which would create an artificial regime shift caused by our data
    provider rather than the market.
    """
    con = core.db()
    tables = {str(r[0]) for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
    sample_count = int(con.execute('SELECT COUNT(*) FROM learning_samples').fetchone()[0]) if 'learning_samples' in tables else 0
    cursor = int(core.get_state(v5_runtime.REPLAY_STATE_KEY, core.START_TS) or core.START_TS)
    if sample_count <= 0 and cursor <= int(core.START_TS):
        con.close()
        return

    if 'learning_samples' in tables:
        con.execute('DELETE FROM learning_samples')
    if 'learning_feature_snapshots' in tables:
        con.execute('DELETE FROM learning_feature_snapshots')
    if 'model_registry' in tables:
        con.execute("UPDATE model_registry SET status='ARCHIVED' WHERE status='CHAMPION'")
    if 'execution_registry_v7' in tables:
        con.execute("UPDATE execution_registry_v7 SET status='ARCHIVED' WHERE status='CHAMPION'")
    con.commit(); con.close()

    core.set_state(v5_runtime.REPLAY_STATE_KEY, int(core.START_TS))
    core.set_state('v5_last_train_sample_total', 0)
    core.set_state('last_train_ts_v5', 0)
    state = ms._load(core)
    state['source_generation'] = int(state.get('source_generation') or 1) + 1
    state['last_generation_reset'] = {
        'at': int(time.time()), 'source': source, 'reason': reason,
        'preserved_market_bars': True, 'preserved_derivative_cache': True,
        'cleared_only_labels_and_certifications': True,
    }
    ms._save(core, state)
    core.state.setdefault('learning', {})['source_generation_reset'] = state['last_generation_reset']


async def _source_safe_cg_forward(core: Any, metric: str, source_key: str, fn: Any, pages: int) -> dict[str, Any]:
    history = core.derivative_history
    cursor = ms._cursor(core, source_key)
    added = 0
    if not getattr(history, 'coinglass_key', ''):
        rec = ms._record(core, source_key, ok=False, cursor=cursor, error='CoinGlass key not configured')
        rec['disabled'] = True
        state = ms._load(core); state['sources'][source_key] = rec; ms._save(core, state)
        return {'source': source_key, 'added': 0, 'cursor': cursor, 'disabled': True}
    if not ms._should_probe(core, source_key):
        return {'source': source_key, 'added': 0, 'cursor': cursor, 'disabled': True, 'probe_deferred': True}
    try:
        for _ in range(max(1, pages)):
            if cursor >= int(ms.time.time()):
                break
            window_end = min(int(ms.time.time()), cursor + 999 * ms.INTERVAL)
            n = await fn(cursor, window_end)
            added += int(n)
            # Only CoinGlass rows can advance a CoinGlass cursor. Gate rows stored in
            # the same generic metric must never make this provider look complete.
            latest = _latest_for_source(history, 'coinglass', metric)
            if latest is None or latest < cursor:
                cursor = window_end + ms.INTERVAL
            else:
                cursor = max(window_end + ms.INTERVAL, int(latest) + ms.INTERVAL)
        ms._record(core, source_key, ok=True, cursor=cursor, added=added)
        return {'source': source_key, 'added': added, 'cursor': cursor, 'disabled': ms._disabled(core, source_key)}
    except Exception as exc:
        ms._record(core, source_key, ok=False, cursor=cursor, error=str(exc), added=added)
        return {'source': source_key, 'added': added, 'cursor': cursor, 'error': str(exc), 'disabled': ms._disabled(core, source_key)}


def install(core: Any) -> None:
    original_record = ms._record

    def generation_safe_record(c: Any, source: str, **kwargs: Any) -> dict[str, Any]:
        was_disabled = ms._disabled(c, source)
        rec = original_record(c, source, **kwargs)
        is_disabled = bool(rec.get('disabled'))
        if not was_disabled and is_disabled:
            _reset_learning_generation(c, source, str(rec.get('disabled_reason') or rec.get('last_error') or 'source disabled'))
        return rec

    ms._record = generation_safe_record
    ms._cg_forward = _source_safe_cg_forward
    policy = core.state.setdefault('strict_replay', {}).setdefault('multisource_policy', {})
    policy['source_isolated_readiness_cursors'] = True
    policy['source_set_frozen_per_replay_generation'] = True
    policy['source_change_resets_labels_not_raw_market_data'] = True
