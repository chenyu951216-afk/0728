from __future__ import annotations

from typing import Any

import v9_multisource_derivatives as ms


def _latest_for_source(history: Any, source: str, metric: str) -> int | None:
    con = history._con()
    row = con.execute(
        'SELECT MAX(ts) FROM derivative_history WHERE source=? AND metric=?',
        (source, metric),
    ).fetchone()
    con.close()
    return int(row[0]) if row and row[0] is not None else None


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
            # Critical: only this provider may advance its own readiness cursor.
            # Gate rows in the same oi_usd/liq metric must never make CoinGlass look ready.
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
    ms._cg_forward = _source_safe_cg_forward
    core.state.setdefault('strict_replay', {}).setdefault('multisource_policy', {})['source_isolated_readiness_cursors'] = True
