from __future__ import annotations

import json
import math
import os
from typing import Any

import adaptive_v5 as signal


STORE_VERSION = '8.0.2-20260809'
MODEL_MAX_ROWS = max(24000, min(90000, int(os.getenv('STRICT_MODEL_MAX_ROWS', '60000'))))
MODEL_RECENT_ROWS = max(6000, min(MODEL_MAX_ROWS // 2, int(os.getenv('STRICT_MODEL_RECENT_ROWS', '20000'))))

_ORIGINAL_SAMPLES = signal.ModelStore.samples


def _ensure(con: Any) -> None:
    con.execute('''CREATE TABLE IF NOT EXISTS learning_feature_snapshots(
        ts INTEGER PRIMARY KEY,
        features TEXT NOT NULL
    )''')
    con.commit()


def _decode(rows: list[Any]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for x in rows:
        raw = x[4]
        if raw is None or (isinstance(raw, str) and raw.startswith('@')):
            # A dangling normalized reference is a data-integrity error. Silently
            # replacing it with zeros would teach the model fabricated features.
            raise RuntimeError(f'missing normalized feature snapshot for learning sample ts={x[0]}')
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode('utf-8')
        out.append({
            'ts': int(x[0]), 'direction': x[1], 'regime': x[2], 'phase': x[3],
            'features': json.loads(raw), 'success': int(x[5]), 'pnl_r': float(x[6]),
            'mfe_r': float(x[7]), 'mae_r': float(x[8]), 'source_quality': float(x[9]),
        })
    return out


def normalized_add_sample(self: Any, r: dict[str, Any]) -> None:
    _ensure(self.con)
    ts = int(r['ts'])
    features = json.dumps(r['features'], separators=(',', ':'))
    self.con.execute(
        'INSERT OR IGNORE INTO learning_feature_snapshots(ts,features) VALUES(?,?)',
        (ts, features),
    )
    # The same feature snapshot is shared by all strategy x direction outcomes at T.
    # A tiny reference avoids storing the identical JSON fourteen times.
    self.con.execute(
        'INSERT OR IGNORE INTO learning_samples(ts,strategy,direction,regime,phase,features,success,pnl_r,mfe_r,mae_r,source_quality) VALUES(?,?,?,?,?,?,?,?,?,?,?)',
        (ts, r['strategy'], r['direction'], r['regime'], r['phase'], f'@{ts}', int(r['success']),
         float(r['pnl_r']), float(r['mfe_r']), float(r['mae_r']), float(r.get('source_quality', 100.0))),
    )


def _where(direction: str | None) -> tuple[str, tuple[Any, ...]]:
    if direction:
        return 'strategy=? AND direction=?', (direction,)
    return 'strategy=?', ()


def full_span_samples(self: Any, strategy: str, limit: int = MODEL_MAX_ROWS, direction: str | None = None) -> list[dict[str, Any]]:
    """Return a bounded training set that still spans the entire 2020->now history.

    The old implementation used ORDER BY ts DESC LIMIT 30000, silently discarding
    early cycles. Here old history is deterministically time-decimated while the most
    recent block stays dense. Chronological order is preserved for purged OOS folds.
    """
    _ensure(self.con)
    cap = max(4000, min(int(limit or MODEL_MAX_ROWS), MODEL_MAX_ROWS))
    condition = 'ls.strategy=?' + (' AND ls.direction=?' if direction else '')
    args: list[Any] = [strategy] + ([direction] if direction else [])
    total = int(self.con.execute(
        'SELECT COUNT(*) FROM learning_samples ls WHERE ' + condition,
        args,
    ).fetchone()[0])
    select_cols = (
        'ls.ts,ls.direction,ls.regime,ls.phase,'
        'CASE WHEN fs.features IS NOT NULL THEN fs.features ELSE ls.features END AS feature_json,'
        'ls.success,ls.pnl_r,ls.mfe_r,ls.mae_r,ls.source_quality'
    )
    join = ' FROM learning_samples ls LEFT JOIN learning_feature_snapshots fs ON fs.ts=ls.ts '
    if total <= cap:
        rows = self.con.execute(
            'SELECT ' + select_cols + join + 'WHERE ' + condition + ' ORDER BY ls.ts', args,
        ).fetchall()
        self._strict_sampling_info = {
            'strategy': strategy, 'direction': direction, 'db_rows': total, 'model_rows': len(rows),
            'mode': 'ALL_ROWS', 'max_rows': cap,
        }
        return _decode(rows)

    recent_n = min(MODEL_RECENT_ROWS, max(6000, cap // 3))
    boundary_row = self.con.execute(
        'SELECT ls.ts FROM learning_samples ls WHERE ' + condition + ' ORDER BY ls.ts DESC LIMIT 1 OFFSET ?',
        args + [recent_n - 1],
    ).fetchone()
    if not boundary_row:
        return _ORIGINAL_SAMPLES(self, strategy, cap, direction)
    boundary = int(boundary_row[0])
    old_count = int(self.con.execute(
        'SELECT COUNT(*) FROM learning_samples ls WHERE ' + condition + ' AND ls.ts<?',
        args + [boundary],
    ).fetchone()[0])
    old_target = max(1, cap - recent_n)
    stride = max(1, int(math.ceil(old_count / old_target)))

    # SQLite window numbering lets us decimate only the older section without first
    # materializing and JSON-decoding the entire six-year history in Python.
    old_sql = (
        'WITH ranked AS (SELECT ' + select_cols + ',ROW_NUMBER() OVER (ORDER BY ls.ts) AS rn' + join +
        'WHERE ' + condition + ' AND ls.ts<?) '
        'SELECT ts,direction,regime,phase,feature_json,success,pnl_r,mfe_r,mae_r,source_quality '
        'FROM ranked WHERE ((rn-1) % ?) = 0 ORDER BY ts'
    )
    old_rows = self.con.execute(old_sql, args + [boundary, stride]).fetchall()
    recent_rows = self.con.execute(
        'SELECT ' + select_cols + join + 'WHERE ' + condition + ' AND ls.ts>=? ORDER BY ls.ts',
        args + [boundary],
    ).fetchall()
    rows = list(old_rows) + list(recent_rows)
    if len(rows) > cap:
        # Only trim from the oldest decimated side. Never remove the dense recent block.
        excess = len(rows) - cap
        rows = list(old_rows)[excess:] + list(recent_rows)
    self._strict_sampling_info = {
        'strategy': strategy, 'direction': direction, 'db_rows': total, 'model_rows': len(rows),
        'mode': 'FULL_SPAN_DECIMATED_PLUS_DENSE_RECENT', 'max_rows': cap,
        'recent_rows': len(recent_rows), 'old_stride': stride,
        'span_start_ts': int(rows[0][0]) if rows else None,
        'span_end_ts': int(rows[-1][0]) if rows else None,
    }
    return _decode(rows)


def install(core: Any) -> None:
    con = core.db(); _ensure(con); con.close()
    signal.ModelStore.add_sample = normalized_add_sample
    signal.ModelStore.samples = full_span_samples
    core.state.setdefault('strict_replay', {})['training_store'] = {
        'version': STORE_VERSION,
        'normalized_feature_snapshots': True,
        'full_span_sampling': True,
        'max_model_rows_per_strategy_direction': MODEL_MAX_ROWS,
        'dense_recent_rows': MODEL_RECENT_ROWS,
        'old_history_policy': 'deterministic temporal decimation; never tail-only truncation',
    }
    core.state['runtime_version'] = STORE_VERSION
    core.state['strict_replay']['runtime'] = STORE_VERSION
    core.app.version = '8.0.2'

    if not any(getattr(r, 'path', None) == '/api/v9/training-store' for r in core.app.router.routes):
        @core.app.get('/api/v9/training-store')
        def training_store_status() -> dict[str, Any]:
            con = core.db(); _ensure(con)
            samples = int(con.execute('SELECT COUNT(*) FROM learning_samples').fetchone()[0])
            snapshots = int(con.execute('SELECT COUNT(*) FROM learning_feature_snapshots').fetchone()[0])
            oldest = con.execute('SELECT MIN(ts),MAX(ts) FROM learning_feature_snapshots').fetchone()
            con.close()
            return {
                'runtime': STORE_VERSION,
                'learning_samples': samples,
                'unique_feature_snapshots': snapshots,
                'oldest_feature_ts': oldest[0] if oldest else None,
                'newest_feature_ts': oldest[1] if oldest else None,
                'max_model_rows_per_strategy_direction': MODEL_MAX_ROWS,
                'dense_recent_rows': MODEL_RECENT_ROWS,
                'rule': 'training is bounded for CPU, but old cycles are time-decimated across the full history instead of discarded',
            }
