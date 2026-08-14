from __future__ import annotations

"""Authoritative causal feature loader for V30 autonomous discovery.

Schema-6 replay normalizes the shared feature JSON into learning_feature_snapshots and
stores only an '@timestamp' reference in each of the 14 legacy sample rows. Autonomous
research must therefore read the snapshot table directly; parsing learning_samples.features
would yield empty vectors. This overlay makes the snapshot table authoritative and refuses
to train on degenerate/empty feature matrices.
"""

import json
import math
import time
from typing import Any

import numpy as np

SCHEMA = 35
STATE_KEY = 'v35_autonomous_feature_integrity'
_INSTALLED = False


def install(production: Any, autonomous: Any) -> None:
    global _INSTALLED
    if _INSTALLED:
        return
    _INSTALLED = True
    core = production.core

    def load_feature_snapshots(c: Any) -> dict[str, Any]:
        con = c.db()
        try:
            tables = {str(r[0]) for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
            if 'learning_feature_snapshots' not in tables or 'learning_samples' not in tables:
                c.state[STATE_KEY] = {'schema': SCHEMA, 'status': 'WAITING_SNAPSHOT_TABLE', 'updated_at': int(time.time())}
                return {}
            rows = con.execute('''
                SELECT fs.ts, fs.features, MAX(ls.source_quality) AS quality, COUNT(ls.rowid) AS sample_rows
                FROM learning_feature_snapshots fs
                JOIN learning_samples ls ON ls.ts=fs.ts
                WHERE fs.ts>=? AND fs.ts<?
                GROUP BY fs.ts,fs.features
                ORDER BY fs.ts
            ''', (autonomous.RESEARCH_START_TS, autonomous.RESEARCH_END_EXCLUSIVE_TS)).fetchall()
        finally:
            con.close()
        if len(rows) < 5000:
            c.state[STATE_KEY] = {'schema': SCHEMA, 'status': 'WAITING_ENOUGH_CAUSAL_SNAPSHOTS', 'snapshots': len(rows), 'updated_at': int(time.time())}
            return {}
        ts = np.asarray([int(r[0]) for r in rows], dtype=np.int64)
        x = np.empty((len(rows), len(autonomous.FEATURE_NAMES)), dtype=np.float32)
        quality = np.asarray([autonomous._finite(r[2], 0.0) for r in rows], dtype=np.float32)
        invalid_json = 0; wrong_group_rows = 0
        expected_legacy_rows = 14
        for i, r in enumerate(rows):
            if int(r[3] or 0) != expected_legacy_rows:
                wrong_group_rows += 1
            try:
                raw = r[1].decode('utf-8') if isinstance(r[1], (bytes, bytearray)) else str(r[1])
                f = json.loads(raw)
                if not isinstance(f, dict):
                    raise ValueError('feature snapshot is not a dict')
            except Exception:
                invalid_json += 1; f = {}
            x[i] = np.asarray([autonomous._finite(f.get(name), 0.0) for name in autonomous.FEATURE_NAMES], dtype=np.float32)
        finite = bool(np.isfinite(x).all())
        variances = np.nanvar(x.astype(np.float64), axis=0)
        varying = int(np.sum(variances > 1e-14))
        nonzero = int(np.sum(np.any(np.abs(x) > 1e-12, axis=0)))
        status = 'VALID' if finite and invalid_json == 0 and wrong_group_rows == 0 and varying >= 6 and nonzero >= 6 else 'FAILED'
        detail = {
            'schema': SCHEMA, 'status': status, 'snapshots': len(rows),
            'feature_count': len(autonomous.FEATURE_NAMES), 'varying_features': varying,
            'nonzero_features': nonzero, 'invalid_json': invalid_json,
            'wrong_legacy_rows_per_snapshot': wrong_group_rows,
            'source': 'learning_feature_snapshots.features',
            'learning_samples_features_reference_only': True,
            'manual_regime_feature_codes_excluded': sorted(autonomous.EXCLUDED_FEATURES),
            'updated_at': int(time.time()),
        }
        c.state[STATE_KEY] = detail
        if status != 'VALID':
            return {}
        return {'ts': ts, 'x': x, 'quality': quality}

    autonomous._load_feature_snapshots = load_feature_snapshots
    core.state[STATE_KEY] = {
        'schema': SCHEMA,
        'status': 'INSTALLED_WAITING_REPLAY',
        'source': 'learning_feature_snapshots.features',
        'normalized_at_timestamp_refs_are_never_parsed_as_json': True,
        'degenerate_feature_matrix_fail_closed': True,
        'updated_at': int(time.time()),
    }

    if not any(getattr(r, 'path', None) == '/api/v35/autonomous-features' for r in core.app.router.routes):
        @core.app.get('/api/v35/autonomous-features')
        def feature_integrity() -> dict[str, Any]:
            return dict(core.state.get(STATE_KEY) or {})
