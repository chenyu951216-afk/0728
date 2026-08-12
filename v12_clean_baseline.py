from __future__ import annotations

import time
import uuid
from typing import Any

import execution_v7
import v5_runtime
import runtime_identity

VERSION = runtime_identity.RUNTIME_VERSION
BASELINE_ID = 'final-clean-baseline-20260809-v1'
STATE_KEY = 'final_dataset_baseline_v1'


def _table_exists(con: Any, name: str) -> bool:
    return bool(con.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)).fetchone())


def _count(con: Any, table: str) -> int:
    if not _table_exists(con, table):
        return 0
    return int(con.execute(f'SELECT COUNT(*) FROM {table}').fetchone()[0] or 0)


def _raw_counts(core: Any) -> dict[str, int]:
    con = core.db()
    try:
        return {
            'market_bars': _count(con, 'market_bars'),
            'derivative_history': _count(con, 'derivative_history'),
            'learning_samples': _count(con, 'learning_samples'),
            'feature_snapshots': _count(con, 'learning_feature_snapshots'),
            'signals': _count(con, 'signals'),
        }
    finally:
        con.close()


def _read_marker(core: Any) -> dict[str, Any] | None:
    raw = core.get_state(STATE_KEY, None)
    return dict(raw) if isinstance(raw, dict) else None


def _write_marker(core: Any, marker: dict[str, Any]) -> None:
    core.set_state(STATE_KEY, marker)
    core.state['dataset_baseline'] = marker


def initialize_or_classify(core: Any) -> dict[str, Any]:
    marker = _read_marker(core)
    counts = _raw_counts(core)
    if marker and marker.get('baseline_id') == BASELINE_ID and marker.get('clean') is True:
        marker['current_counts'] = counts
        core.state['dataset_baseline'] = marker
        return marker

    # The only automatic way to claim a CLEAN dataset is to see a genuinely empty raw
    # database before workers start writing new market/derivative rows. Existing raw
    # caches from older versions are deliberately not retro-certified as clean.
    raw_empty = counts['market_bars'] == 0 and counts['derivative_history'] == 0
    learned_empty = counts['learning_samples'] == 0 and counts['feature_snapshots'] == 0
    if raw_empty and learned_empty:
        marker = {
            'baseline_id': BASELINE_ID,
            'dataset_id': str(uuid.uuid4()),
            'clean': True,
            'status': 'CLEAN',
            'created_at': int(time.time()),
            'created_by_runtime': VERSION,
            'reason': 'empty persistent database initialized by Final Clean Baseline',
            'current_counts': counts,
        }
        _write_marker(core, marker)
        return marker

    marker = {
        'baseline_id': BASELINE_ID,
        'dataset_id': None,
        'clean': False,
        'status': 'LEGACY_CARRYOVER',
        'created_at': int(time.time()),
        'created_by_runtime': VERSION,
        'reason': 'raw market/derivative cache existed before Final Clean Baseline; formal certification is fail-closed until one clean reset',
        'current_counts': counts,
        'required_action': 'reset the persistent DB once, then redeploy the same Final Clean Baseline runtime',
    }
    core.state['dataset_baseline'] = marker
    return marker


def _is_clean(core: Any) -> bool:
    marker = _read_marker(core)
    return bool(marker and marker.get('baseline_id') == BASELINE_ID and marker.get('clean') is True)


def _install_certification_gate(core: Any) -> None:
    original_train = v5_runtime.train_v5

    def clean_train(c: Any, *args: Any, **kwargs: Any):
        if not _is_clean(c):
            c.state.setdefault('learning', {})['dataset_provenance_gate'] = {
                'ready': False,
                'reason': 'Final Clean Baseline not established; legacy raw cache cannot certify a Champion',
            }
            return []
        c.state.setdefault('learning', {})['dataset_provenance_gate'] = {
            'ready': True,
            'reason': 'Final Clean Baseline verified',
        }
        return original_train(c, *args, **kwargs)

    v5_runtime.train_v5 = clean_train

    original_optimize = execution_v7.optimize_all

    def clean_optimize(c: Any, *args: Any, **kwargs: Any):
        if not _is_clean(c):
            c.state.setdefault('execution_learning', {})['dataset_provenance_gate'] = {
                'ready': False,
                'reason': 'Final Clean Baseline not established',
            }
            return []
        return original_optimize(c, *args, **kwargs)

    execution_v7.optimize_all = clean_optimize

    original_create = core.create_signal

    def clean_create(*args: Any, **kwargs: Any):
        if not _is_clean(core):
            core.state.setdefault('analysis', {}).setdefault('selection', {})['tradeable'] = False
            core.state['analysis']['selection']['reason'] = 'Final Clean Baseline not established; new paper signals are fail-closed'
            return None
        return original_create(*args, **kwargs)

    core.create_signal = clean_create


def install(core: Any) -> None:
    marker = initialize_or_classify(core)
    _install_certification_gate(core)
    strict = core.state.setdefault('strict_replay', {})
    strict['dataset_provenance'] = {
        'runtime': VERSION,
        'baseline_id': BASELINE_ID,
        'clean': bool(marker.get('clean')),
        'legacy_raw_cache_cannot_be_retro_certified': True,
        'champion_certification_requires_clean_baseline': True,
        'new_signal_requires_clean_baseline': True,
    }
    core.state['runtime_version'] = VERSION
    runtime_identity.stamp(core)

    if not any(getattr(r, 'path', None) == '/api/v12/baseline' for r in core.app.router.routes):
        @core.app.get('/api/v12/baseline')
        def baseline_status() -> dict[str, Any]:
            current = _read_marker(core)
            counts = _raw_counts(core)
            if not current:
                current = initialize_or_classify(core)
            return {
                **current,
                'current_counts': counts,
                'certification_allowed': _is_clean(core),
                'rule': 'only a database observed empty at Final Clean Baseline initialization can certify new Champions',
            }
