from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import v48_runtime_continuity as v48


class FakeCore:
    def __init__(self, path: Path):
        self.DB_PATH = str(path)
        self.state = {'storage': {'healthy': True, 'market_bars': 123, 'learning_samples': 456}}


def test_resource_worker_schedule_only(monkeypatch):
    throughput = SimpleNamespace(MAX_WORKERS=3)
    worker = v48._safe_workers_factory(throughput)
    monkeypatch.setattr(v48, '_memory', lambda: {'ratio': .50}); assert worker() == 3
    monkeypatch.setattr(v48, '_memory', lambda: {'ratio': .65}); assert worker() == 2
    monkeypatch.setattr(v48, '_memory', lambda: {'ratio': .80}); assert worker() == 1
    monkeypatch.setattr(v48, '_memory', lambda: {'ratio': None}); assert worker() == 1


def test_storage_snapshot_prefers_current_file_truth(tmp_path):
    db = tmp_path / 'eth_adaptive.db'; db.write_bytes(b'sqlite-placeholder')
    core = FakeCore(db); core.state['storage'].update({'database_exists': False, 'database_size_bytes': 0})
    snap = v48._storage_snapshot(core)
    assert snap['database_path'] == str(db)
    assert snap['database_exists'] is True
    assert snap['database_size_bytes'] == len(b'sqlite-placeholder')
    assert snap['market_bars'] == 123 and snap['learning_samples'] == 456


def test_v48_contract_is_semantic_neutral():
    source = Path(v48.__file__).read_text(encoding='utf-8')
    for needle in ("'research_semantics_changed': False", "'history_changed': False",
                   "'feature_set_changed': False", "'candidate_search_space_changed': False",
                   "'oos_rules_changed': False", "'trade_simulation_changed': False",
                   "'future_peeking_enabled': False"):
        assert needle in source
    assert 'DELETE FROM learning_samples' not in source
    assert 'DELETE FROM market_bars' not in source
    assert 'INSERT INTO learning_samples' not in source


def test_boot_id_is_process_scoped():
    import os
    assert str(v48.BOOT_ID).startswith(str(os.getpid()) + '-')
