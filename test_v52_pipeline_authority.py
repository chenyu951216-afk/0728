from __future__ import annotations

import hashlib
import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace

import v52_pipeline_authority as v52


class DummyCore:
    def __init__(self, path: Path):
        self.path = str(path)
        self.state = {}
        self.saved = {}

    def db(self):
        return sqlite3.connect(self.path)

    def get_state(self, key, default=None):
        return self.saved.get(key, default)

    def set_state(self, key, value):
        self.saved[key] = value


def _hash_payload(payload, n=18):
    raw = json.dumps(payload, sort_keys=True, separators=(',', ':'))
    return hashlib.sha256(raw.encode()).hexdigest()[:n]


def _autonomous(registry=None):
    registry = list(registry or [])
    return SimpleNamespace(
        CHECKPOINT_KEY='autonomous_evolution_checkpoint_v30',
        POPULATION=48,
        GENERATIONS=8,
        _hash_payload=_hash_payload,
        _load_registry=lambda _core, active_only=True: list(registry),
    )


class DummyThroughput:
    _RUN_ID = 'v52-test-run'

    @staticmethod
    def _counts(_core, _run):
        return {'persisted': 0, 'scored': 0, 'no_result': 0}


def test_stale_terminal_checkpoint_is_cleared_without_touching_replay(tmp_path):
    core = DummyCore(tmp_path / 'vault.db')
    a = _autonomous()
    core.saved[a.CHECKPOINT_KEY] = {'status': 'COMPLETE', 'champions': 0}
    core.saved['v49_stage6_outer_cursor'] = {'generation': 8, 'candidate': 48}
    core.saved['important_replay_cursor'] = {'ts': 123456}

    v52._migrate(core, a)

    assert core.saved[a.CHECKPOINT_KEY] == {}
    assert core.saved['v49_stage6_outer_cursor'] == {}
    assert core.saved['important_replay_cursor'] == {'ts': 123456}
    migration = core.saved[v52.MIGRATION_KEY]
    assert migration['stale_terminal_checkpoint_cleared'] is True
    assert migration['raw_market_preserved'] is True
    assert migration['learning_samples_preserved'] is True
    assert migration['replay_cursor_preserved'] is True


def test_certified_champion_prevents_terminal_checkpoint_migration(tmp_path):
    core = DummyCore(tmp_path / 'vault.db')
    champion = {'strategy_id': 'AUTO_OK', 'status': 'CHAMPION', 'active': True}
    a = _autonomous([champion])
    original = {'status': 'COMPLETE', 'champions': 1}
    core.saved[a.CHECKPOINT_KEY] = dict(original)

    v52._migrate(core, a)

    assert core.saved[a.CHECKPOINT_KEY] == original
    assert core.saved[v52.MIGRATION_KEY]['stale_terminal_checkpoint_cleared'] is False


def test_development_eligible_strategy_is_saved_before_final_oos(tmp_path):
    core = DummyCore(tmp_path / 'vault.db')
    a = _autonomous()
    core.state['autonomous_live_progress'] = {'candidate_id': 'candidate-1'}
    genome = {'direction': 'LONG', 'gene': 7}
    dev = {
        'score': 1.25,
        'eligible_for_finalist': True,
        'final_oos_eligible': True,
        'development_status': 'DEVELOPMENT_WALK_FORWARD_ELIGIBLE',
    }

    v52._save_candidate(core, a, DummyThroughput, genome, dev)
    counts = v52._counts(core, DummyThroughput._RUN_ID)
    assert counts['saved'] == 1
    assert counts['finalists'] == 0
    assert counts['audited'] == 0

    con = core.db()
    try:
        row = con.execute(f'SELECT status,candidate_id FROM {v52.VAULT_TABLE}').fetchone()
    finally:
        con.close()
    assert row == ('DEVELOPMENT_ELIGIBLE_SAVED', 'candidate-1')


def test_finalists_are_atomically_frozen_before_oos(tmp_path):
    core = DummyCore(tmp_path / 'vault.db')
    a = _autonomous()
    g1 = {'direction': 'LONG', 'gene': 1}
    g2 = {'direction': 'SHORT', 'gene': 2}
    d1 = {'score': 1.1, 'eligible_for_finalist': True}
    d2 = {'score': 0.9, 'eligible_for_finalist': True}

    v52._save_candidate(core, a, DummyThroughput, g1, d1)
    v52._save_candidate(core, a, DummyThroughput, g2, d2)
    v52._freeze_finalists(core, a, DummyThroughput,
                          [(1.1, g1, d1), (0.9, g2, d2)])

    counts = v52._counts(core, DummyThroughput._RUN_ID)
    assert counts['saved'] == 2
    assert counts['finalists'] == 2
    assert counts['audited'] == 0
    assert core.state[v52.STATE_KEY]['finalist_freeze_complete'] is True
    assert core.state[v52.STATE_KEY]['oos_may_open_only_after_finalist_freeze'] is True


def test_generation_one_candidate_one_cannot_display_stage6_as_100_percent(tmp_path):
    core = DummyCore(tmp_path / 'vault.db')
    a = _autonomous()
    core.state['autonomous_live_progress'] = {
        'stage': 'DIRECT_R_AUTONOMOUS_EVOLUTION',
        'generation': 1,
        'generations': 8,
        'candidate': 1,
        'population': 48,
        'outer_status': 'EVALUATING',
    }
    core.state['v49_stage6_atomic_orchestration'] = {
        'run_id': DummyThroughput._RUN_ID,
        'checkpoint_counts': {'persisted': 0, 'scored': 0, 'no_result': 0},
    }
    # This is the stale state that produced the production symptom.
    core.saved[a.CHECKPOINT_KEY] = {'status': 'COMPLETE', 'generation': 7}

    progress = v52._run_progress(core, a, DummyThroughput)
    assert progress['total_candidates'] == 384
    assert progress['completed_candidates'] == 0
    assert progress['evolution_percent'] == 0.0

    core.state['autonomous_live_progress']['outer_status'] = 'COMMITTED'
    core.state['v49_stage6_atomic_orchestration']['checkpoint_counts']['persisted'] = 1
    progress = v52._run_progress(core, a, DummyThroughput)
    assert progress['completed_candidates'] == 1
    assert 0.0 < progress['evolution_percent'] < 1.0
