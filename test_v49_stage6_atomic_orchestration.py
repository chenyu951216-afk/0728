from __future__ import annotations

import hashlib
import json
from types import SimpleNamespace

import numpy as np

import v49_stage6_atomic_orchestration as v49


class FakeCore:
    def __init__(self):
        self.state = {}
        self.persisted = {}

    def get_state(self, key, default=None):
        return self.persisted.get(key, default)

    def set_state(self, key, value):
        self.persisted[key] = value


class FakeThroughput:
    def __init__(self):
        self._RUN_ID = None
        self.rows = []

    def _run_fingerprint(self, core, autonomous, snapshots, market):
        return 'exact-run-v49'

    def _counts(self, core, run):
        rows = [r for r in self.rows if r['run'] == run]
        return {'persisted': len(rows), 'scored': len(rows), 'no_result': 0}

    def _finalists(self, core, autonomous, run):
        return []


def _hash_payload(value, length=16):
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()[:length]


def _new_genome(rng, parent=None):
    base = int(parent['gene']) if parent else 0
    return {
        'gene': base + rng.randint(1, 100000),
        'direction': 'LONG',
        'max_hold_bars': 16,
        'gate': [],
    }


def test_durable_outer_cursor_advances_after_each_candidate(monkeypatch):
    core = FakeCore()
    tp = FakeThroughput()
    autonomous = SimpleNamespace(
        POPULATION=4,
        GENERATIONS=2,
        ELITES=2,
        FINALISTS=3,
        SCHEMA=30,
        CHECKPOINT_KEY='cp',
        _new_genome=_new_genome,
        _hash_payload=_hash_payload,
        _diversity_key=lambda g: (g['direction'], g['gene'] % 3),
    )

    evaluated = []

    def evaluate(snapshots, market, genome, seed):
        active = dict(core.state['autonomous_live_progress'])
        evaluated.append((active['generation'], active['candidate'], genome['gene'], seed))
        tp.rows.append({'run': tp._RUN_ID, 'generation': active['generation'], 'candidate': active['candidate']})
        return {'score': float(genome['gene'] % 97), 'ev': 0.1, 'pf': 1.2}

    autonomous._evaluate_candidate = evaluate
    evolution = v49._durable_evolution_factory(core, autonomous, tp)
    snapshots = {'ts': np.asarray([1, 2, 3], dtype=np.int64), 'x': np.zeros((3, 1), dtype=np.float32)}
    finalists = evolution(core, snapshots, {})

    assert len(evaluated) == 8
    assert tp._RUN_ID == 'exact-run-v49'
    cursor = core.get_state('v49_stage6_outer_cursor', {})
    assert cursor['generation'] == 2
    assert cursor['candidate'] == 4
    assert cursor['status'] == 'SCORED'
    cp = core.get_state('cp', {})
    assert cp['generation'] == 1
    assert cp['v49_run_id'] == 'exact-run-v49'
    assert cp['v46_run_id'] == 'exact-run-v49'
    assert cp['v49_generation_complete'] is True
    assert core.state[v49.STATE_KEY]['status'] == 'DEVELOPMENT_EVOLUTION_COMPLETE'
    assert finalists


def test_startup_barrier_is_explicit_and_semantic_neutral():
    core = FakeCore()
    v49.mark_startup_barrier(core, False, 'installing')
    assert core.state[v49.STATE_KEY]['startup_barrier_open'] is False
    v49.mark_startup_barrier(core, True, 'ready')
    assert core.state[v49.STATE_KEY]['startup_barrier_open'] is True
    assert core.state[v49.STATE_KEY]['startup_barrier_reason'] == 'ready'
