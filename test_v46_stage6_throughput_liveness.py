from __future__ import annotations

import json
import sqlite3
from types import SimpleNamespace

import numpy as np

import v46_stage6_throughput_liveness as v46


class Core:
    def __init__(self, path):
        self.path = str(path)
        self.state = {}
        self._kv = {}
    def db(self):
        return sqlite3.connect(self.path)
    def get_state(self, key, default=None):
        return self._kv.get(key, default)
    def set_state(self, key, value):
        self._kv[key] = value


def fake_auto():
    def diversity(g):
        return (g['direction'], g.get('bucket', 0))
    return SimpleNamespace(
        RESET_MARKER='test-reset', RESEARCH_START_TS=1000, RESEARCH_END_EXCLUSIVE_TS=10000,
        SETTLEMENT_END_EXCLUSIVE_TS=20000, POPULATION=2, GENERATIONS=2, ELITES=1,
        FINALISTS=2, MAX_CHAMPIONS=2, TRAIN_SIM_CAP=10, CAL_SIM_CAP=10, TEST_SIM_CAP=10,
        FINAL_REFIT_CAP=20, HOLD_BARS_15M=(1, 4), EXPIRE_BARS_15M=(1, 2),
        DECISION_STRIDES=(1, 2), FINAL_HOLDOUT_PCT=.18, MIN_OOS_FILLS=35, MIN_OOS_PF=1.25,
        MIN_OOS_EV_R=.08, MAX_OOS_DD_R=12.0, MIN_WF_STABILITY=.60,
        MIN_PROFITABLE_FOLDS=.66, MIN_WORST_FOLD_EV=-.08, MIN_BOOTSTRAP_CI05=0.0,
        ALL_IN_COST_BPS=8.0, FEATURE_NAMES=('a', 'b'), CHECKPOINT_KEY='cp',
        _hash_payload=lambda g, n=18: 'gid-' + str(g['id']), _diversity_key=diversity,
    )


def test_parallel_simulation_preserves_scalar_order_and_values(tmp_path, monkeypatch):
    core = Core(tmp_path / 'db.sqlite'); auto = fake_auto()
    calls = []
    def scalar(market, ts, features, genome):
        calls.append(ts)
        return {'valid': True, 'filled': True, 'pnl_r': float(ts) / 1000.0}
    auto._simulate_trade = scalar
    monkeypatch.setattr(v46.v43, '_FAST_VERIFIED', True)
    monkeypatch.setattr(v46.v43, '_FAST_ENABLED', True)
    monkeypatch.setattr(v46, 'MAX_WORKERS', 3)
    monkeypatch.setattr(v46, 'CHUNK', 4)
    monkeypatch.setattr(v46, '_memory', lambda: {'ratio': .2})
    monkeypatch.setattr(v46, '_sync_phase', lambda *_: None)
    v46._install_parallel(core, auto)
    snapshots = {'ts': np.arange(10, dtype=np.int64) * 900 + 1000, 'x': np.arange(20, dtype=np.float32).reshape(10, 2)}
    idx = np.array([7, 1, 9, 3, 2], dtype=np.int64)
    xs, ys, results = auto._simulate_indices(idx, snapshots, {}, {'id': 1})
    assert [x['pnl_r'] for x in results] == [float(snapshots['ts'][i]) / 1000.0 for i in idx]
    assert np.array_equal(xs, snapshots['x'][idx])
    assert np.allclose(ys, [float(snapshots['ts'][i]) / 1000.0 for i in idx])
    assert sorted(calls) == sorted(int(snapshots['ts'][i]) for i in idx)
    assert core.state['autonomous_live_progress']['future_prices_as_features'] is False


def test_candidate_result_and_no_result_are_exactly_resumable(tmp_path):
    core = Core(tmp_path / 'db.sqlite'); auto = fake_auto(); v46._ensure(core)
    run = 'run'; genome = {'id': 7, 'direction': 'LONG', 'bucket': 0}; result = {'score': 1.2, 'ev': .2}
    v46._save(core, run, 1, 1, 'gid-7', 123, genome, result)
    row = v46._load(core, run, 1, 1)
    assert row['candidate_id'] == 'gid-7' and row['seed'] == 123 and row['result'] == result
    v46._save(core, run, 1, 2, 'gid-8', 124, {'id': 8, 'direction': 'SHORT', 'bucket': 1}, None)
    row2 = v46._load(core, run, 1, 2)
    assert row2['status'] == 'NO_RESULT' and row2['result'] is None
    assert v46._counts(core, run) == {'persisted': 2, 'scored': 1, 'no_result': 1}


def test_full_candidate_archive_reconstructs_across_generations(tmp_path):
    core = Core(tmp_path / 'db.sqlite'); auto = fake_auto(); v46._ensure(core); run = 'r2'
    rows = [
        (1, 1, 1.0, {'id': 1, 'direction': 'LONG', 'bucket': 0}),
        (1, 2, 2.0, {'id': 2, 'direction': 'SHORT', 'bucket': 0}),
        (2, 1, 3.0, {'id': 3, 'direction': 'LONG', 'bucket': 1}),
    ]
    for gen, cand, score, genome in rows:
        v46._save(core, run, gen, cand, 'gid-' + str(genome['id']), 100 + cand, genome, {'score': score, 'ev': score / 10})
    finalists = v46._finalists(core, auto, run)
    assert [x[1]['id'] for x in finalists] == [3, 2]


def test_run_fingerprint_changes_when_dataset_identity_changes(tmp_path):
    core = Core(tmp_path / 'db.sqlite'); auto = fake_auto()
    snapshots = {'ts': np.array([1000, 1900, 2800]), 'x': np.array([[1., 2.], [2., 3.], [3., 4.]], dtype=np.float32)}
    market = {'ts5': np.array([1000, 1300, 1600]), 'o5': np.ones(3), 'h5': np.ones(3)*2, 'l5': np.ones(3)*.5, 'c5': np.ones(3)*1.5, 'source5': 'canonical'}
    core.set_state('final_dataset_baseline_v1', {'dataset_id': 'A'})
    a = v46._run_fingerprint(core, auto, snapshots, market)
    core.set_state('final_dataset_baseline_v1', {'dataset_id': 'B'})
    b = v46._run_fingerprint(core, auto, snapshots, market)
    assert a != b
