from __future__ import annotations

import sqlite3
from unittest.mock import patch

import numpy as np

import adaptive_v5 as base
import v5_runtime
import v17_certification_orchestrator as certification
import v20_historical_signal_evolution as evolution


class _DeterministicModel:
    def fit(self, x, y, sample_weight=None):
        self.classes_ = np.asarray([0, 1])
        return self

    def predict_proba(self, x):
        count = len(np.asarray(x))
        return np.tile(np.asarray([[0.20, 0.80]]), (count, 1))


def _development_result(*_args, **_kwargs):
    return {
        'score': 2.0, 'ev': .20, 'pf': 2.0, 'win': .80, 'n': 150,
        'dd': .25, 'stability': .95, 'profitable_folds': 1.0,
        'worst_fold_ev': .10, 'threshold': .50, 'folds': [],
        'regime_metrics': {'BULL_MARKUP': {'n': 150, 'ev': .20, 'pf': 2.0}},
        'allowed_regimes': ['BULL_MARKUP'],
    }


def _add_rows(store, start_index: int, count: int) -> None:
    for i in range(start_index, start_index + count):
        success = int(i % 5 != 0)
        store.add_sample({
            'ts': 1_600_000_000 + i * 5 * 3600,
            'strategy': 'TREND_PULLBACK', 'direction': 'LONG',
            'regime': 'BULL_MARKUP', 'phase': 'IMPULSE',
            'features': {name: 0.0 for name in base.FEATURE_NAMES},
            'success': success, 'pnl_r': .40 if success else -.20,
            'mfe_r': .50, 'mae_r': .20, 'source_quality': 95.0,
        })
    store.commit()


def test_genome_identity_ignores_generation_labels():
    left = {'feature_mode': 'lean', 'generation': 1, 'id': 'old', 'regimes': ['BULL_MARKUP']}
    right = {'feature_mode': 'lean', 'generation': 9, 'id': 'new', 'regimes': ['BULL_MARKUP']}
    assert evolution._fingerprint(left) == evolution._fingerprint(right)


def test_failed_or_passed_holdout_is_not_reopened_and_incumbent_uses_next_block():
    con = sqlite3.connect(':memory:')
    store = base.ModelStore(con)
    _add_rows(store, 0, 1200)
    learner = evolution.HistoricalEvolutionLearner(store)

    with (
        patch.object(evolution, 'POPULATION', 4),
        patch.object(evolution, 'GENERATIONS', 2),
        patch.object(evolution, 'ELITES', 1),
        patch.object(evolution, '_development_score', side_effect=_development_result),
        patch.object(evolution, '_model', side_effect=lambda *_: _DeterministicModel()),
    ):
        first = learner.train_strategy_direction('TREND_PULLBACK', 'LONG')
        assert first is not None and first.promoted
        first_run = evolution._latest_run(store, 'TREND_PULLBACK', 'LONG')
        assert first_run and first_run['status'] == 'PROMOTED'

        # Explicit/manual retries on byte-identical evidence cannot re-open the block.
        assert learner.train_strategy_direction('TREND_PULLBACK', 'LONG') is None
        run_count = con.execute('SELECT COUNT(*) FROM signal_evolution_runs').fetchone()[0]
        assert run_count == 1

        # A complete later block permits a new generation. The previous champion is
        # evaluated on that exact block and is retained when the challenger is no better.
        _add_rows(store, 1200, evolution.MIN_UNTOUCHED_HOLDOUT)
        second = learner.train_strategy_direction('TREND_PULLBACK', 'LONG')
        assert second is not None
        assert second.evaluation_status == 'ABSOLUTE_PASS_INCUMBENT_HELD'
        second_run = evolution._latest_run(store, 'TREND_PULLBACK', 'LONG')
        assert second_run and second_run['holdout_start_ts'] > first_run['holdout_end_ts']
        incumbent = second_run['metrics']['incumbent_same_holdout']
        assert incumbent['selected_n'] == second.selected_n


def test_orchestrator_distinguishes_new_rejection_from_later_wait(tmp_path):
    path = str(tmp_path / 'lineage.db')

    class Core:
        def db(self):
            return sqlite3.connect(path)

    class Learner:
        def __init__(self, store):
            self.store = store

        def train_strategy_direction(self, strategy, direction):
            if evolution._latest_run(self.store, strategy, direction) is None:
                evolution._record_run(
                    self.store, strategy, direction,
                    development_end_ts=100, holdout_start_ts=200, holdout_end_ts=300,
                    status='NO_ELIGIBLE_DEVELOPMENT_CANDIDATE', winner=None,
                    metrics={'reason': 'development population had no robust edge'},
                )
            return None

    con = sqlite3.connect(path)
    base.ModelStore(con)
    con.close()
    with patch.object(v5_runtime, 'Learner', Learner):
        opened = certification._run_detailed_certification(Core())
        assert opened and all(x['status'] == 'NO_ELIGIBLE_DEVELOPMENT_CANDIDATE' for x in opened)
        waiting = certification._run_detailed_certification(Core())
        assert waiting and all(x['status'] == 'WAITING_NEW_UNTOUCHED_HOLDOUT' for x in waiting)
