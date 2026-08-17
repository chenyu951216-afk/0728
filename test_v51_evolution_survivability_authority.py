from __future__ import annotations

import math
import random
from types import SimpleNamespace

import v51_evolution_survivability_authority as v51


class DummyCore:
    def __init__(self):
        self.state = {}
        self.saved = {}

    def get_state(self, key, default=None):
        return self.saved.get(key, default)

    def set_state(self, key, value):
        self.saved[key] = value


class DummyThroughput:
    _RUN_ID = None

    @staticmethod
    def _run_fingerprint(_core, _a, _snapshots, _market):
        return 'exact-v51-test-run'

    @staticmethod
    def _counts(_core, _run):
        return {'persisted': 0, 'scored': 0, 'no_result': 0}

    @staticmethod
    def _finalists(_core, _a, _run):
        return []


class DummyOrchestration:
    @staticmethod
    def _state(core, **patch):
        old = dict(core.state.get('v49_stage6_atomic_orchestration') or {})
        old.update(patch)
        core.state['v49_stage6_atomic_orchestration'] = old
        return old


def _autonomous(evaluator):
    def new_genome(rng: random.Random, parent=None):
        base = int((parent or {}).get('gene', 0))
        return {
            'gene': base + rng.randrange(1, 1000000),
            'direction': 'LONG',
            'max_hold_bars': 64,
            'gate': [],
        }

    def hash_payload(payload, _n=18):
        return f"g-{int(payload.get('gene', 0))}"

    return SimpleNamespace(
        SCHEMA=30,
        CHECKPOINT_KEY='autonomous_evolution_checkpoint_v30',
        POPULATION=2,
        GENERATIONS=3,
        ELITES=1,
        FINALISTS=2,
        _new_genome=new_genome,
        _hash_payload=hash_payload,
        _diversity_key=lambda genome: (int(genome['gene']),),
        _evaluate_candidate=evaluator,
    )


def _search_only(score=-90.0):
    return {
        'score': float(score),
        'eligible_for_finalist': False,
        'final_oos_eligible': False,
        'development_status': 'SEARCH_ONLY_INSUFFICIENT_WALK_FORWARD',
        'future_holdout_used_for_search_score': False,
    }


def _eligible_result(score=1.25):
    return {
        'score': float(score),
        'eligible_for_finalist': True,
        'final_oos_eligible': True,
        'development_status': 'DEVELOPMENT_WALK_FORWARD_ELIGIBLE',
        'future_holdout_used_for_search_score': False,
    }


def test_search_only_score_is_finite_and_never_oos_eligible():
    result = v51._search_only_result(
        [0.25, 0.5, 0.75], [], [], {}, 0, 0,
    )
    assert math.isfinite(result['score'])
    assert result['score'] < -50.0
    assert result['eligible_for_finalist'] is False
    assert result['final_oos_eligible'] is False
    assert result['future_holdout_used_for_search_score'] is False
    assert v51._eligible((result['score'], {}, result)) is False


def test_generation_one_with_zero_eligible_candidates_does_not_end_evolution():
    core = DummyCore()
    calls = []

    def evaluator(_snapshots, _market, _genome, seed):
        calls.append(seed)
        return _search_only(-90.0 + len(calls) / 1000.0)

    a = _autonomous(evaluator)
    evolve = v51._evolution_factory(core, a, DummyThroughput, DummyOrchestration)
    finalists = evolve(core, {'ts': [1, 2, 3]}, {})

    # Old V30/V49 stopped after generation 1 here. V51 must execute every generation.
    assert len(calls) == a.POPULATION * a.GENERATIONS
    assert finalists == []
    state = core.state[v51.STATE_KEY]
    assert state['status'] == 'DEVELOPMENT_EVOLUTION_COMPLETE'
    assert state['generations_executed'] == a.GENERATIONS
    assert state['search_only_excluded_from_finalists'] is True
    assert core.saved[a.CHECKPOINT_KEY]['v51_generation_complete'] is True


def test_search_only_candidates_can_parent_but_never_enter_finalist_pool():
    core = DummyCore()
    calls = []

    def evaluator(_snapshots, _market, genome, _seed):
        calls.append(int(genome['gene']))
        # Guarantee at least one genuine development-eligible result while the rest
        # remain search-only. Finalists must contain only the former class.
        return _eligible_result(1.0 + len(calls) / 100.0) if len(calls) % 2 else _search_only(-80.0)

    a = _autonomous(evaluator)
    evolve = v51._evolution_factory(core, a, DummyThroughput, DummyOrchestration)
    finalists = evolve(core, {'ts': [1, 2, 3]}, {})

    assert len(calls) == a.POPULATION * a.GENERATIONS
    assert finalists
    assert all(item[2]['eligible_for_finalist'] is True for item in finalists)
    assert all(item[2]['final_oos_eligible'] is True for item in finalists)
    assert not any(item[2]['development_status'].startswith('SEARCH_ONLY') for item in finalists)


def test_invalid_market_path_failure_is_not_converted_to_search_only(monkeypatch):
    # The evaluator's fail-closed path is implemented as a RuntimeError threshold,
    # and the evolution wrapper must re-raise BaseException instead of ranking it.
    core = DummyCore()

    def evaluator(_snapshots, _market, _genome, _seed):
        raise RuntimeError('Stage6 causal settlement alignment invalid: synthetic')

    a = _autonomous(evaluator)
    evolve = v51._evolution_factory(core, a, DummyThroughput, DummyOrchestration)
    try:
        evolve(core, {'ts': [1, 2, 3]}, {})
    except RuntimeError as exc:
        assert 'causal settlement alignment invalid' in str(exc)
    else:
        raise AssertionError('data-path failure must remain fail-closed')

    assert core.state[v51.STATE_KEY]['status'] == 'FAIL_CLOSED_DATA_OR_MODEL_ERROR'
