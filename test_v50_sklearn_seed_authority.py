from __future__ import annotations

from types import SimpleNamespace

import numpy as np
from sklearn.ensemble import HistGradientBoostingRegressor

import v50_sklearn_seed_authority as v50


class DummyCore:
    def __init__(self):
        self.state = {}


class DummyAutonomous(SimpleNamespace):
    pass


def _base_model(_genome, seed):
    return HistGradientBoostingRegressor(
        max_iter=2,
        max_leaf_nodes=3,
        min_samples_leaf=2,
        random_state=int(seed),
    )


def test_problem_seed_maps_to_valid_uint32_deterministically():
    original = 83166691192533
    fixed = v50.normalize_sklearn_random_state(original)
    assert fixed == 3239440085
    assert 0 <= fixed <= 4294967295
    assert v50.normalize_sklearn_random_state(original) == fixed


def test_already_valid_sklearn_seeds_are_identity():
    for seed in (0, 1, 42, 2**31, 2**32 - 1):
        assert v50.normalize_sklearn_random_state(seed) == seed


def test_negative_and_huge_python_ints_are_always_normalized():
    for seed in (-1, -(2**80), 2**80 + 123456789):
        fixed = v50.normalize_sklearn_random_state(seed)
        assert 0 <= fixed <= 2**32 - 1
        assert fixed == int(seed) % (2**32)


def test_model_boundary_accepts_exact_production_failure_seed_and_can_fit():
    core = DummyCore()
    autonomous = DummyAutonomous(_model=_base_model)
    v50._install_model_boundary(core, autonomous)

    model = autonomous._model({}, 83166691192533)
    assert model.random_state == 3239440085

    x = np.asarray([[0.0], [1.0], [2.0], [3.0], [4.0], [5.0]], dtype=float)
    y = np.asarray([0.0, 0.1, 0.2, 0.1, 0.3, 0.5], dtype=float)
    model.fit(x, y)
    pred = model.predict(x)
    assert pred.shape == (6,)
    assert np.isfinite(pred).all()

    state = core.state[v50.STATE_KEY]
    assert state['last_original_seed'] == 83166691192533
    assert state['last_normalized_seed'] == 3239440085
    assert state['last_seed_was_out_of_range'] is True


def test_known_error_detector_is_narrow():
    exact = (
        "InvalidParameterError: The 'random_state' parameter of "
        "HistGradientBoostingRegressor must be an int in the range [0, 4294967295], "
        "an instance of 'numpy.random.mtrand.RandomState' or None."
    )
    assert v50._known_seed_error(exact) is True
    assert v50._known_seed_error('MemoryError: out of memory') is False
    assert v50._known_seed_error('some unrelated random state issue') is False
