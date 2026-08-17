from __future__ import annotations

"""Deterministic sklearn random-state compatibility authority for Stage 6.

V49 finally surfaced the real Stage-6 failure: the autonomous evolution seed is derived
from 12 hexadecimal SHA digits (up to 48 bits), while sklearn's legacy RandomState
contract accepts integer seeds only in [0, 2**32 - 1].  Candidate path simulation can
therefore finish successfully and then fail exactly when HistGradientBoostingRegressor
is fitted.

V50 fixes only that invalid interface boundary.  It does not alter historical data,
features, genomes, folds, fills, costs, stops, targets, OOS gates, or no-lookahead
semantics.  Every arbitrary Python integer is deterministically mapped into sklearn's
valid uint32 seed domain.  Already-valid seeds are unchanged.
"""

import time
from typing import Any

import runtime_identity

VERSION = 'V50_SKLEARN_SEED_AUTHORITY'
SCHEMA = 50
STATE_KEY = 'v50_sklearn_seed_authority'
SKLEARN_SEED_MODULUS = 1 << 32
SKLEARN_SEED_MAX = SKLEARN_SEED_MODULUS - 1
KNOWN_ERROR_TOKEN = "'random_state' parameter"

_INSTALLED = False
_BASE_MODEL: Any | None = None


def normalize_sklearn_random_state(seed: Any) -> int:
    """Map any integer-like seed into sklearn RandomState's exact valid domain.

    Python's modulo is deterministic for negative and arbitrarily-large integers.
    For every already-valid seed this is an identity transform.
    """
    value = int(seed)
    return value % SKLEARN_SEED_MODULUS


def _state(core: Any, **patch: Any) -> dict[str, Any]:
    old = core.state.get(STATE_KEY)
    out = dict(old) if isinstance(old, dict) else {}
    out.update(patch)
    out.update({
        'schema': SCHEMA,
        'runtime': VERSION,
        'public_runtime': runtime_identity.RUNTIME_VERSION,
        'updated_at': int(time.time()),
    })
    core.state[STATE_KEY] = out
    return out


def _known_seed_error(value: Any) -> bool:
    text = str(value or '')
    return KNOWN_ERROR_TOKEN in text and ('4294967295' in text or 'RandomState' in text)


def _install_model_boundary(core: Any, autonomous: Any) -> None:
    global _BASE_MODEL
    if getattr(autonomous, '_v50_seed_boundary_installed', False):
        return
    base_model = autonomous._model
    _BASE_MODEL = base_model

    def model_with_valid_seed(genome: dict[str, Any], seed: int):
        original = int(seed)
        normalized = normalize_sklearn_random_state(original)
        model = base_model(genome, normalized)
        try:
            actual = model.get_params(deep=False).get('random_state')
        except Exception:
            actual = getattr(model, 'random_state', None)
        if int(actual) != normalized:
            raise RuntimeError(
                f'sklearn random_state authority mismatch: expected {normalized}, got {actual!r}'
            )
        _state(
            core,
            status='ACTIVE',
            last_original_seed=original,
            last_normalized_seed=normalized,
            last_seed_was_out_of_range=not (0 <= original <= SKLEARN_SEED_MAX),
            seed_domain=f'0..{SKLEARN_SEED_MAX}',
        )
        return model

    autonomous._model = model_with_valid_seed
    autonomous._v50_seed_boundary_installed = True


def _heal_known_persisted_error(core: Any, transition: Any) -> None:
    """Clear only the exact now-fixed seed-contract error; preserve unrelated failures."""
    learning = core.state.setdefault('learning', {})
    learning_error = learning.get('error') if isinstance(learning, dict) else None
    if _known_seed_error(learning_error):
        learning['previous_error_v50'] = str(learning_error)
        learning['error'] = None
        learning['blocker'] = None

    try:
        raw = core.get_state(transition.STATE_KEY, {})
    except Exception:
        raw = core.state.get('replay_transition_stability') or {}
    trans = dict(raw) if isinstance(raw, dict) else {}
    if _known_seed_error(trans.get('error')):
        previous = str(trans.get('error'))
        patch = {
            'status': 'CERTIFICATION_RETRY_READY',
            'ready_after': 0,
            'error': None,
            'v50_previous_seed_error': previous,
            'reason': 'V50 normalized autonomous sklearn seeds into the valid uint32 RandomState domain; exact Stage-6 retry is allowed',
            'raw_market_preserved': True,
            'learning_samples_preserved': True,
            'replay_cursor_preserved': True,
        }
        try:
            transition._persist(core, patch)
        except Exception:
            trans.update(patch)
            core.state['replay_transition_stability'] = trans
        _state(core, healed_previous_seed_error=True, previous_seed_error=previous)


def install(production: Any, autonomous: Any, integrity: Any, transition: Any) -> None:
    global _INSTALLED
    if _INSTALLED:
        return
    _INSTALLED = True
    core = production.core

    # This boundary changes model reproducibility for previously-invalid >32-bit seeds,
    # therefore it must participate in V47's exact code identity before Stage 6 starts.
    modules = tuple(getattr(integrity, 'SEMANTIC_MODULES', ()))
    if 'v50_sklearn_seed_authority' not in modules:
        integrity.SEMANTIC_MODULES = modules + ('v50_sklearn_seed_authority',)

    _install_model_boundary(core, autonomous)
    _heal_known_persisted_error(core, transition)

    example_bad = 83166691192533
    example_fixed = normalize_sklearn_random_state(example_bad)
    _state(
        core,
        installed=True,
        status='ACTIVE',
        seed_domain=f'0..{SKLEARN_SEED_MAX}',
        identity_for_valid_seed=True,
        arbitrary_integer_seed_supported=True,
        example_original_seed=example_bad,
        example_normalized_seed=example_fixed,
        rules={
            'historical_data_changed': False,
            'features_changed': False,
            'candidate_genomes_changed': False,
            'folds_changed': False,
            'trade_simulation_changed': False,
            'fitness_changed': False,
            'oos_rules_changed': False,
            'execution_semantics_changed': False,
            'future_peeking_enabled': False,
            'replay_reset': False,
            'raw_data_deleted': False,
            'valid_32bit_seeds_unchanged': True,
            'oversized_seed_mapping': 'python_int_mod_2_pow_32',
            'exact_resume_identity_includes_v50': True,
        },
    )

    app = getattr(core, 'app', None)
    if app is not None and not any(getattr(r, 'path', None) == '/api/v50/seed-authority' for r in app.router.routes):
        @app.get('/api/v50/seed-authority')
        def seed_authority_status() -> dict[str, Any]:
            return {
                'schema': SCHEMA,
                'runtime': VERSION,
                'state': dict(core.state.get(STATE_KEY) or {}),
                'rules': dict((core.state.get(STATE_KEY) or {}).get('rules') or {}),
            }
