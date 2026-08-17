from __future__ import annotations

from types import SimpleNamespace

import numpy as np

import v47_dataset_integrity_authority as v47


def test_full_array_hash_changes_for_one_unsampled_byte():
    a = np.arange(200_000, dtype=np.float32).reshape(20_000, 10)
    b = a.copy()
    # Change one interior value. V47 hashes every byte, not sampled checkpoints.
    b[12_347, 7] += np.float32(0.125)
    assert v47._hash_array(a)['sha256'] != v47._hash_array(b)['sha256']


def test_full_array_hash_is_stable_for_identical_values_and_layouts():
    a = np.arange(50_000, dtype=np.float64).reshape(5_000, 10)
    b = np.array(a, copy=True)
    assert v47._hash_array(a) == v47._hash_array(b)


def test_close15_hash_covers_every_decision_close():
    a = {1_700_000_000 + i * 900: 1800.0 + i / 1000.0 for i in range(20_000)}
    b = dict(a)
    key = 1_700_000_000 + 12_345 * 900
    b[key] += 0.01
    assert v47._hash_close15(a)['sha256'] != v47._hash_close15(b)['sha256']


def test_frozen_contract_ignores_wall_clock_fields_but_keeps_execution_semantics():
    class Core:
        def __init__(self, payload): self.payload = payload
        def get_state(self, key, default=None): return self.payload if key == v47.leverage_truth.FROZEN_KEY else default

    base = {
        'schema': 36, 'ok': True, 'symbol': 'ETHUSDT', 'product_type': 'USDT-FUTURES',
        'notional_usdt': 20000.0, 'effective_max_leverage': 50.0,
        'maintenance_margin_rate': 0.005, 'conservative_stop_headroom_fraction': 0.0135,
        'fetched_at': 100, 'frozen_at': 101,
    }
    auto = SimpleNamespace(PAPER_NOTIONAL_USDT=20000.0)
    a = v47._frozen_execution_contract(Core(base), auto)
    changed_clock = dict(base, fetched_at=999, frozen_at=1000)
    b = v47._frozen_execution_contract(Core(changed_clock), auto)
    assert a == b
    changed_semantics = dict(base, conservative_stop_headroom_fraction=0.0125)
    c = v47._frozen_execution_contract(Core(changed_semantics), auto)
    assert a != c
