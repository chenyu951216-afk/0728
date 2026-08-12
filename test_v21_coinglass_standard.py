from __future__ import annotations

import asyncio
import sqlite3
import time

import execution_v7
from derivative_data import DerivativeHistory
import v15_data_resilience as resilience
import v21_coinglass_standard as standard


async def _unexpected_heatmap(*_args, **_kwargs):
    raise AssertionError('Standard plan must never call the Professional heatmap endpoint')


class _Core:
    def __init__(self):
        self.values = {}
        self.state = {}
        self.derivative_history = type('History', (), {
            'coinglass_key': 'configured',
            'coinglass_liquidation_heatmap': staticmethod(_unexpected_heatmap),
        })()

    def get_state(self, key, default=None):
        return self.values.get(key, default)

    def set_state(self, key, value):
        self.values[key] = value
def test_standard_plan_never_calls_professional_heatmap(monkeypatch):
    core = _Core()
    monkeypatch.setattr(standard, 'COINGLASS_PLAN', 'STANDARD')
    result = asyncio.run(standard._refresh_heatmap(core, force=True))
    assert result['available'] is False
    assert result['mode'] == 'PLAN_UNAVAILABLE'


def test_missing_optional_standard_key_does_not_deadlock_source_freeze():
    core = _Core()
    core.derivative_history.coinglass_key = ''
    assert resilience._settled(core, resilience.COINGLASS_STANDARD_ENRICHMENT)


def test_heatmap_parser_is_robust_and_gate_only_vetoes_without_mutating():
    parsed = standard._heatmap_zones({
        'y_axis': ['bad', 1905.5, 1915.0],
        'liquidation_leverage_data': [[9, 1, 5000], [10, 1, 9000]],
        'price_candlesticks': [[1, 1910, 1920, 1900, 1915, 100]],
    })
    assert parsed['zones'][0]['price'] == 1905.5
    core = _Core()
    core.values[standard.STATE_KEY] = {
        'heatmap': {**parsed, 'available': True, 'observed_at': int(time.time())},
    }
    plan = {'entry': 1915.0, 'stop': 1905.0}
    gate = standard.liquidation_stop_gate(core, plan)
    assert gate['allowed'] is False
    assert gate['plan_mutated'] is False
    assert plan == {'entry': 1915.0, 'stop': 1905.0}


def test_standard_taker_parser_uses_documented_aggregated_fields(tmp_path):
    history = DerivativeHistory(str(tmp_path / 'derivatives.db'), coinglass_key='x')
    captured = {}

    async def fake_cg(path, params):
        captured.update({'path': path, 'params': params})
        return [{
            'time': 1_700_000_000_000,
            'aggregated_buy_volume_usd': '300', 'aggregated_sell_volume_usd': '100',
        }]

    history._cg = fake_cg
    added = asyncio.run(history._backfill_coinglass_taker(1_699_000_000, 1_701_000_000))
    assert added == 1
    assert captured['path'] == '/futures/aggregated-taker-buy-sell-volume/history'
    assert captured['params']['exchange_list'] == 'Binance,OKX,Bybit,Gate'
    con = sqlite3.connect(history.db_path)
    value = con.execute(
        "SELECT value FROM derivative_history WHERE source='coinglass' AND metric='taker_imbalance'"
    ).fetchone()[0]
    con.close()
    assert value == .5


def test_cost_and_noise_floor_rejects_four_dollar_eth_stop():
    bars = [
        {'ts': 1_700_000_000 + i * 900, 'o': 1915.0, 'h': 1915.5, 'l': 1914.5, 'c': 1915.0, 'v': 10.0}
        for i in range(120)
    ]
    policy = {
        'entry_atr': .05, 'stop_atr': .60, 'noise_floor_mult': .75,
        'structure_mode': 'tight', 'target_rr': [1.0, 1.5, 2.0, 3.0],
        'allocations': [20, 30, 30, 20], 'all_in_cost_bps': 8.0,
        'min_stop_pct': .002,
    }
    plan = execution_v7.plan_from_policy(
        'LIQUIDITY_SWEEP_REVERSAL', 'LONG', 1915.0, bars, policy, bars, bars,
    )
    assert plan['risk'] > 4.0
    assert plan['risk'] >= plan['entry'] * .0008 * execution_v7.MIN_STOP_COST_MULTIPLE - .02
    assert plan['profile']['binding_stop_floor'] == 'cost_multiple'
