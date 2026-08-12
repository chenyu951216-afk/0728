from __future__ import annotations

import asyncio
import time

import adaptive_engine
import execution_v7
from market_data import MarketDataHub, SourceRangeUnavailable


class _NoNetworkClient:
    calls = 0

    async def get(self, *_args, **_kwargs):
        self.calls += 1
        raise AssertionError('a capability-impossible request must not reach the network')


def _bars(count: int, step: int, start: int = 1_600_000_000) -> list[dict]:
    out = []
    for i in range(count):
        close = 1000.0 + i * .5
        out.append({
            'ts': start + i * step, 'o': close - .2, 'h': close + 1.0,
            'l': close - 1.0, 'c': close, 'v': 100.0 + i,
        })
    return out


def test_gate_old_history_is_capability_skipped_without_repeated_http_400():
    hub = MarketDataHub()
    client = _NoNetworkClient()
    old_end = int(time.time()) - 300 * 20_000
    try:
        asyncio.run(hub.gate_candles(client, 'ETH_USDT', '5m', end_ts=old_end))
    except SourceRangeUnavailable:
        pass
    else:
        raise AssertionError('old Gate history must be classified as range-unavailable')
    assert client.calls == 0


def test_execution_higher_timeframes_require_close_before_decision():
    rows = [{'ts': ts} for ts in (0, 1800, 3600, 5400, 7200)]
    timestamps = [row['ts'] for row in rows]
    # At 01:15 only bars opened 00:00 and 00:30 have closed. The 01:00 bar
    # is visible on an exchange screen but cannot be used in a historical decision.
    selected = execution_v7._slice_closed_to(rows, timestamps, 1800, 4500, 100)
    assert [row['ts'] for row in selected] == [0, 1800]

    hourly = [{'ts': ts} for ts in (0, 3600, 7200)]
    selected_hourly = execution_v7._slice_closed_to(hourly, [0, 3600, 7200], 3600, 4500, 100)
    assert [row['ts'] for row in selected_hourly] == [0]


def test_model_features_preserve_macro_then_structure_context():
    regime = {
        'regime': 'BULL_MARKUP', 'phase': 'IMPULSE',
        'daily_direction': 1, 'h4_direction': 1, 'h1_direction': -1,
        'daily_adx': 40, 'h4_adx': 30, 'h1_adx': 20,
        'daily_slope': 2.5, 'h4_slope': 1.5, 'h1_slope': -.5,
        'volatility_rank': .7, 'h4_atr_pct': .025,
    }
    features = adaptive_engine.build_features(
        _bars(120, 900), _bars(120, 3600), _bars(120, 3600), regime, {},
    )
    assert features['daily_direction'] == 1
    assert features['h4_direction'] == 1
    assert features['h1_direction'] == -1
    assert features['macro_alignment'] == 1
    assert features['structure_alignment'] == -1
    assert features['daily_adx_norm'] == .4
    assert features['macro_atr_pct'] == .025
