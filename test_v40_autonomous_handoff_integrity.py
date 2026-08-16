from __future__ import annotations

import unittest

import v15_data_resilience as resilience
import v16_runtime_integrity as runtime_integrity
import v30_autonomous_strategy_discovery as autonomous
import v40_autonomous_handoff_integrity as v40


class Core:
    TIMEFRAME_SECONDS = {'5m': 300, '15m': 900, '30m': 1800, '1h': 3600, '4h': 14400, '1d': 86400}

    def __init__(self):
        self.state: dict = {}


def candle(ts: int, price: float = 100.0) -> dict:
    return {'ts': ts, 'o': price, 'h': price + 1, 'l': price - 1, 'c': price, 'v': 1.0, 'qv': 100.0, '_source': 'gate'}


class AutonomousMarketHandoffTests(unittest.TestCase):
    def setUp(self):
        self.old_canonical = resilience.canonical_bars
        self.old_start = autonomous.RESEARCH_START_TS
        self.old_end = autonomous.RESEARCH_END_EXCLUSIVE_TS
        self.old_settlement = autonomous.SETTLEMENT_END_EXCLUSIVE_TS
        autonomous.RESEARCH_START_TS = 0
        autonomous.RESEARCH_END_EXCLUSIVE_TS = 1800
        autonomous.SETTLEMENT_END_EXCLUSIVE_TS = 1200

    def tearDown(self):
        resilience.canonical_bars = self.old_canonical
        autonomous.RESEARCH_START_TS = self.old_start
        autonomous.RESEARCH_END_EXCLUSIVE_TS = self.old_end
        autonomous.SETTLEMENT_END_EXCLUSIVE_TS = self.old_settlement

    def test_virtual_canonical_series_loads_without_physical_canonical_sql_rows(self):
        core = Core()
        series = {
            ('ETH', '5m'): [candle(0), candle(300), candle(600), candle(900)],
            ('ETH', '15m'): [candle(0), candle(900)],
        }
        resilience.canonical_bars = lambda _core, asset, tf: series[(asset, tf)]

        market = v40._load_market_grid_safe(core, autonomous)

        self.assertEqual(market['source5'], 'canonical-grid-fixed-priority')
        self.assertEqual(market['source15'], 'canonical-grid-fixed-priority')
        self.assertEqual(market['ts5'].tolist(), [0, 300, 600, 900])
        self.assertEqual(sorted(market['close15']), [0, 900])
        self.assertEqual(core.state[v40.MARKET_STATE_KEY]['status'], 'VALID')
        self.assertTrue(core.state[v40.MARKET_STATE_KEY]['virtual_canonical_sql_deadlock_fixed'])

    def test_real_gap_fail_closes_autonomous_market_cache(self):
        core = Core()
        series = {
            ('ETH', '5m'): [candle(0), candle(300), candle(900)],
            ('ETH', '15m'): [candle(0), candle(900)],
        }
        resilience.canonical_bars = lambda _core, asset, tf: series[(asset, tf)]

        market = v40._load_market_grid_safe(core, autonomous)

        self.assertEqual(market, {})
        status = core.state[v40.MARKET_STATE_KEY]
        self.assertEqual(status['status'], 'WAITING_REAL_CANONICAL_PRICE_WINDOW')
        self.assertEqual(status['series']['ETH:5m']['first_gap_ts'], 600)
        self.assertEqual(core.state['learning']['phase'], 'WAITING_AUTONOMOUS_MARKET_CACHE_INTEGRITY')

    def test_off_grid_row_does_not_make_window_complete(self):
        core = Core()
        series = {
            ('ETH', '5m'): [candle(0), candle(300), candle(750), candle(900)],
            ('ETH', '15m'): [candle(0), candle(900)],
        }
        resilience.canonical_bars = lambda _core, asset, tf: series[(asset, tf)]

        market = v40._load_market_grid_safe(core, autonomous)

        self.assertEqual(market, {})
        self.assertEqual(core.state[v40.MARKET_STATE_KEY]['series']['ETH:5m']['first_gap_ts'], 600)


class TerminalReplayBlockerTests(unittest.TestCase):
    def setUp(self):
        self.old_progress = runtime_integrity.replay_progress

    def tearDown(self):
        runtime_integrity.replay_progress = self.old_progress

    def test_completed_replay_terminal_future_probe_is_stale(self):
        core = Core()
        runtime_integrity.replay_progress = lambda _core: {
            'complete': True,
            'percent': 100.0,
            'legal_frontier_ts': 900,
        }
        blocker = {
            'blocked': True,
            'state': 'BLOCK_FUTURE_PATH',
            'at_ts': 1800,
            'decision_close_ts': 2700,
            'reason': '5m future path is not complete yet',
        }
        self.assertTrue(v40._terminal_future_path_blocker_is_stale(core, blocker, None))

    def test_future_probe_is_not_cleared_before_replay_complete(self):
        core = Core()
        runtime_integrity.replay_progress = lambda _core: {
            'complete': False,
            'percent': 99.0,
            'legal_frontier_ts': 900,
        }
        blocker = {
            'blocked': True,
            'state': 'BLOCK_FUTURE_PATH',
            'at_ts': 1800,
            'reason': '5m future path is not complete yet',
        }
        self.assertFalse(v40._terminal_future_path_blocker_is_stale(core, blocker, None))

    def test_real_gap_prevents_terminal_blocker_clear(self):
        core = Core()
        runtime_integrity.replay_progress = lambda _core: {
            'complete': True,
            'percent': 100.0,
            'legal_frontier_ts': 900,
        }
        blocker = {
            'blocked': True,
            'state': 'BLOCK_FUTURE_PATH',
            'at_ts': 1800,
            'reason': '5m future path is not complete yet',
        }
        self.assertFalse(v40._terminal_future_path_blocker_is_stale(
            core, blocker, {'asset': 'ETH', 'tf': '5m', 'missing_ts': 1200}
        ))


if __name__ == '__main__':
    unittest.main()
