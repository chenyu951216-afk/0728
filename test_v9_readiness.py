import unittest

import v9_readiness as readiness


class History:
    def __init__(self, key='x', states=None):
        self.coinglass_key = key
        self.states = states or {}
    def _get_state(self, key, default=None):
        return self.states.get(key, default)


class Core:
    START_TS = 1577836800
    def __init__(self, history):
        self.derivative_history = history


class ReplayReadinessTests(unittest.TestCase):
    def test_no_coinglass_does_not_block_replay(self):
        self.assertIsNone(readiness._coinglass_ready_through(Core(History(key=''))))

    def test_ready_through_uses_slowest_processed_metric(self):
        base = Core.START_TS
        core = Core(History(states={
            'cg_cursor:oi_usd': base + 2000,
            'cg_cursor:liq_long_usd': base + 1800,
            'cg_cursor:book_imbalance': base + 2300,
        }))
        self.assertEqual(readiness._coinglass_ready_through(core), base + 1800)

    def test_missing_cursor_fails_closed_to_learning_start(self):
        core = Core(History(states={'cg_cursor:oi_usd': 2000}))
        self.assertEqual(readiness._coinglass_ready_through(core), core.START_TS)


if __name__ == '__main__':
    unittest.main()
