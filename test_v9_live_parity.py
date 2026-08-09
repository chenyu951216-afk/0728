import unittest
from unittest.mock import patch

import v9_live_parity as parity


class Router:
    routes = []


class App:
    def __init__(self):
        self.router = Router()
        self.version = 'x'
    def get(self, _path):
        def deco(fn):
            return fn
        return deco


class Core:
    def __init__(self):
        self.state = {'strict_replay': {}}
        self.app = App()
        self.derivative_history = object()
        self.created = []
        self._raw_derivatives = lambda bundle: {
            'oi_change': .9, 'funding': .001, 'book_imbalance': .8,
            'oi_available': 1.0, 'funding_available': 1.0, 'book_available': 1.0,
            'source_agreement_bps': 7.0,
        }
        self.create_signal = self._create
    def _create(self, analysis, m15):
        self.created.append((analysis, m15))
        return {'ok': True, 'price': analysis['price']}


class LiveParityTests(unittest.TestCase):
    def test_live_model_derivatives_use_strict_historical_semantics(self):
        core = Core()
        with patch('v9_live_parity.v9_final._strict_derivative_extras', return_value={
            'oi_change': .12, 'funding': .0002, 'book_imbalance': -.1,
            'oi_available': 1.0, 'funding_available': 1.0, 'book_available': 1.0,
            'liquidation_available': 0.0, 'derivative_coverage': .75, 'derivative_quality': .8,
        }):
            parity.install(core)
            out = core._raw_derivatives({'eth_15m': [{'ts': 1000, 'c': 100}]})
        self.assertEqual(out['oi_change'], .12)
        self.assertEqual(out['book_imbalance'], -.1)
        self.assertEqual(out['spot_perp_basis_bps'], 0.0)
        self.assertEqual(out['source_agreement_bps'], 7.0)
        self.assertFalse(core.state['live_derivative_parity']['instantaneous_fields_used_by_signal_model'])

    def test_fresh_live_signal_uses_closed_15m_reference_price(self):
        core = Core(); parity.install(core)
        decision_close = 10_000
        m15 = [{'ts': decision_close - 900, 'c': 101.25}]
        with patch('v9_live_parity.time.time', return_value=decision_close + 60):
            result = core.create_signal({'price': 105.0}, m15)
        self.assertTrue(result['ok'])
        self.assertEqual(core.created[-1][0]['price'], 101.25)
        self.assertEqual(core.created[-1][0]['live_ticker_at_scan'], 105.0)
        self.assertTrue(core.state['strict_live_decision']['fresh'])

    def test_stale_recovery_cannot_create_new_signal(self):
        core = Core(); parity.install(core)
        decision_close = 10_000
        m15 = [{'ts': decision_close - 900, 'c': 101.25}]
        with patch('v9_live_parity.time.time', return_value=decision_close + parity.MAX_DECISION_AGE_SECONDS + 1):
            result = core.create_signal({'price': 99.0}, m15)
        self.assertIsNone(result)
        self.assertEqual(core.created, [])
        self.assertFalse(core.state['strict_live_decision']['fresh'])


if __name__ == '__main__':
    unittest.main()
