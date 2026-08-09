import unittest

import v9_derivative_gate as gate


class History:
    def __init__(self, key='x', states=None):
        self.coinglass_key = key
        self.states = states or {}
    def _get_state(self, key, default=None):
        return self.states.get(key, default)


class Core:
    START_TS = 1577836800
    def __init__(self, history=None):
        self.derivative_history = history or History()
        self.saved = {}
        self.state = {}
    def get_state(self, key, default=None):
        return self.saved.get(key, default)
    def set_state(self, key, value):
        self.saved[key] = value


class DerivativeGateTests(unittest.TestCase):
    def test_repeated_persistent_enrichment_error_is_excluded(self):
        core = Core()
        result = {'errors': ["liq_long_usd: 400 Bad Request invalid parameter"], 'attempted': []}
        gate._update_gate_state(core, result)
        state = gate._update_gate_state(core, result)
        self.assertIn('liq_long_usd', state['disabled_metrics'])
        self.assertFalse(state['global_disabled'])

    def test_transient_error_never_auto_disables_enrichment(self):
        core = Core()
        result = {'errors': ["book_imbalance: 429 Too Many Requests"], 'attempted': []}
        for _ in range(5):
            state = gate._update_gate_state(core, result)
        self.assertNotIn('book_imbalance', state['disabled_metrics'])
        self.assertFalse(state['global_disabled'])

    def test_readiness_ignores_explicitly_disabled_enrichment(self):
        base = Core.START_TS
        history = History(states={
            'cg_cursor:oi_usd': base + 5000,
            'cg_cursor:liq_long_usd': base,
            'cg_cursor:book_imbalance': base + 4000,
        })
        core = Core(history)
        core.saved[gate.STATE_KEY] = {
            'disabled_metrics': {'liq_long_usd': {'reason': '400'}},
            'metrics': {}, 'global_disabled': False,
        }
        self.assertEqual(gate._ready_through(core), base + 4000)

    def test_auth_failure_disables_coinglass_gate_but_not_learning(self):
        core = Core()
        result = {'errors': ["oi_usd: 401 Unauthorized invalid API key"], 'attempted': []}
        gate._update_gate_state(core, result)
        state = gate._update_gate_state(core, result)
        self.assertTrue(state['global_disabled'])
        self.assertIsNone(gate._ready_through(core))

    def test_core_non_auth_bad_request_remains_fail_closed(self):
        base = Core.START_TS
        core = Core(History(states={'cg_cursor:oi_usd': base}))
        result = {'errors': ["oi_usd: 400 Bad Request invalid parameter"], 'attempted': []}
        for _ in range(3):
            state = gate._update_gate_state(core, result)
        self.assertFalse(state['global_disabled'])
        self.assertEqual(gate._ready_through(core), base)


if __name__ == '__main__':
    unittest.main()
