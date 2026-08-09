import unittest

import v9_multisource_derivatives as ms


class History:
    def __init__(self, key='x'):
        self.coinglass_key = key


class Core:
    START_TS = 1577836800
    def __init__(self):
        self.derivative_history = History()
        self.saved = {}
        self.state = {}
    def get_state(self, key, default=None):
        return self.saved.get(key, default)
    def set_state(self, key, value):
        self.saved[key] = value


class MultiSourceTests(unittest.TestCase):
    def test_persistent_failure_disables_only_one_source(self):
        core = Core()
        for _ in range(ms.PERSISTENT_FAILURE_LIMIT):
            ms._record(core, 'cg_book', ok=False, cursor=core.START_TS, error='400 invalid parameter')
        self.assertTrue(ms._disabled(core, 'cg_book'))
        self.assertFalse(ms._disabled(core, 'gate_stats'))
        self.assertFalse(ms._disabled(core, 'bybit_oi'))

    def test_transient_failure_never_disables_source(self):
        core = Core()
        for _ in range(ms.PERSISTENT_FAILURE_LIMIT + 3):
            ms._record(core, 'gate_stats', ok=False, cursor=core.START_TS, error='429 Too Many Requests')
        self.assertFalse(ms._disabled(core, 'gate_stats'))

    def test_readiness_uses_all_active_sources(self):
        core = Core()
        b = core.START_TS
        for source, cursor in [('cg_oi', b+9000), ('cg_liq', b+8000), ('cg_book', b+7000), ('gate_stats', b+6000), ('bybit_oi', b+5000)]:
            ms._record(core, source, ok=True, cursor=cursor)
        self.assertEqual(ms._ready_through(core), b+5000)

    def test_disabled_stuck_source_no_longer_deadlocks_readiness(self):
        core = Core()
        b = core.START_TS
        ms._record(core, 'gate_stats', ok=True, cursor=b+10000)
        ms._record(core, 'bybit_oi', ok=True, cursor=b+9000)
        for _ in range(ms.PERSISTENT_FAILURE_LIMIT):
            ms._record(core, 'cg_oi', ok=False, cursor=b, error='403 forbidden plan')
            ms._record(core, 'cg_liq', ok=False, cursor=b, error='403 forbidden plan')
            ms._record(core, 'cg_book', ok=False, cursor=b, error='403 forbidden plan')
        self.assertEqual(ms._ready_through(core), b+9000)


if __name__ == '__main__':
    unittest.main()
