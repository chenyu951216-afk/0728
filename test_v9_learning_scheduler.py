import unittest

import v5_async_runtime as async_rt
import v5_runtime


class DummyCon:
    def close(self):
        pass


class DummyDerivativeHistory:
    def __init__(self):
        self.called = 0
        self.path = None
    def set_db_path(self, path):
        self.path = path
    async def backfill_tick(self, hub, start_ts, pages=2):
        self.called += 1
        return {'ok': True, 'pages': pages}
    def status(self):
        return {'ok': True}


class DummyCore:
    START_TS = 1577836800
    DB_PATH = '/tmp/test.db'
    BACKFILL_PLAN = [('ETH', '5m')]
    TIMEFRAME_SECONDS = {'5m': 300}
    BACKFILL_PAGES_PER_TICK = 5
    def __init__(self, fail_price=False):
        self.state = {'runtime_version': '8.0.5-20260809', 'learning': {}}
        self.derivative_history = DummyDerivativeHistory()
        self.hub = object()
        self.fail_price = fail_price
        self.price_called = 0
    def ingest_completed_live_samples(self):
        return 0
    def db(self):
        return DummyCon()
    def bootstrap_progress(self, con):
        return {'overall': 99.99}
    def _earliest(self, asset, tf):
        return self.START_TS + 3600  # legacy selector still considers this incomplete
    async def backfill_one(self, asset, tf):
        self.price_called += 1
        if self.fail_price:
            raise RuntimeError('price provider timeout')
        return {'asset': asset, 'tf': tf, 'added': 0}
    def get_state(self, key, default=None):
        return default
    def set_state(self, key, value):
        pass


class SchedulerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.orig_generate = v5_runtime.generate_learning_samples_v5
        self.orig_train = v5_runtime.train_v5
        self.orig_champs = v5_runtime._all_champions
        self.orig_counts = v5_runtime._sample_counts
        self.orig_replay = v5_runtime._replay_progress
        self.orig_daily = v5_runtime.maybe_daily_report
        async def no_daily(core):
            return None
        v5_runtime.generate_learning_samples_v5 = lambda core: 14
        v5_runtime.train_v5 = lambda core: []
        v5_runtime._all_champions = lambda core: []
        v5_runtime._sample_counts = lambda core: {}
        v5_runtime._replay_progress = lambda core: 0.01
        v5_runtime.maybe_daily_report = no_daily

    async def asyncTearDown(self):
        v5_runtime.generate_learning_samples_v5 = self.orig_generate
        v5_runtime.train_v5 = self.orig_train
        v5_runtime._all_champions = self.orig_champs
        v5_runtime._sample_counts = self.orig_counts
        v5_runtime._replay_progress = self.orig_replay
        v5_runtime.maybe_daily_report = self.orig_daily

    async def test_incomplete_price_target_cannot_starve_modern_replay(self):
        core = DummyCore()
        await async_rt.learning_tick_v5_async(core)
        self.assertEqual(core.price_called, 1)
        self.assertEqual(core.derivative_history.called, 1)
        self.assertEqual(core.state['learning']['v5_samples_added'], 14)
        self.assertEqual(core.state['learning']['phase'], 'STRICT_REPLAY_ADVANCING')
        self.assertTrue(core.state['learning']['price_backfill_cannot_starve_modern_replay'])

    async def test_price_backfill_exception_is_isolated_in_modern_runtime(self):
        core = DummyCore(fail_price=True)
        await async_rt.learning_tick_v5_async(core)
        self.assertEqual(core.derivative_history.called, 1)
        self.assertEqual(core.state['learning']['v5_samples_added'], 14)
        self.assertTrue(core.state['learning']['scheduler_errors'])
        self.assertTrue(core.state['learning']['provider_failure_cannot_exceed_previous_safe_watermark'])


if __name__ == '__main__':
    unittest.main()
