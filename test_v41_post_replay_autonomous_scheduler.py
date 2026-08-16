from __future__ import annotations

import asyncio
import time
import unittest

import v16_runtime_integrity as runtime_integrity
import v41_post_replay_autonomous_scheduler as v41


class DummyApp:
    class Router:
        routes = []
    router = Router()

    def get(self, _path):
        def deco(fn):
            return fn
        return deco


class DummyTransition:
    STATE_KEY = 'dummy_transition'


class DummyCore:
    def __init__(self):
        self.state = {'runtime_role': {'role': 'LEADER'}, 'learning': {'phase': 'READY_FOR_SIGNAL_CERTIFICATION'}}
        self.persisted = {}
        self.train_calls = 0
        self.app = DummyApp()

        async def learning_tick():
            self.state['learning']['phase'] = 'READY_FOR_SIGNAL_CERTIFICATION'

        async def scan():
            return {'ok': True}

        self.learning_tick = learning_tick
        self.scan = scan

    def get_state(self, key, default=None):
        return self.persisted.get(key, default)

    def set_state(self, key, value):
        self.persisted[key] = value

    def train_if_due(self, force=False):
        self.train_calls += 1
        self.persisted[DummyTransition.STATE_KEY] = {'status': 'CERTIFICATION_QUEUED_BACKGROUND', 'certification_queued_at': int(time.time())}
        return []


class DummyAutonomous:
    def __init__(self, status=None):
        self.status = status or {'status': 'NOT_STARTED', 'research_complete': False, 'active': {}, 'champions': []}

    def autonomous_status(self, _core):
        return dict(self.status)


class SchedulerTests(unittest.TestCase):
    def setUp(self):
        self.old_replay = runtime_integrity.replay_progress
        runtime_integrity.replay_progress = lambda _core: {'complete': True, 'percent': 100.0}

    def tearDown(self):
        runtime_integrity.replay_progress = self.old_replay

    def test_boot_requests_certification(self):
        core = DummyCore()
        v41.install(type('P', (), {'core': core})(), DummyAutonomous(), DummyTransition)
        self.assertEqual(core.train_calls, 1)
        self.assertEqual(core.state[v41.STATE_KEY]['transition_after'], 'CERTIFICATION_QUEUED_BACKGROUND')

    def test_learning_tick_retries(self):
        core = DummyCore()
        v41.install(type('P', (), {'core': core})(), DummyAutonomous(), DummyTransition)
        core.train_calls = 0
        core.persisted[DummyTransition.STATE_KEY] = {'status': 'REPLAY_COMPLETE'}
        core.state[v41.STATE_KEY]['last_kick_at'] = 0
        asyncio.run(core.learning_tick())
        self.assertEqual(core.train_calls, 1)
        self.assertEqual(core.state['learning']['phase'], 'AUTONOMOUS_RESEARCH_QUEUED')

    def test_backoff_is_respected(self):
        core = DummyCore()
        core.persisted[DummyTransition.STATE_KEY] = {'status': 'CERTIFICATION_DEFERRED_MEMORY_PRESSURE', 'ready_after': int(time.time()) + 120}
        v41.install(type('P', (), {'core': core})(), DummyAutonomous(), DummyTransition)
        self.assertEqual(core.train_calls, 0)
        self.assertEqual(core.state[v41.STATE_KEY]['decision'], 'WAIT_AUTHORITATIVE_BACKOFF')

    def test_complete_does_not_requeue(self):
        core = DummyCore()
        auto = DummyAutonomous({'status': 'COMPLETE', 'research_complete': True, 'champions': [{'strategy_id': 'AUTO_X'}], 'active': {}})
        v41.install(type('P', (), {'core': core})(), auto, DummyTransition)
        self.assertEqual(core.train_calls, 0)
        self.assertEqual(core.state['learning']['phase'], 'AUTONOMOUS_RESEARCH_COMPLETE')


if __name__ == '__main__':
    unittest.main()
