from __future__ import annotations

import asyncio
import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

import v8_stability
import v8_storage_guard


class FakeStateCore:
    def __init__(self, db_path: str):
        self.DB_PATH = db_path
        self.state = {'service': 'OK', 'error': None, 'analysis': {'selection': {'tradeable': True}}}
        self._states = {}
        self.bootstrap_progress = lambda con: {'overall': 88.5}

    def db(self):
        con = sqlite3.connect(self.DB_PATH)
        con.row_factory = sqlite3.Row
        con.execute('CREATE TABLE IF NOT EXISTS market_bars(source TEXT,asset TEXT,tf TEXT,ts INTEGER,o REAL,h REAL,l REAL,c REAL,v REAL,qv REAL)')
        con.execute('CREATE TABLE IF NOT EXISTS learning_samples(ts INTEGER,strategy TEXT,direction TEXT)')
        con.execute('CREATE TABLE IF NOT EXISTS model_registry(status TEXT)')
        con.execute('CREATE TABLE IF NOT EXISTS system_state(key TEXT PRIMARY KEY,value TEXT,updated_at INTEGER)')
        con.commit()
        return con

    def get_state(self, key, default=None):
        return self._states.get(key, default)

    def set_state(self, key, value):
        self._states[key] = value

    def latest_signal(self, *args, **kwargs):
        return None


class StorageGuardTests(unittest.TestCase):
    def test_storage_status_reads_real_database_counts(self):
        with tempfile.TemporaryDirectory() as d:
            path = str(Path(d) / 'eth.db')
            core = FakeStateCore(path)
            con = core.db()
            for i in range(20):
                con.execute('INSERT INTO market_bars VALUES(?,?,?,?,?,?,?,?,?,?)', ('gate','ETH','15m',1577836800+i*900,1,1,1,1,1,1))
            for i in range(7):
                con.execute('INSERT INTO learning_samples VALUES(?,?,?)', (i,'X','LONG'))
            con.commit(); con.close()
            status = v8_storage_guard.storage_status(core, update_identity=False)
            self.assertEqual(status['market_bars'], 20)
            self.assertEqual(status['learning_samples'], 7)
            self.assertEqual(status['historical_price_coverage'], 88.5)

    def test_larger_alternative_database_is_reported(self):
        with tempfile.TemporaryDirectory() as d:
            current = Path(d) / 'eth_adaptive.db'
            current.write_bytes(b'x' * 1024)
            other = Path(d) / 'old.db'
            other.write_bytes(b'x' * 6_000_000)
            core = FakeStateCore(str(current))
            status = v8_storage_guard.storage_status(core, update_identity=False)
            self.assertTrue(status['possible_db_mismatch'])
            self.assertIn('old.db', status['reason'])


class StabilityTests(unittest.IsolatedAsyncioTestCase):
    async def test_single_scan_failure_recovers_without_persistent_error(self):
        core = SimpleNamespace()
        core.state = {'service':'OK','error':None,'analysis':{'selection':{'tradeable':False}},'storage':{'healthy':True}}
        core.latest_signal = lambda *a, **k: None
        calls = {'n': 0}
        async def scan():
            calls['n'] += 1
            if calls['n'] == 1:
                raise RuntimeError('temporary timeout')
            return {'snapshot_ts': 123, 'selection': {'tradeable': False}}
        result = await v8_stability._safe_scan(core, scan)
        self.assertEqual(result['snapshot_ts'], 123)
        self.assertEqual(core.state['subsystem_health']['market_scan']['status'], 'OK')
        self.assertEqual(core.state['subsystem_health']['market_scan']['consecutive_errors'], 0)

    async def test_repeated_scan_failure_fail_closes_old_analysis(self):
        core = SimpleNamespace()
        core.state = {'service':'OK','error':None,'analysis':{'selection':{'tradeable':True}},'storage':{'healthy':True}}
        core.latest_signal = lambda *a, **k: None
        async def scan():
            raise RuntimeError('exchange unavailable')
        await v8_stability._safe_scan(core, scan)
        self.assertTrue(core.state['analysis_stale'])
        self.assertFalse(core.state['analysis']['selection']['tradeable'])
        self.assertIn('fail-closed', core.state['analysis']['selection']['reason'])

    async def test_learning_failure_is_isolated_from_market_health(self):
        core = SimpleNamespace()
        core.state = {'service':'OK','error':None,'analysis':{},'storage':{'healthy':True}}
        core.latest_signal = lambda *a, **k: None
        async def learning():
            raise RuntimeError('training failed')
        old = v8_stability.LEARNING_RETRY_SECONDS
        v8_stability.LEARNING_RETRY_SECONDS = 0
        try:
            await v8_stability._safe_learning(core, learning)
        finally:
            v8_stability.LEARNING_RETRY_SECONDS = old
        self.assertEqual(core.state['subsystem_health']['learning']['status'], 'DEGRADED')
        self.assertNotEqual(core.state.get('service'), 'DEGRADED')


if __name__ == '__main__':
    unittest.main()
