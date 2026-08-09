import asyncio
import sqlite3
import tempfile
import unittest

import adaptive_v5
import v11_sqlite_stability as stab


class Core:
    def __init__(self, path):
        self.DB_PATH = path
        self.state = {'subsystem_health': {}, 'learning': {}}
        self.SCAN_SECONDS = 60
        self.app = type('App', (), {'version': 'x'})()
        self.derivative_history = type('DH', (), {})()
        self.derivative_history.db_path = path
        self.derivative_history.ensure_schema = lambda: None
        self.derivative_history._con = lambda: sqlite3.connect(path)
        self.learning_tick = self._learning
        self.scan = self._scan
        self.scan_worker = None
        self.latest_signal = lambda: None
    def db(self):
        con = sqlite3.connect(self.DB_PATH, timeout=5, check_same_thread=False)
        con.row_factory = sqlite3.Row
        con.execute('PRAGMA journal_mode=WAL')
        con.execute('CREATE TABLE IF NOT EXISTS system_state(key TEXT PRIMARY KEY,value TEXT,updated_at INTEGER)')
        adaptive_v5.ModelStore(con)
        con.commit()
        return con
    async def _learning(self):
        await asyncio.sleep(0)
    async def _scan(self):
        return {}


class SqliteStabilityTests(unittest.TestCase):
    def test_sample_writer_releases_lock_each_full_decision(self):
        with tempfile.NamedTemporaryFile(suffix='.db') as f:
            con = sqlite3.connect(f.name)
            adaptive_v5.ModelStore(con)
            con.close()
            stab._install_short_sample_transactions()
            writer = sqlite3.connect(f.name, timeout=1)
            store = adaptive_v5.ModelStore(writer)
            row = {'ts': 1, 'strategy': 'TREND_PULLBACK', 'direction': 'LONG', 'regime': 'R', 'phase': 'P', 'features': {}, 'success': 1, 'pnl_r': 1.0, 'mfe_r': 1.0, 'mae_r': 0.0, 'source_quality': 100.0}
            for i in range(stab.SAMPLE_COMMIT_EVERY):
                x = dict(row); x['ts'] = i + 1
                store.add_sample(x)
            reader = sqlite3.connect(f.name, timeout=1)
            n = reader.execute('SELECT COUNT(*) FROM learning_samples').fetchone()[0]
            reader.close(); writer.close()
            self.assertEqual(n, stab.SAMPLE_COMMIT_EVERY)

    def test_runtime_connection_uses_short_busy_timeout(self):
        with tempfile.NamedTemporaryFile(suffix='.db') as f:
            core = Core(f.name)
            stab._install_light_connections(core)
            con = core.db()
            timeout = con.execute('PRAGMA busy_timeout').fetchone()[0]
            mode = con.execute('PRAGMA journal_mode').fetchone()[0]
            con.close()
            self.assertEqual(timeout, stab.BUSY_TIMEOUT_MS)
            self.assertEqual(str(mode).lower(), 'wal')

    def test_learning_health_reports_running(self):
        with tempfile.NamedTemporaryFile(suffix='.db') as f:
            core = Core(f.name)
            stab._set_running_health(core)
            self.assertEqual(core.state['subsystem_health']['learning']['status'], 'RUNNING')
            self.assertEqual(core.state['learning']['runtime_status'], 'RUNNING')


if __name__ == '__main__':
    unittest.main()
