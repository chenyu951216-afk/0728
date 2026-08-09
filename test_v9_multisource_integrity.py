import sqlite3
import tempfile
import unittest

import v5_runtime
import v9_multisource_derivatives as ms
import v9_multisource_integrity as integ


class Core:
    START_TS = 1577836800
    def __init__(self, path):
        self.path = path
        self.saved = {}
        self.state = {}
    def db(self):
        con = sqlite3.connect(self.path)
        con.execute('CREATE TABLE IF NOT EXISTS learning_samples(ts INTEGER,strategy TEXT,direction TEXT,regime TEXT,phase TEXT,features TEXT,success INTEGER,pnl_r REAL,mfe_r REAL,mae_r REAL,source_quality REAL)')
        con.execute('CREATE TABLE IF NOT EXISTS learning_feature_snapshots(ts INTEGER PRIMARY KEY,features TEXT)')
        con.execute('CREATE TABLE IF NOT EXISTS model_registry(strategy TEXT,direction TEXT,version INTEGER,status TEXT,created_at INTEGER,metrics TEXT,model BLOB)')
        con.execute('CREATE TABLE IF NOT EXISTS execution_registry_v7(strategy TEXT,direction TEXT,model_version INTEGER,version INTEGER,status TEXT,created_at INTEGER,metrics TEXT,policy TEXT)')
        con.commit(); return con
    def get_state(self, key, default=None):
        return self.saved.get(key, default)
    def set_state(self, key, value):
        self.saved[key] = value


class IntegrityTests(unittest.TestCase):
    def test_source_change_resets_labels_not_market_cache(self):
        with tempfile.NamedTemporaryFile(suffix='.db') as f:
            core = Core(f.name)
            con = core.db()
            con.execute("INSERT INTO learning_samples VALUES(?,?,?,?,?,?,?,?,?,?,?)", (core.START_TS+900,'S','LONG','R','P','{}',1,1,1,0,100))
            con.execute("INSERT INTO learning_feature_snapshots VALUES(?,?)", (core.START_TS+900,'{}'))
            con.commit(); con.close()
            core.set_state(v5_runtime.REPLAY_STATE_KEY, core.START_TS+900)
            integ._reset_learning_generation(core, 'cg_book', 'provider rejected')
            con = core.db()
            self.assertEqual(con.execute('SELECT COUNT(*) FROM learning_samples').fetchone()[0], 0)
            self.assertEqual(con.execute('SELECT COUNT(*) FROM learning_feature_snapshots').fetchone()[0], 0)
            con.close()
            self.assertEqual(core.get_state(v5_runtime.REPLAY_STATE_KEY), core.START_TS)


if __name__ == '__main__':
    unittest.main()
