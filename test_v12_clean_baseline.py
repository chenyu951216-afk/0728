import json
import sqlite3
import tempfile
import unittest

import v12_clean_baseline as clean


class Core:
    def __init__(self, path):
        self.path = path
        self.state = {}
        self.saved = {}
    def db(self):
        con = sqlite3.connect(self.path)
        con.row_factory = sqlite3.Row
        con.execute('CREATE TABLE IF NOT EXISTS market_bars(source TEXT,asset TEXT,tf TEXT,ts INTEGER,o REAL,h REAL,l REAL,c REAL,v REAL,qv REAL)')
        con.execute('CREATE TABLE IF NOT EXISTS derivative_history(source TEXT,metric TEXT,ts INTEGER,value REAL,quality REAL,meta TEXT)')
        con.execute('CREATE TABLE IF NOT EXISTS learning_samples(ts INTEGER,strategy TEXT,direction TEXT,regime TEXT,phase TEXT,features TEXT,success INTEGER,pnl_r REAL,mfe_r REAL,mae_r REAL,source_quality REAL)')
        con.execute('CREATE TABLE IF NOT EXISTS learning_feature_snapshots(ts INTEGER PRIMARY KEY,features TEXT)')
        con.execute('CREATE TABLE IF NOT EXISTS signals(signal_id TEXT)')
        con.commit(); return con
    def get_state(self, key, default=None):
        return self.saved.get(key, default)
    def set_state(self, key, value):
        self.saved[key] = json.loads(json.dumps(value))


class CleanBaselineTests(unittest.TestCase):
    def test_empty_database_gets_clean_baseline(self):
        with tempfile.NamedTemporaryFile(suffix='.db') as f:
            core = Core(f.name)
            marker = clean.initialize_or_classify(core)
            self.assertTrue(marker['clean'])
            self.assertEqual(marker['status'], 'CLEAN')
            self.assertTrue(marker['dataset_id'])
            self.assertTrue(clean._is_clean(core))

    def test_existing_raw_cache_is_never_retro_certified_clean(self):
        with tempfile.NamedTemporaryFile(suffix='.db') as f:
            core = Core(f.name)
            con = core.db()
            con.execute("INSERT INTO market_bars VALUES('gate','ETH','15m',1,1,1,1,1,1,1)")
            con.commit(); con.close()
            marker = clean.initialize_or_classify(core)
            self.assertFalse(marker['clean'])
            self.assertEqual(marker['status'], 'LEGACY_CARRYOVER')
            self.assertFalse(clean._is_clean(core))

    def test_clean_marker_remains_clean_after_new_rows_arrive(self):
        with tempfile.NamedTemporaryFile(suffix='.db') as f:
            core = Core(f.name)
            first = clean.initialize_or_classify(core)
            con = core.db()
            con.execute("INSERT INTO market_bars VALUES('gate','ETH','15m',1,1,1,1,1,1,1)")
            con.commit(); con.close()
            second = clean.initialize_or_classify(core)
            self.assertTrue(second['clean'])
            self.assertEqual(first['dataset_id'], second['dataset_id'])


if __name__ == '__main__':
    unittest.main()
