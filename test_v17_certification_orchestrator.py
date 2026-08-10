import sqlite3
import tempfile
import unittest
from unittest.mock import patch

import v5_runtime
import v12_clean_baseline
import v17_certification_orchestrator as cert


class Core:
    START_TS = 1577836800
    def __init__(self, path):
        self.path = path
        self.saved = {
            'point_in_time_sample_schema': 6,
            'replay_cursor_integrity_schema': 2,
            'final_data_resilience_schema': 1,
            v12_clean_baseline.STATE_KEY: {'clean': True, 'status': 'CLEAN', 'dataset_id': 'dataset-test'},
        }
        self.state = {}
    def db(self):
        con = sqlite3.connect(self.path)
        con.row_factory = sqlite3.Row
        con.execute('CREATE TABLE IF NOT EXISTS learning_samples(ts INTEGER,strategy TEXT,direction TEXT,regime TEXT,phase TEXT,features TEXT,success INTEGER,pnl_r REAL,mfe_r REAL,mae_r REAL,source_quality REAL,PRIMARY KEY(ts,strategy,direction))')
        con.execute('CREATE TABLE IF NOT EXISTS learning_feature_snapshots(ts INTEGER PRIMARY KEY,features TEXT NOT NULL)')
        con.execute('CREATE TABLE IF NOT EXISTS model_registry(strategy TEXT,version INTEGER,status TEXT,created_at INTEGER,metrics TEXT,model BLOB,direction TEXT,PRIMARY KEY(strategy,version))')
        con.execute('CREATE TABLE IF NOT EXISTS execution_registry_v7(strategy TEXT,direction TEXT,model_version INTEGER,execution_version INTEGER,status TEXT)')
        con.execute('CREATE TABLE IF NOT EXISTS market_bars(source TEXT,asset TEXT,tf TEXT,ts INTEGER,o REAL,h REAL,l REAL,c REAL,v REAL,qv REAL,PRIMARY KEY(source,asset,tf,ts))')
        con.commit(); return con
    def get_state(self, key, default=None): return self.saved.get(key, default)
    def set_state(self, key, value): self.saved[key] = value


def populate_full_decision(core, ts):
    con = core.db()
    con.execute('INSERT OR REPLACE INTO learning_feature_snapshots(ts,features) VALUES(?,?)', (ts, '{"ret_1":0.1}'))
    for strategy in v5_runtime.STRATEGIES:
        for direction in v5_runtime.DIRECTIONS:
            con.execute('INSERT INTO learning_samples VALUES(?,?,?,?,?,?,?,?,?,?,?)', (ts,strategy,direction,'RANGE_LOW_VOL','RANGE','@'+str(ts),1,0.5,1.0,0.2,80.0))
    con.commit(); con.close()


class CertificationOrchestratorTests(unittest.TestCase):
    def test_clean_schema6_samples_are_reused_without_rebuild(self):
        with tempfile.NamedTemporaryFile(suffix='.db') as f:
            core = Core(f.name)
            ts = core.START_TS + 100 * 86400
            populate_full_decision(core, ts)
            with patch.object(cert.runtime_integrity, 'replay_progress', return_value={'complete': True, 'legal_frontier_ts': ts, 'cursor_ts': ts}), patch.object(cert.resilience, '_load', return_value={'effective_model_start': core.START_TS + 80 * 86400}):
                out = cert.audit_derived_dataset(core)
            self.assertTrue(out['valid'])
            self.assertEqual(out['status'], 'VALID')
            self.assertEqual(out['learning_samples'], 14)
            self.assertEqual(out['partial_decision_timestamps'], 0)
            con = core.db(); self.assertEqual(con.execute('SELECT COUNT(*) FROM learning_samples').fetchone()[0], 14); con.close()

    def test_partial_cross_version_decision_auto_rebuilds_only_derived_data(self):
        with tempfile.NamedTemporaryFile(suffix='.db') as f:
            core = Core(f.name)
            ts = core.START_TS + 100 * 86400
            populate_full_decision(core, ts)
            con = core.db()
            con.execute('DELETE FROM learning_samples WHERE strategy=? AND direction=?', (v5_runtime.STRATEGIES[0], v5_runtime.DIRECTIONS[0]))
            con.execute("INSERT INTO market_bars VALUES('gate','ETH','15m',?,?,?,?,?,?,?)", (ts,100,101,99,100,1,100))
            con.commit(); con.close()
            core.saved[v5_runtime.REPLAY_STATE_KEY] = ts
            with patch.object(cert.runtime_integrity, 'replay_progress', return_value={'complete': True, 'legal_frontier_ts': ts, 'cursor_ts': ts}), patch.object(cert.resilience, '_load', return_value={'effective_model_start': core.START_TS + 80 * 86400}):
                out = cert.audit_derived_dataset(core)
            self.assertEqual(out['status'], 'AUTO_REBUILDING_DERIVED_ONLY')
            con = core.db()
            self.assertEqual(con.execute('SELECT COUNT(*) FROM learning_samples').fetchone()[0], 0)
            self.assertEqual(con.execute('SELECT COUNT(*) FROM market_bars').fetchone()[0], 1)
            con.close()
            self.assertEqual(core.saved[v12_clean_baseline.STATE_KEY]['dataset_id'], 'dataset-test')
            self.assertEqual(core.saved[v5_runtime.REPLAY_STATE_KEY], core.START_TS)

    def test_completed_replay_is_immediate_initial_certification_trigger(self):
        state = {'status': 'NOT_STARTED', 'last_sample_total': 0, 'last_sample_max_ts': None, 'last_completed_at': 0}
        self.assertTrue(cert._certification_due(None, state, {'total': 1000, 'max_ts': 123}, False))

    def test_same_signature_does_not_recertify_immediately(self):
        state = {'status': 'NO_SIGNAL_MODEL_PASSED_OOS', 'last_sample_total': 1000, 'last_sample_max_ts': 123, 'last_completed_at': 10**12}
        self.assertFalse(cert._certification_due(None, state, {'total': 1000, 'max_ts': 123}, False))


if __name__ == '__main__':
    unittest.main()
