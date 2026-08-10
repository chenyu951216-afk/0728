import json
import sqlite3
import tempfile
import unittest
from datetime import timezone
from unittest.mock import patch

import v5_runtime
import v7_runtime
import v12_clean_baseline
import v15_data_resilience as resilience
import v16_runtime_integrity as runtime


class ReplayCore:
    START_TS = 1577836800
    TIMEFRAME_SECONDS = {'5m':300,'15m':900,'30m':1800,'1h':3600,'4h':14400,'1d':86400}
    def __init__(self, path):
        self.path = path
        self.saved = {}
        self.state = {}
    def db(self):
        con = sqlite3.connect(self.path)
        con.row_factory = sqlite3.Row
        con.execute('CREATE TABLE IF NOT EXISTS market_bars(source TEXT,asset TEXT,tf TEXT,ts INTEGER,o REAL,h REAL,l REAL,c REAL,v REAL,qv REAL,PRIMARY KEY(source,asset,tf,ts))')
        con.execute('CREATE TABLE IF NOT EXISTS learning_samples(ts INTEGER,strategy TEXT,direction TEXT,regime TEXT,phase TEXT,features TEXT,success INTEGER,pnl_r REAL,mfe_r REAL,mae_r REAL,source_quality REAL)')
        con.execute('CREATE TABLE IF NOT EXISTS model_registry(strategy TEXT,version INTEGER,status TEXT,created_at INTEGER,metrics TEXT,model BLOB,direction TEXT)')
        con.execute('CREATE TABLE IF NOT EXISTS execution_registry_v7(strategy TEXT,direction TEXT,model_version INTEGER,version INTEGER,status TEXT,created_at INTEGER,metrics TEXT,policy TEXT)')
        con.execute('CREATE TABLE IF NOT EXISTS system_state(key TEXT PRIMARY KEY,value TEXT,updated_at INTEGER)')
        con.commit(); return con
    def get_state(self, key, default=None):
        return self.saved.get(key, default)
    def set_state(self, key, value):
        self.saved[key] = value


def add_bars(core, asset, tf, count, step):
    con = core.db()
    rows = []
    for i in range(count):
        ts = core.START_TS + i * step
        px = 1000 + i * .1
        rows.append(('gate', asset, tf, ts, px, px+2, px-2, px+1, 100, 1000))
    con.executemany('INSERT OR IGNORE INTO market_bars VALUES(?,?,?,?,?,?,?,?,?,?)', rows)
    con.commit(); con.close()


class RuntimeFrontierTests(unittest.TestCase):
    def setUp(self):
        resilience._CANON_CACHE.clear()

    def test_completed_replay_is_measured_against_matured_label_frontier(self):
        with tempfile.NamedTemporaryFile(suffix='.db') as f:
            core = ReplayCore(f.name)
            add_bars(core, 'ETH', '15m', 500, 900)
            add_bars(core, 'ETH', '5m', 1500, 300)
            frontier = runtime._legal_frontier(core)
            self.assertTrue(frontier['ready'])
            # The legal frontier must intentionally trail the live edge by >7.5h,
            # proving why the former latest-30-bars completion rule could deadlock.
            self.assertGreater(frontier['latest_market_ts'] - frontier['legal_frontier_ts'], 30 * 900)
            core.set_state(v5_runtime.REPLAY_STATE_KEY, frontier['legal_frontier_ts'])
            progress = runtime.replay_progress(core)
            self.assertTrue(progress['complete'])
            self.assertEqual(progress['percent'], 100.0)
            self.assertEqual(progress['pending_eligible_decisions'], 0)
            self.assertEqual(progress['completion_basis'], 'LATEST_LEGALLY_LABELABLE_DECISION_NOT_LIVE_EDGE')

    def test_one_matured_stride_behind_is_not_complete(self):
        with tempfile.NamedTemporaryFile(suffix='.db') as f:
            core = ReplayCore(f.name)
            add_bars(core, 'ETH', '15m', 500, 900)
            add_bars(core, 'ETH', '5m', 1500, 300)
            frontier = runtime._legal_frontier(core)
            core.set_state(v5_runtime.REPLAY_STATE_KEY, frontier['legal_frontier_ts'] - 2 * 900)
            progress = runtime.replay_progress(core)
            self.assertFalse(progress['complete'])
            self.assertGreaterEqual(progress['pending_eligible_decisions'], 1)
            self.assertLess(progress['percent'], 100.0)

    def test_pipeline_becomes_ready_without_requiring_live_edge(self):
        with tempfile.NamedTemporaryFile(suffix='.db') as f:
            core = ReplayCore(f.name)
            add_bars(core, 'ETH', '15m', 500, 900)
            add_bars(core, 'ETH', '5m', 1500, 300)
            frontier = runtime._legal_frontier(core)
            core.set_state(v5_runtime.REPLAY_STATE_KEY, frontier['legal_frontier_ts'])
            core.set_state(v12_clean_baseline.STATE_KEY, {
                'baseline_id': 'final-clean-baseline-20260809-v1', 'dataset_id': 'test', 'clean': True, 'status': 'CLEAN'
            })
            core.set_state(resilience.STATE_KEY, {
                'version': resilience.VERSION, 'source_set_frozen': True,
                'model_oi_sources': [], 'model_funding_sources': [], 'model_enrichment_sources': [],
                'provider_capabilities': {}, 'effective_model_start': core.START_TS + resilience.WARMUP_SECONDS,
            })
            core.set_state(resilience.GAP_KEY, {'version':1,'gaps':{}})
            pipe = runtime.certification_pipeline(core)
            self.assertTrue(pipe['signal_training_ready'])
            self.assertEqual(pipe['stage'], 'READY_FOR_SIGNAL_CERTIFICATION')


class SafeScanTests(unittest.IsolatedAsyncioTestCase):
    async def test_scan_uses_composed_core_create_signal_gate(self):
        class Hub:
            async def live_bundle(self):
                return {'eth_15m':[{'ts':1700000000,'o':100,'h':101,'l':99,'c':100,'v':1,'qv':1}]}
        class Core:
            def __init__(self):
                self.hub=Hub(); self.state={'scan_count':0}; self.timezone=type('TZ',(),{'utc':timezone.utc}); self.created=0
            def upsert_live_gate(self,b): pass
            def _analysis_from_bundle(self,b): return {'selection':{'tradeable':True},'price':100,'regime':{},'features':{}}
            def db(self):
                con=sqlite3.connect(':memory:')
                con.execute('CREATE TABLE snapshots(ts INTEGER,payload TEXT)')
                return con
            def latest_signal(self): return None
            def create_signal(self,a,m): self.created+=1; return None
            def get_state(self,k,d=None): return d
        core=Core()
        with patch.object(v7_runtime, 'reentry_gate', lambda c,a,m:{'allowed':True}), \
             patch.object(v5_runtime, 'robust_send_discord', new=lambda *a,**k: None):
            await runtime._safe_scan(core)
        self.assertEqual(core.created,1)


class ManualTrainingTests(unittest.TestCase):
    def test_legacy_manual_train_name_routes_into_modern_train_chain(self):
        core=object()
        with patch.object(v5_runtime,'train_v5',return_value=[{'promoted':False}]) as fn:
            out=runtime._safe_manual_train(core,True)
        self.assertEqual(out,[{'promoted':False}])
        fn.assert_called_once_with(core)


if __name__ == '__main__':
    unittest.main()
