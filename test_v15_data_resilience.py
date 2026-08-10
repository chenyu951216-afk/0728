import sqlite3
import tempfile
import time
import unittest

from derivative_data import DerivativeHistory
from market_data import Candle
import v5_runtime
import v10_final_integrity as fin
import v12_clean_baseline
import v15_data_resilience as res


class Core:
    START_TS = 1577836800
    TIMEFRAME_SECONDS = {'5m':300,'15m':900,'30m':1800,'1h':3600,'4h':14400,'1d':86400}
    def __init__(self, path):
        self.path = path
        self.saved = {}
        self.state = {}
        self.derivative_history = DerivativeHistory(path)
        self.derivative_history.ensure_schema()
        self.hub = None
    def db(self):
        con = sqlite3.connect(self.path)
        con.row_factory = sqlite3.Row
        con.execute('CREATE TABLE IF NOT EXISTS system_state(key TEXT PRIMARY KEY,value TEXT,updated_at INTEGER)')
        con.execute('CREATE TABLE IF NOT EXISTS market_bars(source TEXT,asset TEXT,tf TEXT,ts INTEGER,o REAL,h REAL,l REAL,c REAL,v REAL,qv REAL,PRIMARY KEY(source,asset,tf,ts))')
        con.execute('CREATE TABLE IF NOT EXISTS learning_samples(ts INTEGER,strategy TEXT,direction TEXT,regime TEXT,phase TEXT,features TEXT,success INTEGER,pnl_r REAL,mfe_r REAL,mae_r REAL,source_quality REAL,PRIMARY KEY(ts,strategy,direction))')
        con.execute('CREATE TABLE IF NOT EXISTS learning_feature_snapshots(ts INTEGER PRIMARY KEY,features TEXT)')
        con.execute('CREATE TABLE IF NOT EXISTS model_registry(strategy TEXT,direction TEXT,version INTEGER,status TEXT,created_at INTEGER,metrics TEXT,model BLOB)')
        con.execute('CREATE TABLE IF NOT EXISTS execution_registry_v7(strategy TEXT,direction TEXT,model_version INTEGER,version INTEGER,status TEXT,created_at INTEGER,metrics TEXT,policy TEXT)')
        con.commit()
        return con
    def get_state(self, key, default=None):
        return self.saved.get(key, default)
    def set_state(self, key, value):
        self.saved[key] = value
    def insert_bars(self, source, asset, tf, rows):
        if not rows:
            return 0
        con = self.db()
        before = con.total_changes
        con.executemany(
            'INSERT OR IGNORE INTO market_bars(source,asset,tf,ts,o,h,l,c,v,qv) VALUES(?,?,?,?,?,?,?,?,?,?)',
            [(source,asset,tf,int(x['ts']),float(x['o']),float(x['h']),float(x['l']),float(x['c']),float(x.get('v',0)),float(x.get('qv',0))) for x in rows]
        )
        n = con.total_changes - before
        con.commit(); con.close()
        return n


class Hub:
    def __init__(self, exact_source=None, exact_ts=None, transient_source=None):
        self.exact_source = exact_source
        self.exact_ts = exact_ts
        self.transient_source = transient_source
    async def fetch_history(self, source, asset, tf, end_ts=None, limit=30):
        if source == self.transient_source:
            raise RuntimeError('timeout while fetching')
        if source == self.exact_source:
            ts = int(self.exact_ts)
            return [Candle(ts=ts,o=100,h=102,l=99,c=101,v=10,qv=1000,source=source)]
        return []


class DataResilienceTests(unittest.TestCase):
    def test_parse_coinglass_range_limit_ms(self):
        self.assertEqual(
            res.parse_provider_earliest('Coinglass code=400 msg=Invalid time range: the earliest allowed start_time is 1770797000000'),
            1770797000,
        )

    def test_canonical_price_priority_is_per_timestamp_not_future_row_count(self):
        with tempfile.NamedTemporaryFile(suffix='.db') as f:
            c = Core(f.name)
            t = c.START_TS
            c.insert_bars('bybit','ETH','5m',[{'ts':t,'o':20,'h':21,'l':19,'c':20,'v':1,'qv':1},{'ts':t+300,'o':21,'h':22,'l':20,'c':21,'v':1,'qv':1}])
            c.insert_bars('gate','ETH','5m',[{'ts':t,'o':10,'h':11,'l':9,'c':10,'v':1,'qv':1}])
            c.insert_bars('binance','ETH','5m',[{'ts':t+600,'o':30,'h':31,'l':29,'c':30,'v':1,'qv':1}])
            rows = res.canonical_bars(c,'ETH','5m')
            self.assertEqual([(x['ts'],x['_source'],x['c']) for x in rows], [(t,'gate',10.0),(t+300,'bybit',21.0),(t+600,'binance',30.0)])

    def test_range_limited_source_is_not_full_span_model_eligible(self):
        with tempfile.NamedTemporaryFile(suffix='.db') as f:
            c = Core(f.name); now = int(time.time())
            c.derivative_history._insert('coinglass','liq_long_usd',[(now-3600,100,90,{})])
            st = fin._default_state(c)
            st['sources']={'cg_liq':{'last_success_at':now,'processed_through':now,'range_limited':True,'detail':{'range_limited':True,'rows':1}}}
            fin._save(c,st)
            self.assertTrue(res._range_limited(c,'cg_liq'))
            self.assertFalse(res._full_span(c,'cg_liq',c.START_TS + res.WARMUP_SECONDS))

    def test_full_span_source_freeze_excludes_recent_only_enrichment(self):
        with tempfile.NamedTemporaryFile(suffix='.db') as f:
            c = Core(f.name); now = int(time.time()); base = c.START_TS + res.WARMUP_SECONDS
            c.derivative_history._insert('bybit','oi_coin',[(base-30*3600,100,90,{}),(now-4*3600,120,90,{})])
            c.derivative_history._insert('binance','funding',[(base-24*3600,.0001,90,{}),(now-3600,.0002,90,{})])
            c.derivative_history._insert('coinglass','liq_long_usd',[(now-3600,100,90,{})])
            st=fin._default_state(c)
            st['sources']={
                'bybit_oi':{'last_success_at':now,'processed_through':now,'detail':{'oi_rows':2}},
                'funding_binance':{'last_success_at':now,'processed_through':now,'detail':{'funding_rows':2}},
                'cg_liq':{'last_success_at':now,'processed_through':now,'range_limited':True,'detail':{'range_limited':True,'rows':1}},
            }
            fin._save(c,st)
            frozen=res._freeze_sources(c)
            self.assertTrue(frozen['source_set_frozen'])
            self.assertEqual(frozen['model_oi_sources'],['bybit_oi'])
            self.assertEqual(frozen['model_funding_sources'],['funding_binance'])
            self.assertNotIn('cg_liq',frozen['model_enrichment_sources'])

    def test_resilience_migration_preserves_raw_and_clean_dataset_marker(self):
        with tempfile.NamedTemporaryFile(suffix='.db') as f:
            c=Core(f.name)
            c.insert_bars('gate','ETH','15m',[{'ts':c.START_TS,'o':1,'h':1,'l':1,'c':1,'v':1,'qv':1}])
            c.derivative_history._insert('bybit','oi_coin',[(c.START_TS,100,90,{})])
            con=c.db()
            con.execute("INSERT INTO learning_samples VALUES(?,?,?,?,?,?,?,?,?,?,?)",(c.START_TS+900,'S','LONG','R','P','{}',1,1,1,0,100))
            con.commit(); con.close()
            marker={'baseline_id':v12_clean_baseline.BASELINE_ID,'dataset_id':'clean-id','clean':True,'status':'CLEAN'}
            c.set_state(v12_clean_baseline.STATE_KEY,marker)
            c.set_state(v5_runtime.REPLAY_STATE_KEY,c.START_TS+999999)
            res._ensure_migration(c)
            con=c.db()
            self.assertEqual(con.execute('SELECT COUNT(*) FROM market_bars').fetchone()[0],1)
            self.assertEqual(con.execute('SELECT COUNT(*) FROM derivative_history').fetchone()[0],1)
            self.assertEqual(con.execute('SELECT COUNT(*) FROM learning_samples').fetchone()[0],0)
            con.close()
            self.assertEqual(c.get_state(v12_clean_baseline.STATE_KEY),marker)
            self.assertEqual(c.get_state(v5_runtime.REPLAY_STATE_KEY),c.START_TS)


class GapRepairTests(unittest.IsolatedAsyncioTestCase):
    async def test_gap_repair_uses_real_fallback_candle(self):
        with tempfile.NamedTemporaryFile(suffix='.db') as f:
            c=Core(f.name); ts=c.START_TS+123*300; c.hub=Hub(exact_source='bybit',exact_ts=ts)
            rec=await res.repair_gap(c,{'asset':'ETH','tf':'5m','missing_ts':ts})
            self.assertEqual(rec['status'],'REPAIRED')
            rows=res.canonical_bars(c,'ETH','5m')
            self.assertEqual(len(rows),1)
            self.assertEqual(rows[0]['_source'],'bybit')
            self.assertEqual(rows[0]['ts'],ts)

    async def test_unavailable_gap_is_quarantined_only_after_settled_rounds(self):
        with tempfile.NamedTemporaryFile(suffix='.db') as f:
            c=Core(f.name); ts=c.START_TS+321*300; c.hub=Hub()
            target={'asset':'ETH','tf':'5m','missing_ts':ts}
            first=await res.repair_gap(c,target)
            self.assertEqual(first['status'],'PENDING_REPAIR')
            second=await res.repair_gap(c,target)
            self.assertEqual(second['status'],'QUARANTINED_UNRECOVERABLE')
            con=c.db()
            self.assertEqual(con.execute('SELECT COUNT(*) FROM market_bars').fetchone()[0],0)
            con.close()

    async def test_transient_provider_error_cannot_quarantine_gap(self):
        with tempfile.NamedTemporaryFile(suffix='.db') as f:
            c=Core(f.name); ts=c.START_TS+555*300; c.hub=Hub(transient_source='gate')
            target={'asset':'ETH','tf':'5m','missing_ts':ts}
            for _ in range(3):
                rec=await res.repair_gap(c,target)
            self.assertEqual(rec['status'],'PENDING_REPAIR')
            self.assertEqual(rec['settled_rounds'],0)


if __name__ == '__main__':
    unittest.main()
