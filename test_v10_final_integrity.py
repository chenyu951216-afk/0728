import sqlite3
import tempfile
import time
import unittest

import derivative_data
import v10_final_integrity as fin
import v10_source_freeze as freeze


class Core:
    START_TS=1577836800
    TIMEFRAME_SECONDS={'5m':300,'15m':900,'30m':1800,'1h':3600,'4h':14400,'1d':86400}
    def __init__(self,path):
        self.path=path; self.saved={}; self.state={}; self.POST_EXIT_BARS=96
        self.derivative_history=derivative_data.DerivativeHistory(path)
    def db(self):
        con=sqlite3.connect(self.path); con.row_factory=sqlite3.Row
        con.execute('CREATE TABLE IF NOT EXISTS system_state(key TEXT PRIMARY KEY,value TEXT,updated_at INTEGER)')
        con.execute('CREATE TABLE IF NOT EXISTS market_bars(source TEXT,asset TEXT,tf TEXT,ts INTEGER,o REAL,h REAL,l REAL,c REAL,v REAL,qv REAL,PRIMARY KEY(source,asset,tf,ts))')
        con.execute('CREATE TABLE IF NOT EXISTS learning_samples(ts INTEGER,strategy TEXT,direction TEXT,regime TEXT,phase TEXT,features TEXT,success INTEGER,pnl_r REAL,mfe_r REAL,mae_r REAL,source_quality REAL,PRIMARY KEY(ts,strategy,direction))')
        con.execute('CREATE TABLE IF NOT EXISTS learning_feature_snapshots(ts INTEGER PRIMARY KEY,features TEXT)')
        con.execute('CREATE TABLE IF NOT EXISTS model_registry(strategy TEXT,version INTEGER,status TEXT,created_at INTEGER,metrics TEXT,model BLOB,direction TEXT,PRIMARY KEY(strategy,version))')
        con.execute('CREATE TABLE IF NOT EXISTS execution_registry_v7(strategy TEXT,direction TEXT,model_version INTEGER,version INTEGER,status TEXT,created_at INTEGER,metrics TEXT,policy TEXT,PRIMARY KEY(strategy,direction,model_version,version))')
        con.commit(); return con
    def get_state(self,key,default=None): return self.saved.get(key,default)
    def set_state(self,key,value): self.saved[key]=value
    def insert_bars(self,source,asset,tf,rows):
        con=self.db(); before=con.total_changes
        con.executemany('INSERT OR IGNORE INTO market_bars VALUES(?,?,?,?,?,?,?,?,?,?)',[(source,asset,tf,int(x['ts']),float(x['o']),float(x['h']),float(x['l']),float(x['c']),float(x.get('v',0)),float(x.get('qv',0))) for x in rows])
        n=con.total_changes-before; con.commit(); con.close(); return n


class FinalIntegrityTests(unittest.TestCase):
    def test_optional_source_cannot_deadlock_frozen_core(self):
        with tempfile.NamedTemporaryFile(suffix='.db') as f:
            c=Core(f.name); now=int(time.time())
            state=fin._default_state(c); state['core_frozen']=True; state['frozen_core_oi']=['bybit_oi']; state['frozen_core_funding']=['funding_binance']; state['frozen_enrichment']=[]
            state['sources']={'bybit_oi':{'last_success_at':now,'processed_through':now,'detail':{'oi_rows':100}},'funding_binance':{'last_success_at':now,'processed_through':now-60,'detail':{'funding_rows':100}},'cg_book':{'processed_through':c.START_TS}}
            fin._save(c,state)
            self.assertGreater(freeze.core_ready_through(c),c.START_TS)
            self.assertEqual(freeze.core_ready_through(c),now-60)

    def test_freeze_requires_complete_oi_and_funding_with_real_rows(self):
        with tempfile.NamedTemporaryFile(suffix='.db') as f:
            c=Core(f.name); now=int(time.time())
            state=fin._default_state(c); state['sources']={'bybit_oi':{'last_success_at':now,'processed_through':now,'detail':{'oi_rows':200}},'funding_binance':{'last_success_at':now,'processed_through':c.START_TS,'detail':{'funding_rows':200}}}
            fin._save(c,state); freeze._freeze_if_ready(c); self.assertFalse(fin._load(c).get('core_frozen',False))
            state=fin._load(c); state['sources']['funding_binance']['processed_through']=now; fin._save(c,state); freeze._freeze_if_ready(c)
            frozen=fin._load(c); self.assertTrue(frozen.get('core_frozen')); self.assertIn('bybit_oi',frozen['frozen_core_oi']); self.assertIn('funding_binance',frozen['frozen_core_funding'])

    def test_zero_row_completed_source_cannot_certify_core_while_real_source_is_pending(self):
        with tempfile.NamedTemporaryFile(suffix='.db') as f:
            c=Core(f.name); now=int(time.time())
            state=fin._default_state(c); state['sources']={
                'cg_oi':{'last_success_at':now,'processed_through':now,'detail':{'rows':0}},
                'bybit_oi':{'last_success_at':now,'processed_through':c.START_TS,'detail':{'oi_rows':5}},
                'funding_binance':{'last_success_at':now,'processed_through':now,'detail':{'funding_rows':100}},
            }
            fin._save(c,state); freeze._freeze_if_ready(c)
            self.assertFalse(fin._load(c).get('core_frozen',False))

    def test_source_choice_does_not_rank_by_future_row_count(self):
        with tempfile.NamedTemporaryFile(suffix='.db') as f:
            c=Core(f.name); con=c.db()
            con.execute('INSERT INTO market_bars VALUES(?,?,?,?,?,?,?,?,?,?)',('gate','ETH','15m',c.START_TS,1,1,1,1,1,1))
            for i in range(100): con.execute('INSERT INTO market_bars VALUES(?,?,?,?,?,?,?,?,?,?)',('bybit','ETH','15m',c.START_TS+i*900,1,1,1,1,1,1))
            con.commit(); con.close(); self.assertEqual(fin.deterministic_best_source(c,'ETH','15m'),'gate')

    def test_spot_cannot_pollute_futures_market_bars(self):
        with tempfile.NamedTemporaryFile(suffix='.db') as f:
            c=Core(f.name); ts=c.START_TS
            bundle={'eth_1d':[],'eth_4h':[],'eth_1h':[],'eth_30m':[],'eth_15m':[{'source':'gate','ts':ts,'o':100,'h':101,'l':99,'c':100,'v':1,'qv':1}],
                    'eth_5m':[],'btc_1h':[],'eth_spot_15m':[{'source':'gate','ts':ts,'o':200,'h':201,'l':199,'c':200,'v':1,'qv':1}],'validators':{}}
            fin._upsert_live_without_spot_contamination(c,bundle); con=c.db()
            fut=con.execute("SELECT c FROM market_bars WHERE asset='ETH' AND tf='15m' AND source='gate'").fetchone()[0]
            spot=con.execute("SELECT c FROM market_bars WHERE asset='ETH_SPOT' AND tf='15m' AND source='gate_spot'").fetchone()[0]; con.close()
            self.assertEqual(fut,100); self.assertEqual(spot,200)

    def test_fill_bar_target_is_not_credited(self):
        bars=[]
        for i in range(120): bars.append({'ts':i*900,'o':100,'h':100,'l':100,'c':100,'v':1})
        future=[{'ts':108000,'o':100,'h':110,'l':100,'c':100},{'ts':108300,'o':100,'h':100,'l':100,'c':100}]
        filled,pnl,_,_=fin._one_outcome_5m(bars,119,future,'SQUEEZE_EXPANSION','LONG',0.0,1.0,1.5)
        self.assertTrue(filled); self.assertLess(pnl,1.5); self.assertAlmostEqual(pnl,0.0,places=6)

    def test_schema_reset_preserves_raw_market_and_derivatives(self):
        with tempfile.NamedTemporaryFile(suffix='.db') as f:
            c=Core(f.name); con=c.db(); con.execute('INSERT INTO market_bars VALUES(?,?,?,?,?,?,?,?,?,?)',('gate','ETH','15m',c.START_TS,1,1,1,1,1,1)); con.execute("INSERT INTO learning_samples VALUES(?,?,?,?,?,?,?,?,?,?,?)",(c.START_TS,'S','LONG','R','P','{}',1,1,1,0,100)); con.commit(); con.close()
            c.derivative_history.ensure_schema(); c.derivative_history._insert('bybit','funding',[(c.START_TS,0.001,80,{})])
            c.set_state('point_in_time_sample_schema',5); fin._migrate(c); con=c.db()
            self.assertEqual(con.execute('SELECT COUNT(*) FROM market_bars').fetchone()[0],1); self.assertEqual(con.execute('SELECT COUNT(*) FROM learning_samples').fetchone()[0],0); con.close()
            self.assertEqual(fin._row_count(c.derivative_history,'bybit','funding'),1); self.assertEqual(c.get_state('point_in_time_sample_schema'),fin.SAMPLE_SCHEMA)


if __name__=='__main__': unittest.main()
