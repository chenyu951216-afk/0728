import json
import os
import sqlite3
import tempfile
import time
import unittest

import v7_trade_monitor as tm


class Core:
    POST_EXIT_BARS=96
    def __init__(self):
        fd,self.path=tempfile.mkstemp(suffix='.db'); os.close(fd)
        con=self.db(); con.execute('''CREATE TABLE signals(signal_id TEXT PRIMARY KEY,created_at INTEGER,updated_at INTEGER,status TEXT,strategy TEXT,direction TEXT,regime TEXT,phase TEXT,probability REAL,entry REAL,initial_stop REAL,current_stop REAL,targets TEXT,payload TEXT,filled_at INTEGER,exit_ts INTEGER,exit_price REAL,exit_reason TEXT,realized_r REAL,review_until INTEGER,post_mfe_r REAL DEFAULT 0,post_mae_r REAL DEFAULT 0,review_label TEXT)'''); con.commit(); con.close()
    def db(self):
        con=sqlite3.connect(self.path); con.row_factory=sqlite3.Row; return con
    def latest_signal(self,statuses=('PLANNED','OPEN')):
        con=self.db(); ph=','.join('?' for _ in statuses); r=con.execute(f'SELECT * FROM signals WHERE status IN ({ph}) ORDER BY created_at DESC LIMIT 1',statuses).fetchone(); con.close()
        if not r:return None
        x=dict(r); x['targets']=json.loads(x['targets']); x['payload']=json.loads(x['payload']); return x
    def close(self):
        try:os.remove(self.path)
        except OSError:pass


class TradeMonitorTests(unittest.TestCase):
    def setUp(self):self.c=Core()
    def tearDown(self):self.c.close()
    def _insert(self,status='PLANNED',created=None):
        created=created or int(time.time())-10
        payload={'execution_policy':{'expire_bars':6,'all_in_cost_bps':8,'lock_after_tp2_r':.55,'lock_after_tp3_r':1.05},'management':{'hit_targets':[],'remaining_fraction':1.0,'realized_partial_r':0.0}}
        targets=[{'price':101,'rr':1,'allocation':20},{'price':102,'rr':2,'allocation':30},{'price':103,'rr':3,'allocation':30},{'price':104,'rr':4,'allocation':20}]
        con=self.c.db(); con.execute('INSERT INTO signals(signal_id,created_at,updated_at,status,strategy,direction,regime,phase,probability,entry,initial_stop,current_stop,targets,payload,filled_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',('s',created,created,status,'MOMENTUM_CONTINUATION','LONG','BULL_MARKUP','COMPRESSION',.7,100,99,99,json.dumps(targets),json.dumps(payload),created if status=='OPEN' else None)); con.commit();con.close()
    def test_long_limit_fills_when_trade_is_below_entry(self):
        self._insert('PLANNED'); t=time.time(); tm.process_trade_event(self.c,{'kind':'trade','trade_id':1,'time':t,'price':99.5,'source':'test'}); self.assertEqual(self.c.latest_signal(('OPEN',))['status'],'OPEN')
    def test_ordered_trades_change_management_in_sequence(self):
        self._insert('OPEN'); t=time.time(); tm.process_trade_event(self.c,{'kind':'trade','trade_id':1,'time':t,'price':101.1,'source':'test'}); r=self.c.latest_signal(('OPEN',)); self.assertEqual(r['current_stop'],100); self.assertEqual(r['payload']['management']['hit_targets'],[0]); tm.process_trade_event(self.c,{'kind':'trade','trade_id':2,'time':t+.1,'price':99.8,'source':'test'}); con=self.c.db(); row=dict(con.execute("SELECT * FROM signals WHERE signal_id='s'").fetchone()); con.close(); self.assertEqual(row['status'],'CLOSED'); self.assertAlmostEqual(row['exit_price'],100)
    def test_duplicate_trade_is_ignored(self):
        self._insert('OPEN'); t=time.time(); e={'kind':'trade','trade_id':9,'time':t,'price':101.1,'source':'test'}; tm.process_trade_event(self.c,e); first=self.c.latest_signal(('OPEN',))['payload']['management']['realized_partial_r']; tm.process_trade_event(self.c,e); second=self.c.latest_signal(('OPEN',))['payload']['management']['realized_partial_r']; self.assertEqual(first,second)

if __name__=='__main__':unittest.main()
