import json
import time
import unittest

import v7_timeout_guard as guard
from test_v7_trade_monitor import Core


class TimeoutGuardTests(unittest.TestCase):
    def setUp(self): self.c=Core()
    def tearDown(self): self.c.close()

    def test_open_trade_times_out_from_signal_decision(self):
        created=int(time.time())-3600
        payload={'execution_policy':{'max_hold_bars':2,'all_in_cost_bps':8},'management':{'hit_targets':[],'remaining_fraction':1.0,'realized_partial_r':0.0}}
        targets=[{'price':101,'rr':1,'allocation':20},{'price':102,'rr':2,'allocation':30},{'price':103,'rr':3,'allocation':30},{'price':104,'rr':4,'allocation':20}]
        con=self.c.db();con.execute('INSERT INTO signals(signal_id,created_at,updated_at,status,strategy,direction,regime,phase,probability,entry,initial_stop,current_stop,targets,payload,filled_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',('timeout',created,created,'OPEN','TEST','LONG','RANGE_LOW_VOL','BALANCE',.7,100,99,99,json.dumps(targets),json.dumps(payload),created));con.commit();con.close()
        guard.process_with_timeout(self.c,{'kind':'trade','trade_id':1,'time':created+2*900+1,'price':100.5,'source':'test'})
        con=self.c.db();row=dict(con.execute("SELECT * FROM signals WHERE signal_id='timeout'").fetchone());con.close()
        self.assertEqual(row['status'],'CLOSED');self.assertEqual(row['exit_reason'],'TIMEOUT')

if __name__=='__main__':unittest.main()
