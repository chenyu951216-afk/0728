import json
import os
import sqlite3
import tempfile
import time
import unittest

import execution_v7 as ex
import v7_runtime as rt


class FakeCore:
    POST_EXIT_BARS = 96
    def __init__(self):
        fd, self.path = tempfile.mkstemp(suffix='.db'); os.close(fd)
        con = self.db()
        con.execute('''CREATE TABLE signals(
            signal_id TEXT PRIMARY KEY, created_at INTEGER, updated_at INTEGER, status TEXT,
            strategy TEXT, direction TEXT, regime TEXT, phase TEXT, probability REAL,
            entry REAL, initial_stop REAL, current_stop REAL, targets TEXT, payload TEXT,
            filled_at INTEGER, exit_ts INTEGER, exit_price REAL, exit_reason TEXT,
            realized_r REAL, review_until INTEGER, post_mfe_r REAL DEFAULT 0,
            post_mae_r REAL DEFAULT 0, review_label TEXT
        )''')
        con.commit(); con.close()
    def db(self):
        con = sqlite3.connect(self.path); con.row_factory = sqlite3.Row; return con
    def latest_signal(self, statuses=('PLANNED','OPEN')):
        con=self.db(); ph=','.join('?' for _ in statuses); row=con.execute(f'SELECT * FROM signals WHERE status IN ({ph}) ORDER BY created_at DESC LIMIT 1', statuses).fetchone(); con.close()
        if not row:return None
        x=dict(row); x['targets']=json.loads(x['targets']); x['payload']=json.loads(x['payload']); return x
    def close(self):
        try: os.remove(self.path)
        except OSError: pass


def bars(n=180, start=1900.0, step=0.2):
    out=[]; p=start
    for i in range(n):
        p += step; wig = 2.0 if i % 7 == 0 else 0.7
        out.append({'ts':1700000000+i*900,'o':p-.1,'h':p+wig,'l':p-wig,'c':p,'v':1000+i})
    return out


class ExecutionV7Tests(unittest.TestCase):
    def test_grid_is_not_scalper_only(self):
        grid=ex.policy_candidates('MOMENTUM_CONTINUATION')
        self.assertIn(2.20,{x['stop_atr'] for x in grid})
        self.assertEqual({'15m','30m','1h','balanced'},{x['structure_mode'] for x in grid})
        self.assertGreaterEqual(len(grid),250)

    def test_multitimeframe_plan_keeps_minimum_stop(self):
        m15=bars(); m30=bars(160,1880,.35); h1=bars(150,1850,.55)
        policy={'entry_atr':.04,'stop_atr':1.5,'structure_mode':'1h','target_rr':[1,1.5,2.2,3.2],
                'allocations':[20,30,30,20],'min_stop_pct':.002,'all_in_cost_bps':8,
                'lock_after_tp2_r':.55,'lock_after_tp3_r':1.05,'expire_bars':6,'max_hold_bars':32}
        plan=ex.plan_from_policy('MOMENTUM_CONTINUATION','LONG',m15[-1]['c'],m15,policy,m30,h1)
        self.assertGreaterEqual(abs(plan['entry']-plan['stop'])/plan['entry'],.00199)
        self.assertIn(plan['profile']['structure_used'],('1h','none'))
        self.assertEqual(sum(x['allocation'] for x in plan['targets']),100)

    def test_bootstrap_interval_orders_bounds(self):
        # A confidence interval is allowed to degenerate when every sampled block
        # has exactly the same mean. The invariant is lower <= upper, not strict <.
        low,high=ex._block_bootstrap_ev([.2,-.1,.3,.1,-.05,.2,.15,.1]*10)
        self.assertLessEqual(low,high)


class RuntimeV7Tests(unittest.TestCase):
    def setUp(self): self.core=FakeCore()
    def tearDown(self): self.core.close()

    def _insert_planned(self, created=1000):
        con=self.core.db(); con.execute(
            'INSERT INTO signals(signal_id,created_at,updated_at,status,strategy,direction,regime,phase,probability,entry,initial_stop,current_stop,targets,payload) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
            ('s1',created,created,'PLANNED','MOMENTUM_CONTINUATION','LONG','BULL_MARKUP','COMPRESSION',.7,100,99,99,
             json.dumps([{'price':101,'rr':1,'allocation':20},{'price':102,'rr':2,'allocation':30},{'price':103,'rr':3,'allocation':30},{'price':104,'rr':4,'allocation':20}]),
             json.dumps({'execution_policy':{'expire_bars':6,'all_in_cost_bps':8},'management':{'hit_targets':[],'remaining_fraction':1.0,'realized_partial_r':0.0}})))
        con.commit();con.close()

    def test_pre_signal_candle_cannot_retroactively_fill(self):
        self._insert_planned(1000)
        rt.update_signal_with_event_v7(self.core,{'start_ts':990,'end_ts':1000,'low':98,'high':102,'last':101,'observed_at':1001,'source':'test'})
        self.assertEqual(self.core.latest_signal()['status'],'PLANNED')
        rt.update_signal_with_event_v7(self.core,{'start_ts':995,'end_ts':1005,'low':98,'high':102,'last':101,'observed_at':1005,'source':'test'})
        self.assertEqual(self.core.latest_signal()['status'],'PLANNED')

    def test_recent_losing_stop_blocks_immediate_reentry(self):
        now=int(time.time()); con=self.core.db(); con.execute(
            'INSERT INTO signals(signal_id,created_at,updated_at,status,strategy,direction,regime,phase,probability,entry,initial_stop,current_stop,targets,payload,exit_ts,exit_reason,realized_r) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
            ('loss',now-1000,now-60,'CLOSED','MOMENTUM_CONTINUATION','LONG','BULL_MARKUP','COMPRESSION',.7,1915,1911,1911,'[]','{}',now-60,'STOP_OR_TRAIL',-1.0))
        con.commit();con.close()
        analysis={'selection':{'direction':'LONG','strategy':'MOMENTUM_CONTINUATION'},'features':{},'regime':{'regime':'BULL_MARKUP'},'price':1914}
        gate=rt.reentry_gate(self.core,analysis,bars())
        self.assertFalse(gate['allowed'])
        self.assertIn('cooldown',gate['reason'])


if __name__=='__main__': unittest.main()
