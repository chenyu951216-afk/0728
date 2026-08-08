import json
import os
import sqlite3
import tempfile
import unittest

import v7_live_health as health
import v7_monitor_gate as monitor_gate


class GateCore:
    def __init__(self, ready=False):
        self.state={'risk_feed_probe':{'gate_trades_ok':ready,'coverage_complete':ready}}
        self.choose_strategy=lambda store,learner,features,regime,data_quality:{'strategy':'TEST','direction':'LONG','tradeable':True,'certified':True,'score':.8,'candidates':[{'strategy':'TEST','direction':'LONG','tradeable':True,'certified':True,'score':.8}]}


class HealthCore:
    def __init__(self):
        fd,self.path=tempfile.mkstemp(suffix='.db');os.close(fd);self.states={}
        con=self.db();con.execute('CREATE TABLE live_execution_samples(signal_id TEXT PRIMARY KEY,ts INTEGER,strategy TEXT,direction TEXT,regime TEXT,model_version INTEGER,execution_version INTEGER,probability REAL,realized_r REAL,mfe_r REAL,mae_r REAL,review_label TEXT,payload TEXT)');con.commit();con.close()
    def db(self):
        con=sqlite3.connect(self.path);con.row_factory=sqlite3.Row;return con
    def get_state(self,k,d=None):return self.states.get(k,d)
    def set_state(self,k,v):self.states[k]=v
    def close(self):
        try:os.remove(self.path)
        except OSError:pass


class SafetyTests(unittest.TestCase):
    def test_monitor_gate_fails_closed(self):
        core=GateCore(False);monitor_gate.install(core);out=core.choose_strategy(None,None,{}, {},100)
        self.assertFalse(out['tradeable']);self.assertFalse(out['risk_feed_ready']);self.assertEqual(out['tradeable_candidates'],[])

    def test_monitor_gate_allows_when_complete(self):
        core=GateCore(True);monitor_gate.install(core);out=core.choose_strategy(None,None,{}, {},100)
        self.assertTrue(out['tradeable']);self.assertTrue(out['risk_feed_ready'])

    def test_live_health_quarantines_clear_three_loss_failure(self):
        core=HealthCore()
        try:
            con=core.db()
            for i,p in enumerate((-0.9,-0.8,-1.0)):
                con.execute('INSERT INTO live_execution_samples VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)',(f's{i}',i,'TEST','LONG','BULL_MARKUP',3,2,.7,p,0,0,None,'{}'))
            con.commit();con.close()
            candidate={'strategy':'TEST','direction':'LONG','model':{'model_version':3},'execution':{'metrics':{'execution_version':2}}}
            out=health._health(core,candidate)
            self.assertTrue(out['blocked']);self.assertTrue(out['trigger']['emergency_3_loss'])
        finally:core.close()


if __name__=='__main__':unittest.main()
