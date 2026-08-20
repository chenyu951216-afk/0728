import json
import sqlite3

import v60_rejected_strategy_diagnostics as v60


class Auto:
    MIN_OOS_FILLS = 80
    MIN_OOS_PF = 1.30
    MIN_OOS_EV_R = 0.10
    MAX_OOS_DD_R = 10.0
    MIN_BOOTSTRAP_CI05 = 0.0
    MIN_WF_STABILITY = 0.65
    MIN_PROFITABLE_FOLDS = 0.66
    MIN_WORST_FOLD_EV = -0.05


class Core:
    def __init__(self, path):
        self.path = str(path)
        self.state = {'v49_stage6_atomic_orchestration': {'run_id': 'RUN1'}}

    def db(self):
        return sqlite3.connect(self.path)


class Pipe:
    VAULT_TABLE = 'vault'

    @staticmethod
    def _ensure(core):
        con = core.db()
        try:
            con.execute('''CREATE TABLE IF NOT EXISTS vault(
                run_id TEXT, genome_hash TEXT, candidate_id TEXT, finalist_id TEXT,
                strategy_id TEXT, created_at INTEGER, updated_at INTEGER, rank INTEGER,
                direction TEXT, status TEXT, selected_finalist INTEGER, active_champion INTEGER,
                genome TEXT, development TEXT, audit TEXT, model BLOB)''')
            con.commit()
        finally:
            con.close()


def test_rejected_finalist_reports_exact_failed_gates(tmp_path):
    core = Core(tmp_path / 'v60.db')
    Pipe._ensure(core)
    genome = {'direction': 'LONG', 'entry_market': True, 'entry_offset_atr': 0.0,
              'stop_atr': 2.0, 'target_rr': [2.0, 4.0], 'allocations': [.5, .5],
              'max_hold_bars': 32, 'decision_stride': 1, 'cooldown_bars': 2}
    dev = {'development_score': 1.2, 'ev': .2, 'pf': 1.5, 'stability': .70,
           'profitable_folds': .75, 'worst_fold_ev': -.02, 'development_fills': 300,
           'folds': [{}, {}, {}]}
    metrics = {'oos_fills': 92, 'profit_factor': 1.22, 'expectancy_r': .12,
               'test_win': .55, 'max_drawdown_r': 8.0, 'total_oos_r': 11.0,
               'bootstrap_ci05_r': -.01, 'invalid_future_paths': 0,
               'stability': .70, 'profitable_folds': .75, 'worst_fold_ev': -.02,
               'development_ev': .2, 'development_pf': 1.5,
               'reason': 'OOS rejected'}
    audit = {'status': 'REJECTED_AUTONOMOUS_OOS', 'promoted': False,
             'metrics': metrics, 'gate_thresholds': []}
    con = core.db()
    try:
        con.execute('''INSERT INTO vault VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
                    ('RUN1','g1','c1','f1','',1,2,1,'LONG','OOS_AUDITED',1,0,
                     json.dumps(genome),json.dumps(dev),json.dumps(audit),None))
        con.commit()
    finally:
        con.close()

    payload = v60._build_payload(core, Auto, Pipe)
    assert payload['summary']['finalists'] == 1
    assert payload['summary']['rejected'] == 1
    row = payload['rejected'][0]
    assert row['oos']['pf'] == 1.22
    assert row['oos']['ev_r'] == .12
    assert row['failed_gates'] == ['OOS PF', 'Bootstrap CI05']
    assert row['execution']['entry_type'] == 'MARKET'
    assert row['execution']['max_hold_hours'] == 8.0


def test_rules_require_all_certification_gates():
    metrics = {'oos_fills': 80, 'profit_factor': 1.30, 'expectancy_r': .10,
               'max_drawdown_r': 10.0, 'bootstrap_ci05_r': .001,
               'invalid_future_paths': 0, 'stability': .65,
               'profitable_folds': .66, 'worst_fold_ev': -.05}
    rules = v60._rules(metrics, Auto)
    assert len(rules) == 9
    assert all(x['passed'] for x in rules)
