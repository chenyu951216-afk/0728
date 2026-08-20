from __future__ import annotations

import json
import sqlite3

import v64_current_paper_ux_authority as v64


def test_gate_failure_is_clear_chinese_with_original_feature_code():
    out = v64.gate_failures_zh({'wick_ratio': .70}, [
        {'feature': 'wick_ratio', 'op': 'LE', 'value': .49},
    ])
    assert len(out) == 1
    assert '影線比例' in out[0]
    assert 'wick_ratio' in out[0]
    assert '策略要求 ≤ 0.49' in out[0]


def test_v64_card_is_inserted_before_existing_v63_card_and_hides_old_duplicate():
    html = '<html><body><main><section id="v63-top-authority">old</section><h1>Later</h1></main></body></html>'
    out = v64._inject(html)
    assert out.index('v64-top-overview') < out.index('v63-top-authority')
    assert '#v63-top-authority{display:none!important}' in out
    assert '/api/v64/overview' in out
    assert '已完成策略 / 目前訊號與持倉' in out


class Core:
    def __init__(self, path: str):
        self.path = path

    def db(self):
        con = sqlite3.connect(self.path)
        con.row_factory = sqlite3.Row
        return con


def test_unfilled_opposite_replacement_is_cancel_not_fake_realised_exit(tmp_path):
    path = str(tmp_path / 'x.db')
    core = Core(path)
    con = core.db()
    con.execute('''CREATE TABLE signals(
        signal_id TEXT PRIMARY KEY, status TEXT, updated_at INTEGER, exit_ts INTEGER,
        exit_reason TEXT, realized_r REAL, targets TEXT, payload TEXT
    )''')
    con.execute('''INSERT INTO signals(signal_id,status,updated_at,targets,payload)
                   VALUES('S1','PLANNED',1,'[]','{}')''')
    con.commit(); con.close()

    row = {'signal_id': 'S1', 'status': 'PLANNED', 'payload': {}}
    v64._cancel_planned(core, row, 'AUTONOMOUS_OPPOSITE_STRATEGY_REPLACED')
    con = core.db(); got = con.execute('SELECT status,exit_reason,realized_r,payload FROM signals WHERE signal_id="S1"').fetchone(); con.close()
    assert got['status'] == 'CANCELLED'
    assert got['exit_reason'] == 'AUTONOMOUS_OPPOSITE_STRATEGY_REPLACED'
    assert got['realized_r'] is None
    assert json.loads(got['payload'])['cancel_reason'] == 'AUTONOMOUS_OPPOSITE_STRATEGY_REPLACED'


def test_reason_translation_covers_live_wait_words():
    s = v64.reason_zh('Pred EV +0.10R < Required +0.20R；OOD 40%')
    assert '預測期望值' in s
    assert '最低要求' in s
    assert '目前市場偏離歷史分布' in s
