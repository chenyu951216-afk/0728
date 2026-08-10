import math
import sqlite3
import tempfile
import unittest

import v5_runtime
import v10_final_integrity as final
import v13_replay_cursor_integrity as guard


class Core:
    START_TS = 1577836800
    def __init__(self, path):
        self.path = path
        self.saved = {}
        self.state = {}
    def db(self):
        con = sqlite3.connect(self.path)
        con.execute('CREATE TABLE IF NOT EXISTS market_bars(source TEXT,asset TEXT,tf TEXT,ts INTEGER,o REAL,h REAL,l REAL,c REAL,v REAL,qv REAL)')
        con.execute('CREATE TABLE IF NOT EXISTS derivative_history(source TEXT,metric TEXT,ts INTEGER,value REAL,quality REAL,meta TEXT)')
        con.execute('CREATE TABLE IF NOT EXISTS learning_samples(ts INTEGER,strategy TEXT,direction TEXT,regime TEXT,phase TEXT,features TEXT,success INTEGER,pnl_r REAL,mfe_r REAL,mae_r REAL,source_quality REAL)')
        con.execute('CREATE TABLE IF NOT EXISTS learning_feature_snapshots(ts INTEGER PRIMARY KEY,features TEXT)')
        con.execute('CREATE TABLE IF NOT EXISTS model_registry(strategy TEXT,direction TEXT,version INTEGER,status TEXT,created_at INTEGER,metrics TEXT,model BLOB)')
        con.execute('CREATE TABLE IF NOT EXISTS execution_registry_v7(strategy TEXT,direction TEXT,model_version INTEGER,version INTEGER,status TEXT,created_at INTEGER,metrics TEXT,policy TEXT)')
        con.commit(); return con
    def get_state(self, key, default=None):
        return self.saved.get(key, default)
    def set_state(self, key, value):
        self.saved[key] = value


def synthetic_15m(n=260):
    rows = []
    base = 1500.0
    for i in range(n):
        drift = i * 0.18 + math.sin(i / 7.0) * 4.5 + math.sin(i / 23.0) * 9.0
        close = base + drift
        open_ = close - math.sin(i / 5.0) * 1.3
        high = max(open_, close) + 3.2 + (i % 4) * .25
        low = min(open_, close) - 3.0 - (i % 3) * .2
        rows.append({'ts': 1577836800 + i * 900, 'o': open_, 'h': high, 'l': low, 'c': close, 'v': 1000 + i, 'qv': 0})
    return rows


def synthetic_future5(close, start_ts):
    rows = []
    for j in range(96):
        center = close + math.sin(j / 4.0) * 7.0 + j * .04
        rows.append({'ts': start_ts + j * 300, 'o': center - .5, 'h': center + 4.2, 'l': center - 4.0, 'c': center, 'v': 100})
    return rows


class ReplayCursorIntegrityTests(unittest.TestCase):
    def test_unresolved_future_or_price_gap_is_blocking(self):
        self.assertEqual(guard._decision_state(htf_ready=True, future_ready=False, continuity_ready=True), 'BLOCK_FUTURE_PATH')
        self.assertEqual(guard._decision_state(htf_ready=True, future_ready=True, continuity_ready=False), 'BLOCK_PRICE_GAP')
        self.assertEqual(guard._decision_state(htf_ready=True, future_ready=True, continuity_ready=True), 'READY')

    def test_only_explicit_warmup_is_skippable(self):
        self.assertEqual(guard._decision_state(htf_ready=False, future_ready=False, continuity_ready=False), 'WARMUP')

    def test_feature_builder_contract_calls_builder_instead_of_calling_result_dict(self):
        calls = []
        class FeatureCore:
            def build_features(self, m15, h1, btc, regime, extras):
                calls.append((m15, h1, btc, regime, extras))
                return {'ema_gap': 1.0, 'source_agreement_bps': 99.0}
        core = FeatureCore()
        out = guard._build_model_features(core, [{'c': 1}], [{'c': 1}], [{'c': 1}], {'regime': 'R'}, {'funding': 0.0})
        self.assertEqual(len(calls), 1)
        self.assertEqual(out['ema_gap'], 1.0)
        self.assertEqual(out['source_agreement_bps'], 0.0)

    def test_feature_builder_contract_rejects_non_callable_builder_with_clear_error(self):
        class BrokenCore:
            build_features = {'wrong': 'dict'}
        with self.assertRaisesRegex(TypeError, 'must be callable'):
            guard._build_model_features(BrokenCore(), [], [], [], {}, {})

    def test_causal_prefix_ema_and_atr_match_reference(self):
        m15 = synthetic_15m()
        ema20 = guard._ema20_prefix(m15)
        tr = guard._true_ranges(m15)
        for i in (40, 100, 180, 240):
            past = m15[:i + 1]
            self.assertAlmostEqual(ema20[i], final.signal.ema([final.signal.f(x['c']) for x in past], 20), places=12)
            self.assertAlmostEqual(guard._atr14_at(tr, i), final.signal.atr(past), places=12)

    def test_precomputed_5m_outcomes_match_reference_for_every_strategy_direction(self):
        m15 = synthetic_15m()
        ema20 = guard._ema20_prefix(m15)
        tr = guard._true_ranges(m15)
        for i in (120, 180, 220):
            future5 = synthetic_future5(float(m15[i]['c']), int(m15[i]['ts']) + 900)
            atr14 = guard._atr14_at(tr, i)
            for strategy in final.signal.STRATEGIES:
                for direction in final.signal.DIRECTIONS:
                    ref = final.strategy_outcome_5m(m15, i, future5, strategy, direction)
                    fast = guard._strategy_outcome_5m_precomputed(m15, i, future5, strategy, direction, ema20[i], atr14)
                    self.assertEqual(ref[0], fast[0], (strategy, direction, i, ref, fast))
                    for a, b in zip(ref[1:], fast[1:]):
                        self.assertAlmostEqual(a, b, places=12, msg=str((strategy, direction, i, ref, fast)))

    def test_integrity_reset_preserves_raw_cache_and_dataset_marker(self):
        with tempfile.NamedTemporaryFile(suffix='.db') as f:
            core = Core(f.name)
            con = core.db()
            con.execute("INSERT INTO market_bars VALUES('gate','ETH','15m',?,?,?,?,?,?,?)", (core.START_TS,1,1,1,1,1,1))
            con.execute("INSERT INTO derivative_history VALUES('bybit','oi_usd',?,?,?,?)", (core.START_TS,100,90,'{}'))
            con.execute("INSERT INTO learning_samples VALUES(?,?,?,?,?,?,?,?,?,?,?)", (core.START_TS+900,'S','LONG','R','P','{}',1,1,1,0,100))
            con.execute("INSERT INTO learning_feature_snapshots VALUES(?,?)", (core.START_TS+900,'{}'))
            con.commit(); con.close()
            marker = {'baseline_id':'final-clean-baseline-20260809-v1','dataset_id':'abc','clean':True,'status':'CLEAN'}
            core.set_state('final_dataset_baseline_v1', marker)
            core.set_state(v5_runtime.REPLAY_STATE_KEY, core.START_TS + 999999)

            guard._reset_derived_replay(core, 'test')

            con = core.db()
            self.assertEqual(con.execute('SELECT COUNT(*) FROM market_bars').fetchone()[0], 1)
            self.assertEqual(con.execute('SELECT COUNT(*) FROM derivative_history').fetchone()[0], 1)
            self.assertEqual(con.execute('SELECT COUNT(*) FROM learning_samples').fetchone()[0], 0)
            self.assertEqual(con.execute('SELECT COUNT(*) FROM learning_feature_snapshots').fetchone()[0], 0)
            con.close()
            self.assertEqual(core.get_state(v5_runtime.REPLAY_STATE_KEY), core.START_TS)
            self.assertEqual(core.get_state('final_dataset_baseline_v1'), marker)


if __name__ == '__main__':
    unittest.main()
