import os
import sqlite3
import tempfile
import unittest

import adaptive_v5 as signal
import execution_v7 as execution
import v9_final as final


class StrictReplayTests(unittest.TestCase):
    def test_closed_slice_excludes_unfinished_htf_bar(self):
        rows = [
            {'ts': 0, 'c': 1},
            {'ts': 3600, 'c': 2},
            {'ts': 7200, 'c': 3},
        ]
        # At 01:15 only the 00:00-01:00 candle is knowable.
        got = final._closed_slice(rows, 3600, 4500, 10)
        self.assertEqual([x['ts'] for x in got], [0])
        # At 02:00 the 01:00-02:00 candle becomes eligible.
        got = final._closed_slice(rows, 3600, 7200, 10)
        self.assertEqual([x['ts'] for x in got], [0, 3600])

    def test_reference_label_does_not_credit_fill_bar_target(self):
        # Build stable history so ATR is deterministic enough for a huge fill candle.
        bars = []
        for i in range(120):
            px = 100.0
            bars.append({'ts': i * 900, 'o': px, 'h': 100.2, 'l': 99.8, 'c': px, 'v': 1})
        i = 100
        # First future candle touches entry and far above any target, but that high
        # may have happened BEFORE the entry. A strict replay may not credit it.
        bars[i + 1] = {'ts': (i + 1) * 900, 'o': 100.0, 'h': 110.0, 'l': 99.0, 'c': 100.0, 'v': 1}
        # Later candles never reach a target and finish around entry.
        for j in range(i + 2, i + 20):
            bars[j] = {'ts': j * 900, 'o': 100.0, 'h': 100.1, 'l': 99.9, 'c': 100.0, 'v': 1}
        filled, pnl, _, _ = final._one_reference_outcome(
            bars, i, 'MOMENTUM_CONTINUATION', 'LONG', 1.0, 1.4, 1.7, 18
        )
        self.assertTrue(filled)
        self.assertLess(pnl, 1.7, 'fill-bar high must not be credited as a target win')

    def test_execution_future_path_starts_after_decision_close(self):
        # Decision bar opens at t=100*900 and closes 900 seconds later.
        opp_ts = 100 * 900
        m15 = []
        for i in range(150):
            m15.append({'ts': i * 900, 'o': 100, 'h': 100.2, 'l': 99.8, 'c': 100, 'v': 1})
        m30 = []
        for i in range(90):
            m30.append({'ts': i * 1800, 'o': 100, 'h': 100.2, 'l': 99.8, 'c': 100, 'v': 1})
        h1 = []
        for i in range(60):
            h1.append({'ts': i * 3600, 'o': 100, 'h': 100.2, 'l': 99.8, 'c': 100, 'v': 1})
        m5 = []
        for i in range(500):
            m5.append({'ts': i * 300, 'o': 100, 'h': 100.2, 'l': 99.8, 'c': 100, 'v': 1})
        data = {
            'm15': m15, 'index15': {x['ts']: i for i, x in enumerate(m15)},
            'm30': m30, 'ts30': [x['ts'] for x in m30],
            'h1': h1, 'ts1h': [x['ts'] for x in h1],
            'm5': m5, 'ts5': [x['ts'] for x in m5],
        }
        policy = {
            'entry_atr': .04, 'stop_atr': 1.4, 'structure_mode': 'balanced',
            'target_rr': [.8, 1.4, 2.0, 3.0], 'allocations': [20, 30, 30, 20],
            'lock_after_tp2_r': .55, 'lock_after_tp3_r': 1.05,
            'expire_bars': 6, 'max_hold_bars': 20, 'all_in_cost_bps': 8,
            'min_stop_pct': .002,
        }
        result = final.strict_simulate_policy(data, {'ts': opp_ts, 'regime': 'TEST'}, 'MOMENTUM_CONTINUATION', 'LONG', policy)
        if not result.get('invalid_data'):
            self.assertEqual(result.get('decision_close_ts'), opp_ts + 900)
            self.assertTrue(result.get('strict_replay'))

    def test_policy_mutation_is_deterministic_for_seed(self):
        parent = {
            'entry_atr': .06, 'stop_atr': 1.4, 'structure_mode': 'balanced',
            'target_rr': [.8, 1.4, 2.0, 3.0], 'allocations': [20, 30, 30, 20],
            'lock_after_tp2_r': .55, 'lock_after_tp3_r': 1.05,
            'expire_bars': 6, 'max_hold_bars': 32,
        }
        import random
        a = final._mutate_policy(parent, random.Random(123), 2)
        b = final._mutate_policy(parent, random.Random(123), 2)
        self.assertEqual(final._policy_key(a), final._policy_key(b))
        self.assertEqual(a['search_origin'], 'STRICT_DEV_EVOLUTION_GEN_2')

    def test_expanded_genomes_keep_existing_and_add_regularized_choices(self):
        ids = {g['id'] for g in final._expanded_genomes()}
        self.assertIn('balanced_all_730d', ids)
        self.assertIn('conservative_all_1460d', ids)
        self.assertGreaterEqual(len(ids), 9)


class MigrationSafetyTests(unittest.TestCase):
    def test_migration_preserves_market_bars(self):
        class Core:
            START_TS = 1577836800
            def __init__(self, path):
                self.path = path; self._state = {}
            def db(self):
                con = sqlite3.connect(self.path)
                con.row_factory = sqlite3.Row
                con.execute('CREATE TABLE IF NOT EXISTS system_state(key TEXT PRIMARY KEY,value TEXT,updated_at INTEGER)')
                con.execute('CREATE TABLE IF NOT EXISTS market_bars(source TEXT,asset TEXT,tf TEXT,ts INTEGER,o REAL,h REAL,l REAL,c REAL,v REAL,qv REAL,PRIMARY KEY(source,asset,tf,ts))')
                con.execute('CREATE TABLE IF NOT EXISTS signals(signal_id TEXT PRIMARY KEY,status TEXT,payload TEXT,updated_at INTEGER)')
                signal.ModelStore(con)
                con.commit(); return con
            def get_state(self, key, default=None): return self._state.get(key, default)
            def set_state(self, key, value): self._state[key] = value
        with tempfile.TemporaryDirectory() as td:
            path = os.path.join(td, 'x.db'); core = Core(path)
            con = core.db()
            con.execute("INSERT INTO market_bars VALUES('gate','ETH','15m',1,1,1,1,1,1,1)")
            con.commit(); con.close()
            final._migrate(core)
            con = core.db()
            bars = con.execute('SELECT COUNT(*) FROM market_bars').fetchone()[0]
            samples = con.execute('SELECT COUNT(*) FROM learning_samples').fetchone()[0]
            con.close()
            self.assertEqual(bars, 1)
            self.assertEqual(samples, 0)
            self.assertEqual(core.get_state('point_in_time_sample_schema'), final.STRICT_SCHEMA)


if __name__ == '__main__':
    unittest.main()
