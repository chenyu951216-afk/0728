import json
import sqlite3
import unittest

import adaptive_v5 as signal
import v9_training_store as storemod


class TrainingStoreTests(unittest.TestCase):
    def setUp(self):
        self.con = sqlite3.connect(':memory:')
        self.con.row_factory = sqlite3.Row
        self.store = signal.ModelStore(self.con)
        storemod._ensure(self.con)

    def tearDown(self):
        self.con.close()

    def test_same_timestamp_features_are_stored_once(self):
        base = {
            'ts': 1000, 'direction': 'LONG', 'regime': 'BULL_MARKUP', 'phase': 'IMPULSE',
            'features': {'ret_1': .01, 'rsi': .6}, 'success': 1, 'pnl_r': .4,
            'mfe_r': .8, 'mae_r': .2, 'source_quality': 90,
        }
        a = {**base, 'strategy': 'TREND_PULLBACK'}
        b = {**base, 'strategy': 'MOMENTUM_CONTINUATION'}
        storemod.normalized_add_sample(self.store, a)
        storemod.normalized_add_sample(self.store, b)
        self.store.commit()
        snapshots = self.con.execute('SELECT COUNT(*) FROM learning_feature_snapshots').fetchone()[0]
        samples = self.con.execute('SELECT COUNT(*) FROM learning_samples').fetchone()[0]
        self.assertEqual(snapshots, 1)
        self.assertEqual(samples, 2)
        rows = storemod.full_span_samples(self.store, 'TREND_PULLBACK', direction='LONG')
        self.assertEqual(rows[0]['features']['rsi'], .6)

    def test_large_history_keeps_oldest_and_newest_span(self):
        snapshots = [(i, json.dumps({'ret_1': i / 100000.0}, separators=(',', ':'))) for i in range(5000)]
        samples = [
            (i, 'MOMENTUM_CONTINUATION', 'LONG', 'TRANSITION', 'BALANCE', f'@{i}', 1, .1, .2, .1, 80.0)
            for i in range(5000)
        ]
        self.con.executemany('INSERT INTO learning_feature_snapshots(ts,features) VALUES(?,?)', snapshots)
        self.con.executemany(
            'INSERT INTO learning_samples(ts,strategy,direction,regime,phase,features,success,pnl_r,mfe_r,mae_r,source_quality) VALUES(?,?,?,?,?,?,?,?,?,?,?)',
            samples,
        )
        self.con.commit()
        rows = storemod.full_span_samples(self.store, 'MOMENTUM_CONTINUATION', limit=4000, direction='LONG')
        self.assertLessEqual(len(rows), 4000)
        self.assertEqual(rows[0]['ts'], 0)
        self.assertEqual(rows[-1]['ts'], 4999)
        self.assertTrue(all(rows[i]['ts'] < rows[i + 1]['ts'] for i in range(len(rows) - 1)))
        info = self.store._strict_sampling_info
        self.assertEqual(info['mode'], 'FULL_SPAN_DECIMATED_PLUS_DENSE_RECENT')
        self.assertEqual(info['span_start_ts'], 0)
        self.assertEqual(info['span_end_ts'], 4999)

    def test_dangling_feature_reference_fails_closed(self):
        self.con.execute(
            "INSERT INTO learning_samples(ts,strategy,direction,regime,phase,features,success,pnl_r,mfe_r,mae_r,source_quality) VALUES(1,'TREND_PULLBACK','LONG','TRANSITION','BALANCE','@1',0,0,0,0,80)"
        )
        self.con.commit()
        with self.assertRaises(RuntimeError):
            storemod.full_span_samples(self.store, 'TREND_PULLBACK', direction='LONG')


if __name__ == '__main__':
    unittest.main()
