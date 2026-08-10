import json
import sqlite3
import tempfile
import time
import unittest

from derivative_data import DerivativeHistory
import v10_final_integrity as reference
import v14_operational_throughput as fast


class DerivativeCore:
    START_TS = 1577836800
    def __init__(self, path, state_value):
        self.path = path
        self.saved = {reference.STATE_KEY: state_value}
        self.state = {'strict_replay': {'final_derivative_coverage': state_value}}
    def db(self):
        con = sqlite3.connect(self.path)
        con.row_factory = sqlite3.Row
        con.execute('PRAGMA journal_mode=WAL')
        con.execute('CREATE TABLE IF NOT EXISTS system_state(key TEXT PRIMARY KEY,value TEXT,updated_at INTEGER)')
        con.commit()
        return con
    def get_state(self, key, default=None):
        return self.saved.get(key, default)


class MarketCore:
    def __init__(self, path):
        self.path = path
    def db(self):
        con = sqlite3.connect(self.path)
        con.row_factory = sqlite3.Row
        con.execute('CREATE TABLE IF NOT EXISTS market_bars(source TEXT,asset TEXT,tf TEXT,ts INTEGER,o REAL,h REAL,l REAL,c REAL,v REAL,qv REAL,PRIMARY KEY(source,asset,tf,ts))')
        con.commit()
        return con


class OperationalThroughputTests(unittest.TestCase):
    def test_fast_derivative_query_matches_reference_semantics(self):
        decision = 1700000000
        lagged = decision - 4 * 3600
        now = int(time.time())
        source_state = {
            'version': reference.VERSION,
            'sources': {
                'gate_stats': {'last_success_at': now, 'processed_through': decision + 1000},
                'bybit_oi': {'last_success_at': now, 'processed_through': decision + 1000},
                'funding_bybit': {'last_success_at': now, 'processed_through': decision + 1000},
                'funding_binance': {'last_success_at': now, 'processed_through': decision + 1000},
                'cg_oi': {'last_success_at': now, 'processed_through': decision + 1000},
                'cg_liq': {'last_success_at': now, 'processed_through': decision + 1000},
                'cg_book': {'last_success_at': now, 'processed_through': decision + 1000},
            },
            'frozen_enrichment': ['cg_oi', 'cg_liq', 'cg_book'],
        }
        with tempfile.NamedTemporaryFile(suffix='.db') as f:
            core = DerivativeCore(f.name, source_state)
            history = DerivativeHistory(f.name)
            history.ensure_schema()
            history._insert('gate', 'oi_usd', [
                (lagged - 3600, 110.0, 92.0, {}), (lagged - 5 * 3600, 100.0, 92.0, {})
            ])
            history._insert('bybit', 'oi_coin', [
                (lagged - 1800, 220.0, 86.0, {}), (lagged - 6 * 3600, 200.0, 86.0, {})
            ])
            history._insert('coinglass', 'oi_usd', [
                (lagged - 1200, 330.0, 95.0, {}), (lagged - 7 * 3600, 300.0, 95.0, {})
            ])
            history._insert('bybit', 'funding', [(decision - 3600, 0.0002, 86.0, {})])
            history._insert('binance', 'funding', [(decision - 1800, 0.0004, 86.0, {})])
            history._insert('coinglass', 'liq_long_usd', [(lagged - 600, 400000.0, 92.0, {})])
            history._insert('coinglass', 'liq_short_usd', [(lagged - 600, 600000.0, 92.0, {})])
            history._insert('coinglass', 'book_imbalance', [(lagged - 300, 0.17, 88.0, {})])

            expected = reference.strict_derivative_extras(core, history, decision)
            actual = fast.fast_strict_derivative_extras(core, history, decision)
            self.assertEqual(set(expected), set(actual))
            for key in expected:
                self.assertAlmostEqual(float(expected[key]), float(actual[key]), places=12, msg=key)

    def test_fast_derivative_query_does_not_use_future_rows(self):
        decision = 1700000000
        lagged = decision - 4 * 3600
        now = int(time.time())
        source_state = {
            'sources': {
                'bybit_oi': {'last_success_at': now, 'processed_through': decision + 99999},
                'funding_binance': {'last_success_at': now, 'processed_through': decision + 99999},
            },
            'frozen_enrichment': [],
        }
        with tempfile.NamedTemporaryFile(suffix='.db') as f:
            core = DerivativeCore(f.name, source_state)
            history = DerivativeHistory(f.name)
            history.ensure_schema()
            history._insert('bybit', 'oi_coin', [
                (lagged - 3600, 100.0, 86.0, {}), (lagged - 7200, 90.0, 86.0, {}),
                (lagged + 60, 10000.0, 86.0, {}),
            ])
            history._insert('binance', 'funding', [
                (decision - 3600, 0.0001, 86.0, {}), (decision + 60, 0.9, 86.0, {})
            ])
            out = fast.fast_strict_derivative_extras(core, history, decision)
            self.assertAlmostEqual(out['oi_change'], 100.0 / 90.0 - 1.0, places=12)
            self.assertAlmostEqual(out['funding'], 0.0001, places=12)

    def test_live_market_bundle_is_written_in_one_batched_path_without_spot_contamination(self):
        with tempfile.NamedTemporaryFile(suffix='.db') as f:
            core = MarketCore(f.name)
            fast._install_batched_live_market_writes(core)
            row = {'ts': 1700000000, 'o': 100, 'h': 102, 'l': 99, 'c': 101, 'v': 10, 'qv': 1000, 'source': 'gate'}
            bundle = {
                'eth_15m': [dict(row)],
                'eth_5m': [{**row, 'ts': row['ts'] + 300}],
                'eth_spot_15m': [{**row, 'c': 100.5}],
                'btc_1h': [{**row, 'source': 'binance'}],
                'validators': {'bybit': [{**row, 'source': 'bybit'}]},
            }
            core.upsert_live_gate(bundle)
            con = core.db()
            rows = con.execute('SELECT source,asset,tf,COUNT(*) FROM market_bars GROUP BY source,asset,tf ORDER BY source,asset,tf').fetchall()
            con.close()
            got = {(r[0], r[1], r[2]): int(r[3]) for r in rows}
            self.assertEqual(got[('gate', 'ETH', '15m')], 1)
            self.assertEqual(got[('gate', 'ETH', '5m')], 1)
            self.assertEqual(got[('gate_spot', 'ETH_SPOT', '15m')], 1)
            self.assertEqual(got[('binance', 'BTC', '1h')], 1)
            self.assertEqual(got[('bybit', 'ETH', '15m')], 1)
            self.assertNotIn(('gate', 'ETH', '15m', 'spot'), got)

    def test_runtime_busy_timeout_is_long_enough_for_writer_fairness(self):
        con = sqlite3.connect(':memory:')
        fast._configure_runtime_connection(con)
        self.assertEqual(con.execute('PRAGMA busy_timeout').fetchone()[0], fast.BUSY_TIMEOUT_MS)
        self.assertGreaterEqual(fast.BUSY_TIMEOUT_MS, 10000)
        con.close()


if __name__ == '__main__':
    unittest.main()
