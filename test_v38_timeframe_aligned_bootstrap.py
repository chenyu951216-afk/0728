from __future__ import annotations

import sqlite3
import tempfile
from pathlib import Path

import v22_hierarchical_pipeline as hierarchical
import v38_timeframe_aligned_bootstrap as alignment


class Core:
    # Production default: 2020-01-01 00:00 Asia/Taipei = 2019-12-31 16:00 UTC.
    START_TS = 1577808000
    TIMEFRAME_SECONDS = {'5m': 300, '15m': 900, '30m': 1800, '1h': 3600, '4h': 14400, '1d': 86400}

    def __init__(self, path: str, cutoff: int):
        self.path = path
        self.saved = {hierarchical.COLLECTION_CUTOFF_KEY: cutoff}
        self.state: dict = {}

    def db(self):
        con = sqlite3.connect(self.path)
        con.row_factory = sqlite3.Row
        con.execute('''CREATE TABLE IF NOT EXISTS market_bars(
            source TEXT,asset TEXT,tf TEXT,ts INTEGER,o REAL,h REAL,l REAL,c REAL,v REAL,qv REAL,
            PRIMARY KEY(source,asset,tf,ts))''')
        con.commit()
        return con

    def get_state(self, key, default=None):
        return self.saved.get(key, default)

    def set_state(self, key, value):
        self.saved[key] = value


def _insert_series(core: Core, asset: str, tf: str) -> None:
    sec, _requested, start, target_end = alignment._aligned_series_window(core, tf)
    rows = []
    for i, ts in enumerate(range(start, target_end + 1, sec)):
        p = 1000.0 + i
        rows.append(('binance', asset, tf, ts, p, p + 1, p - 1, p, 10.0, 100.0))
    con = core.db()
    con.executemany('INSERT OR IGNORE INTO market_bars VALUES(?,?,?,?,?,?,?,?,?,?)', rows)
    con.commit()
    con.close()


def test_production_taipei_midnight_does_not_create_impossible_daily_gap():
    with tempfile.TemporaryDirectory() as directory:
        cutoff = Core.START_TS + 14 * 86400
        core = Core(str(Path(directory) / 'test.db'), cutoff)
        _insert_series(core, 'ETH', '1d')

        progress = alignment._series_progress_aligned(core, 'ETH', '1d')
        assert progress['requested_from'] == 1577808000
        assert progress['target_from'] == 1577836800
        assert progress['alignment_shift_seconds'] == 8 * 3600
        assert progress['history_ready'] is True
        assert progress['gaps_estimate'] == 0

        # Other series are intentionally empty. The first gap must therefore move
        # to the next real required series instead of inventing ETH 1d @ 16:00 UTC.
        target = alignment._first_collection_gap_aligned(core)
        assert target is not None
        assert (target['asset'], target['timeframe']) == ('ETH', '4h')
        assert target['missing_ts'] == Core.START_TS


def test_already_aligned_intraday_start_is_unchanged():
    with tempfile.TemporaryDirectory() as directory:
        cutoff = Core.START_TS + 2 * 86400
        core = Core(str(Path(directory) / 'test.db'), cutoff)
        for tf in ('4h', '1h', '30m', '15m', '5m'):
            _sec, requested, start, _end = alignment._aligned_series_window(core, tf)
            assert start == requested == Core.START_TS


def test_install_replaces_only_collection_timestamp_grid_logic():
    with tempfile.TemporaryDirectory() as directory:
        cutoff = Core.START_TS + 2 * 86400
        core = Core(str(Path(directory) / 'test.db'), cutoff)
        existing = type('Route', (), {'path': '/api/v38/time-alignment'})()
        core.app = type('App', (), {'router': type('Router', (), {'routes': [existing]})()})()

        old_series = hierarchical._series_progress
        old_gap = hierarchical._first_collection_gap
        old_flag = getattr(hierarchical, '_v38_timeframe_alignment_installed', False)
        try:
            hierarchical._v38_timeframe_alignment_installed = False
            alignment.install(core)
            assert hierarchical._series_progress is alignment._series_progress_aligned
            assert hierarchical._first_collection_gap is alignment._first_collection_gap_aligned
            safety = core.state['strict_replay']['timeframe_alignment_v38']
            assert safety['strict_coverage_requirement_unchanged'] is True
            assert safety['future_peeking'] is False
            assert safety['synthetic_gap_fill'] is False
        finally:
            hierarchical._series_progress = old_series
            hierarchical._first_collection_gap = old_gap
            hierarchical._v38_timeframe_alignment_installed = old_flag
