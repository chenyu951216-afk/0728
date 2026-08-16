from __future__ import annotations

import sqlite3
import tempfile
from pathlib import Path

import v15_data_resilience as resilience
import v16_runtime_integrity as runtime_integrity
import v22_hierarchical_pipeline as hierarchical
import v39_replay_liveness_grid_integrity as v39


class Core:
    START_TS = 0
    TIMEFRAME_SECONDS = {'5m': 300, '15m': 900, '30m': 1800, '1h': 3600, '4h': 14400, '1d': 86400}

    def __init__(self, path: str, cutoff: int = 1200):
        self.path = path
        self.saved = {hierarchical.COLLECTION_CUTOFF_KEY: cutoff}
        self.state: dict = {}

    def db(self):
        con = sqlite3.connect(self.path)
        con.row_factory = sqlite3.Row
        con.execute('''CREATE TABLE IF NOT EXISTS market_bars(
            source TEXT,asset TEXT,tf TEXT,ts INTEGER,o REAL,h REAL,l REAL,c REAL,v REAL,qv REAL,
            PRIMARY KEY(source,asset,tf,ts))''')
        con.execute('''CREATE TABLE IF NOT EXISTS learning_samples(
            ts INTEGER,strategy TEXT,direction TEXT,regime TEXT,phase TEXT,features TEXT,
            success INTEGER,pnl_r REAL,mfe_r REAL,mae_r REAL,source_quality REAL)''')
        con.commit()
        return con

    def get_state(self, key, default=None):
        return self.saved.get(key, default)

    def set_state(self, key, value):
        self.saved[key] = value


def insert(core: Core, asset: str, tf: str, timestamps: list[int], source: str = 'binance') -> None:
    rows = []
    for i, ts in enumerate(timestamps):
        price = 1000.0 + i
        rows.append((source, asset, tf, ts, price, price + 1, price - 1, price, 10.0, 100.0))
    con = core.db()
    con.executemany('INSERT OR IGNORE INTO market_bars VALUES(?,?,?,?,?,?,?,?,?,?)', rows)
    con.commit()
    con.close()


def test_off_grid_timestamp_cannot_fake_100_percent_history_ready():
    with tempfile.TemporaryDirectory() as directory:
        core = Core(str(Path(directory) / 'test.db'), cutoff=1200)
        # Expected 5m grid is 0,300,600,900.  600 is truly missing, while 750 is
        # an off-grid extra.  A raw COUNT(DISTINCT ts) would incorrectly see 4/4.
        insert(core, 'ETH', '5m', [0, 300, 750, 900])
        progress = v39._series_progress_grid_safe(core, 'ETH', '5m')
        assert progress['expected_bars'] == 4
        assert progress['bars'] == 3
        assert progress['gaps_estimate'] == 1
        assert progress['off_grid_distinct_timestamps_ignored'] == 1
        assert progress['history_ready'] is False


def test_off_grid_timestamp_is_excluded_from_canonical_replay_sequence():
    with tempfile.TemporaryDirectory() as directory:
        core = Core(str(Path(directory) / 'test.db'), cutoff=1200)
        insert(core, 'ETH', '5m', [0, 300, 750, 900])
        resilience._CANON_CACHE.clear()
        rows = v39._canonical_bars_grid_safe(core, 'ETH', '5m')
        assert [row['ts'] for row in rows] == [0, 300, 900]
        assert all(row['ts'] % 300 == 0 for row in rows)


def test_gap_registry_self_heals_when_real_grid_candle_now_exists():
    with tempfile.TemporaryDirectory() as directory:
        core = Core(str(Path(directory) / 'test.db'), cutoff=1200)
        ts = 600
        core.saved[resilience.GAP_KEY] = {
            'version': 1,
            'gaps': {
                'ETH:5m:600': {
                    'gap_id': 'ETH:5m:600', 'asset': 'ETH', 'tf': '5m',
                    'missing_ts': ts, 'status': 'PENDING_REPAIR', 'attempts': 2,
                }
            },
        }
        insert(core, 'ETH', '5m', [ts])
        result = v39._reconcile_gap_registry(core)
        assert result['count'] == 1
        repaired = core.saved[resilience.GAP_KEY]['gaps']['ETH:5m:600']
        assert repaired['status'] == 'REPAIRED'
        assert repaired['reconciled_by'] == v39.VERSION


def test_resolved_price_gap_blocker_is_stale_only_after_authoritative_recheck():
    blocker = {'blocked': True, 'state': 'BLOCK_PRICE_GAP', 'reason': 'core price continuity gap is unresolved'}
    with tempfile.TemporaryDirectory() as directory:
        core = Core(str(Path(directory) / 'test.db'))
        gate = {'ready': True}
        assert v39._blocker_stale(core, blocker, gate, current_gap=None) is True
        assert v39._blocker_stale(
            core, blocker, gate,
            current_gap={'asset': 'ETH', 'tf': '5m', 'missing_ts': 600},
        ) is False


def test_full_history_and_price_blockers_are_cleared_when_authority_says_ready():
    with tempfile.TemporaryDirectory() as directory:
        core = Core(str(Path(directory) / 'test.db'))
        core.state['learning'] = {
            'phase': 'WAITING_FOR_FULL_HISTORY',
            'replay_price_blocker': {
                'blocked': True, 'state': 'WAITING_FOR_FULL_HISTORY',
                'reason': 'strict point-in-time replay cannot start until every required price timeframe meets the frozen full-history coverage contract',
            },
        }
        core.state['strict_replay_gap_blocker'] = {
            'blocked': True, 'state': 'BLOCK_PRICE_GAP',
            'reason': 'core price continuity gap is unresolved',
            'at_ts': 600,
        }
        core.saved['v18_final_system_state'] = {
            'status': 'WAITING_FOR_FULL_HISTORY', 'reason': 'stale persisted status',
        }

        old_gate = hierarchical.price_collection_gate
        old_replay = runtime_integrity.replay_progress
        old_detect = resilience.detect_gap_near_cursor
        old_ready = resilience.ready_through
        try:
            hierarchical.price_collection_gate = lambda _core: {'ready': True, 'blockers': [], 'percent': 100.0}
            runtime_integrity.replay_progress = lambda _core: {'complete': False, 'percent': 35.69}
            resilience.detect_gap_near_cursor = lambda _core: None
            resilience.ready_through = lambda _core: None
            status = v39._normalize_authority_state(core)
        finally:
            hierarchical.price_collection_gate = old_gate
            runtime_integrity.replay_progress = old_replay
            resilience.detect_gap_near_cursor = old_detect
            resilience.ready_through = old_ready

        assert status['learning_blocker']['blocked'] is False
        assert status['strict_blocker']['blocked'] is False
        assert core.state['learning']['phase'] == 'STRICT_REPLAY_ADVANCING'
        assert core.saved['v18_final_system_state']['status'] == 'STRICT_REPLAY_ADVANCING'
        assert 'learning.replay_price_blocker' in status['cleared']
        assert 'strict_replay_gap_blocker' in status['cleared']


def test_resume_never_bypasses_a_real_gap_or_derivative_watermark():
    base = {
        'gate': {'ready': True},
        'replay': {'complete': False},
        'current_gap': None,
        'learning_blocker': {'blocked': False},
        'strict_blocker': {'blocked': False},
        'derivative_watermark': {'blocked': False},
    }
    assert v39._resume_eligible(base) is True
    with_gap = {**base, 'current_gap': {'asset': 'ETH', 'tf': '5m', 'missing_ts': 600}}
    assert v39._resume_eligible(with_gap) is False
    with_derivative_block = {**base, 'derivative_watermark': {'blocked': True}}
    assert v39._resume_eligible(with_derivative_block) is False
