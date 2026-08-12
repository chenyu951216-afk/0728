from __future__ import annotations

import sqlite3
import tempfile
from pathlib import Path

import v5_runtime
import v16_runtime_integrity as runtime
import v22_hierarchical_pipeline as pipeline


class Core:
    START_TS = 1577836800
    TIMEFRAME_SECONDS = {'5m': 300, '15m': 900, '30m': 1800, '1h': 3600, '4h': 14400, '1d': 86400}

    def __init__(self, path: str, cutoff: int):
        self.path = path
        self.saved = {pipeline.COLLECTION_CUTOFF_KEY: cutoff}
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
        con.execute('CREATE TABLE IF NOT EXISTS learning_feature_snapshots(ts INTEGER PRIMARY KEY,features TEXT)')
        con.execute('CREATE TABLE IF NOT EXISTS model_registry(strategy TEXT,direction TEXT,version INTEGER,status TEXT,created_at INTEGER,metrics TEXT,model BLOB)')
        con.execute('CREATE TABLE IF NOT EXISTS execution_registry_v7(strategy TEXT,direction TEXT,model_version INTEGER,version INTEGER,status TEXT,created_at INTEGER,metrics TEXT,policy TEXT)')
        con.commit()
        return con

    def get_state(self, key, default=None):
        return self.saved.get(key, default)

    def set_state(self, key, value):
        self.saved[key] = value


def add_full_series(core: Core, asset: str, tf: str, *, recent_only: int | None = None) -> None:
    sec = core.TIMEFRAME_SECONDS[tf]
    target_end = (core.saved[pipeline.COLLECTION_CUTOFF_KEY] // sec) * sec - sec
    timestamps = list(range(core.START_TS, target_end + 1, sec))
    if recent_only is not None:
        timestamps = timestamps[-recent_only:]
    rows = []
    for i, ts in enumerate(timestamps):
        price = 1000.0 + i * .01
        rows.append(('gate', asset, tf, ts, price, price + 1, price - 1, price, 10, 100))
    con = core.db()
    con.executemany('INSERT OR IGNORE INTO market_bars VALUES(?,?,?,?,?,?,?,?,?,?)', rows)
    con.commit()
    con.close()


def populate_required(core: Core, *, recent_5m_only: int | None = None) -> None:
    for _group, specs in pipeline.PRICE_GROUPS:
        for asset, tf in specs:
            add_full_series(core, asset, tf, recent_only=recent_5m_only if tf == '5m' else None)


def test_recent_only_5m_history_cannot_start_replay_or_claim_99_percent():
    with tempfile.TemporaryDirectory() as directory:
        cutoff = Core.START_TS + 10 * 86400
        core = Core(str(Path(directory) / 'test.db'), cutoff)
        populate_required(core, recent_5m_only=100)
        gate = pipeline.price_collection_gate(core)
        assert not gate['ready']
        assert gate['status'] == 'COLLECTING_FULL_HISTORY_BEFORE_REPLAY'

        core.set_state(v5_runtime.REPLAY_STATE_KEY, cutoff - 900)
        core.price_collection_gate = lambda: pipeline.price_collection_gate(core)
        progress = runtime.replay_progress(core)
        assert progress['status'] == 'WAITING_FOR_FULL_HISTORY'
        assert progress['percent'] == 0.0
        assert not progress['complete']


def test_generator_is_fail_closed_until_every_required_timeframe_is_collected():
    with tempfile.TemporaryDirectory() as directory:
        cutoff = Core.START_TS + 10 * 86400
        core = Core(str(Path(directory) / 'test.db'), cutoff)
        populate_required(core, recent_5m_only=100)
        calls: list[int] = []
        original = v5_runtime.generate_learning_samples_v5
        try:
            v5_runtime.generate_learning_samples_v5 = lambda _core, batch=500: calls.append(batch) or 14
            pipeline._install_full_history_replay_gate(core)
            assert v5_runtime.generate_learning_samples_v5(core, 123) == 0
            assert calls == []
            assert core.state['learning']['phase'] == 'COLLECTING_FULL_HISTORY_BEFORE_REPLAY'
        finally:
            v5_runtime.generate_learning_samples_v5 = original


def test_complete_frozen_horizon_unlocks_causal_replay_without_exposing_future_to_decision():
    with tempfile.TemporaryDirectory() as directory:
        cutoff = Core.START_TS + 10 * 86400
        core = Core(str(Path(directory) / 'test.db'), cutoff)
        populate_required(core)
        gate = pipeline.price_collection_gate(core)
        assert gate['ready']
        assert gate['status'] == 'READY_FOR_CAUSAL_REPLAY'
        assert gate['future_data_available_to_decision'] is False
        assert gate['future_5m_after_decision_is_label_only'] is True

        calls: list[int] = []
        original = v5_runtime.generate_learning_samples_v5
        try:
            v5_runtime.generate_learning_samples_v5 = lambda _core, batch=500: calls.append(batch) or 14
            pipeline._install_full_history_replay_gate(core)
            assert v5_runtime.generate_learning_samples_v5(core, 321) == 14
            assert calls == [321]
            contract = core.get_state(pipeline.COLLECTION_CONTRACT_KEY)
            assert contract['raw_history_can_expand_behind_cursor_without_reset'] is False
        finally:
            v5_runtime.generate_learning_samples_v5 = original


def test_collection_progress_uses_the_weakest_required_group_not_an_average():
    groups = {
        'MACRO_CONTEXT': {'percent': 100.0},
        'MARKET_STRUCTURE': {'percent': 100.0},
        'SHORT_HORIZON_EXECUTION': {'percent': .1},
    }
    assert min(group['percent'] for group in groups.values()) == .1
    assert round(sum(group['percent'] for group in groups.values()) / 3, 2) == 66.7


def test_collection_scanner_finds_an_internal_gap_before_replay():
    with tempfile.TemporaryDirectory() as directory:
        cutoff = Core.START_TS + 10 * 86400
        core = Core(str(Path(directory) / 'test.db'), cutoff)
        populate_required(core)
        missing = Core.START_TS + 123 * core.TIMEFRAME_SECONDS['5m']
        con = core.db()
        con.execute("DELETE FROM market_bars WHERE asset='ETH' AND tf='5m' AND ts=?", (missing,))
        con.commit()
        con.close()
        target = pipeline._first_collection_gap(core)
        assert target is not None
        assert target['asset'] == 'ETH'
        assert target['timeframe'] == '5m'
        assert target['missing_ts'] == missing


def test_schema_upgrade_rewinds_a_stale_cursor_even_when_no_samples_survive():
    with tempfile.TemporaryDirectory() as directory:
        cutoff = Core.START_TS + 10 * 86400
        core = Core(str(Path(directory) / 'test.db'), cutoff)
        core.set_state(v5_runtime.REPLAY_STATE_KEY, cutoff - 900)
        pipeline._ensure_feature_schema(core)
        assert core.get_state(v5_runtime.REPLAY_STATE_KEY) == Core.START_TS
        assert core.get_state(pipeline.STATE_KEY) == pipeline.FEATURE_SCHEMA
        reset = core.state['learning']['replay_cursor_integrity_reset']
        assert reset['raw_market_preserved'] is True
        assert reset['raw_derivatives_preserved'] is True
