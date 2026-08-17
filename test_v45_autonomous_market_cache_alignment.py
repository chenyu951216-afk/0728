from __future__ import annotations

import asyncio
import sqlite3

import v15_data_resilience as resilience
import v45_autonomous_market_cache_alignment as v45


class Core:
    TIMEFRAME_SECONDS = {'5m': 300, '15m': 900}
    START_TS = 0

    def __init__(self, path: str):
        self.path = path
        self.state = {
            'runtime_role': {'role': 'LEADER'},
            'learning': {'phase': 'AUTONOMOUS_DIRECT_R_EVOLUTION_RUNNING'},
        }

    def db(self):
        return sqlite3.connect(self.path)


class Autonomous:
    RESEARCH_START_TS = 0
    RESEARCH_END_EXCLUSIVE_TS = 1800
    SETTLEMENT_END_EXCLUSIVE_TS = 1800

    def __init__(self):
        self.status = {
            'status': 'WAITING_MARKET_CACHE',
            'research_complete': False,
            'active': {'stage': 'DIRECT_R_AUTONOMOUS_EVOLUTION'},
            'champions': [],
            'market_cache_integrity': {},
        }

    def autonomous_status(self, _core):
        return dict(self.status)


def _seed(core: Core, missing_5m: int | None = None):
    con = core.db()
    try:
        con.execute('CREATE TABLE market_bars(asset TEXT, tf TEXT, ts INTEGER, source TEXT)')
        source = resilience.PRICE_PRIORITY[0]
        for ts in range(0, 1800, 300):
            if ts != missing_5m:
                con.execute('INSERT INTO market_bars VALUES(?,?,?,?)', ('ETH', '5m', ts, source))
        for ts in range(0, 1800, 900):
            con.execute('INSERT INTO market_bars VALUES(?,?,?,?)', ('ETH', '15m', ts, source))
        con.commit()
    finally:
        con.close()


def test_market_truth_separates_stage6_window_from_replay(tmp_path, monkeypatch):
    core = Core(str(tmp_path / 'truth.db'))
    auto = Autonomous()
    _seed(core)
    monkeypatch.setattr(v45.runtime_integrity, 'replay_progress', lambda _c: {'complete': True, 'percent': 100.0})

    truth = v45.market_truth(core, auto, refresh=True)

    assert truth['ready'] is True
    assert truth['percent'] == 100.0
    assert truth['historical_replay_percent'] == 100.0
    assert truth['historical_replay_is_separate'] is True
    assert truth['post_research_5m_role'] == 'OUTCOME_SETTLEMENT_ONLY'


def test_first_missing_stage6_bar_is_reported_exactly(tmp_path, monkeypatch):
    core = Core(str(tmp_path / 'gap.db'))
    auto = Autonomous()
    _seed(core, missing_5m=900)
    monkeypatch.setattr(v45.runtime_integrity, 'replay_progress', lambda _c: {'complete': True, 'percent': 100.0})

    truth = v45.market_truth(core, auto, refresh=True)

    assert truth['ready'] is False
    assert truth['first_blocking_gap']['timeframe'] == '5m'
    assert truth['first_blocking_gap']['missing_ts'] == 900
    assert truth['historical_replay_percent'] == 100.0


def test_waiting_market_cache_overrides_stale_running_phase(tmp_path, monkeypatch):
    core = Core(str(tmp_path / 'phase.db'))
    auto = Autonomous()
    _seed(core, missing_5m=900)
    monkeypatch.setattr(v45.runtime_integrity, 'replay_progress', lambda _c: {'complete': True, 'percent': 100.0})

    truth = v45.market_truth(core, auto, refresh=True)
    v45._normalize_phase(core, auto, truth)

    assert core.state['learning']['phase'] == 'WAITING_AUTONOMOUS_MARKET_CACHE_INTEGRITY'
    assert 'ETH 5m' in core.state['learning']['blocker']


def test_priority_repair_fills_blocking_gap_without_resetting_history(tmp_path, monkeypatch):
    core = Core(str(tmp_path / 'repair.db'))
    auto = Autonomous()
    _seed(core, missing_5m=900)
    monkeypatch.setattr(v45.runtime_integrity, 'replay_progress', lambda _c: {'complete': True, 'percent': 100.0})
    v45._LAST_REPAIR_AT = 0.0

    calls = []

    async def fake_repair(c, target):
        calls.append(dict(target))
        con = c.db()
        try:
            con.execute(
                'INSERT INTO market_bars VALUES(?,?,?,?)',
                ('ETH', target['timeframe'], int(target['missing_ts']), resilience.PRICE_PRIORITY[0]),
            )
            con.commit()
        finally:
            con.close()
        return {'status': 'REPAIRED', 'provider': resilience.PRICE_PRIORITY[0]}

    monkeypatch.setattr(v45.hierarchical, '_repair_collection_gap', fake_repair)

    result = asyncio.run(v45.repair_stage6_market(core, auto, force=True))

    assert calls and calls[0]['timeframe'] == '5m'
    assert calls[0]['missing_ts'] == 900
    assert result['status'] == 'READY'
    assert result['market']['ready'] is True
    assert core.state['learning']['phase'] == 'AUTONOMOUS_MARKET_CACHE_REBUILD_PENDING'
