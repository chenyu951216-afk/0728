from __future__ import annotations

from unittest.mock import patch

import v44_fixed_research_horizon_authority as v44


class Core:
    START_TS = 0
    TIMEFRAME_SECONDS = {
        '5m': 300,
        '15m': 900,
        '30m': 1800,
        '1h': 3600,
        '4h': 14400,
        '1d': 86400,
    }


class Autonomous:
    RESEARCH_START_TS = 0
    RESEARCH_END_EXCLUSIVE_TS = 220 * 900
    SETTLEMENT_END_EXCLUSIVE_TS = 320 * 900


def _bars(step: int, last: int):
    return [
        {'ts': ts, 'o': 1.0, 'h': 1.0, 'l': 1.0, 'c': 1.0, 'v': 1.0, 'qv': 1.0}
        for ts in range(0, last + 1, step)
    ]


def test_decision_horizon_is_research_end_minus_one_15m_bar():
    core = Core()
    auto = Autonomous()
    assert v44._decision_last_open(core, auto) == auto.RESEARCH_END_EXCLUSIVE_TS - 900


def test_collection_windows_are_role_specific_and_never_wall_clock_based():
    core = Core()
    auto = Autonomous()
    assert v44._series_end_exclusive(core, auto, '1h') == auto.RESEARCH_END_EXCLUSIVE_TS
    assert v44._series_end_exclusive(core, auto, '15m') == auto.RESEARCH_END_EXCLUSIVE_TS + 33 * 900
    assert v44._series_end_exclusive(core, auto, '5m') == auto.SETTLEMENT_END_EXCLUSIVE_TS


def test_live_candles_after_research_end_cannot_expand_replay_frontier():
    core = Core()
    auto = Autonomous()
    desired = auto.RESEARCH_END_EXCLUSIVE_TS - 900
    # Give both series substantially more data than the research window.  The legal
    # historical frontier must still stop at the configured research end.
    m15 = _bars(900, auto.RESEARCH_END_EXCLUSIVE_TS + 80 * 900)
    m5 = _bars(300, auto.SETTLEMENT_END_EXCLUSIVE_TS + 300 * 300)

    def canonical(_core, asset, tf):
        assert asset == 'ETH'
        return m15 if tf == '15m' else m5

    with patch.object(v44.hierarchical.resilience, 'canonical_bars', side_effect=canonical):
        result = v44._fixed_legal_frontier(core, auto)

    assert result['ready'] is True
    assert result['legal_frontier_ts'] == desired
    assert result['fixed_research_last_decision_ts'] == desired
    assert result['moving_frontier'] is False
    assert result['research_horizon_reached'] is True


def test_daily_start_alignment_preserves_requested_start_without_phantom_bar():
    class TaipeiCore(Core):
        START_TS = 1577808000  # 2020-01-01 00:00 Asia/Taipei

    class RealAuto(Autonomous):
        RESEARCH_END_EXCLUSIVE_TS = 1785600000
        SETTLEMENT_END_EXCLUSIVE_TS = 1786723200

    sec, requested, first_valid, target_end = v44._aligned_series_window(TaipeiCore(), RealAuto(), '1d')
    assert sec == 86400
    assert requested == 1577808000
    assert first_valid == 1577836800
    assert first_valid % 86400 == 0
    assert target_end % 86400 == 0
    assert target_end < RealAuto.RESEARCH_END_EXCLUSIVE_TS
