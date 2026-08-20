from __future__ import annotations

import asyncio
import v66_current_mode_strategy_explainer as v66


class Auto:
    CHECKPOINT_KEY = "checkpoint"

    @staticmethod
    def _load_registry(core, active_only=True):
        return list(core.strategies)


class Core:
    def __init__(self):
        self.state = {}
        self.cp = {"status": "RUNNING"}
        self.strategies = []

    def get_state(self, key, default=None):
        return self.cp if key == Auto.CHECKPOINT_KEY else self.state.get(key, default)


class V63:
    LIVE_MIN_SCORE = 55.0
    REVERSAL_SCORE_MARGIN = 7.5


class V64:
    @staticmethod
    def feature_label(name):
        return {"rsi": "RSI", "funding": "資金費率"}.get(name, name)


def test_current_time_latch_is_sticky_while_checkpoint_remains_terminal():
    core = Core()
    assert v66.refresh_mode(core, Auto, source="test")["mode"] == "HISTORICAL_RESEARCH"
    core.cp = {"status": "COMPLETE"}
    assert v66.refresh_mode(core, Auto, source="test")["mode"] == "POST_OOS_WAITING_STRATEGY_PERSIST"
    core.strategies = [{"strategy_id": "A"}]
    assert v66.refresh_mode(core, Auto, source="test")["latched_current_time"] is True
    core.strategies = []
    z = v66.refresh_mode(core, Auto, source="test")
    assert z["mode"] == "CURRENT_TIME_PAPER"
    assert z["historical_restart_suppressed"] is True


def test_real_semantic_reset_can_return_to_historical_mode():
    core = Core()
    core.cp = {"status": "COMPLETE"}
    core.strategies = [{"strategy_id": "A"}]
    assert v66.refresh_mode(core, Auto, source="prime")["latched_current_time"] is True
    core.cp = {"status": "RUNNING"}
    z = v66.refresh_mode(core, Auto, source="reset")
    assert z["latched_current_time"] is False
    assert z["mode"] == "HISTORICAL_RESEARCH"


def test_no_strategy_after_latch_never_calls_historical_tick():
    core = Core()
    core.cp = {"status": "COMPLETE"}
    core.strategies = [{"strategy_id": "A"}]
    v66.refresh_mode(core, Auto, source="prime")
    core.strategies = []
    calls = {"n": 0}

    async def base():
        calls["n"] += 1

    asyncio.run(v66._learning_wrapper(core, Auto, base)())
    assert calls["n"] == 0
    assert core.state["learning"]["phase"] == "CURRENT_TIME_LATCHED_NO_ACTIVE_STRATEGY"


def test_strategy_explanation_has_exact_data_and_execution_plan():
    item = {
        "strategy_id": "AUTO_TEST", "direction": "LONG", "behavior_label": "test",
        "genome": {
            "direction": "LONG", "feature_names": ["rsi", "funding"], "decision_stride": 2,
            "entry_market": False, "entry_offset_atr": -0.2, "stop_atr": 1.6,
            "target_rr": [1.0, 2.0], "allocations": [40.0, 60.0],
            "expire_bars": 4, "max_hold_bars": 32, "breakeven_after_r": 0.8,
            "trail_start_r": 1.5, "trail_lock_r": 0.5, "cooldown_bars": 2,
        },
        "metrics": {"feature_names": ["rsi", "funding"],
                    "gate_thresholds": [{"feature": "rsi", "op": "GE", "value": 50}]},
    }
    z = v66.strategy_explanation(item, {}, V63, V64)
    assert [x["name"] for x in z["data_used"]] == ["rsi", "funding"]
    assert "RSI (rsi) ≥ 50" in z["state_gate_rules_zh"]
    assert z["decision_cadence_minutes"] == 30
    assert z["closed_candle_only"] is True
    assert z["execution_plan"]["entry_type"] == "LIMIT"
    assert z["execution_plan"]["stop_atr"] == 1.6
    assert z["execution_plan"]["targets"][1]["rr"] == 2.0
    assert "55.0/100" in z["entry_basis_zh"]
    assert "7.5 分" in z["entry_basis_zh"]
    assert "所有完成策略同輪獨立判斷" in z["entry_basis_zh"]


def _five_strategy_analysis():
    return {
        "strategy_diagnostics": [
            {"strategy": "A", "qualified": False, "v63_live_score": 42.0, "edge_r": -0.1},
            {"strategy": "B", "qualified": False, "v63_live_score": 48.0, "edge_r": -0.02},
            {"strategy": "C", "qualified": True, "v63_live_score": 73.0, "edge_r": 0.12},
            {"strategy": "D", "qualified": False, "v63_live_score": 51.0, "edge_r": 0.01},
            {"strategy": "E", "qualified": False, "v63_live_score": 39.0, "edge_r": -0.2},
        ],
        "selection": {"strategy": "C", "tradeable": True},
    }


def test_five_completed_strategies_are_all_required_in_same_current_scan():
    core = Core()
    core.cp = {"status": "COMPLETE"}
    core.strategies = [{"strategy_id": x} for x in "ABCDE"]
    core.state[v66.V65_STATE_KEY] = {
        "current_time_roster_ready": True,
        "completed_current_time_strategy_count": 5,
        "missing_current_time_strategy_ids": [],
    }
    v66.refresh_mode(core, Auto, source="prime")

    analysis = _five_strategy_analysis()
    roster = v66._parallel_roster(core, Auto, analysis)
    assert roster["completed_strategy_count"] == 5
    assert roster["evaluated_strategy_count"] == 5
    assert roster["all_completed_evaluated_same_scan"] is True
    assert roster["qualified_count"] == 1
    assert roster["best_qualified_strategy"] == "C"
    assert roster["ready_for_new_entry"] is True


def test_one_qualified_of_five_can_enter_after_all_five_were_evaluated():
    core = Core()
    core.cp = {"status": "COMPLETE"}
    core.strategies = [{"strategy_id": x} for x in "ABCDE"]
    core.state[v66.V65_STATE_KEY] = {
        "current_time_roster_ready": True,
        "completed_current_time_strategy_count": 5,
        "missing_current_time_strategy_ids": [],
    }
    v66.refresh_mode(core, Auto, source="prime")
    calls = {"n": 0}

    def base_create(analysis, m15):
        calls["n"] += 1
        return {"strategy": analysis["selection"]["strategy"]}

    wrapped = v66._create_wrapper(core, Auto, base_create)
    out = wrapped(_five_strategy_analysis(), [])
    assert out == {"strategy": "C"}
    assert calls["n"] == 1


def test_partial_one_of_five_current_scan_is_fail_closed_and_cannot_open_signal():
    core = Core()
    core.cp = {"status": "COMPLETE"}
    core.strategies = [{"strategy_id": x} for x in "ABCDE"]
    core.state[v66.V65_STATE_KEY] = {
        "current_time_roster_ready": True,
        "completed_current_time_strategy_count": 5,
        "missing_current_time_strategy_ids": [],
    }
    v66.refresh_mode(core, Auto, source="prime")
    calls = {"n": 0}

    def base_create(analysis, m15):
        calls["n"] += 1
        return {"strategy": analysis["selection"]["strategy"]}

    partial = {
        "strategy_diagnostics": [{"strategy": "C", "qualified": True, "v63_live_score": 73.0, "edge_r": 0.12}],
        "selection": {"strategy": "C", "tradeable": True},
    }
    out = v66._create_wrapper(core, Auto, base_create)(partial, [])
    assert out is None
    assert calls["n"] == 0
    guard = core.state[v66.STATE_KEY]["parallel_current_time"]
    assert guard["ready_for_new_entry"] is False
    assert set(guard["missing_from_current_scan"]) == {"A", "B", "D", "E"}
    assert core.state[v66.STATE_KEY]["partial_roster_entry_blocked"] is True
