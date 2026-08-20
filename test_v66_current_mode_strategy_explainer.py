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


def test_current_time_latch_is_sticky():
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
