from __future__ import annotations

import v65_multistrategy_position_authority as v65


class Auto:
    CHECKPOINT_KEY = "checkpoint"

    @staticmethod
    def _hash_payload(genome, n=12):
        return str(genome["id"])

    @staticmethod
    def _load_registry(core, active_only=True):
        return list(core.registry)


class Core:
    def __init__(self, *, fail_id=None):
        self.state = {}
        self.cp = {"status": "COMPLETE"}
        self.registry = [{"strategy_id": "AUTO_PROV_A", "metrics": {"certification_tier": "PROVISIONAL_SCORE_PAPER"}}]
        self.rows = [
            {"rank": i + 1, "finalist_id": x, "genome": {"id": x}, "rationale": {"score_total": 80 - i}}
            for i, x in enumerate("ABCDE")
        ]
        self.strict = []
        self.refit_calls = []
        self.persist_calls = []
        self.v61_state = {}
        self.fail_id = fail_id

    def get_state(self, key, default=None):
        return self.cp if key == Auto.CHECKPOINT_KEY else self.state.get(key, default)


class V61:
    ENABLED = True
    MAX_PROVISIONALS = 5

    @staticmethod
    def _state(core, **patch):
        core.v61_state.update(patch)
        return dict(core.v61_state)

    @staticmethod
    def _strict_champions(core, autonomous):
        return list(core.strict)

    @staticmethod
    def _existing_provisionals(core, autonomous):
        return [x for x in core.registry if str(x.get("strategy_id", "")).startswith("AUTO_PROV_")]

    @staticmethod
    def _rejected_rows(core, autonomous, pipeline52):
        return list(core.rows)

    @staticmethod
    def _refit_frozen_package(core, autonomous, row):
        sid = str(row["genome"]["id"])
        core.refit_calls.append(sid)
        if core.fail_id == sid:
            raise RuntimeError("synthetic refit failure")
        return {"genome": dict(row["genome"]), "metrics": {"ok": True}, "model_blob": b"model"}

    @staticmethod
    def _persist_provisional(core, autonomous, pipeline52, row, fitted):
        sid = v65._provisional_strategy_id(autonomous, row)
        saved = {"strategy_id": sid, "metrics": {"certification_tier": "PROVISIONAL_SCORE_PAPER"}}
        core.persist_calls.append(sid)
        core.registry = [x for x in core.registry if x.get("strategy_id") != sid] + [saved]
        return saved


def run_worker(core):
    old = v65._CONFIGURED
    try:
        v65._CONFIGURED = False
        v65.configure_worker(V61)
        V61._worker(core, Auto, object())
    finally:
        v65._CONFIGURED = old


def test_existing_one_provisional_does_not_short_circuit_remaining_four():
    core = Core()
    run_worker(core)

    ids = {x["strategy_id"] for x in core.registry}
    assert ids == {"AUTO_PROV_A", "AUTO_PROV_B", "AUTO_PROV_C", "AUTO_PROV_D", "AUTO_PROV_E"}
    assert core.refit_calls == ["B", "C", "D", "E"]
    assert set(core.persist_calls) == {"AUTO_PROV_B", "AUTO_PROV_C", "AUTO_PROV_D", "AUTO_PROV_E"}

    state = core.state[v65.STATE_KEY]
    assert state["current_time_roster_ready"] is True
    assert state["completed_current_time_strategy_count"] == 5
    assert state["active_current_time_strategy_count"] == 5
    assert state["missing_current_time_strategy_ids"] == []
    assert state["no_single_strategy_short_circuit"] is True


def test_one_refit_failure_does_not_hide_other_completed_strategies_but_fails_closed():
    core = Core(fail_id="C")
    run_worker(core)

    ids = {x["strategy_id"] for x in core.registry}
    assert {"AUTO_PROV_A", "AUTO_PROV_B", "AUTO_PROV_D", "AUTO_PROV_E"}.issubset(ids)
    assert "AUTO_PROV_C" not in ids
    assert core.refit_calls == ["B", "C", "D", "E"]

    state = core.state[v65.STATE_KEY]
    assert state["current_time_roster_ready"] is False
    assert state["missing_current_time_strategy_ids"] == ["AUTO_PROV_C"]
    assert state["roster_refit_errors"][0]["strategy_id"] == "AUTO_PROV_C"
    assert "fail-closed" in state["roster_reason"]
