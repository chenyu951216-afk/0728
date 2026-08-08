from __future__ import annotations

from typing import Any


def install(core: Any) -> None:
    original = core.choose_strategy
    def guarded(store: Any, learner: Any, features: dict[str, float], regime: dict[str, Any], data_quality: float) -> dict[str, Any]:
        result = original(store, learner, features, regime, data_quality); probe = core.state.get('risk_feed_probe') or {}; ready = bool(probe.get('gate_trades_ok') and probe.get('coverage_complete'))
        if ready:
            return {**result, 'risk_feed_ready': True}
        candidates = []
        for raw in result.get('candidates') or []:
            c = dict(raw); c['tradeable_without_risk_feed'] = bool(c.get('tradeable')); c['tradeable'] = False; candidates.append(c)
        selected = candidates[0] if candidates else dict(result); selected['tradeable'] = False
        return {**result, **selected, 'tradeable': False, 'candidates': candidates, 'tradeable_candidates': [], 'risk_feed_ready': False, 'risk_feed_probe': probe, 'reason': 'WAIT: ordered Gate trade feed is not fully available; opening a position without reliable SL/TP monitoring is forbidden'}
    core.choose_strategy = guarded
