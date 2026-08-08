from __future__ import annotations

import time
from typing import Any


def _health(core: Any, candidate: dict[str, Any]) -> dict[str, Any]:
    model = candidate.get('model') or {}; model_version = int(model.get('model_version') or 0); ex = candidate.get('execution') or {}; meta = ex.get('metrics') or {}; execution_version = int(meta.get('execution_version') or 0)
    if model_version <= 0 or execution_version <= 0:
        return {'n': 0, 'blocked': False, 'reason': 'no deployed version'}
    con = core.db(); rows = con.execute('SELECT realized_r,ts,review_label FROM live_execution_samples WHERE strategy=? AND direction=? AND model_version=? AND execution_version=? ORDER BY ts DESC LIMIT 20', (candidate.get('strategy'), candidate.get('direction'), model_version, execution_version)).fetchall(); con.close()
    pnls = [float(x[0] or 0) for x in reversed(rows)]; labels = [str(x[2]) for x in rows if x[2]]; n = len(pnls); gains = sum(max(x, 0) for x in pnls); losses = sum(max(-x, 0) for x in pnls); ev = sum(pnls) / n if n else 0.0; pf = gains / max(losses, 1e-9) if n else 0.0; last3 = pnls[-3:]; last6 = pnls[-6:]
    emergency = len(last3) == 3 and all(x < 0 for x in last3) and sum(last3) <= -2.4
    persistent = len(last6) == 6 and sum(last6) / 6 <= -.20
    statistical = n >= 10 and (ev <= -.12 or pf < .75)
    recent_labels = labels[:6]; tight_stop_pattern = recent_labels.count('STOP_TOO_TIGHT_CANDIDATE') >= 3; early_exit_pattern = recent_labels.count('EARLY_EXIT_RUNNER_OPPORTUNITY') >= 3
    if tight_stop_pattern or early_exit_pattern:
        # Do not mutate parameters from a few live outcomes. Force the normal
        # historical point-in-time execution audit to reconsider policy instead.
        core.set_state('v7_execution_last_attempt_ts', 0)
    key = f"v7_live_quarantine:{candidate.get('strategy')}:{candidate.get('direction')}:{model_version}:{execution_version}"; until = int(core.get_state(key, 0) or 0); now = int(time.time()); trigger = bool(emergency or persistent or statistical or tight_stop_pattern)
    if trigger and until <= now:
        until = now + (12 * 3600 if statistical or tight_stop_pattern else 6 * 3600); core.set_state(key, until)
    blocked = until > now
    return {'n': n, 'ev_r': ev, 'profit_factor': pf, 'last3_r': sum(last3) if last3 else 0.0, 'last6_ev_r': sum(last6) / len(last6) if last6 else 0.0, 'recent_review_labels': recent_labels, 'blocked': blocked, 'blocked_until': until if blocked else None, 'reason': 'live deployment drift quarantine' if blocked else 'live health acceptable or insufficient sample', 'trigger': {'emergency_3_loss': emergency, 'persistent_6': persistent, 'statistical_10': statistical, 'tight_stop_pattern': tight_stop_pattern, 'early_exit_reaudit': early_exit_pattern}}


def install(core: Any) -> None:
    original = core.choose_strategy
    def guarded(store: Any, learner: Any, features: dict[str, float], regime: dict[str, Any], data_quality: float) -> dict[str, Any]:
        result = original(store, learner, features, regime, data_quality); candidates = []
        for raw in result.get('candidates') or []:
            c = dict(raw); h = _health(core, c) if c.get('tradeable') else {'n': 0, 'blocked': False, 'reason': 'not currently tradeable'}; c['live_health'] = h
            if h.get('blocked'): c['tradeable'] = False
            candidates.append(c)
        candidates.sort(key=lambda x: x.get('final_score', x.get('score', 0)), reverse=True); eligible = [x for x in candidates if x.get('tradeable')]
        if eligible:
            selected = eligible[0]; reason = result.get('reason')
        else:
            clean = [x for x in candidates if x.get('certified')]; selected = {**(clean[0] if clean else candidates[0] if candidates else result), 'tradeable': False}; blocked_any = any((x.get('live_health') or {}).get('blocked') for x in candidates); reason = 'all otherwise-eligible deployments are quarantined by live drift guard' if blocked_any else result.get('reason')
        return {**result, **selected, 'tradeable': bool(selected.get('tradeable')), 'candidates': candidates, 'tradeable_candidates': eligible[:5], 'reason': reason, 'live_drift_guard': True}
    core.choose_strategy = guarded
