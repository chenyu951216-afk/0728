from __future__ import annotations

import time
from typing import Any

import adaptive_v5 as signal
import v7_runtime


def reentry_gate(core: Any, analysis: dict[str, Any], m15: list[dict[str, Any]]) -> dict[str, Any]:
    selection = analysis.get('selection') or {}; direction = str(selection.get('direction') or ''); strategy = str(selection.get('strategy') or '')
    if direction not in ('LONG', 'SHORT'):
        return {'allowed': False, 'reason': 'no valid direction'}
    now = int(time.time()); con = core.db(); raw = con.execute("SELECT signal_id,exit_ts,entry,realized_r,regime,strategy,direction,exit_reason FROM signals WHERE status='CLOSED' AND direction=? AND exit_ts>=? ORDER BY exit_ts DESC LIMIT 20", (direction, now - 24 * 3600)).fetchall(); con.close(); rows = [dict(x) for x in raw]
    if not rows:
        return {'allowed': True, 'reason': 'no recent closed trade in this direction', 'loss_streak': 0, 'loss_stops_12h': 0}
    def losing_stop(x: dict[str, Any]) -> bool:
        return x.get('exit_reason') == 'STOP_OR_TRAIL' and float(x.get('realized_r') or 0) < 0
    streak = 0
    for row in rows:
        if losing_stop(row): streak += 1
        else: break
    losses12 = [x for x in rows if int(x.get('exit_ts') or 0) >= now - 12 * 3600 and losing_stop(x)]
    if streak == 0:
        return {'allowed': True, 'reason': 'most recent directional trade was not a losing stop', 'loss_streak': 0, 'loss_stops_12h': len(losses12)}
    last = rows[0]; base = int(getattr(v7_runtime, 'REENTRY_BASE_BARS', 6)); cooldown_bars = base if streak == 1 else base * 2 if streak == 2 else 96; elapsed = now - int(last.get('exit_ts') or now)
    if elapsed < cooldown_bars * 900:
        return {'allowed': False, 'reason': f'losing-stop cooldown: {cooldown_bars}x15m bars required', 'seconds_remaining': cooldown_bars * 900 - elapsed, 'loss_streak': streak, 'loss_stops_12h': len(losses12), 'last_stop_signal_id': last['signal_id']}
    if streak >= 3 or len(losses12) >= 3:
        return {'allowed': False, 'reason': 'directional whipsaw quarantine: >=3 losing stops in 12h / active 3-loss streak', 'loss_streak': streak, 'loss_stops_12h': len(losses12), 'last_stop_signal_id': last['signal_id']}
    features = analysis.get('features') or {}; current_regime = str((analysis.get('regime') or {}).get('regime') or ''); price = float(analysis.get('price') or 0); a = max(signal.atr(m15), price * .001) if m15 and price > 0 else 0.0; old_entry = float(last.get('entry') or 0)
    if direction == 'LONG': reset = bool(float(features.get('bos_up') or 0) > 0 or float(features.get('sweep_low') or 0) > 0 or current_regime != str(last.get('regime') or '') or (a > 0 and price >= old_entry + .50 * a))
    else: reset = bool(float(features.get('bos_down') or 0) > 0 or float(features.get('sweep_high') or 0) > 0 or current_regime != str(last.get('regime') or '') or (a > 0 and price <= old_entry - .50 * a))
    if not reset:
        return {'allowed': False, 'reason': 'cooldown elapsed but no new BOS/sweep/regime/0.5ATR structural reset', 'loss_streak': streak, 'loss_stops_12h': len(losses12), 'last_stop_signal_id': last['signal_id']}
    return {'allowed': True, 'reason': 'cooldown elapsed and a new structural reset is present', 'loss_streak': streak, 'loss_stops_12h': len(losses12), 'last_stop_signal_id': last['signal_id'], 'strategy': strategy}


def install() -> None:
    v7_runtime.reentry_gate = reentry_gate
