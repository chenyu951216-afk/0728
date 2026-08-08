from __future__ import annotations

import bisect
from typing import Any

import execution_v7

_ORIGINAL_STATS = execution_v7._stats


def _continuous(rows: list[dict[str, Any]], seconds: int = 300) -> bool:
    return bool(rows) and all(int(rows[i]['ts']) - int(rows[i-1]['ts']) == seconds for i in range(1, len(rows)))


def simulate_policy_5m(data: dict[str, Any], opp: dict[str, Any], strategy: str, direction: str, policy: dict[str, Any]) -> dict[str, Any]:
    m15 = data['m15']; i15 = data['index15'].get(int(opp['ts']))
    if i15 is None or i15 < 100:
        return {'invalid_data': True, 'filled': False, 'pnl_r': 0.0}
    decision_close = int(opp['ts']) + 900
    past15 = m15[max(0, i15 - 500):i15 + 1]
    past30 = execution_v7._slice_to(data['m30'], data['ts30'], int(opp['ts']), 300)
    past1h = execution_v7._slice_to(data['h1'], data['ts1h'], int(opp['ts']), 300)
    live = execution_v7.f(m15[i15]['c'])
    plan = execution_v7.plan_from_policy(strategy, direction, live, past15, policy, past30, past1h)
    entry, stop0 = execution_v7.f(plan['entry']), execution_v7.f(plan['stop']); risk = abs(entry - stop0)
    if risk <= 1e-9:
        return {'invalid_data': True, 'filled': False, 'pnl_r': 0.0}
    m5 = data.get('m5') or []; ts5 = data.get('ts5') or []
    start5 = bisect.bisect_left(ts5, decision_close)
    max_5m = int(policy.get('max_hold_bars', execution_v7.MAX_HOLD_BARS)) * 3
    future = m5[start5:start5 + max_5m]
    # Execution outcomes with missing 5m candles are unknown, not losses or
    # unfilled orders. Exclude them entirely from policy statistics.
    if len(future) < min(12, max_5m) or not _continuous(future):
        return {'invalid_data': True, 'filled': False, 'pnl_r': 0.0}
    expire_5m = min(len(future), int(policy.get('expire_bars', 6)) * 3)
    fill_idx = next((j for j,b in enumerate(future[:expire_5m]) if execution_v7.f(b['l']) <= entry <= execution_v7.f(b['h'])), None)
    if fill_idx is None:
        return {'filled': False, 'pnl_r': 0.0, 'entry': entry, 'stop': stop0, 'path_timeframe': '5m'}
    sign = 1 if direction == 'LONG' else -1; remaining = 1.0; realized = 0.0; current_stop = stop0; hit=set(); mfe=mae=0.0; exit_reason='TIMEOUT'; last=entry
    for b in future[fill_idx:]:
        low,high,close=execution_v7.f(b['l']),execution_v7.f(b['h']),execution_v7.f(b['c']);last=close
        favorable=(high-entry)/risk if direction=='LONG' else (entry-low)/risk; adverse=(entry-low)/risk if direction=='LONG' else (high-entry)/risk;mfe=max(mfe,favorable);mae=max(mae,adverse)
        stop_hit=low<=current_stop if direction=='LONG' else high>=current_stop
        # Conservative inside a 5m OHLC bar: stop wins same-bar ties.
        if stop_hit:
            exit_rr=(current_stop-entry)*sign/risk;realized+=remaining*exit_rr;remaining=0.0;exit_reason='STOP_OR_TRAIL';break
        for idx,target in enumerate(plan['targets']):
            if idx in hit:continue
            px=execution_v7.f(target['price']);target_hit=high>=px if direction=='LONG' else low<=px
            if not target_hit:continue
            frac=min(remaining,execution_v7.f(target['allocation'])/100.0);realized+=frac*execution_v7.f(target['rr']);remaining-=frac;hit.add(idx)
        if 0 in hit:current_stop=max(current_stop,entry) if direction=='LONG' else min(current_stop,entry)
        if 1 in hit:
            locked=entry+sign*execution_v7.f(policy.get('lock_after_tp2_r'),.55)*risk;current_stop=max(current_stop,locked) if direction=='LONG' else min(current_stop,locked)
        if 2 in hit:
            locked=entry+sign*execution_v7.f(policy.get('lock_after_tp3_r'),1.05)*risk;current_stop=max(current_stop,locked) if direction=='LONG' else min(current_stop,locked)
        if remaining<=1e-9:exit_reason='ALL_TARGETS';break
    if remaining>1e-9:
        exit_rr=(last-entry)*sign/risk;realized+=remaining*exit_rr
    cost_r=(execution_v7.f(policy.get('all_in_cost_bps'),execution_v7.ALL_IN_COST_BPS)/10000.0)*entry/risk;net=realized-cost_r
    return {'filled':True,'pnl_r':net,'gross_r':realized,'cost_r':cost_r,'mfe_r':mfe,'mae_r':mae,'exit_reason':exit_reason,'entry':entry,'stop':stop0,'stop_pct':risk/max(entry,1e-9),'hit_targets':sorted(hit),'regime':opp.get('regime'),'path_timeframe':'5m'}


def stats_without_data_gaps(results: list[dict[str, Any]]) -> dict[str, float]:
    clean = [x for x in results if not x.get('invalid_data')]
    out = _ORIGINAL_STATS(clean)
    out['invalid_data_paths'] = len(results) - len(clean)
    out['valid_path_rate'] = len(clean) / max(len(results), 1)
    return out


def install() -> None:
    execution_v7.simulate_policy = simulate_policy_5m
    execution_v7._stats = stats_without_data_gaps
