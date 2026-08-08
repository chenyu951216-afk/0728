from __future__ import annotations

import json
import time
from datetime import datetime
from typing import Any

import adaptive_v5 as signal
import execution_v6 as execution
import v5_runtime

V6_VERSION = '6.0.0-20260808'


def _candidate_execution(core: Any, candidate: dict[str, Any], regime: str) -> dict[str, Any]:
    policy, meta = execution.execution_for_candidate(core, candidate)
    blocked = set(meta.get('blocked_regimes') or [])
    certified = bool(policy and meta.get('certified') and regime not in blocked)
    return {
        'certified': certified,
        'policy': policy,
        'metrics': meta,
        'regime_ok': bool(policy and regime not in blocked),
        'reason': meta.get('reason') if meta else 'no execution Champion for this signal-model version',
    }


def choose_strategy_v6(core: Any, store: Any, learner: Any, features: dict[str, float], regime: dict[str, Any], data_quality: float) -> dict[str, Any]:
    base = signal.choose_strategy(store, learner, features, regime, data_quality)
    candidates: list[dict[str, Any]] = []
    for raw in base.get('candidates') or []:
        c = dict(raw)
        signal_ok = bool(c.get('tradeable'))
        ex = _candidate_execution(core, c, regime['regime']) if c.get('certified') else {'certified': False, 'policy': None, 'metrics': {}, 'regime_ok': False, 'reason': 'signal model not certified'}
        c['signal_tradeable'] = signal_ok
        c['execution'] = ex
        c['tradeable'] = bool(signal_ok and ex['certified'])
        if c['tradeable']:
            ev = float((ex.get('metrics') or {}).get('expectancy_r') or 0)
            pf = float((ex.get('metrics') or {}).get('profit_factor') or 0)
            c['final_score'] = float(c.get('score') or 0) + min(.10, max(0.0, ev) * .08) + min(.05, max(0.0, pf - 1) * .025)
        else:
            c['final_score'] = float(c.get('score') or 0)
        candidates.append(c)
    candidates.sort(key=lambda x: x.get('final_score', 0), reverse=True)
    tradeable = [x for x in candidates if x.get('tradeable')]
    certified_signal = [x for x in candidates if x.get('certified')]
    if tradeable:
        selected = tradeable[0]
        reason = 'signal Champion + exact execution Champion both passed OOS and current regime gates'
    elif certified_signal:
        selected = certified_signal[0]
        selected = {**selected, 'tradeable': False}
        reason = 'signal Champion exists, but no matching OOS-certified execution policy is currently eligible'
    else:
        selected = candidates[0] if candidates else base
        selected = {**selected, 'tradeable': False}
        reason = 'no direction-specific signal Champion is certified yet'
    research = candidates[0] if candidates else base.get('research_best')
    return {
        **selected,
        'tradeable': bool(selected.get('tradeable')),
        'certified': bool(selected.get('certified')),
        'research_best': research,
        'tradeable_candidates': tradeable[:5],
        'certified_candidates': certified_signal[:8],
        'candidates': candidates,
        'reason': reason,
        'validation_stack': 'SIGNAL_OOS + EXECUTION_OOS',
    }


def create_signal_v6(core: Any, analysis: dict[str, Any], m15: list[dict[str, Any]]) -> dict[str, Any] | None:
    selection = analysis['selection']
    if not selection.get('tradeable'):
        return None
    current = core.latest_signal()
    if current:
        return current
    ex = selection.get('execution') or {}
    policy = ex.get('policy')
    metrics = ex.get('metrics') or {}
    if not policy or not ex.get('certified'):
        return None
    plan = execution.plan_from_policy(selection['strategy'], selection['direction'], float(analysis['price']), m15, policy)
    plan['execution_validation'] = {
        'certified': True,
        'execution_version': metrics.get('execution_version'),
        'model_version': metrics.get('model_version'),
        'oos_pf': metrics.get('profit_factor'),
        'oos_ev_r': metrics.get('expectancy_r'),
        'oos_win_rate': metrics.get('win_rate'),
        'oos_fills': metrics.get('oos_fills'),
        'fill_rate': metrics.get('fill_rate'),
        'max_drawdown_r': metrics.get('max_drawdown_r'),
        'estimated_all_in_cost_bps': metrics.get('estimated_all_in_cost_bps'),
    }
    now = int(time.time())
    signal_id = f"{now}-{selection['strategy'][:4]}-{selection['direction'][0]}"
    payload = {
        'initial_plan': plan,
        'selection': selection,
        'regime': analysis['regime'],
        'features': analysis.get('features', {}),
        'data_quality': float((analysis.get('data_quality') or {}).get('score', 0)),
        'created_from_snapshot': analysis.get('snapshot_ts'),
        'immutable': True,
        'model_schema_version': 2,
        'execution_schema_version': execution.EXECUTION_SCHEMA,
        'execution_policy': policy,
        'execution_validation': plan['execution_validation'],
        'management': {
            'hit_targets': [],
            'mfe_r': 0.0,
            'mae_r': 0.0,
            'remaining_fraction': 1.0,
            'realized_partial_r': 0.0,
            'trail_reason': None,
        },
    }
    con = core.db()
    con.execute(
        'INSERT INTO signals(signal_id,created_at,updated_at,status,strategy,direction,regime,phase,probability,entry,initial_stop,current_stop,targets,payload) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
        (signal_id, now, now, 'PLANNED', selection['strategy'], selection['direction'], analysis['regime']['regime'], analysis['regime']['phase'], selection['probability'], plan['entry'], plan['stop'], plan['stop'], json.dumps(plan['targets']), json.dumps(payload, ensure_ascii=False)),
    )
    con.commit()
    con.close()
    return core.latest_signal()


def _infer_partial(row: dict[str, Any], payload: dict[str, Any]) -> tuple[float, float]:
    mgmt = payload.setdefault('management', {})
    if 'remaining_fraction' in mgmt and 'realized_partial_r' in mgmt:
        return float(mgmt['remaining_fraction']), float(mgmt['realized_partial_r'])
    hit = set(mgmt.get('hit_targets') or [])
    remaining = 1.0
    realized = 0.0
    for idx, target in enumerate(row.get('targets') or []):
        if idx not in hit:
            continue
        frac = min(remaining, float(target.get('allocation') or 0) / 100.0)
        realized += frac * float(target.get('rr') or 0)
        remaining -= frac
    mgmt['remaining_fraction'] = max(0.0, remaining)
    mgmt['realized_partial_r'] = realized
    return max(0.0, remaining), realized


def close_signal_v6(core: Any, row: dict[str, Any], price: float, reason: str, ts: int) -> None:
    entry = float(row['entry'])
    stop0 = float(row['initial_stop'])
    sign = 1 if row['direction'] == 'LONG' else -1
    risk = abs(entry - stop0) or 1e-9
    payload = row['payload'] if isinstance(row.get('payload'), dict) else json.loads(row['payload'])
    if isinstance(row.get('targets'), str):
        row['targets'] = json.loads(row['targets'])
    remaining, partial = _infer_partial(row, payload)
    exit_rr = (float(price) - entry) * sign / risk
    gross = partial + remaining * exit_rr
    policy = payload.get('execution_policy') or {}
    cost_bps = float(policy.get('all_in_cost_bps') or (0.0 if payload.get('legacy_execution_plan') else execution.ALL_IN_COST_BPS))
    cost_r = (cost_bps / 10000.0) * entry / risk if cost_bps > 0 else 0.0
    net = gross - cost_r
    mgmt = payload.setdefault('management', {})
    mgmt.update({
        'closed_reason': reason,
        'remaining_fraction': 0.0,
        'realized_partial_r': partial,
        'final_exit_rr': exit_rr,
        'gross_realized_r': gross,
        'estimated_cost_r': cost_r,
        'net_realized_r': net,
    })
    con = core.db()
    con.execute(
        "UPDATE signals SET status='CLOSED',updated_at=?,exit_ts=?,exit_price=?,exit_reason=?,realized_r=?,review_until=?,payload=? WHERE signal_id=?",
        (ts, ts, float(price), reason, net, ts + core.POST_EXIT_BARS * 900, json.dumps(payload, ensure_ascii=False), row['signal_id']),
    )
    con.commit()
    con.close()


def update_signal_with_bar_v6(core: Any, bar: dict[str, Any]) -> dict[str, Any] | None:
    row = core.latest_signal()
    if not row:
        return None
    ts = int(bar['ts'])
    entry = float(row['entry'])
    stop0 = float(row['initial_stop'])
    current_stop = float(row['current_stop'])
    direction = row['direction']
    sign = 1 if direction == 'LONG' else -1
    low, high = float(bar['l']), float(bar['h'])
    payload = row['payload']
    targets = row['targets']
    policy = payload.get('execution_policy') or {}
    touched = low <= entry <= high

    if row['status'] == 'PLANNED':
        expire_bars = int(policy.get('expire_bars') or 6)
        if not touched:
            if ts - int(row['created_at']) > expire_bars * 900:
                con = core.db()
                con.execute("UPDATE signals SET status='EXPIRED',updated_at=? WHERE signal_id=?", (ts, row['signal_id']))
                con.commit(); con.close()
                return None
            return row
        row['status'] = 'OPEN'
        row['filled_at'] = ts
        con = core.db()
        con.execute("UPDATE signals SET status='OPEN',filled_at=?,updated_at=? WHERE signal_id=?", (ts, ts, row['signal_id']))
        con.commit(); con.close()

    risk = abs(entry - stop0) or 1e-9
    favorable = (high - entry) / risk if direction == 'LONG' else (entry - low) / risk
    adverse = (entry - low) / risk if direction == 'LONG' else (high - entry) / risk
    mgmt = payload.setdefault('management', {})
    mgmt['mfe_r'] = max(float(mgmt.get('mfe_r', 0)), favorable)
    mgmt['mae_r'] = max(float(mgmt.get('mae_r', 0)), adverse)
    hit_targets = set(mgmt.get('hit_targets') or [])
    remaining, partial = _infer_partial(row, payload)

    # Conservative same-bar ordering, identical to the execution OOS simulator.
    stop_hit = low <= current_stop if direction == 'LONG' else high >= current_stop
    if stop_hit:
        close_signal_v6(core, row, current_stop, 'STOP_OR_TRAIL', ts)
        return None

    for idx, target in enumerate(targets):
        if idx in hit_targets:
            continue
        px = float(target['price'])
        target_hit = high >= px if direction == 'LONG' else low <= px
        if not target_hit:
            continue
        frac = min(remaining, float(target.get('allocation') or 0) / 100.0)
        partial += frac * float(target.get('rr') or 0)
        remaining -= frac
        hit_targets.add(idx)
        mgmt['last_target_hit'] = idx + 1
        mgmt.setdefault('realized_legs', []).append({'target': idx + 1, 'fraction': frac, 'rr': float(target.get('rr') or 0), 'ts': ts})

    mgmt['hit_targets'] = sorted(hit_targets)
    mgmt['remaining_fraction'] = max(0.0, remaining)
    mgmt['realized_partial_r'] = partial
    new_stop = current_stop
    plan_mgmt = (payload.get('initial_plan') or {}).get('management') or {}
    if 0 in hit_targets:
        new_stop = max(new_stop, entry) if direction == 'LONG' else min(new_stop, entry)
        mgmt['trail_reason'] = 'TP1 -> breakeven'
    if 1 in hit_targets:
        lock2 = float(plan_mgmt.get('lock_after_tp2_r') or policy.get('lock_after_tp2_r') or .55)
        locked = entry + sign * lock2 * risk
        new_stop = max(new_stop, locked) if direction == 'LONG' else min(new_stop, locked)
        mgmt['trail_reason'] = f'TP2 -> lock {lock2:.2f}R'
    if 2 in hit_targets:
        lock3 = float(plan_mgmt.get('lock_after_tp3_r') or policy.get('lock_after_tp3_r') or 1.05)
        locked = entry + sign * lock3 * risk
        new_stop = max(new_stop, locked) if direction == 'LONG' else min(new_stop, locked)
        mgmt['trail_reason'] = f'TP3 -> lock {lock3:.2f}R'
    new_stop = max(new_stop, current_stop) if direction == 'LONG' else min(new_stop, current_stop)

    if remaining <= 1e-9:
        close_signal_v6(core, row, float(targets[-1]['price']), 'ALL_TARGETS', ts)
        return None
    con = core.db()
    con.execute('UPDATE signals SET updated_at=?,current_stop=?,payload=? WHERE signal_id=?', (ts, new_stop, json.dumps(payload, ensure_ascii=False), row['signal_id']))
    con.commit(); con.close()
    return core.latest_signal()


def execution_status(core: Any) -> list[dict[str, Any]]:
    con = core.db()
    rows = con.execute("SELECT strategy,direction,model_version,version,status,created_at,metrics,policy FROM execution_registry ORDER BY created_at DESC,version DESC").fetchall()
    con.close()
    out = []
    for r in rows:
        out.append({
            'strategy': r[0], 'direction': r[1], 'model_version': r[2], 'execution_version': r[3],
            'status': r[4], 'created_at': r[5], 'metrics': json.loads(r[6]), 'policy': json.loads(r[7]),
        })
    return out


def migrate(core: Any) -> None:
    con = core.db()
    execution.ExecutionStore(con)
    if core.get_state('v6_execution_migration') != execution.EXECUTION_SCHEMA:
        planned = con.execute("SELECT signal_id,payload FROM signals WHERE status='PLANNED'").fetchall()
        for r in planned:
            payload = json.loads(r[1])
            payload['superseded_reason'] = 'v6 requires a matching OOS-certified execution policy'
            con.execute("UPDATE signals SET status='EXPIRED',updated_at=?,payload=? WHERE signal_id=?", (int(time.time()), json.dumps(payload, ensure_ascii=False), r[0]))
        opened = con.execute("SELECT signal_id,payload FROM signals WHERE status='OPEN'").fetchall()
        for r in opened:
            payload = json.loads(r[1])
            payload['legacy_execution_plan'] = True
            payload.setdefault('management', {})['legacy_note'] = 'plan preserved at v6 migration; new signals require execution OOS certification'
            con.execute('UPDATE signals SET payload=? WHERE signal_id=?', (json.dumps(payload, ensure_ascii=False), r[0]))
        con.commit()
        core.set_state('v6_execution_migration', execution.EXECUTION_SCHEMA)
    con.close()


async def notify_execution_results(core: Any, results: list[dict[str, Any]]) -> None:
    for x in results:
        if x.get('status') != 'CHAMPION':
            continue
        await v5_runtime.robust_send_discord(
            core,
            f"🧭 Execution Champion｜{x['strategy']} {x['direction']}",
            f"實際 Entry/SL/TP/分批/BE/trailing 已用未見歷史資料驗證。\n"
            f"Execution OOS PF `{float(x.get('profit_factor') or 0):.2f}`｜EV `{float(x.get('expectancy_r') or 0):+.3f}R`｜勝率 `{float(x.get('win_rate') or 0):.1%}`\n"
            f"OOS fills `{int(x.get('oos_fills') or 0)}`｜fill rate `{float(x.get('fill_rate') or 0):.1%}`｜DD `{float(x.get('max_drawdown_r') or 0):.1f}R`\n"
            f"已包含估計 all-in 成本 `{float(x.get('estimated_all_in_cost_bps') or 0):.1f} bps`。只有 Signal + Execution 雙 Champion 才能發正式訊號。",
            0x2ECC71,
        )


def optimize_execution(core: Any, force: bool = False) -> list[dict[str, Any]]:
    results = execution.optimize_all(core, force=force)
    core.state['execution_learning'] = {
        'version': V6_VERSION,
        'results': results,
        'registry': execution_status(core)[:40],
        'updated_at': datetime.now(core.timezone.utc).isoformat(),
    }
    return results


def install(core: Any) -> None:
    migrate(core)

    def chooser(store: Any, learner: Any, features: dict[str, float], regime: dict[str, Any], data_quality: float) -> dict[str, Any]:
        return choose_strategy_v6(core, store, learner, features, regime, data_quality)

    core.choose_strategy = chooser
    core.create_signal = lambda analysis, m15: create_signal_v6(core, analysis, m15)
    core.update_signal_with_bar = lambda bar: update_signal_with_bar_v6(core, bar)
    core._close_signal = lambda row, price, reason, ts: close_signal_v6(core, row, price, reason, ts)
    core.optimize_execution = lambda force=False: optimize_execution(core, force)
    core.app.version = '6.0.0'
    core.state['runtime_version'] = V6_VERSION

    if not any(getattr(route, 'path', None) == '/api/v6/execution' for route in core.app.router.routes):
        @core.app.get('/api/v6/execution')
        def api_execution() -> dict[str, Any]:
            return {'runtime': V6_VERSION, 'registry': execution_status(core), 'state': core.state.get('execution_learning', {})}

    if not any(getattr(route, 'path', None) == '/api/v6/execution/train' for route in core.app.router.routes):
        @core.app.post('/api/v6/execution/train')
        async def api_execution_train() -> dict[str, Any]:
            import asyncio
            results = await asyncio.to_thread(optimize_execution, core, True)
            await notify_execution_results(core, results)
            return {'runtime': V6_VERSION, 'results': results}
