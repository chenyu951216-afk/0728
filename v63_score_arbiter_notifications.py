from __future__ import annotations

"""V63 capped-score provisional ranking, live arbiter, top UI and Discord lifecycle.

Strict historical 9/9 certification remains unchanged. V63 adds a PAPER-ONLY score tier
whose inputs each have a fixed contribution cap, then uses a second capped score in
current time to select the best currently-qualified strategy. One ETH setup/position is
allowed at a time. Same-direction alternatives are suppressed until exit. A materially
stronger opposite-direction strategy may cancel/reverse the current paper position.
"""

import json
import math
import os
import pickle
import time
from typing import Any, Callable

import numpy as np
import runtime_identity

VERSION = 'V63_CAPPED_SCORE_ARBITER_DISCORD_LIFECYCLE'
SCHEMA = 63
STATE_KEY = 'v63_capped_score_arbiter_notifications'
TIER = 'PROVISIONAL_SCORE_PAPER'
OUTBOX_TABLE = 'v63_discord_outbox'

# Fixed caps sum to 100. No single historical metric can dominate admission.
HIST_CAPS = {
    'profit_factor': 18.0,
    'expectancy_r': 18.0,
    'bootstrap_ci05_r': 12.0,
    'wf_stability': 14.0,
    'profitable_folds': 10.0,
    'worst_fold_ev_r': 10.0,
    'drawdown_efficiency': 10.0,
    'sample_size': 8.0,
}
HIST_MIN_SCORE = max(40.0, min(85.0, float(os.getenv('AUTONOMOUS_V63_MIN_PROVISIONAL_SCORE', '55'))))
MAX_PROVISIONALS = max(1, min(8, int(os.getenv('AUTONOMOUS_V63_MAX_PROVISIONALS', '5'))))
HARD_MIN_FILLS = max(20, int(os.getenv('AUTONOMOUS_V63_HARD_MIN_FILLS', '30')))
HARD_MIN_PF = max(1.0, float(os.getenv('AUTONOMOUS_V63_HARD_MIN_PF', '1.05')))
HARD_MIN_EV_R = max(0.0, float(os.getenv('AUTONOMOUS_V63_HARD_MIN_EV_R', '0.0')))
HARD_MIN_CI05_R = max(-.40, float(os.getenv('AUTONOMOUS_V63_HARD_MIN_CI05_R', '-.15')))
HARD_MIN_WORST_FOLD_EV_R = max(-.50, float(os.getenv('AUTONOMOUS_V63_HARD_MIN_WORST_FOLD_EV_R', '-.30')))
HARD_MAX_DD_R = max(20.0, float(os.getenv('AUTONOMOUS_V63_HARD_MAX_DD_R', '100.0')))

# Current-time caps also sum to 100.
LIVE_CAPS = {
    'current_edge_r': 40.0,
    'ood_fit': 15.0,
    'data_quality': 10.0,
    'historical_quality': 30.0,
    'forward_health': 5.0,
}
LIVE_MIN_SCORE = max(35.0, min(90.0, float(os.getenv('AUTONOMOUS_V63_LIVE_MIN_SCORE', '55'))))
REVERSAL_SCORE_MARGIN = max(0.0, min(30.0, float(os.getenv('AUTONOMOUS_V63_REVERSAL_SCORE_MARGIN', '7.5'))))

_INSTALLED = False
_ORIGINAL_SEND: Callable[..., Any] | None = None


def _now() -> int:
    return int(time.time())


def _f(v: Any, default: float = 0.0) -> float:
    try:
        x = float(v)
        return x if math.isfinite(x) else default
    except (TypeError, ValueError):
        return default


def _d(v: Any) -> dict[str, Any]:
    return dict(v) if isinstance(v, dict) else {}


def _clamp(v: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, float(v)))


def _norm(v: float, lo: float, hi: float) -> float:
    return 0.0 if hi <= lo else _clamp((float(v) - lo) / (hi - lo))


def _state(core: Any, **patch: Any) -> dict[str, Any]:
    z = _d(core.state.get(STATE_KEY)); z.update(patch)
    z.update({'schema': SCHEMA, 'runtime': VERSION, 'public_runtime': runtime_identity.RUNTIME_VERSION,
              'updated_at': _now()})
    core.state[STATE_KEY] = z
    return z


def historical_score(metrics: dict[str, Any]) -> dict[str, Any]:
    fills = max(0, int(metrics.get('oos_fills') or 0)); pf = _f(metrics.get('profit_factor'))
    ev = _f(metrics.get('expectancy_r')); ci = _f(metrics.get('bootstrap_ci05_r'), -.5)
    stability = _clamp(_f(metrics.get('stability'))); profitable = _clamp(_f(metrics.get('profitable_folds')))
    worst = _f(metrics.get('worst_fold_ev'), -.5); dd = max(0.0, _f(metrics.get('max_drawdown_r'), 1e9))
    total = _f(metrics.get('total_oos_r'), ev * fills)
    ret_dd = total / max(dd, 1e-9) if total > 0 and dd > 0 else 0.0
    raw = {
        'profit_factor': HIST_CAPS['profit_factor'] * _norm(pf, 1.0, 1.8),
        'expectancy_r': HIST_CAPS['expectancy_r'] * _norm(ev, 0.0, .30),
        'bootstrap_ci05_r': HIST_CAPS['bootstrap_ci05_r'] * _norm(ci, -.15, .12),
        'wf_stability': HIST_CAPS['wf_stability'] * stability,
        'profitable_folds': HIST_CAPS['profitable_folds'] * profitable,
        'worst_fold_ev_r': HIST_CAPS['worst_fold_ev_r'] * _norm(worst, -.30, .10),
        'drawdown_efficiency': HIST_CAPS['drawdown_efficiency'] * (
            .55 * _norm(ret_dd, .50, 3.0) + .45 * (1.0 - _norm(dd, 0.0, 80.0))),
        'sample_size': HIST_CAPS['sample_size'] * _clamp(fills / 80.0),
    }
    comp = {k: round(min(HIST_CAPS[k], max(0.0, _f(v))), 4) for k, v in raw.items()}
    hard = {
        'fills': fills >= HARD_MIN_FILLS,
        'profit_factor': pf >= HARD_MIN_PF,
        'expectancy_positive': ev > HARD_MIN_EV_R,
        'ci05_floor': ci >= HARD_MIN_CI05_R,
        'worst_fold_floor': worst >= HARD_MIN_WORST_FOLD_EV_R,
        'drawdown_hard_ceiling': dd <= HARD_MAX_DD_R,
        'invalid_future_paths_zero': int(metrics.get('invalid_future_paths') or 0) == 0,
    }
    score = round(min(100.0, sum(comp.values())), 4)
    return {'score_total': score, 'score_threshold': HIST_MIN_SCORE, 'components': comp,
            'component_caps': dict(HIST_CAPS), 'hard_checks': hard,
            'hard_checks_passed': all(hard.values()), 'return_to_drawdown': ret_dd,
            'eligible': bool(all(hard.values()) and score >= HIST_MIN_SCORE)}


def score_eligible(metrics: dict[str, Any], autonomous: Any) -> tuple[bool, dict[str, Any]]:
    s = historical_score(metrics); failed: list[str] = []
    if int(metrics.get('oos_fills') or 0) < int(autonomous.MIN_OOS_FILLS): failed.append('OOS fills')
    if _f(metrics.get('profit_factor')) < float(autonomous.MIN_OOS_PF): failed.append('OOS PF')
    if _f(metrics.get('expectancy_r')) < float(autonomous.MIN_OOS_EV_R): failed.append('OOS EV')
    if _f(metrics.get('max_drawdown_r'), 1e9) > float(autonomous.MAX_OOS_DD_R): failed.append('OOS DD')
    if _f(metrics.get('bootstrap_ci05_r'), -1e9) <= float(autonomous.MIN_BOOTSTRAP_CI05): failed.append('Bootstrap CI05')
    if int(metrics.get('invalid_future_paths') or 0) != 0: failed.append('Invalid future paths')
    if _f(metrics.get('stability')) < float(autonomous.MIN_WF_STABILITY): failed.append('WF stability')
    if _f(metrics.get('profitable_folds')) < float(autonomous.MIN_PROFITABLE_FOLDS): failed.append('Profitable folds')
    if _f(metrics.get('worst_fold_ev'), -1e9) < float(autonomous.MIN_WORST_FOLD_EV): failed.append('Worst fold EV')
    why = {**s, 'failed_strict_gates': failed, 'paper_only': True,
           'strict_historical_certified': False, 'selection_after_oos_visibility': True,
           'scoring_policy': 'FIXED_CAP_100_POINT', 'pf_alone_can_dominate': False}
    return bool(s['eligible']), why


def _score_rejected_rows(v61: Any, pipeline52: Any):
    def rows(core: Any, autonomous: Any, _p52: Any) -> list[dict[str, Any]]:
        run = v61._latest_run(core, pipeline52)
        if not run: return []
        pipeline52._ensure(core); con = core.db()
        try:
            raw = con.execute(f'''SELECT rank,finalist_id,direction,genome,development,audit
                FROM {pipeline52.VAULT_TABLE} WHERE run_id=? AND selected_finalist=1 AND audit IS NOT NULL
                ORDER BY COALESCE(rank,999999)''', (run,)).fetchall()
        finally: con.close()
        out = []
        for rank, fid, direction, genome_raw, dev_raw, audit_raw in raw:
            audit = v61._j(audit_raw, {}); metrics = _d(audit.get('metrics'))
            if audit.get('promoted'): continue
            ok, rationale = score_eligible(metrics, autonomous)
            if ok:
                out.append({'rank': int(rank or 0), 'finalist_id': str(fid or ''), 'direction': str(direction or ''),
                            'genome': v61._j(genome_raw, {}), 'development': v61._j(dev_raw, {}),
                            'audit': audit, 'metrics': metrics, 'rationale': rationale})
        out.sort(key=lambda x: (_f(x['rationale'].get('score_total')), _f(x['metrics'].get('expectancy_r')),
                                _f(x['metrics'].get('profit_factor'))), reverse=True)
        return out[:MAX_PROVISIONALS]
    return rows


def _strict_nonprovisional(core: Any, autonomous: Any) -> list[dict[str, Any]]:
    out = []
    for item in autonomous._load_registry(core, active_only=True) or []:
        tier = str(_d(item.get('metrics')).get('certification_tier') or '')
        if not tier.startswith('PROVISIONAL_'):
            out.append(item)
    return out


def _cleanup_legacy_provisionals(core: Any, autonomous: Any) -> None:
    active = core.latest_signal(); active_sid = str(active.get('strategy')) if active else ''
    con = core.db()
    try:
        rows = con.execute(f'''SELECT strategy_id,metrics FROM {autonomous.REGISTRY_TABLE}
                               WHERE active=1''').fetchall()
        for sid, raw in rows:
            try: m = json.loads(raw) if isinstance(raw, str) else dict(raw or {})
            except Exception: m = {}
            tier = str(m.get('certification_tier') or '')
            if tier.startswith('PROVISIONAL_') and tier != TIER and str(sid) != active_sid:
                con.execute(f"UPDATE {autonomous.REGISTRY_TABLE} SET active=0,status='RETIRED_PROVISIONAL' WHERE strategy_id=?", (sid,))
        con.commit()
    finally: con.close()


def _gate_failures(features: dict[str, Any], thresholds: list[dict[str, Any]]) -> list[str]:
    out = []
    for c in thresholds:
        name = str(c.get('feature')); op = str(c.get('op') or 'GE').upper(); req = _f(c.get('value'))
        actual = _f(features.get(name))
        ok = actual <= req if op == 'LE' else actual >= req
        if not ok:
            out.append(f'{name}={actual:.5g} 不符合 {"≤" if op == "LE" else "≥"} {req:.5g}')
    return out


def _live_score(candidate: dict[str, Any], quality: float, max_ood: float) -> dict[str, Any]:
    edge = _f(candidate.get('edge_r')); ood = max(0.0, _f(candidate.get('ood_fraction')))
    hist = historical_score(_d(candidate.get('metrics'))); q = _clamp(quality / 100.0 if quality > 1.0 else quality)
    ood_fit = 1.0 - _clamp(ood / max(max_ood, 1e-9))
    raw = {
        'current_edge_r': LIVE_CAPS['current_edge_r'] * _norm(edge, 0.0, .40),
        'ood_fit': LIVE_CAPS['ood_fit'] * ood_fit,
        'data_quality': LIVE_CAPS['data_quality'] * q,
        'historical_quality': LIVE_CAPS['historical_quality'] * _clamp(_f(hist.get('score_total')) / 100.0),
        'forward_health': 0.0 if candidate.get('quarantined') else LIVE_CAPS['forward_health'],
    }
    comp = {k: round(min(LIVE_CAPS[k], max(0.0, _f(v))), 4) for k, v in raw.items()}
    return {'score_total': round(min(100.0, sum(comp.values())), 4), 'score_threshold': LIVE_MIN_SCORE,
            'components': comp, 'component_caps': dict(LIVE_CAPS), 'historical_score': hist['score_total'],
            'edge_r': edge}


def _evaluate_all(core: Any, autonomous: Any, z: dict[str, Any]) -> list[dict[str, Any]]:
    features = _d(z.get('features')); quality = _f(_d(z.get('data_quality')).get('score'))
    max_ood = float(getattr(autonomous, 'LIVE_MAX_OOD_FRACTION', .35))
    qstate = _d(core.get_state('v56_champion_quarantine', {})); out = []
    for item in autonomous._load_registry(core, active_only=True) or []:
        genome = _d(item.get('genome')); metrics = _d(item.get('metrics')); sid = str(item.get('strategy_id'))
        waits: list[str] = []; gates = list(metrics.get('gate_thresholds') or [])
        gf = _gate_failures(features, gates)
        if gf: waits += ['AI State Gate：' + x for x in gf]
        ood = autonomous._ood_fraction(features, metrics)
        if ood > max_ood: waits.append(f'OOD {ood:.1%} > 上限 {max_ood:.1%}')
        quarantined = bool(_d(qstate.get(sid)).get('active'))
        if quarantined: waits.append('Forward 表現惡化，策略已 quarantine')
        pred = None; threshold = max(_f(metrics.get('direct_r_threshold')), float(autonomous.LIVE_MIN_PREDICTED_EV_R))
        if not gf and ood <= max_ood and not quarantined:
            try:
                model = pickle.loads(item.get('model_blob'))
                vec = np.asarray([[_f(features.get(n)) for n in genome.get('feature_names', [])]], dtype=np.float32)
                pred = float(model.predict(vec)[0])
                if pred < threshold: waits.append(f'Pred EV {pred:+.3f}R < Required {threshold:+.3f}R，差 {threshold-pred:.3f}R')
            except Exception as exc:
                waits.append(f'模型無法即時預測：{type(exc).__name__}')
        edge = (_f(pred) - threshold) if pred is not None else -999.0
        base_ok = bool(pred is not None and pred >= threshold and not gf and ood <= max_ood and not quarantined)
        c = {'strategy': sid, 'direction': genome.get('direction'), 'behavior_label': item.get('behavior_label'),
             'genome': genome, 'metrics': metrics, 'predicted_ev_r': pred, 'required_ev_r': threshold,
             'threshold': threshold, 'edge_r': edge, 'ood_fraction': ood, 'quarantined': quarantined,
             'base_tradeable': base_ok, 'wait_reasons': waits, 'certification_tier': metrics.get('certification_tier') or 'STRICT'}
        score = _live_score(c, quality, max_ood); c['v63_score'] = score; c['v63_live_score'] = score['score_total']
        if base_ok and score['score_total'] < LIVE_MIN_SCORE:
            c['wait_reasons'].append(f'V63 當下分數 {score["score_total"]:.1f} < 最低 {LIVE_MIN_SCORE:.1f}')
        c['qualified'] = bool(base_ok and score['score_total'] >= LIVE_MIN_SCORE)
        c['tradeable'] = c['qualified']; c['reason'] = '符合目前進場條件' if c['qualified'] else ('；'.join(c['wait_reasons']) or '目前不符合進場條件')
        out.append(c)
    out.sort(key=lambda x: (_f(x.get('v63_live_score'), -999), _f(x.get('edge_r'), -999)), reverse=True)
    return out


def _analysis_wrapper(core: Any, autonomous: Any, base: Callable[[dict[str, Any]], dict[str, Any]]):
    def analysis(bundle: dict[str, Any]) -> dict[str, Any]:
        z = dict(base(bundle) or {}); candidates = _evaluate_all(core, autonomous, z)
        active = core.latest_signal(); qualified = [x for x in candidates if x.get('qualified')]
        selected: dict[str, Any] | None = None; arb: dict[str, Any]
        if not active:
            if qualified:
                selected = qualified[0]
                for x in qualified[1:]:
                    x['tradeable'] = False
                    x['wait_reasons'].append(f'同時有多套策略合格，但分數 {x["v63_live_score"]:.1f} < 最佳 {selected["v63_live_score"]:.1f}')
                    x['reason'] = '；'.join(x['wait_reasons'])
                arb = {'status': 'V63_SCORE_SELECTED', 'selected': selected['strategy'], 'score': selected['v63_live_score'],
                       'qualified_count': len(qualified), 'rule': '同時合格時取當下總分最高者'}
            else:
                selected = candidates[0] if candidates else {'strategy': 'WAIT', 'direction': 'NONE', 'tradeable': False}
                arb = {'status': 'WAIT', 'qualified_count': 0}
        else:
            active_dir = str(active.get('direction')); active_sid = str(active.get('strategy'))
            amap = {str(x.get('strategy')): x for x in candidates}; ac = amap.get(active_sid)
            active_score = _f(ac.get('v63_live_score')) if ac and ac.get('qualified') else 0.0
            for x in candidates:
                if str(x.get('direction')) == active_dir:
                    x['tradeable'] = False
                    x['wait_reasons'].append('目前已有同方向策略持倉/掛單，出場前不重複通知或進場')
                    x['reason'] = '；'.join(x['wait_reasons'])
            opp = [x for x in candidates if x.get('qualified') and str(x.get('direction')) != active_dir]
            opp.sort(key=lambda x: (_f(x.get('v63_live_score')), _f(x.get('edge_r'))), reverse=True)
            if opp and (not ac or not ac.get('qualified') or _f(opp[0].get('v63_live_score')) >= active_score + REVERSAL_SCORE_MARGIN):
                selected = opp[0]; selected['tradeable'] = True; selected['v63_reversal_authorized'] = True
                for x in opp[1:]:
                    x['tradeable'] = False; x['wait_reasons'].append(f'反向策略競爭落後：{x["v63_live_score"]:.1f} < {selected["v63_live_score"]:.1f}')
                    x['reason'] = '；'.join(x['wait_reasons'])
                arb = {'status': 'V63_OPPOSITE_REVERSAL_SELECTED', 'selected': selected['strategy'],
                       'score': selected['v63_live_score'], 'active_strategy': active_sid, 'active_score': active_score,
                       'required_margin': REVERSAL_SCORE_MARGIN}
            else:
                if opp:
                    for x in opp:
                        x['tradeable'] = False
                        x['wait_reasons'].append(f'雖為反方向但分數 {x["v63_live_score"]:.1f} < 目前策略 {active_score:.1f} + 反轉門檻 {REVERSAL_SCORE_MARGIN:.1f}')
                        x['reason'] = '；'.join(x['wait_reasons'])
                selected = ac if ac else (candidates[0] if candidates else {'strategy': active_sid, 'direction': active_dir})
                selected = dict(selected); selected['tradeable'] = False
                selected['reason'] = '目前已有持倉/掛單，等待原策略出場；只有足夠強的反方向策略可提前反轉'
                arb = {'status': 'POSITION_LOCK', 'active_strategy': active_sid, 'active_direction': active_dir,
                       'active_score': active_score, 'required_reversal_margin': REVERSAL_SCORE_MARGIN}
        z['autonomous_candidates'] = candidates; z['strategy_diagnostics'] = candidates; z['selection'] = selected
        z['champion_arbitration'] = arb; z['trade_label'] = 'AUTONOMOUS V63 SCORE TRADE' if selected.get('tradeable') else 'WAIT / V63 SCORE'
        z['v63_scoring'] = {'historical_caps': HIST_CAPS, 'live_caps': LIVE_CAPS,
                            'historical_min_score': HIST_MIN_SCORE, 'live_min_score': LIVE_MIN_SCORE}
        core.state['v63_current_analysis'] = z; _state(core, last_selection=arb)
        return z
    return analysis


def _ensure_outbox(core: Any) -> None:
    con = core.db()
    try:
        con.execute(f'''CREATE TABLE IF NOT EXISTS {OUTBOX_TABLE}(
            event_key TEXT PRIMARY KEY, signal_id TEXT, event TEXT NOT NULL, title TEXT NOT NULL,
            body TEXT NOT NULL, color INTEGER NOT NULL, status TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0,
            last_error TEXT, created_at INTEGER NOT NULL, sent_at INTEGER)'''); con.commit()
    finally: con.close()


def _enqueue(core: Any, key: str, signal_id: str, event: str, title: str, body: str, color: int) -> None:
    _ensure_outbox(core); con = core.db()
    try:
        con.execute(f'''INSERT OR IGNORE INTO {OUTBOX_TABLE}
            (event_key,signal_id,event,title,body,color,status,created_at) VALUES(?,?,?,?,?,?,?,?)''',
            (key, signal_id, event, title, body[:3900], int(color), 'PENDING', _now())); con.commit()
    finally: con.close()


async def _flush_outbox(core: Any) -> None:
    if not callable(_ORIGINAL_SEND): return
    _ensure_outbox(core); con = core.db()
    try:
        rows = con.execute(f'''SELECT event_key,title,body,color,attempts FROM {OUTBOX_TABLE}
                               WHERE status='PENDING' ORDER BY created_at LIMIT 12''').fetchall()
    finally: con.close()
    for key, title, body, color, attempts in rows:
        try:
            await _ORIGINAL_SEND(str(title), str(body), int(color)); con = core.db()
            try:
                con.execute(f"UPDATE {OUTBOX_TABLE} SET status='SENT',sent_at=?,attempts=attempts+1,last_error=NULL WHERE event_key=?", (_now(), key)); con.commit()
            finally: con.close()
        except Exception as exc:
            con = core.db()
            try:
                con.execute(f"UPDATE {OUTBOX_TABLE} SET attempts=?,last_error=? WHERE event_key=?", (int(attempts or 0)+1, f'{type(exc).__name__}: {exc}', key)); con.commit()
            finally: con.close()
            _state(core, last_discord_error=f'{type(exc).__name__}: {exc}'); break


def _signal_by_id(core: Any, sid: str) -> dict[str, Any] | None:
    con = core.db()
    try: row = con.execute('SELECT * FROM signals WHERE signal_id=?', (sid,)).fetchone()
    finally: con.close()
    if not row: return None
    z = dict(row)
    try: z['targets'] = json.loads(z['targets']) if isinstance(z.get('targets'), str) else list(z.get('targets') or [])
    except Exception: z['targets'] = []
    try: z['payload'] = json.loads(z['payload']) if isinstance(z.get('payload'), str) else _d(z.get('payload'))
    except Exception: z['payload'] = {}
    return z


def _pnl_u(row: dict[str, Any]) -> float | None:
    if row.get('realized_r') is None: return None
    e, s = _f(row.get('entry')), _f(row.get('initial_stop')); p = _d(row.get('payload')); n = _f(p.get('paper_notional_usdt'))
    return None if e <= 0 or s <= 0 or n <= 0 else _f(row.get('realized_r')) * abs(e-s)/e * n


def _entry_message(core: Any) -> tuple[str, str, int, str] | None:
    row = core.latest_signal()
    if not row: return None
    p = _d(row.get('payload')); sel = _d(p.get('selection')); mg = _d(p.get('management')); ts = list(row.get('targets') or [])
    tps = '\n'.join(f"TP{i}: {_f(t.get('price')):.4f} | {_f(t.get('rr')):.2f}R | {_f(t.get('allocation')):.1f}%" for i,t in enumerate(ts,1)) or 'TP: —'
    score = _f(_d(sel.get('v63_score')).get('score_total'), _f(sel.get('v63_live_score')))
    mode = 'MARKET' if mg.get('entry_market') else 'LIMIT'
    body = (f"策略: {row.get('strategy')} | {row.get('direction')} | {mode}\n"
            f"進場點: {_f(row.get('entry')):.4f}\n止損 SL: {_f(row.get('initial_stop')):.4f}\n{tps}\n"
            f"當下分數: {score:.1f}/100 | Pred EV {_f(sel.get('predicted_ev_r')):+.3f}R | Required {_f(sel.get('required_ev_r',sel.get('threshold'))):+.3f}R | Edge {_f(sel.get('edge_r')):+.3f}R\n"
            f"名目: {_f(p.get('paper_notional_usdt')):.0f} USDT | 安全槓桿: {_f(p.get('selected_leverage')):.2f}x | 最大持有 {int(mg.get('max_hold_bars') or 0)*.25:.1f}h\nPaper only")
    title = 'ETH Paper 即時進場' if str(row.get('status')) == 'OPEN' else 'ETH Paper 掛單策略'
    return title, body, 5025616, str(row.get('signal_id')) + ':created'


def _exit_message(row: dict[str, Any]) -> tuple[str, str, int]:
    reason = str(row.get('exit_reason') or 'UNKNOWN'); pnl = _pnl_u(row)
    title = 'ETH Paper 資料反轉出場' if 'DATA' in reason else ('ETH Paper 止盈完成' if 'TARGET' in reason else ('ETH Paper 止損/移動止損出場' if ('STOP' in reason or 'TRAIL' in reason) else 'ETH Paper 策略出場'))
    body = (f"策略: {row.get('strategy')} | {row.get('direction')}\n進場: {_f(row.get('entry')):.4f} | 出場: {_f(row.get('exit_price')):.4f}\n"
            f"結果: {_f(row.get('realized_r')):+.3f}R" + (f" | 約 {pnl:+.2f} USDT" if pnl is not None else '') + f"\n原因: {reason}\nPaper only")
    return title, body, 5763719 if _f(row.get('realized_r')) >= 0 else 15548997


def _update_wrapper(core: Any, v56: Any, base: Callable[[dict[str, Any]], Any]):
    def update(bar: dict[str, Any]) -> Any:
        before = core.latest_signal(); bid = str(before.get('signal_id')) if before else ''
        before_hits = set(_d(_d(before.get('payload')).get('management')).get('hit_targets') or []) if before else set()
        result = base(bar)
        if bid:
            after = _signal_by_id(core, bid)
            if after:
                if str(before.get('status')) == 'PLANNED' and str(after.get('status')) == 'OPEN':
                    _enqueue(core, bid+':filled', bid, 'FILLED', 'ETH Paper 掛單已成交', f"策略: {after.get('strategy')} | {after.get('direction')}\n實際進場: {_f(after.get('entry')):.4f}\nSL: {_f(after.get('current_stop')):.4f}", 5025616)
                if str(after.get('status')) == 'CLOSED' and str(before.get('status')) in {'PLANNED','OPEN'}:
                    t,b,c = _exit_message(after); _enqueue(core, bid+':closed:'+str(after.get('exit_reason')), bid, 'EXIT', t,b,c)
                elif str(after.get('status')) in {'EXPIRED','CANCELLED'} and str(before.get('status')) == 'PLANNED':
                    _enqueue(core, bid+':cancelled', bid, 'CANCEL', 'ETH Paper 掛單取消', f"策略: {after.get('strategy')} | {after.get('direction')}\n原進場: {_f(after.get('entry')):.4f}\n原因: {after.get('exit_reason') or after.get('status')}", 16753920)
                else:
                    ah = set(_d(_d(after.get('payload')).get('management')).get('hit_targets') or [])
                    new_hits = sorted(ah-before_hits)
                    if new_hits:
                        _enqueue(core, bid+':tp:'+','.join(map(str,new_hits)), bid, 'PARTIAL_TP', 'ETH Paper 部分止盈', f"策略: {after.get('strategy')} | {after.get('direction')}\n新命中 TP: {','.join(str(i+1) for i in new_hits)}\n目前 Stop: {_f(after.get('current_stop')):.4f}", 5763719)
        active = core.latest_signal(); analysis = _d(core.state.get('v63_current_analysis')); sel = _d(analysis.get('selection'))
        if active and sel.get('tradeable') and sel.get('v63_reversal_authorized') and str(sel.get('direction')) != str(active.get('direction')):
            sid = str(active.get('signal_id'))
            if str(active.get('status')) == 'PLANNED':
                con = core.db()
                try:
                    con.execute("UPDATE signals SET status='CANCELLED',updated_at=?,exit_reason=? WHERE signal_id=?", (_now(),'AUTONOMOUS_DATA_REVERSAL_CANCEL',sid)); con.commit()
                finally: con.close()
                _enqueue(core, sid+':data-cancel', sid, 'DATA_CANCEL', 'ETH Paper 反向策略取消原掛單', f"原策略: {active.get('strategy')} {active.get('direction')}\n新策略: {sel.get('strategy')} {sel.get('direction')}\n原因: 反方向策略當下分數明顯更高", 16753920)
            else:
                payload = _d(active.get('payload')); mg = _d(payload.get('management'))
                v56._finalize_signal(core, active, _f(analysis.get('price'), _f(active.get('entry'))), 'AUTONOMOUS_DATA_REVERSAL', _now(), mg, payload)
                closed = _signal_by_id(core, sid) or active; t,b,c = _exit_message(closed)
                _enqueue(core, sid+':data-reversal', sid, 'DATA_EXIT', t,b,c)
            _state(core, position_lock='OPPOSITE_REVERSAL_RELEASED', previous_strategy=active.get('strategy'), next_strategy=sel.get('strategy'))
            return core.latest_signal()
        return result
    return update


def _send_wrapper(core: Any, base_send: Callable[..., Any]):
    async def send(title: str, body: str, color: int=6000633) -> None:
        if title == 'ETH learned setup created':
            d = _entry_message(core)
            if d:
                t,b,c,key = d; row = core.latest_signal(); _enqueue(core,key,str(row.get('signal_id')),'ENTRY',t,b,c); await _flush_outbox(core); return
        await base_send(title, body, color)
    return send


def _scan_wrapper(core: Any, base_scan: Callable[..., Any]):
    async def scan() -> dict[str, Any]:
        try: return await base_scan()
        finally: await _flush_outbox(core)
    return scan


def _api(core: Any, autonomous: Any) -> dict[str, Any]:
    a = _d(core.state.get('v63_current_analysis')); active = core.latest_signal(); rows = []
    for x in list(a.get('strategy_diagnostics') or []):
        rows.append({'strategy': x.get('strategy'),'direction':x.get('direction'),'tier':x.get('certification_tier'),
                     'historical_score':_f(_d(x.get('v63_score')).get('historical_score')),
                     'live_score':_f(x.get('v63_live_score')),'qualified':bool(x.get('qualified')),'tradeable':bool(x.get('tradeable')),
                     'predicted_ev_r':x.get('predicted_ev_r'),'required_ev_r':x.get('required_ev_r'),'edge_r':x.get('edge_r'),
                     'ood_fraction':x.get('ood_fraction'),'wait_reasons':list(x.get('wait_reasons') or []),'reason':x.get('reason')})
    act = None
    if active:
        act = {'signal_id':active.get('signal_id'),'strategy':active.get('strategy'),'direction':active.get('direction'),'status':active.get('status'),
               'entry':active.get('entry'),'initial_stop':active.get('initial_stop'),'current_stop':active.get('current_stop'),'targets':active.get('targets')}
    return {'schema':SCHEMA,'runtime':VERSION,'tier':TIER,
            'historical_score_policy':{'caps':HIST_CAPS,'sum_caps':sum(HIST_CAPS.values()),'minimum':HIST_MIN_SCORE,'max_provisionals':MAX_PROVISIONALS},
            'live_score_policy':{'caps':LIVE_CAPS,'sum_caps':sum(LIVE_CAPS.values()),'minimum':LIVE_MIN_SCORE,'reversal_margin':REVERSAL_SCORE_MARGIN},
            'arbiter':a.get('champion_arbitration') or {},'active_signal':act,'strategies':rows,
            'rules':{'every_score_input_has_fixed_cap':True,'pf_alone_can_dominate':False,'all_nonentry_strategies_show_reasons':True,
                     'simultaneous_qualified_choose_highest_current_score':True,'one_same_direction_position_until_exit':True,
                     'opposite_direction_may_reverse_with_margin':True,'discord_entry_contains_entry_sl_tp':True,
                     'discord_data_exit_is_separate':True,'paper_only':True,'future_peeking_enabled':False},'updated_at':_now()}


def _inject(html: str) -> str:
    if 'v63-top-authority' in html: return html
    card = '''<section class="card" id="v63-top-authority"><h2>🎯 目前訊號 / 策略分數仲裁</h2><div id="v63active" class="notice">讀取目前持倉…</div><div id="v63strategies" style="margin-top:12px"></div></section>'''
    js = r'''<script id="v63-top-js">(function(){const E=x=>String(x??'—').replace(/[&<>"']/g,s=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[s]));const R=x=>Number.isFinite(Number(x))?((Number(x)>=0?'+':'')+Number(x).toFixed(3)+'R'):'—';async function V(){const a=document.getElementById('v63active'),b=document.getElementById('v63strategies');if(!a||!b)return;try{const r=await fetch('/api/v63/score-authority',{cache:'no-store'}),z=await r.json(),p=z.active_signal||{},arb=z.arbiter||{};a.className='notice '+(p.strategy?'g':'y');a.innerHTML=p.strategy?'<b>目前 '+E(p.status)+'</b> · '+E(p.strategy)+' '+E(p.direction)+'<br>Entry '+E(p.entry)+' · SL '+E(p.current_stop)+' · TP '+E((p.targets||[]).map(x=>x.price).join(' / '))+'<br>Arbiter '+E(arb.status||'POSITION_LOCK'):'<b>目前無持倉/掛單</b><br>Arbiter '+E(arb.status||'WAIT');const rows=z.strategies||[];b.innerHTML=rows.length?rows.map((x,i)=>'<div class="notice '+(x.tradeable?'g':'y')+'" style="margin:8px 0"><b>#'+(i+1)+' '+E(x.strategy)+' · '+E(x.direction)+'</b>｜當下 '+Number(x.live_score||0).toFixed(1)+'/100｜歷史 '+Number(x.historical_score||0).toFixed(1)+'/100<br>Pred '+R(x.predicted_ev_r)+'｜Required '+R(x.required_ev_r)+'｜Edge '+R(x.edge_r)+'｜OOD '+(Number(x.ood_fraction||0)*100).toFixed(1)+'%<br><span style="color:#9fb0c8">'+E(x.tradeable?'✅ 本輪最佳，可進場':((x.wait_reasons||[]).length?(x.wait_reasons||[]).join('；'):x.reason||'目前不進場'))+'</span></div>').join(''):'尚無已保存策略';}catch(e){a.className='notice r';a.textContent='V63 讀取失敗：'+String(e)}}V();setInterval(()=>{if(!document.hidden)V()},8000)})();</script>'''
    if '<main>' in html: html = html.replace('<main>','<main>'+card,1)
    elif '</body>' in html: html = html.replace('</body>',card+'</body>',1)
    else: html += card
    return html.replace('</body>',js+'</body>',1) if '</body>' in html else html+js


def _wrap_html(core: Any, path: str) -> None:
    route = next((r for r in core.app.router.routes if getattr(r,'path',None)==path and 'GET' in (getattr(r,'methods',set()) or set())),None)
    old = getattr(route,'endpoint',None)
    if not callable(old): return
    from fastapi.responses import HTMLResponse
    core.app.router.routes = [r for r in core.app.router.routes if getattr(r,'path',None)!=path]
    def endpoint() -> HTMLResponse:
        raw = old(); html = raw.body.decode('utf-8',errors='replace') if hasattr(raw,'body') else str(raw)
        return HTMLResponse(_inject(html),headers={'Cache-Control':'no-store,max-age=0','X-ETH-Adaptive-Score':VERSION})
    core.app.add_api_route(path,endpoint,methods=['GET'],response_class=HTMLResponse,name='v63_'+('root' if path=='/' else 'full'))


def install(production: Any, autonomous: Any, pipeline52: Any, pipeline: Any, v56: Any, v61: Any, v62: Any) -> None:
    global _INSTALLED, _ORIGINAL_SEND
    if _INSTALLED: return
    _INSTALLED = True; core = production.core; _ensure_outbox(core)

    # Configure V62/V61 before their worker starts.
    v62.TIER = TIER; v62.MAX_PROVISIONALS = MAX_PROVISIONALS; v62.eligible = score_eligible
    v61.TIER = TIER; v61.MAX_PROVISIONALS = MAX_PROVISIONALS; v61._eligible = score_eligible
    v61._rejected_rows = _score_rejected_rows(v61, pipeline52); v61._strict_champions = _strict_nonprovisional
    base_worker = v61._worker
    def worker(c: Any, a: Any, p52: Any) -> None:
        base_worker(c,a,p52); _cleanup_legacy_provisionals(c,a)
    v61._worker = worker
    v62.install(production, autonomous, pipeline52, pipeline, v56, v61)

    base_analysis = core._analysis_from_bundle; core._analysis_from_bundle = _analysis_wrapper(core, autonomous, base_analysis)
    autonomous._autonomous_analysis = lambda *args,**kwargs: core._analysis_from_bundle(kwargs.get('bundle') if 'bundle' in kwargs else args[-1])
    base_update = core.update_signal_with_bar; core.update_signal_with_bar = _update_wrapper(core, v56, base_update)
    _ORIGINAL_SEND = core.send_discord; core.send_discord = _send_wrapper(core, _ORIGINAL_SEND)
    base_scan = core.scan; core.scan = _scan_wrapper(core, base_scan)

    core.app.router.routes = [r for r in core.app.router.routes if getattr(r,'path',None)!='/api/v63/score-authority']
    core.app.add_api_route('/api/v63/score-authority',lambda:_api(core,autonomous),methods=['GET'],name='v63_score_authority')
    _wrap_html(core,'/'); _wrap_html(core,'/dashboard/full')
    _state(core,status='READY',tier=TIER,historical_caps=HIST_CAPS,live_caps=LIVE_CAPS,
           historical_min_score=HIST_MIN_SCORE,live_min_score=LIVE_MIN_SCORE,max_provisionals=MAX_PROVISIONALS,
           same_direction_position_lock=True,opposite_reversal_margin=REVERSAL_SCORE_MARGIN,
           all_nonentry_reasons_visible=True,discord_trade_lifecycle=True,paper_only=True,
           strict_historical_oos_changed=False,historical_oos_rewritten=False,future_peeking_enabled=False)
    role = core.state.get('bootstrap_replica_role')
    if isinstance(role,dict):
        role.update({'final_runtime_overlay':VERSION,'production_entry':'server_entry_v63.py','score_authority':VERSION,
                     'fixed_component_caps':True,'all_nonentry_reasons_visible':True,'strategies_and_position_at_top':True,
                     'single_active_same_direction_position':True,'opposite_direction_score_reversal':True,
                     'discord_trade_lifecycle_notifications':True,'strict_historical_oos_rewritten_by_v63':False})
    runtime_identity.stamp(core)
