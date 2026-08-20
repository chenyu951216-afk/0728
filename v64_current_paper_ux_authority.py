from __future__ import annotations

"""V64 current-paper UX/lifecycle authority.

V63 remains the score authority: every historical/live score input has a fixed cap and
simultaneously-qualified strategies are ranked by current-time fit.  V64 makes the
runtime legible in Chinese, keeps completed strategies + the current signal/position at
the top of both dashboards, and corrects the opposite-direction lifecycle for an
unfilled LIMIT setup (cancel it; never book a fictitious realised exit).

Paper/research only.  Historical OOS is never rewritten and no future feature is used.
"""

import json
import time
from typing import Any, Callable

import runtime_identity

VERSION = 'V64_CURRENT_PAPER_CHINESE_UX_LIFECYCLE'
SCHEMA = 64
STATE_KEY = 'v64_current_paper_chinese_ux_lifecycle'

FEATURE_ZH = {
    'ret_1': '15分鐘報酬', 'ret_4': '1小時報酬', 'ret_16': '4小時報酬',
    'ema20_gap': '價格與 EMA20 距離', 'ema50_gap': '價格與 EMA50 距離',
    'ema20_slope': 'EMA20 斜率', 'atr_pct': 'ATR 波動率', 'atr_rank': 'ATR 波動排名',
    'adx': 'ADX 趨勢強度', 'rsi': 'RSI', 'volume_z': '成交量 Z 分數',
    'range_z': 'K棒振幅 Z 分數', 'wick_ratio': '影線比例', 'dist_vwap_atr': '價格距 VWAP（ATR）',
    'bos_up': '向上 BOS', 'bos_down': '向下 BOS', 'sweep_low': '掃低點流動性',
    'sweep_high': '掃高點流動性', 'fvg_up': '多方 FVG', 'fvg_down': '空方 FVG',
    'btc_ret_4': 'BTC 1小時報酬', 'btc_ret_16': 'BTC 4小時報酬', 'eth_btc_rel': 'ETH/BTC 相對強弱',
    'spot_perp_basis_bps': '現貨/永續基差', 'funding': '資金費率', 'oi_change': '未平倉量變化',
    'book_imbalance': '訂單簿買賣失衡', 'liquidation_imbalance': '清算方向失衡',
    'liquidation_intensity': '清算強度', 'oi_weighted_funding': 'OI 加權資金費率',
    'taker_imbalance': '主動買賣失衡', 'crowd_skew': '市場多空偏斜', 'top_position_skew': '大戶部位偏斜',
    'oi_available': 'OI 資料可用性', 'funding_available': '資金費率資料可用性',
    'liquidation_available': '清算資料可用性', 'book_available': '訂單簿資料可用性',
    'oi_weighted_funding_available': 'OI加權資金費率可用性', 'taker_available': 'Taker 資料可用性',
    'crowd_available': '市場多空資料可用性', 'top_position_available': '大戶部位資料可用性',
    'derivative_coverage': '衍生品資料覆蓋率', 'derivative_quality': '衍生品資料品質',
    'source_agreement_bps': '多來源價格一致度', 'hour_sin': '時段週期 sin', 'hour_cos': '時段週期 cos',
    'dow_sin': '星期週期 sin', 'dow_cos': '星期週期 cos', 'daily_adx_norm': '日線 ADX 標準化',
    'h4_adx_norm': '4H ADX 標準化', 'h1_adx_norm': '1H ADX 標準化',
    'daily_slope_norm': '日線斜率標準化', 'h4_slope_norm': '4H 斜率標準化',
    'h1_slope_norm': '1H 斜率標準化', 'macro_volatility_rank': '大週期波動排名',
    'macro_atr_pct': '大週期 ATR 波動率',
}

_INSTALLED = False


def _now() -> int:
    return int(time.time())


def _f(value: Any, default: float = 0.0) -> float:
    try:
        x = float(value)
        return x if x == x and abs(x) != float('inf') else default
    except (TypeError, ValueError):
        return default


def _d(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _state(core: Any, **patch: Any) -> dict[str, Any]:
    z = _d(core.state.get(STATE_KEY)); z.update(patch)
    z.update({'schema': SCHEMA, 'runtime': VERSION,
              'public_runtime': runtime_identity.RUNTIME_VERSION, 'updated_at': _now()})
    core.state[STATE_KEY] = z
    return z


def feature_label(name: str) -> str:
    return FEATURE_ZH.get(str(name), str(name))


def gate_failures_zh(features: dict[str, Any], thresholds: list[dict[str, Any]]) -> list[str]:
    out: list[str] = []
    for c in thresholds:
        name = str(c.get('feature') or '')
        op = str(c.get('op') or 'GE').upper()
        required = _f(c.get('value')); actual = _f(features.get(name))
        ok = actual <= required if op == 'LE' else actual >= required
        if not ok:
            sign = '≤' if op == 'LE' else '≥'
            out.append(f'{feature_label(name)}（{name}）目前 {actual:.5g}，策略要求 {sign} {required:.5g}')
    return out


def reason_zh(text: Any) -> str:
    s = str(text or '')
    replacements = (
        ('AI State Gate：', '市場狀態條件：'),
        ('Pred EV', '預測期望值'), ('Required', '最低要求'),
        ('Forward 表現惡化，策略已 quarantine', '目前時間的前向表現惡化，策略已暫停使用'),
        ('模型無法即時預測', '即時模型無法完成預測'),
        ('OOD ', '目前市場偏離歷史分布 OOD '),
        ('同時有多套策略合格', '同時有多套策略符合進場條件'),
        ('目前已有同方向策略持倉/掛單', '目前已有同方向策略持倉或掛單'),
        ('反向策略競爭落後', '反方向策略分數較低'),
        ('雖為反方向但分數', '雖然是反方向策略，但分數'),
    )
    for a, b in replacements:
        s = s.replace(a, b)
    return s


def _row_by_id(core: Any, signal_id: str) -> dict[str, Any] | None:
    con = core.db()
    try:
        row = con.execute('SELECT * FROM signals WHERE signal_id=?', (str(signal_id),)).fetchone()
    finally:
        con.close()
    if not row:
        return None
    z = dict(row)
    try: z['targets'] = json.loads(z['targets']) if isinstance(z.get('targets'), str) else list(z.get('targets') or [])
    except Exception: z['targets'] = []
    try: z['payload'] = json.loads(z['payload']) if isinstance(z.get('payload'), str) else _d(z.get('payload'))
    except Exception: z['payload'] = {}
    return z


def _cancel_planned(core: Any, row: dict[str, Any], reason: str) -> dict[str, Any]:
    ts = _now(); payload = _d(row.get('payload'))
    payload['cancel_reason'] = str(reason); payload['cancelled_at'] = ts
    con = core.db()
    try:
        con.execute("UPDATE signals SET status='CANCELLED',updated_at=?,exit_ts=?,exit_reason=?,payload=? WHERE signal_id=?",
                    (ts, ts, str(reason), json.dumps(payload, ensure_ascii=False, default=str), row['signal_id']))
        con.commit()
    finally:
        con.close()
    return _row_by_id(core, str(row['signal_id'])) or row


def _corrected_update(core: Any, autonomous: Any, v56: Any, v63: Any,
                      base_update: Callable[[dict[str, Any]], Any]):
    """Use canonical V56 management, V63 notifications, and correct reversal semantics."""
    def update(bar: dict[str, Any]) -> Any:
        before = core.latest_signal(); bid = str(before.get('signal_id')) if before else ''
        before_hits = set(_d(_d(before.get('payload')).get('management')).get('hit_targets') or []) if before else set()
        result = base_update(bar)

        if bid:
            after = v63._signal_by_id(core, bid)
            if after:
                if str(before.get('status')) == 'PLANNED' and str(after.get('status')) == 'OPEN':
                    v63._enqueue(core, bid + ':filled', bid, 'FILLED', 'ETH Paper 掛單已成交',
                                 f"策略：{after.get('strategy')}｜{after.get('direction')}\n實際進場：{_f(after.get('entry')):.4f}\n目前止損：{_f(after.get('current_stop')):.4f}", 5025616)
                if str(after.get('status')) == 'CLOSED' and str(before.get('status')) in {'PLANNED', 'OPEN'}:
                    title, body, color = v63._exit_message(after)
                    v63._enqueue(core, bid + ':closed:' + str(after.get('exit_reason')), bid, 'EXIT', title, body, color)
                elif str(after.get('status')) in {'EXPIRED', 'CANCELLED'} and str(before.get('status')) == 'PLANNED':
                    v63._enqueue(core, bid + ':cancelled', bid, 'CANCEL', 'ETH Paper 掛單取消',
                                 f"策略：{after.get('strategy')}｜{after.get('direction')}\n原進場：{_f(after.get('entry')):.4f}\n原因：{reason_zh(after.get('exit_reason') or after.get('status'))}", 16753920)
                else:
                    after_hits = set(_d(_d(after.get('payload')).get('management')).get('hit_targets') or [])
                    new_hits = sorted(after_hits - before_hits)
                    if new_hits:
                        v63._enqueue(core, bid + ':tp:' + ','.join(map(str, new_hits)), bid, 'PARTIAL_TP',
                                     'ETH Paper 部分止盈',
                                     f"策略：{after.get('strategy')}｜{after.get('direction')}\n新命中 TP：{','.join(str(i+1) for i in new_hits)}\n目前止損：{_f(after.get('current_stop')):.4f}", 5763719)

        active = core.latest_signal(); analysis = _d(core.state.get('v63_current_analysis')); sel = _d(analysis.get('selection'))
        if active and sel.get('tradeable') and sel.get('v63_reversal_authorized') and str(sel.get('direction')) != str(active.get('direction')):
            sid = str(active.get('signal_id'))
            if str(active.get('status')) == 'PLANNED':
                cancelled = _cancel_planned(core, active, 'AUTONOMOUS_OPPOSITE_STRATEGY_REPLACED')
                v63._enqueue(core, sid + ':opposite-cancel', sid, 'CANCEL', 'ETH Paper 原反向掛單取消',
                             f"原策略：{active.get('strategy')}｜{active.get('direction')}\n原進場：{_f(active.get('entry')):.4f}\n原因：出現分數明顯更高的反方向策略 {sel.get('strategy')}，未成交掛單直接取消，不計損益。", 16753920)
                _state(core, planned_reversal_handled='CANCEL_NOT_FAKE_EXIT', cancelled_signal=sid,
                       next_strategy=sel.get('strategy'))
                return core.latest_signal()
            payload = _d(active.get('payload')); mgmt = _d(payload.get('management'))
            v56._finalize_signal(core, active, _f(analysis.get('price'), _f(active.get('entry'))),
                                 'AUTONOMOUS_DATA_REVERSAL', _now(), mgmt, payload)
            closed = v63._signal_by_id(core, sid) or active
            title, body, color = v63._exit_message(closed)
            v63._enqueue(core, sid + ':data-reversal', sid, 'DATA_EXIT', title, body, color)
            _state(core, position_lock='OPPOSITE_REVERSAL_RELEASED', previous_strategy=active.get('strategy'),
                   next_strategy=sel.get('strategy'), data_exit_notification_enqueued=True)
            return core.latest_signal()
        return result
    return update


def _strategy_summary(item: dict[str, Any], v63: Any, live: dict[str, Any] | None,
                      active: dict[str, Any] | None) -> dict[str, Any]:
    metrics = _d(item.get('metrics')); score = v63.historical_score(metrics); live = _d(live)
    sid = str(item.get('strategy_id')); active_here = bool(active and str(active.get('strategy')) == sid)
    waits = [reason_zh(x) for x in list(live.get('wait_reasons') or [])]
    if active_here:
        why = '目前就是持倉/掛單中的策略；同方向其他策略暫不重複進場。'
    elif bool(live.get('tradeable')):
        why = '目前符合進場條件，且是本輪分數仲裁選出的最佳策略。'
    elif waits:
        why = '；'.join(waits)
    else:
        why = reason_zh(live.get('reason') or '目前尚未符合這套策略的即時進場條件。')
    win = metrics.get('win_rate')
    if win is None: win = metrics.get('oos_win_rate')
    if win is None: win = metrics.get('win')
    return {
        'strategy_id': sid, 'direction': item.get('direction'), 'behavior_label': item.get('behavior_label'),
        'tier': metrics.get('certification_tier') or ('STRICT' if str(item.get('status')) == 'CHAMPION' else item.get('status')),
        'historical_score': score.get('score_total'), 'historical_score_components': score.get('components'),
        'historical_score_caps': score.get('component_caps'), 'live_score': live.get('v63_live_score'),
        'live_score_components': _d(live.get('v63_score')).get('components'),
        'profit_factor': metrics.get('profit_factor'), 'expectancy_r': metrics.get('expectancy_r'),
        'win_rate': win, 'oos_fills': metrics.get('oos_fills'), 'max_drawdown_r': metrics.get('max_drawdown_r'),
        'bootstrap_ci05_r': metrics.get('bootstrap_ci05_r'), 'stability': metrics.get('stability'),
        'profitable_folds': metrics.get('profitable_folds'), 'worst_fold_ev': metrics.get('worst_fold_ev'),
        'predicted_ev_r': live.get('predicted_ev_r'), 'required_ev_r': live.get('required_ev_r'),
        'edge_r': live.get('edge_r'), 'ood_fraction': live.get('ood_fraction'), 'qualified': bool(live.get('qualified')),
        'tradeable': bool(live.get('tradeable')), 'active_position': active_here, 'why_not_enter_zh': why,
    }


def _overview(core: Any, autonomous: Any, v63: Any) -> dict[str, Any]:
    analysis = _d(core.state.get('v63_current_analysis')); diagnostics = {
        str(x.get('strategy')): x for x in list(analysis.get('strategy_diagnostics') or [])
    }
    active = core.latest_signal(); completed = []
    for item in autonomous._load_registry(core, active_only=True) or []:
        completed.append(_strategy_summary(item, v63, diagnostics.get(str(item.get('strategy_id'))), active))
    completed.sort(key=lambda x: (bool(x.get('active_position')), _f(x.get('live_score'), -1),
                                  _f(x.get('historical_score'), -1)), reverse=True)
    current = None
    if active:
        payload = _d(active.get('payload')); sel = _d(payload.get('selection'))
        current = {
            'signal_id': active.get('signal_id'), 'strategy': active.get('strategy'), 'direction': active.get('direction'),
            'status': active.get('status'), 'entry': active.get('entry'), 'initial_stop': active.get('initial_stop'),
            'current_stop': active.get('current_stop'), 'targets': active.get('targets'),
            'predicted_ev_r': sel.get('predicted_ev_r'), 'required_ev_r': sel.get('required_ev_r', sel.get('threshold')),
            'edge_r': sel.get('edge_r'), 'live_score': _d(sel.get('v63_score')).get('score_total', sel.get('v63_live_score')),
            'paper_notional_usdt': payload.get('paper_notional_usdt'), 'selected_leverage': payload.get('selected_leverage'),
        }
    return {
        'schema': SCHEMA, 'runtime': VERSION, 'current_signal_position': current,
        'arbiter': analysis.get('champion_arbitration') or {}, 'completed_strategies': completed,
        'score_policy': {'historical_caps': dict(v63.HIST_CAPS), 'historical_min': v63.HIST_MIN_SCORE,
                         'live_caps': dict(v63.LIVE_CAPS), 'live_min': v63.LIVE_MIN_SCORE,
                         'reversal_score_margin': v63.REVERSAL_SCORE_MARGIN},
        'rules': {'completed_strategies_and_position_at_top': True, 'all_nonentry_reasons_in_chinese': True,
                  'one_same_direction_position_until_exit': True, 'opposite_strategy_may_reverse_only_if_score_wins': True,
                  'unfilled_opposite_replacement_is_cancel_not_exit': True,
                  'discord_entry_has_entry_sl_tp': True, 'discord_data_exit_is_separate': True,
                  'every_score_component_has_fixed_cap': True, 'paper_only': True, 'future_peeking_enabled': False},
        'updated_at': _now(),
    }


def _inject(html: str) -> str:
    if 'v64-top-overview' in html:
        return html
    card = '''<section class="card" id="v64-top-overview"><h2>🏆 已完成策略 / 目前訊號與持倉</h2><div id="v64position" class="notice">讀取目前訊號與持倉…</div><div id="v64strategies" style="margin-top:12px"></div><details style="margin-top:10px"><summary>查看分數制固定上限</summary><pre id="v64caps">—</pre></details></section><style>#v63-top-authority{display:none!important}</style>'''
    js = r'''<script id="v64-top-js">(function(){const E=x=>String(x??'—').replace(/[&<>"']/g,s=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[s]));const N=(x,d=2)=>Number.isFinite(Number(x))?Number(x).toFixed(d):'—';const R=x=>Number.isFinite(Number(x))?((Number(x)>=0?'+':'')+Number(x).toFixed(3)+'R'):'—';async function V(){const a=document.getElementById('v64position'),b=document.getElementById('v64strategies'),c=document.getElementById('v64caps');if(!a||!b)return;try{const r=await fetch('/api/v64/overview',{cache:'no-store'}),z=await r.json(),p=z.current_signal_position||{},arb=z.arbiter||{};a.className='notice '+(p.strategy?'g':'y');a.innerHTML=p.strategy?'<b>目前 '+E(p.status)+'｜'+E(p.strategy)+'｜'+E(p.direction)+'</b><br>進場 '+E(p.entry)+'｜初始 SL '+E(p.initial_stop)+'｜目前 SL '+E(p.current_stop)+'<br>TP '+E((p.targets||[]).map((x,i)=>'TP'+(i+1)+' '+x.price+' ('+N(x.allocation,1)+'%)').join(' / '))+'<br>當下分數 '+N(p.live_score,1)+'/100｜Pred '+R(p.predicted_ev_r)+'｜Required '+R(p.required_ev_r)+'｜Edge '+R(p.edge_r)+'｜名目 '+N(p.paper_notional_usdt,0)+'U｜槓桿 '+N(p.selected_leverage,2)+'x':'<b>目前沒有持倉或掛單</b><br>仲裁狀態：'+E(arb.status||'WAIT');const rows=z.completed_strategies||[];b.innerHTML='<h3 style="margin:12px 0 6px">已完成並保存的策略</h3>'+(rows.length?rows.map((x,i)=>'<div class="notice '+(x.active_position||x.tradeable?'g':'y')+'" style="margin:8px 0"><b>#'+(i+1)+' '+E(x.strategy_id)+' · '+E(x.direction)+' · '+E(x.tier)+'</b>'+(x.active_position?'　<strong>🟢 目前持倉/掛單</strong>':'')+'<br>歷史分數 '+N(x.historical_score,1)+'/100｜當下分數 '+N(x.live_score,1)+'/100｜PF '+N(x.profit_factor,2)+'｜EV '+R(x.expectancy_r)+'｜fills '+E(x.oos_fills)+'｜DD '+R(x.max_drawdown_r)+'<br>CI05 '+R(x.bootstrap_ci05_r)+'｜WF '+N(x.stability,3)+'｜正EV folds '+(Number.isFinite(Number(x.profitable_folds))?(Number(x.profitable_folds)*100).toFixed(1)+'%':'—')+'｜worst '+R(x.worst_fold_ev)+'<br>Pred '+R(x.predicted_ev_r)+'｜Required '+R(x.required_ev_r)+'｜Edge '+R(x.edge_r)+'｜OOD '+(Number.isFinite(Number(x.ood_fraction))?(Number(x.ood_fraction)*100).toFixed(1)+'%':'—')+'<br><span style="color:#c8d3e6"><b>目前'+(x.tradeable?'可進場':'不進場')+'：</b>'+E(x.why_not_enter_zh)+'</span></div>').join(''):'尚無已保存策略');if(c)c.textContent=JSON.stringify(z.score_policy,null,2)}catch(e){a.className='notice r';a.textContent='V64 讀取失敗：'+String(e)}}V();setInterval(()=>{if(!document.hidden)V()},8000)})();</script>'''
    if '<main>' in html: html = html.replace('<main>', '<main>' + card, 1)
    elif '</body>' in html: html = html.replace('</body>', card + '</body>', 1)
    else: html += card
    return html.replace('</body>', js + '</body>', 1) if '</body>' in html else html + js


def _wrap_html(core: Any, path: str) -> None:
    route = next((r for r in core.app.router.routes if getattr(r, 'path', None) == path and 'GET' in (getattr(r, 'methods', set()) or set())), None)
    old = getattr(route, 'endpoint', None)
    if not callable(old):
        return
    from fastapi.responses import HTMLResponse
    core.app.router.routes = [r for r in core.app.router.routes if getattr(r, 'path', None) != path]
    def endpoint() -> HTMLResponse:
        raw = old(); html = raw.body.decode('utf-8', errors='replace') if hasattr(raw, 'body') else str(raw)
        return HTMLResponse(_inject(html), headers={'Cache-Control': 'no-store,max-age=0', 'X-ETH-Adaptive-UX': VERSION})
    core.app.add_api_route(path, endpoint, methods=['GET'], response_class=HTMLResponse,
                           name='v64_' + ('root' if path == '/' else 'full'))


def install(production: Any, autonomous: Any, v56: Any, v63: Any) -> None:
    global _INSTALLED
    if _INSTALLED:
        return
    _INSTALLED = True; core = production.core

    # V63 analysis dynamically resolves this helper, so all Gate failures become readable
    # Chinese without changing the model/genome or its thresholds.
    v63._gate_failures = gate_failures_zh

    # Replace only V63's outer update wrapper.  The semantic base is still the V57/V56
    # canonical update hook; historical execution and OOS remain untouched.
    base_update = autonomous._autonomous_update_signal
    core.update_signal_with_bar = _corrected_update(core, autonomous, v56, v63, base_update)

    core.app.router.routes = [r for r in core.app.router.routes if getattr(r, 'path', None) != '/api/v64/overview']
    core.app.add_api_route('/api/v64/overview', lambda: _overview(core, autonomous, v63), methods=['GET'], name='v64_overview')
    _wrap_html(core, '/'); _wrap_html(core, '/dashboard/full')

    _state(core, status='READY', score_authority=str(getattr(v63, 'VERSION', 'V63')),
           all_strategy_wait_reasons_chinese=True, completed_strategies_at_top=True,
           current_signal_position_at_top=True, planned_opposite_replacement_cancelled_without_fake_pnl=True,
           discord_entry_sl_tp=True, discord_data_exit_separate=True, same_direction_position_lock=True,
           strict_historical_oos_changed=False, historical_oos_rewritten=False,
           current_paper_execution_semantics_changed=False, future_peeking_enabled=False, paper_only=True)
    role = core.state.get('bootstrap_replica_role')
    if isinstance(role, dict):
        role.update({'final_runtime_overlay': VERSION, 'production_entry': 'server_entry_v64.py',
                     'current_paper_ux_authority': VERSION, 'score_authority': getattr(v63, 'VERSION', 'V63'),
                     'all_strategy_wait_reasons_chinese': True, 'strategies_and_position_at_top': True,
                     'unfilled_reversal_is_cancel_not_exit': True, 'discord_data_exit_separate': True,
                     'strict_historical_oos_rewritten_by_v64': False})
    runtime_identity.stamp(core)
