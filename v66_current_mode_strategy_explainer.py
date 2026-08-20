from __future__ import annotations

"""V66 forced Current-Time handoff and strategy-explainer authority.

V56 remains execution/forward-learning authority; V63 remains score authority.
V66 only adds a sticky current-time handoff guard and makes every persisted strategy
legible at the top of the dashboard. Historical OOS/model semantics are unchanged.
"""

import math
import time
from typing import Any, Callable

import runtime_identity

VERSION = "V66_FORCED_CURRENT_TIME_STRATEGY_EXPLAINER"
SCHEMA = 66
STATE_KEY = "v66_forced_current_time_strategy_explainer"
MODE_KEY = "forced_runtime_mode_v66"
_INSTALLED = False


def _now() -> int:
    return int(time.time())


def _d(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _f(value: Any, default: float = 0.0) -> float:
    try:
        x = float(value)
        return x if math.isfinite(x) else default
    except (TypeError, ValueError):
        return default


def _state(core: Any, **patch: Any) -> dict[str, Any]:
    z = _d(core.state.get(STATE_KEY))
    z.update(patch)
    z.update({"schema": SCHEMA, "runtime": VERSION,
              "public_runtime": runtime_identity.RUNTIME_VERSION, "updated_at": _now()})
    core.state[STATE_KEY] = z
    core.state[MODE_KEY] = {
        "mode": z.get("mode"),
        "latched_current_time": bool(z.get("latched_current_time")),
        "historical_restart_suppressed": bool(z.get("historical_restart_suppressed")),
        "updated_at": z["updated_at"],
    }
    return z


def _active_strategies(core: Any, autonomous: Any) -> list[dict[str, Any]]:
    try:
        return list(autonomous._load_registry(core, active_only=True) or [])
    except Exception:
        return []


def refresh_mode(core: Any, autonomous: Any, *, source: str) -> dict[str, Any]:
    """Once terminal history has produced a strategy, never auto-fall back to Stage 6."""
    old = _d(core.state.get(STATE_KEY))
    cp = _d(core.get_state(autonomous.CHECKPOINT_KEY, {}))
    strategies = _active_strategies(core, autonomous)
    terminal = str(cp.get("status") or "") == "COMPLETE"
    latched = bool(old.get("latched_current_time"))
    if not latched and terminal and strategies:
        latched = True

    if latched:
        mode = "CURRENT_TIME_PAPER"
        reason = ("歷史研發/OOS 已完成並保存可執行策略；已強制鎖定現在時間 Paper，"
                  "禁止自動回退或重跑歷史 Stage 6。")
    elif terminal:
        mode = "POST_OOS_WAITING_STRATEGY_PERSIST"
        reason = "歷史 checkpoint 已完成，等待可執行策略保存；不會因此重啟歷史研發。"
    else:
        mode = "HISTORICAL_RESEARCH"
        reason = "歷史研發尚未完成 terminal handoff。"

    return _state(
        core,
        status="READY",
        mode=mode,
        mode_reason_zh=reason,
        latched_current_time=latched,
        forced_current_time=latched,
        historical_checkpoint_complete=terminal,
        historical_checkpoint_status=cp.get("status"),
        active_strategy_count=len(strategies),
        active_strategy_ids=[str(x.get("strategy_id")) for x in strategies],
        historical_restart_suppressed=latched,
        current_scan_must_use_live_bundle=latched,
        discord_signal_lifecycle_required=True,
        no_future_features=True,
        historical_oos_frozen=latched,
        source=str(source),
    )


def _scan_wrapper(core: Any, autonomous: Any, base_scan: Callable[..., Any]):
    async def scan(*args: Any, **kwargs: Any) -> Any:
        refresh_mode(core, autonomous, source="scan_before")
        try:
            return await base_scan(*args, **kwargs)
        finally:
            refresh_mode(core, autonomous, source="scan_after")
    return scan


def _learning_wrapper(core: Any, autonomous: Any, base_tick: Callable[..., Any]):
    async def learning_tick(*args: Any, **kwargs: Any) -> Any:
        mode = refresh_mode(core, autonomous, source="learning_before")
        if mode.get("latched_current_time"):
            strategies = _active_strategies(core, autonomous)
            if not strategies:
                # Sticky latch: temporary registry absence is NOT permission to restart history.
                core.state["learning"] = {
                    **_d(core.state.get("learning")),
                    "phase": "CURRENT_TIME_LATCHED_NO_ACTIVE_STRATEGY",
                    "mode": "CURRENT_TIME_PAPER",
                    "historical_oos_frozen": True,
                    "historical_restart_suppressed": True,
                    "waiting_for_active_strategy": True,
                    "updated_at": _now(),
                }
                _state(core, last_learning_action="SUPPRESS_HISTORICAL_RESTART_NO_ACTIVE_STRATEGY")
                return None

            # This is the already-installed V56 learning tick. With terminal checkpoint
            # + strategy it executes CURRENT_PAPER_FORWARD_LEARNING, not historical Stage 6.
            result = await base_tick(*args, **kwargs)
            core.state["learning"] = {
                **_d(core.state.get("learning")),
                "phase": "CURRENT_TIME_PAPER_FORWARD_LEARNING",
                "mode": "CURRENT_TIME_PAPER",
                "historical_oos_frozen": True,
                "historical_restart_suppressed": True,
                "waiting_for_active_strategy": False,
                "updated_at": _now(),
            }
            refresh_mode(core, autonomous, source="learning_after_current")
            return result

        result = await base_tick(*args, **kwargs)
        refresh_mode(core, autonomous, source="learning_after_historical")
        return result
    return learning_tick


def _feature_category(name: str) -> str:
    n = str(name)
    if n.startswith(("ret_", "ema", "adx", "rsi", "atr_", "range_", "wick_", "dist_vwap")):
        return "價格 / 趨勢 / 波動"
    if n in {"bos_up", "bos_down", "sweep_low", "sweep_high", "fvg_up", "fvg_down"}:
        return "結構 / ICT-SMC"
    if n.startswith("btc_") or n == "eth_btc_rel":
        return "BTC / 跨資產"
    if any(k in n for k in ("funding", "oi_", "book_", "liquidation", "taker_",
                            "crowd_", "position_", "derivative_", "basis")):
        return "衍生品 / 部位 / 訂單流"
    if n.endswith("_available") or "quality" in n or "coverage" in n or "agreement" in n:
        return "資料品質"
    if n.startswith(("hour_", "dow_", "daily_", "h4_", "h1_", "macro_")):
        return "時間 / 大週期"
    if "volume" in n:
        return "成交量"
    return "其他已學特徵"


def _feature_docs(names: list[str], v64: Any) -> tuple[list[dict[str, str]], dict[str, list[str]]]:
    docs: list[dict[str, str]] = []
    groups: dict[str, list[str]] = {}
    for raw in names:
        name = str(raw)
        label = str(v64.feature_label(name))
        category = _feature_category(name)
        docs.append({"name": name, "label_zh": label, "category": category})
        groups.setdefault(category, []).append(f"{label} ({name})")
    return docs, groups


def _gate_docs(thresholds: list[dict[str, Any]], v64: Any) -> list[str]:
    out: list[str] = []
    for g in thresholds:
        name = str(g.get("feature") or "")
        sign = "≤" if str(g.get("op") or "GE").upper() == "LE" else "≥"
        out.append(f"{v64.feature_label(name)} ({name}) {sign} {_f(g.get('value')):.5g}")
    return out


def strategy_explanation(item: dict[str, Any], live: dict[str, Any], v63: Any, v64: Any) -> dict[str, Any]:
    genome = _d(item.get("genome"))
    metrics = _d(item.get("metrics"))
    direction = str(genome.get("direction") or item.get("direction") or "UNKNOWN").upper()
    names = [str(x) for x in (genome.get("feature_names") or metrics.get("feature_names") or [])]
    data_used, groups = _feature_docs(names, v64)
    gates = _gate_docs(list(metrics.get("gate_thresholds") or []), v64)

    stride = max(1, int(genome.get("decision_stride") or 1))
    minutes = 15 * stride
    market = bool(genome.get("entry_market"))
    offset = _f(genome.get("entry_offset_atr"))
    stop_atr = _f(genome.get("stop_atr"))
    rr = [_f(x) for x in (genome.get("target_rr") or [])]
    allocations = [_f(x) for x in (genome.get("allocations") or [])]
    targets = [{"rr": r, "allocation_pct": allocations[i] if i < len(allocations) else None}
               for i, r in enumerate(rr)]
    expiry = max(0, int(genome.get("expire_bars") or 0))
    hold = max(0, int(genome.get("max_hold_bars") or 0))
    be = _f(genome.get("breakeven_after_r"))
    trail_start = _f(genome.get("trail_start_r"))
    trail_lock = _f(genome.get("trail_lock_r"))
    cooldown = max(0, int(genome.get("cooldown_bars") or 0))

    side = "做多" if direction == "LONG" else ("做空" if direction == "SHORT" else direction)
    what = (f"{side}的 AI 自主 Direct-R 完整交易策略。它用已收 K 的市場狀態與保存的 "
            f"{len(names)} 個特徵，預測完整 Entry/SL/TP/管理方案的期望 R；不是單一指標觸發。")
    basis = (f"每 {minutes} 分鐘最多評估一次；先過策略狀態 Gate、OOD、Forward quarantine，"
             f"再要求 Pred EV ≥ Required EV 且 V63 當下總分 ≥ {float(v63.LIVE_MIN_SCORE):.1f}/100。"
             "同時多套合格只取當下分數最高者；已有同方向掛單/持倉不重複進場；"
             f"反方向至少需多 {float(v63.REVERSAL_SCORE_MARGIN):.1f} 分才可替換/反轉。")
    data_summary = ("；".join(f"{k}: " + "、".join(v) for k, v in groups.items())
                    if groups else "模型沒有保存 feature_names。")
    tps = " / ".join(
        f"{x['rr']:.2f}R({x['allocation_pct']:.1f}%)" if x["allocation_pct"] is not None else f"{x['rr']:.2f}R"
        for x in targets
    ) or "未保存"
    execution = (
        ("市價進場" if market else f"LIMIT，決策價偏移 {offset:+.3f} ATR")
        + f"；初始止損 {stop_atr:.3f} ATR；TP {tps}；掛單期限 {expiry * 15} 分鐘；"
          f"最長持有 {hold * 15 / 60:.2f} 小時；保本 {be:.2f}R；"
          f"移動止損 {trail_start:.2f}R 起、鎖 {trail_lock:.2f}R；冷卻 {cooldown * 15} 分鐘。"
    )
    return {
        "strategy_id": str(item.get("strategy_id") or live.get("strategy") or ""),
        "direction": direction,
        "behavior_label": item.get("behavior_label"),
        "what_it_does_zh": what,
        "entry_basis_zh": basis,
        "state_gate_rules_zh": gates,
        "data_used": data_used,
        "data_groups_zh": groups,
        "data_used_summary_zh": data_summary,
        "decision_cadence_minutes": minutes,
        "closed_candle_only": True,
        "execution_plan_zh": execution,
        "execution_plan": {
            "entry_type": "MARKET" if market else "LIMIT",
            "entry_offset_atr": 0.0 if market else offset,
            "stop_atr": stop_atr,
            "targets": targets,
            "expire_bars_15m": expiry,
            "max_hold_bars_15m": hold,
            "breakeven_after_r": be,
            "trail_start_r": trail_start,
            "trail_lock_r": trail_lock,
            "cooldown_bars_15m": cooldown,
            "never_widen_stop": True,
            "initial_plan_immutable": True,
        },
    }


def _authority(core: Any, autonomous: Any, v63: Any, v64: Any) -> dict[str, Any]:
    mode = refresh_mode(core, autonomous, source="api")
    overview = dict(v64._overview(core, autonomous, v63) or {})
    diagnostics = {str(x.get("strategy")): x for x in list(
        _d(core.state.get("v63_current_analysis")).get("strategy_diagnostics") or [])}
    registry = _active_strategies(core, autonomous)
    by_id = {str(x.get("strategy_id")): x for x in registry}
    rows = []
    for base in list(overview.get("completed_strategies") or []):
        sid = str(base.get("strategy_id") or "")
        item = by_id.get(sid, {"strategy_id": sid, "direction": base.get("direction"), "metrics": {}})
        rows.append({**base, **strategy_explanation(item, _d(diagnostics.get(sid)), v63, v64)})
    overview.update({
        "schema": SCHEMA,
        "runtime": VERSION,
        "runtime_mode": mode,
        "completed_strategies": rows,
        "strategy_count": len(rows),
        "score_policy": {
            "historical_caps": dict(v63.HIST_CAPS),
            "historical_cap_total": sum(v63.HIST_CAPS.values()),
            "historical_min": v63.HIST_MIN_SCORE,
            "live_caps": dict(v63.LIVE_CAPS),
            "live_cap_total": sum(v63.LIVE_CAPS.values()),
            "live_min": v63.LIVE_MIN_SCORE,
            "reversal_score_margin": v63.REVERSAL_SCORE_MARGIN,
        },
        "rules": {
            **_d(overview.get("rules")),
            "forced_current_time_after_terminal_strategy": True,
            "current_time_latch_is_sticky": True,
            "historical_restart_after_latch": False,
            "current_scan_uses_live_bundle": True,
            "closed_candle_decisions_only": True,
            "all_strategy_purpose_basis_data_plan_visible": True,
            "all_nonentry_reasons_visible": True,
            "simultaneous_qualified_choose_highest_current_score": True,
            "one_same_direction_position_until_exit": True,
            "stronger_opposite_may_reverse_with_margin": True,
            "discord_entry_contains_entry_sl_tp": True,
            "discord_data_exit_is_separate": True,
            "paper_only": True,
            "future_peeking_enabled": False,
        },
        "updated_at": _now(),
    })
    return overview


def _inject(html: str) -> str:
    if "v66-top-authority" in html:
        return html
    card = r"""<section class="card" id="v66-top-authority">
<h2>🧭 Current-Time 策略權威 / 目前訊號</h2>
<div id="v66mode" class="notice">讀取模式…</div>
<div id="v66position" class="notice" style="margin-top:8px">讀取目前訊號…</div>
<div id="v66strategies" style="margin-top:12px"></div>
<details style="margin-top:10px"><summary>查看固定分數上限 / 仲裁規則</summary><pre id="v66policy">—</pre></details>
</section><style>#v64-top-overview,#v63-top-authority{display:none!important}</style>"""
    js = r"""<script id="v66-top-js">(function(){
const E=x=>String(x??'—').replace(/[&<>"']/g,s=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[s]));
const N=(x,d=2)=>Number.isFinite(Number(x))?Number(x).toFixed(d):'—';
const R=x=>Number.isFinite(Number(x))?((Number(x)>=0?'+':'')+Number(x).toFixed(3)+'R'):'—';
async function V(){
 const m=document.getElementById('v66mode'),p=document.getElementById('v66position'),
       s=document.getElementById('v66strategies'),q=document.getElementById('v66policy');
 if(!m||!p||!s)return;
 try{
  const r=await fetch('/api/v66/current-authority',{cache:'no-store'}),z=await r.json(),
        md=z.runtime_mode||{},a=z.current_signal_position||{},arb=z.arbiter||{};
  m.className='notice '+(md.latched_current_time?'g':'y');
  m.innerHTML='<b>模式：'+E(md.mode)+'</b>｜Current latch '+(md.latched_current_time?'✅':'⏳')+
   '｜歷史重跑 '+(md.historical_restart_suppressed?'禁止':'尚未鎖定')+'<br>'+E(md.mode_reason_zh||'');
  p.className='notice '+(a.strategy?'g':'y');
  p.innerHTML=a.strategy?'<b>目前 '+E(a.status)+'｜'+E(a.strategy)+'｜'+E(a.direction)+'</b>'+
   '<br>進場 '+E(a.entry)+'｜初始 SL '+E(a.initial_stop)+'｜目前 SL '+E(a.current_stop)+
   '<br>TP '+E((a.targets||[]).map((x,i)=>'TP'+(i+1)+' '+x.price+' ('+N(x.allocation,1)+'%)').join(' / '))+
   '<br>當下分數 '+N(a.live_score,1)+'/100｜Pred '+R(a.predicted_ev_r)+'｜Required '+R(a.required_ev_r)+
   '｜Edge '+R(a.edge_r)+'｜仲裁 '+E(arb.status||'POSITION_LOCK')
   :'<b>目前沒有持倉或掛單</b>｜仲裁 '+E(arb.status||'WAIT');
  const rows=z.completed_strategies||[];
  s.innerHTML='<h3 style="margin:12px 0 6px">已完成 / 可被 Current-Time 仲裁的策略</h3>'+
   (rows.length?rows.map((x,i)=>{
    const data=(x.data_used||[]).map(d=>E(d.label_zh)+' <code>'+E(d.name)+'</code>').join('、')||'—';
    const gates=(x.state_gate_rules_zh||[]).map(E).join('；')||'無額外 Gate';
    return '<div class="notice '+(x.active_position||x.tradeable?'g':'y')+'" style="margin:8px 0">'+
     '<b>#'+(i+1)+' '+E(x.strategy_id)+' · '+E(x.direction)+' · '+E(x.tier)+'</b>'+
     (x.active_position?'　<strong>🟢 目前掛單/持倉</strong>':'')+
     '<br><b>這套策略在做什麼：</b>'+E(x.what_it_does_zh)+
     '<br><b>下單依據：</b>'+E(x.entry_basis_zh)+
     '<br><b>用哪些資料：</b>'+data+
     '<br><b>狀態 Gate：</b>'+gates+
     '<br><b>進出場 / 管理：</b>'+E(x.execution_plan_zh)+
     '<br><b>目前'+(x.tradeable?'可進場':'不下單')+'原因：</b>'+E(x.why_not_enter_zh)+
     '<br>歷史 '+N(x.historical_score,1)+'/100｜當下 '+N(x.live_score,1)+'/100｜PF '+N(x.profit_factor,2)+
     '｜EV '+R(x.expectancy_r)+'｜fills '+E(x.oos_fills)+'｜DD '+R(x.max_drawdown_r)+
     '<br>Pred '+R(x.predicted_ev_r)+'｜Required '+R(x.required_ev_r)+'｜Edge '+R(x.edge_r)+
     '｜OOD '+(Number.isFinite(Number(x.ood_fraction))?(Number(x.ood_fraction)*100).toFixed(1)+'%':'—')+'</div>';
   }).join(''):'尚無已保存策略');
  if(q)q.textContent=JSON.stringify({score_policy:z.score_policy,rules:z.rules},null,2);
 }catch(e){m.className='notice r';m.textContent='V66 讀取失敗：'+String(e)}
}
V();setInterval(()=>{if(!document.hidden)V()},3000);
})();</script>"""
    if "<main>" in html:
        html = html.replace("<main>", "<main>" + card, 1)
    elif "</body>" in html:
        html = html.replace("</body>", card + "</body>", 1)
    else:
        html += card
    return html.replace("</body>", js + "</body>", 1) if "</body>" in html else html + js


def _wrap_html(core: Any, path: str) -> None:
    route = next((r for r in core.app.router.routes
                  if getattr(r, "path", None) == path
                  and "GET" in (getattr(r, "methods", set()) or set())), None)
    old = getattr(route, "endpoint", None)
    if not callable(old):
        return
    from fastapi.responses import HTMLResponse
    core.app.router.routes = [r for r in core.app.router.routes if getattr(r, "path", None) != path]

    def endpoint() -> HTMLResponse:
        raw = old()
        html = raw.body.decode("utf-8", errors="replace") if hasattr(raw, "body") else str(raw)
        return HTMLResponse(_inject(html), headers={
            "Cache-Control": "no-store,max-age=0", "X-ETH-Adaptive-Mode": VERSION})

    core.app.add_api_route(path, endpoint, methods=["GET"], response_class=HTMLResponse,
                           name="v66_" + ("root" if path == "/" else "full"))


def install(production: Any, autonomous: Any, v63: Any, v64: Any) -> None:
    global _INSTALLED
    if _INSTALLED:
        return
    _INSTALLED = True
    core = production.core

    core.scan = _scan_wrapper(core, autonomous, core.scan)
    core.learning_tick = _learning_wrapper(core, autonomous, core.learning_tick)

    core.app.router.routes = [r for r in core.app.router.routes
                              if getattr(r, "path", None) != "/api/v66/current-authority"]
    core.app.add_api_route("/api/v66/current-authority",
                           lambda: _authority(core, autonomous, v63, v64),
                           methods=["GET"], name="v66_current_authority")
    _wrap_html(core, "/")
    _wrap_html(core, "/dashboard/full")

    mode = refresh_mode(core, autonomous, source="install")
    _state(core, status="READY", mode=mode.get("mode"),
           latched_current_time=bool(mode.get("latched_current_time")),
           historical_restart_suppressed=bool(mode.get("historical_restart_suppressed")),
           strategy_explanations_at_top=True, all_strategy_data_visible=True,
           all_strategy_nonentry_reasons_visible=True, current_signal_at_top=True,
           score_caps_changed=False, v56_execution_semantics_changed=False,
           v63_score_semantics_changed=False, historical_oos_changed=False,
           future_peeking_enabled=False, paper_only=True)

    role = core.state.get("bootstrap_replica_role")
    if isinstance(role, dict):
        role.update({
            "final_runtime_overlay": VERSION,
            "production_entry": "server_entry_v66.py",
            "forced_current_time_authority": VERSION,
            "current_time_latch_after_terminal_strategy": True,
            "historical_restart_after_current_latch": False,
            "strategy_explainer_at_dashboard_top": True,
            "all_strategy_nonentry_reasons_visible": True,
            "score_caps_changed_by_v66": False,
            "historical_oos_rewritten_by_v66": False,
        })
    runtime_identity.stamp(core)
