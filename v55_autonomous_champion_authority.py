from __future__ import annotations

"""V55: make the persisted autonomous complete-package Champion the single live/UI authority.

This overlay is post-certification only. It does not change research features, candidate
generation, historical simulation, OOS thresholds, costs, stops/targets, or no-lookahead
rules. It only projects the durable autonomous Champion into legacy UI/API surfaces and
fail-closes Stage-9 signal creation unless the selected genome exactly matches that
persisted certified Champion.
"""

import time
from typing import Any, Callable

import runtime_identity

VERSION = "V55_AUTONOMOUS_CHAMPION_AUTHORITY"
SCHEMA = 55
STATE_KEY = "v55_autonomous_champion_authority"
_INSTALLED = False
_BASE_CREATE: Callable[..., Any] | None = None


def _dict(v: Any) -> dict[str, Any]:
    return dict(v) if isinstance(v, dict) else {}


def _now() -> int:
    return int(time.time())


def _champions(core: Any, autonomous: Any) -> list[dict[str, Any]]:
    try:
        return list(autonomous._load_registry(core, active_only=True) or [])
    except Exception:
        return []


def _gate_text(gate: list[dict[str, Any]]) -> str:
    if not gate:
        return "all-state"
    out = []
    for x in gate:
        op = "≤" if str(x.get("op") or "").upper() == "LE" else "≥"
        try:
            value = f"{float(x.get('value')):.6g}"
        except Exception:
            value = str(x.get("value") or "—")
        out.append(f"{x.get('feature') or '?'} {op} {value}")
    return " AND ".join(out)


def _logic(item: dict[str, Any], autonomous: Any, execution52: Any) -> dict[str, Any]:
    g, m = _dict(item.get("genome")), _dict(item.get("metrics"))
    gate = [dict(x) for x in (m.get("gate_thresholds") or []) if isinstance(x, dict)]
    targets = []
    for rr, alloc in zip(g.get("target_rr") or [], g.get("allocations") or []):
        try:
            targets.append({"rr": float(rr), "allocation_pct": float(alloc)})
        except Exception:
            pass
    try:
        direct_r_threshold = float(m.get("direct_r_threshold"))
    except Exception:
        direct_r_threshold = None
    return {
        "strategy_id": str(item.get("strategy_id") or ""),
        "direction": str(item.get("direction") or g.get("direction") or ""),
        "behavior_label": str(item.get("behavior_label") or m.get("behavior_label") or ""),
        "state_gate": {"text": _gate_text(gate), "conditions": gate},
        "model_gate": {
            "direct_r_threshold": direct_r_threshold,
            "live_min_predicted_ev_r": float(getattr(autonomous, "LIVE_MIN_PREDICTED_EV_R", 0.0)),
            "live_max_ood_fraction": float(getattr(autonomous, "LIVE_MAX_OOD_FRACTION", 0.0)),
            "rule": "predicted EV_R >= max(frozen direct-R threshold, live safety floor) and OOD within limit",
        },
        "entry": {
            "order_type": "MARKET" if bool(g.get("entry_market")) else "ATR_OFFSET_LIMIT",
            "entry_offset_atr": float(g.get("entry_offset_atr") or 0.0),
            "expire_bars_15m": int(g.get("expire_bars") or 0),
        },
        "stop_loss": {
            "stop_atr": float(g.get("stop_atr") or 0.0),
            "minimum_stop_pct": 0.08,
            "never_widen_stop": True,
        },
        "take_profit": {"targets": targets},
        "management": {
            "breakeven_after_r": float(g.get("breakeven_after_r") or 0.0),
            "trail_start_r": float(g.get("trail_start_r") or 0.0),
            "trail_lock_r": float(g.get("trail_lock_r") or 0.0),
            "max_hold_bars_15m": int(g.get("max_hold_bars") or 0),
            "max_hold_hours": round(int(g.get("max_hold_bars") or 0) * 0.25, 2),
            "cooldown_bars_15m": int(g.get("cooldown_bars") or 0),
            "initial_plan_immutable": True,
        },
        "execution": {
            "paper_notional_usdt": float(getattr(autonomous, "PAPER_NOTIONAL_USDT", 0.0)),
            "leverage_mode": str(getattr(execution52, "LEVERAGE_MODE", "MAX_SAFE_WITH_STOP_HEADROOM_AT_ORDER_TIME")),
            "fail_closed": True,
        },
        "oos": {
            "profit_factor": m.get("profit_factor"),
            "expectancy_r": m.get("expectancy_r"),
            "win_rate": m.get("test_win"),
            "fills": m.get("oos_fills"),
            "max_drawdown_r": m.get("max_drawdown_r"),
            "bootstrap_ci05_r": m.get("bootstrap_ci05_r"),
            "stability": m.get("stability"),
            "profitable_folds": m.get("profitable_folds"),
            "worst_fold_ev_r": m.get("worst_fold_ev"),
            "historical_no_lookahead": bool(m.get("historical_no_lookahead", True)),
        },
    }


def _reconcile(core: Any, autonomous: Any, execution52: Any) -> dict[str, Any]:
    cp = _dict(core.get_state(autonomous.CHECKPOINT_KEY, {}))
    champions = _champions(core, autonomous)
    ids = [str(x.get("strategy_id")) for x in champions if x.get("strategy_id")]
    terminal = str(cp.get("status") or "") == "COMPLETE"
    ready = bool(terminal and ids)

    if ready:
        handoff = _dict(core.state.get("v52_current_paper_handoff"))
        handoff.update({
            "ready": True, "mode": "CERTIFIED_CURRENT_PAPER", "strategy_ids": ids,
            "paper_only": True, "historical_replay_complete": True,
            "historical_certification_complete": True,
            "champion_source": "AUTONOMOUS_COMPLETE_PACKAGE",
            "v55_authoritative": True, "updated_at": _now(),
        })
        core.state["v52_current_paper_handoff"] = handoff
        learning = core.state.setdefault("learning", {})
        learning["phase"] = "CURRENT_PAPER_MONITORING"
        pipe = _dict(learning.get("certification_pipeline"))
        pipe.update({
            "stage": "CURRENT_PAPER_MONITORING",
            "signal_champions": len(ids), "execution_champions": len(ids),
            "autonomous_full_package_champions": len(ids),
            "legacy_split_signal_execution_pipeline_used": False,
            "current_paper_ready": True,
            "champion_source": "AUTONOMOUS_COMPLETE_PACKAGE",
        })
        learning["certification_pipeline"] = pipe

    out = {
        "schema": SCHEMA, "runtime": VERSION, "public_runtime": runtime_identity.RUNTIME_VERSION,
        "status": "CURRENT_PAPER_MONITORING" if ready else "WAITING_FOR_CERTIFIED_AUTONOMOUS_CHAMPION",
        "terminal": terminal, "current_paper_ready": ready,
        "champion_count": len(champions), "champion_ids": ids,
        "champions": [_logic(x, autonomous, execution52) for x in champions],
        "canonical_champion_source": "AUTONOMOUS_COMPLETE_PACKAGE",
        "research_semantics_changed": False, "historical_results_recomputed": False,
        "oos_thresholds_changed": False, "future_peeking_enabled": False,
        "updated_at": _now(),
    }
    core.state[STATE_KEY] = out
    return out


def _signal_rows(core: Any, autonomous: Any, execution52: Any) -> list[dict[str, Any]]:
    rows = []
    for x in _champions(core, autonomous):
        m = _dict(x.get("metrics"))
        rows.append({
            "strategy": x.get("strategy_id"), "strategy_id": x.get("strategy_id"),
            "direction": x.get("direction"), "version": "AUTO_PACKAGE",
            "profit_factor": m.get("profit_factor"), "expectancy_r": m.get("expectancy_r"),
            "recent_fold_ev_r": None, "test_win": m.get("test_win"),
            "threshold": None, "selected_n": m.get("oos_fills"), "signals_per_day": None,
            "source": "AUTONOMOUS_COMPLETE_PACKAGE",
            "behavior_label": x.get("behavior_label"),
            "direct_r_threshold": m.get("direct_r_threshold"),
        })
    return rows


def _execution_rows(core: Any, autonomous: Any, execution52: Any) -> dict[str, Any]:
    rows = []
    for x in _champions(core, autonomous):
        m = _dict(x.get("metrics"))
        rows.append({
            "status": "CHAMPION", "strategy": x.get("strategy_id"),
            "strategy_id": x.get("strategy_id"), "direction": x.get("direction"),
            "model_version": "AUTO_PACKAGE", "execution_version": getattr(execution52, "VERSION", "V52"),
            "source": "AUTONOMOUS_COMPLETE_PACKAGE",
            "metrics": {
                "profit_factor": m.get("profit_factor"), "expectancy_r": m.get("expectancy_r"),
                "ev_bootstrap_05": m.get("bootstrap_ci05_r"), "oos_fills": m.get("oos_fills"),
                "qualified_walkforward_folds": None, "recent_fold_ev_r": None,
                "max_drawdown_r": m.get("max_drawdown_r"),
            },
        })
    return {"registry": rows, "authority": VERSION, "source": "AUTONOMOUS_COMPLETE_PACKAGE"}


def _replace_get(core: Any, path: str, fn: Callable[[], Any], name: str) -> None:
    core.app.router.routes = [r for r in core.app.router.routes if getattr(r, "path", None) != path]
    core.app.add_api_route(path, fn, methods=["GET"], name=name)


def _install_routes(core: Any, autonomous: Any, execution52: Any) -> None:
    cert_route = next((r for r in core.app.router.routes if getattr(r, "path", None) == "/api/v17/certification"), None)
    old_cert = getattr(cert_route, "endpoint", None)

    def authority():
        return _reconcile(core, autonomous, execution52)

    def signals():
        _reconcile(core, autonomous, execution52)
        return _signal_rows(core, autonomous, execution52)

    def execution():
        _reconcile(core, autonomous, execution52)
        return _execution_rows(core, autonomous, execution52)

    def certification():
        try:
            base = dict(old_cert() or {}) if callable(old_cert) else {}
        except Exception as exc:
            base = {"endpoint_error": f"{type(exc).__name__}: {exc}"}
        state = _reconcile(core, autonomous, execution52)
        if state["current_paper_ready"]:
            cert = _dict(base.get("certification"))
            cert.update({
                "status": "SIGNAL_CHAMPION_CERTIFIED",
                "reason": "autonomous complete-package Champion passed chronological OOS and owns Signal+Execution semantics",
                "signal_champions": state["champion_count"], "execution_champions": state["champion_count"],
                "authority": VERSION, "champion_source": "AUTONOMOUS_COMPLETE_PACKAGE",
                "legacy_split_pipeline_used": False,
            })
            pipe = _dict(base.get("pipeline"))
            pipe.update({
                "stage": "FULLY_OPERATIONAL", "signal_champions": state["champion_count"],
                "execution_champions": state["champion_count"],
                "autonomous_full_package_champions": state["champion_count"],
                "legacy_split_signal_execution_pipeline_used": False,
            })
            base.update({"certification": cert, "pipeline": pipe, "autonomous_champion_authority": state})
        return base

    _replace_get(core, "/api/v55/champion-authority", authority, "v55_champion_authority")
    _replace_get(core, "/api/latest/champions", signals, "v55_signal_projection")
    _replace_get(core, "/api/latest/execution", execution, "v55_execution_projection")
    if callable(old_cert):
        _replace_get(core, "/api/v17/certification", certification, "v55_certification_projection")


def _install_execution_guard(core: Any, autonomous: Any, execution52: Any) -> None:
    global _BASE_CREATE
    if _BASE_CREATE is not None:
        return
    _BASE_CREATE = core.create_signal

    def guarded(analysis: dict[str, Any], m15: list[dict[str, Any]]):
        sel = _dict((analysis or {}).get("selection"))
        if not sel.get("tradeable"):
            return _BASE_CREATE(analysis, m15)
        sid = str(sel.get("strategy") or "")
        registry = {str(x.get("strategy_id")): x for x in _champions(core, autonomous)}
        item = registry.get(sid)
        state = _reconcile(core, autonomous, execution52)
        if item is None or not state.get("current_paper_ready"):
            core.state["v55_execution_fail_closed"] = {"at": _now(), "strategy_id": sid, "reason": "selection is not the active persisted OOS-certified autonomous Champion"}
            return None
        try:
            selected_hash = autonomous._hash_payload(_dict(sel.get("genome")), 20)
            persisted_hash = autonomous._hash_payload(_dict(item.get("genome")), 20)
        except Exception:
            selected_hash = persisted_hash = ""
        if not selected_hash or selected_hash != persisted_hash:
            core.state["v55_execution_fail_closed"] = {"at": _now(), "strategy_id": sid, "reason": "selected genome differs from persisted certified Champion", "selected_hash": selected_hash, "persisted_hash": persisted_hash}
            return None
        core.state["v55_execution_binding"] = {"at": _now(), "strategy_id": sid, "genome_hash": persisted_hash, "source": "AUTONOMOUS_COMPLETE_PACKAGE", "paper_only": True}
        return _BASE_CREATE(analysis, m15)

    core.create_signal = guarded


def _install_dashboard(core: Any) -> None:
    root = next((r for r in core.app.router.routes if getattr(r, "path", None) == "/"), None)
    old = getattr(root, "endpoint", None)
    if not callable(old):
        return
    from fastapi.responses import HTMLResponse
    core.app.router.routes = [r for r in core.app.router.routes if getattr(r, "path", None) != "/"]

    @core.app.get("/", response_class=HTMLResponse, name="v55_champion_dashboard")
    def dashboard_v55() -> str:
        raw = old()
        html = raw.body.decode() if hasattr(raw, "body") else str(raw)
        card = '<section class="card"><h2>🏆 Autonomous Champion / 真正下單邏輯</h2><div id="v55champion" class="notice">讀取已認證完整策略…</div></section>'
        marker = '</div><div class="footer">'
        html = html.replace(marker, card + marker, 1) if marker in html else html.replace("</body>", card + "</body>", 1)
        js = """<script id="v55-champion-ui">(function(){
const E=x=>String(x??'—').replace(/[&<>\"']/g,s=>({'&':'&amp;','<':'&lt;','>':'&gt;','\"':'&quot;',\"'\":'&#39;'}[s]));
const N=(x,d=3)=>Number.isFinite(Number(x))?Number(x).toFixed(d):'—';
const P=x=>Number.isFinite(Number(x))?(Number(x)*100).toFixed(1)+'%':'—';
async function tick(){const el=document.getElementById('v55champion');if(!el)return;try{
const r=await fetch('/api/v55/champion-authority',{cache:'no-store'}),z=await r.json(),cs=z.champions||[];
if(!z.current_paper_ready||!cs.length){el.className='notice y';el.innerHTML='<b>WAITING</b><br>尚無 OOS-certified autonomous complete-package Champion。';return}
el.className='notice g';el.innerHTML=cs.map((c,i)=>{const o=c.oos||{},m=c.model_gate||{},en=c.entry||{},sl=c.stop_loss||{},tp=c.take_profit||{},mg=c.management||{},ex=c.execution||{};
const t=(tp.targets||[]).map((x,j)=>'TP'+(j+1)+' '+N(x.rr,3)+'R/'+N(x.allocation_pct,1)+'%').join('；')||'—';
return '<b>#'+(i+1)+' '+E(c.strategy_id)+' · '+E(c.direction)+'</b><br><b>AI 狀態：</b>'+E(c.behavior_label||c.state_gate?.text)+'<br><b>OOS：</b>PF '+N(o.profit_factor,2)+'｜EV '+N(o.expectancy_r,3)+'R｜勝率 '+P(o.win_rate)+'｜fills '+E(o.fills)+'｜DD '+N(o.max_drawdown_r,2)+'R<br><b>觸發：</b>'+E(c.state_gate?.text)+'；pred EV_R ≥ max('+N(m.direct_r_threshold,3)+'R, '+N(m.live_min_predicted_ev_r,3)+'R)，OOD ≤ '+P(m.live_max_ood_fraction)+'<br><b>Entry：</b>'+E(en.order_type)+'｜offset '+N(en.entry_offset_atr,3)+' ATR｜'+E(en.expire_bars_15m)+' 根15m失效<br><b>SL：</b>'+N(sl.stop_atr,3)+' ATR，至少 '+N(sl.minimum_stop_pct,2)+'%，永不放寬<br><b>TP：</b>'+E(t)+'<br><b>管理：</b>+'+N(mg.breakeven_after_r,3)+'R→BE；+'+N(mg.trail_start_r,3)+'R→鎖'+N(mg.trail_lock_r,3)+'R；最大持有 '+N(mg.max_hold_hours,2)+'h；cooldown '+E(mg.cooldown_bars_15m)+' 根15m<br><b>執行：</b>'+N(ex.paper_notional_usdt,0)+' USDT paper｜'+E(ex.leverage_mode)+'<br><small>全部數字直接讀取 DB 內已認證 Champion genome/metrics；實際 Entry/SL/TP 價格以當下已收 K 的價格與 ATR 代入。</small>'}).join('<hr>')}
catch(x){el.className='notice r';el.textContent='V55 Champion authority 讀取失敗：'+String(x)}}tick();setInterval(tick,5000)})();</script>"""
        return html.replace("</body>", js + "</body>", 1) if "</body>" in html else html + js


def install(production: Any, autonomous: Any, execution52: Any) -> None:
    global _INSTALLED
    if _INSTALLED:
        return
    _INSTALLED = True
    core = production.core
    _install_execution_guard(core, autonomous, execution52)
    _install_routes(core, autonomous, execution52)
    _install_dashboard(core)
    state = _reconcile(core, autonomous, execution52)
    core.state.setdefault("strict_replay", {})["v55_autonomous_champion_authority"] = {
        "schema": SCHEMA, "research_semantics_changed": False,
        "historical_results_recomputed": False, "raw_history_deleted": False,
        "replay_reset": False, "oos_thresholds_changed": False,
        "future_peeking_enabled": False,
        "legacy_champion_ui_projected_from_autonomous_registry": True,
        "stage9_executor_requires_exact_persisted_champion_genome": True,
        "strategy_logic_exposed_from_persisted_genome": True,
        "certified_champion_count": state.get("champion_count", 0),
    }
    runtime_identity.stamp(core)
