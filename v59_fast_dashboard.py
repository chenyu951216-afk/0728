from __future__ import annotations

"""V59: fast first-paint dashboard and static full diagnostics.

The production trading/research stack is intentionally unchanged: V56 remains the
research/execution semantic authority, V57 remains the live-hook adapter, and V58 keeps
its read-only endpoint cache. V59 only decouples the mobile root page from the layered
legacy dashboard so a busy SQLite/research worker cannot leave Safari on a white screen.
"""

import inspect
import re
import time
from typing import Any

import runtime_identity

VERSION = 'V59_FAST_DASHBOARD_FIRST_PAINT'
SCHEMA = 59
STATE_KEY = 'v59_fast_dashboard'
REFRESH_MS = 8000
FETCH_TIMEOUT_MS = 3500
_INSTALLED = False
_FULL_HTML = ''
_RENDER_MS = 0.0


def _now() -> int:
    return int(time.time())


def _dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _state(core: Any, **patch: Any) -> dict[str, Any]:
    out = _dict(core.state.get(STATE_KEY))
    out.update(patch)
    out.update({'schema': SCHEMA, 'runtime': VERSION,
                'public_runtime': runtime_identity.RUNTIME_VERSION, 'updated_at': _now()})
    core.state[STATE_KEY] = out
    return out


def _strip_v58_fetch_monkeypatch(html: str) -> str:
    # V58's global fetch de-duplication is unnecessary once the root becomes a small
    # shell and can be brittle on embedded/mobile WebKit. Keep server-side endpoint
    # caching, but remove only this client monkeypatch from the cached full dashboard.
    return re.sub(r'<script id="v58-dashboard-governor">.*?</script>', '', html,
                  count=1, flags=re.DOTALL)


def _render_full_once(old: Any) -> str:
    global _RENDER_MS
    started = time.perf_counter()
    raw = old()
    if inspect.isawaitable(raw):
        raise RuntimeError('V59 requires a synchronous dashboard endpoint')
    html = raw.body.decode('utf-8', errors='replace') if hasattr(raw, 'body') else str(raw)
    html = _strip_v58_fetch_monkeypatch(html)
    # The full page is diagnostic-only now; reduce all periodic refreshes without
    # changing one-shot button/actions or any trading/runtime loop.
    governor = f'''<script id="v59-full-dashboard-governor">(function(){{
const nativeSetInterval=window.setInterval.bind(window),MIN={REFRESH_MS};
window.setInterval=function(fn,delay,...args){{const ms=Math.max(MIN,Number(delay)||MIN);return nativeSetInterval(function(){{if(document.hidden)return;return fn(...args)}},ms)}};
}})();</script>'''
    html = html.replace('<head>', '<head>' + governor, 1) if '<head>' in html else governor + html
    _RENDER_MS = (time.perf_counter() - started) * 1000.0
    return html


def _fast_shell() -> str:
    # Intentionally small, dependency-free HTML. It paints before any API request and
    # every request has a timeout, so backend load becomes a visible stale/error state
    # rather than a blank page.
    return f'''<!doctype html><html lang="zh-Hant"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<meta name="color-scheme" content="dark"><title>ETH Adaptive AI 10.4 · V59</title>
<style>
:root{{--bg:#061221;--card:#0b1b31;--line:#25466d;--text:#e8f1ff;--muted:#8fa8c8;--good:#55e3b5;--warn:#ffd56a;--bad:#ff6e87}}
*{{box-sizing:border-box}}body{{margin:0;background:var(--bg);color:var(--text);font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;min-height:100vh}}
main{{max-width:900px;margin:auto;padding:calc(20px + env(safe-area-inset-top)) 18px 40px}}h1{{font-size:30px;margin:8px 0}}h2{{font-size:18px;margin:0 0 10px}}.sub{{color:var(--muted);line-height:1.5}}.badge{{display:inline-flex;padding:8px 12px;border:1px solid var(--line);border-radius:999px;margin:12px 0 18px;font-weight:700}}.g{{color:var(--good)}}.y{{color:var(--warn)}}.r{{color:var(--bad)}}.grid{{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:12px}}.card{{background:var(--card);border:1px solid var(--line);border-radius:22px;padding:18px;margin:12px 0;box-shadow:0 8px 30px #0003}}.value{{font-size:24px;font-weight:800;word-break:break-word}}.small{{font-size:13px;color:var(--muted);line-height:1.55}}button,a.btn{{appearance:none;border:1px solid var(--line);background:#102846;color:var(--text);padding:11px 14px;border-radius:13px;text-decoration:none;font-weight:700;display:inline-block;margin:6px 6px 0 0}}pre{{white-space:pre-wrap;word-break:break-word;font-size:11px;color:var(--muted);max-height:260px;overflow:auto}}@media(max-width:620px){{.grid{{grid-template-columns:1fr}}h1{{font-size:27px}}main{{padding-left:14px;padding-right:14px}}}}
</style></head><body><main>
<h1>ETH Adaptive AI 10.4</h1><div class="sub">Fixed-Horizon Autonomous Strategy Research · Production Runtime V59</div>
<div id="healthBadge" class="badge y">● 網頁已載入，正在讀取後端…</div>
<div class="grid"><section class="card"><h2>Production Runtime</h2><div id="runtime" class="value">V59</div><div id="wiring" class="small">V56 research/execution → V57 live hooks → V58 API cache → V59 fast UI</div></section>
<section class="card"><h2>Market Scan</h2><div id="market" class="value">讀取中</div><div id="marketMeta" class="small">頁面本身不等待 Market Scan 才顯示。</div></section></div>
<section class="card"><h2>🧬 Autonomous Strategy Discovery</h2><div id="research" class="value">讀取中</div><div id="researchMeta" class="small">Stage 6–9 / Champion 狀態讀取中。</div></section>
<section class="card"><h2>🏆 Current Champion / Arbiter</h2><div id="champion" class="value">尚未讀取</div><div id="championMeta" class="small">Pred EV / Required EV / Edge / Current Paper</div></section>
<section class="card"><h2>Stage 1–9 最終權威 / Runtime Convergence</h2><div id="pipeline" class="small">讀取中…</div></section>
<section class="card"><h2>網頁控制</h2><button id="refresh">立即更新</button><a class="btn" href="/dashboard/full">完整診斷頁</a><div class="small" style="margin-top:10px">V59 每 8 秒更新一次；切到背景會暫停。每個 API 最多等待 {FETCH_TIMEOUT_MS/1000:.1f} 秒，失敗只顯示錯誤，不會再讓整頁白屏。</div></section>
<details class="card"><summary>診斷資訊</summary><pre id="debug">等待第一次更新…</pre></details>
<!-- Compatibility/diagnostic markers: autonomous-v30-js autonomous-v32-compat Stage 6 外層提交 / 原子啟動 -->
</main><script id="v59-fast-shell-js">
(function(){{
const TIMEOUT={FETCH_TIMEOUT_MS},REFRESH={REFRESH_MS};
const $=id=>document.getElementById(id);
function esc(x){{return String(x??'—').replace(/[&<>"']/g,s=>({{'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}}[s]))}}
async function get(url){{const c=new AbortController(),t=setTimeout(()=>c.abort(),TIMEOUT);try{{const r=await fetch(url,{{cache:'no-store',signal:c.signal}});if(!r.ok)throw new Error('HTTP '+r.status);return await r.json()}}finally{{clearTimeout(t)}}}}
function pct(v){{const n=Number(v);return Number.isFinite(n)?n.toFixed(1)+'%':'—'}}
function rr(v){{const n=Number(v);return Number.isFinite(n)?((n>=0?'+':'')+n.toFixed(3)+'R'):'—'}}
async function refresh(){{
 const targets=['/api/v59/runtime','/api/v58/runtime','/api/v30/autonomous','/api/v56/authority','/api/latest/pipeline'];
 const settled=await Promise.allSettled(targets.map(get)); const data={{}};let failures=[];
 settled.forEach((r,i)=>{{if(r.status==='fulfilled')data[targets[i]]=r.value;else failures.push(targets[i]+': '+String(r.reason))}});
 const r59=data['/api/v59/runtime']||{{}},r58=data['/api/v58/runtime']||{{}},a=data['/api/v30/autonomous']||{{}},v=data['/api/v56/authority']||{{}},p=data['/api/latest/pipeline']||{{}};
 $('runtime').textContent='V59'; $('wiring').textContent='V56 research/execution → V57 live hooks → V58 API cache → V59 fast UI';
 const ms=(r58.market_scan||{{}}); $('market').textContent=ms.status||'UNKNOWN'; $('market').className='value '+((ms.status==='OK')?'g':(ms.status?'y':'')); $('marketMeta').textContent='last success '+(ms.last_success??'—')+' · errors '+(ms.consecutive_errors??'—');
 const prog=a.progress||{{}}; $('research').textContent=(a.status||'NOT_STARTED')+' · Champions '+((a.champions||[]).length); $('researchMeta').textContent='Stage6 '+pct(prog.evolution_percent)+' · OOS '+pct(prog.oos_percent)+' · Current Paper '+(a.live_ready?'READY':'WAITING');
 const s=v.current_selection||{{}},arb=v.arbiter||{{}}; $('champion').textContent=(s.strategy||'WAIT')+' '+(s.direction||''); $('championMeta').textContent='Pred '+rr(s.predicted_ev_r)+' · Required '+rr(s.required_ev_r)+' · Edge '+rr(s.edge_r)+' · '+(arb.status||s.reason||'WAIT');
 const stages=Array.isArray(p.stages)?p.stages:[]; $('pipeline').innerHTML=stages.slice(-4).map(x=>'<div><b>'+esc(x.name||x.stage)+'</b> · '+pct(x.percent)+' · '+esc(x.status)+'</div>').join('')||'尚無 Stage 資料';
 const ok=failures.length===0; $('healthBadge').className='badge '+(ok?'g':'y'); $('healthBadge').textContent=ok?'● 系統狀態 API 正常':'● 網頁正常；部分後端狀態暫時忙碌';
 $('debug').textContent=JSON.stringify({{at:new Date().toISOString(),failures,r59,r58}},null,2);
}}
$('refresh').addEventListener('click',refresh);refresh();setInterval(()=>{{if(!document.hidden)refresh()}},REFRESH);
}})();
</script></body></html>'''


def install(production: Any, autonomous: Any, v56: Any, v57: Any, v58: Any) -> None:
    global _INSTALLED, _FULL_HTML
    if _INSTALLED:
        return
    _INSTALLED = True
    core = production.core
    root = next((r for r in core.app.router.routes if getattr(r, 'path', None) == '/'), None)
    old = getattr(root, 'endpoint', None)
    if not callable(old):
        raise RuntimeError('V59 could not capture existing dashboard endpoint')

    try:
        _FULL_HTML = _render_full_once(old)
        render_error = None
    except Exception as exc:
        _FULL_HTML = '<!doctype html><meta charset="utf-8"><body><h1>Full diagnostics unavailable</h1><pre>' + str(exc) + '</pre></body>'
        render_error = f'{type(exc).__name__}: {exc}'

    from fastapi.responses import HTMLResponse
    core.app.router.routes = [r for r in core.app.router.routes if getattr(r, 'path', None) not in ('/', '/dashboard/full', '/api/v59/runtime')]

    @core.app.get('/', response_class=HTMLResponse, name='v59_fast_dashboard')
    def fast_dashboard() -> HTMLResponse:
        return HTMLResponse(_fast_shell(), headers={
            'Cache-Control': 'no-store, no-cache, must-revalidate, max-age=0',
            'Pragma': 'no-cache', 'X-ETH-Adaptive-Runtime': VERSION,
        })

    @core.app.get('/dashboard/full', response_class=HTMLResponse, name='v59_full_dashboard')
    def full_dashboard() -> HTMLResponse:
        return HTMLResponse(_FULL_HTML, headers={
            'Cache-Control': 'no-store, max-age=0', 'X-ETH-Adaptive-Runtime': VERSION,
        })

    def runtime_status() -> dict[str, Any]:
        v57s = _dict(core.state.get(getattr(v57, 'STATE_KEY', 'v57_live_hook_runtime_authority')))
        v58s = _dict(core.state.get(getattr(v58, 'STATE_KEY', 'v58_runtime_convergence')))
        return {
            'schema': SCHEMA, 'runtime': VERSION, 'final_overlay': VERSION,
            'production_entry': 'server_entry_v59.py',
            'research_execution_semantics': str(getattr(v56, 'VERSION', 'V56')),
            'live_hook_authority': str(getattr(v57, 'VERSION', 'V57')),
            'api_cache_authority': str(getattr(v58, 'VERSION', 'V58')),
            'first_paint_independent_of_sqlite': True,
            'root_calls_legacy_dashboard_per_request': False,
            'full_dashboard_prerendered_once': True,
            'full_dashboard_bytes': len(_FULL_HTML.encode('utf-8')),
            'full_dashboard_render_ms': round(_RENDER_MS, 3),
            'full_dashboard_render_error': render_error,
            'refresh_ms': REFRESH_MS, 'fetch_timeout_ms': FETCH_TIMEOUT_MS,
            'v57_ready': bool(v57s.get('installed') or v57s.get('status') == 'READY'),
            'v58_ready': bool(v58s.get('installed') or v58s.get('status') == 'READY'),
            'historical_semantics_changed': False, 'oos_rules_changed': False,
            'strategy_results_changed': False, 'replay_reset': False,
            'future_peeking_enabled': False, 'updated_at': _now(),
        }

    core.app.add_api_route('/api/v59/runtime', runtime_status, methods=['GET'], name='v59_runtime')

    # Compress the static HTML payloads when the client advertises gzip. This middleware
    # does not touch API/trading semantics and is added before the ASGI server starts.
    try:
        from starlette.middleware.gzip import GZipMiddleware
        if not any(getattr(m.cls, '__name__', '') == 'GZipMiddleware' for m in core.app.user_middleware):
            core.app.add_middleware(GZipMiddleware, minimum_size=1024, compresslevel=4)
            gzip_enabled = True
        else:
            gzip_enabled = True
    except Exception:
        gzip_enabled = False

    _state(core, installed=True, status='READY', production_entry='server_entry_v59.py',
           first_paint_independent_of_sqlite=True, root_calls_legacy_dashboard_per_request=False,
           full_dashboard_prerendered_once=True, full_dashboard_bytes=len(_FULL_HTML.encode('utf-8')),
           full_dashboard_render_ms=round(_RENDER_MS, 3), full_dashboard_render_error=render_error,
           refresh_ms=REFRESH_MS, fetch_timeout_ms=FETCH_TIMEOUT_MS, gzip_enabled=gzip_enabled,
           research_semantics_changed=False, oos_rules_changed=False, strategy_results_changed=False,
           v47_identity_changed=False, historical_data_deleted=False, replay_reset=False,
           future_peeking_enabled=False)
    role = core.state.get('bootstrap_replica_role')
    if isinstance(role, dict):
        role.update({'final_runtime_overlay': VERSION, 'production_entry': 'server_entry_v59.py',
                     'fast_dashboard_first_paint': True, 'root_sqlite_independent': True,
                     'legacy_dashboard_prerendered': True, 'research_runtime': 'V56_CAUSAL_MULTICHAMPION_20260818',
                     'live_hook_runtime_authority': str(getattr(v57, 'VERSION', 'V57'))})
    runtime_identity.stamp(core)
