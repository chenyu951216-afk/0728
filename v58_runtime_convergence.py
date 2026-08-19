from __future__ import annotations

"""V58 production-runtime convergence and dashboard performance authority.

V56 remains the research/execution semantic authority and V57 remains the live-hook
signature/binding fix.  V58 changes neither historical research nor Current Paper trade
semantics.  It makes the final production overlay explicit, prevents the layered legacy
dashboard from hammering SQLite/API endpoints every 1-2 seconds, and exposes one small
runtime endpoint that proves how the stack is wired.
"""

import inspect
import threading
import time
from dataclasses import dataclass
from typing import Any, Callable

import runtime_identity

VERSION = 'V58_RUNTIME_CONVERGENCE_DASHBOARD_PERFORMANCE'
SCHEMA = 58
STATE_KEY = 'v58_runtime_convergence'
DASHBOARD_MIN_POLL_MS = 5000
DASHBOARD_API_DEDUPE_MS = 900
SERVER_CACHE_TTL_SECONDS = 2.5
_INSTALLED = False
_CACHE_LOCK = threading.Lock()


@dataclass
class _CacheItem:
    at: float
    value: Any


_ENDPOINT_CACHE: dict[str, _CacheItem] = {}


def _now() -> int:
    return int(time.time())


def _dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _state(core: Any, **patch: Any) -> dict[str, Any]:
    out = _dict(core.state.get(STATE_KEY))
    out.update(patch)
    out.update({
        'schema': SCHEMA,
        'runtime': VERSION,
        'public_runtime': runtime_identity.RUNTIME_VERSION,
        'updated_at': _now(),
    })
    core.state[STATE_KEY] = out
    return out


def _cached_call(key: str, fn: Callable[[], Any], ttl: float = SERVER_CACHE_TTL_SECONDS) -> Any:
    now = time.monotonic()
    with _CACHE_LOCK:
        item = _ENDPOINT_CACHE.get(key)
        if item is not None and now - item.at < max(0.05, float(ttl)):
            return item.value
    value = fn()
    with _CACHE_LOCK:
        _ENDPOINT_CACHE[key] = _CacheItem(time.monotonic(), value)
    return value


def _replace_get_with_cache(core: Any, path: str, ttl: float = SERVER_CACHE_TTL_SECONDS) -> bool:
    route = next((r for r in core.app.router.routes if getattr(r, 'path', None) == path), None)
    endpoint = getattr(route, 'endpoint', None)
    if not callable(endpoint):
        return False
    if inspect.iscoroutinefunction(endpoint):
        # Current dashboard endpoints are sync.  Fail safe instead of accidentally
        # caching an un-awaited coroutine if a future stack changes one of them.
        return False
    methods = set(getattr(route, 'methods', set()) or set())
    if 'GET' not in methods:
        return False
    core.app.router.routes = [r for r in core.app.router.routes if getattr(r, 'path', None) != path]

    def cached_endpoint():
        return _cached_call(path, endpoint, ttl)

    core.app.add_api_route(path, cached_endpoint, methods=['GET'], name='v58_cached_' + path.strip('/').replace('/', '_'))
    return True


def _governor_script() -> str:
    # This script is inserted before all legacy dashboard scripts.  Existing cards keep
    # working, but every layered setInterval is clamped and suspended while the tab is
    # hidden.  Duplicate GETs for the same endpoint within one paint burst share the
    # first response clone instead of hitting SQLite twice.
    return f'''<script id="v58-dashboard-governor">(function(){{
const V58_MIN={DASHBOARD_MIN_POLL_MS},V58_DEDUPE={DASHBOARD_API_DEDUPE_MS};
const nativeSetInterval=window.setInterval.bind(window),nativeFetch=window.fetch.bind(window);
window.setInterval=function(fn,delay,...args){{const ms=Math.max(V58_MIN,Number(delay)||V58_MIN);return nativeSetInterval(function(){{if(document.hidden)return;return fn(...args)}},ms)}};
const inflight=new Map();
window.fetch=function(input,init){{
  const method=String((init&&init.method)||'GET').toUpperCase();
  const url=typeof input==='string'?input:(input&&input.url)||'';
  if(method==='GET'&&url.startsWith('/api/')){{
    const now=Date.now(),hit=inflight.get(url);
    if(hit&&now-hit.at<V58_DEDUPE)return hit.promise.then(r=>r.clone());
    const request=nativeFetch(input,init);
    const clonePromise=request.then(r=>r.clone());
    inflight.set(url,{{at:now,promise:clonePromise}});
    window.setTimeout(()=>{{const x=inflight.get(url);if(x&&Date.now()-x.at>=V58_DEDUPE)inflight.delete(url)}},V58_DEDUPE+50);
    return request;
  }}
  return nativeFetch(input,init);
}};
}})();</script>'''


def _runtime_banner() -> str:
    return '''<section id="v58-runtime-banner" class="card" style="margin-top:14px">
<h2>✅ Production Runtime V58</h2>
<div class="notice g"><b>FINAL OVERLAY：V58</b><br>
研究/回測語意：V56｜Live hook 對接：V57｜Dashboard/API 效能治理：V58<br>
V58 不改 OOS、策略 genome、下單規則或歷史資料；只統一 production wiring 與降低網頁輪詢負載。</div>
</section>'''


def _install_dashboard(core: Any) -> None:
    root = next((r for r in core.app.router.routes if getattr(r, 'path', None) == '/'), None)
    old = getattr(root, 'endpoint', None)
    if not callable(old):
        return
    from fastapi.responses import HTMLResponse

    core.app.router.routes = [r for r in core.app.router.routes if getattr(r, 'path', None) != '/']

    @core.app.get('/', response_class=HTMLResponse, name='v58_runtime_converged_dashboard')
    def dashboard_v58() -> HTMLResponse:
        raw = old()
        html = raw.body.decode() if hasattr(raw, 'body') else str(raw)
        governor = _governor_script()
        if '<head>' in html:
            html = html.replace('<head>', '<head>' + governor, 1)
        else:
            html = governor + html
        # Keep V56 named where it describes research semantics, but make it impossible
        # to mistake that card for the final production process version.
        html = html.replace('🧠 V56 真實執行 / 多策略協調 / 現在式學習',
                            '🧠 V56 研究/執行語意（Production V58）', 1)
        banner = _runtime_banner()
        if '<body>' in html:
            html = html.replace('<body>', '<body>' + banner, 1)
        else:
            html = banner + html
        return HTMLResponse(html, headers={
            'Cache-Control': 'no-store, no-cache, must-revalidate, max-age=0',
            'Pragma': 'no-cache',
            'X-ETH-Adaptive-Runtime': VERSION,
        })


def _runtime_status(core: Any, autonomous: Any, v56: Any, v57: Any) -> dict[str, Any]:
    checkpoint = _dict(core.get_state(autonomous.CHECKPOINT_KEY, {}))
    role = _dict(core.state.get('bootstrap_replica_role'))
    market = _dict(core.state.get('market_scan'))
    return {
        'schema': SCHEMA,
        'runtime': VERSION,
        'final_overlay': VERSION,
        'research_execution_semantics': str(getattr(v56, 'VERSION', 'V56_CAUSAL_MULTICHAMPION_ONLINE_LEARNING')),
        'live_hook_authority': str(getattr(v57, 'VERSION', 'V57_LIVE_HOOK_RUNTIME_AUTHORITY')),
        'public_runtime': runtime_identity.RUNTIME_VERSION,
        'production_entry': 'server_entry_v58.py',
        'wiring': {
            'analysis': 'V56_CANONICAL_ANALYSIS via V57 dual-signature adapter',
            'create_signal': 'V56_CANONICAL_CREATE via V57 dual-signature adapter',
            'update_signal': 'V56_CANONICAL_5M_MANAGEMENT via V57 dual-signature adapter',
            'single_eth_multi_champion_arbiter': True,
            'current_forward_learning': True,
        },
        'dashboard': {
            'minimum_poll_ms': DASHBOARD_MIN_POLL_MS,
            'duplicate_get_window_ms': DASHBOARD_API_DEDUPE_MS,
            'server_cache_ttl_seconds': SERVER_CACHE_TTL_SECONDS,
            'polling_paused_when_hidden': True,
            'html_cache_disabled': True,
        },
        'historical': {
            'checkpoint_status': checkpoint.get('status'),
            'research_semantics_changed_by_v58': False,
            'oos_rules_changed_by_v58': False,
            'strategy_results_changed_by_v58': False,
            'v47_identity_changed_by_v58': False,
            'raw_history_deleted': False,
            'replay_reset': False,
            'future_peeking_enabled': False,
        },
        'market_scan': {
            'status': market.get('status'),
            'last_success': market.get('last_success') or market.get('last_success_at'),
            'consecutive_errors': market.get('consecutive_errors'),
        },
        'bootstrap_role': role.get('role') or role.get('bootstrap_role'),
        'updated_at': _now(),
    }


def install(production: Any, autonomous: Any, v56: Any, v57: Any) -> None:
    global _INSTALLED
    if _INSTALLED:
        return
    _INSTALLED = True
    core = production.core

    cached_paths = []
    for path in (
        '/api/v30/autonomous',
        '/api/latest/pipeline',
        '/api/latest/champions',
        '/api/latest/execution',
        '/api/v55/champion-authority',
        '/api/v56/authority',
    ):
        if _replace_get_with_cache(core, path):
            cached_paths.append(path)

    core.app.router.routes = [r for r in core.app.router.routes if getattr(r, 'path', None) != '/api/v58/runtime']
    core.app.add_api_route('/api/v58/runtime', lambda: _runtime_status(core, autonomous, v56, v57),
                           methods=['GET'], name='v58_runtime_status')
    _install_dashboard(core)

    state = _state(core,
        installed=True,
        status='READY',
        final_production_overlay=VERSION,
        underlying_research_execution_semantics=str(getattr(v56, 'VERSION', 'V56')),
        underlying_live_hook_authority=str(getattr(v57, 'VERSION', 'V57')),
        cached_dashboard_endpoints=cached_paths,
        dashboard_min_poll_ms=DASHBOARD_MIN_POLL_MS,
        duplicate_get_window_ms=DASHBOARD_API_DEDUPE_MS,
        research_semantics_changed=False,
        oos_rules_changed=False,
        strategy_results_changed=False,
        v47_identity_changed=False,
        historical_data_deleted=False,
        replay_reset=False,
        future_peeking_enabled=False,
    )
    role = core.state.get('bootstrap_replica_role')
    if isinstance(role, dict):
        role.update({
            'final_runtime_overlay': VERSION,
            'production_entry': 'server_entry_v58.py',
            'research_runtime': 'V56_CAUSAL_MULTICHAMPION_20260818',
            'live_hook_runtime_authority': str(getattr(v57, 'VERSION', 'V57_LIVE_HOOK_RUNTIME_AUTHORITY')),
            'dashboard_poll_governor': True,
            'dashboard_endpoint_ttl_cache': True,
            'research_semantics_changed_by_v58': False,
            'v47_exact_identity_changed_by_v58': False,
        })
    state['role_updated'] = isinstance(role, dict)
    runtime_identity.stamp(core)
