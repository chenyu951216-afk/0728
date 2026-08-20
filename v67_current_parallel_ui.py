from __future__ import annotations

"""V67 UI placement authority for the V65/V66 parallel Current-Time contract.

The legacy dashboard has no <main> element, so earlier overlays that attempted to place
the all-strategy card at the top fell back to the bottom of the page. V67 does not alter
strategy, score, OOS, or execution semantics. It moves the V66 Current-Time authority
card to the first position in the actual .grid and renames the legacy single-selection
metric so it is clear that it is the arbitration result, not the only evaluated strategy.
"""

import time
from typing import Any

import runtime_identity

VERSION = "V67_PARALLEL_CURRENT_UI_PLACEMENT"
SCHEMA = 67
STATE_KEY = "v67_parallel_current_ui_placement"
_INSTALLED = False


def _now() -> int:
    return int(time.time())


def _d(v: Any) -> dict[str, Any]:
    return dict(v) if isinstance(v, dict) else {}


def _state(core: Any, **patch: Any) -> dict[str, Any]:
    z = _d(core.state.get(STATE_KEY)); z.update(patch)
    z.update({"schema": SCHEMA, "runtime": VERSION,
              "public_runtime": runtime_identity.RUNTIME_VERSION, "updated_at": _now()})
    core.state[STATE_KEY] = z
    return z


def _inject(html: str) -> str:
    if "v67-current-parallel-ui" in html:
        return html
    script = r'''<script id="v67-current-parallel-ui">(function(){
function place(){
  const card=document.getElementById('v66-top-authority');
  const grid=document.querySelector('.grid');
  if(card&&grid&&grid.firstElementChild!==card)grid.insertBefore(card,grid.firstElementChild);
  document.querySelectorAll('.k').forEach(function(el){
    if((el.textContent||'').trim()==='實際選用')el.textContent='本輪仲裁結果';
  });
}
if(document.readyState==='loading')document.addEventListener('DOMContentLoaded',place);else place();
setTimeout(place,0);
})();</script>'''
    return html.replace('</body>', script + '</body>', 1) if '</body>' in html else html + script


def _wrap(core: Any, path: str, name: str) -> None:
    route = next((r for r in core.app.router.routes
                  if getattr(r, 'path', None) == path
                  and 'GET' in (getattr(r, 'methods', set()) or set())), None)
    old = getattr(route, 'endpoint', None)
    if not callable(old):
        return
    from fastapi.responses import HTMLResponse
    core.app.router.routes = [r for r in core.app.router.routes if getattr(r, 'path', None) != path]

    def endpoint() -> HTMLResponse:
        raw = old()
        html = raw.body.decode('utf-8', errors='replace') if hasattr(raw, 'body') else str(raw)
        return HTMLResponse(_inject(html), headers={
            'Cache-Control': 'no-store,max-age=0', 'X-ETH-Adaptive-UI': VERSION})

    core.app.add_api_route(path, endpoint, methods=['GET'], response_class=HTMLResponse, name=name)


def install(production: Any) -> None:
    global _INSTALLED
    if _INSTALLED:
        return
    _INSTALLED = True
    core = production.core
    _wrap(core, '/', 'v67_parallel_current_root')
    _wrap(core, '/dashboard/full', 'v67_parallel_current_full')
    _state(core, status='READY', v66_authority_card_first_in_dashboard_grid=True,
           legacy_actual_selection_relabelled_as_arbitration_result=True,
           strategy_semantics_changed=False, score_semantics_changed=False,
           historical_oos_changed=False, execution_semantics_changed=False,
           future_peeking_enabled=False, paper_only=True)
    role = core.state.get('bootstrap_replica_role')
    if isinstance(role, dict):
        role.update({
            'final_runtime_overlay': VERSION,
            'production_entry': 'server_entry_v67.py',
            'parallel_current_ui_at_top': True,
            'legacy_single_selection_label_is_arbitration_result': True,
            'strategy_semantics_changed_by_v67': False,
        })
    runtime_identity.stamp(core)
