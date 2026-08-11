from __future__ import annotations

import logging
import threading
import time

import uvicorn
from fastapi.responses import HTMLResponse

import server_v17 as base
import v18_final_system as final_system
import v18_operational_guard as operational_guard
from v18_final_system import install as install_final_system
from v18_operational_guard import install as install_operational_guard

LOG = logging.getLogger('eth-adaptive.startup')
core = base.core

# Install every final safety/learning layer first. server_entry.py binds Zeabur's
# liveness endpoint before importing this module, so slow persistent-DB initialization
# cannot become an opaque gateway 502.
install_final_system(core)
install_operational_guard(core)

RUNTIME_VERSION = '9.0.3-20260811'
core.state['runtime_version'] = RUNTIME_VERSION
core.state.setdefault('strict_replay', {})['runtime'] = RUNTIME_VERSION
core.app.version = '9.0.3'

app = core.app
PORT = core.PORT

_PREFLIGHT_READY = threading.Event()
_PREFLIGHT_FAILED = threading.Event()

_original_certify = operational_guard.certify_and_execute
_original_live_gate = operational_guard.final_live_gate


def _certify_after_preflight(core_obj, force: bool = False):
    if not _PREFLIGHT_READY.is_set():
        core_obj.state['certification_startup_gate'] = {
            'ready': False,
            'status': 'PREFLIGHT_FAILED' if _PREFLIGHT_FAILED.is_set() else 'PREFLIGHT_RUNNING',
            'reason': 'persistent source-provenance preflight must complete before Signal/Execution certification',
        }
        return []
    core_obj.state['certification_startup_gate'] = {'ready': True, 'status': 'READY'}
    return _original_certify(core_obj, force)


def _live_after_preflight(core_obj, original_create, analysis, m15):
    if not _PREFLIGHT_READY.is_set():
        core_obj.state['live_startup_gate'] = {
            'ready': False,
            'status': 'PREFLIGHT_FAILED' if _PREFLIGHT_FAILED.is_set() else 'PREFLIGHT_RUNNING',
            'reason': 'new orders are forbidden until persistent source provenance is verified',
        }
        return None
    core_obj.state['live_startup_gate'] = {'ready': True, 'status': 'READY'}
    return _original_live_gate(core_obj, original_create, analysis, m15)


operational_guard.certify_and_execute = _certify_after_preflight
operational_guard.final_live_gate = _live_after_preflight
final_system.certify_and_execute = _certify_after_preflight
final_system._final_live_gate = _live_after_preflight


def _preflight_worker() -> None:
    started = int(time.time())
    core.state['startup_preflight'] = {
        'status': 'RUNNING', 'ready': False, 'started_at': started,
        'reason': 'recovering/auditing persistent source provenance in background; web stays available and trading remains fail-closed',
    }
    try:
        result = operational_guard.preflight_source_provenance(core)
        core.state['startup_preflight'] = {
            'status': 'COMPLETE', 'ready': True, 'started_at': started,
            'completed_at': int(time.time()), 'result': result,
        }
        _PREFLIGHT_READY.set()
        LOG.info('9.0.3 startup provenance preflight complete: %s', result.get('status'))
    except Exception as exc:
        _PREFLIGHT_FAILED.set()
        core.state['startup_preflight'] = {
            'status': 'FAILED', 'ready': False, 'started_at': started,
            'failed_at': int(time.time()), 'error': f'{type(exc).__name__}: {exc}',
            'reason': 'web remains online; certification and new orders remain fail-closed until this is repaired',
        }
        LOG.exception('9.0.3 startup provenance preflight failed')


threading.Thread(target=_preflight_worker, name='source-provenance-preflight', daemon=True).start()

app.router.routes = [route for route in app.router.routes if getattr(route, 'path', None) != '/']


@app.get('/', response_class=HTMLResponse)
def dashboard() -> str:
    html = base.dashboard()
    html = html.replace('ETH Adaptive AI 8.4.1 Certification Orchestrator', 'ETH Adaptive AI 9.0.3 Startup Integrity')
    html = html.replace('ETH Adaptive AI 8.4', 'ETH Adaptive AI 9.0.3')
    startup_card = '''
<section class="card"><h2>🛡️ Startup / Final Authority Gate</h2>
<div id="startup19" class="notice">讀取啟動稽核狀態…</div>
<details><summary>查看啟動與認證狀態</summary><pre id="startup19detail">—</pre></details></section>
'''
    marker = '</div><div class="footer">'
    if marker in html:
        html = html.replace(marker, startup_card + marker, 1)
    script = r'''<script id="v19-startup-script">
async function refreshStartup19(){
  const root=document.getElementById('startup19'), detail=document.getElementById('startup19detail');
  if(!root)return;
  try{
    const s=await fetch('/api/state',{cache:'no-store'}).then(r=>{if(!r.ok)throw new Error('HTTP '+r.status);return r.json()});
    const p=s.startup_preflight||{};
    const ok=p.ready===true;
    root.className='notice '+(ok?'g':(p.status==='FAILED'?'r':'y'));
    root.innerHTML='<b>'+String(p.status||'BOOTING')+'</b><br>'+String(p.reason||((p.result||{}).status)||'等待背景來源稽核完成')+'<br>認證/新單：<b>'+(ok?'可依其餘安全門檻運作':'Fail-closed')+'</b>';
    if(detail)detail.textContent=JSON.stringify({startup_preflight:p,certification_startup_gate:s.certification_startup_gate,live_startup_gate:s.live_startup_gate,runtime:s.runtime_version},null,2);
  }catch(e){root.className='notice r';root.textContent='Startup state 讀取失敗：'+String(e)}
}
refreshStartup19();setInterval(refreshStartup19,5000);
</script>'''
    if '</body>' in html:
        html = html.replace('</body>', script + '</body>', 1)
    else:
        html += script
    return html


if __name__ == '__main__':
    uvicorn.run('server_v19:app', host='0.0.0.0', port=PORT)
