from __future__ import annotations

import uvicorn
from fastapi.responses import HTMLResponse

import server as base
from v17_certification_orchestrator import install as install_certification_orchestrator

core = base.core
install_certification_orchestrator(core)

RUNTIME_VERSION = '8.4.1-20260810'
core.state['runtime_version'] = RUNTIME_VERSION
core.state.setdefault('strict_replay', {})['runtime'] = RUNTIME_VERSION
core.app.version = '8.4.1'

app = core.app
PORT = core.PORT

# Replace only the presentation route. All API/runtime behavior remains the already
# validated 8.4 stack plus the final v17 certification authority installed above.
app.router.routes = [route for route in app.router.routes if getattr(route, 'path', None) != '/']


@app.get('/', response_class=HTMLResponse)
def dashboard() -> str:
    html = base.dashboard()
    html = html.replace('ETH Adaptive AI 8.4.0 Runtime Integrity', 'ETH Adaptive AI 8.4.1 Certification Orchestrator')
    html = html.replace(
        'Clean Dataset · Matured-Label Strict Replay · Multi-Exchange Gap Recovery · OOS Signal + Walk-Forward Execution',
        'Clean Dataset Audit · Matured-Label Strict Replay · Explicit Signal Certification · Walk-Forward Execution',
    )
    cert_card = '''
<section class="card"><h2>🧪 正式策略認證 / Derived Data 稽核</h2>
<div id="cert17" class="notice">讀取正式認證狀態…</div>
<details><summary>查看 14 個策略×方向認證結果</summary><pre id="cert17detail">—</pre></details></section>
'''
    # Put the card inside the existing grid but do not touch existing script text.
    grid_close = '</div><div class="footer">'
    if grid_close in html:
        html = html.replace(grid_close, cert_card + grid_close, 1)

    # Keep v17 JS in its own script tag. Never splice source text into an existing
    # script block: that made presentation syntax depend on legacy dashboard endings.
    script = r'''<script id="v17-certification-script">
async function refreshCert17(){
  try{
    const z=await fetch('/api/v17/certification').then(r=>r.json());
    const a=z.audit||{}, c=z.certification||{}, p=z.pipeline||{}, el=document.getElementById('cert17');
    if(!el) return;
    const acceptable=['SIGNAL_CHAMPION_CERTIFIED','NO_SIGNAL_MODEL_PASSED_OOS','SIGNAL_CERTIFICATION_RUNNING','NOT_STARTED','WAITING_FOR_REPLAY'];
    const cls=(a.valid&&acceptable.includes(c.status))?'g':((c.status==='SIGNAL_CERTIFICATION_FAILED'||a.status==='FAILED')?'r':'y');
    el.className='notice '+cls;
    const safe=(typeof esc==='function')?esc:(x=>String(x??'—'));
    el.innerHTML='<b>'+safe(c.status||'NOT_STARTED')+'</b><br>'+safe(c.reason||'等待正式認證')+
      '<br>Derived audit：<b>'+safe(a.status||'—')+'</b>｜samples '+Number(a.learning_samples||0).toLocaleString()+
      '｜decision timestamps '+Number(a.decision_timestamps||0).toLocaleString()+
      '｜14-row partial '+Number(a.partial_decision_timestamps||0).toLocaleString()+
      '<br>Signal / Execution Champion：'+String(p.signal_champions??0)+' / '+String(p.execution_champions??0);
    const detail=document.getElementById('cert17detail');
    if(detail) detail.textContent=JSON.stringify({audit:a,certification:c,pipeline:p},null,2);
  }catch(e){
    const el=document.getElementById('cert17');
    if(el){el.className='notice r';el.textContent='Certification endpoint 讀取失敗：'+String(e)}
  }
}
refreshCert17();
setInterval(refreshCert17,5000);
</script>'''
    if '</body>' in html:
        html = html.replace('</body>', script + '</body>', 1)
    else:
        html += script
    return html


if __name__ == '__main__':
    uvicorn.run('server_v17:app', host='0.0.0.0', port=PORT)
