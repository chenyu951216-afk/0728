from __future__ import annotations

import uvicorn
from fastapi.responses import HTMLResponse

import server_v17 as base
import runtime_identity
from v18_final_system import install as install_final_system
from v18_operational_guard import preflight_source_provenance, install as install_operational_guard

core = base.core
# Resolve cross-version source provenance BEFORE the final authority is allowed to
# certify anything. If persistent source semantics were lost, only derived labels are
# rebuilt; raw market/derivative caches and the CLEAN Dataset ID are preserved.
preflight_source_provenance(core)
install_final_system(core)
install_operational_guard(core)

RUNTIME_VERSION = runtime_identity.RUNTIME_VERSION
runtime_identity.stamp(core)

app = core.app
PORT = core.PORT

app.router.routes = [route for route in app.router.routes if getattr(route, 'path', None) != '/']


@app.get('/', response_class=HTMLResponse)
def dashboard() -> str:
    html = base.dashboard()
    html = html.replace('ETH Adaptive AI 8.4.1 Certification Orchestrator', f'{runtime_identity.PRODUCT_NAME} {runtime_identity.DISPLAY_VERSION}')
    html = html.replace(
        'Clean Dataset Audit · Matured-Label Strict Replay · Explicit Signal Certification · Walk-Forward Execution',
        'SQLite Truth Recovery · No-Lookahead Replay · Regime Signal Evolution · Untouched Execution Audit · Live/Post-Exit Learning',
    )
    final_card = '''
<section class="card"><h2>🧠 Final Authority / 最終學習與交易鏈</h2>
<div id="final18" class="notice">讀取 SQLite 真實狀態…</div>
<div id="final18grid" class="healthgrid" style="margin-top:10px"></div>
<details><summary>查看完整 Final Authority / Regime Portfolio / Safety Contract</summary><pre id="final18detail">—</pre></details></section>
'''
    grid_close = '</div><div class="footer">'
    if grid_close in html:
        html = html.replace(grid_close, final_card + grid_close, 1)

    script = r'''<script id="v18-final-authority-script">
function v18safe(x){return (typeof esc==='function')?esc(x):String(x??'—')}
function v18pct(x){const n=Number(x);return Number.isFinite(n)?n.toFixed(2)+'%':'—'}
function v18box(name,value,cls,small){return '<div class="health"><div class="name">'+v18safe(name)+'</div><div class="stat '+cls+'">'+v18safe(value)+'</div><div class="tiny">'+v18safe(small||'')+'</div></div>'}
async function refreshFinal18(){
  const root=document.getElementById('final18'), grid=document.getElementById('final18grid'), detail=document.getElementById('final18detail');
  if(!root) return;
  try{
    const z=await fetch('/api/v18/final-status',{cache:'no-store'}).then(r=>{if(!r.ok)throw new Error('HTTP '+r.status);return r.json()});
    const audit=((z.dataset||{}).audit)||{}, base=((z.dataset||{}).baseline)||{}, rp=z.replay||{}, sm=z.samples||{}, c=z.certification||{}, sc=z.source_contract||{}, dc=z.discord||{}, live=z.live_learning||{}, sp=z.source_provenance_preflight||{};
    const good=(audit.valid===true && base.status==='CLEAN');
    const full=(z.status==='FULLY_OPERATIONAL');
    root.className='notice '+(full?'g':(good?'y':'r'));
    root.innerHTML='<b>'+v18safe(z.status||'BOOTING')+'</b><br>'+v18safe(z.reason||'—')+
      '<br>Dataset <b>'+v18safe(base.status||'—')+'</b> / Audit <b>'+v18safe(audit.status||'—')+'</b> / Replay <b>'+v18pct(rp.percent)+'</b>'+
      '<br>Samples <b>'+Number(sm.rows||0).toLocaleString()+'</b>｜Signal/Execution Champion <b>'+String(c.signal_champions??0)+' / '+String(c.execution_champions??0)+'</b>';
    const port=((z.regime_portfolio||{}).regimes)||{};
    grid.innerHTML=
      v18box('Strict Replay',v18pct(rp.percent),rp.complete?'ok':'warn',rp.complete?'合法 label frontier 已追平':'剩餘成熟決策 '+String(rp.pending_eligible_decisions??'—'))+
      v18box('Derived Audit',audit.status||'—',audit.valid?'ok':'bad',audit.reason||'')+
      v18box('Source Provenance',sp.status||'—',(sp.status==='PERSISTENT_SOURCE_CONTRACT_RECOVERED'||sp.status==='NO_EXISTING_DERIVED_SAMPLES'||sp.status==='DERIVED_REBUILD_ALREADY_APPLIED'||sp.status==='DERIVED_REBUILD_REQUIRED_AND_APPLIED')?'ok':'warn',sp.reason||'跨版本來源契約檢查')+
      v18box('Signal Certification',String(c.signal_champions??0)+' Champion',Number(c.signal_champions||0)>0?'ok':'warn','7策略 × LONG/SHORT；genome/OOS/anti-overfit')+
      v18box('Execution Audit',String(c.execution_champions??0)+' Champion',Number(c.execution_champions||0)>0?'ok':'warn','Entry/SL/TP/分批/BE/trailing + untouched audit')+
      v18box('Regime Specialists',String(Object.keys(port).length)+' regimes',Object.keys(port).length?'ok':'warn','不同市場階段只使用已認證 specialist')+
      v18box('Discord',dc.configured?'Configured':'Not configured',dc.configured?'ok':'bad','交易生命週期/學習/複盤通知')+
      v18box('Live Evidence',Number(live.live_execution_samples||0).toLocaleString(),'ok','不直接污染 Signal label；累積後重新 audit')+
      v18box('Source Contract',sc.frozen?'Frozen':'Waiting',sc.frozen?'ok':'warn',(sc.oi_mode||'—')+' / '+(sc.funding_mode||'—'));
    if(detail) detail.textContent=JSON.stringify(z,null,2);
  }catch(e){
    root.className='notice r';root.textContent='Final Authority endpoint 讀取失敗：'+String(e);
    if(grid) grid.innerHTML='';
  }
}
refreshFinal18();setInterval(refreshFinal18,5000);
</script>'''
    if '</body>' in html:
        html = html.replace('</body>', script + '</body>', 1)
    else:
        html += script
    return html


if __name__ == '__main__':
    uvicorn.run('server_v18:app', host='0.0.0.0', port=PORT)
