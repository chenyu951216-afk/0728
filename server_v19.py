from __future__ import annotations

import hashlib
import json
import logging
import threading
import time

import uvicorn
from fastapi.responses import HTMLResponse

import server_v17 as base
import v18_final_system as final_system
import v18_operational_guard as operational_guard
import v20_historical_signal_evolution as signal_evolution
import v21_coinglass_standard as coinglass_standard
import v22_hierarchical_pipeline as hierarchical_pipeline
from v18_final_system import install as install_final_system
from v18_operational_guard import install as install_operational_guard
from v20_historical_signal_evolution import install as install_signal_evolution
from v21_coinglass_standard import install as install_coinglass_standard
from v22_hierarchical_pipeline import install as install_hierarchical_pipeline

LOG = logging.getLogger('eth-adaptive.startup')
core = base.core

install_final_system(core)
install_operational_guard(core)
install_signal_evolution(core)
install_coinglass_standard(core)
install_hierarchical_pipeline(core)

RUNTIME_VERSION = '10.0.0-20260812'
core.state['runtime_version'] = RUNTIME_VERSION
core.state.setdefault('strict_replay', {})['runtime'] = RUNTIME_VERSION
core.app.version = '10.0.0'

app = core.app
PORT = core.PORT

# A failed generation must not be re-run hourly on byte-identical evidence. Repeatedly
# peeking at the same OOS until something passes is itself meta-overfitting. A new
# generation becomes due only after enough newly matured labels arrive, unless explicitly forced.
def _evolution_certification_due(core_obj, snap: dict, force: bool) -> bool:
    if force:
        return True
    state = final_system._final_state(core_obj)
    now = int(time.time())
    total = int(snap.get('learning_samples') or 0)
    max_ts = int(snap.get('sample_max_ts') or 0)
    last_total = int(state.get('last_cert_sample_total') or 0)
    last_max = int(state.get('last_cert_sample_max_ts') or 0)
    last_at = int(state.get('last_cert_completed_at') or 0)
    if last_at <= 0 and total > 0:
        return True
    con = core_obj.db()
    try:
        new_decisions = int(con.execute(
            'SELECT COUNT(DISTINCT ts) FROM learning_samples WHERE ts>?', (last_max,)
        ).fetchone()[0] or 0)
    finally:
        con.close()
    core_obj.state['evolution_recertification_gate'] = {
        'new_untouched_decisions': new_decisions,
        'required_untouched_decisions': signal_evolution.MIN_UNTOUCHED_HOLDOUT,
        'last_certified_sample_ts': last_max,
        'same_holdout_retry_forbidden': True,
        'ready': new_decisions >= signal_evolution.MIN_UNTOUCHED_HOLDOUT,
        'checked_at': now,
    }
    if new_decisions >= signal_evolution.MIN_UNTOUCHED_HOLDOUT:
        return True
    return False

final_system._certification_due = _evolution_certification_due

# Deduplicate Discord certification summaries by semantic result rather than timestamp.
# A new generation/result still notifies; an unchanged waiting/result summary does not repeat hourly.
_original_send_pending_notice = final_system._send_pending_notice
async def _dedup_send_pending_notice(core_obj):
    notice = core_obj.state.get('v18_pending_notice')
    if not isinstance(notice, dict):
        return
    semantic = {k: notice.get(k) for k in (
        'status', 'reason', 'signal_promoted', 'signal_rejected',
        'execution_promoted', 'execution_rejected', 'signal_champions', 'execution_champions',
        'signal_waiting_new_holdout', 'signal_incumbent_held', 'signal_genomes_evaluated',
    )}
    fp = hashlib.sha256(json.dumps(semantic, sort_keys=True, ensure_ascii=False).encode()).hexdigest()[:24]
    if core_obj.get_state('v20_last_cert_notice_fingerprint', '') == fp:
        return
    await _original_send_pending_notice(core_obj)
    sent = int(core_obj.get_state('v18_last_notice_at', 0) or 0)
    if sent >= int(notice.get('at') or 0):
        core_obj.set_state('v20_last_cert_notice_fingerprint', fp)

final_system._send_pending_notice = _dedup_send_pending_notice

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
        LOG.info('10.0.0 startup provenance preflight complete: %s', result.get('status'))
    except Exception as exc:
        _PREFLIGHT_FAILED.set()
        core.state['startup_preflight'] = {
            'status': 'FAILED', 'ready': False, 'started_at': started,
            'failed_at': int(time.time()), 'error': f'{type(exc).__name__}: {exc}',
            'reason': 'web remains online; certification and new orders remain fail-closed until this is repaired',
        }
        LOG.exception('10.0.0 startup provenance preflight failed')


threading.Thread(target=_preflight_worker, name='source-provenance-preflight', daemon=True).start()

app.router.routes = [route for route in app.router.routes if getattr(route, 'path', None) != '/']


@app.get('/', response_class=HTMLResponse)
def dashboard() -> str:
    html = base.dashboard()
    html = html.replace('ETH Adaptive AI 8.4.1 Certification Orchestrator', 'ETH Adaptive AI 10.0 Hierarchical Learning')
    html = html.replace('ETH Adaptive AI 8.4', 'ETH Adaptive AI 10.0')
    startup_card = '''
<section class="card"><h2>🧭 Hierarchical Point-in-Time Learning / Final Authority</h2>
<div id="startup19" class="notice">讀取八階段學習狀態…</div>
<div id="pipelineStages" style="margin-top:10px"></div>
<details><summary>查看資料、進化、sealed OOS 與 execution audit 證據</summary><pre id="startup19detail">—</pre></details></section>
'''
    marker = '</div><div class="footer">'
    if marker in html:
        html = html.replace(marker, startup_card + marker, 1)
    script = r'''<script id="v19-startup-script">
async function refreshStartup19(){
  const root=document.getElementById('startup19'), detail=document.getElementById('startup19detail');
  if(!root)return;
  try{
    const s=await fetch('/api/v22/pipeline',{cache:'no-store'}).then(r=>{if(!r.ok)throw new Error('HTTP '+r.status);return r.json()});
    const p=s.startup_preflight||{}, ok=s.operational===true, stages=s.stages||[];
    root.className='notice '+(ok?'g':(p.status==='FAILED'?'r':'y'));
    root.innerHTML='<b>'+String(s.final_status||'LEARNING')+'</b>｜整體 '+Number(s.overall_percent||0).toFixed(2)+'%<br>目前：'+String(s.active_stage||'初始化')+
      '<br>'+String(s.final_reason||'由 1D/4H 宏觀 → 1H/30M 結構 → 15M/5M 短線，逐時點回放且禁止重用 sealed OOS')+
      '<br>認證/新單：<b>'+(ok?'Signal + Execution 雙認證通過':'Fail-closed，尚不可下單')+'</b>';
    const box=document.getElementById('pipelineStages');
    if(box)box.innerHTML=stages.map(x=>'<div class="row"><span>'+String(x.name)+'</span><b>'+Number(x.percent||0).toFixed(2)+'% · '+String(x.status||'—')+'</b></div>').join('');
    if(detail)detail.textContent=JSON.stringify(s,null,2);
  }catch(e){root.className='notice r';root.textContent='Final state 讀取失敗：'+String(e)}
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
