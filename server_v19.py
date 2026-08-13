from __future__ import annotations

import hashlib
import json
import logging
import threading
import time

import uvicorn
from fastapi.responses import HTMLResponse

import runtime_identity
import server_v17 as base
import v18_final_system as final_system
import v18_operational_guard as operational_guard
import v20_historical_signal_evolution as signal_evolution
import v21_coinglass_standard as coinglass_standard
import v22_hierarchical_pipeline as hierarchical_pipeline
import v16_runtime_integrity as runtime_integrity
import v25_fixed_horizon_runtime as fixed_horizon_runtime
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

RUNTIME_VERSION = runtime_identity.RUNTIME_VERSION
runtime_identity.stamp(core)

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
# 10.2 fixed-horizon contract: historical replay targets the immutable deployment
# cutoff, certification begins only after that replay is complete, and candles that
# arrive during learning are intentionally skipped before current-live handoff.
fixed_horizon_runtime.install(core, runtime_integrity, final_system, signal_evolution)

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
        LOG.info('%s startup provenance preflight complete: %s', RUNTIME_VERSION, result.get('status'))
    except Exception as exc:
        _PREFLIGHT_FAILED.set()
        core.state['startup_preflight'] = {
            'status': 'FAILED', 'ready': False, 'started_at': started,
            'failed_at': int(time.time()), 'error': f'{type(exc).__name__}: {exc}',
            'reason': 'web remains online; certification and new orders remain fail-closed until this is repaired',
        }
        LOG.exception('%s startup provenance preflight failed', RUNTIME_VERSION)


threading.Thread(target=_preflight_worker, name='source-provenance-preflight', daemon=True).start()

app.router.routes = [route for route in app.router.routes if getattr(route, 'path', None) != '/']


@app.get('/', response_class=HTMLResponse)
def dashboard() -> str:
    html = base.dashboard()
    html = html.replace('ETH Adaptive AI 8.4.1 Certification Orchestrator', f'{runtime_identity.PRODUCT_NAME} {runtime_identity.DISPLAY_VERSION}')
    html = html.replace('ETH Adaptive AI 8.4', f'{runtime_identity.PRODUCT_NAME} {runtime_identity.DISPLAY_VERSION}')
    startup_card = '''
<style id="v25-progress-style">
.v25stage{margin:12px 0 16px}.v25head{display:flex;justify-content:space-between;gap:12px;align-items:flex-end;margin-bottom:6px}.v25name{font-weight:700}.v25meta{font-size:12px;opacity:.72;text-align:right}.v25track{height:10px;border-radius:999px;background:#142742;overflow:hidden;border:1px solid #29466d}.v25fill{height:100%;border-radius:999px;background:linear-gradient(90deg,#56dcb2,#6aa9ff,#9f78ff);min-width:0}.v25grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:8px;margin-top:10px}.v25pill{padding:9px 10px;border-radius:12px;background:#0b1930;border:1px solid #29466d;font-size:12px}.v25lineage{padding:8px 0;border-bottom:1px solid #203653;font-size:12px}.v25mono{font-family:ui-monospace,SFMono-Regular,Menlo,monospace;word-break:break-word}@media(max-width:620px){.v25grid{grid-template-columns:1fr}.v25head{align-items:flex-start}.v25meta{max-width:48%}}
</style>
<section class="card"><h2>🧭 完整學習進度 / Fixed-Horizon Final Authority</h2>
<div id="startup19" class="notice">讀取八階段學習狀態…</div>
<div id="fixedHorizon" class="notice" style="margin-top:10px">讀取固定歷史截止點…</div>
<div id="pipelineStages" style="margin-top:14px"></div>
<div id="strategyProgress" style="margin-top:16px"></div>
<details><summary>查看資料、候選策略、進化、sealed OOS、execution audit 與 no-lookahead 證據</summary><pre id="startup19detail">—</pre></details></section>
'''
    marker = '</div><div class="footer">'
    if marker in html:
        html = html.replace(marker, startup_card + marker, 1)
    script = r'''<script id="v19-startup-script">
function v25dt(ts){if(!ts)return '—';try{return new Date(Number(ts)*1000).toLocaleString('zh-TW',{hour12:false})}catch(_){return String(ts)}}
function v25bar(name,pct,status,meta){pct=Math.max(0,Math.min(100,Number(pct||0)));return '<div class="v25stage"><div class="v25head"><div class="v25name">'+name+'</div><div class="v25meta">'+pct.toFixed(2)+'% · '+String(status||'—')+(meta?'<br>'+meta:'')+'</div></div><div class="v25track"><div class="v25fill" style="width:'+pct+'%"></div></div></div>'}
async function refreshStartup19(){
  const root=document.getElementById('startup19'), detail=document.getElementById('startup19detail');
  if(!root)return;
  try{
    const [s,d]=await Promise.all([
      fetch('/api/latest/pipeline',{cache:'no-store'}).then(r=>{if(!r.ok)throw new Error('pipeline HTTP '+r.status);return r.json()}),
      fetch('/api/latest/progress-detail',{cache:'no-store'}).then(r=>{if(!r.ok)throw new Error('detail HTTP '+r.status);return r.json()})
    ]);
    const p=s.startup_preflight||{}, ok=s.operational===true, stages=s.stages||[], replay=d.replay||{}, sig=d.signal_certification||{}, ex=d.execution_audit||{}, hand=d.live_handoff||{}, tc=d.trading_contract||{};
    root.className='notice '+(ok?'g':(p.status==='FAILED'?'r':'y'));
    root.innerHTML='<b>'+String(s.final_status||'LEARNING')+'</b>｜整體 '+Number(s.overall_percent||0).toFixed(2)+'%<br>目前：'+String(s.active_stage||'初始化')+
      '<br>歷史 Replay：<b>'+(replay.complete?'完成':'進行中')+'</b>｜待處理 '+String(replay.pending_eligible_decisions??'—')+
      '<br>認證/新單：<b>'+(ok?'Signal + Execution 雙認證通過':'Fail-closed，尚不可正式下單')+'</b>';
    const fixed=document.getElementById('fixedHorizon');
    if(fixed){fixed.className='notice '+(replay.complete?'g':'y');fixed.innerHTML='<b>固定歷史截止：</b>'+v25dt(d.fixed_replay_cutoff_ts)+'<br>此截止點不再隨現在時間移動；學習/認證期間新產生的 K 線不補進歷史 Replay。<br>策略完成後直接從當下市場開始實測，中間空窗刻意略過。'}
    const box=document.getElementById('pipelineStages');
    if(box){
      let rows=stages.map(x=>v25bar(String(x.name),x.percent,x.status,String(x.blocker||''))).join('');
      rows+=v25bar('9. SIGNAL_CERTIFICATION',sig.percent,(sig.percent>=100?'COMPLETE':'RUNNING'),'lineage '+String(sig.terminal_lineages||0)+' / '+String(sig.expected_lineages||0)+' · candidates '+String(sig.candidates_evaluated||0));
      rows+=v25bar('10. SEALED_OOS',sig.sealed_oos_percent,(sig.sealed_oos_percent>=100?'COMPLETE':'WAITING'),'opened '+String(sig.sealed_oos_opened||0)+' / '+String(sig.expected_lineages||0));
      rows+=v25bar('11. ENTRY_SL_TP_EXECUTION_AUDIT',ex.percent,(ex.execution_champions>0?'COMPLETE':(ex.signal_champions>0?'RUNNING':'WAITING')),'Signal Champion '+String(ex.signal_champions||0)+' · Execution Champion '+String(ex.execution_champions||0));
      rows+=v25bar('12. CURRENT_LIVE_HANDOFF',hand.percent,(hand.ready?'READY':'WAITING'),'完成後直接從目前時間開始，不追部署後空窗');
      box.innerHTML=rows;
    }
    const sp=document.getElementById('strategyProgress');
    if(sp){
      const ls=sig.lineages||[];
      let head='<h3>🧬 各策略族群 / 方向</h3><div class="v25grid"><div class="v25pill">交易標的：<b>'+String(tc.exchange||'bitget')+' '+String(tc.symbol||'ETHUSDT')+'</b></div><div class="v25pill">模擬名目：<b>'+Number(tc.paper_notional_usdt||20000).toLocaleString()+' USDT</b></div><div class="v25pill">槓桿：<b>'+String(tc.leverage_policy||'MAX_AVAILABLE_AT_ORDER_TIME')+'</b></div><div class="v25pill">Entry / SL / TP：<b>'+String(tc.entry_stop_targets_source||'EXECUTION_CHAMPION_ONLY')+'</b></div></div>';
      const lines=ls.length?ls.slice(0,40).map(x=>'<div class="v25lineage"><b>'+String(x.strategy||'—')+' '+String(x.direction||'—')+'</b> · '+String(x.status||'—')+' · gen '+String(x.generation??'—')+' · candidates '+String(x.candidates_evaluated??0)+' · PF '+Number(x.profit_factor||0).toFixed(2)+' · EV '+Number(x.expectancy_r||0).toFixed(3)+'R</div>').join(''):'<div class="v25lineage">候選族群尚未產生可顯示的 lineage 結果。</div>';
      sp.innerHTML=head+lines;
    }
    if(detail)detail.textContent=JSON.stringify({pipeline:s,detail:d},null,2);
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
