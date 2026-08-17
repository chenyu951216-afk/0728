from __future__ import annotations

"""Runtime continuity and truthful Stage-6 liveness overlay.

This layer is deliberately semantic-neutral. It does not change historical inputs,
features, candidate genomes, trade simulation rules, OOS gates, costs, stops, targets,
or no-lookahead boundaries. It only:

* keeps Stage-6 resource use inside a safer envelope on 4C/8GB hosts;
* publishes a low-cost, single-request runtime snapshot for the dashboard;
* keeps a candidate-level heartbeat alive even while model/fold work has no path loop;
* prevents terminal replay from being presented as STRICT_REPLAY_ADVANCING; and
* treats rolling-redeploy/bootstrap API loss as transient instead of implying DB loss.
"""

import gc
import os
import threading
import time
from pathlib import Path
from typing import Any

import runtime_identity
import v16_runtime_integrity as runtime_integrity
import v43_unified_performance_authority as performance

VERSION = 'V48_RUNTIME_CONTINUITY_AUTHORITY'
SCHEMA = 48
STATE_KEY = 'v48_runtime_continuity_authority'
BOOT_ID = f"{os.getpid()}-{int(time.time())}"

_INSTALL_LOCK = threading.Lock()
_ACTIVE_LOCK = threading.Lock()
_INSTALLED = False
_ACTIVE: dict[str, Any] = {}
_MONITOR_STARTED = False


def _now() -> int:
    return int(time.time())


def _state(core: Any, **patch: Any) -> dict[str, Any]:
    raw = core.state.get(STATE_KEY)
    out = dict(raw) if isinstance(raw, dict) else {}
    out.update(patch)
    out.update({'schema': SCHEMA, 'runtime': VERSION, 'public_runtime': runtime_identity.RUNTIME_VERSION,
                'boot_id': BOOT_ID, 'pid': os.getpid(), 'updated_at': _now()})
    core.state[STATE_KEY] = out
    return out


def _memory() -> dict[str, Any]:
    try:
        return dict(performance._memory() or {})
    except Exception:
        return {'ratio': None, 'current_bytes': None, 'limit_bytes': None, 'rss_bytes': None}


def _sync_terminal_phase(core: Any, autonomous: Any) -> None:
    """Presentation/state normalization only; never advances replay or research."""
    try:
        replay = dict(runtime_integrity.replay_progress(core) or {})
    except Exception:
        return
    if not replay.get('complete'):
        return
    try:
        astate = str((autonomous.autonomous_status(core) or {}).get('status') or '')
    except Exception:
        astate = ''
    if astate in ('AUTONOMOUS_EVOLUTION_RUNNING', 'AUTONOMOUS_OOS_RUNNING', 'CERTIFICATION_RUNNING'):
        phase = 'AUTONOMOUS_DIRECT_R_EVOLUTION_RUNNING'
    elif astate in ('COMPLETE', 'COMPLETE_NO_CERTIFIED_PACKAGE'):
        phase = 'AUTONOMOUS_RESEARCH_COMPLETE'
    elif astate == 'WAITING_MARKET_CACHE':
        phase = 'WAITING_AUTONOMOUS_MARKET_CACHE_INTEGRITY'
    else:
        phase = 'AUTONOMOUS_RESEARCH_QUEUED'
    learning = core.state.setdefault('learning', {})
    if isinstance(learning, dict):
        learning['phase'] = phase
        for key in ('formal_stage', 'official_stage', 'certification_stage', 'stage'):
            if str(learning.get(key) or '') == 'STRICT_REPLAY_ADVANCING':
                learning[key] = phase
        cp = learning.get('certification_pipeline')
        if isinstance(cp, dict) and str(cp.get('stage') or '') == 'STRICT_REPLAY_ADVANCING':
            cp['stage'] = phase
            cp['reason'] = 'fixed historical replay is terminal; autonomous Stage 6 owns current research state'


def _safe_workers_factory(throughput: Any):
    max_workers = max(1, int(getattr(throughput, 'MAX_WORKERS', 1) or 1))
    def safe_workers() -> int:
        ratio_raw = _memory().get('ratio')
        if ratio_raw is None:
            return 1
        ratio = float(ratio_raw)
        if ratio >= .74:
            return 1
        if ratio >= .60:
            return min(2, max_workers)
        return min(3, max_workers)
    return safe_workers


def _candidate_monitor_loop(core: Any, autonomous: Any, throughput: Any) -> None:
    while True:
        time.sleep(2.0)
        with _ACTIVE_LOCK:
            active = dict(_ACTIVE)
        if not active.get('running'):
            continue
        started_mono = float(active.get('started_mono') or time.monotonic())
        live = dict(core.state.get('autonomous_live_progress') or {})
        live.update({'candidate_process_heartbeat_at': _now(),
                     'candidate_elapsed_seconds': round(time.monotonic() - started_mono, 1),
                     'candidate_process_alive': True, 'candidate_liveness_source': 'V48_WATCHDOG'})
        core.state['autonomous_live_progress'] = live
        _sync_terminal_phase(core, autonomous)
        _state(core, status='STAGE6_RUNNING', candidate_watchdog={
            'running': True, 'generation': active.get('generation'), 'candidate': active.get('candidate'),
            'candidate_id': active.get('candidate_id'), 'started_at': active.get('started_at'),
            'heartbeat_at': _now(), 'elapsed_seconds': round(time.monotonic() - started_mono, 1)},
            stage6_throughput=dict(core.state.get(getattr(throughput, 'STATE_KEY', 'v46_stage6_throughput_liveness')) or {}),
            memory=_memory())


def _start_monitor(core: Any, autonomous: Any, throughput: Any) -> None:
    global _MONITOR_STARTED
    if _MONITOR_STARTED:
        return
    _MONITOR_STARTED = True
    threading.Thread(target=_candidate_monitor_loop, args=(core, autonomous, throughput),
                     name='stage6-v48-liveness', daemon=True).start()


def _wrap_candidate(core: Any, autonomous: Any) -> None:
    base_eval = autonomous._evaluate_candidate
    def guarded_eval(snapshots: dict[str, Any], market: dict[str, Any], genome: dict[str, Any], seed: int):
        live = dict(core.state.get('autonomous_live_progress') or {})
        with _ACTIVE_LOCK:
            _ACTIVE.clear()
            _ACTIVE.update({'running': True, 'generation': live.get('generation'), 'candidate': live.get('candidate'),
                            'candidate_id': live.get('candidate_id') or autonomous._hash_payload(genome, 18),
                            'started_at': _now(), 'started_mono': time.monotonic()})
        mem_before = _memory()
        if float(mem_before.get('ratio') or 0.0) >= .74:
            try:
                performance._reclaim(core, snapshots, market, aggressive=True)
            except Exception:
                gc.collect()
        try:
            result = base_eval(snapshots, market, genome, seed)
            status = 'COMPLETE' if result is not None else 'NO_RESULT'
            return result
        except BaseException as exc:
            status = 'ERROR'
            _state(core, last_candidate_error=f'{type(exc).__name__}: {exc}')
            raise
        finally:
            with _ACTIVE_LOCK:
                started = float(_ACTIVE.get('started_mono') or time.monotonic()); last = dict(_ACTIVE)
                _ACTIVE.clear(); _ACTIVE['running'] = False
            elapsed = round(time.monotonic() - started, 3)
            try:
                reclaim = performance._reclaim(core, snapshots, market,
                    aggressive=float(_memory().get('ratio') or 0.0) >= .72)
            except Exception:
                reclaim = {'error': 'reclaim unavailable'}; gc.collect()
            _state(core, candidate_watchdog={'running': False, 'generation': last.get('generation'),
                'candidate': last.get('candidate'), 'candidate_id': last.get('candidate_id'),
                'started_at': last.get('started_at'), 'completed_at': _now(), 'elapsed_seconds': elapsed,
                'status': status}, last_candidate_reclaim=reclaim, memory=_memory())
    autonomous._evaluate_candidate = guarded_eval


def _storage_snapshot(core: Any) -> dict[str, Any]:
    cached = core.state.get('storage')
    out = dict(cached) if isinstance(cached, dict) else {}
    path = str(getattr(core, 'DB_PATH', os.getenv('DATABASE_PATH', '/data/eth_adaptive.db')))
    p = Path(path); exists = p.exists()
    out['database_path'] = path; out['database_exists'] = exists
    out['database_size_bytes'] = int(p.stat().st_size) if exists else None
    out['persistent_expected'] = path.startswith('/data/')
    if not out:
        out = {'status': 'BOOTING', 'database_path': path}
    return out


def _snapshot(core: Any, autonomous: Any, throughput: Any, integrity: Any) -> dict[str, Any]:
    _sync_terminal_phase(core, autonomous)
    try:
        replay = dict(runtime_integrity.replay_progress(core) or {})
    except Exception as exc:
        replay = {'error': f'{type(exc).__name__}: {exc}'}
    try:
        autonomous_status = dict(autonomous.autonomous_status(core) or {})
    except Exception as exc:
        autonomous_status = {'status': 'UNAVAILABLE', 'error': f'{type(exc).__name__}: {exc}'}
    tkey = getattr(throughput, 'STATE_KEY', 'v46_stage6_throughput_liveness')
    ikey = getattr(integrity, 'STATE_KEY', 'v47_dataset_integrity_authority')
    stage6 = dict(core.state.get(tkey) or {}); exact = dict(core.state.get(ikey) or {})
    watch = dict(core.state.get(STATE_KEY) or {}).get('candidate_watchdog') or {}
    return {'ok': True, 'schema': SCHEMA, 'runtime': VERSION, 'public_runtime': runtime_identity.RUNTIME_VERSION,
        'boot_id': BOOT_ID, 'pid': os.getpid(), 'generated_at': _now(), 'storage': _storage_snapshot(core),
        'replay': replay, 'autonomous': autonomous_status, 'stage6': stage6, 'exact_integrity': exact,
        'candidate_watchdog': watch, 'memory': _memory(), 'rules': {
            'research_semantics_changed': False, 'history_changed': False, 'feature_set_changed': False,
            'candidate_search_space_changed': False, 'oos_rules_changed': False, 'trade_simulation_changed': False,
            'future_peeking_enabled': False, 'rolling_deploy_endpoint_loss_is_transient': True,
            'storage_unavailable_response_does_not_mean_data_deleted': True}}


def _install_routes(core: Any, autonomous: Any, throughput: Any, integrity: Any) -> None:
    app = core.app
    if not any(getattr(r, 'path', None) == '/api/latest/autonomous' for r in app.router.routes):
        @app.get('/api/latest/autonomous')
        def latest_autonomous() -> dict[str, Any]: return autonomous.autonomous_status(core)
    if not any(getattr(r, 'path', None) == '/api/latest/storage' for r in app.router.routes):
        @app.get('/api/latest/storage')
        def latest_storage() -> dict[str, Any]: return _storage_snapshot(core)
    if not any(getattr(r, 'path', None) == '/api/latest/stage6' for r in app.router.routes):
        @app.get('/api/latest/stage6')
        def latest_stage6() -> dict[str, Any]:
            snap = _snapshot(core, autonomous, throughput, integrity)
            return {'ok': True, 'boot_id': snap['boot_id'], 'autonomous': snap['autonomous'], 'stage6': snap['stage6'],
                    'exact_integrity': snap['exact_integrity'], 'candidate_watchdog': snap['candidate_watchdog'],
                    'memory': snap['memory']}
    if not any(getattr(r, 'path', None) == '/api/v48/runtime-snapshot' for r in app.router.routes):
        @app.get('/api/v48/runtime-snapshot')
        def runtime_snapshot() -> dict[str, Any]: return _snapshot(core, autonomous, throughput, integrity)
    if not any(getattr(r, 'path', None) == '/api/latest/runtime-snapshot' for r in app.router.routes):
        @app.get('/api/latest/runtime-snapshot')
        def latest_runtime_snapshot() -> dict[str, Any]: return _snapshot(core, autonomous, throughput, integrity)


def _install_dashboard(core: Any) -> None:
    root = next((r for r in list(core.app.router.routes) if getattr(r, 'path', None) == '/'), None)
    old_root = getattr(root, 'endpoint', None)
    if not callable(old_root) or getattr(root, 'name', '') == 'v48_runtime_continuity_dashboard':
        return
    from fastapi.responses import HTMLResponse
    core.app.router.routes = [r for r in core.app.router.routes if getattr(r, 'path', None) != '/']
    @core.app.get('/', response_class=HTMLResponse, name='v48_runtime_continuity_dashboard')
    def dashboard_v48() -> str:
        raw = old_root(); html = raw.body.decode() if hasattr(raw, 'body') else str(raw)
        card = '''<section class="card"><h2>🛡️ Runtime Continuity / Stage 6 真實存活</h2><div id="v48continuity" class="notice">讀取單一一致性快照…</div></section>'''
        marker = '</div><div class="footer">'; html = html.replace(marker, card + marker, 1) if marker in html else html.replace('</body>', card + '</body>')
        js = r'''<script id="v48-runtime-continuity-ui">(function(){let lastGood=null;function e(x){return String(x??'—').replace(/[&<>"']/g,s=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[s]))}async function g(){const u=new URL('/api/latest/runtime-snapshot',window.location.href),r=await fetch(u.href,{cache:'no-store'}),t=await r.text();if(!r.ok)throw new Error('runtime snapshot HTTP '+r.status);try{return JSON.parse(t)}catch(_){throw new Error('runtime snapshot non-JSON')}}function p(z,stale){const el=document.getElementById('v48continuity');if(!el)return;const a=z.autonomous||{},q=a.progress||{},w=z.candidate_watchdog||{},m=z.memory||{},s=z.storage||{},hb=w.heartbeat_at||w.completed_at||0,age=hb?Math.max(0,Math.floor(Date.now()/1000-hb)):null,ratio=Number(m.ratio||0),running=!!w.running;el.className='notice '+(stale?'y':running?'g':'y');el.innerHTML='<b>'+(stale?'ROLLING RESTART / 保留最後正常快照':running?'STAGE 6 PROCESS ALIVE':'RUNTIME SNAPSHOT HEALTHY')+'</b><br>Boot：<code>'+e(z.boot_id)+'</code>｜Stage 6：'+Number(q.evolution_percent||0).toFixed(2)+'%<br>Candidate：'+e(w.generation||a.active?.generation)+'/'+e(a.active?.generations)+' · '+e(w.candidate||a.active?.candidate)+'/'+e(a.active?.population)+'<br>Candidate elapsed：'+e(w.elapsed_seconds)+'s'+(age!=null?'｜heartbeat age '+age+'s':'')+'<br>Memory：'+(ratio?Math.round(ratio*100)+'%':'—')+'｜Storage：'+e(s.healthy===true?'OK':s.database_exists===true?'PRESENT':'CHECKING')+(stale?'<br><b>API 暫時切換不代表 SQLite 被刪除；此時不要清 /data。</b>':'');const ae=document.getElementById('auto30status');if(ae&&!stale){ae.className='notice '+(a.live_ready?'g':'y');ae.innerHTML='<b>'+e(a.status||'WAITING')+'</b><br>Stage 6 '+Number(q.evolution_percent||0).toFixed(2)+'%｜Champions '+Number((a.champions||[]).length||0)+'｜單一快照 Boot '+e(z.boot_id)}}async function tick(){try{const z=await g();lastGood=z;try{sessionStorage.setItem('eth-v48-last-good',JSON.stringify(z))}catch(_){}p(z,false)}catch(x){if(!lastGood){try{lastGood=JSON.parse(sessionStorage.getItem('eth-v48-last-good')||'null')}catch(_){}}if(lastGood)p(lastGood,true);else{const el=document.getElementById('v48continuity');if(el){el.className='notice y';el.innerHTML='<b>Runtime 正在切換 / 重啟</b><br>'+e(x)+'<br>此狀態不代表資料被刪除。'}}}}tick();setInterval(tick,4000)})();</script>'''
        return html.replace('</body>', js + '</body>') if '</body>' in html else html + js


def install(production: Any, autonomous: Any, throughput: Any, integrity: Any) -> None:
    global _INSTALLED
    with _INSTALL_LOCK:
        if _INSTALLED: return
        _INSTALLED = True
    core = production.core
    throughput._workers = _safe_workers_factory(throughput)
    _wrap_candidate(core, autonomous); _start_monitor(core, autonomous, throughput)
    _install_routes(core, autonomous, throughput, integrity); _install_dashboard(core)
    _sync_terminal_phase(core, autonomous)
    _state(core, installed=True, status='READY', memory=_memory(), rules={
        'history_reduced': False, 'features_reduced': False, 'population_reduced': False,
        'generations_reduced': False, 'holding_horizons_reduced': False, 'oos_relaxed': False,
        'cost_model_changed': False, 'trade_simulator_changed': False, 'no_lookahead_changed': False,
        'candidate_worker_count_is_resource_only': True, 'candidate_watchdog_is_observability_only': True,
        'v47_exact_resume_identity_unchanged': True})
