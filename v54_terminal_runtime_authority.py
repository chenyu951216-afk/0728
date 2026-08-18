from __future__ import annotations

"""Final post-certification convergence/resource authority for the autonomous pipeline.

V54 intentionally changes no research semantics.  It is not part of the V47 exact
Stage-6 fingerprint.  It repairs only post-terminal runtime state that is reconstructed
from durable V30/V46/V47/V52 evidence after a restart, and tightens the scheduling
resource governor without changing candidate inputs, scalar trade results, chronology,
OOS gates, costs, stops/targets or no-lookahead boundaries.
"""

import time
from typing import Any, Callable

import runtime_identity

VERSION = 'V54_TERMINAL_RUNTIME_AUTHORITY'
SCHEMA = 54
STATE_KEY = 'v54_terminal_runtime_authority'
_INSTALLED = False
_BASE_RUN_PROGRESS: Callable[..., dict[str, Any]] | None = None
_BASE_WORKERS: Callable[[], int] | None = None


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


def _checkpoint(core: Any, autonomous: Any) -> dict[str, Any]:
    return _dict(core.get_state(autonomous.CHECKPOINT_KEY, {}))


def _champions(core: Any, autonomous: Any) -> list[dict[str, Any]]:
    try:
        return list(autonomous._load_registry(core, active_only=True) or [])
    except Exception:
        return []


def _terminal_truth(core: Any, autonomous: Any) -> tuple[bool, dict[str, Any], list[dict[str, Any]]]:
    cp = _checkpoint(core, autonomous)
    champions = _champions(core, autonomous)
    return str(cp.get('status') or '') == 'COMPLETE', cp, champions


def _persistent_run_identity(core: Any, checkpoint: dict[str, Any], throughput: Any) -> tuple[str, dict[str, Any]]:
    manifest = _dict(core.get_state('v47_last_stage6_manifest', {}))
    run = str(
        checkpoint.get('v53_run_id') or checkpoint.get('v49_run_id') or checkpoint.get('v46_run_id') or
        manifest.get('run_id') or getattr(throughput, '_RUN_ID', '') or ''
    )
    return run, manifest


def _restore_v47_summary(core: Any, integrity: Any, run: str, manifest: dict[str, Any], terminal: bool) -> None:
    if not terminal or not manifest.get('full_sha256'):
        return
    baseline = _dict(core.get_state('final_dataset_baseline_v1', {}))
    manifest_dataset = manifest.get('dataset_id')
    current_dataset = baseline.get('dataset_id')
    if manifest_dataset and current_dataset and str(manifest_dataset) != str(current_dataset):
        _state(core, integrity_restore_blocked='persistent V47 manifest dataset_id differs from current clean baseline')
        return
    current = _dict(core.state.get(integrity.STATE_KEY))
    current.update({
        'schema': int(getattr(integrity, 'SCHEMA', 47)),
        'runtime': str(getattr(integrity, 'VERSION', 'V47_DATASET_INTEGRITY_AUTHORITY')),
        'public_runtime': runtime_identity.RUNTIME_VERSION,
        'status': 'VERIFIED_TERMINAL_STAGE6_INPUT',
        'run_id': run or manifest.get('run_id'),
        'full_sha256': manifest.get('full_sha256'),
        'dataset_id': manifest_dataset,
        'hash_scope': manifest.get('hash_scope'),
        'restored_from_persistent_manifest': True,
        'terminal': True,
        'stale_candidate_reuse_possible': False,
        'exact_resume_requires_full_data_and_code_identity': True,
        'updated_at': _now(),
    })
    core.state[integrity.STATE_KEY] = current


def _restore_v49_summary(core: Any, autonomous: Any, throughput: Any, orchestration: Any,
                         run: str, terminal: bool) -> dict[str, int]:
    counts: dict[str, int] = {}
    if run:
        try:
            counts = {k: int(v or 0) for k, v in dict(throughput._counts(core, run) or {}).items()}
        except Exception:
            counts = {}
    if terminal:
        if run and not getattr(throughput, '_RUN_ID', None):
            throughput._RUN_ID = run
        outer = _dict(core.get_state('v49_stage6_outer_cursor', {}))
        total = int(autonomous.POPULATION) * int(autonomous.GENERATIONS)
        last = outer or {
            'generation': int(autonomous.GENERATIONS),
            'candidate': int(autonomous.POPULATION),
            'status': 'TERMINAL_CHECKPOINT',
            'run_id': run,
        }
        state = _dict(core.state.get(orchestration.STATE_KEY))
        state.update({
            'schema': int(getattr(orchestration, 'SCHEMA', 49)),
            'runtime': str(getattr(orchestration, 'VERSION', 'V49_STAGE6_ATOMIC_ORCHESTRATION')),
            'public_runtime': runtime_identity.RUNTIME_VERSION,
            'status': 'HISTORICAL_CERTIFICATION_COMPLETE',
            'run_id': run,
            'startup_barrier_open': True,
            'startup_barrier_reason': 'terminal autonomous certification is durable; Stage 6 boot kick is quiesced',
            'current_generation': int(autonomous.GENERATIONS),
            'current_candidate': int(autonomous.POPULATION),
            'outer_status': 'COMMITTED',
            'last_committed_candidate': last,
            'checkpoint_counts': counts,
            'expected_candidates': total,
            'terminal': True,
            'error': None,
            'future_error': None,
            'updated_at': _now(),
        })
        core.state[orchestration.STATE_KEY] = state
    return counts


def _normalize_terminal_progress(core: Any, autonomous: Any, cp: dict[str, Any], champions: list[dict[str, Any]]) -> None:
    active = _dict(core.state.get('autonomous_live_progress'))
    active.update({
        'stage': 'HISTORICAL_CERTIFICATION_COMPLETE',
        'terminal': True,
        'terminal_checkpoint_status': 'COMPLETE',
        'generation': int(autonomous.GENERATIONS),
        'generations': int(autonomous.GENERATIONS),
        'candidate': int(autonomous.POPULATION),
        'population': int(autonomous.POPULATION),
        'outer_status': 'COMMITTED',
        'champions': len(champions),
        'finalists': int(cp.get('finalists') or 0),
        'updated_at': int(cp.get('updated_at') or active.get('updated_at') or _now()),
        'v54_terminal_normalized': True,
    })
    core.state['autonomous_live_progress'] = active


def _sync_current_paper(core: Any, autonomous: Any, cp: dict[str, Any], champions: list[dict[str, Any]]) -> dict[str, Any]:
    ids = [str(x.get('strategy_id')) for x in champions if x.get('strategy_id')]
    ready = bool(str(cp.get('status') or '') == 'COMPLETE' and ids)
    handoff = _dict(core.state.get('v52_current_paper_handoff'))
    handoff.update({
        'ready': ready,
        'mode': 'CERTIFIED_CURRENT_PAPER' if ready else 'WAITING_FOR_CERTIFIED_CHAMPION',
        'strategy_ids': ids,
        'paper_only': True,
        'historical_replay_complete': True,
        'historical_certification_complete': str(cp.get('status') or '') == 'COMPLETE',
        'updated_at': _now(),
        'v54_authoritative': True,
    })
    core.state['v52_current_paper_handoff'] = handoff

    learning = core.state.setdefault('learning', {})
    if ready:
        for key in ('phase', 'formal_stage', 'official_stage', 'certification_stage', 'stage'):
            learning[key] = 'CURRENT_PAPER_MONITORING'
        pipe = _dict(learning.get('certification_pipeline'))
        pipe.update({
            'stage': 'CURRENT_PAPER_MONITORING',
            'reason': 'autonomous complete-package chronological OOS certification is authoritative',
            'signal_champions': len(ids),
            'execution_champions': len(ids),
            'autonomous_full_package_champions': len(ids),
            'legacy_split_signal_execution_pipeline_used': False,
            'current_paper_ready': True,
        })
        learning['certification_pipeline'] = pipe

        health = core.state.setdefault('subsystem_health', {})
        for name, mode in (
            ('learning', 'CURRENT_PAPER_MONITORING'),
            ('execution_audit', 'AUTONOMOUS_FULL_PACKAGE_CERTIFIED'),
        ):
            h = _dict(health.get(name))
            h.update({
                'status': 'OK',
                'consecutive_errors': 0,
                'last_success_at': _now(),
                'last_error': None,
                'mode': mode,
                'v54_terminal_normalized': True,
            })
            health[name] = h
    return handoff


def _shutdown_stage6_executor(throughput: Any, terminal: bool) -> bool:
    if not terminal:
        return False
    executor = getattr(throughput, '_EXECUTOR', None)
    if executor is None:
        return False
    try:
        executor.shutdown(wait=False, cancel_futures=True)
    except TypeError:
        executor.shutdown(wait=False)
    except Exception:
        return False
    try:
        throughput._EXECUTOR = None
    except Exception:
        pass
    return True


def _memory_ratio(performance: Any) -> float:
    try:
        raw = dict(performance._memory() or {})
        return float(raw.get('ratio') or 0.0)
    except Exception:
        return 0.0


def _install_resource_governor(throughput: Any, performance: Any) -> None:
    global _BASE_WORKERS
    if _BASE_WORKERS is not None:
        return
    _BASE_WORKERS = throughput._workers
    # 8-GB-class deployments were memory-bound, not CPU-bound.  Two scalar-equivalent
    # workers keep useful parallelism while preserving headroom for the web/API/SQLite.
    throughput.MAX_WORKERS = max(1, min(int(getattr(throughput, 'MAX_WORKERS', 1)), 2))
    throughput.CHUNK = max(8, min(int(getattr(throughput, 'CHUNK', 48)), 32))

    def governed_workers() -> int:
        cap = max(1, int(getattr(throughput, 'MAX_WORKERS', 1)))
        ratio = _memory_ratio(performance)
        if ratio >= 0.68:
            return 1
        if ratio >= 0.52:
            return min(2, cap)
        return cap

    throughput._workers = governed_workers


def _install_progress_authority(core: Any, autonomous: Any, pipeline52: Any) -> None:
    global _BASE_RUN_PROGRESS
    if _BASE_RUN_PROGRESS is not None:
        return
    _BASE_RUN_PROGRESS = pipeline52._run_progress

    def terminal_run_progress(c: Any, a: Any, throughput: Any) -> dict[str, Any]:
        out = dict(_BASE_RUN_PROGRESS(c, a, throughput) or {})
        cp = _dict(c.get_state(a.CHECKPOINT_KEY, {}))
        active = _dict(c.state.get('autonomous_live_progress'))
        if str(cp.get('status') or '') == 'COMPLETE' and str(active.get('stage') or '') == 'HISTORICAL_CERTIFICATION_COMPLETE':
            total = max(1, int(out.get('total_candidates') or (a.POPULATION * a.GENERATIONS)))
            out.update({
                'completed_candidates': total,
                'total_candidates': total,
                'evolution_percent': 100.0,
                'error': None,
                'terminal': True,
            })
        return out

    pipeline52._run_progress = terminal_run_progress


def reconcile(core: Any, autonomous: Any, throughput: Any, integrity: Any,
              orchestration: Any, pipeline52: Any, performance: Any) -> dict[str, Any]:
    terminal, cp, champions = _terminal_truth(core, autonomous)
    run, manifest = _persistent_run_identity(core, cp, throughput)

    if terminal:
        _normalize_terminal_progress(core, autonomous, cp, champions)
    _restore_v47_summary(core, integrity, run, manifest, terminal)
    counts = _restore_v49_summary(core, autonomous, throughput, orchestration, run, terminal)
    handoff = _sync_current_paper(core, autonomous, cp, champions) if terminal else _dict(core.state.get('v52_current_paper_handoff'))
    executor_released = _shutdown_stage6_executor(throughput, terminal)

    memory = {}
    try:
        memory = dict(performance._memory() or {})
    except Exception:
        memory = {}

    total = int(autonomous.POPULATION) * int(autonomous.GENERATIONS)
    status = (
        'CURRENT_PAPER_MONITORING' if terminal and champions and handoff.get('ready') else
        'HISTORICAL_CERTIFICATION_COMPLETE_NO_CHAMPION' if terminal else
        'AUTONOMOUS_RESEARCH_ACTIVE'
    )
    return _state(
        core,
        status=status,
        terminal=terminal,
        current_paper_ready=bool(handoff.get('ready')),
        champion_count=len(champions),
        champion_ids=[str(x.get('strategy_id')) for x in champions if x.get('strategy_id')],
        run_id=run,
        full_sha256=manifest.get('full_sha256'),
        checkpoint_counts=counts,
        expected_candidates=total,
        stage6_percent=100.0 if terminal else None,
        stage7_percent=100.0 if terminal else None,
        stage8_percent=100.0 if terminal and champions else 0.0 if terminal else None,
        stage9_percent=100.0 if terminal and champions and handoff.get('ready') else 0.0 if terminal else None,
        memory=memory,
        simulation_worker_cap=int(getattr(throughput, 'MAX_WORKERS', 1)),
        simulation_chunk=int(getattr(throughput, 'CHUNK', 0)),
        stage6_executor_released=executor_released,
        research_semantics_changed=False,
        v47_identity_changed=False,
        historical_data_deleted=False,
        replay_reset=False,
        oos_thresholds_changed=False,
        future_peeking_enabled=False,
    )


def terminal_boot_gate(core: Any, autonomous: Any, *, source: str) -> dict[str, Any] | None:
    terminal, cp, champions = _terminal_truth(core, autonomous)
    if not terminal or not champions:
        return None
    gate = {
        'schema': SCHEMA,
        'runtime': VERSION,
        'suppressed': True,
        'source': str(source),
        'reason': 'durable historical checkpoint + certified autonomous Champion already complete; do not re-kick Stage 6',
        'champions': len(champions),
        'checkpoint_updated_at': cp.get('updated_at'),
        'at': _now(),
    }
    core.state['v54_terminal_boot_gate'] = gate
    return gate


def _wrap_status_route(core: Any, path: str, reconcile_fn: Callable[[], Any]) -> None:
    app = core.app
    route = next((r for r in list(app.router.routes) if getattr(r, 'path', None) == path), None)
    old = getattr(route, 'endpoint', None)
    if not callable(old):
        return
    app.router.routes = [r for r in app.router.routes if getattr(r, 'path', None) != path]

    def wrapped():
        reconcile_fn()
        return old()

    app.add_api_route(path, wrapped, methods=['GET'])


def _install_dashboard(core: Any) -> None:
    root = next((r for r in list(core.app.router.routes) if getattr(r, 'path', None) == '/'), None)
    old = getattr(root, 'endpoint', None)
    if not callable(old):
        return
    from fastapi.responses import HTMLResponse
    core.app.router.routes = [r for r in core.app.router.routes if getattr(r, 'path', None) != '/']

    @core.app.get('/', response_class=HTMLResponse, name='v54_terminal_runtime_dashboard')
    def dashboard_v54() -> str:
        raw = old()
        html = raw.body.decode() if hasattr(raw, 'body') else str(raw)
        card = '''<section class="card"><h2>✅ Stage 1–9 最終權威 / Runtime Convergence</h2><div id="v54authority" class="notice">讀取 V54 最終狀態…</div></section>'''
        marker = '</div><div class="footer">'
        html = html.replace(marker, card + marker, 1) if marker in html else html.replace('</body>', card + '</body>')
        js = r'''<script id="v54-runtime-authority-ui">(function(){function e(x){return String(x??'—').replace(/[&<>"']/g,s=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[s]))}async function tick(){const el=document.getElementById('v54authority');if(!el)return;try{const r=await fetch('/api/v54/runtime-authority',{cache:'no-store'}),z=await r.json();el.className='notice '+(z.current_paper_ready?'g':z.terminal?'y':'y');let m=z.memory||{};el.innerHTML='<b>'+e(z.status)+'</b><br>Stage 1–5：歷史/資料/Replay 已由既有 authority 驗證｜Stage 6：'+e(z.stage6_percent??'RUNNING')+(Number.isFinite(Number(z.stage6_percent))?'%':'')+'｜Stage 7：'+e(z.stage7_percent??'WAITING')+(Number.isFinite(Number(z.stage7_percent))?'%':'')+'｜Stage 8：'+e(z.stage8_percent??'WAITING')+(Number.isFinite(Number(z.stage8_percent))?'%':'')+'｜Stage 9：'+e(z.stage9_percent??'WAITING')+(Number.isFinite(Number(z.stage9_percent))?'%':'')+'<br>Champion：<b>'+e(z.champion_count)+'</b>｜Run：<code>'+e(z.run_id)+'</code><br>V47 SHA-256：<code>'+e(z.full_sha256)+'</code><br>資源：workers cap '+e(z.simulation_worker_cap)+'｜chunk '+e(z.simulation_chunk)+(m.ratio!=null?'｜memory '+(Number(m.ratio)*100).toFixed(1)+'%':'')+'<br><b>規則：</b>不刪歷史、不重跑 Replay、不放寬 OOS、不改策略結果、不窺視未來；認證完成後停止 Stage 6 重啟與釋放模擬 executor。';}catch(x){el.className='notice r';el.textContent='V54 authority 讀取失敗：'+String(x)}}tick();setInterval(tick,3000)})();</script>'''
        return html.replace('</body>', js + '</body>') if '</body>' in html else html + js


def install(production: Any, autonomous: Any, throughput: Any, integrity: Any,
            orchestration: Any, pipeline52: Any, performance: Any) -> None:
    global _INSTALLED
    if _INSTALLED:
        return
    _INSTALLED = True
    core = production.core

    _install_resource_governor(throughput, performance)
    _install_progress_authority(core, autonomous, pipeline52)

    def sync() -> dict[str, Any]:
        return reconcile(core, autonomous, throughput, integrity, orchestration, pipeline52, performance)

    # Normalize before the first user-visible status read, then make the legacy status
    # endpoints converge on the autonomous full-package authority on every request.
    sync()
    for path in ('/api/status', '/api/stability', '/api/latest/pipeline', '/api/latest/progress-detail'):
        _wrap_status_route(core, path, sync)

    if not any(getattr(r, 'path', None) == '/api/v54/runtime-authority' for r in core.app.router.routes):
        @core.app.get('/api/v54/runtime-authority')
        def runtime_authority_api() -> dict[str, Any]:
            return sync()

    _install_dashboard(core)
    core.state.setdefault('strict_replay', {})['v54_terminal_runtime_authority'] = {
        'schema': SCHEMA,
        'research_semantics_changed': False,
        'v47_exact_identity_changed': False,
        'raw_history_deleted': False,
        'replay_reset': False,
        'candidate_archive_reset': False,
        'oos_thresholds_changed': False,
        'future_peeking_enabled': False,
        'terminal_progress_is_100_when_durable_checkpoint_complete': True,
        'persistent_v47_summary_restored_after_restart': True,
        'persistent_v49_run_diagnostics_restored_after_restart': True,
        'legacy_health_normalized_to_autonomous_current_paper': True,
        'stage6_executor_released_after_terminal': True,
        'memory_pressure_reduces_parallelism_not_research_scope': True,
    }
    runtime_identity.stamp(core)
