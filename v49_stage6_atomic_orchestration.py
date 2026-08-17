from __future__ import annotations

"""Atomic, durable Stage-6 orchestration authority.

V49 does not change research inputs, candidate genomes, folds, execution semantics,
fitness, costs, OOS gates or no-lookahead boundaries.  It fixes orchestration only:

* Stage 6 cannot start until every runtime overlay is installed.
* The generation/candidate loop commits a durable outer cursor after every candidate.
* Exact V46/V47 candidate checkpoints remain authoritative for resume.
* A normal exception in the single V26 background Future is surfaced and retried
  instead of leaving CERTIFICATION_RUNNING stale forever.
* The dashboard distinguishes path-simulation completion from candidate commit.
"""

import gc
import hashlib
import math
import random
import threading
import time
import traceback
from typing import Any

import runtime_identity

VERSION = 'V49_STAGE6_ATOMIC_ORCHESTRATION'
SCHEMA = 49
STATE_KEY = 'v49_stage6_atomic_orchestration'

_LOCK = threading.Lock()
_INSTALLED = False
_MONITOR_STARTED = False
_LAST_FUTURE_TOKEN: int | None = None
_PENDING_RETRY_AT = 0
_STARTUP_BARRIER_OPEN = False


def _now() -> int:
    return int(time.time())


def _state(core: Any, **patch: Any) -> dict[str, Any]:
    raw = core.state.get(STATE_KEY)
    out = dict(raw) if isinstance(raw, dict) else {}
    out.update(patch)
    out.update({'schema': SCHEMA, 'runtime': VERSION,
                'public_runtime': runtime_identity.RUNTIME_VERSION,
                'updated_at': _now()})
    core.state[STATE_KEY] = out
    return out


def mark_startup_barrier(core: Any, opened: bool, reason: str) -> None:
    global _STARTUP_BARRIER_OPEN
    _STARTUP_BARRIER_OPEN = bool(opened)
    _state(core, startup_barrier_open=bool(opened), startup_barrier_reason=str(reason))


def _durable_evolution_factory(core: Any, a: Any, throughput: Any):
    def durable_evolution(c: Any, snapshots: dict[str, Any], market: dict[str, Any]):
        run = str(throughput._run_fingerprint(c, a, snapshots, market))
        throughput._RUN_ID = run
        counts = throughput._counts(c, run)
        checkpoint = c.get_state(a.CHECKPOINT_KEY, {})
        checkpoint = dict(checkpoint) if isinstance(checkpoint, dict) else {}

        # A generation checkpoint from a different exact run may not seed this run.
        cp_run = str(checkpoint.get('v49_run_id') or checkpoint.get('v46_run_id') or '')
        if checkpoint.get('status') == 'RUNNING' and cp_run and cp_run != run:
            checkpoint = {}
            c.set_state(a.CHECKPOINT_KEY, {})
        elif checkpoint.get('status') == 'RUNNING' and not cp_run and counts.get('persisted', 0) == 0:
            # Pre-V46/V47 checkpoint: no exact candidate archive proves compatibility.
            checkpoint = {}
            c.set_state(a.CHECKPOINT_KEY, {})

        seed_base = int(hashlib.sha256(
            f'v30|{len(snapshots["ts"])}|{snapshots["ts"][-1]}'.encode()
        ).hexdigest()[:12], 16)
        rng = random.Random(seed_base)
        population = [a._new_genome(rng) for _ in range(a.POPULATION)]
        elites: list[tuple[float, dict[str, Any], dict[str, Any]]] = []
        archive: dict[str, tuple[float, dict[str, Any], dict[str, Any]]] = {}
        start_generation = 0

        if checkpoint.get('schema') == a.SCHEMA and checkpoint.get('status') == 'RUNNING':
            saved = checkpoint.get('elites') or []
            if saved:
                try:
                    elites = [(float(x['score']), dict(x['genome']), dict(x['result'])) for x in saved]
                    start_generation = max(0, min(a.GENERATIONS - 1, int(checkpoint.get('generation') or 0) + 1))
                    rr = random.Random(seed_base + start_generation * 100003)
                    population = []
                    while len(population) < a.POPULATION:
                        if elites and len(population) < int(a.POPULATION * .75):
                            population.append(a._new_genome(rr, rr.choice(elites)[1]))
                        else:
                            population.append(a._new_genome(rr))
                except Exception:
                    elites = []
                    start_generation = 0
                    rng = random.Random(seed_base)
                    population = [a._new_genome(rng) for _ in range(a.POPULATION)]

        _state(c, status='EVOLUTION_RUNNING', run_id=run,
               exact_candidate_resume=True, outer_cursor_durable=True,
               checkpoint_counts=counts, start_generation=start_generation + 1)

        for generation in range(start_generation, a.GENERATIONS):
            scored: list[tuple[float, dict[str, Any], dict[str, Any]]] = []
            for ci, genome in enumerate(population):
                gid = a._hash_payload(genome)
                live = {
                    'stage': 'DIRECT_R_AUTONOMOUS_EVOLUTION',
                    'generation': generation + 1, 'generations': a.GENERATIONS,
                    'candidate': ci + 1, 'population': len(population),
                    'candidate_id': gid, 'direction': genome['direction'],
                    'max_hold_bars': genome['max_hold_bars'],
                    'gate_conditions': genome['gate'],
                    'outer_status': 'EVALUATING', 'updated_at': _now(),
                }
                c.state['autonomous_live_progress'] = live
                _state(c, status='EVOLUTION_RUNNING', run_id=run,
                       current_generation=generation + 1, current_candidate=ci + 1,
                       current_candidate_id=gid, outer_status='EVALUATING')
                try:
                    result = a._evaluate_candidate(
                        snapshots, market, genome,
                        seed_base + generation * 1000 + ci * 17,
                    )
                except BaseException as exc:
                    err = f'{type(exc).__name__}: {exc}'
                    _state(c, status='CANDIDATE_ERROR', run_id=run,
                           current_generation=generation + 1, current_candidate=ci + 1,
                           current_candidate_id=gid, outer_status='ERROR', error=err,
                           traceback_tail='\n'.join(traceback.format_exc(limit=18).splitlines()[-24:]),
                           raw_data_deleted=False, replay_reset=False, future_peeking=False)
                    c.state.setdefault('learning', {})['error'] = err
                    raise

                candidate_status = 'NO_RESULT'
                if result is not None:
                    if not isinstance(result, dict):
                        err = f'invalid candidate result type {type(result).__name__}'
                        _state(c, status='CANDIDATE_RESULT_INVALID', error=err,
                               current_generation=generation + 1, current_candidate=ci + 1,
                               current_candidate_id=gid)
                        raise RuntimeError(err)
                    score_raw = result.get('score')
                    try:
                        score = float(score_raw)
                    except Exception as exc:
                        raise RuntimeError(f'candidate result missing numeric score: {score_raw!r}') from exc
                    if not math.isfinite(score):
                        raise RuntimeError(f'candidate result score is non-finite: {score!r}')
                    item = (score, genome, result)
                    scored.append(item)
                    prev = archive.get(gid)
                    if prev is None or item[0] > prev[0]:
                        archive[gid] = item
                    candidate_status = 'SCORED'

                # This is the authoritative OUTER commit point.  V46 has already
                # persisted the exact candidate result before returning here.
                committed = {
                    'generation': generation + 1, 'candidate': ci + 1,
                    'candidate_id': gid, 'status': candidate_status,
                    'completed_at': _now(), 'run_id': run,
                }
                c.set_state('v49_stage6_outer_cursor', committed)
                live = dict(c.state.get('autonomous_live_progress') or {})
                live.update({'outer_status': 'COMMITTED', 'candidate_committed_at': _now(),
                             'next_candidate': (ci + 2) if ci + 1 < len(population) else None,
                             'updated_at': _now()})
                c.state['autonomous_live_progress'] = live
                _state(c, status='EVOLUTION_RUNNING', run_id=run,
                       last_committed_candidate=committed,
                       checkpoint_counts=throughput._counts(c, run), outer_status='COMMITTED')

                if (ci + 1) % 6 == 0:
                    gc.collect()

            dedup: dict[str, tuple[float, dict[str, Any], dict[str, Any]]] = {}
            for item in elites + scored:
                gid = a._hash_payload(item[1])
                if gid not in dedup or item[0] > dedup[gid][0]:
                    dedup[gid] = item
            elites = sorted(dedup.values(), key=lambda z: z[0], reverse=True)[:a.ELITES]
            c.set_state(a.CHECKPOINT_KEY, {
                'schema': a.SCHEMA, 'status': 'RUNNING', 'generation': generation,
                'elites': [{'score': s, 'genome': g, 'result': r} for s, g, r in elites],
                'updated_at': _now(), 'v46_run_id': run, 'v49_run_id': run,
                'v49_generation_complete': True,
            })
            _state(c, status='GENERATION_COMMITTED', run_id=run,
                   generation_completed=generation + 1,
                   checkpoint_counts=throughput._counts(c, run), elites=len(elites))
            if generation == a.GENERATIONS - 1 or not elites:
                break
            rr = random.Random(seed_base + (generation + 1) * 100003)
            population = []
            while len(population) < a.POPULATION:
                if len(population) < int(a.POPULATION * .72):
                    population.append(a._new_genome(rr, rr.choice(elites)[1]))
                else:
                    population.append(a._new_genome(rr))
            gc.collect()

        # Reconstruct from the exact persisted V46 archive so a restart cannot lose
        # a previously completed non-elite candidate that belongs in the finalist set.
        rebuilt = throughput._finalists(c, a, run)
        if rebuilt:
            finalists = rebuilt
        else:
            ranked = sorted(archive.values(), key=lambda z: z[0], reverse=True)
            finalists: list[tuple[float, dict[str, Any], dict[str, Any]]] = []
            used: dict[tuple[Any, ...], int] = {}
            for item in ranked:
                key = a._diversity_key(item[1])
                if used.get(key, 0) >= 2:
                    continue
                used[key] = used.get(key, 0) + 1
                finalists.append(item)
                if len(finalists) >= a.FINALISTS:
                    break
        _state(c, status='DEVELOPMENT_EVOLUTION_COMPLETE', run_id=run,
               finalists_reconstructed=len(finalists), checkpoint_counts=throughput._counts(c, run))
        return finalists

    return durable_evolution


def _future_monitor_loop(core: Any, autonomous: Any, transition: Any, scheduler: Any) -> None:
    global _LAST_FUTURE_TOKEN, _PENDING_RETRY_AT
    while True:
        time.sleep(2.0)
        future = getattr(transition, '_CERT_FUTURE', None)
        if future is not None and future.done():
            token = id(future)
            if token != _LAST_FUTURE_TOKEN:
                _LAST_FUTURE_TOKEN = token
                exc = None
                try:
                    exc = future.exception()
                except BaseException as err:
                    exc = err
                if exc is not None:
                    err = f'{type(exc).__name__}: {exc}'
                    retry_at = _now() + 20
                    _PENDING_RETRY_AT = retry_at
                    try:
                        transition._persist(core, {
                            'status': 'CERTIFICATION_ERROR',
                            'certification_finished_at': _now(),
                            'ready_after': retry_at,
                            'error': err,
                            'reason': 'V49 surfaced a non-memory background exception; retry exact Stage-6 run without deleting historical data',
                            'raw_market_preserved': True,
                            'learning_samples_preserved': True,
                            'replay_cursor_preserved': True,
                        })
                    except Exception:
                        pass
                    core.state.setdefault('learning', {})['error'] = err
                    _state(core, status='BACKGROUND_FUTURE_ERROR', future_error=err,
                           retry_at=retry_at,
                           traceback_hint='see v49 candidate/orchestrator error state; no raw/replay deletion performed')
                else:
                    _state(core, last_background_future_completed_at=_now(), future_error=None)
        if _PENDING_RETRY_AT and _now() >= _PENDING_RETRY_AT:
            _PENDING_RETRY_AT = 0
            try:
                scheduler._kick(core, autonomous, transition,
                                source='v49_background_exception_retry', force_interval=True)
                _state(core, retry_requested_at=_now())
            except Exception as exc:
                _state(core, retry_request_error=f'{type(exc).__name__}: {exc}')


def _install_dashboard(core: Any) -> None:
    root = next((r for r in list(core.app.router.routes) if getattr(r, 'path', None) == '/'), None)
    old_root = getattr(root, 'endpoint', None)
    if not callable(old_root):
        return
    from fastapi.responses import HTMLResponse
    core.app.router.routes = [r for r in core.app.router.routes if getattr(r, 'path', None) != '/']

    @core.app.get('/', response_class=HTMLResponse, name='v49_stage6_atomic_dashboard')
    def dashboard_v49() -> str:
        raw = old_root()
        html = raw.body.decode() if hasattr(raw, 'body') else str(raw)
        card = '''<section class="card"><h2>🧭 Stage 6 外層提交 / 原子啟動</h2><div id="v49orchestration" class="notice">讀取 Stage 6 外層游標…</div></section>'''
        marker = '</div><div class="footer">'
        html = html.replace(marker, card + marker, 1) if marker in html else html.replace('</body>', card + '</body>')
        js = r'''<script id="v49-stage6-orchestration-ui">(function(){function e(x){return String(x??'—').replace(/[&<>"']/g,s=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[s]))}async function tick(){const el=document.getElementById('v49orchestration');if(!el)return;try{const r=await fetch(new URL('/api/v49/orchestration',location.href).href,{cache:'no-store'}),z=await r.json(),s=z.state||{},c=s.last_committed_candidate||{},o=z.outer_cursor||{};el.className='notice '+(s.error||s.future_error?'r':s.startup_barrier_open?'g':'y');el.innerHTML='<b>'+(s.startup_barrier_open?'ATOMIC STACK READY':'STARTUP BARRIER CLOSED')+'</b><br>Run：<code>'+e(s.run_id)+'</code><br>最後已提交 Candidate：'+e(c.generation||o.generation)+'/'+e(c.candidate||o.candidate)+' · '+e(c.status||o.status)+'<br>目前：'+e(s.current_generation)+'/'+e(s.current_candidate)+' · '+e(s.outer_status)+'<br>Checkpoint：'+e(JSON.stringify(s.checkpoint_counts||{}))+(s.error?'<br><b>Candidate error：</b>'+e(s.error):'')+(s.future_error?'<br><b>Background error：</b>'+e(s.future_error):'')}catch(x){el.className='notice r';el.textContent='V49 狀態讀取失敗：'+x}}tick();setInterval(tick,3000)})();</script>'''
        return html.replace('</body>', js + '</body>')


def install(production: Any, autonomous: Any, throughput: Any, integrity: Any,
            transition: Any, scheduler: Any) -> None:
    global _INSTALLED, _MONITOR_STARTED
    with _LOCK:
        if _INSTALLED:
            return
        _INSTALLED = True
    core = production.core

    # V49 changes orchestration code that can decide which exact candidate result is
    # resumed/committed, so include this module in V47's exact code identity BEFORE the
    # startup barrier is opened and before the first Stage-6 run fingerprint is made.
    mods = tuple(getattr(integrity, 'SEMANTIC_MODULES', ()))
    if 'v49_stage6_atomic_orchestration' not in mods:
        integrity.SEMANTIC_MODULES = mods + ('v49_stage6_atomic_orchestration',)

    autonomous._evolution = _durable_evolution_factory(core, autonomous, throughput)
    _state(core, installed=True, startup_barrier_open=_STARTUP_BARRIER_OPEN,
           status='READY_BEHIND_STARTUP_BARRIER',
           rules={
               'research_inputs_changed': False, 'features_changed': False,
               'candidate_search_space_changed': False, 'folds_changed': False,
               'fitness_changed': False, 'execution_semantics_changed': False,
               'oos_rules_changed': False, 'future_peeking_enabled': False,
               'raw_data_deleted': False, 'replay_reset': False,
               'outer_candidate_cursor_durable': True,
               'normal_background_exceptions_visible_and_retryable': True,
               'stage6_start_is_atomic_after_all_overlays': True,
           })

    if not _MONITOR_STARTED:
        _MONITOR_STARTED = True
        threading.Thread(target=_future_monitor_loop,
                         args=(core, autonomous, transition, scheduler),
                         name='stage6-v49-future-monitor', daemon=True).start()

    if not any(getattr(r, 'path', None) == '/api/v49/orchestration' for r in core.app.router.routes):
        @core.app.get('/api/v49/orchestration')
        def orchestration_status() -> dict[str, Any]:
            return {
                'schema': SCHEMA, 'runtime': VERSION,
                'state': dict(core.state.get(STATE_KEY) or {}),
                'outer_cursor': dict(core.get_state('v49_stage6_outer_cursor', {}) or {}),
                'autonomous_live_progress': dict(core.state.get('autonomous_live_progress') or {}),
                'transition': dict(core.get_state(getattr(transition, 'STATE_KEY', ''), {}) or {}),
                'rules': dict((core.state.get(STATE_KEY) or {}).get('rules') or {}),
            }
    _install_dashboard(core)
