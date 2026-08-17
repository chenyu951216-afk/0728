from __future__ import annotations

"""Stage-6 evolution survivability and truthful candidate diagnostics.

V50 fixed a hard sklearn seed crash.  The next surfaced failure was semantic: a
candidate that could not accumulate two fully-scored development folds returned None.
When every member of generation 1 returned None, the evolutionary loop had no elites,
stopped immediately, and the UI later presented development/generalization as complete
although no package had ever obtained a development score.

V51 keeps the strict chronological/OOS contract but gives *development search* a
causal feasibility signal.  Candidates that are not yet eligible for final OOS receive
only a low, finite SEARCH_ONLY score derived from pre-holdout walk-forward feasibility.
They may be parents for mutation, but can never become finalists or champions.  Final
OOS thresholds are unchanged.  Data-path invalidity remains fail-closed.
"""

from collections import Counter
import gc
import hashlib
import math
import random
import statistics
import time
import traceback
from typing import Any

import numpy as np

import runtime_identity

VERSION = 'V51_EVOLUTION_SURVIVABILITY_AUTHORITY'
SCHEMA = 51
STATE_KEY = 'v51_evolution_survivability_authority'

_INSTALLED = False
_BASE_STATUS: Any | None = None


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


def _reason_counts(results: list[dict[str, Any]], counter: Counter[str]) -> tuple[int, int]:
    invalid = 0
    attempted = 0
    for item in results:
        attempted += 1
        reason = str(item.get('reason') or ('filled' if item.get('filled') else 'unknown'))
        counter[reason] += 1
        if not item.get('valid'):
            invalid += 1
    return attempted, invalid


def _support_progress(fit_n: int, cal_n: int, test_n: int) -> float:
    return float((min(1.0, fit_n / 140.0) + min(1.0, cal_n / 50.0) + min(1.0, test_n / 60.0)) / 3.0)


def _search_only_result(fold_progress: list[float], fold_stats: list[dict[str, float]],
                        diagnostics: list[dict[str, Any]], reason_counts: Counter[str],
                        attempted: int, invalid: int) -> dict[str, Any]:
    # This score is deliberately far below normal direct-R development scores.  Its
    # only purpose is to give evolution an ordering when no candidate is yet eligible.
    # It never participates in final OOS selection.
    mean_progress = statistics.mean(fold_progress) if fold_progress else 0.0
    best_progress = max(fold_progress) if fold_progress else 0.0
    partial_ev = statistics.mean([float(x['ev']) for x in fold_stats]) if fold_stats else 0.0
    score = -100.0 + 12.0 * mean_progress + 4.0 * best_progress + 3.0 * len(fold_stats)
    score += max(-1.0, min(1.0, partial_ev))
    return {
        'score': float(score),
        'eligible_for_finalist': False,
        'development_status': 'SEARCH_ONLY_INSUFFICIENT_WALK_FORWARD',
        'completed_development_folds': int(len(fold_stats)),
        'fold_progress_mean': float(mean_progress),
        'fold_progress_best': float(best_progress),
        'folds': list(fold_stats),
        'fold_diagnostics': diagnostics,
        'path_reason_counts': dict(reason_counts),
        'path_attempts': int(attempted),
        'invalid_paths': int(invalid),
        'invalid_path_fraction': float(invalid / max(1, attempted)),
        'future_holdout_used_for_search_score': False,
        'final_oos_eligible': False,
    }


def _evaluate_factory(core: Any, a: Any):
    def evaluate(snapshots: dict[str, Any], market: dict[str, Any], genome: dict[str, Any], seed: int) -> dict[str, Any]:
        ts = snapshots['ts']; x = snapshots['x']; n = len(ts)
        holdout_start = int(n * (1.0 - a.FINAL_HOLDOUT_PCT))
        dev_end = max(2000, holdout_start - 32)
        if dev_end < 3000:
            raise RuntimeError(f'Stage6 development history contract too short: dev_end={dev_end}, n={n}')

        anchors = (.48, .66, .82)
        fold_stats: list[dict[str, float]] = []
        fold_thresholds: list[float] = []
        fold_progress: list[float] = []
        diagnostics: list[dict[str, Any]] = []
        reasons: Counter[str] = Counter()
        attempted_paths = 0
        invalid_paths = 0

        for fi, frac in enumerate(anchors):
            d: dict[str, Any] = {'fold': fi + 1, 'status': 'STARTED'}
            test_start = int(dev_end * frac)
            test_end = int(dev_end * (anchors[fi + 1] if fi + 1 < len(anchors) else .98))
            train_end = max(800, test_start - 32)
            if test_end - test_start < 200:
                d.update(status='FOLD_WINDOW_TOO_SHORT', progress=0.0)
                diagnostics.append(d); fold_progress.append(0.0); continue
            fit_end = max(600, int(train_end * .80) - 16)
            cal_start = min(train_end - 120, fit_end + 16)
            if cal_start <= fit_end or train_end - cal_start < 100:
                d.update(status='CALIBRATION_WINDOW_TOO_SHORT', progress=0.02)
                diagnostics.append(d); fold_progress.append(0.02); continue

            thresholds = a._gate_thresholds(x[:fit_end], genome['gate'])
            stride_mask = a._decision_mask(ts, int(genome['decision_stride']))
            fit_mask = a._gate_mask(x[:fit_end], thresholds) & stride_mask[:fit_end]
            cal_mask = a._gate_mask(x[cal_start:train_end], thresholds) & stride_mask[cal_start:train_end]
            test_mask = a._gate_mask(x[test_start:test_end], thresholds) & stride_mask[test_start:test_end]
            fit_idx = np.where(fit_mask)[0]
            cal_idx = np.where(cal_mask)[0] + cal_start
            test_idx = np.where(test_mask)[0] + test_start
            support = _support_progress(len(fit_idx), len(cal_idx), len(test_idx))
            d.update(fit_support=int(len(fit_idx)), cal_support=int(len(cal_idx)),
                     test_support=int(len(test_idx)), support_progress=round(support, 6))
            if len(fit_idx) < 140 or len(cal_idx) < 50 or len(test_idx) < 60:
                p = 0.28 * support
                d.update(status='INSUFFICIENT_CAUSAL_STATE_SUPPORT', progress=p)
                diagnostics.append(d); fold_progress.append(p); continue

            fit_idx = a._sample_evenly(fit_idx, a.TRAIN_SIM_CAP)
            cal_idx = a._sample_evenly(cal_idx, a.CAL_SIM_CAP)
            test_idx = a._sample_evenly(test_idx, a.TEST_SIM_CAP)
            x_fit, y_fit, fit_results = a._simulate_indices(fit_idx, snapshots, market, genome)
            x_cal, y_cal, cal_results = a._simulate_indices(cal_idx, snapshots, market, genome)
            n1, bad1 = _reason_counts(fit_results, reasons); n2, bad2 = _reason_counts(cal_results, reasons)
            attempted_paths += n1 + n2; invalid_paths += bad1 + bad2
            fill_progress = (min(1.0, len(y_fit) / 100.0) + min(1.0, len(y_cal) / 30.0)) / 2.0
            d.update(fit_fills=int(len(y_fit)), cal_fills=int(len(y_cal)), fill_progress=round(fill_progress, 6))
            if len(y_fit) < 100 or len(y_cal) < 30 or float(np.std(y_fit)) < 1e-6:
                p = 0.28 + 0.30 * fill_progress
                d.update(status='INSUFFICIENT_FILLED_TRAINING', progress=p,
                         y_fit_std=float(np.std(y_fit)) if len(y_fit) else 0.0)
                diagnostics.append(d); fold_progress.append(p); continue

            m = a._model(genome, seed + fi * 31)
            m.fit(a._feature_subset_matrix(x_fit, genome), y_fit)
            pred_cal = m.predict(a._feature_subset_matrix(x_cal, genome))
            picked = a._threshold_from_cal(pred_cal, y_cal)
            if picked is None:
                d.update(status='NO_DEVELOPMENT_SELECTION_THRESHOLD', progress=0.64)
                diagnostics.append(d); fold_progress.append(0.64)
                del m, x_fit, y_fit, x_cal, y_cal, pred_cal; gc.collect(); continue
            threshold, _ = picked
            pred_test = m.predict(a._feature_subset_matrix(snapshots['x'][test_idx], genome))
            selected_idx = test_idx[pred_test >= threshold]
            d.update(selected_test=int(len(selected_idx)), threshold=float(threshold))
            if len(selected_idx) < 20:
                p = 0.64 + 0.14 * min(1.0, len(selected_idx) / 20.0)
                d.update(status='INSUFFICIENT_SELECTED_TEST_SUPPORT', progress=p)
                diagnostics.append(d); fold_progress.append(p)
                del m, x_fit, y_fit, x_cal, y_cal, pred_cal, pred_test; gc.collect(); continue

            selected_idx = a._sample_evenly(selected_idx, a.TEST_SIM_CAP)
            _, _, test_results = a._simulate_indices(selected_idx, snapshots, market, genome)
            n3, bad3 = _reason_counts(test_results, reasons)
            attempted_paths += n3; invalid_paths += bad3
            st = a._stats(test_results)
            d.update(test_fills=int(st['fills']), test_ev=float(st['ev']), test_pf=float(st['pf']))
            if st['fills'] < 16:
                p = 0.78 + 0.18 * min(1.0, st['fills'] / 16.0)
                d.update(status='INSUFFICIENT_TEST_FILLS', progress=p)
                diagnostics.append(d); fold_progress.append(p)
                del m, x_fit, y_fit, x_cal, y_cal, pred_cal, pred_test; gc.collect(); continue

            fold_stats.append(st); fold_thresholds.append(float(threshold))
            d.update(status='DEVELOPMENT_FOLD_SCORED', progress=1.0)
            diagnostics.append(d); fold_progress.append(1.0)
            del m, x_fit, y_fit, x_cal, y_cal, pred_cal, pred_test; gc.collect()

        invalid_fraction = invalid_paths / max(1, attempted_paths)
        # Missing/gapped future settlement is a data contract failure, not a strategy
        # weakness.  Never let evolution "learn around" broken causal market paths.
        if attempted_paths >= 200 and invalid_fraction > 0.05:
            top = ', '.join(f'{k}={v}' for k, v in reasons.most_common(6))
            raise RuntimeError(
                f'Stage6 causal settlement alignment invalid: {invalid_paths}/{attempted_paths} '
                f'({invalid_fraction:.2%}) invalid paths; reasons: {top}'
            )

        if len(fold_stats) < 2:
            result = _search_only_result(fold_progress, fold_stats, diagnostics, reasons,
                                         attempted_paths, invalid_paths)
            _state(core, status='SEARCH_ONLY_CANDIDATE', last_candidate_diagnostics={
                'development_status': result['development_status'],
                'completed_folds': result['completed_development_folds'],
                'fold_progress_mean': result['fold_progress_mean'],
                'fold_diagnostics': diagnostics,
                'path_reason_counts': dict(reasons),
                'invalid_path_fraction': result['invalid_path_fraction'],
            })
            return result

        evs = [z['ev'] for z in fold_stats]; pfs = [z['pf'] for z in fold_stats]
        stability = a._clamp(1.0 - .62 * statistics.pstdev(evs) - .025 * statistics.pstdev(pfs), 0.0, 1.0)
        profitable = sum(v > 0 for v in evs) / len(evs); worst = min(evs)
        fills = sum(z['fills'] for z in fold_stats); avg_ev = statistics.mean(evs)
        avg_pf = statistics.mean(pfs); avg_dd = statistics.mean(z['dd'] for z in fold_stats)
        score = avg_ev * 5.0 + math.log(max(avg_pf, 1e-6)) * .38 + stability * .32 + profitable * .24 - avg_dd * .006 + min(worst, .12)
        result = {
            'score': float(score), 'ev': float(avg_ev), 'pf': float(avg_pf), 'dd': float(avg_dd),
            'stability': float(stability), 'profitable_folds': float(profitable),
            'worst_fold_ev': float(worst), 'development_fills': int(fills),
            'threshold_hint': float(statistics.median(fold_thresholds)), 'folds': fold_stats,
            'eligible_for_finalist': True, 'final_oos_eligible': True,
            'development_status': 'DEVELOPMENT_WALK_FORWARD_ELIGIBLE',
            'fold_diagnostics': diagnostics, 'path_reason_counts': dict(reasons),
            'path_attempts': int(attempted_paths), 'invalid_paths': int(invalid_paths),
            'invalid_path_fraction': float(invalid_fraction),
            'future_holdout_used_for_search_score': False,
        }
        _state(core, status='ELIGIBLE_DEVELOPMENT_CANDIDATE', last_candidate_diagnostics={
            'development_status': result['development_status'], 'completed_folds': len(fold_stats),
            'score': result['score'], 'ev': result['ev'], 'pf': result['pf'],
            'path_reason_counts': dict(reasons), 'invalid_path_fraction': invalid_fraction,
        })
        return result
    return evaluate


def _eligible(item: tuple[float, dict[str, Any], dict[str, Any]]) -> bool:
    try:
        return bool(item[2].get('eligible_for_finalist', True))
    except Exception:
        return False


def _evolution_factory(core: Any, a: Any, throughput: Any, orchestration: Any):
    def evolution(c: Any, snapshots: dict[str, Any], market: dict[str, Any]):
        run = str(throughput._run_fingerprint(c, a, snapshots, market))
        throughput._RUN_ID = run
        counts = throughput._counts(c, run)
        checkpoint = c.get_state(a.CHECKPOINT_KEY, {})
        checkpoint = dict(checkpoint) if isinstance(checkpoint, dict) else {}
        cp_run = str(checkpoint.get('v51_run_id') or checkpoint.get('v49_run_id') or checkpoint.get('v46_run_id') or '')
        if checkpoint.get('status') == 'RUNNING' and cp_run and cp_run != run:
            checkpoint = {}; c.set_state(a.CHECKPOINT_KEY, {})
        elif checkpoint.get('status') == 'RUNNING' and not cp_run and counts.get('persisted', 0) == 0:
            checkpoint = {}; c.set_state(a.CHECKPOINT_KEY, {})

        seed_base = int(hashlib.sha256(f'v30|{len(snapshots["ts"])}|{snapshots["ts"][-1]}'.encode()).hexdigest()[:12], 16)
        rng = random.Random(seed_base)
        population = [a._new_genome(rng) for _ in range(a.POPULATION)]
        elites: list[tuple[float, dict[str, Any], dict[str, Any]]] = []
        archive: dict[str, tuple[float, dict[str, Any], dict[str, Any]]] = {}
        start_generation = 0

        if checkpoint.get('schema') == a.SCHEMA and checkpoint.get('status') == 'RUNNING':
            saved = checkpoint.get('elites') or []
            if saved:
                try:
                    elites = [(float(z['score']), dict(z['genome']), dict(z['result'])) for z in saved]
                    start_generation = max(0, min(a.GENERATIONS - 1, int(checkpoint.get('generation') or 0) + 1))
                    rr = random.Random(seed_base + start_generation * 100003)
                    population = []
                    while len(population) < a.POPULATION:
                        if elites and len(population) < int(a.POPULATION * .75):
                            population.append(a._new_genome(rr, rr.choice(elites)[1]))
                        else:
                            population.append(a._new_genome(rr))
                except Exception:
                    elites = []; start_generation = 0
                    rng = random.Random(seed_base)
                    population = [a._new_genome(rng) for _ in range(a.POPULATION)]

        orchestration._state(c, status='EVOLUTION_RUNNING', run_id=run,
                             exact_candidate_resume=True, outer_cursor_durable=True,
                             checkpoint_counts=counts, start_generation=start_generation + 1,
                             v51_search_only_parenting=True)
        _state(c, status='EVOLUTION_RUNNING', run_id=run, generation=start_generation + 1,
               rule='SEARCH_ONLY may parent mutations but can never become finalist')

        for generation in range(start_generation, a.GENERATIONS):
            scored: list[tuple[float, dict[str, Any], dict[str, Any]]] = []
            eligible_this_generation = 0
            search_only_this_generation = 0
            for ci, genome in enumerate(population):
                gid = a._hash_payload(genome)
                live = {'stage': 'DIRECT_R_AUTONOMOUS_EVOLUTION', 'generation': generation + 1,
                        'generations': a.GENERATIONS, 'candidate': ci + 1, 'population': len(population),
                        'candidate_id': gid, 'direction': genome['direction'],
                        'max_hold_bars': genome['max_hold_bars'], 'gate_conditions': genome['gate'],
                        'outer_status': 'EVALUATING', 'updated_at': _now()}
                c.state['autonomous_live_progress'] = live
                orchestration._state(c, status='EVOLUTION_RUNNING', run_id=run,
                                     current_generation=generation + 1, current_candidate=ci + 1,
                                     current_candidate_id=gid, outer_status='EVALUATING')
                try:
                    result = a._evaluate_candidate(snapshots, market, genome,
                                                   seed_base + generation * 1000 + ci * 17)
                except BaseException as exc:
                    err = f'{type(exc).__name__}: {exc}'
                    orchestration._state(c, status='CANDIDATE_ERROR', run_id=run,
                                         current_generation=generation + 1, current_candidate=ci + 1,
                                         current_candidate_id=gid, outer_status='ERROR', error=err,
                                         traceback_tail='\n'.join(traceback.format_exc(limit=18).splitlines()[-24:]),
                                         raw_data_deleted=False, replay_reset=False, future_peeking=False)
                    c.state.setdefault('learning', {})['error'] = err
                    _state(c, status='FAIL_CLOSED_DATA_OR_MODEL_ERROR', error=err)
                    raise

                if not isinstance(result, dict):
                    raise RuntimeError(f'V51 candidate evaluator returned invalid type {type(result).__name__}')
                score = float(result.get('score'))
                if not math.isfinite(score):
                    raise RuntimeError(f'V51 candidate result score is non-finite: {score!r}')
                item = (score, genome, result); scored.append(item)
                prev = archive.get(gid)
                if prev is None or score > prev[0]: archive[gid] = item
                final_eligible = bool(result.get('eligible_for_finalist', False))
                if final_eligible: eligible_this_generation += 1; candidate_status = 'SCORED_ELIGIBLE'
                else: search_only_this_generation += 1; candidate_status = 'SEARCH_ONLY'

                committed = {'generation': generation + 1, 'candidate': ci + 1,
                             'candidate_id': gid, 'status': candidate_status,
                             'development_status': result.get('development_status'),
                             'completed_at': _now(), 'run_id': run}
                c.set_state('v49_stage6_outer_cursor', committed)
                live = dict(c.state.get('autonomous_live_progress') or {})
                live.update({'outer_status': 'COMMITTED', 'candidate_committed_at': _now(),
                             'candidate_result_class': candidate_status,
                             'next_candidate': (ci + 2) if ci + 1 < len(population) else None,
                             'updated_at': _now()})
                c.state['autonomous_live_progress'] = live
                orchestration._state(c, status='EVOLUTION_RUNNING', run_id=run,
                                     last_committed_candidate=committed,
                                     checkpoint_counts=throughput._counts(c, run), outer_status='COMMITTED')
                if (ci + 1) % 6 == 0: gc.collect()

            dedup: dict[str, tuple[float, dict[str, Any], dict[str, Any]]] = {}
            for item in elites + scored:
                gid = a._hash_payload(item[1])
                if gid not in dedup or item[0] > dedup[gid][0]: dedup[gid] = item
            elites = sorted(dedup.values(), key=lambda z: z[0], reverse=True)[:a.ELITES]
            c.set_state(a.CHECKPOINT_KEY, {'schema': a.SCHEMA, 'status': 'RUNNING', 'generation': generation,
                'elites': [{'score': s, 'genome': g, 'result': r} for s, g, r in elites],
                'updated_at': _now(), 'v46_run_id': run, 'v49_run_id': run, 'v51_run_id': run,
                'v49_generation_complete': True, 'v51_generation_complete': True,
                'eligible_this_generation': eligible_this_generation,
                'search_only_this_generation': search_only_this_generation})
            orchestration._state(c, status='GENERATION_COMMITTED', run_id=run,
                                 generation_completed=generation + 1,
                                 checkpoint_counts=throughput._counts(c, run), elites=len(elites),
                                 eligible_this_generation=eligible_this_generation,
                                 search_only_this_generation=search_only_this_generation)
            _state(c, status='GENERATION_COMMITTED', run_id=run, generation_completed=generation + 1,
                   eligible_this_generation=eligible_this_generation,
                   search_only_this_generation=search_only_this_generation,
                   elites=len(elites))
            if generation == a.GENERATIONS - 1:
                break
            rr = random.Random(seed_base + (generation + 1) * 100003)
            population = []
            # Even if a future code path yields no scored elites, continue deterministic
            # exploration rather than falsely declaring research complete after gen 1.
            while len(population) < a.POPULATION:
                if elites and len(population) < int(a.POPULATION * .72):
                    population.append(a._new_genome(rr, rr.choice(elites)[1]))
                else:
                    population.append(a._new_genome(rr))
            gc.collect()

        # Final OOS receives only candidates that completed >=2 scored development
        # folds. SEARCH_ONLY parents are categorically excluded regardless of score.
        rebuilt = [item for item in throughput._finalists(c, a, run) if _eligible(item)]
        pool = rebuilt if rebuilt else [item for item in archive.values() if _eligible(item)]
        ranked = sorted(pool, key=lambda z: z[0], reverse=True)
        finalists: list[tuple[float, dict[str, Any], dict[str, Any]]] = []
        used: dict[tuple[Any, ...], int] = {}
        for item in ranked:
            key = a._diversity_key(item[1])
            if used.get(key, 0) >= 2: continue
            used[key] = used.get(key, 0) + 1; finalists.append(item)
            if len(finalists) >= a.FINALISTS: break

        orchestration._state(c, status='DEVELOPMENT_EVOLUTION_COMPLETE', run_id=run,
                             finalists_reconstructed=len(finalists),
                             checkpoint_counts=throughput._counts(c, run),
                             search_only_excluded_from_finalists=True,
                             generations_executed=a.GENERATIONS)
        _state(c, status='DEVELOPMENT_EVOLUTION_COMPLETE', run_id=run,
               finalists=len(finalists), generations_executed=a.GENERATIONS,
               search_only_excluded_from_finalists=True)
        return finalists
    return evolution


def _patch_status(core: Any, a: Any) -> None:
    global _BASE_STATUS
    if getattr(a, '_v51_status_installed', False): return
    _BASE_STATUS = a.autonomous_status
    def status(c: Any) -> dict[str, Any]:
        z = dict(_BASE_STATUS(c) or {})
        z['v51'] = dict(c.state.get(STATE_KEY) or {})
        progress = dict(z.get('progress') or {})
        cp = c.get_state(a.CHECKPOINT_KEY, {})
        cp = dict(cp) if isinstance(cp, dict) else {}
        # A terminal run with zero finalists did not perform a package OOS audit.
        if z.get('research_complete') and not z.get('research_best') and int(cp.get('finalists') or 0) == 0:
            progress['oos_percent'] = 0.0
            progress['oos_state'] = 'SKIPPED_NO_ELIGIBLE_DEVELOPMENT_FINALIST'
        z['progress'] = progress
        return z
    a.autonomous_status = status
    a._v51_status_installed = True


def install(production: Any, autonomous: Any, throughput: Any, integrity: Any,
            runtime_continuity: Any, orchestration: Any) -> None:
    global _INSTALLED
    if _INSTALLED: return
    _INSTALLED = True
    core = production.core

    modules = tuple(getattr(integrity, 'SEMANTIC_MODULES', ()))
    if 'v51_evolution_survivability_authority' not in modules:
        integrity.SEMANTIC_MODULES = modules + ('v51_evolution_survivability_authority',)

    # Rebuild the candidate call stack in the safe order:
    # exact-resume -> V48 resource/liveness guard -> V51 evaluator.
    autonomous._evaluate_candidate = _evaluate_factory(core, autonomous)
    runtime_continuity._wrap_candidate(core, autonomous)
    throughput._install_resume(core, autonomous)
    autonomous._evolution = _evolution_factory(core, autonomous, throughput, orchestration)
    _patch_status(core, autonomous)

    _state(core, installed=True, status='READY',
           semantics={
               'final_oos_thresholds_changed': False,
               'final_holdout_opened_early': False,
               'future_peeking_enabled': False,
               'trade_simulation_changed': False,
               'historical_data_changed': False,
               'replay_reset_required': False,
               'search_only_score_role': 'DEVELOPMENT_PARENT_SELECTION_ONLY',
               'search_only_can_be_finalist': False,
               'search_only_can_be_champion': False,
               'all_generations_run_even_if_generation1_has_no_eligible_candidate': True,
               'invalid_causal_settlement_paths_fail_closed': True,
           })

    app = core.app
    if not any(getattr(r, 'path', None) == '/api/v51/evolution-diagnostics' for r in app.router.routes):
        @app.get('/api/v51/evolution-diagnostics')
        def v51_diagnostics() -> dict[str, Any]:
            return {'ok': True, 'state': dict(core.state.get(STATE_KEY) or {}),
                    'outer_cursor': core.get_state('v49_stage6_outer_cursor', {}),
                    'checkpoint': core.get_state(autonomous.CHECKPOINT_KEY, {})}

    root = next((r for r in list(app.router.routes) if getattr(r, 'path', None) == '/'), None)
    old_root = getattr(root, 'endpoint', None)
    if callable(old_root):
        from fastapi.responses import HTMLResponse
        app.router.routes = [r for r in app.router.routes if getattr(r, 'path', None) != '/']
        @app.get('/', response_class=HTMLResponse, name='v51_evolution_survivability_dashboard')
        def dashboard_v51() -> str:
            raw = old_root(); html = raw.body.decode() if hasattr(raw, 'body') else str(raw)
            card = '''<section class="card"><h2>🧬 Stage 6 候選可進化性 / 拒絕診斷</h2><div id="v51diag" class="notice">讀取候選診斷…</div></section>'''
            marker = '</div><div class="footer">'
            html = html.replace(marker, card + marker, 1) if marker in html else html.replace('</body>', card + '</body>')
            js = r'''<script id="v51-diag-ui">(function(){function e(x){return String(x??'—').replace(/[&<>"']/g,s=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[s]))}async function t(){const el=document.getElementById('v51diag');if(!el)return;try{const r=await fetch('/api/v51/evolution-diagnostics',{cache:'no-store'}),z=await r.json(),s=z.state||{},d=s.last_candidate_diagnostics||{},cp=z.checkpoint||{};el.className='notice '+(s.error?'r':'g');el.innerHTML='<b>'+e(s.status||'READY')+'</b><br>Generation 已完成：'+e(s.generation_completed)+'｜本代可進 OOS：'+e(s.eligible_this_generation)+'｜Search-only：'+e(s.search_only_this_generation)+'<br>最近候選：'+e(d.development_status)+'｜完成 folds '+e(d.completed_folds)+'｜fold progress '+e(d.fold_progress_mean)+'<br>拒絕/路徑：'+e(JSON.stringify(d.path_reason_counts||{}))+'<br><b>規則：</b>Search-only 只能當進化父代，永遠不能進 Final OOS / Champion。'}catch(x){el.className='notice r';el.textContent='V51 診斷讀取失敗：'+x}}t();setInterval(t,3000)})();</script>'''
            return html.replace('</body>', js + '</body>')

    runtime_identity.stamp(core)
