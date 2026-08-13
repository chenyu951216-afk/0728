from __future__ import annotations

import math
import statistics
import threading
import time
from typing import Any

import numpy as np

import runtime_identity


VERSION = runtime_identity.RUNTIME_VERSION
STATE_KEY = 'v27_signal_certification_progress'
_LOCK = threading.RLock()
_CORE: Any | None = None
_SIGNAL_EVOLUTION: Any | None = None
_FIXED_HORIZON: Any | None = None
_ACTIVE: dict[str, Any] = {}
_INSTALLED = False


def _now() -> int:
    return int(time.time())


def _pair_index(signal_evolution: Any, strategy: str, direction: str) -> tuple[int, int]:
    strategies = list(signal_evolution.v5_runtime.STRATEGIES) if hasattr(signal_evolution, 'v5_runtime') else []
    directions = list(signal_evolution.v5_runtime.DIRECTIONS) if hasattr(signal_evolution, 'v5_runtime') else []
    if not strategies:
        try:
            import v5_runtime
            strategies = list(v5_runtime.STRATEGIES)
            directions = list(v5_runtime.DIRECTIONS)
        except Exception:
            strategies, directions = [], []
    expected = max(1, len(strategies) * len(directions))
    try:
        index = strategies.index(strategy) * len(directions) + directions.index(direction) + 1
    except Exception:
        index = 0
    return index, expected


def _publish(patch: dict[str, Any]) -> dict[str, Any]:
    global _ACTIVE
    with _LOCK:
        _ACTIVE.update(patch)
        _ACTIVE['runtime'] = VERSION
        _ACTIVE['heartbeat_at'] = _now()
        snapshot = dict(_ACTIVE)
    core = _CORE
    if core is not None:
        core.state[STATE_KEY] = snapshot
        core.state['signal_certification_live_progress'] = snapshot
    return snapshot


def _candidate_started(g: dict[str, Any]) -> tuple[int, float]:
    signal_evolution = _SIGNAL_EVOLUTION
    if signal_evolution is None:
        return 0, 0.0
    generation = max(0, int(g.get('generation') or 0))
    with _LOCK:
        counts = dict(_ACTIVE.get('generation_candidate_counts') or {})
        key = str(generation)
        counts[key] = int(counts.get(key) or 0) + 1
        calls = int(_ACTIVE.get('candidate_calls') or 0) + 1
        population = max(1, int(signal_evolution.POPULATION))
        generations = max(1, int(signal_evolution.GENERATIONS))
        within = min(1.0, counts[key] / population)
        fraction = min(.995, max(0.0, (generation + within) / generations))
    _publish({
        'status': 'RUNNING_CANDIDATE',
        'current_generation': generation,
        'generation_candidate_index': counts[key],
        'generation_candidate_counts': counts,
        'candidate_calls': calls,
        'population': population,
        'generations': generations,
        'lineage_fraction': fraction,
        'current_genome_id': str(g.get('id') or ''),
        'current_evolution_stage': str(g.get('evolution_stage') or ''),
        'candidate_started_at': _now(),
    })
    return calls, time.monotonic()


def _candidate_finished(started: float, result: dict[str, Any] | None) -> None:
    elapsed = max(0.0, time.monotonic() - started)
    with _LOCK:
        completed = int(_ACTIVE.get('candidate_completed') or 0) + 1
        total_seconds = float(_ACTIVE.get('candidate_total_seconds') or 0.0) + elapsed
        eligible = int(_ACTIVE.get('eligible_development_candidates') or 0) + (1 if result is not None else 0)
        avg = total_seconds / max(1, completed)
    _publish({
        'status': 'RUNNING_CANDIDATE',
        'candidate_completed': completed,
        'candidate_total_seconds': round(total_seconds, 3),
        'average_candidate_seconds': round(avg, 3),
        'eligible_development_candidates': eligible,
        'last_candidate_seconds': round(elapsed, 3),
        'last_candidate_eligible': result is not None,
        'last_candidate_finished_at': _now(),
    })


def _optimized_development_score(rows: list[dict[str, Any]], g: dict[str, Any], fee: float, seed: int) -> dict[str, Any] | None:
    """Same development-only walk-forward score, but materialize X only once per candidate.

    The former implementation rebuilt Python feature dictionaries into numpy matrices
    independently for fit/cal/train/test in every fold. That repeated conversion does
    not add statistical information and becomes expensive on the 60k-row full-span
    store. Here the exact same scoped rows, features, folds, purge gaps, models,
    thresholds, labels and weights are used; only the matrix conversion is shared.
    """
    se = _SIGNAL_EVOLUTION
    if se is None:
        raise RuntimeError('v27 signal evolution module is not installed')

    _, timer = _candidate_started(g)
    result: dict[str, Any] | None = None
    try:
        scoped = se._scope(rows, g)
        if len(scoped) < 520:
            return None
        idx = se._indices(g)
        if len(idx) < 7:
            return None

        purge = 32
        n = len(scoped)
        anchors = (.58, .72, .86)
        fold_stats: list[dict[str, float]] = []
        selected_all: list[dict[str, Any]] = []
        thresholds: list[float] = []

        # One matrix build per candidate instead of up to twelve. Keep float64 so the
        # estimator sees the same numeric precision as before.
        x_all = se._matrix(scoped, idx)
        y_all = np.asarray([r['success'] for r in scoped])

        for fi, frac in enumerate(anchors):
            test_start = int(n * frac)
            test_end = int(n * (anchors[fi + 1] if fi + 1 < len(anchors) else .98))
            train_end = max(0, test_start - purge)
            train = scoped[:train_end]
            test = scoped[test_start:test_end]
            if len(train) < 300 or len(test) < 55:
                continue

            cal_n = max(70, int(len(train) * .18))
            fit_end = max(0, len(train) - cal_n - purge)
            cal_start = fit_end + purge
            fit = scoped[:fit_end]
            cal = scoped[cal_start:train_end]
            if len(fit) < 220 or len(cal) < 60:
                continue

            yf = y_all[:fit_end]
            yc = y_all[cal_start:train_end]
            if len(set(yf)) < 2 or len(set(yc)) < 2:
                continue

            m = se._model(g, seed + fi)
            m.fit(x_all[:fit_end], yf, sample_weight=se._weights(fit, float(g['half_life_days'])))
            cp = m.predict_proba(x_all[cal_start:train_end])[:, 1]
            th = se._choose_threshold(cal, cp, fee)
            if th is None:
                continue
            threshold, _ = th

            yt = y_all[:train_end]
            m2 = se._model(g, seed + 100 + fi)
            m2.fit(x_all[:train_end], yt, sample_weight=se._weights(train, float(g['half_life_days'])))
            tp = m2.predict_proba(x_all[test_start:test_end])[:, 1]
            chosen = [r for r, p in zip(test, tp) if float(p) >= threshold]
            if len(chosen) < 16:
                continue

            st = se.base._stats(chosen, fee)
            dd = se.base._dd([se.base.f(r['pnl_r']) - fee for r in chosen])
            fold_stats.append({**st, 'dd': dd})
            selected_all.extend(chosen)
            thresholds.append(threshold)

        if len(fold_stats) < 2 or len(selected_all) < 45:
            return None

        evs = [x['ev'] for x in fold_stats]
        pfs = [x['pf'] for x in fold_stats]
        overall = se.base._stats(selected_all, fee)
        dd = se.base._dd([se.base.f(r['pnl_r']) - fee for r in selected_all])
        stability = se.base.clamp(1 - .70 * statistics.pstdev(evs) - .03 * statistics.pstdev(pfs), 0, 1)
        profitable = sum(v > 0 for v in evs) / len(evs)
        worst = min(evs)

        regime_metrics: dict[str, dict[str, float]] = {}
        for regime in sorted({str(row['regime']) for row in selected_all}):
            subset = [row for row in selected_all if str(row['regime']) == regime]
            if len(subset) >= 12:
                regime_metrics[regime] = se.base._stats(subset, fee)
        allowed_regimes = [
            regime for regime, stats in regime_metrics.items()
            if stats['n'] >= 18 and stats['ev'] >= .015 and stats['pf'] >= 1.03
        ]
        if not allowed_regimes:
            return None

        phase_metrics: dict[str, dict[str, float]] = {}
        for phase in sorted({str(row['phase']) for row in selected_all}):
            subset = [row for row in selected_all if str(row['phase']) == phase]
            if len(subset) >= 12:
                phase_metrics[phase] = se.base._stats(subset, fee)
        allowed_phases = [
            phase for phase, stats in phase_metrics.items()
            if stats['n'] >= 14 and stats['ev'] >= .01 and stats['pf'] >= 1.02
        ] or [
            phase for phase, stats in phase_metrics.items()
            if stats['n'] >= 24 and stats['ev'] > 0
        ]
        if not allowed_phases:
            return None

        score = (
            overall['ev'] * 4.5 + math.log(max(overall['pf'], 1e-6)) * .35 +
            stability * .30 + profitable * .20 - dd * .004 + min(worst, .10) +
            min(len(allowed_regimes), 4) * .015
        )
        result = {
            'score': score, 'ev': overall['ev'], 'pf': overall['pf'], 'win': overall['win'],
            'n': len(selected_all), 'dd': dd, 'stability': stability, 'profitable_folds': profitable,
            'worst_fold_ev': worst, 'threshold': round(statistics.median(thresholds), 2), 'folds': fold_stats,
            'regime_metrics': regime_metrics, 'allowed_regimes': allowed_regimes,
            'phase_metrics': phase_metrics, 'allowed_phases': allowed_phases,
        }
        return result
    finally:
        _candidate_finished(timer, result)


def _install_lineage_progress(signal_evolution: Any) -> None:
    original = signal_evolution.HistoricalEvolutionLearner.train_strategy_direction
    if getattr(original, '_v27_progress', False):
        return

    def progress_lineage(self: Any, strategy: str, direction: str, *args: Any, **kwargs: Any):
        index, expected = _pair_index(signal_evolution, strategy, direction)
        started_wall = _now()
        started_mono = time.monotonic()
        _publish({
            'status': 'RUNNING_LINEAGE',
            'strategy': strategy,
            'direction': direction,
            'lineage_index': index,
            'expected_lineages': expected,
            'lineage_started_at': started_wall,
            'candidate_calls': 0,
            'candidate_completed': 0,
            'candidate_total_seconds': 0.0,
            'average_candidate_seconds': 0.0,
            'eligible_development_candidates': 0,
            'generation_candidate_counts': {},
            'current_generation': 0,
            'generation_candidate_index': 0,
            'population': int(signal_evolution.POPULATION),
            'generations': int(signal_evolution.GENERATIONS),
            'lineage_fraction': 0.0,
            'current_genome_id': '',
            'current_evolution_stage': 'MACRO_REGIME',
            'error': None,
        })
        try:
            outcome = original(self, strategy, direction, *args, **kwargs)
            elapsed = max(0.0, time.monotonic() - started_mono)
            _publish({
                'status': 'LINEAGE_COMPLETE',
                'lineage_fraction': 1.0,
                'lineage_elapsed_seconds': round(elapsed, 3),
                'lineage_finished_at': _now(),
                'returned_evaluation': outcome is not None,
            })
            return outcome
        except Exception as exc:
            _publish({
                'status': 'LINEAGE_ERROR',
                'lineage_elapsed_seconds': round(max(0.0, time.monotonic() - started_mono), 3),
                'lineage_finished_at': _now(),
                'error': f'{type(exc).__name__}: {exc}',
            })
            raise

    progress_lineage._v27_progress = True  # type: ignore[attr-defined]
    signal_evolution.HistoricalEvolutionLearner.train_strategy_direction = progress_lineage


def _install_dashboard_progress(fixed_horizon: Any) -> None:
    original = fixed_horizon._lineage_progress
    if getattr(original, '_v27_progress', False):
        return

    def live_lineage_progress(evolution_module: Any, core: Any) -> dict[str, Any]:
        out = dict(original(evolution_module, core) or {})
        active = core.state.get(STATE_KEY) or core.state.get('signal_certification_live_progress') or {}
        if not isinstance(active, dict):
            active = {}
        status = str(active.get('status') or '')
        terminal = int(out.get('terminal_lineages') or 0)
        expected = max(1, int(out.get('expected_lineages') or 1))
        persisted_candidates = int(out.get('candidates_evaluated') or 0)

        if status.startswith('RUNNING_'):
            fraction = max(0.0, min(.995, float(active.get('lineage_fraction') or 0.0)))
            out['terminal_percent'] = round(100.0 * terminal / expected, 2)
            out['percent'] = round(100.0 * min(expected, terminal + fraction) / expected, 2)
            out['candidates_evaluated'] = persisted_candidates + int(active.get('candidate_completed') or 0)
            out['active_lineage'] = dict(active)
            rows = list(out.get('lineages') or [])
            key = (str(active.get('strategy') or ''), str(active.get('direction') or ''))
            if key[0] and key[1] and not any((str(x.get('strategy') or ''), str(x.get('direction') or '')) == key for x in rows):
                rows.append({
                    'strategy': key[0], 'direction': key[1],
                    'status': f"RUNNING {active.get('current_evolution_stage') or ''}",
                    'generation': active.get('current_generation'),
                    'candidates_evaluated': int(active.get('candidate_completed') or 0),
                    'profit_factor': None, 'expectancy_r': None,
                    'reason': 'currently evaluating development-only candidates; sealed holdout remains unopened',
                    'active': True,
                })
                out['lineages'] = rows
        else:
            out['active_lineage'] = dict(active) if active else {}
        return out

    live_lineage_progress._v27_progress = True  # type: ignore[attr-defined]
    fixed_horizon._lineage_progress = live_lineage_progress


def install(core: Any, signal_evolution: Any, fixed_horizon: Any) -> None:
    global _CORE, _SIGNAL_EVOLUTION, _FIXED_HORIZON, _INSTALLED
    if _INSTALLED:
        return
    _INSTALLED = True
    _CORE = core
    _SIGNAL_EVOLUTION = signal_evolution
    _FIXED_HORIZON = fixed_horizon

    # Install before v26 so v26's memory-trim wrapper surrounds this optimized scorer.
    signal_evolution._development_score = _optimized_development_score
    _install_lineage_progress(signal_evolution)
    _install_dashboard_progress(fixed_horizon)

    core.state.setdefault('strict_replay', {})['certification_progress_v27'] = {
        'runtime': VERSION,
        'single_matrix_materialization_per_candidate': True,
        'candidate_search_space_reduced': False,
        'folds_reduced': False,
        'features_reduced': False,
        'holdout_rules_changed': False,
        'no_lookahead_rules_changed': False,
        'live_active_lineage_visible': True,
        'live_generation_candidate_progress_visible': True,
    }
    core.state[STATE_KEY] = {
        'runtime': VERSION,
        'status': 'WAITING_FOR_SIGNAL_CERTIFICATION',
        'heartbeat_at': _now(),
    }

    if not any(getattr(route, 'path', None) == '/api/v27/certification-progress' for route in core.app.router.routes):
        @core.app.get('/api/v27/certification-progress')
        def certification_progress() -> dict[str, Any]:
            return {
                'runtime': VERSION,
                'progress': core.state.get(STATE_KEY) or {},
                'rules': core.state.get('strict_replay', {}).get('certification_progress_v27', {}),
            }
