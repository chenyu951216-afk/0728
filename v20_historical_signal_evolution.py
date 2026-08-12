from __future__ import annotations

import hashlib
import json
import math
import os
import random
import statistics
import time
from dataclasses import dataclass
from typing import Any

import numpy as np
from sklearn.ensemble import HistGradientBoostingClassifier

import adaptive_v5 as base
import v5_runtime
import v8_evolution as evo

VERSION = '9.1.0-20260812'
GENOME_SCHEMA = 2
GENERATIONS = max(3, min(12, int(os.getenv('SIGNAL_EVOLUTION_GENERATIONS', '7'))))
POPULATION = max(16, min(96, int(os.getenv('SIGNAL_EVOLUTION_POPULATION', '36'))))
ELITES = max(3, min(16, int(os.getenv('SIGNAL_EVOLUTION_ELITES', '6'))))
FINAL_HOLDOUT_PCT = max(.15, min(.30, float(os.getenv('SIGNAL_FINAL_HOLDOUT_PCT', '.20'))))
MIN_HOLDOUT_SELECTED = max(24, int(os.getenv('SIGNAL_MIN_HOLDOUT_SELECTED', '40')))

REGIME_SCOPES: tuple[tuple[str, tuple[str, ...]], ...] = (
    ('ALL', tuple(base.REGIMES)),
    ('TREND', ('BULL_MARKUP', 'BULL_PULLBACK', 'BEAR_MARKDOWN', 'BEAR_RALLY')),
    ('EXPANSION', ('SQUEEZE', 'EXPANSION_UP', 'EXPANSION_DOWN')),
    ('RANGE', ('RANGE_LOW_VOL', 'RANGE_HIGH_VOL', 'TRANSITION')),
    ('REVERSAL', ('CAPITULATION', 'REBOUND', 'TRANSITION', 'BULL_PULLBACK', 'BEAR_RALLY')),
)
FEATURE_MODES = ('all', 'price_action', 'momentum_structure', 'flow_structure', 'lean')


@dataclass
class EvolutionEvaluation:
    strategy: str
    direction: str
    train_n: int
    test_n: int
    selected_n: int
    test_win: float
    profit_factor: float
    expectancy_r: float
    threshold: float
    brier: float
    max_drawdown_r: float
    stability: float
    promoted: bool
    reason: str
    generation: int
    candidates_evaluated: int
    regime_scope: str
    allowed_regimes: list[str]
    holdout_ev_bootstrap_05: float
    signals_per_day: float
    genome_id: str


def _fingerprint(g: dict[str, Any]) -> str:
    payload = json.dumps(g, sort_keys=True, separators=(',', ':'))
    return hashlib.sha256(payload.encode()).hexdigest()[:16]


def _rng(strategy: str, direction: str) -> random.Random:
    seed = int(hashlib.sha256(f'{strategy}|{direction}|{GENOME_SCHEMA}'.encode()).hexdigest()[:12], 16)
    return random.Random(seed)


def _candidate(rng: random.Random, generation: int, parent: dict[str, Any] | None = None) -> dict[str, Any]:
    if parent is None:
        mode = rng.choice(FEATURE_MODES)
        scope_name, scope = rng.choice(REGIME_SCOPES)
        g = {
            'feature_mode': mode,
            'scope_name': scope_name,
            'regimes': list(scope),
            'half_life_days': rng.choice((270, 365, 540, 730, 1095, 1460)),
            'learning_rate': rng.choice((.022, .028, .034, .040, .046, .052)),
            'max_iter': rng.choice((170, 200, 230, 260, 300)),
            'max_leaf_nodes': rng.choice((7, 9, 13, 17, 23)),
            'min_samples_leaf': rng.choice((22, 30, 40, 52, 68)),
            'l2_regularization': rng.choice((1.2, 1.8, 2.5, 3.4, 4.5)),
            'generation': generation,
        }
    else:
        g = dict(parent)
        g['regimes'] = list(parent['regimes'])
        g['generation'] = generation
        mut = rng.randint(1, 3)
        keys = rng.sample(('feature_mode', 'scope', 'half_life_days', 'learning_rate', 'max_iter', 'max_leaf_nodes', 'min_samples_leaf', 'l2_regularization'), mut)
        for key in keys:
            if key == 'feature_mode': g['feature_mode'] = rng.choice(FEATURE_MODES)
            elif key == 'scope':
                name, scope = rng.choice(REGIME_SCOPES); g['scope_name'] = name; g['regimes'] = list(scope)
            elif key == 'half_life_days': g[key] = rng.choice((270, 365, 540, 730, 1095, 1460))
            elif key == 'learning_rate': g[key] = rng.choice((.022, .028, .034, .040, .046, .052))
            elif key == 'max_iter': g[key] = rng.choice((170, 200, 230, 260, 300))
            elif key == 'max_leaf_nodes': g[key] = rng.choice((7, 9, 13, 17, 23))
            elif key == 'min_samples_leaf': g[key] = rng.choice((22, 30, 40, 52, 68))
            elif key == 'l2_regularization': g[key] = rng.choice((1.2, 1.8, 2.5, 3.4, 4.5))
    g['id'] = f"evo{generation}_{_fingerprint(g)}"
    return g


def _indices(g: dict[str, Any]) -> list[int]:
    names = set(evo._feature_names(str(g['feature_mode'])))
    return [i for i, name in enumerate(base.FEATURE_NAMES) if name in names]


def _matrix(rows: list[dict[str, Any]], idx: list[int]) -> np.ndarray:
    return np.vstack([base._vec(r['features'])[idx] for r in rows])


def _weights(rows: list[dict[str, Any]], half_life_days: float) -> np.ndarray:
    return evo._weights(rows, half_life_days, int(rows[-1]['ts']))


def _model(g: dict[str, Any], seed: int) -> HistGradientBoostingClassifier:
    return HistGradientBoostingClassifier(
        learning_rate=float(g['learning_rate']), max_iter=int(g['max_iter']),
        max_leaf_nodes=int(g['max_leaf_nodes']), min_samples_leaf=int(g['min_samples_leaf']),
        l2_regularization=float(g['l2_regularization']), random_state=seed,
    )


def _scope(rows: list[dict[str, Any]], g: dict[str, Any]) -> list[dict[str, Any]]:
    allowed = set(g['regimes'])
    return [r for r in rows if str(r['regime']) in allowed]


def _choose_threshold(rows: list[dict[str, Any]], probs: np.ndarray, fee: float) -> tuple[float, dict[str, float]] | None:
    if len(rows) < 30: return None
    best: tuple[float, float, dict[str, float]] | None = None
    span = max(1.0, (int(rows[-1]['ts']) - int(rows[0]['ts'])) / 86400.0)
    for t in np.arange(.50, .801, .01):
        chosen = [r for r, p in zip(rows, probs) if float(p) >= float(t)]
        if len(chosen) < 20: continue
        st = base._stats(chosen, fee)
        dd = base._dd([base.f(r['pnl_r']) - fee for r in chosen])
        freq = len(chosen) / span
        score = st['ev'] * 4.0 + math.log(max(st['pf'], 1e-6)) * .30 - dd * .004 + min(len(chosen), 160) / 160 * .08
        if freq < .025: score -= .15
        if freq > 5: score -= min(.30, (freq - 5) * .04)
        meta = {**st, 'dd': dd, 'frequency': freq, 'score': score}
        if best is None or score > best[0]: best = (score, round(float(t), 2), meta)
    return None if best is None else (best[1], best[2])


def _development_score(rows: list[dict[str, Any]], g: dict[str, Any], fee: float, seed: int) -> dict[str, Any] | None:
    rows = _scope(rows, g)
    if len(rows) < 520: return None
    idx = _indices(g)
    if len(idx) < 7: return None
    purge = 32
    n = len(rows)
    anchors = (.58, .72, .86)
    fold_stats: list[dict[str, float]] = []
    selected_all: list[dict[str, Any]] = []
    thresholds: list[float] = []
    for fi, frac in enumerate(anchors):
        test_start = int(n * frac)
        test_end = int(n * (anchors[fi + 1] if fi + 1 < len(anchors) else .98))
        train = rows[:max(0, test_start - purge)]
        test = rows[test_start:test_end]
        if len(train) < 300 or len(test) < 55: continue
        cal_n = max(70, int(len(train) * .18))
        fit = train[:max(0, len(train) - cal_n - purge)]
        cal = train[len(fit) + purge:]
        if len(fit) < 220 or len(cal) < 60: continue
        yf = np.asarray([r['success'] for r in fit]); yc = np.asarray([r['success'] for r in cal])
        if len(set(yf)) < 2 or len(set(yc)) < 2: continue
        m = _model(g, seed + fi)
        m.fit(_matrix(fit, idx), yf, sample_weight=_weights(fit, float(g['half_life_days'])))
        cp = m.predict_proba(_matrix(cal, idx))[:, 1]
        th = _choose_threshold(cal, cp, fee)
        if th is None: continue
        threshold, _ = th
        yt = np.asarray([r['success'] for r in train])
        m2 = _model(g, seed + 100 + fi)
        m2.fit(_matrix(train, idx), yt, sample_weight=_weights(train, float(g['half_life_days'])))
        tp = m2.predict_proba(_matrix(test, idx))[:, 1]
        chosen = [r for r, p in zip(test, tp) if float(p) >= threshold]
        if len(chosen) < 16: continue
        st = base._stats(chosen, fee); dd = base._dd([base.f(r['pnl_r']) - fee for r in chosen])
        fold_stats.append({**st, 'dd': dd}); selected_all.extend(chosen); thresholds.append(threshold)
    if len(fold_stats) < 2 or len(selected_all) < 45: return None
    evs = [x['ev'] for x in fold_stats]; pfs = [x['pf'] for x in fold_stats]
    overall = base._stats(selected_all, fee); dd = base._dd([base.f(r['pnl_r']) - fee for r in selected_all])
    stability = base.clamp(1 - .70 * statistics.pstdev(evs) - .03 * statistics.pstdev(pfs), 0, 1)
    profitable = sum(v > 0 for v in evs) / len(evs)
    worst = min(evs)
    score = overall['ev'] * 4.5 + math.log(max(overall['pf'], 1e-6)) * .35 + stability * .30 + profitable * .20 - dd * .004 + min(worst, .10)
    return {
        'score': score, 'ev': overall['ev'], 'pf': overall['pf'], 'win': overall['win'],
        'n': len(selected_all), 'dd': dd, 'stability': stability, 'profitable_folds': profitable,
        'worst_fold_ev': worst, 'threshold': round(statistics.median(thresholds), 2), 'folds': fold_stats,
    }


def _cluster_bootstrap_ev(rows: list[dict[str, Any]], fee: float, seed: int, reps: int = 400) -> float:
    if not rows: return -9.0
    buckets: dict[int, list[float]] = {}
    for r in rows:
        bucket = int(r['ts']) // (8 * 3600)
        buckets.setdefault(bucket, []).append(base.f(r['pnl_r']) - fee)
    groups = list(buckets.values())
    if len(groups) < 8: return -9.0
    rng = random.Random(seed)
    means: list[float] = []
    for _ in range(reps):
        sample: list[float] = []
        for __ in groups: sample.extend(groups[rng.randrange(len(groups))])
        means.append(statistics.mean(sample) if sample else -9.0)
    means.sort()
    return float(means[max(0, int(.05 * (len(means) - 1)))])


class HistoricalEvolutionLearner(evo.GenomeEvolutionLearner):
    """True multi-generation Signal evolution with a sealed final chronological holdout.

    Candidate mutation/selection is performed only on the development era. The final
    chronological holdout is never inspected until the candidate has been fixed.
    """

    def train_strategy_direction(self, strategy: str, direction: str, min_train: int = 300, min_test: int = 120):
        rows = [x for x in self.store.samples(strategy, direction=direction) if x['source_quality'] >= 55]
        if len(rows) < 1100: return None
        n = len(rows); purge = 32
        holdout_start = max(760, int(n * (1.0 - FINAL_HOLDOUT_PCT)))
        development = rows[:max(0, holdout_start - purge)]
        holdout = rows[holdout_start:]
        if len(development) < 700 or len(holdout) < 160: return None

        rng = _rng(strategy, direction)
        population: list[dict[str, Any]] = [_candidate(rng, 0) for _ in range(POPULATION)]
        seen: set[str] = set(); evaluated = 0; best_history: list[dict[str, Any]] = []
        elites: list[tuple[float, dict[str, Any], dict[str, Any]]] = []
        for generation in range(GENERATIONS):
            scored: list[tuple[float, dict[str, Any], dict[str, Any]]] = []
            for ci, g in enumerate(population):
                fp = _fingerprint(g)
                if fp in seen: continue
                seen.add(fp); evaluated += 1
                result = _development_score(development, g, self.fee_r, 91000 + generation * 1000 + ci * 7)
                if result is not None: scored.append((float(result['score']), g, result))
            scored.sort(key=lambda z: z[0], reverse=True)
            elites = scored[:ELITES]
            if elites:
                best_history.append({'generation': generation, 'genome_id': elites[0][1]['id'], 'scope': elites[0][1]['scope_name'], **elites[0][2]})
            if generation == GENERATIONS - 1 or not elites: break
            population = [dict(x[1]) for x in elites]
            while len(population) < POPULATION:
                parent = rng.choice(elites)[1]
                population.append(_candidate(rng, generation + 1, parent))

        if not elites:
            return None
        _, winner, dev = elites[0]
        scoped_dev = _scope(development, winner); scoped_holdout = _scope(holdout, winner)
        idx = _indices(winner)
        if len(scoped_dev) < 500 or len(scoped_holdout) < 80: return None

        cal_n = max(90, int(len(scoped_dev) * .16))
        fit = scoped_dev[:max(0, len(scoped_dev) - cal_n - purge)]
        cal = scoped_dev[len(fit) + purge:]
        if len(fit) < 320 or len(cal) < 70: return None
        yf = np.asarray([r['success'] for r in fit])
        if len(set(yf)) < 2: return None
        m = _model(winner, 99001)
        m.fit(_matrix(fit, idx), yf, sample_weight=_weights(fit, float(winner['half_life_days'])))
        cp = m.predict_proba(_matrix(cal, idx))[:, 1]
        th = _choose_threshold(cal, cp, self.fee_r)
        if th is None: return None
        threshold, _ = th

        yd = np.asarray([r['success'] for r in scoped_dev])
        final_model = _model(winner, 99002)
        final_model.fit(_matrix(scoped_dev, idx), yd, sample_weight=_weights(scoped_dev, float(winner['half_life_days'])))
        hp = final_model.predict_proba(_matrix(scoped_holdout, idx))[:, 1]
        selected = [r for r, p in zip(scoped_holdout, hp) if float(p) >= threshold]
        stats = base._stats(selected, self.fee_r) if selected else {'n': 0, 'pf': 0.0, 'ev': -1.0, 'win': 0.0}
        dd = base._dd([base.f(r['pnl_r']) - self.fee_r for r in selected]) if selected else 999.0
        ci05 = _cluster_bootstrap_ev(selected, self.fee_r, 99117)
        span_days = max(1.0, (int(scoped_holdout[-1]['ts']) - int(scoped_holdout[0]['ts'])) / 86400.0)
        freq = len(selected) / span_days
        promote = bool(
            len(selected) >= MIN_HOLDOUT_SELECTED and stats['pf'] >= 1.12 and stats['ev'] >= .04 and
            ci05 > 0 and dd <= 14 and .02 <= freq <= 6 and dev['stability'] >= .72 and
            dev['profitable_folds'] >= .66 and dev['worst_fold_ev'] >= -.06
        )
        reason = (
            'multi-generation development evolution passed; one-time sealed chronological holdout also passed'
            if promote else
            f"evolved {evaluated} candidates across {len(best_history)} generations, but sealed holdout did not pass: n={len(selected)}, PF={stats['pf']:.2f}, EV={stats['ev']:+.3f}R, CI05={ci05:+.3f}R, DD={dd:.2f}R"
        )
        metrics = {
            'schema_version': 5, 'evolution_schema': GENOME_SCHEMA, 'evolution_runtime': VERSION,
            'strategy': strategy, 'direction': direction, 'generation': int(winner['generation']),
            'candidates_evaluated': evaluated, 'development_generations': best_history,
            'genome_id': winner['id'], 'feature_mode': winner['feature_mode'],
            'feature_names': evo._feature_names(str(winner['feature_mode'])), 'feature_count': len(idx),
            'params': {k: winner[k] for k in ('learning_rate','max_iter','max_leaf_nodes','min_samples_leaf','l2_regularization')},
            'recency_half_life_days': winner['half_life_days'], 'regime_scope': winner['scope_name'],
            'allowed_regimes': list(winner['regimes']), 'threshold': threshold,
            'profit_factor': stats['pf'], 'expectancy_r': stats['ev'], 'test_win': stats['win'],
            'selected_n': len(selected), 'max_drawdown_r': dd, 'signals_per_day': freq,
            'stability': dev['stability'], 'clustered_ev_bootstrap_05': ci05,
            'effective_oos_selected_n': len(selected), 'overfit_guard_passed': promote,
            'trained_through_ts': int(rows[-1]['ts']),
            'validation_method': 'MULTI_GENERATION_DEV_ONLY_EVOLUTION_THEN_SINGLE_SEALED_CHRONOLOGICAL_HOLDOUT',
            'comparison_method': 'candidate mutation and selection never inspect final holdout; holdout opened once only after winner fixed',
            'holdout_start_ts': int(holdout[0]['ts']), 'holdout_end_ts': int(holdout[-1]['ts']),
            'reason': reason,
        }
        wrapped = evo.GenomeModel(final_model, idx, winner['id'])
        self.store.save_challenger(strategy, direction, wrapped, metrics, promote)
        return EvolutionEvaluation(
            strategy=strategy, direction=direction, train_n=len(development), test_n=len(scoped_holdout),
            selected_n=len(selected), test_win=stats['win'], profit_factor=stats['pf'], expectancy_r=stats['ev'],
            threshold=threshold, brier=0.0, max_drawdown_r=dd, stability=dev['stability'], promoted=promote,
            reason=reason, generation=int(winner['generation']), candidates_evaluated=evaluated,
            regime_scope=str(winner['scope_name']), allowed_regimes=list(winner['regimes']),
            holdout_ev_bootstrap_05=ci05, signals_per_day=freq, genome_id=winner['id'],
        )


def install(core: Any) -> None:
    # This is the final Signal learner authority. It deliberately leaves the historical
    # replay labels and the later Execution Evolution untouched.
    v5_runtime.Learner = HistoricalEvolutionLearner
    core.Learner = HistoricalEvolutionLearner
    core.state['historical_signal_evolution'] = {
        'runtime': VERSION, 'schema': GENOME_SCHEMA, 'generations': GENERATIONS,
        'population': POPULATION, 'elites': ELITES, 'final_holdout_pct': FINAL_HOLDOUT_PCT,
        'rule': 'all mutation/selection occurs inside development history; final chronological holdout is sealed until winner is fixed',
        'no_lookahead': True, 'execution_learning_separate': True,
    }
