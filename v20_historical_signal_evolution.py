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

VERSION = '10.0.0-20260812'
GENOME_SCHEMA = 4
GENERATIONS = max(3, min(12, int(os.getenv('SIGNAL_EVOLUTION_GENERATIONS', '7'))))
POPULATION = max(16, min(96, int(os.getenv('SIGNAL_EVOLUTION_POPULATION', '36'))))
ELITES = max(3, min(16, int(os.getenv('SIGNAL_EVOLUTION_ELITES', '6'))))
FINAL_HOLDOUT_PCT = max(.15, min(.30, float(os.getenv('SIGNAL_FINAL_HOLDOUT_PCT', '.20'))))
MIN_HOLDOUT_SELECTED = max(60, int(os.getenv('SIGNAL_MIN_HOLDOUT_SELECTED', '60')))
MIN_UNTOUCHED_HOLDOUT = max(240, int(os.getenv('SIGNAL_MIN_UNTOUCHED_HOLDOUT', '320')))

REGIME_SCOPES: tuple[tuple[str, tuple[str, ...]], ...] = (
    ('ALL', tuple(base.REGIMES)),
    ('TREND', ('BULL_MARKUP', 'BULL_PULLBACK', 'BEAR_MARKDOWN', 'BEAR_RALLY')),
    ('BULL', ('BULL_MARKUP', 'BULL_PULLBACK', 'REBOUND')),
    ('BEAR', ('BEAR_MARKDOWN', 'BEAR_RALLY', 'CAPITULATION')),
    ('EXPANSION', ('SQUEEZE', 'EXPANSION_UP', 'EXPANSION_DOWN')),
    ('RANGE', ('RANGE_LOW_VOL', 'RANGE_HIGH_VOL', 'TRANSITION')),
    ('REVERSAL', ('CAPITULATION', 'REBOUND', 'TRANSITION', 'BULL_PULLBACK', 'BEAR_RALLY')),
)
ALL_PHASES = ('PULLBACK', 'COMPRESSION', 'EXPANSION', 'IMPULSE', 'BALANCE', 'TRANSITION')
PHASE_SCOPES: tuple[tuple[str, tuple[str, ...]], ...] = (
    ('ALL', ALL_PHASES),
    ('TREND_STRUCTURE', ('PULLBACK', 'IMPULSE', 'TRANSITION')),
    ('BREAKOUT_STRUCTURE', ('COMPRESSION', 'EXPANSION', 'IMPULSE')),
    ('RANGE_STRUCTURE', ('BALANCE', 'COMPRESSION', 'TRANSITION')),
    ('REVERSAL_STRUCTURE', ('PULLBACK', 'BALANCE', 'TRANSITION')),
)
FINAL_FEATURE_MODES = ('all', 'price_action', 'momentum_structure', 'flow_structure', 'lean')
FEATURE_MODES = ('macro_context', 'structure_context') + FINAL_FEATURE_MODES


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
    structure_scope: str
    allowed_phases: list[str]
    holdout_ev_bootstrap_05: float
    signals_per_day: float
    genome_id: str
    absolute_guard_passed: bool
    champion_comparison_passed: bool
    holdout_start_ts: int
    holdout_end_ts: int
    evaluation_status: str


def _fingerprint(g: dict[str, Any]) -> str:
    # Identity is the actual phenotype. Generation labels and a previous id must not
    # make byte-identical candidates look novel and silently multiply-test the data.
    payload = json.dumps({k: v for k, v in g.items() if k not in ('id', 'generation', 'evolution_stage')}, sort_keys=True, separators=(',', ':'))
    return hashlib.sha256(payload.encode()).hexdigest()[:16]


def _rng(strategy: str, direction: str, evidence_key: int = 0) -> random.Random:
    seed = int(hashlib.sha256(f'{strategy}|{direction}|{GENOME_SCHEMA}|{evidence_key}'.encode()).hexdigest()[:12], 16)
    return random.Random(seed)


def _evolution_stage(generation: int) -> str:
    if generation <= 1:
        return 'MACRO_REGIME'
    if generation <= 3:
        return 'MARKET_STRUCTURE'
    return 'SHORT_HORIZON_SIGNAL'


def _candidate(rng: random.Random, generation: int, parent: dict[str, Any] | None = None) -> dict[str, Any]:
    stage = _evolution_stage(generation)
    if parent is None:
        scope_name, scope = rng.choice(REGIME_SCOPES)
        phase_name, phases = ('ALL', ALL_PHASES) if stage == 'MACRO_REGIME' else rng.choice(PHASE_SCOPES)
        mode = 'macro_context' if stage == 'MACRO_REGIME' else 'structure_context' if stage == 'MARKET_STRUCTURE' else rng.choice(FINAL_FEATURE_MODES)
        g = {
            'feature_mode': mode,
            'scope_name': scope_name,
            'regimes': list(scope),
            'phase_scope_name': phase_name,
            'phases': list(phases),
            'half_life_days': rng.choice((270, 365, 540, 730, 1095, 1460)),
            # Macro selection starts from one fixed learner shape. Later stages may
            # change structure and short-horizon capacity, so micro parameters cannot
            # accidentally decide which market regime survives the first stage.
            'learning_rate': .034,
            'max_iter': 230,
            'max_leaf_nodes': 13,
            'min_samples_leaf': 40,
            'l2_regularization': rng.choice((1.2, 1.8, 2.5, 3.4, 4.5)),
            'generation': generation,
        }
    else:
        g = dict(parent)
        g['regimes'] = list(parent['regimes'])
        g['phases'] = list(parent.get('phases') or ALL_PHASES)
        g['phase_scope_name'] = str(parent.get('phase_scope_name') or 'ALL')
        g['generation'] = generation
        if stage == 'MACRO_REGIME':
            available = ('scope', 'half_life_days', 'l2_regularization')
        elif stage == 'MARKET_STRUCTURE':
            g['feature_mode'] = 'structure_context'
            available = ('phase_scope', 'min_samples_leaf', 'max_leaf_nodes')
        else:
            if g.get('feature_mode') in ('macro_context', 'structure_context'):
                g['feature_mode'] = rng.choice(FINAL_FEATURE_MODES)
            available = ('feature_mode', 'half_life_days', 'learning_rate', 'max_iter', 'max_leaf_nodes', 'min_samples_leaf', 'l2_regularization')
        keys = rng.sample(available, rng.randint(1, min(3, len(available))))
        for key in keys:
            if key == 'feature_mode': g['feature_mode'] = rng.choice(FINAL_FEATURE_MODES)
            elif key == 'scope':
                name, scope = rng.choice(REGIME_SCOPES); g['scope_name'] = name; g['regimes'] = list(scope)
            elif key == 'phase_scope':
                name, phases = rng.choice(PHASE_SCOPES); g['phase_scope_name'] = name; g['phases'] = list(phases)
            elif key == 'half_life_days': g[key] = rng.choice((270, 365, 540, 730, 1095, 1460))
            elif key == 'learning_rate': g[key] = rng.choice((.022, .028, .034, .040, .046, .052))
            elif key == 'max_iter': g[key] = rng.choice((170, 200, 230, 260, 300))
            elif key == 'max_leaf_nodes': g[key] = rng.choice((7, 9, 13, 17, 23))
            elif key == 'min_samples_leaf': g[key] = rng.choice((22, 30, 40, 52, 68))
            elif key == 'l2_regularization': g[key] = rng.choice((1.2, 1.8, 2.5, 3.4, 4.5))
    g['evolution_stage'] = _evolution_stage(generation)
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
    phases = set(g.get('phases') or ALL_PHASES)
    return [r for r in rows if str(r['regime']) in allowed and str(r['phase']) in phases]


def _ensure_run_table(store: Any) -> None:
    store.con.execute('''CREATE TABLE IF NOT EXISTS signal_evolution_runs(
        run_id INTEGER PRIMARY KEY AUTOINCREMENT,
        strategy TEXT NOT NULL,
        direction TEXT NOT NULL,
        created_at INTEGER NOT NULL,
        evolution_schema INTEGER NOT NULL,
        development_end_ts INTEGER,
        holdout_start_ts INTEGER,
        holdout_end_ts INTEGER,
        status TEXT NOT NULL,
        winner_genome TEXT,
        metrics TEXT NOT NULL
    )''')
    store.con.execute('CREATE INDEX IF NOT EXISTS ix_signal_evolution_lineage ON signal_evolution_runs(strategy,direction,run_id)')
    store.con.commit()


def _latest_run(store: Any, strategy: str, direction: str) -> dict[str, Any] | None:
    _ensure_run_table(store)
    row = store.con.execute(
        '''SELECT development_end_ts,holdout_start_ts,holdout_end_ts,status,winner_genome,metrics
           FROM signal_evolution_runs WHERE strategy=? AND direction=? AND evolution_schema=?
           ORDER BY run_id DESC LIMIT 1''',
        (strategy, direction, GENOME_SCHEMA),
    ).fetchone()
    if not row:
        return None
    try:
        genome = json.loads(row[4]) if row[4] else None
        metrics = json.loads(row[5]) if row[5] else {}
    except (TypeError, json.JSONDecodeError):
        genome, metrics = None, {}
    return {
        'development_end_ts': int(row[0] or 0), 'holdout_start_ts': int(row[1] or 0),
        'holdout_end_ts': int(row[2] or 0), 'status': str(row[3]),
        'winner_genome': genome if isinstance(genome, dict) else None,
        'metrics': metrics if isinstance(metrics, dict) else {},
    }


def _record_run(store: Any, strategy: str, direction: str, *, development_end_ts: int,
                holdout_start_ts: int, holdout_end_ts: int, status: str,
                winner: dict[str, Any] | None, metrics: dict[str, Any]) -> None:
    _ensure_run_table(store)
    store.con.execute(
        '''INSERT INTO signal_evolution_runs(
           strategy,direction,created_at,evolution_schema,development_end_ts,
           holdout_start_ts,holdout_end_ts,status,winner_genome,metrics
           ) VALUES(?,?,?,?,?,?,?,?,?,?)''',
        (strategy, direction, int(time.time()), GENOME_SCHEMA, int(development_end_ts),
         int(holdout_start_ts), int(holdout_end_ts), status,
         json.dumps(winner, ensure_ascii=False, separators=(',', ':'), default=_json_default) if winner else None,
         json.dumps(metrics, ensure_ascii=False, separators=(',', ':'), default=_json_default)),
    )
    store.con.commit()


def _json_default(value: Any) -> Any:
    if hasattr(value, 'item'):
        return value.item()
    raise TypeError(f'{type(value).__name__} is not JSON serializable')


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
    regime_metrics: dict[str, dict[str, float]] = {}
    for regime in sorted({str(row['regime']) for row in selected_all}):
        subset = [row for row in selected_all if str(row['regime']) == regime]
        if len(subset) >= 12:
            regime_metrics[regime] = base._stats(subset, fee)
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
            phase_metrics[phase] = base._stats(subset, fee)
    allowed_phases = [
        phase for phase, stats in phase_metrics.items()
        if stats['n'] >= 14 and stats['ev'] >= .01 and stats['pf'] >= 1.02
    ] or [
        phase for phase, stats in phase_metrics.items()
        if stats['n'] >= 24 and stats['ev'] > 0
    ]
    if not allowed_phases:
        return None
    score = overall['ev'] * 4.5 + math.log(max(overall['pf'], 1e-6)) * .35 + stability * .30 + profitable * .20 - dd * .004 + min(worst, .10) + min(len(allowed_regimes), 4) * .015
    return {
        'score': score, 'ev': overall['ev'], 'pf': overall['pf'], 'win': overall['win'],
        'n': len(selected_all), 'dd': dd, 'stability': stability, 'profitable_folds': profitable,
        'worst_fold_ev': worst, 'threshold': round(statistics.median(thresholds), 2), 'folds': fold_stats,
        'regime_metrics': regime_metrics, 'allowed_regimes': allowed_regimes,
        'phase_metrics': phase_metrics, 'allowed_phases': allowed_phases,
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


def _evaluate_incumbent(model: Any, meta: dict[str, Any], rows: list[dict[str, Any]], fee: float) -> dict[str, Any] | None:
    if model is None or not rows:
        return None
    allowed = set(meta.get('allowed_regimes') or [])
    phases = set(meta.get('allowed_phases') or [])
    scoped = [
        row for row in rows
        if (not allowed or str(row['regime']) in allowed)
        and (not phases or str(row['phase']) in phases)
    ]
    if not scoped:
        return None
    try:
        x = np.vstack([base._vec(row['features']) for row in scoped])
        probs = model.predict_proba(x)[:, 1]
    except Exception:
        return None
    threshold = float(meta.get('threshold') or .60)
    selected = [row for row, probability in zip(scoped, probs) if float(probability) >= threshold]
    stats = base._stats(selected, fee) if selected else {'n': 0, 'pf': 0.0, 'ev': -1.0, 'win': 0.0}
    dd = base._dd([base.f(row['pnl_r']) - fee for row in selected]) if selected else 999.0
    ci05 = _cluster_bootstrap_ev(selected, fee, 99231)
    return {**stats, 'selected_n': len(selected), 'max_drawdown_r': dd, 'clustered_ev_bootstrap_05': ci05}


class HistoricalEvolutionLearner(evo.GenomeEvolutionLearner):
    """True multi-generation Signal evolution with a sealed final chronological holdout.

    Candidate mutation/selection is performed only on the development era. The final
    chronological holdout is never inspected until the candidate has been fixed.
    """

    def train_strategy_direction(self, strategy: str, direction: str, min_train: int = 300, min_test: int = 120):
        rows = [x for x in self.store.samples(strategy, direction=direction) if x['source_quality'] >= 55]
        if len(rows) < 1100: return None
        n = len(rows); purge = 32
        previous = _latest_run(self.store, strategy, direction)
        previous_holdout_end = int((previous or {}).get('holdout_end_ts') or 0)
        if previous_holdout_end > 0:
            novel_start = next((i for i, row in enumerate(rows) if int(row['ts']) > previous_holdout_end), n)
            if n - novel_start < MIN_UNTOUCHED_HOLDOUT:
                return None
            holdout_start = novel_start
        else:
            holdout_start = max(760, int(n * (1.0 - FINAL_HOLDOUT_PCT)))
        development = rows[:max(0, holdout_start - purge)]
        holdout = rows[holdout_start:]
        if len(development) < 700 or len(holdout) < 160: return None

        def consume_holdout(status: str, reason: str, *, winner: dict[str, Any] | None = None,
                            metrics: dict[str, Any] | None = None) -> None:
            # Conservative rule: once this chronological block has been assigned to a
            # generation, every terminal path seals it. A later retry needs a wholly new
            # block even if failure happened before a probability model could be fit.
            payload = {'reason': reason, 'candidates_evaluated': 0, **(metrics or {})}
            _record_run(
                self.store, strategy, direction,
                development_end_ts=int(development[-1]['ts']),
                holdout_start_ts=int(holdout[0]['ts']), holdout_end_ts=int(holdout[-1]['ts']),
                status=status, winner=winner, metrics=payload,
            )

        rng = _rng(strategy, direction, int(holdout[-1]['ts']))
        population: list[dict[str, Any]] = []
        prior_genome = (previous or {}).get('winner_genome')
        if isinstance(prior_genome, dict):
            parent = {k: v for k, v in prior_genome.items() if k not in ('id', 'generation')}
            parent['feature_mode'] = 'macro_context'
            parent['phases'] = list(ALL_PHASES)
            parent['phase_scope_name'] = 'ALL'
            parent['evolution_stage'] = 'MACRO_REGIME'
            parent['generation'] = 0; parent['id'] = f"evo0_{_fingerprint(parent)}"
            population.append(parent)
            while len(population) < max(ELITES * 3, POPULATION // 2):
                population.append(_candidate(rng, 0, parent))
        while len(population) < POPULATION:
            population.append(_candidate(rng, 0))
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
            # A completed stage is frozen into its children, then the preceding-stage
            # candidates leave the pool. This makes the search truly macro -> structure
            # -> short-horizon instead of selecting every degree of freedom at once.
            carry = elites if generation == 0 or _evolution_stage(generation) == _evolution_stage(generation - 1) else []
            pool: dict[str, tuple[float, dict[str, Any], dict[str, Any]]] = {
                _fingerprint(item[1]): item for item in (carry + scored)
            }
            elites = sorted(pool.values(), key=lambda z: z[0], reverse=True)[:ELITES]
            if elites:
                best_history.append({'generation': generation, 'evolution_stage': _evolution_stage(generation), 'genome_id': elites[0][1]['id'], 'macro_scope': elites[0][1]['scope_name'], 'structure_scope': elites[0][1]['phase_scope_name'], 'new_candidates': len(scored), **elites[0][2]})
            if generation == GENERATIONS - 1 or not elites: break
            population = []
            while len(population) < POPULATION:
                if len(population) >= int(POPULATION * .82):
                    population.append(_candidate(rng, generation + 1))
                else:
                    parent = rng.choice(elites)[1]
                    population.append(_candidate(rng, generation + 1, parent))

        if not elites:
            _record_run(
                self.store, strategy, direction, development_end_ts=int(development[-1]['ts']),
                holdout_start_ts=int(holdout[0]['ts']), holdout_end_ts=int(holdout[-1]['ts']),
                status='NO_ELIGIBLE_DEVELOPMENT_CANDIDATE', winner=None,
                metrics={'candidates_evaluated': evaluated, 'development_generations': best_history},
            )
            return None
        _, winner, dev = elites[0]
        fixed_regimes = list(dev.get('allowed_regimes') or winner['regimes'])
        fixed_phases = list(dev.get('allowed_phases') or winner.get('phases') or ALL_PHASES)
        deployment_genome = {**winner, 'regimes': fixed_regimes, 'phases': fixed_phases}
        deployment_genome['id'] = f"evo{int(winner['generation'])}_{_fingerprint(deployment_genome)}"
        scoped_dev = _scope(development, deployment_genome); scoped_holdout = _scope(holdout, deployment_genome)
        idx = _indices(winner)
        if len(scoped_dev) < 500 or len(scoped_holdout) < 100:
            consume_holdout(
                'NO_SCOPE_HOLDOUT_SUPPORT',
                f'winner scope has insufficient sealed evidence: development={len(scoped_dev)}, holdout={len(scoped_holdout)}',
                winner=deployment_genome,
                metrics={'candidates_evaluated': evaluated, 'development_generations': best_history},
            )
            return None

        cal_n = max(90, int(len(scoped_dev) * .16))
        fit = scoped_dev[:max(0, len(scoped_dev) - cal_n - purge)]
        cal = scoped_dev[len(fit) + purge:]
        if len(fit) < 320 or len(cal) < 70:
            consume_holdout(
                'NO_CALIBRATION_SUPPORT', f'insufficient development-only fit/calibration rows: fit={len(fit)}, cal={len(cal)}',
                winner=deployment_genome, metrics={'candidates_evaluated': evaluated, 'development_generations': best_history},
            )
            return None
        yf = np.asarray([r['success'] for r in fit])
        if len(set(yf)) < 2:
            consume_holdout(
                'NO_CLASS_SUPPORT', 'development fit partition contains only one outcome class',
                winner=deployment_genome, metrics={'candidates_evaluated': evaluated, 'development_generations': best_history},
            )
            return None
        m = _model(winner, 99001)
        m.fit(_matrix(fit, idx), yf, sample_weight=_weights(fit, float(winner['half_life_days'])))
        cp = m.predict_proba(_matrix(cal, idx))[:, 1]
        th = _choose_threshold(cal, cp, self.fee_r)
        if th is None:
            consume_holdout(
                'NO_DEVELOPMENT_THRESHOLD', 'development-only calibration could not select a viable threshold',
                winner=deployment_genome, metrics={'candidates_evaluated': evaluated, 'development_generations': best_history},
            )
            return None
        threshold, _ = th

        yd = np.asarray([r['success'] for r in scoped_dev])
        if len(set(yd)) < 2:
            consume_holdout(
                'NO_CLASS_SUPPORT', 'complete development partition contains only one outcome class',
                winner=deployment_genome, metrics={'candidates_evaluated': evaluated, 'development_generations': best_history},
            )
            return None
        final_model = _model(winner, 99002)
        final_model.fit(_matrix(scoped_dev, idx), yd, sample_weight=_weights(scoped_dev, float(winner['half_life_days'])))
        hp = final_model.predict_proba(_matrix(scoped_holdout, idx))[:, 1]
        selected = [r for r, p in zip(scoped_holdout, hp) if float(p) >= threshold]
        stats = base._stats(selected, self.fee_r) if selected else {'n': 0, 'pf': 0.0, 'ev': -1.0, 'win': 0.0}
        dd = base._dd([base.f(r['pnl_r']) - self.fee_r for r in selected]) if selected else 999.0
        ci05 = _cluster_bootstrap_ev(selected, self.fee_r, 99117)
        labels = np.asarray([r['success'] for r in scoped_holdout], dtype=float)
        brier = float(np.mean((hp - labels) ** 2)) if len(labels) else 1.0
        span_days = max(1.0, (int(scoped_holdout[-1]['ts']) - int(scoped_holdout[0]['ts'])) / 86400.0)
        freq = len(selected) / span_days
        absolute_guard = bool(
            len(selected) >= MIN_HOLDOUT_SELECTED and stats['pf'] >= 1.12 and stats['ev'] >= .04 and
            ci05 > 0 and dd <= 14 and .02 <= freq <= 6 and dev['stability'] >= .72 and
            dev['profitable_folds'] >= .66 and dev['worst_fold_ev'] >= -.06 and brier <= .265
        )
        incumbent_model, incumbent_meta = self.store.champion(strategy, direction)
        incumbent = _evaluate_incumbent(incumbent_model, incumbent_meta, holdout, self.fee_r)
        incumbent_viable = bool(
            incumbent and incumbent['selected_n'] >= 40 and incumbent['pf'] >= 1.03 and
            incumbent['ev'] > 0 and incumbent['clustered_ev_bootstrap_05'] >= -.02
        )
        candidate_utility = stats['ev'] - .006 * dd + .025 * math.log(max(stats['pf'], 1e-6))
        incumbent_utility = (
            float(incumbent['ev']) - .006 * float(incumbent['max_drawdown_r']) +
            .025 * math.log(max(float(incumbent['pf']), 1e-6))
        ) if incumbent else -999.0
        comparison_passed = bool(not incumbent_viable or candidate_utility >= incumbent_utility + .005)
        promote = bool(absolute_guard and comparison_passed)
        reason = (
            f'multi-generation development evolution passed; {len(selected)} trades passed a never-before-seen chronological holdout and safely replaced the incumbent'
            if promote else
            f"evolved {evaluated} candidates across {len(best_history)} generations; absolute holdout passed but incumbent remained stronger on the same new evidence"
            if absolute_guard else
            f"evolved {evaluated} candidates across {len(best_history)} generations, but the new sealed holdout did not pass: n={len(selected)}, PF={stats['pf']:.2f}, EV={stats['ev']:+.3f}R, CI05={ci05:+.3f}R, DD={dd:.2f}R, Brier={brier:.3f}"
        )
        metrics = {
            'schema_version': 5, 'evolution_schema': GENOME_SCHEMA, 'evolution_runtime': VERSION,
            'strategy': strategy, 'direction': direction, 'generation': int(winner['generation']),
            'candidates_evaluated': evaluated, 'development_generations': best_history,
            'genome_id': deployment_genome['id'], 'winner_genome': deployment_genome, 'feature_mode': winner['feature_mode'],
            'feature_names': evo._feature_names(str(winner['feature_mode'])), 'feature_count': len(idx),
            'params': {k: winner[k] for k in ('learning_rate','max_iter','max_leaf_nodes','min_samples_leaf','l2_regularization')},
            'recency_half_life_days': winner['half_life_days'], 'regime_scope': winner['scope_name'],
            'structure_scope': winner['phase_scope_name'], 'hierarchical_search_order': ['MACRO_REGIME', 'MARKET_STRUCTURE', 'SHORT_HORIZON_SIGNAL'],
            'allowed_regimes': fixed_regimes, 'allowed_phases': fixed_phases,
            'development_regime_metrics': dev.get('regime_metrics') or {}, 'development_phase_metrics': dev.get('phase_metrics') or {}, 'threshold': threshold,
            'profit_factor': stats['pf'], 'expectancy_r': stats['ev'], 'test_win': stats['win'],
            'selected_n': len(selected), 'max_drawdown_r': dd, 'signals_per_day': freq,
            'brier': brier, 'stability': dev['stability'], 'clustered_ev_bootstrap_05': ci05,
            'effective_oos_selected_n': len(selected), 'absolute_guard_passed': absolute_guard,
            'champion_comparison_passed': comparison_passed, 'overfit_guard_passed': absolute_guard,
            'incumbent_same_holdout': incumbent, 'candidate_utility': candidate_utility,
            'incumbent_utility': incumbent_utility if incumbent else None,
            'trained_through_ts': int(development[-1]['ts']),
            'evaluated_through_ts': int(holdout[-1]['ts']),
            'validation_method': 'MULTI_GENERATION_DEV_ONLY_EVOLUTION_THEN_NEVER_REUSED_SEALED_CHRONOLOGICAL_HOLDOUT',
            'comparison_method': 'candidate and incumbent are compared once on the same new holdout; a failed holdout cannot be retried until a complete later holdout matures',
            'parent_holdout_end_ts': previous_holdout_end or None,
            'holdout_start_ts': int(holdout[0]['ts']), 'holdout_end_ts': int(holdout[-1]['ts']),
            'reason': reason,
        }
        wrapped = evo.GenomeModel(final_model, idx, deployment_genome['id'])
        self.store.save_challenger(strategy, direction, wrapped, metrics, promote)
        _record_run(
            self.store, strategy, direction, development_end_ts=int(development[-1]['ts']),
            holdout_start_ts=int(holdout[0]['ts']), holdout_end_ts=int(holdout[-1]['ts']),
            status='PROMOTED' if promote else 'ABSOLUTE_PASS_INCUMBENT_HELD' if absolute_guard else 'REJECTED_NEW_HOLDOUT',
            winner=deployment_genome, metrics=metrics,
        )
        return EvolutionEvaluation(
            strategy=strategy, direction=direction, train_n=len(development), test_n=len(scoped_holdout),
            selected_n=len(selected), test_win=stats['win'], profit_factor=stats['pf'], expectancy_r=stats['ev'],
            threshold=threshold, brier=brier, max_drawdown_r=dd, stability=dev['stability'], promoted=promote,
            reason=reason, generation=int(winner['generation']), candidates_evaluated=evaluated,
            regime_scope=str(winner['scope_name']), allowed_regimes=fixed_regimes,
            structure_scope=str(winner['phase_scope_name']), allowed_phases=fixed_phases,
            holdout_ev_bootstrap_05=ci05, signals_per_day=freq, genome_id=deployment_genome['id'],
            absolute_guard_passed=absolute_guard, champion_comparison_passed=comparison_passed,
            holdout_start_ts=int(holdout[0]['ts']), holdout_end_ts=int(holdout[-1]['ts']),
            evaluation_status='PROMOTED' if promote else 'ABSOLUTE_PASS_INCUMBENT_HELD' if absolute_guard else 'REJECTED_NEW_HOLDOUT',
        )


def evolution_status(core: Any) -> dict[str, Any]:
    con = core.db()
    try:
        exists = con.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='signal_evolution_runs'"
        ).fetchone()
        rows = [] if not exists else con.execute(
            '''SELECT r.strategy,r.direction,r.created_at,r.development_end_ts,
                      r.holdout_start_ts,r.holdout_end_ts,r.status,r.winner_genome,r.metrics
               FROM signal_evolution_runs r
               JOIN (SELECT strategy,direction,MAX(run_id) run_id FROM signal_evolution_runs
                     WHERE evolution_schema=? GROUP BY strategy,direction) latest
                 ON latest.run_id=r.run_id
               ORDER BY r.strategy,r.direction''',
            (GENOME_SCHEMA,),
        ).fetchall()
    finally:
        con.close()
    lineages: list[dict[str, Any]] = []
    counts: dict[str, int] = {}
    for row in rows:
        try:
            genome = json.loads(row[7]) if row[7] else {}
            metrics = json.loads(row[8]) if row[8] else {}
        except (TypeError, json.JSONDecodeError):
            genome, metrics = {}, {}
        status = str(row[6]); counts[status] = counts.get(status, 0) + 1
        lineages.append({
            'strategy': str(row[0]), 'direction': str(row[1]), 'created_at': int(row[2]),
            'development_end_ts': int(row[3] or 0), 'holdout_start_ts': int(row[4] or 0),
            'holdout_end_ts': int(row[5] or 0), 'status': status,
            'genome_id': genome.get('id'), 'macro_scope': genome.get('scope_name'),
            'structure_scope': genome.get('phase_scope_name'),
            'allowed_regimes': list(metrics.get('allowed_regimes') or []),
            'allowed_phases': list(metrics.get('allowed_phases') or []),
            'candidates_evaluated': int(metrics.get('candidates_evaluated') or 0),
            'selected_n': int(metrics.get('selected_n') or 0),
            'profit_factor': metrics.get('profit_factor'), 'expectancy_r': metrics.get('expectancy_r'),
            'holdout_ci05_r': metrics.get('clustered_ev_bootstrap_05'),
            'reason': metrics.get('reason'),
        })
    return {
        'runtime': VERSION, 'schema': GENOME_SCHEMA, 'latest_lineages': lineages,
        'status_counts': counts, 'same_failed_holdout_can_be_retried': False,
        'minimum_new_untouched_decisions': MIN_UNTOUCHED_HOLDOUT,
        'recertification_gate': core.state.get('evolution_recertification_gate') or {},
    }


def install(core: Any) -> None:
    # This is the final Signal learner authority. It deliberately leaves the historical
    # replay labels and the later Execution Evolution untouched.
    v5_runtime.Learner = HistoricalEvolutionLearner
    core.Learner = HistoricalEvolutionLearner
    core.state['historical_signal_evolution'] = {
        'runtime': VERSION, 'schema': GENOME_SCHEMA, 'generations': GENERATIONS,
        'population': POPULATION, 'elites': ELITES, 'final_holdout_pct': FINAL_HOLDOUT_PCT,
        'minimum_new_untouched_decisions': MIN_UNTOUCHED_HOLDOUT,
        'minimum_selected_holdout_trades': MIN_HOLDOUT_SELECTED,
        'fixed_strategy_direction_pairs_are_population_entrypoints': True,
        'hierarchical_search_order': ['MACRO_REGIME', 'MARKET_STRUCTURE', 'SHORT_HORIZON_SIGNAL'],
        'macro_and_structure_are_frozen_before_sealed_holdout': True,
        'same_failed_holdout_can_be_retried': False,
        'incumbent_compared_on_same_new_holdout': True,
        'rule': 'all mutation/selection occurs inside development history; final chronological holdout is sealed until winner is fixed',
        'no_lookahead': True, 'execution_learning_separate': True,
    }
    if not any(getattr(route, 'path', None) == '/api/v20/historical-evolution' for route in core.app.router.routes):
        @core.app.get('/api/v20/historical-evolution')
        def historical_evolution_status() -> dict[str, Any]:
            return evolution_status(core)
