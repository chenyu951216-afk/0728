from __future__ import annotations

import asyncio
import json
import math
import os
import random
import statistics
import time
from collections import Counter
from typing import Any

import numpy as np
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.metrics import brier_score_loss, log_loss

import adaptive_v5 as base
import execution_v7
import v5_runtime
import v7_runtime

EVOLUTION_VERSION = '7.1.0-20260809'
GENOME_SCHEMA = 1
LIVE_REAUDIT_BATCH = max(3, int(os.getenv('EVOLUTION_LIVE_REAUDIT_BATCH', '5')))
EXECUTION_RANDOM_CANDIDATES = max(48, min(160, int(os.getenv('EVOLUTION_EXECUTION_CANDIDATES', '96'))))

DERIVATIVE_FEATURES = {
    'spot_perp_basis_bps', 'funding', 'oi_change', 'book_imbalance',
    'liquidation_imbalance', 'liquidation_intensity', 'oi_available',
    'funding_available', 'liquidation_available', 'book_available',
    'oi_weighted_funding', 'taker_imbalance', 'crowd_skew',
    'top_position_skew', 'oi_weighted_funding_available', 'taker_available',
    'crowd_available', 'top_position_available', 'derivative_coverage',
    'derivative_quality', 'source_agreement_bps',
}
STRUCTURE_FEATURES = {
    'bos_up', 'bos_down', 'sweep_low', 'sweep_high', 'fvg_up', 'fvg_down',
    'wick_ratio', 'range_z', 'dist_vwap_atr', 'volume_z',
}
MOMENTUM_FEATURES = {
    'ret_1', 'ret_4', 'ret_16', 'ema20_gap', 'ema50_gap', 'ema20_slope',
    'atr_pct', 'atr_rank', 'adx', 'rsi', 'volume_z', 'range_z',
    'btc_ret_4', 'btc_ret_16', 'eth_btc_rel',
}
CONTEXT_FEATURES = {'hour_sin', 'hour_cos', 'dow_sin', 'dow_cos', 'macro_code', 'phase_code'}


def _feature_names(mode: str) -> list[str]:
    all_names = list(base.FEATURE_NAMES)
    if mode == 'all':
        return all_names
    if mode == 'price_action':
        return [x for x in all_names if x not in DERIVATIVE_FEATURES]
    if mode == 'momentum_structure':
        keep = MOMENTUM_FEATURES | STRUCTURE_FEATURES | CONTEXT_FEATURES
        return [x for x in all_names if x in keep]
    if mode == 'flow_structure':
        keep = STRUCTURE_FEATURES | DERIVATIVE_FEATURES | CONTEXT_FEATURES | {
            'ret_4', 'ret_16', 'ema20_gap', 'ema20_slope', 'atr_pct', 'atr_rank',
            'adx', 'rsi', 'btc_ret_4', 'eth_btc_rel',
        }
        return [x for x in all_names if x in keep]
    keep = MOMENTUM_FEATURES | STRUCTURE_FEATURES | CONTEXT_FEATURES | {
        'oi_change', 'funding', 'book_imbalance', 'oi_available', 'funding_available',
        'book_available', 'oi_weighted_funding', 'taker_imbalance',
        'oi_weighted_funding_available', 'taker_available', 'derivative_coverage',
    }
    return [x for x in all_names if x in keep]


GENOMES = (
    {
        'id': 'balanced_all_730d', 'feature_mode': 'all', 'half_life_days': 730,
        'params': {'learning_rate': .04, 'max_iter': 220, 'max_leaf_nodes': 15, 'min_samples_leaf': 30, 'l2_regularization': 1.6},
    },
    {
        'id': 'robust_price_1095d', 'feature_mode': 'price_action', 'half_life_days': 1095,
        'params': {'learning_rate': .032, 'max_iter': 230, 'max_leaf_nodes': 9, 'min_samples_leaf': 45, 'l2_regularization': 2.6},
    },
    {
        'id': 'responsive_momentum_365d', 'feature_mode': 'momentum_structure', 'half_life_days': 365,
        'params': {'learning_rate': .045, 'max_iter': 190, 'max_leaf_nodes': 19, 'min_samples_leaf': 28, 'l2_regularization': 2.0},
    },
    {
        'id': 'flow_structure_540d', 'feature_mode': 'flow_structure', 'half_life_days': 540,
        'params': {'learning_rate': .038, 'max_iter': 210, 'max_leaf_nodes': 13, 'min_samples_leaf': 34, 'l2_regularization': 2.2},
    },
    {
        'id': 'lean_regime_730d', 'feature_mode': 'lean', 'half_life_days': 730,
        'params': {'learning_rate': .035, 'max_iter': 240, 'max_leaf_nodes': 7, 'min_samples_leaf': 50, 'l2_regularization': 3.0},
    },
)


class GenomeModel:
    """Pickle-safe wrapper: production prediction still accepts the full feature vector."""

    def __init__(self, estimator: Any, indices: list[int], genome_id: str) -> None:
        self.estimator = estimator
        self.indices = list(indices)
        self.genome_id = genome_id

    def predict_proba(self, x: Any) -> np.ndarray:
        arr = np.asarray(x, dtype=float)
        if arr.ndim == 1:
            arr = arr.reshape(1, -1)
        return self.estimator.predict_proba(arr[:, self.indices])


def _indices(genome: dict[str, Any]) -> list[int]:
    names = set(_feature_names(str(genome['feature_mode'])))
    return [i for i, name in enumerate(base.FEATURE_NAMES) if name in names]


def _matrix(rows: list[dict[str, Any]], indices: list[int]) -> np.ndarray:
    return np.vstack([base._vec(r['features'])[indices] for r in rows])


def _weights(rows: list[dict[str, Any]], half_life_days: float, asof: int | None = None) -> np.ndarray:
    if not rows:
        return np.array([], dtype=float)
    asof = int(asof or rows[-1]['ts'])
    counts: dict[str, int] = {}
    for r in rows:
        counts[str(r['regime'])] = counts.get(str(r['regime']), 0) + 1
    med = statistics.median(counts.values()) if counts else 1.0
    hl = max(180.0, float(half_life_days)) * 86400.0
    out = []
    for r in rows:
        age = max(0, asof - int(r['ts']))
        recency = .30 + .70 * math.exp(-math.log(2) * age / hl)
        balance = base.clamp(math.sqrt(med / max(counts[str(r['regime'])], 1)), .70, 1.45)
        quality = base.clamp(base.f(r.get('source_quality'), 75) / 100.0, .55, 1.0)
        out.append(recency * balance * quality)
    return np.asarray(out, dtype=float)


def _estimator(genome: dict[str, Any], seed: int) -> HistGradientBoostingClassifier:
    return HistGradientBoostingClassifier(random_state=seed, **dict(genome['params']))


def _inner_pick(fit: list[dict[str, Any]], cal: list[dict[str, Any]], fee_r: float, seed: int) -> tuple[dict[str, Any], float, dict[str, Any]] | None:
    yf = np.array([r['success'] for r in fit])
    yc = np.array([r['success'] for r in cal])
    if len(set(yf)) < 2 or len(set(yc)) < 2:
        return None
    best: tuple[float, dict[str, Any], float, dict[str, Any]] | None = None
    for gi, genome in enumerate(GENOMES):
        idx = _indices(genome)
        if len(idx) < 8:
            continue
        model = _estimator(genome, seed + gi * 31)
        model.fit(_matrix(fit, idx), yf, sample_weight=_weights(fit, genome['half_life_days'], int(fit[-1]['ts'])))
        probs = model.predict_proba(_matrix(cal, idx))[:, 1]
        threshold, threshold_meta = base._threshold(cal, probs, fee_r)
        chosen = [r for r, p in zip(cal, probs) if float(p) >= threshold]
        if len(chosen) < 28:
            continue
        stats = base._stats(chosen, fee_r)
        brier = float(brier_score_loss(yc, probs))
        dd = base._dd([base.f(r['pnl_r']) - fee_r for r in chosen])
        utility = float((threshold_meta or {}).get('utility', -999.0))
        score = utility - .22 * brier - .0025 * dd - .0008 * len(idx)
        meta = {
            'genome_id': genome['id'], 'feature_mode': genome['feature_mode'],
            'feature_count': len(idx), 'half_life_days': genome['half_life_days'],
            'params': genome['params'], 'threshold': threshold, 'calibration': threshold_meta,
            'cal_pf': stats['pf'], 'cal_ev_r': stats['ev'], 'cal_win': stats['win'],
            'cal_n': stats['n'], 'cal_brier': brier, 'cal_dd_r': dd, 'inner_score': score,
        }
        if best is None or score > best[0]:
            best = (score, genome, threshold, meta)
    if best is None:
        return None
    return best[1], best[2], best[3]


class GenomeEvolutionLearner(base.Learner):
    """Nested genome search. Genome/threshold selection happens only inside each training fold."""

    def train_strategy_direction(self, strategy: str, direction: str, min_train: int = 300, min_test: int = 120):
        rows = [x for x in self.store.samples(strategy, direction=direction) if x['source_quality'] >= 55]
        if len(rows) < min_train + min_test + 80:
            return None
        purge = 32
        n = len(rows)
        first = max(min_train + purge, int(n * .50))
        remain = n - first
        folds = 4 if remain >= 4 * min_test else 3 if remain >= 3 * min_test else 2
        fold_stats: list[dict[str, Any]] = []
        oos_rows: list[dict[str, Any]] = []
        oos_probs: list[float] = []
        selected: list[dict[str, Any]] = []
        thresholds: list[float] = []
        genome_choices: list[str] = []
        inner_history: list[dict[str, Any]] = []

        for fold in range(folds):
            test_start = first + fold * max(min_test, remain // folds)
            test_end = n if fold == folds - 1 else min(n, test_start + max(min_test, remain // folds))
            train = rows[:max(0, test_start - purge)]
            test = rows[test_start:test_end]
            if len(train) < min_train or len(test) < 60:
                continue
            calibration_n = max(80, int(len(train) * .20))
            fit_end = len(train) - calibration_n - purge
            if fit_end < 220:
                continue
            fit = train[:fit_end]
            cal = train[fit_end + purge:]
            yt = np.array([r['success'] for r in test])
            if len(set(yt)) < 2:
                continue
            picked = _inner_pick(fit, cal, self.fee_r, 8100 + fold * 100)
            if picked is None:
                continue
            genome, threshold, inner_meta = picked
            idx = _indices(genome)
            train_y = np.array([r['success'] for r in train])
            if len(set(train_y)) < 2:
                continue
            model = _estimator(genome, 8200 + fold)
            model.fit(_matrix(train, idx), train_y, sample_weight=_weights(train, genome['half_life_days'], int(train[-1]['ts'])))
            probs = model.predict_proba(_matrix(test, idx))[:, 1]
            chosen = [r for r, p in zip(test, probs) if float(p) >= threshold]
            stats = base._stats(chosen, self.fee_r) if chosen else {'n': 0, 'pf': 0.0, 'ev': -1.0, 'win': 0.0}
            fold_stats.append({
                **stats, 'threshold': threshold,
                'dd': base._dd([base.f(r['pnl_r']) - self.fee_r for r in chosen]),
                'genome_id': genome['id'], 'start_ts': int(test[0]['ts']), 'end_ts': int(test[-1]['ts']),
                'inner_selection': inner_meta,
            })
            thresholds.append(float(threshold))
            genome_choices.append(str(genome['id']))
            inner_history.append(inner_meta)
            oos_rows += test
            oos_probs += list(map(float, probs))
            selected += chosen

        if len(fold_stats) < 2 or len(oos_rows) < min_test or len(selected) < 60:
            return None

        stats = base._stats(selected, self.fee_r)
        pnls = [base.f(r['pnl_r']) - self.fee_r for r in selected]
        brier = float(brier_score_loss(np.array([r['success'] for r in oos_rows]), np.array(oos_probs)))
        ll = float(log_loss(np.array([r['success'] for r in oos_rows]), np.array(oos_probs), labels=[0, 1]))
        evs = [x['ev'] for x in fold_stats]
        wins = [x['win'] for x in fold_stats]
        stability = base.clamp(1 - .65 * (statistics.pstdev(evs) if len(evs) > 1 else 0) - .55 * (statistics.pstdev(wins) if len(wins) > 1 else 0), 0, 1)
        worst_fold = min(evs)
        profitable_folds = sum(x > 0 for x in evs) / len(evs)
        dd = base._dd(pnls)
        threshold = round(statistics.median(thresholds), 2)
        span_days = max(1.0, (int(oos_rows[-1]['ts']) - int(oos_rows[0]['ts'])) / 86400.0)
        frequency = len(selected) / span_days
        regime_metrics: dict[str, Any] = {}
        for rg in base.REGIMES:
            z = [r for r in selected if r['regime'] == rg]
            if z:
                regime_metrics[rg] = base._stats(z, self.fee_r)
        allowed_regimes = [rg for rg, z in regime_metrics.items() if z['n'] >= 18 and z['ev'] >= .02 and z['pf'] >= 1.04] or [rg for rg, z in regime_metrics.items() if z['n'] >= 30 and z['ev'] > 0]
        recent = fold_stats[-1]
        recent_ev = float(recent['ev'])
        recent_pf = float(recent['pf'])

        old, old_meta = self.store.champion(strategy, direction)
        old_ev = old_pf = old_age_days = None
        improve = True
        if old is not None:
            old_ev = base.f(old_meta.get('expectancy_r'), -9.0)
            old_pf = base.f(old_meta.get('profit_factor'), 0.0)
            row = self.store.con.execute("SELECT created_at FROM model_registry WHERE strategy=? AND direction=? AND status='CHAMPION' ORDER BY version DESC LIMIT 1", (strategy, direction)).fetchone()
            old_created = int(row[0]) if row else int(time.time())
            old_age_days = max(0.0, (time.time() - old_created) / 86400.0)
            normal_upgrade = stats['ev'] >= old_ev - .012 and stats['pf'] >= old_pf * .975
            stale_rotation = old_age_days >= 45 and stats['ev'] >= max(.08, old_ev * .82) and stats['pf'] >= max(1.20, old_pf * .92) and recent_ev >= .08 and recent_pf >= 1.10
            improve = bool(normal_upgrade or stale_rotation)

        core_ok = bool(
            stats['pf'] >= 1.20 and stats['ev'] >= .08 and stability >= .80 and brier <= .255 and
            dd <= 16 and worst_fold >= -.05 and profitable_folds >= .66 and len(selected) >= 60 and
            .04 <= frequency <= 6 and bool(allowed_regimes) and recent_ev >= .025 and recent_pf >= 1.04
        )
        promote = bool(core_ok and improve)
        votes = Counter(genome_choices)
        max_vote = max(votes.values())
        tied = {g for g, c in votes.items() if c == max_vote}
        final_genome_id = next((g for g in reversed(genome_choices) if g in tied), genome_choices[-1])
        final_genome = next(g for g in GENOMES if g['id'] == final_genome_id)
        final_idx = _indices(final_genome)
        final_estimator = _estimator(final_genome, 8999)
        final_estimator.fit(_matrix(rows, final_idx), np.array([r['success'] for r in rows]), sample_weight=_weights(rows, final_genome['half_life_days'], int(rows[-1]['ts'])))
        final_model = GenomeModel(final_estimator, final_idx, final_genome_id)
        reason = 'nested genome search + purged OOS + recent fold + safe Champion comparison passed' if promote else f"rejected evolution: PF={stats['pf']:.2f}, EV={stats['ev']:.3f}R, recentEV={recent_ev:.3f}R, recentPF={recent_pf:.2f}, selected={len(selected)}, worstFold={worst_fold:.3f}R, profitableFolds={profitable_folds:.0%}, stability={stability:.2f}, brier={brier:.3f}, DD={dd:.1f}R, freq={frequency:.2f}/day, safeImprove={improve}"
        meta = {
            'schema_version': 4, 'evolution_schema': GENOME_SCHEMA, 'strategy': strategy, 'direction': direction,
            'train_n': first - purge, 'test_n': len(oos_rows), 'selected_n': len(selected),
            'test_win': stats['win'], 'profit_factor': stats['pf'], 'expectancy_r': stats['ev'],
            'threshold': threshold, 'brier': brier, 'logloss': ll, 'max_drawdown_r': dd,
            'stability': stability, 'worst_fold_ev_r': worst_fold, 'profitable_fold_ratio': profitable_folds,
            'signals_per_day': frequency, 'folds': fold_stats, 'regime_metrics': regime_metrics,
            'allowed_regimes': allowed_regimes, 'recent_fold_ev_r': recent_ev, 'recent_fold_pf': recent_pf,
            'trained_through_ts': int(rows[-1]['ts']), 'old_oos_ev_r': old_ev, 'old_oos_pf': old_pf,
            'old_age_days': old_age_days, 'comparison_method': 'stored_clean_oos_metrics_only; genome chosen inside train/cal folds; untouched fold is never used to select genome',
            'genome_id': final_genome_id, 'genome_votes': dict(votes), 'genome_inner_history': inner_history,
            'feature_mode': final_genome['feature_mode'], 'feature_names': _feature_names(final_genome['feature_mode']),
            'feature_count': len(final_idx), 'recency_half_life_days': final_genome['half_life_days'],
            'model_params': final_genome['params'], 'genome_candidates': [g['id'] for g in GENOMES],
            'reason': reason,
        }
        self.store.save_challenger(strategy, direction, final_model, meta, promote)
        return base.StrategyEvaluation(strategy, direction, first - purge, len(oos_rows), len(selected), stats['win'], stats['pf'], stats['ev'], threshold, brier, dd, stability, promote, reason)


def _strategy_seed(strategy: str) -> int:
    return 17000 + sum((i + 1) * ord(ch) for i, ch in enumerate(strategy))


def evolving_policy_candidates(strategy: str) -> list[dict[str, Any]]:
    """Broad deterministic search on development data; untouched audit remains untouched."""
    original = _ORIGINAL_POLICY_CANDIDATES(strategy)
    step = max(1, len(original) // 48)
    anchors = [dict(x) for x in original[::step][:48]]
    rng = random.Random(_strategy_seed(strategy))
    base_entry = execution_v7._base_entry_factor(strategy)
    out = list(anchors)
    for _ in range(EXECUTION_RANDOM_CANDIDATES):
        entry_atr = round(rng.uniform(max(.02, base_entry * .35), min(.32, base_entry * 3.2)), 4)
        stop_atr = round(rng.uniform(.70, 2.80), 2)
        tp1 = round(rng.uniform(.55, 1.15), 2)
        tp2 = round(max(tp1 + .30, rng.uniform(1.05, 1.90)), 2)
        tp3 = round(max(tp2 + .35, rng.uniform(1.55, 2.90)), 2)
        tp4 = round(max(tp3 + .45, rng.uniform(2.25, 4.80)), 2)
        lock2 = round(rng.uniform(.20, .90), 2)
        lock3 = round(max(lock2 + .25, rng.uniform(.65, 1.65)), 2)
        out.append({
            'schema': execution_v7.EXECUTION_SCHEMA,
            'entry_atr': entry_atr, 'stop_atr': stop_atr,
            'structure_mode': rng.choice(('15m', '30m', '1h', 'balanced')),
            'target_rr': [tp1, tp2, tp3, tp4], 'allocations': [20, 30, 30, 20],
            'lock_after_tp1_r': 0.0, 'lock_after_tp2_r': lock2, 'lock_after_tp3_r': lock3,
            'expire_bars': rng.randint(4, 12), 'max_hold_bars': rng.randint(20, 48),
            'all_in_cost_bps': execution_v7.ALL_IN_COST_BPS,
            'min_stop_pct': execution_v7.MIN_STOP_PCT,
            'search_origin': 'EVOLUTION_CONTINUOUS_DEV_ONLY',
        })
    return out


_ORIGINAL_POLICY_CANDIDATES = execution_v7.policy_candidates


def _migrate(core: Any) -> None:
    con = core.db()
    con.execute('''CREATE TABLE IF NOT EXISTS evolution_trade_ledger(
        signal_id TEXT PRIMARY KEY,
        created_at INTEGER NOT NULL,
        closed_at INTEGER,
        strategy TEXT NOT NULL,
        direction TEXT NOT NULL,
        regime TEXT,
        phase TEXT,
        model_version INTEGER,
        execution_version INTEGER,
        genome_id TEXT,
        probability REAL,
        threshold REAL,
        entry REAL,
        initial_stop REAL,
        stop_pct REAL,
        equity_usdt REAL,
        initial_risk_usdt REAL,
        notional_usdt REAL,
        status TEXT NOT NULL,
        exit_price REAL,
        exit_reason TEXT,
        realized_r REAL,
        realized_usdt REAL,
        review_label TEXT,
        payload TEXT NOT NULL
    )''')
    con.execute('CREATE INDEX IF NOT EXISTS ix_evolution_ledger_versions ON evolution_trade_ledger(strategy,direction,model_version,execution_version,created_at)')
    con.commit(); con.close()


def _sync_ledger(core: Any, signal_id: str) -> None:
    row = v7_runtime._signal_by_id(core, signal_id)
    if not row:
        return
    payload = row.get('payload') or {}
    selection = payload.get('selection') or {}
    model = selection.get('model') or {}
    model_meta = model.get('metrics') or {}
    ex = payload.get('execution_validation') or {}
    risk = payload.get('risk_snapshot') or {}
    realized_r = float(row.get('realized_r') or 0.0) if row.get('status') == 'CLOSED' else None
    initial_risk = float(risk.get('initial_risk_usdt') or 0.0)
    realized_usdt = realized_r * initial_risk if realized_r is not None and initial_risk > 0 else None
    stop_pct = abs(float(row.get('entry') or 0) - float(row.get('initial_stop') or 0)) / max(float(row.get('entry') or 0), 1e-9)
    con = core.db()
    con.execute('''INSERT INTO evolution_trade_ledger(
        signal_id,created_at,closed_at,strategy,direction,regime,phase,model_version,execution_version,genome_id,probability,threshold,entry,initial_stop,stop_pct,equity_usdt,initial_risk_usdt,notional_usdt,status,exit_price,exit_reason,realized_r,realized_usdt,review_label,payload
    ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    ON CONFLICT(signal_id) DO UPDATE SET
        closed_at=excluded.closed_at,status=excluded.status,exit_price=excluded.exit_price,exit_reason=excluded.exit_reason,realized_r=excluded.realized_r,realized_usdt=excluded.realized_usdt,review_label=excluded.review_label,payload=excluded.payload''', (
        row['signal_id'], int(row.get('created_at') or 0), row.get('exit_ts'), row['strategy'], row['direction'], row.get('regime'), row.get('phase'),
        model.get('model_version'), ex.get('execution_version'), model_meta.get('genome_id'), float(row.get('probability') or 0), float(selection.get('threshold') or model_meta.get('threshold') or 0),
        float(row.get('entry') or 0), float(row.get('initial_stop') or 0), stop_pct, float(risk.get('equity_usdt') or 0), initial_risk, risk.get('notional_usdt'),
        row.get('status') or '', row.get('exit_price'), row.get('exit_reason'), realized_r, realized_usdt, row.get('review_label'), json.dumps(payload, ensure_ascii=False),
    ))
    con.commit(); con.close()


def _evolution_summary(core: Any, row: dict[str, Any]) -> str:
    payload = row.get('payload') or {}
    selection = payload.get('selection') or {}
    model = selection.get('model') or {}
    mm = model.get('metrics') or {}
    ev = payload.get('execution_validation') or {}
    plan = payload.get('initial_plan') or {}
    profile = plan.get('profile') or {}
    risk = payload.get('risk_snapshot') or {}
    targets = row.get('targets') or []
    target_text = '｜'.join(f"TP{i+1} `{float(x.get('price') or 0):,.2f}` ({float(x.get('rr') or 0):.2f}R/{int(x.get('allocation') or 0)}%)" for i, x in enumerate(targets))
    signal_v = model.get('model_version') or mm.get('version') or '—'
    exec_v = ev.get('execution_version') or '—'
    threshold = float(selection.get('threshold') or mm.get('threshold') or 0)
    stop_pct = abs(float(row.get('entry') or 0) - float(row.get('initial_stop') or 0)) / max(float(row.get('entry') or 0), 1e-9)
    lines = [
        f"ID `{row.get('signal_id')}`｜`{row.get('direction')}`｜`{row.get('strategy')}`｜{row.get('regime')}/{row.get('phase')}",
        f"Signal v`{signal_v}`｜Genome `{mm.get('genome_id') or 'legacy'}`｜信心 `{float(row.get('probability') or 0):.1%}` / 門檻 `{threshold:.1%}`",
        f"Signal OOS PF `{float(mm.get('profit_factor') or 0):.2f}`｜EV `{float(mm.get('expectancy_r') or 0):+.3f}R`｜勝率 `{float(mm.get('test_win') or 0):.1%}`｜最近Fold EV `{float(mm.get('recent_fold_ev_r') or 0):+.3f}R`",
        f"Execution v`{exec_v}`｜Audit PF `{float(ev.get('audit_pf') or 0):.2f}`｜EV `{float(ev.get('audit_ev_r') or 0):+.3f}R`｜CI05 `{float(ev.get('audit_ev_ci05_r') or 0):+.3f}R`｜fills `{int(ev.get('audit_fills') or 0)}`",
        f"Entry `{float(row.get('entry') or 0):,.2f}`｜初始SL `{float(row.get('initial_stop') or 0):,.2f}` ({stop_pct:.2%})｜結構 `{profile.get('structure_used') or '—'}`",
        target_text,
    ]
    if float(risk.get('equity_usdt') or 0) > 0:
        lines.append(f"帳戶 `{float(risk.get('equity_usdt') or 0):,.2f}U`｜初始風險 `{float(risk.get('initial_risk_usdt') or 0):,.2f}U` ({float(risk.get('risk_pct') or 0):.1%})｜名目 `{float(risk.get('notional_usdt') or 0):,.2f}U`")
    if row.get('status') == 'CLOSED':
        rr = float(row.get('realized_r') or 0)
        pnl_u = rr * float(risk.get('initial_risk_usdt') or 0)
        lines.append(f"結果 `{rr:+.2f}R`｜估算損益 `{pnl_u:+.2f}U`｜Exit `{float(row.get('exit_price') or 0):,.2f}`｜`{row.get('exit_reason')}`")
    lines.append('學習：本單會進 deployment evidence；Signal 只在未來標籤成熟後進乾淨 Challenger，不會用單筆輸贏直接改模型。')
    return '\n'.join(lines)


def install(core: Any) -> None:
    _migrate(core)

    # Signal evolution: nested train/cal genome search inside every purged OOS fold.
    v5_runtime.Learner = GenomeEvolutionLearner
    core.Learner = GenomeEvolutionLearner

    # Execution evolution: broaden parameter search, but keep validation and untouched audit unchanged.
    execution_v7.policy_candidates = evolving_policy_candidates
    execution_v7.ALLOCATIONS = (
        (20, 30, 30, 20), (25, 30, 25, 20), (30, 30, 25, 15),
        (20, 25, 30, 25), (35, 30, 20, 15), (25, 25, 25, 25),
    )

    original_create = v7_runtime.create_signal_v7
    def create_with_evidence(c: Any, analysis: dict[str, Any], m15: list[dict[str, Any]]):
        row = original_create(c, analysis, m15)
        if not row:
            return row
        payload = row.get('payload') or {}
        if not payload.get('risk_snapshot'):
            sizing = c._notional_for_risk(float(row['entry']), float(row['initial_stop']))
            equity = float(sizing.get('equity_usdt') or 0)
            payload['risk_snapshot'] = {
                **sizing,
                'initial_risk_usdt': equity * float(c.RISK_PER_TRADE) if equity > 0 else 0.0,
                'captured_at': int(time.time()),
            }
            payload['evolution_version'] = EVOLUTION_VERSION
            con = c.db(); con.execute('UPDATE signals SET payload=? WHERE signal_id=?', (json.dumps(payload, ensure_ascii=False), row['signal_id'])); con.commit(); con.close()
            row = v7_runtime._signal_by_id(c, row['signal_id']) or row
        _sync_ledger(c, row['signal_id'])
        return row
    v7_runtime.create_signal_v7 = create_with_evidence
    core.create_signal = lambda analysis, m15: create_with_evidence(core, analysis, m15)

    original_ingest = v7_runtime.ingest_completed_live_samples_v7
    def ingest_with_evolution(c: Any) -> int:
        added = int(original_ingest(c) or 0)
        con = c.db(); ids = [str(x[0]) for x in con.execute("SELECT signal_id FROM signals WHERE status='CLOSED' ORDER BY exit_ts DESC LIMIT 200").fetchall()]; con.close()
        for sid in ids:
            _sync_ledger(c, sid)
        if added > 0:
            pending = int(c.get_state('evolution_live_evidence_pending', 0) or 0) + added
            c.set_state('evolution_live_evidence_pending', pending)
            c.set_state('evolution_last_live_evidence_ts', int(time.time()))
            c.state.setdefault('learning', {})['evolution_live_samples_added'] = added
            c.state['learning']['evolution_live_evidence_pending'] = pending
        return added
    v7_runtime.ingest_completed_live_samples_v7 = ingest_with_evolution
    core.ingest_completed_live_samples = lambda: ingest_with_evolution(core)

    original_learning_tick = core.learning_tick
    async def evolution_learning_tick() -> None:
        await original_learning_tick()
        pending = int(core.get_state('evolution_live_evidence_pending', 0) or 0)
        if pending >= LIVE_REAUDIT_BATCH:
            results = await asyncio.to_thread(execution_v7.optimize_all, core, True)
            core.state['execution_learning'] = {
                'version': EVOLUTION_VERSION, 'results': results,
                'registry': v7_runtime._execution_status(core)[:50],
                'updated_at': time.time(), 'reason': f'{pending} new live execution evidence samples',
            }
            await v7_runtime._notify_execution_results(core, results)
            core.set_state('evolution_live_evidence_pending', 0)
            core.set_state('evolution_last_execution_reaudit_ts', int(time.time()))
        core.state.setdefault('learning', {})['evolution'] = {
            'version': EVOLUTION_VERSION,
            'signal_genome_search': True,
            'genomes': [g['id'] for g in GENOMES],
            'execution_continuous_candidates': EXECUTION_RANDOM_CANDIDATES,
            'live_evidence_reaudit_batch': LIVE_REAUDIT_BATCH,
            'live_evidence_pending': int(core.get_state('evolution_live_evidence_pending', 0) or 0),
            'safety': 'live outcomes never become direct Signal labels; point-in-time matured market labels remain mandatory',
        }
    core.learning_tick = evolution_learning_tick

    # All existing Discord lifecycle messages automatically gain model versions, OOS evidence and risk sizing.
    v7_runtime._summary = _evolution_summary
    core.state['runtime_version'] = EVOLUTION_VERSION
    core.app.version = '7.1.0'

    if not any(getattr(r, 'path', None) == '/api/v8/evolution' for r in core.app.router.routes):
        @core.app.get('/api/v8/evolution')
        def evolution_status() -> dict[str, Any]:
            con = core.db()
            total = int(con.execute('SELECT COUNT(*) FROM evolution_trade_ledger').fetchone()[0])
            closed = int(con.execute("SELECT COUNT(*) FROM evolution_trade_ledger WHERE status='CLOSED'").fetchone()[0])
            pnl_r = float(con.execute("SELECT COALESCE(SUM(realized_r),0) FROM evolution_trade_ledger WHERE status='CLOSED'").fetchone()[0] or 0)
            pnl_u = float(con.execute("SELECT COALESCE(SUM(realized_usdt),0) FROM evolution_trade_ledger WHERE status='CLOSED'").fetchone()[0] or 0)
            recent = [dict(x) for x in con.execute('SELECT signal_id,created_at,closed_at,strategy,direction,model_version,execution_version,genome_id,status,realized_r,realized_usdt,review_label FROM evolution_trade_ledger ORDER BY created_at DESC LIMIT 30').fetchall()]
            con.close()
            return {
                'runtime': EVOLUTION_VERSION,
                'signal_evolution': {'genomes': [g['id'] for g in GENOMES], 'nested_selection': True, 'point_in_time_oos_required': True},
                'execution_evolution': {'continuous_candidates': EXECUTION_RANDOM_CANDIDATES, 'untouched_audit_required': True},
                'live_learning': {'reaudit_batch': LIVE_REAUDIT_BATCH, 'pending': int(core.get_state('evolution_live_evidence_pending', 0) or 0), 'direct_signal_label_mutation': False},
                'ledger': {'signals': total, 'closed': closed, 'net_r': pnl_r, 'estimated_net_usdt': pnl_u, 'recent': recent},
            }
