from __future__ import annotations

from typing import Any

import numpy as np

import adaptive_v5 as base
import execution_v7
import v8_evolution as evolution


def evolving_signal_oof_opportunities(core: Any, strategy: str, direction: str) -> list[dict[str, Any]]:
    """Generate execution opportunities using only models/genomes available before each test fold."""
    con = core.db(); store = base.ModelStore(con)
    rows = [x for x in store.samples(strategy, direction=direction) if x['source_quality'] >= 55]
    con.close()
    min_train, min_test, purge = 300, 120, 32
    if len(rows) < min_train + min_test + 80:
        return []
    n = len(rows)
    first = max(min_train + purge, int(n * .50))
    remain = n - first
    folds = 4 if remain >= 4 * min_test else 3 if remain >= 3 * min_test else 2
    out: list[dict[str, Any]] = []
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
        picked = evolution._inner_pick(fit, cal, .035, 9100 + fold * 100)
        if picked is None:
            continue
        genome, threshold, _ = picked
        idx = evolution._indices(genome)
        train_y = np.array([r['success'] for r in train])
        if len(set(train_y)) < 2:
            continue
        model = evolution._estimator(genome, 9200 + fold)
        model.fit(evolution._matrix(train, idx), train_y, sample_weight=evolution._weights(train, genome['half_life_days'], int(train[-1]['ts'])))
        probs = model.predict_proba(evolution._matrix(test, idx))[:, 1]
        for row, probability in zip(test, probs):
            if float(probability) >= float(threshold):
                out.append({
                    'ts': int(row['ts']), 'regime': str(row['regime']),
                    'probability': float(probability), 'threshold': float(threshold),
                    'fold': fold, 'genome_id': genome['id'],
                })
    return out


def install() -> None:
    execution_v7._signal_oof_opportunities = evolving_signal_oof_opportunities
