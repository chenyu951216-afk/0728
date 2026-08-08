from __future__ import annotations

import statistics
import time
from typing import Any

import numpy as np
from sklearn.metrics import brier_score_loss, log_loss

import adaptive_v5 as base
import v5_runtime


class EvolutionLearner(base.Learner):
    """Signal learner that never compares an old final model on rows it may have seen."""

    def train_strategy_direction(self, strategy: str, direction: str, min_train: int = 300, min_test: int = 120):
        rows = [x for x in self.store.samples(strategy, direction=direction) if x['source_quality'] >= 55]
        if len(rows) < min_train + min_test + 80:
            return None
        purge = 32; n = len(rows); first = max(min_train + purge, int(n * .50)); remain = n - first
        folds = 4 if remain >= 4 * min_test else 3 if remain >= 3 * min_test else 2
        fs = []; ots = []; ops = []; selected = []; ths = []
        for fold in range(folds):
            ts = first + fold * max(min_test, remain // folds); te = n if fold == folds - 1 else min(n, ts + max(min_test, remain // folds)); train = rows[:max(0, ts - purge)]; test = rows[ts:te]
            if len(train) < min_train or len(test) < 60:
                continue
            cn = max(80, int(len(train) * .2)); fe = len(train) - cn - purge
            if fe < 220:
                continue
            fit = train[:fe]; cal = train[fe + purge:]
            yf = np.array([r['success'] for r in fit]); yc = np.array([r['success'] for r in cal]); yt = np.array([r['success'] for r in test])
            if min(len(set(yf)), len(set(yc)), len(set(yt))) < 2:
                continue
            mi = self._model(100 + fold); mi.fit(np.vstack([base._vec(r['features']) for r in fit]), yf, sample_weight=base._weights(fit, int(fit[-1]['ts']))); cp = mi.predict_proba(np.vstack([base._vec(r['features']) for r in cal]))[:, 1]; th, tmeta = base._threshold(cal, cp, self.fee_r)
            mo = self._model(200 + fold); yo = np.array([r['success'] for r in train]); mo.fit(np.vstack([base._vec(r['features']) for r in train]), yo, sample_weight=base._weights(train, int(train[-1]['ts']))); pr = mo.predict_proba(np.vstack([base._vec(r['features']) for r in test]))[:, 1]; ch = [r for r, p in zip(test, pr) if p >= th]; st = base._stats(ch, self.fee_r) if ch else {'n': 0, 'pf': 0., 'ev': -1., 'win': 0.}
            fs.append({**st, 'threshold': th, 'dd': base._dd([base.f(r['pnl_r']) - self.fee_r for r in ch]), 'threshold_calibration': tmeta, 'start_ts': int(test[0]['ts']), 'end_ts': int(test[-1]['ts'])}); ths.append(th); ots += test; ops += list(map(float, pr)); selected += ch
        if len(fs) < 2 or len(ots) < min_test or len(selected) < 60:
            return None
        st = base._stats(selected, self.fee_r); pn = [base.f(r['pnl_r']) - self.fee_r for r in selected]; b = brier_score_loss(np.array([r['success'] for r in ots]), np.array(ops)); ll = log_loss(np.array([r['success'] for r in ots]), np.array(ops), labels=[0, 1]); evs = [x['ev'] for x in fs]; wins = [x['win'] for x in fs]; stab = base.clamp(1 - .65 * (statistics.pstdev(evs) if len(evs) > 1 else 0) - .55 * (statistics.pstdev(wins) if len(wins) > 1 else 0), 0, 1); wf = min(evs); prof = sum(x > 0 for x in evs) / len(evs); dd = base._dd(pn); th = round(statistics.median(ths), 2); span = max(1., (int(ots[-1]['ts']) - int(ots[0]['ts'])) / 86400); freq = len(selected) / span
        rm = {}
        for rg in base.REGIMES:
            z = [r for r in selected if r['regime'] == rg]
            if z: rm[rg] = base._stats(z, self.fee_r)
        allowed = [rg for rg, z in rm.items() if z['n'] >= 18 and z['ev'] >= .02 and z['pf'] >= 1.04] or [rg for rg, z in rm.items() if z['n'] >= 30 and z['ev'] > 0]
        recent = fs[-1]; recent_ev = float(recent['ev']); recent_pf = float(recent['pf'])
        old, om = self.store.champion(strategy, direction); old_ev = old_pf = None; improve = True; old_age_days = None
        if old is not None:
            old_ev = base.f(om.get('expectancy_r'), -9.0); old_pf = base.f(om.get('profit_factor'), 0.0)
            row = self.store.con.execute("SELECT created_at FROM model_registry WHERE strategy=? AND direction=? AND status='CHAMPION' ORDER BY version DESC LIMIT 1", (strategy, direction)).fetchone(); old_created = int(row[0]) if row else int(time.time()); old_age_days = max(0.0, (time.time() - old_created) / 86400)
            normal_upgrade = st['ev'] >= old_ev - .015 and st['pf'] >= old_pf * .97
            stale_rotation = old_age_days >= 45 and st['ev'] >= max(.075, old_ev * .80) and st['pf'] >= max(1.18, old_pf * .90) and recent_ev >= .08 and recent_pf >= 1.10
            improve = bool(normal_upgrade or stale_rotation)
        core_ok = st['pf'] >= 1.18 and st['ev'] >= .075 and stab >= .80 and b <= .255 and dd <= 16 and wf >= -.06 and prof >= .66 and len(selected) >= 60 and .04 <= freq <= 6 and bool(allowed) and recent_ev >= .02 and recent_pf >= 1.03
        promote = bool(core_ok and improve)
        reason = 'purged OOS guardrails + recent fold + safe Champion comparison passed' if promote else f"rejected: PF={st['pf']:.2f}, EV={st['ev']:.3f}R, recentEV={recent_ev:.3f}R, recentPF={recent_pf:.2f}, selected={len(selected)}, worstFold={wf:.3f}R, profitableFolds={prof:.0%}, stability={stab:.2f}, brier={b:.3f}, DD={dd:.1f}R, freq={freq:.2f}/day, safeImprove={improve}"
        final = self._model(999); final.fit(np.vstack([base._vec(r['features']) for r in rows]), np.array([r['success'] for r in rows]), sample_weight=base._weights(rows, int(rows[-1]['ts'])))
        meta = {'schema_version': 3, 'strategy': strategy, 'direction': direction, 'train_n': first - purge, 'test_n': len(ots), 'selected_n': len(selected), 'test_win': st['win'], 'profit_factor': st['pf'], 'expectancy_r': st['ev'], 'threshold': th, 'brier': b, 'logloss': ll, 'max_drawdown_r': dd, 'stability': stab, 'worst_fold_ev_r': wf, 'profitable_fold_ratio': prof, 'signals_per_day': freq, 'folds': fs, 'regime_metrics': rm, 'allowed_regimes': allowed, 'recent_fold_ev_r': recent_ev, 'recent_fold_pf': recent_pf, 'trained_through_ts': int(rows[-1]['ts']), 'old_oos_ev_r': old_ev, 'old_oos_pf': old_pf, 'old_age_days': old_age_days, 'comparison_method': 'stored_clean_oos_metrics_only; old final model is never self-scored on historical OOS', 'reason': reason}
        self.store.save_challenger(strategy, direction, final, meta, promote)
        return base.StrategyEvaluation(strategy, direction, first - purge, len(ots), len(selected), st['win'], st['pf'], st['ev'], th, b, dd, stab, promote, reason)


def install(core: Any) -> None:
    v5_runtime.Learner = EvolutionLearner
    core.Learner = EvolutionLearner
