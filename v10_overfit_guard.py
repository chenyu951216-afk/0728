from __future__ import annotations

import math
import random
from typing import Any

import adaptive_v5 as base
import v8_evolution as evo
import runtime_identity


VERSION=runtime_identity.RUNTIME_VERSION
CLUSTER_SECONDS=8*3600
MIN_EFFECTIVE_OOS=60
MIN_FOLDS=3
BOOTSTRAPS=400

_ORIGINAL_STATS=base._stats
_ORIGINAL_SAVE=base.ModelStore.save_challenger
_ORIGINAL_TRAIN=evo.GenomeEvolutionLearner.train_strategy_direction


def _cluster_rows(rows: list[dict[str,Any]])->list[dict[str,Any]]:
    if not rows or not all('ts' in x for x in rows):
        return list(rows)
    ordered=sorted(rows,key=lambda x:int(x['ts']))
    kept=[]; next_allowed=-10**18
    for row in ordered:
        ts=int(row['ts'])
        if ts>=next_allowed:
            kept.append(row); next_allowed=ts+CLUSTER_SECONDS
    return kept


def _bootstrap_ci05(pnls:list[float],seed:int)->float:
    if not pnls: return -9.0
    if len(pnls)<8: return min(pnls)
    rng=random.Random(seed); n=len(pnls); means=[]
    for _ in range(BOOTSTRAPS):
        means.append(sum(pnls[rng.randrange(n)] for __ in range(n))/n)
    means.sort(); return float(means[max(0,int(.05*len(means))-1)])


def clustered_stats(rows:list[dict[str,Any]],fee_r:float=.0)->dict[str,Any]:
    effective=_cluster_rows(rows)
    out=dict(_ORIGINAL_STATS(effective,fee_r))
    pnls=[base.f(x.get('pnl_r'))-fee_r for x in effective]
    seed=(len(effective)*1009 + (int(effective[0]['ts']) if effective else 0) + (int(effective[-1]['ts']) if effective else 0)) & 0xFFFFFFFF
    out['raw_n']=len(rows); out['effective_n']=len(effective); out['cluster_seconds']=CLUSTER_SECONDS
    out['ev_bootstrap_05']=_bootstrap_ci05(pnls,seed)
    return out


def guarded_save(self:Any,strategy:str,direction:str,model:Any,metrics:dict[str,Any],promote:bool):
    meta=dict(metrics); folds=list(meta.get('folds') or [])
    effective_n=sum(int(x.get('effective_n',x.get('n',0)) or 0) for x in folds)
    raw_n=int(meta.get('selected_n') or 0)
    if effective_n<=0 and folds:
        effective_n=sum(int(x.get('n') or 0) for x in folds)
    fold_weight=sum(max(1,int(x.get('effective_n',x.get('n',0)) or 0)) for x in folds)
    aggregate_ci05=(sum(max(1,int(x.get('effective_n',x.get('n',0)) or 0))*float(x.get('ev_bootstrap_05',-9)) for x in folds)/fold_weight) if folds and fold_weight else -9.0
    votes=dict(meta.get('genome_votes') or {}); max_votes=max(votes.values()) if votes else 0
    consensus=(max_votes/max(len(folds),1)) if folds else 0.0
    consensus_ok=bool(len(folds)<MIN_FOLDS or (max_votes>=2 and consensus>=.50))
    guard_ok=bool(len(folds)>=MIN_FOLDS and effective_n>=MIN_EFFECTIVE_OOS and aggregate_ci05>0.0 and consensus_ok)
    requested=bool(promote); final_promote=bool(requested and guard_ok)
    meta['raw_selected_n']=raw_n
    meta['selected_n']=effective_n
    meta['effective_oos_selected_n']=effective_n
    meta['signal_cluster_seconds']=CLUSTER_SECONDS
    meta['clustered_ev_bootstrap_05']=aggregate_ci05
    meta['genome_consensus']=consensus
    meta['overfit_guard_passed']=guard_ok
    meta['overfit_guard']='8h horizon clustering + >=3 chronological folds + effective OOS >=60 + clustered EV CI05>0 + genome consensus'
    if requested and not guard_ok:
        extra=f"overfit guard rejected: effectiveOOS={effective_n}, folds={len(folds)}, clusteredCI05={aggregate_ci05:+.3f}R, genomeConsensus={consensus:.0%}"
        meta['reason']=f"{meta.get('reason') or ''} | {extra}".strip(' |')
    self._v10_last_guard={'requested':requested,'promoted':final_promote,'effective_n':effective_n,'reason':meta.get('reason'),'ci05':aggregate_ci05,'consensus':consensus}
    return _ORIGINAL_SAVE(self,strategy,direction,model,meta,final_promote)


def guarded_train(self:Any,strategy:str,direction:str):
    self.store._v10_last_guard=None
    result=_ORIGINAL_TRAIN(self,strategy,direction)
    guard=getattr(self.store,'_v10_last_guard',None)
    if result is not None and guard:
        result.selected_n=int(guard.get('effective_n') or result.selected_n)
        if result.promoted and not guard.get('promoted'):
            result.promoted=False; result.reason=str(guard.get('reason') or 'final overfit guard rejected')
    return result


def install(core:Any)->None:
    base._stats=clustered_stats
    base.ModelStore.save_challenger=guarded_save
    evo.GenomeEvolutionLearner.train_strategy_direction=guarded_train
    # v5_runtime/core already reference this class object; changing its method is enough.
    strict=core.state.setdefault('strict_replay',{})
    strict['overfit_guard']={'enabled':True,'cluster_seconds':CLUSTER_SECONDS,'minimum_effective_oos':MIN_EFFECTIVE_OOS,
        'minimum_folds':MIN_FOLDS,'clustered_ev_bootstrap_ci05_must_be_positive':True,'genome_consensus_required':True,
        'overlapping_future_label_windows_cannot_count_as_independent_evidence':True}
