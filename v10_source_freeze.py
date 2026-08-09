from __future__ import annotations

import math
import statistics
import time
from typing import Any

import v9_final
import v9_readiness
import v10_final_integrity as fin


VERSION = fin.VERSION
OI_KEYS = ('gate_stats','bybit_oi','cg_oi')
FUNDING_KEYS = ('funding_bybit','funding_binance')
OPTIONAL_KEYS = ('cg_liq','cg_book')


def _complete(core: Any, key: str) -> bool:
    rec = fin._src(core,key)
    return bool(not rec.get('disabled') and rec.get('last_success_at') and int(rec.get('processed_through') or 0) >= int(time.time())-fin.READY_SAFETY_SECONDS)


def _all_disabled(core: Any, keys: tuple[str,...]) -> bool:
    recs=[fin._src(core,k) for k in keys]
    return bool(recs and all(r and r.get('disabled') for r in recs))


def _freeze_if_ready(core: Any) -> None:
    state=fin._load(core)
    if state.get('core_frozen'):
        return
    oi=[k for k in OI_KEYS if _complete(core,k)]
    funding=[k for k in FUNDING_KEYS if _complete(core,k)]
    oi_settled=bool(oi) or _all_disabled(core,OI_KEYS)
    funding_settled=bool(funding) or _all_disabled(core,FUNDING_KEYS)
    if not (oi_settled and funding_settled):
        return
    state['core_frozen']=True
    state['frozen_core_oi']=oi
    state['frozen_core_funding']=funding
    state['frozen_enrichment']=[k for k in ('gate_stats','cg_liq','cg_book','cg_oi') if _complete(core,k)]
    state['frozen_at']=int(time.time())
    state['freeze_reason']='at least one complete OI source and one complete funding source; optional enrichment only if already complete'
    fin._save(core,state)
    core.set_state('final_frozen_core_oi',oi); core.set_state('final_frozen_core_funding',funding)
    core.set_state('final_frozen_enrichment',state['frozen_enrichment'])


def _upgrade_if_all_settled(core: Any) -> None:
    state=fin._load(core)
    if not state.get('core_frozen') or state.get('all_sources_upgrade_applied'):
        return
    tracked=OI_KEYS+FUNDING_KEYS+OPTIONAL_KEYS
    if not all(_complete(core,k) or fin._src(core,k).get('disabled') for k in tracked):
        return
    target_oi=[k for k in OI_KEYS if _complete(core,k)]
    target_funding=[k for k in FUNDING_KEYS if _complete(core,k)]
    target_enrichment=[k for k in ('gate_stats','cg_oi','cg_liq','cg_book') if _complete(core,k)]
    current=(set(state.get('frozen_core_oi') or []),set(state.get('frozen_core_funding') or []),set(state.get('frozen_enrichment') or []))
    target=(set(target_oi),set(target_funding),set(target_enrichment))
    if target==current:
        state['all_sources_upgrade_applied']=True; fin._save(core,state); return
    con=core.db(); samples=int(con.execute('SELECT COUNT(*) FROM learning_samples').fetchone()[0]); con.close()
    if samples>0:
        fin._reset_labels_only(core,'all derivative providers settled; rebuilding one source-consistent richer generation')
    state=fin._load(core)
    state.update({'core_frozen':True,'frozen_core_oi':target_oi,'frozen_core_funding':target_funding,
                  'frozen_enrichment':target_enrichment,'all_sources_upgrade_applied':True,
                  'generation':int(state.get('generation') or 1)+1,'upgraded_at':int(time.time())})
    fin._save(core,state)
    core.set_state('final_data_generation',int(state['generation']))
    core.set_state('final_frozen_core_oi',target_oi); core.set_state('final_frozen_core_funding',target_funding)
    core.set_state('final_frozen_enrichment',target_enrichment)


def core_ready_through(core: Any) -> int|None:
    state=fin._load(core)
    if not state.get('core_frozen'):
        return int(core.START_TS)
    keys=list(state.get('frozen_core_oi') or [])+list(state.get('frozen_core_funding') or [])
    if not keys:
        return None
    vals=[int(fin._src(core,k).get('processed_through') or core.START_TS) for k in keys]
    # All frozen sources are part of the feature definition, so the slowest frozen
    # source is the safe watermark. New/unfrozen providers never influence this generation.
    return min(vals) if vals else None


def _allowed(core: Any,key: str,ts: int,group: str) -> bool:
    state=fin._load(core)
    frozen=set(state.get('frozen_core_oi') or [])|set(state.get('frozen_core_funding') or [])|set(state.get('frozen_enrichment') or [])
    return bool(key in frozen and fin._coverage_allows(core,key,ts))


def strict_extras(core: Any,history: Any,decision_ts: int)->dict[str,float]:
    lagged=max(0,int(decision_ts)-int(v9_final.DERIVATIVE_SAFETY_LAG_SECONDS))
    oi,oiq=[],[]
    for metric in ('oi_usd','oi_coin'):
        for source,rows in fin._series(history,metric,lagged,24*3600,6).items():
            key='gate_stats' if source=='gate' else 'bybit_oi' if source=='bybit' else 'cg_oi' if source=='coinglass' else None
            if not key or not _allowed(core,key,lagged,'oi') or len(rows)<2: continue
            newest,oldest=float(rows[0]['value']),float(rows[-1]['value'])
            if oldest and math.isfinite(newest) and math.isfinite(oldest): oi.append(newest/oldest-1.0); oiq.append(float(rows[0]['quality']))
    funding,fq=[],[]
    for source,rows in fin._series(history,'funding',int(decision_ts),20*3600,6).items():
        key='funding_bybit' if source=='bybit' else 'funding_binance' if source=='binance' else None
        if key and _allowed(core,key,int(decision_ts),'funding') and rows: funding.append(float(rows[0]['value'])); fq.append(float(rows[0]['quality']))
    longs=fin._series(history,'liq_long_usd',lagged,12*3600,3); shorts=fin._series(history,'liq_short_usd',lagged,12*3600,3)
    liq,totals,lq=[],[],[]
    for source in set(longs)&set(shorts):
        key='gate_stats' if source=='gate' else 'cg_liq' if source=='coinglass' else None
        if not key or not _allowed(core,key,lagged,'liquidation'): continue
        lv=max(0.0,float(longs[source][0]['value'])); sv=max(0.0,float(shorts[source][0]['value'])); total=lv+sv
        if total>0: liq.append((sv-lv)/total); totals.append(total); lq.append(min(float(longs[source][0]['quality']),float(shorts[source][0]['quality'])))
    book,bq=[],[]
    if _allowed(core,'cg_book',lagged,'book'):
        for source,rows in fin._series(history,'book_imbalance',lagged,12*3600,3).items():
            if source=='coinglass' and rows: book.append(float(rows[0]['value'])); bq.append(float(rows[0]['quality']))
    avail=(bool(oi),bool(funding),bool(liq),bool(book)); q=oiq+fq+lq+bq
    return {'oi_change':statistics.median(oi) if oi else 0.0,'funding':statistics.median(funding) if funding else 0.0,
            'liquidation_imbalance':statistics.median(liq) if liq else 0.0,'liquidation_intensity':math.log1p(statistics.median(totals))/25.0 if totals else 0.0,
            'book_imbalance':statistics.median(book) if book else 0.0,'oi_available':float(bool(oi)),'funding_available':float(bool(funding)),
            'liquidation_available':float(bool(liq)),'book_available':float(bool(book)),'derivative_coverage':sum(avail)/4.0,
            'derivative_quality':statistics.mean(q)/100.0 if q else 0.0,'historical_derivative_safety_lag_seconds':float(v9_final.DERIVATIVE_SAFETY_LAG_SECONDS)}


def install(core: Any)->None:
    # v10_source_freeze is the sole owner of replay-generation/source-set changes.
    # The earlier helper inside v10_final_integrity must not independently reset a
    # generation after samples start, otherwise two managers can race each other.
    fin._maybe_freeze_enrichment=lambda c: None
    original_backfill=core.derivative_history.backfill_tick
    async def backfill(hub:Any,start_ts:int,pages:int=4):
        result=await original_backfill(hub,start_ts,pages)
        _freeze_if_ready(core); _upgrade_if_all_settled(core)
        result=dict(result or {}); state=fin._load(core)
        result.update({'core_frozen':bool(state.get('core_frozen')),'frozen_core_oi':state.get('frozen_core_oi',[]),
                       'frozen_core_funding':state.get('frozen_core_funding',[]),'frozen_enrichment':state.get('frozen_enrichment',[]),
                       'core_ready_through':core_ready_through(core),'generation':state.get('generation',1)})
        core.state['derivative_multisource']=result
        return result
    core.derivative_history.backfill_tick=backfill
    fin.core_ready_through=lambda c: core_ready_through(c)
    fin.strict_derivative_extras=lambda c,h,ts: strict_extras(c,h,ts)
    v9_readiness._coinglass_ready_through=lambda c: core_ready_through(c)
    v9_final._strict_derivative_extras=lambda h,ts: strict_extras(core,h,ts)
    strict=core.state.setdefault('strict_replay',{}); strict['source_freeze']={'enabled':True,'core_sources_frozen_before_replay':True,
        'mid_generation_provider_join_forbidden':True,'late_provider_upgrade_rebuilds_labels_not_raw_data':True,'single_generation_manager':True}
