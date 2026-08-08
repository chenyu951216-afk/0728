from __future__ import annotations

import json
import math
import pickle
import sqlite3
import statistics
import time
from dataclasses import dataclass
from typing import Any

import numpy as np
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.metrics import brier_score_loss, log_loss

import adaptive_engine as base

f, clamp, mean = base.f, base.clamp, base.mean
atr, ema, pivots = base.atr, base.ema, base.pivots
build_features, detect_regime = base.build_features, base.detect_regime
FEATURE_NAMES, REGIMES = base.FEATURE_NAMES, base.REGIMES
DIRECTIONS = ('LONG', 'SHORT')
STRATEGIES = (
    'TREND_PULLBACK', 'LIQUIDITY_SWEEP_REVERSAL', 'SQUEEZE_EXPANSION',
    'BREAKOUT_RETEST', 'RANGE_MEAN_REVERSION', 'MOMENTUM_CONTINUATION',
    'FAILED_BREAKOUT_REVERSAL'
)
MODEL_SCHEMA_VERSION = 2


def strategy_affinity(regime: str, phase: str) -> dict[str, float]:
    x = {s: .20 for s in STRATEGIES}
    if regime in ('BULL_MARKUP','BEAR_MARKDOWN','BULL_PULLBACK','BEAR_RALLY'):
        x['TREND_PULLBACK']=1.; x['BREAKOUT_RETEST']=.72; x['MOMENTUM_CONTINUATION']=.82
    if regime in ('SQUEEZE','EXPANSION_UP','EXPANSION_DOWN') or phase == 'COMPRESSION':
        x['SQUEEZE_EXPANSION']=1.; x['BREAKOUT_RETEST']=max(x['BREAKOUT_RETEST'],.82); x['MOMENTUM_CONTINUATION']=max(x['MOMENTUM_CONTINUATION'],.86)
    if regime in ('RANGE_LOW_VOL','RANGE_HIGH_VOL'):
        x['RANGE_MEAN_REVERSION']=1.; x['LIQUIDITY_SWEEP_REVERSAL']=.90; x['FAILED_BREAKOUT_REVERSAL']=.88
    if regime in ('CAPITULATION','REBOUND','TRANSITION'):
        x['LIQUIDITY_SWEEP_REVERSAL']=.88; x['FAILED_BREAKOUT_REVERSAL']=.92
    return x


def baseline_direction_scores(features: dict[str,float], regime: dict[str,Any]) -> dict[str,dict[str,float]]:
    a=strategy_affinity(regime['regime'],regime['phase']); h4=int(regime.get('h4_direction',0))
    tl=clamp(.36+.13*features['ema20_gap']+.10*features['ema20_slope']+.08*max(features['volume_z'],0)+.08*features['sweep_low']+(.12 if h4>0 else -.06 if h4<0 else 0),0,1)
    ts=clamp(.36-.13*features['ema20_gap']-.10*features['ema20_slope']+.08*max(features['volume_z'],0)+.08*features['sweep_high']+(.12 if h4<0 else -.06 if h4>0 else 0),0,1)
    sl=clamp(.30+.28*features['sweep_low']+.13*max(.50-features['rsi'],0)+.07*max(-features['book_imbalance'],0)*features['book_available']+.07*max(-features['liquidation_imbalance'],0)*features['liquidation_available'],0,1)
    ss=clamp(.30+.28*features['sweep_high']+.13*max(features['rsi']-.50,0)+.07*max(features['book_imbalance'],0)*features['book_available']+.07*max(features['liquidation_imbalance'],0)*features['liquidation_available'],0,1)
    sq=clamp(.28+.22*(1-features['atr_rank'])+.14*max(features['volume_z'],0)+.16*max(features['range_z'],0),0,1); bias=clamp(features['ret_4']*18+features['ema20_slope']*.12,-.18,.18)
    bl=clamp(.24+.31*features['bos_up']+.12*max(features['volume_z'],0)+.08*max(features['oi_change'],0)*features['oi_available']+.04*features['derivative_coverage'],0,1)
    bs=clamp(.24+.31*features['bos_down']+.12*max(features['volume_z'],0)+.08*max(features['oi_change'],0)*features['oi_available']+.04*features['derivative_coverage'],0,1)
    ml=clamp(.28+.15*max(-features['dist_vwap_atr'],0)+.16*max(.46-features['rsi'],0)+.12*features['sweep_low'],0,1); ms=clamp(.28+.15*max(features['dist_vwap_atr'],0)+.16*max(features['rsi']-.54,0)+.12*features['sweep_high'],0,1)
    cl=clamp(.27+.11*max(features['ema20_slope'],0)+.16*features['bos_up']+.10*max(features['volume_z'],0)+.08*max(features['range_z'],0)+.05*max(features['oi_change'],0)*features['oi_available'],0,1)
    cs=clamp(.27+.11*max(-features['ema20_slope'],0)+.16*features['bos_down']+.10*max(features['volume_z'],0)+.08*max(features['range_z'],0)+.05*max(features['oi_change'],0)*features['oi_available'],0,1)
    fl=clamp(.26+.22*features['sweep_low']+.12*features['bos_down']*max(.48-features['rsi'],0)+.10*features['wick_ratio'],0,1); fs=clamp(.26+.22*features['sweep_high']+.12*features['bos_up']*max(features['rsi']-.52,0)+.10*features['wick_ratio'],0,1)
    raw={'TREND_PULLBACK':{'LONG':tl,'SHORT':ts},'LIQUIDITY_SWEEP_REVERSAL':{'LONG':sl,'SHORT':ss},'SQUEEZE_EXPANSION':{'LONG':sq+bias,'SHORT':sq-bias},'BREAKOUT_RETEST':{'LONG':bl,'SHORT':bs},'RANGE_MEAN_REVERSION':{'LONG':ml,'SHORT':ms},'MOMENTUM_CONTINUATION':{'LONG':cl,'SHORT':cs},'FAILED_BREAKOUT_REVERSAL':{'LONG':fl,'SHORT':fs}}
    return {st:{d:clamp(v*a[st],0,1) for d,v in dirs.items()} for st,dirs in raw.items()}


def _vec(features:dict[str,float])->np.ndarray: return np.array([f(features.get(n)) for n in FEATURE_NAMES],dtype=np.float64)

def _stats(rows:list[dict[str,Any]],fee:float)->dict[str,float]:
    p=[f(r['pnl_r'])-fee for r in rows]; g=sum(max(x,0) for x in p); l=sum(max(-x,0) for x in p)
    return {'n':len(p),'pf':g/max(l,1e-9),'ev':mean(p),'win':mean(1. if x>0 else 0. for x in p)}

def _dd(pnls:list[float])->float:
    eq=peak=dd=0.
    for p in pnls: eq+=p; peak=max(peak,eq); dd=max(dd,peak-eq)
    return dd

def _weights(rows:list[dict[str,Any]],asof:int|None=None)->np.ndarray:
    asof=asof or max(int(r['ts']) for r in rows); counts={}
    for r in rows: counts[r['regime']]=counts.get(r['regime'],0)+1
    med=statistics.median(counts.values()) if counts else 1.; hl=2*365.25*86400; out=[]
    for r in rows:
        rec=.35+.65*math.exp(-math.log(2)*max(0,asof-int(r['ts']))/hl); bal=clamp(math.sqrt(med/max(counts[r['regime']],1)),.70,1.45); src=clamp(f(r.get('source_quality'),75)/100,.55,1.)
        out.append(rec*bal*src)
    return np.array(out,dtype=float)

def _threshold(rows:list[dict[str,Any]],probs:np.ndarray,fee:float)->tuple[float,dict[str,float]]:
    best=(.60,{},-999.)
    span=max(1.,(int(rows[-1]['ts'])-int(rows[0]['ts']))/86400) if len(rows)>1 else 1.
    for t in np.arange(.52,.721,.01):
        chosen=[r for r,p in zip(rows,probs) if float(p)>=t]
        if len(chosen)<28: continue
        st=_stats(chosen,fee); dd=_dd([f(r['pnl_r'])-fee for r in chosen]); freq=len(chosen)/span
        u=st['ev']*3.2+math.log(max(st['pf'],1e-6))*.28-dd*.004+min(len(chosen),180)/180*.10
        if freq<.04:u-=.12
        if freq>4:u-=min(.30,(freq-4)*.035)
        if u>best[2]: best=(round(float(t),2),{**st,'dd':dd,'signals_per_day':freq,'utility':u},u)
    return best[0],best[1]


class ModelStore:
    def __init__(self,con:sqlite3.Connection)->None:
        self.con=con; con.execute("""CREATE TABLE IF NOT EXISTS model_registry(strategy TEXT NOT NULL,version INTEGER NOT NULL,status TEXT NOT NULL,created_at INTEGER NOT NULL,metrics TEXT NOT NULL,model BLOB NOT NULL,direction TEXT NOT NULL DEFAULT 'BOTH',PRIMARY KEY(strategy,version))""")
        cols={str(x[1]) for x in con.execute('PRAGMA table_info(model_registry)').fetchall()}
        if 'direction' not in cols: con.execute("ALTER TABLE model_registry ADD COLUMN direction TEXT NOT NULL DEFAULT 'BOTH'")
        con.execute("""CREATE TABLE IF NOT EXISTS learning_samples(ts INTEGER NOT NULL,strategy TEXT NOT NULL,direction TEXT NOT NULL,regime TEXT NOT NULL,phase TEXT NOT NULL,features TEXT NOT NULL,success INTEGER NOT NULL,pnl_r REAL NOT NULL,mfe_r REAL NOT NULL,mae_r REAL NOT NULL,source_quality REAL NOT NULL,PRIMARY KEY(ts,strategy,direction))""")
        con.execute('CREATE INDEX IF NOT EXISTS ix_learning_samples_strategy_direction_ts ON learning_samples(strategy,direction,ts)'); con.commit()
    def champion(self,strategy:str,direction:str|None=None):
        row=self.con.execute("SELECT model,metrics,version,direction FROM model_registry WHERE strategy=? AND direction=? AND status='CHAMPION' ORDER BY version DESC LIMIT 1",(strategy,direction)).fetchone() if direction else self.con.execute("SELECT model,metrics,version,direction FROM model_registry WHERE strategy=? AND status='CHAMPION' ORDER BY version DESC LIMIT 1",(strategy,)).fetchone()
        return (None,{}) if not row else (pickle.loads(row[0]),{**json.loads(row[1]),'version':row[2],'direction':row[3]})
    def save_challenger(self,strategy,direction,model,metrics,promote):
        v=int((self.con.execute('SELECT MAX(version) FROM model_registry WHERE strategy=?',(strategy,)).fetchone()[0] or 0))+1
        if promote:self.con.execute("UPDATE model_registry SET status='ARCHIVED' WHERE strategy=? AND direction=? AND status='CHAMPION'",(strategy,direction))
        self.con.execute('INSERT INTO model_registry(strategy,version,status,created_at,metrics,model,direction) VALUES(?,?,?,?,?,?,?)',(strategy,v,'CHAMPION' if promote else 'REJECTED',int(time.time()),json.dumps(metrics,ensure_ascii=False),sqlite3.Binary(pickle.dumps(model,pickle.HIGHEST_PROTOCOL)),direction));self.con.commit();return v
    def add_sample(self,r): self.con.execute('INSERT OR IGNORE INTO learning_samples(ts,strategy,direction,regime,phase,features,success,pnl_r,mfe_r,mae_r,source_quality) VALUES(?,?,?,?,?,?,?,?,?,?,?)',(r['ts'],r['strategy'],r['direction'],r['regime'],r['phase'],json.dumps(r['features'],separators=(',',':')),int(r['success']),r['pnl_r'],r['mfe_r'],r['mae_r'],r.get('source_quality',100.)))
    def commit(self): self.con.commit()
    def samples(self,strategy,limit=30000,direction=None):
        q='SELECT ts,direction,regime,phase,features,success,pnl_r,mfe_r,mae_r,source_quality FROM learning_samples WHERE strategy=?'+(' AND direction=?' if direction else '')+' ORDER BY ts DESC LIMIT ?'; args=(strategy,direction,limit) if direction else (strategy,limit); rows=self.con.execute(q,args).fetchall()
        return [{'ts':x[0],'direction':x[1],'regime':x[2],'phase':x[3],'features':json.loads(x[4]),'success':x[5],'pnl_r':x[6],'mfe_r':x[7],'mae_r':x[8],'source_quality':x[9]} for x in reversed(rows)]


@dataclass
class StrategyEvaluation:
    strategy:str; direction:str; train_n:int; test_n:int; selected_n:int; test_win:float; profit_factor:float; expectancy_r:float; threshold:float; brier:float; max_drawdown_r:float; stability:float; promoted:bool; reason:str


class Learner:
    def __init__(self,store:ModelStore,fee_r=.035): self.store=store;self.fee_r=fee_r
    outcome=staticmethod(base.Learner.outcome)
    @staticmethod
    def strategy_outcome(cs,i,strategy,direction,horizon=24):
        past=cs[:i+1]; close=f(cs[i]['c']); a=max(atr(past),close*.001); e20=ema([f(x['c']) for x in past],20); sign=1 if direction=='LONG' else -1; w=past[-28:-1] if len(past)>=29 else past[:-1]; ph=max((f(x['h']) for x in w),default=close); pl=min((f(x['l']) for x in w),default=close)
        if strategy=='TREND_PULLBACK': entry=min(close-.10*a,e20) if direction=='LONG' else max(close+.10*a,e20);wait=6
        elif strategy=='BREAKOUT_RETEST': entry=ph if direction=='LONG' and ph<close else pl if direction=='SHORT' and pl>close else close-sign*.06*a;wait=8
        elif strategy=='RANGE_MEAN_REVERSION': entry=min(close-.06*a,e20) if direction=='LONG' else max(close+.06*a,e20);wait=6
        elif strategy=='SQUEEZE_EXPANSION':entry=close-sign*.05*a;wait=5
        elif strategy=='MOMENTUM_CONTINUATION':entry=close-sign*.035*a;wait=4
        elif strategy=='FAILED_BREAKOUT_REVERSAL':entry=close-sign*.075*a;wait=5
        else:entry=close-sign*.08*a;wait=6
        risk=1.2*a;stop=entry-sign*risk;target=entry+sign*1.25*risk;future=cs[i+1:i+1+horizon];fill=next((j for j,b in enumerate(future[:wait]) if f(b['l'])<=entry<=f(b['h'])),None)
        if fill is None:return 0,0.,0.,0.
        mfe=mae=0.
        for b in future[fill:]:
            fav=(f(b['h'])-entry)*sign/risk if sign>0 else (entry-f(b['l']))/risk;adv=(entry-f(b['l']))/risk if sign>0 else (f(b['h'])-entry)/risk;mfe=max(mfe,fav);mae=max(mae,adv);sh=f(b['l'])<=stop if direction=='LONG' else f(b['h'])>=stop;th=f(b['h'])>=target if direction=='LONG' else f(b['l'])<=target
            if sh:return 0,-1.,mfe,mae
            if th:return 1,1.25,mfe,mae
        last=f(future[-1]['c']) if future else entry;p=clamp((last-entry)*sign/risk,-1.,1.25);return int(p>.15),p,mfe,mae
    @staticmethod
    def _model(seed):return HistGradientBoostingClassifier(learning_rate=.04,max_iter=220,max_leaf_nodes=15,min_samples_leaf=30,l2_regularization=1.6,random_state=seed)
    def predict(self,strategy,direction,features,baseline):
        m,meta=self.store.champion(strategy,direction)
        if m is None:return baseline,{'mode':'BASELINE_UNCERTIFIED','model_version':None,'direction':direction}
        p=float(m.predict_proba(_vec(features).reshape(1,-1))[0][1]);return clamp(.92*p+.08*baseline,.01,.99),{'mode':'LEARNED_CHAMPION','model_version':meta.get('version'),'direction':direction,'metrics':meta}
    def train_strategy_direction(self,strategy,direction,min_train=300,min_test=120):
        rows=[x for x in self.store.samples(strategy,direction=direction) if x['source_quality']>=55]
        if len(rows)<min_train+min_test+80:return None
        purge=32;n=len(rows);first=max(min_train+purge,int(n*.50));remain=n-first;folds=4 if remain>=4*min_test else 3 if remain>=3*min_test else 2
        fs=[];ots=[];ops=[];selected=[];ths=[]
        for fold in range(folds):
            ts=first+fold*max(min_test,remain//folds);te=n if fold==folds-1 else min(n,ts+max(min_test,remain//folds));train=rows[:max(0,ts-purge)];test=rows[ts:te]
            if len(train)<min_train or len(test)<60:continue
            cn=max(80,int(len(train)*.2));fe=len(train)-cn-purge
            if fe<220:continue
            fit=train[:fe];cal=train[fe+purge:];yf=np.array([r['success'] for r in fit]);yc=np.array([r['success'] for r in cal]);yt=np.array([r['success'] for r in test])
            if min(len(set(yf)),len(set(yc)),len(set(yt)))<2:continue
            mi=self._model(100+fold);mi.fit(np.vstack([_vec(r['features']) for r in fit]),yf,sample_weight=_weights(fit,int(fit[-1]['ts'])));cp=mi.predict_proba(np.vstack([_vec(r['features']) for r in cal]))[:,1];th,tmeta=_threshold(cal,cp,self.fee_r)
            mo=self._model(200+fold);yo=np.array([r['success'] for r in train]);mo.fit(np.vstack([_vec(r['features']) for r in train]),yo,sample_weight=_weights(train,int(train[-1]['ts'])));pr=mo.predict_proba(np.vstack([_vec(r['features']) for r in test]))[:,1];ch=[r for r,p in zip(test,pr) if p>=th];st=_stats(ch,self.fee_r) if ch else {'n':0,'pf':0.,'ev':-1.,'win':0.};fs.append({**st,'threshold':th,'dd':_dd([f(r['pnl_r'])-self.fee_r for r in ch]),'threshold_calibration':tmeta});ths.append(th);ots+=test;ops+=list(map(float,pr));selected+=ch
        if len(fs)<2 or len(ots)<min_test or len(selected)<60:return None
        st=_stats(selected,self.fee_r);pn=[f(r['pnl_r'])-self.fee_r for r in selected];b=brier_score_loss(np.array([r['success'] for r in ots]),np.array(ops));ll=log_loss(np.array([r['success'] for r in ots]),np.array(ops),labels=[0,1]);evs=[x['ev'] for x in fs];wins=[x['win'] for x in fs];stab=clamp(1-.65*(statistics.pstdev(evs) if len(evs)>1 else 0)-.55*(statistics.pstdev(wins) if len(wins)>1 else 0),0,1);wf=min(evs);prof=sum(x>0 for x in evs)/len(evs);dd=_dd(pn);th=round(statistics.median(ths),2);span=max(1.,(int(ots[-1]['ts'])-int(ots[0]['ts']))/86400);freq=len(selected)/span
        rm={}
        for rg in REGIMES:
            z=[r for r in selected if r['regime']==rg]
            if z:rm[rg]=_stats(z,self.fee_r)
        allowed=[rg for rg,z in rm.items() if z['n']>=18 and z['ev']>=.02 and z['pf']>=1.04] or [rg for rg,z in rm.items() if z['n']>=30 and z['ev']>0]
        old,om=self.store.champion(strategy,direction);improve=True;oe=opf=None
        if old is not None:
            try:
                op=old.predict_proba(np.vstack([_vec(r['features']) for r in ots]))[:,1];oz=[r for r,p in zip(ots,op) if p>=f(om.get('threshold'),.60)];os=_stats(oz,self.fee_r) if oz else {'ev':-9.,'pf':0.};oe,opf=os['ev'],os['pf'];improve=st['ev']>=oe-.01 and st['pf']>=opf*.98
            except Exception:improve=False
        core=st['pf']>=1.18 and st['ev']>=.075 and stab>=.80 and b<=.255 and dd<=16 and wf>=-.06 and prof>=.66 and len(selected)>=60 and .04<=freq<=6 and bool(allowed);promote=core and improve;reason='nested purged OOS guardrails passed' if promote else f"rejected: PF={st['pf']:.2f}, EV={st['ev']:.3f}R, selected={len(selected)}, worstFold={wf:.3f}R, profitableFolds={prof:.0%}, stability={stab:.2f}, brier={b:.3f}, DD={dd:.1f}R, freq={freq:.2f}/day"
        final=self._model(999);final.fit(np.vstack([_vec(r['features']) for r in rows]),np.array([r['success'] for r in rows]),sample_weight=_weights(rows,int(rows[-1]['ts'])));meta={'schema_version':2,'strategy':strategy,'direction':direction,'train_n':first-purge,'test_n':len(ots),'selected_n':len(selected),'test_win':st['win'],'profit_factor':st['pf'],'expectancy_r':st['ev'],'threshold':th,'brier':b,'logloss':ll,'max_drawdown_r':dd,'stability':stab,'worst_fold_ev_r':wf,'profitable_fold_ratio':prof,'signals_per_day':freq,'folds':fs,'regime_metrics':rm,'allowed_regimes':allowed,'old_oos_ev_r':oe,'old_oos_pf':opf,'reason':reason};self.store.save_challenger(strategy,direction,final,meta,promote)
        return StrategyEvaluation(strategy,direction,first-purge,len(ots),len(selected),st['win'],st['pf'],st['ev'],th,b,dd,stab,promote,reason)
    def train_all(self):
        return [x for s in STRATEGIES for d in DIRECTIONS if (x:=self.train_strategy_direction(s,d))]


def learned_risk_profile(store,strategy,regime,direction):
    rows=[x for x in store.samples(strategy,12000,direction) if x['regime']==regime];w=[x for x in rows if x['pnl_r']>0]
    if len(rows)<80 or len(w)<35:return {'stop_r':1.,'tp1_r':.75,'tp2_r':1.25,'tp3_r':1.9,'runner_r':2.8,'entry_pullback_r':.08,'sample_n':len(rows),'mode':'ROBUST_PRIOR'}
    ma=sorted(clamp(f(x['mae_r']),.02,3) for x in w);mf=sorted(clamp(f(x['mfe_r']),.05,6) for x in w);q=lambda xs,p:xs[min(len(xs)-1,max(0,int((len(xs)-1)*p)))];return {'stop_r':clamp(q(ma,.88)+.08,.72,1.45),'tp1_r':clamp(q(mf,.35),.6,1.),'tp2_r':clamp(q(mf,.55),.95,1.65),'tp3_r':clamp(q(mf,.72),1.35,2.5),'runner_r':clamp(q(mf,.86),1.9,4.2),'entry_pullback_r':clamp(q(ma,.35)*.45,.03,.18),'sample_n':len(rows),'mode':'LEARNED_MAE_MFE'}

def adaptive_entry(store,strategy,regime,direction,live,m15):
    p=learned_risk_profile(store,strategy,regime,direction);a=max(atr(m15),live*.001);e20=ema([f(x['c']) for x in m15],20);sg=1 if direction=='LONG' else -1;off=clamp(f(p.get('entry_pullback_r'),.08)*1.2,.04,.22)*a
    if strategy in ('TREND_PULLBACK','RANGE_MEAN_REVERSION'):v=min(live-off,e20) if direction=='LONG' else max(live+off,e20)
    elif strategy=='BREAKOUT_RETEST':w=m15[-28:-1];v=max(f(x['h']) for x in w) if direction=='LONG' else min(f(x['l']) for x in w)
    elif strategy in ('SQUEEZE_EXPANSION','MOMENTUM_CONTINUATION'):v=live-sg*max(.05*a,off*.65)
    else:v=live-sg*max(.04*a,off*.8)
    return round(min(v,live-.025*a),2) if direction=='LONG' else round(max(v,live+.025*a),2)

def risk_plan(store,strategy,regime,direction,entry,m15):
    p=learned_risk_profile(store,strategy,regime,direction);a=max(atr(m15),entry*.001);hi,lo=pivots(m15[-100:],2)
    if direction=='LONG':c=[x for _,x in lo if x<entry];st=min((max(c) if c else entry-p['stop_r']*a)-.1*a,entry-.65*p['stop_r']*a)
    else:c=[x for _,x in hi if x>entry];st=max((min(c) if c else entry+p['stop_r']*a)+.1*a,entry+.65*p['stop_r']*a)
    risk=abs(entry-st);sg=1 if direction=='LONG' else -1;lev=[p['tp1_r'],p['tp2_r'],p['tp3_r'],p['runner_r']];alloc=[25,30,25,20];targets=[{'price':round(entry+sg*risk*rr,2),'rr':round(rr,2),'allocation':al} for rr,al in zip(lev,alloc)]
    return {'entry':round(entry,2),'stop':round(st,2),'risk':round(risk,4),'targets':targets,'profile':p,'management':{'move_to_be_after_tp1':True,'trail_after_tp2':True,'never_widen_stop':True,'initial_plan_immutable':True}}

def choose_strategy(store,learner,features,regime,data_quality):
    pri=baseline_direction_scores(features,regime);aff=strategy_affinity(regime['regime'],regime['phase']);qp=clamp(data_quality/100,.55,1);cand=[]
    for s,dirs in pri.items():
        for d,b in dirs.items():
            p,m=learner.predict(s,d,features,b);meta=m.get('metrics') or {};th=f(meta.get('threshold'),.60);cert=m.get('mode')=='LEARNED_CHAMPION';rg=cert and regime['regime'] in (meta.get('allowed_regimes') or []);edge=p-th;score=p*aff[s]*qp+clamp(edge,-.2,.2)*.25;trade=bool(cert and rg and data_quality>=70 and p>=th and score>=.42);cand.append({'strategy':s,'direction':d,'baseline':b,'probability':p,'score':score,'model':m,'certified':cert,'threshold':th,'edge':edge,'regime_ok':rg,'tradeable':trade})
    cand.sort(key=lambda x:x['score'],reverse=True);research=cand[0];trade=[x for x in cand if x['tradeable']];cert=[x for x in cand if x['certified']];sel=trade[0] if trade else cert[0] if cert else research;reason='best certified profitable OOS candidate passed its learned threshold' if sel.get('tradeable') else 'certified models exist but none pass current regime/threshold gate' if cert else 'no direction-specific Champion is certified yet';return {**sel,'tradeable':bool(sel.get('tradeable')),'certified':bool(sel.get('certified')),'research_best':research,'tradeable_candidates':trade[:5],'certified_candidates':cert[:8],'candidates':cand,'reason':reason}
