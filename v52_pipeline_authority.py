from __future__ import annotations

"""Truthful Stage 1-9 progress, durable strategy vault, and current-paper handoff."""

import json
import time
from typing import Any

import runtime_identity
import v16_runtime_integrity as runtime_integrity
import v22_hierarchical_pipeline as pipeline
import v52_execution_authority as execution52

VERSION = 'V52_STAGE1_9_PIPELINE_AUTHORITY'
SCHEMA = 52
STATE_KEY = 'v52_stage1_9_pipeline_authority'
VAULT_TABLE = 'autonomous_strategy_vault_v52'
MIGRATION_KEY = 'v52_stage1_9_migration_20260818'
_INSTALLED = False
_BASE_STATUS = None
_BASE_PIPELINE = None


def _now() -> int:
    return int(time.time())


def _jd(v: Any) -> Any:
    if hasattr(v, 'item'):
        return v.item()
    raise TypeError(type(v).__name__)


def _state(core: Any, **patch: Any) -> dict[str, Any]:
    raw = core.state.get(STATE_KEY)
    out = dict(raw) if isinstance(raw, dict) else {}
    out.update(patch)
    out.update({'schema': SCHEMA, 'runtime': VERSION,
                'public_runtime': runtime_identity.RUNTIME_VERSION,
                'updated_at': _now()})
    core.state[STATE_KEY] = out
    return out


def _ensure(core: Any) -> None:
    con = core.db()
    try:
        con.execute(f'''CREATE TABLE IF NOT EXISTS {VAULT_TABLE}(
            run_id TEXT NOT NULL, genome_hash TEXT NOT NULL, candidate_id TEXT,
            finalist_id TEXT, strategy_id TEXT, created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL, rank INTEGER, direction TEXT NOT NULL,
            status TEXT NOT NULL, selected_finalist INTEGER NOT NULL DEFAULT 0,
            active_champion INTEGER NOT NULL DEFAULT 0, genome TEXT NOT NULL,
            development TEXT, audit TEXT, model BLOB,
            PRIMARY KEY(run_id,genome_hash))''')
        con.execute(f'CREATE INDEX IF NOT EXISTS ix_{VAULT_TABLE}_status ON {VAULT_TABLE}(run_id,status,rank)')
        con.execute(f'CREATE INDEX IF NOT EXISTS ix_{VAULT_TABLE}_finalist ON {VAULT_TABLE}(finalist_id)')
        con.commit()
    finally:
        con.close()


def _run(core: Any, throughput: Any) -> str:
    run = str(getattr(throughput, '_RUN_ID', '') or '')
    if run:
        return run
    o = core.state.get('v49_stage6_atomic_orchestration')
    return str(o.get('run_id') or '') if isinstance(o, dict) else ''


def _counts(core: Any, run: str = '') -> dict[str, int]:
    _ensure(core)
    con = core.db()
    try:
        where, args = (' WHERE run_id=?', (run,)) if run else ('', ())
        r = con.execute(f'''SELECT COUNT(*),SUM(selected_finalist=1),
            SUM(audit IS NOT NULL),SUM(active_champion=1) FROM {VAULT_TABLE}{where}''', args).fetchone()
    finally:
        con.close()
    r = r or (0,0,0,0)
    return {'saved': int(r[0] or 0), 'finalists': int(r[1] or 0),
            'audited': int(r[2] or 0), 'champions': int(r[3] or 0)}


def _save_candidate(core: Any, a: Any, throughput: Any,
                    genome: dict[str, Any], result: dict[str, Any]) -> None:
    if not result.get('eligible_for_finalist'):
        return
    run = _run(core, throughput) or 'RUN_PENDING'
    gh = a._hash_payload(genome, 20)
    active = dict(core.state.get('autonomous_live_progress') or {})
    cid = str(active.get('candidate_id') or a._hash_payload(genome, 18))
    now = _now(); _ensure(core)
    con = core.db()
    try:
        con.execute(f'''INSERT INTO {VAULT_TABLE}(
            run_id,genome_hash,candidate_id,created_at,updated_at,direction,status,genome,development)
            VALUES(?,?,?,?,?,?,?,?,?)
            ON CONFLICT(run_id,genome_hash) DO UPDATE SET
            candidate_id=excluded.candidate_id,updated_at=excluded.updated_at,
            direction=excluded.direction,genome=excluded.genome,development=excluded.development,
            status=CASE WHEN {VAULT_TABLE}.selected_finalist=1 THEN {VAULT_TABLE}.status ELSE excluded.status END''',
            (run,gh,cid,now,now,str(genome.get('direction') or 'UNKNOWN'),
             'DEVELOPMENT_ELIGIBLE_SAVED',json.dumps(genome,separators=(',',':'),default=_jd),
             json.dumps(result,separators=(',',':'),default=_jd)))
        con.commit()
    finally:
        con.close()
    _state(core, strategy_vault=_counts(core, run), strategy_vault_run=run,
           last_saved_candidate=cid)


def _freeze_finalists(core: Any, a: Any, throughput: Any,
                      finalists: list[tuple[float,dict[str,Any],dict[str,Any]]]) -> None:
    run = _run(core, throughput) or 'RUN_PENDING'; now = _now(); _ensure(core)
    con = core.db()
    try:
        con.execute('BEGIN IMMEDIATE')
        for rank,(score,g,dev) in enumerate(finalists,1):
            gh = a._hash_payload(g,20); fid = a._hash_payload({'genome':g,'dev':dev},20)
            d = dict(dev); d['development_score'] = float(score)
            con.execute(f'''INSERT INTO {VAULT_TABLE}(
                run_id,genome_hash,finalist_id,created_at,updated_at,rank,direction,status,
                selected_finalist,genome,development) VALUES(?,?,?,?,?,?,?,?,1,?,?)
                ON CONFLICT(run_id,genome_hash) DO UPDATE SET
                finalist_id=excluded.finalist_id,updated_at=excluded.updated_at,rank=excluded.rank,
                direction=excluded.direction,status='FINALIST_FROZEN_BEFORE_OOS',selected_finalist=1,
                genome=excluded.genome,development=excluded.development''',
                (run,gh,fid,now,now,rank,str(g.get('direction') or 'UNKNOWN'),
                 'FINALIST_FROZEN_BEFORE_OOS',json.dumps(g,separators=(',',':'),default=_jd),
                 json.dumps(d,separators=(',',':'),default=_jd)))
        con.commit()
    except Exception:
        con.rollback(); raise
    finally:
        con.close()
    _state(core, strategy_vault=_counts(core,run), strategy_vault_run=run,
           finalist_freeze_complete=True, finalist_freeze_count=len(finalists),
           oos_may_open_only_after_finalist_freeze=True)


def _attach_audit(core: Any, a: Any, fid: str, genome: dict[str,Any], result: dict[str,Any]) -> None:
    gh = a._hash_payload(genome,20); _ensure(core); now=_now()
    con=core.db()
    try:
        row=con.execute(f'SELECT run_id FROM {VAULT_TABLE} WHERE genome_hash=? ORDER BY updated_at DESC LIMIT 1',(gh,)).fetchone()
        run=str(row[0]) if row else 'RUN_PENDING'
        con.execute(f'''INSERT INTO {VAULT_TABLE}(
            run_id,genome_hash,finalist_id,created_at,updated_at,direction,status,selected_finalist,genome,audit,model)
            VALUES(?,?,?,?,?,?,?,1,?,?,?)
            ON CONFLICT(run_id,genome_hash) DO UPDATE SET finalist_id=excluded.finalist_id,
            updated_at=excluded.updated_at,status='OOS_AUDITED',selected_finalist=1,
            audit=excluded.audit,model=COALESCE(excluded.model,{VAULT_TABLE}.model)''',
            (run,gh,str(fid),now,now,str(genome.get('direction') or 'UNKNOWN'),'OOS_AUDITED',
             json.dumps(genome,separators=(',',':'),default=_jd),
             json.dumps(result,separators=(',',':'),default=_jd),result.get('model_blob')))
        con.commit()
    finally:
        con.close()
    _state(core,strategy_vault=_counts(core,run),last_audited_finalist=str(fid))


def _attach_champion(core: Any, a: Any, result: dict[str,Any], saved: dict[str,Any]) -> None:
    g=dict(result.get('genome') or {}); gh=a._hash_payload(g,20); sid=str(saved.get('strategy_id') or '')
    _ensure(core); now=_now(); con=core.db()
    try:
        row=con.execute(f'SELECT run_id FROM {VAULT_TABLE} WHERE genome_hash=? ORDER BY updated_at DESC LIMIT 1',(gh,)).fetchone()
        run=str(row[0]) if row else 'RUN_PENDING'
        con.execute(f'''UPDATE {VAULT_TABLE} SET strategy_id=?,updated_at=?,status='CHAMPION_SAVED',
            active_champion=1,model=COALESCE(?,model) WHERE run_id=? AND genome_hash=?''',
            (sid,now,result.get('model_blob'),run,gh)); con.commit()
    finally:
        con.close()
    h=dict(core.state.get('v52_current_paper_handoff') or {}); ids=list(h.get('strategy_ids') or [])
    if sid and sid not in ids: ids.append(sid)
    h.update({'ready':bool(ids),'mode':'CERTIFIED_CURRENT_PAPER','strategy_ids':ids,
              'paper_only':True,'historical_replay_complete':True,'updated_at':now})
    core.state['v52_current_paper_handoff']=h
    core.state.setdefault('learning',{})['phase']='CURRENT_PAPER_MONITORING' if ids else 'AUTONOMOUS_RESEARCH_COMPLETE'
    _state(core,strategy_vault=_counts(core,run),current_paper_handoff=h)


def _migrate(core: Any, a: Any) -> None:
    if core.get_state(MIGRATION_KEY,None): return
    champions=a._load_registry(core,active_only=True); cp=core.get_state(a.CHECKPOINT_KEY,{})
    cp=dict(cp) if isinstance(cp,dict) else {}; cleared=False
    if not champions and cp.get('status')=='COMPLETE':
        core.set_state(a.CHECKPOINT_KEY,{}); core.set_state('v49_stage6_outer_cursor',{})
        core.state['autonomous_live_progress']={}; cleared=True
    core.set_state(MIGRATION_KEY,{'at':_now(),'stale_terminal_checkpoint_cleared':cleared,
        'raw_market_preserved':True,'learning_samples_preserved':True,'replay_cursor_preserved':True,
        'candidate_archive_preserved':True,'champion_registry_preserved':True})
    _state(core,stale_terminal_checkpoint_cleared=cleared)


def _install_persistence(core: Any, a: Any, throughput: Any) -> None:
    base_eval=a._evaluate_candidate
    def eval_saved(s,m,g,seed):
        r=base_eval(s,m,g,seed)
        if isinstance(r,dict): _save_candidate(core,a,throughput,g,r)
        return r
    a._evaluate_candidate=eval_saved
    base_evo=a._evolution
    def evo_saved(c,s,m):
        finalists=list(base_evo(c,s,m) or []); _freeze_finalists(c,a,throughput,finalists); return finalists
    a._evolution=evo_saved
    base_audit=a._save_audit
    def audit_saved(c,fid,g,r):
        base_audit(c,fid,g,r); _attach_audit(c,a,fid,g,r)
    a._save_audit=audit_saved
    base_champ=a._save_champion
    def champ_saved(c,rank,r):
        saved=dict(base_champ(c,rank,r) or {}); _attach_champion(c,a,r,saved); return saved
    a._save_champion=champ_saved


def _run_progress(core: Any, a: Any, throughput: Any) -> dict[str,Any]:
    active=dict(core.state.get('autonomous_live_progress') or {})
    orch=dict(core.state.get('v49_stage6_atomic_orchestration') or {})
    run=str(orch.get('run_id') or getattr(throughput,'_RUN_ID','') or '')
    counts=dict(orch.get('checkpoint_counts') or {})
    if not counts and run:
        try: counts=dict(throughput._counts(core,run) or {})
        except Exception: counts={}
    pop=int(active.get('population') or a.POPULATION); gens=int(active.get('generations') or a.GENERATIONS)
    total=max(1,pop*gens); gen=int(active.get('generation') or 0); cand=int(active.get('candidate') or 0)
    cursor=max(0,gen-1)*pop+max(0,cand-(0 if str(active.get('outer_status'))=='COMMITTED' else 1))
    done=min(total,max(int(counts.get('persisted') or 0),cursor))
    if str(active.get('stage'))=='ONE_TIME_COMPLETE_PACKAGE_OOS': done=total
    err=orch.get('error') or orch.get('future_error') or (core.state.get(STATE_KEY) or {}).get('error')
    return {'run_id':run,'active':active,'checkpoint_counts':counts,'total_candidates':total,
            'completed_candidates':done,'evolution_percent':round(100*done/total,2),'error':err}


def _install_status(core: Any, a: Any, throughput: Any) -> None:
    global _BASE_STATUS,_BASE_PIPELINE
    _BASE_STATUS=a.autonomous_status; _BASE_PIPELINE=pipeline.pipeline_status
    def status(c: Any) -> dict[str,Any]:
        z=dict(_BASE_STATUS(c) or {}); rp=_run_progress(c,a,throughput); active=rp['active']
        cp=c.get_state(a.CHECKPOINT_KEY,{}); cp=dict(cp) if isinstance(cp,dict) else {}
        run=str(rp.get('run_id') or ''); vault=_counts(c,run) if run else _counts(c)
        allreg=a._load_registry(c,active_only=False)
        champs=[x for x in allreg if str(x.get('status'))=='CHAMPION' and bool(x.get('active'))]
        stage=str(active.get('stage') or ''); running=stage in ('DIRECT_R_AUTONOMOUS_EVOLUTION','ONE_TIME_COMPLETE_PACKAGE_OOS')
        evo=100.0 if stage=='ONE_TIME_COMPLETE_PACKAGE_OOS' else float(rp['evolution_percent'])
        terminal=bool(cp.get('status')=='COMPLETE' and not running and not rp.get('error'))
        finalists=int(vault.get('finalists') or cp.get('finalists') or 0); audited=int(vault.get('audited') or 0)
        oos=100.0*min(audited,finalists)/finalists if finalists>0 else 0.0
        if rp.get('error'): current='STAGE6_ERROR'
        elif stage=='DIRECT_R_AUTONOMOUS_EVOLUTION': current='AUTONOMOUS_EVOLUTION_RUNNING'
        elif stage=='ONE_TIME_COMPLETE_PACKAGE_OOS': current='AUTONOMOUS_OOS_RUNNING'
        elif terminal and champs: current='COMPLETE'
        elif terminal: current='COMPLETE_NO_CERTIFIED_PACKAGE'
        else: current=str(z.get('status') or 'AUTONOMOUS_RESEARCH_QUEUED')
        z.update({'status':current,'active':active,'research_complete':terminal,'live_ready':bool(terminal and champs),
            'leverage_mode':execution52.LEVERAGE_MODE,'strategy_vault':vault,'current_run':rp,
            'champions':[{'strategy_id':x['strategy_id'],'direction':x['direction'],'behavior_label':x['behavior_label'],**dict(x.get('metrics') or {})} for x in champs],
            'v52':dict(c.state.get(STATE_KEY) or {}),
            'progress':{'evolution_percent':round(evo,2),'oos_percent':round(oos,2),'audited':audited,
                'finalists':finalists,'candidates_evaluated':rp['completed_candidates'],'total_candidates':rp['total_candidates'],
                'evolution_state':'ERROR' if rp.get('error') else 'COMPLETE' if evo>=100 else 'RUNNING',
                'oos_state':'COMPLETE' if finalists>0 and audited>=finalists else 'RUNNING' if finalists>0 and evo>=100 else 'WAITING_FOR_FINALISTS'}})
        return z
    a.autonomous_status=status

    def pstatus(c: Any) -> dict[str,Any]:
        original=getattr(a,'_ORIGINAL_PIPELINE_STATUS',None)
        base=dict(original(c) or {}) if callable(original) else dict(_BASE_PIPELINE(c) or {})
        replay=dict(runtime_integrity.replay_progress(c) or {}); auto=status(c); p=auto['progress']
        evo=float(p['evolution_percent']); oos=float(p['oos_percent']); finalists=int(p['finalists']); audited=int(p['audited']); champs=len(auto['champions']); err=auto['current_run'].get('error')
        stages=list(base.get('stages') or [])[:5]
        def st(name,pct,s,e,b=None):
            try: return pipeline._stage(name,pct,s,e,b)
            except Exception: return {'name':name,'percent':pct,'status':s,'evidence':e,'blocker':b}
        s6='ERROR' if err else 'COMPLETE' if evo>=100 else 'RUNNING' if replay.get('complete') else 'WAITING'
        s7='WAITING' if evo<100 else 'WAITING_NO_FINALIST' if finalists<=0 else 'COMPLETE' if audited>=finalists else 'RUNNING'
        s8='CERTIFIED' if s7=='COMPLETE' and champs else 'COMPLETE_NO_CERTIFIED_PACKAGE' if s7=='COMPLETE' else 'WAITING'
        h=dict(c.state.get('v52_current_paper_handoff') or {}); s9='CURRENT_PAPER_RUNNING' if champs and h.get('ready') else 'WAITING'
        stages.extend([
            st('6. AUTONOMOUS_DIRECT_R_STRATEGY_DISCOVERY',evo,s6,{'current_run':auto['current_run'],'strategy_vault':auto['strategy_vault']},str(err) if err else None),
            st('7. COMPLETE_PACKAGE_CHRONOLOGICAL_OOS',oos,s7,{'finalists_frozen_before_oos':finalists,'audited':audited},None if finalists else 'waiting for saved development-eligible finalist'),
            st('8. AUTONOMOUS_PACKAGE_CERTIFICATION',100.0 if s7=='COMPLETE' else oos,s8,{'champions':champs,'final_oos_thresholds_relaxed':False}),
            st('9. CURRENT_LIVE_HANDOFF',100.0 if s9=='CURRENT_PAPER_RUNNING' else 0.0,s9,{'paper_only':True,'strategy_ids':h.get('strategy_ids') or [],'leverage_mode':execution52.LEVERAGE_MODE},None if s9=='CURRENT_PAPER_RUNNING' else 'requires persisted OOS-certified Champion')])
        weights=[8,8,8,8,23,25,12,5,3]; overall=sum(float(x.get('percent') or 0)*weights[i] for i,x in enumerate(stages))/sum(weights[:len(stages)])
        base.update({'stages':stages,'overall_percent':round(overall,2),'active_stage':next((x['name'] for x in stages if str(x.get('status')) not in ('COMPLETE','CERTIFIED','CURRENT_PAPER_RUNNING')),stages[-1]['name']),'operational':s9=='CURRENT_PAPER_RUNNING','autonomous_strategy_discovery':auto,'stage1_9_authority':VERSION,'joint_signal_then_execution_separation':False})
        c.state['hierarchical_pipeline']=base; return base
    pipeline.pipeline_status=pstatus


def _routes(core: Any,a: Any,throughput: Any) -> None:
    app=core.app
    if not any(getattr(r,'path',None)=='/api/v52/pipeline' for r in app.router.routes):
        @app.get('/api/v52/pipeline')
        def api(): return {'schema':SCHEMA,'runtime':VERSION,'state':dict(core.state.get(STATE_KEY) or {}),'autonomous':a.autonomous_status(core),'pipeline':pipeline.pipeline_status(core),'handoff':dict(core.state.get('v52_current_paper_handoff') or {}),'rules':{'raw_history_deleted':False,'replay_reset':False,'future_price_features':False,'final_oos_relaxed':False,'strategy_saved_before_oos':True,'failed_oos_can_be_champion':False}}
    if not any(getattr(r,'path',None)=='/api/v52/strategy-vault' for r in app.router.routes):
        @app.get('/api/v52/strategy-vault')
        def vault_api():
            run=_run(core,throughput); con=core.db()
            try: rows=con.execute(f'SELECT genome_hash,candidate_id,finalist_id,strategy_id,rank,direction,status,selected_finalist,active_champion,development,audit FROM {VAULT_TABLE} WHERE run_id=? ORDER BY COALESCE(rank,999999),updated_at',(run,)).fetchall()
            finally: con.close()
            out=[]
            for r in rows:
                try: dev=json.loads(r[9]) if r[9] else {}
                except Exception: dev={}
                try: aud=json.loads(r[10]) if r[10] else {}
                except Exception: aud={}
                out.append({'genome_hash':r[0],'candidate_id':r[1],'finalist_id':r[2],'strategy_id':r[3],'rank':r[4],'direction':r[5],'status':r[6],'selected_finalist':bool(r[7]),'active_champion':bool(r[8]),'development_score':dev.get('development_score',dev.get('score')),'oos_status':aud.get('status')})
            return {'schema':SCHEMA,'runtime':VERSION,'run_id':run,'counts':_counts(core,run),'strategies':out}


def install(production: Any,a: Any,throughput: Any,integrity: Any,orchestration: Any) -> None:
    global _INSTALLED
    if _INSTALLED: return
    _INSTALLED=True; core=production.core
    mods=tuple(getattr(integrity,'SEMANTIC_MODULES',()))
    if 'v52_pipeline_authority' not in mods: integrity.SEMANTIC_MODULES=mods+('v52_pipeline_authority',)
    _ensure(core); _migrate(core,a); _install_persistence(core,a,throughput); _install_status(core,a,throughput); _routes(core,a,throughput)
    orchestration.mark_startup_barrier(core,False,'V52 Stage 1-9 authority installed; server entry opens barrier after full stack readiness')
    core.state.setdefault('strict_replay',{})['v52_stage1_9_pipeline']={'schema':SCHEMA,'raw_history_deleted':False,'replay_reset':False,'features_reduced':False,'population_reduced':False,'generations_reduced':False,'final_oos_thresholds_relaxed':False,'future_peeking_enabled':False,'strategy_saved_before_oos':True,'old_terminal_checkpoint_can_claim_new_run_complete':False,'current_paper_requires_persisted_oos_champion':True}
    _state(core,installed=True,status='READY_BEHIND_V52_STARTUP_BARRIER',strategy_vault=_counts(core),raw_replay_preserved=True)
    runtime_identity.stamp(core)
