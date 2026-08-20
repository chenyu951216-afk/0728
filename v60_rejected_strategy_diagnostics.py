from __future__ import annotations

"""V60: read-only diagnostics for strategies rejected by final historical certification.

This overlay does not change research, walk-forward, OOS, execution, Champion promotion,
or current-paper semantics. It only exposes the already-persisted V52 strategy vault in a
human-readable form so failed finalists can be compared against the exact certification
thresholds that rejected them.
"""

import json
import math
import time
from collections import Counter
from typing import Any

import runtime_identity

VERSION = 'V60_REJECTED_STRATEGY_DIAGNOSTICS'
SCHEMA = 60
STATE_KEY = 'v60_rejected_strategy_diagnostics'
CACHE_SECONDS = 5.0
_INSTALLED = False
_CACHE: dict[str, Any] = {'at': 0.0, 'payload': None}


def _now() -> int:
    return int(time.time())


def _finite(value: Any) -> float | None:
    try:
        x = float(value)
        return x if math.isfinite(x) else None
    except (TypeError, ValueError):
        return None


def _json(value: Any, default: Any) -> Any:
    if value in (None, ''):
        return default
    try:
        out = json.loads(value) if isinstance(value, str) else value
        return out if isinstance(out, type(default)) else default
    except Exception:
        return default


def _rule(name: str, actual: Any, target: float, op: str, unit: str = '') -> dict[str, Any]:
    x = _finite(actual)
    passed = False
    if x is not None:
        if op == '>=':
            passed = x >= target
        elif op == '<=':
            passed = x <= target
        elif op == '>':
            passed = x > target
        elif op == '==':
            passed = x == target
    return {'name': name, 'actual': x, 'target': float(target), 'op': op,
            'unit': unit, 'passed': bool(passed), 'available': x is not None}


def _rules(metrics: dict[str, Any], autonomous: Any) -> list[dict[str, Any]]:
    return [
        _rule('OOS fills', metrics.get('oos_fills'), autonomous.MIN_OOS_FILLS, '>='),
        _rule('OOS PF', metrics.get('profit_factor'), autonomous.MIN_OOS_PF, '>='),
        _rule('OOS EV', metrics.get('expectancy_r'), autonomous.MIN_OOS_EV_R, '>=', 'R'),
        _rule('OOS DD', metrics.get('max_drawdown_r'), autonomous.MAX_OOS_DD_R, '<=', 'R'),
        _rule('Bootstrap CI05', metrics.get('bootstrap_ci05_r'), autonomous.MIN_BOOTSTRAP_CI05, '>', 'R'),
        _rule('Invalid future paths', metrics.get('invalid_future_paths'), 0.0, '=='),
        _rule('WF stability', metrics.get('stability'), autonomous.MIN_WF_STABILITY, '>='),
        _rule('Profitable folds', metrics.get('profitable_folds'), autonomous.MIN_PROFITABLE_FOLDS, '>='),
        _rule('Worst fold EV', metrics.get('worst_fold_ev'), autonomous.MIN_WORST_FOLD_EV, '>=', 'R'),
    ]


def _entry_summary(genome: dict[str, Any]) -> dict[str, Any]:
    targets = [float(x) for x in list(genome.get('target_rr') or []) if _finite(x) is not None]
    allocs = [float(x) for x in list(genome.get('allocations') or []) if _finite(x) is not None]
    return {
        'entry_type': 'MARKET' if bool(genome.get('entry_market')) else 'LIMIT',
        'entry_offset_atr': _finite(genome.get('entry_offset_atr')),
        'stop_atr': _finite(genome.get('stop_atr')),
        'target_rr': targets,
        'allocations': allocs,
        'max_hold_hours': (float(genome.get('max_hold_bars') or 0) * 0.25) if genome.get('max_hold_bars') is not None else None,
        'decision_stride': int(genome.get('decision_stride') or 0),
        'cooldown_bars': int(genome.get('cooldown_bars') or 0),
    }


def _latest_run(core: Any, vault_table: str) -> str:
    state = core.state.get('v49_stage6_atomic_orchestration')
    if isinstance(state, dict) and state.get('run_id'):
        return str(state['run_id'])
    con = core.db()
    try:
        row = con.execute(f'''SELECT run_id,MAX(updated_at) AS u FROM {vault_table}
                              GROUP BY run_id ORDER BY u DESC LIMIT 1''').fetchone()
        return str(row[0]) if row else ''
    finally:
        con.close()


def _build_payload(core: Any, autonomous: Any, pipeline52: Any) -> dict[str, Any]:
    pipeline52._ensure(core)
    run_id = _latest_run(core, pipeline52.VAULT_TABLE)
    con = core.db()
    try:
        if run_id:
            rows = con.execute(f'''SELECT rank,candidate_id,finalist_id,strategy_id,direction,status,
                                          selected_finalist,active_champion,genome,development,audit,updated_at
                                   FROM {pipeline52.VAULT_TABLE}
                                   WHERE run_id=? ORDER BY COALESCE(rank,999999),updated_at''', (run_id,)).fetchall()
        else:
            rows = []
    finally:
        con.close()

    finalists: list[dict[str, Any]] = []
    development_only: list[dict[str, Any]] = []
    reason_counts: Counter[str] = Counter()
    for row in rows:
        rank, candidate_id, finalist_id, strategy_id, direction, vault_status, selected, champion, genome_raw, dev_raw, audit_raw, updated_at = row
        genome = _json(genome_raw, {})
        development = _json(dev_raw, {})
        audit = _json(audit_raw, {})
        audit_metrics = dict(audit.get('metrics') or {}) if isinstance(audit, dict) else {}
        audit_status = str(audit.get('status') or vault_status or 'UNKNOWN') if isinstance(audit, dict) else str(vault_status or 'UNKNOWN')
        promoted = bool(audit.get('promoted')) if isinstance(audit, dict) else bool(champion)
        reason = str(audit_metrics.get('reason') or (audit.get('reason') if isinstance(audit, dict) else '') or audit_status)

        if not bool(selected):
            development_only.append({
                'candidate_id': str(candidate_id or ''), 'direction': str(direction or genome.get('direction') or 'UNKNOWN'),
                'development_score': _finite(development.get('development_score') or development.get('score')),
                'development_ev_r': _finite(development.get('ev')),
                'development_pf': _finite(development.get('pf')),
                'stability': _finite(development.get('stability')),
                'profitable_folds': _finite(development.get('profitable_folds')),
                'worst_fold_ev_r': _finite(development.get('worst_fold_ev')),
                'development_fills': int(development.get('development_fills') or 0),
                'execution': _entry_summary(genome),
            })
            continue

        rules = _rules(audit_metrics, autonomous)
        failed = [r for r in rules if r['available'] and not r['passed']]
        passed = [r for r in rules if r['available'] and r['passed']]
        unavailable = [r for r in rules if not r['available']]
        if promoted or bool(champion):
            outcome = 'CHAMPION'
        elif audit_raw:
            outcome = 'REJECTED'
        else:
            outcome = 'PENDING_AUDIT'
        if outcome == 'REJECTED':
            if failed:
                for r in failed:
                    reason_counts[r['name']] += 1
            else:
                reason_counts[audit_status] += 1

        gate_thresholds = list(audit.get('gate_thresholds') or []) if isinstance(audit, dict) else []
        finalists.append({
            'rank': int(rank or 0), 'candidate_id': str(candidate_id or ''), 'finalist_id': str(finalist_id or ''),
            'strategy_id': str(strategy_id or ''), 'direction': str(direction or genome.get('direction') or 'UNKNOWN'),
            'outcome': outcome, 'audit_status': audit_status, 'rejection_reason': reason,
            'updated_at': int(updated_at or 0), 'gate_thresholds': gate_thresholds,
            'oos': {
                'fills': int(audit_metrics.get('oos_fills') or 0),
                'pf': _finite(audit_metrics.get('profit_factor')),
                'ev_r': _finite(audit_metrics.get('expectancy_r')),
                'win_rate': _finite(audit_metrics.get('test_win')),
                'dd_r': _finite(audit_metrics.get('max_drawdown_r')),
                'total_r': _finite(audit_metrics.get('total_oos_r')),
                'ci05_r': _finite(audit_metrics.get('bootstrap_ci05_r')),
                'invalid_paths': int(audit_metrics.get('invalid_future_paths') or 0),
            },
            'development': {
                'score': _finite(development.get('development_score') or development.get('score')),
                'ev_r': _finite(audit_metrics.get('development_ev') if audit_metrics else development.get('ev')),
                'pf': _finite(audit_metrics.get('development_pf') if audit_metrics else development.get('pf')),
                'stability': _finite(audit_metrics.get('stability') if audit_metrics else development.get('stability')),
                'profitable_folds': _finite(audit_metrics.get('profitable_folds') if audit_metrics else development.get('profitable_folds')),
                'worst_fold_ev_r': _finite(audit_metrics.get('worst_fold_ev') if audit_metrics else development.get('worst_fold_ev')),
                'fills': int(development.get('development_fills') or 0),
                'folds': len(development.get('folds') or []),
            },
            'execution': _entry_summary(genome),
            'rules': rules,
            'passed_gate_count': len(passed), 'available_gate_count': len(passed) + len(failed),
            'failed_gate_count': len(failed), 'unavailable_gate_count': len(unavailable),
            'failed_gates': [r['name'] for r in failed],
        })

    rejected = [x for x in finalists if x['outcome'] == 'REJECTED']
    rejected.sort(key=lambda x: (-int(x['passed_gate_count']), int(x['failed_gate_count']),
                                 -float(x['oos']['ev_r'] if x['oos']['ev_r'] is not None else -999),
                                 -float(x['oos']['pf'] if x['oos']['pf'] is not None else -999), x['rank']))
    champions = [x for x in finalists if x['outcome'] == 'CHAMPION']
    pending = [x for x in finalists if x['outcome'] == 'PENDING_AUDIT']
    development_only.sort(key=lambda x: float(x['development_score'] if x['development_score'] is not None else -999), reverse=True)

    thresholds = {
        'min_oos_fills': int(autonomous.MIN_OOS_FILLS), 'min_oos_pf': float(autonomous.MIN_OOS_PF),
        'min_oos_ev_r': float(autonomous.MIN_OOS_EV_R), 'max_oos_dd_r': float(autonomous.MAX_OOS_DD_R),
        'min_bootstrap_ci05_r_exclusive': float(autonomous.MIN_BOOTSTRAP_CI05),
        'invalid_future_paths_required': 0, 'min_wf_stability': float(autonomous.MIN_WF_STABILITY),
        'min_profitable_folds': float(autonomous.MIN_PROFITABLE_FOLDS),
        'min_worst_fold_ev_r': float(autonomous.MIN_WORST_FOLD_EV),
    }
    return {
        'schema': SCHEMA, 'runtime': VERSION, 'public_runtime': runtime_identity.RUNTIME_VERSION,
        'run_id': run_id, 'updated_at': _now(), 'read_only': True,
        'research_semantics_changed': False, 'oos_thresholds_changed': False,
        'thresholds': thresholds,
        'summary': {
            'vault_rows': len(rows), 'finalists': len(finalists), 'rejected': len(rejected),
            'champions': len(champions), 'pending_audit': len(pending),
            'development_eligible_not_finalist': len(development_only),
            'rejection_gate_counts': dict(reason_counts),
        },
        'rejected': rejected,
        'champions': champions,
        'pending': pending,
        'development_best_not_finalist': development_only[:20],
    }


def rejected_status(core: Any, autonomous: Any, pipeline52: Any) -> dict[str, Any]:
    now = time.monotonic()
    if _CACHE.get('payload') is not None and now - float(_CACHE.get('at') or 0.0) < CACHE_SECONDS:
        return dict(_CACHE['payload'])
    payload = _build_payload(core, autonomous, pipeline52)
    _CACHE['at'] = now
    _CACHE['payload'] = payload
    return dict(payload)


def _inject(html: str) -> str:
    if 'v60-rejected-strategies-card' in html:
        return html
    card = '''<section class="card" id="v60-rejected-strategies-card"><h2>🧪 被淘汰策略 / Final OOS 診斷</h2>
<div id="v60rejSummary" class="notice y">讀取被淘汰策略資料…</div>
<div id="v60rejReasons" class="small" style="margin:10px 0"></div>
<div id="v60rejRows"></div>
<details><summary>研究期合格、但未進 Finalist 的候選</summary><div id="v60devRows" class="small">讀取中…</div></details></section>'''
    script = r'''<script id="v60-rejected-strategies-js">(function(){
const esc=x=>String(x??'—').replace(/[&<>"']/g,s=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[s]));
const n=(x,d=3)=>Number.isFinite(Number(x))?Number(x).toFixed(d):'—';
const pct=x=>Number.isFinite(Number(x))?(Number(x)*100).toFixed(1)+'%':'—';
async function loadV60(){const sum=document.getElementById('v60rejSummary');if(!sum)return;const c=new AbortController(),t=setTimeout(()=>c.abort(),3500);try{const r=await fetch('/api/v60/rejected-strategies',{cache:'no-store',signal:c.signal});if(!r.ok)throw new Error('HTTP '+r.status);const z=await r.json(),s=z.summary||{};sum.className='notice '+(Number(s.rejected||0)>0?'y':'g');sum.innerHTML=`Finalists <b>${Number(s.finalists||0)}</b>｜淘汰 <b>${Number(s.rejected||0)}</b>｜Champion <b>${Number(s.champions||0)}</b>｜未完成 audit <b>${Number(s.pending_audit||0)}</b><br>下方依「通過認證條件數」排序，最接近過關的放最前面。`;
const rc=s.rejection_gate_counts||{},reason=document.getElementById('v60rejReasons');if(reason)reason.innerHTML=Object.entries(rc).sort((a,b)=>b[1]-a[1]).map(([k,v])=>`<span style="margin-right:12px">${esc(k)} <b>${v}</b></span>`).join('')||'目前沒有可統計的淘汰原因。';
const box=document.getElementById('v60rejRows'),rows=z.rejected||[];if(box)box.innerHTML=rows.length?rows.slice(0,28).map(x=>{const o=x.oos||{},d=x.development||{},e=x.execution||{},rules=x.rules||[];const fail=rules.filter(q=>q.available&&!q.passed).map(q=>`${esc(q.name)} ${q.actual===null?'—':n(q.actual,q.name.includes('fills')||q.name.includes('paths')?0:3)} ${esc(q.op)} ${n(q.target,q.name.includes('fills')||q.name.includes('paths')?0:3)}`).join('；')||esc(x.audit_status);return `<div class="notice y" style="margin:10px 0"><b>#${x.rank} ${esc(x.direction)} · ${esc(x.audit_status)}</b><br>OOS：PF <b>${n(o.pf,2)}</b>｜EV <b>${n(o.ev_r,3)}R</b>｜勝率 <b>${pct(o.win_rate)}</b>｜fills <b>${o.fills??0}</b>｜DD <b>${n(o.dd_r,2)}R</b>｜CI05 <b>${n(o.ci05_r,3)}R</b><br>WF：stability <b>${n(d.stability,3)}</b>｜正 EV folds <b>${pct(d.profitable_folds)}</b>｜worst fold <b>${n(d.worst_fold_ev_r,3)}R</b>｜通過 <b>${x.passed_gate_count}/${x.available_gate_count}</b><br>執行：${esc(e.entry_type)}｜SL ${n(e.stop_atr,3)} ATR｜hold≤${n(e.max_hold_hours,1)}h｜TP ${esc((e.target_rr||[]).map(v=>n(v,2)+'R').join(' / ')||'—')}<br><span style="color:#ffd56a">淘汰：${fail}</span></div>`}).join(''):'<div class="notice g">目前沒有已完成 Final OOS 的淘汰策略。</div>';
const db=document.getElementById('v60devRows'),dev=z.development_best_not_finalist||[];if(db)db.innerHTML=dev.length?dev.slice(0,20).map((x,i)=>`<div style="padding:7px 0;border-bottom:1px solid #25466d">#${i+1} ${esc(x.direction)}｜dev score ${n(x.development_score,3)}｜EV ${n(x.development_ev_r,3)}R｜PF ${n(x.development_pf,2)}｜stability ${n(x.stability,3)}｜worst ${n(x.worst_fold_ev_r,3)}R</div>`).join(''):'—';
}catch(e){sum.className='notice r';sum.textContent='淘汰策略診斷暫時不可用：'+String(e)}finally{clearTimeout(t)}}
loadV60();setInterval(()=>{if(!document.hidden)loadV60()},12000);
})();</script>'''
    if '</main>' in html:
        html = html.replace('</main>', card + '</main>', 1)
    elif '</body>' in html:
        html = html.replace('</body>', card + '</body>', 1)
    else:
        html += card
    return html.replace('</body>', script + '</body>', 1) if '</body>' in html else html + script


def _wrap_html_route(core: Any, path: str, name: str) -> None:
    route = next((r for r in core.app.router.routes if getattr(r, 'path', None) == path), None)
    old = getattr(route, 'endpoint', None)
    if not callable(old):
        return
    from fastapi.responses import HTMLResponse
    core.app.router.routes = [r for r in core.app.router.routes if getattr(r, 'path', None) != path]

    def endpoint() -> HTMLResponse:
        raw = old()
        html = raw.body.decode('utf-8', errors='replace') if hasattr(raw, 'body') else str(raw)
        return HTMLResponse(_inject(html), headers={'Cache-Control': 'no-store, max-age=0',
                                                   'X-ETH-Adaptive-Diagnostics': VERSION})
    core.app.add_api_route(path, endpoint, methods=['GET'], response_class=HTMLResponse, name=name)


def install(production: Any, autonomous: Any, pipeline52: Any) -> None:
    global _INSTALLED
    if _INSTALLED:
        return
    _INSTALLED = True
    core = production.core
    pipeline52._ensure(core)

    if not any(getattr(r, 'path', None) == '/api/v60/rejected-strategies' for r in core.app.router.routes):
        @core.app.get('/api/v60/rejected-strategies')
        def rejected_api() -> dict[str, Any]:
            return rejected_status(core, autonomous, pipeline52)

    if not any(getattr(r, 'path', None) == '/api/v60/runtime' for r in core.app.router.routes):
        @core.app.get('/api/v60/runtime')
        def runtime_api() -> dict[str, Any]:
            return {'schema': SCHEMA, 'runtime': VERSION, 'read_only': True,
                    'research_semantics_changed': False, 'oos_thresholds_changed': False,
                    'replay_reset': False, 'historical_data_deleted': False,
                    'future_peeking_enabled': False, 'updated_at': _now()}

    _wrap_html_route(core, '/', 'v60_fast_dashboard')
    _wrap_html_route(core, '/dashboard/full', 'v60_full_dashboard')
    core.state[STATE_KEY] = {'schema': SCHEMA, 'runtime': VERSION, 'status': 'READY',
                             'read_only': True, 'updated_at': _now()}
