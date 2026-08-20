from __future__ import annotations

"""V61 risk-adjusted provisional Current Paper authority.

This layer exists for a narrow case: a frozen finalist can be statistically strong on
Final OOS and Walk-Forward yet fail only the absolute R drawdown gate.  Because any
exception is defined after the one-time OOS has already been observed, V61 MUST NOT
rewrite that historical verdict to "strictly certified".  Instead it may admit only a
high-conviction, sole-DD-failure package to PAPER-ONLY forward validation.

The frozen genome, gate thresholds and calibration threshold are reused unchanged.  A
fresh model is refit from the already-fixed historical span only to make the frozen
package executable in current time.  Historical OOS metrics are never recomputed or
rewritten.  Forward-only evidence can quarantine or confirm the provisional package.
"""

import json
import math
import os
import pickle
import random
import threading
import time
from typing import Any

import numpy as np

import runtime_identity

VERSION = 'V61_RISK_ADJUSTED_PROVISIONAL_PAPER'
SCHEMA = 61
STATE_KEY = 'v61_risk_adjusted_provisional_paper'
TIER = 'PROVISIONAL_RISK_ADJUSTED_PAPER'

ENABLED = str(os.getenv('AUTONOMOUS_V61_PROVISIONAL_ENABLED', 'true')).strip().lower() not in {'0', 'false', 'no', 'off'}
MAX_PROVISIONALS = max(1, min(4, int(os.getenv('AUTONOMOUS_V61_MAX_PROVISIONALS', '2'))))
MIN_FILLS = max(100, int(os.getenv('AUTONOMOUS_V61_MIN_FILLS', '300')))
MIN_PF = max(1.30, float(os.getenv('AUTONOMOUS_V61_MIN_PF', '1.50')))
MIN_EV_R = max(.10, float(os.getenv('AUTONOMOUS_V61_MIN_EV_R', '.20')))
MIN_CI05_R = max(.0, float(os.getenv('AUTONOMOUS_V61_MIN_CI05_R', '.08')))
MIN_WF_STABILITY = max(.65, float(os.getenv('AUTONOMOUS_V61_MIN_WF_STABILITY', '.70')))
MIN_PROFITABLE_FOLDS = max(.66, float(os.getenv('AUTONOMOUS_V61_MIN_PROFITABLE_FOLDS', '.66')))
MIN_WORST_FOLD_EV_R = max(-.05, float(os.getenv('AUTONOMOUS_V61_MIN_WORST_FOLD_EV_R', '-.05')))
MAX_DD_R = max(10.0, float(os.getenv('AUTONOMOUS_V61_MAX_DD_R', '80.0')))
MIN_RETURN_DD = max(1.5, float(os.getenv('AUTONOMOUS_V61_MIN_RETURN_DD', '2.50')))
FORWARD_DD_QUARANTINE_R = max(5.0, float(os.getenv('AUTONOMOUS_V61_FORWARD_DD_QUARANTINE_R', '12.0')))
FORWARD_CONFIRM_FILLS = max(60, int(os.getenv('AUTONOMOUS_V61_FORWARD_CONFIRM_FILLS', '80')))

_LOCK = threading.Lock()
_INSTALLED = False
_WORKER_STARTED = False
_BASE_PIPELINE: Any | None = None
_BASE_STATUS: Any | None = None
_BASE_QUARANTINE: Any | None = None


def _now() -> int:
    return int(time.time())


def _f(value: Any, default: float = 0.0) -> float:
    try:
        x = float(value)
        return x if math.isfinite(x) else default
    except (TypeError, ValueError):
        return default


def _d(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _j(value: Any, default: Any) -> Any:
    if value in (None, ''):
        return default
    try:
        out = json.loads(value) if isinstance(value, str) else value
        return out if isinstance(out, type(default)) else default
    except Exception:
        return default


def _jd(value: Any) -> Any:
    if hasattr(value, 'item'):
        return value.item()
    if isinstance(value, (bytes, bytearray, memoryview)):
        return {'__binary_bytes__': len(value)}
    raise TypeError(type(value).__name__)


def _state(core: Any, **patch: Any) -> dict[str, Any]:
    out = _d(core.state.get(STATE_KEY))
    out.update(patch)
    out.update({'schema': SCHEMA, 'runtime': VERSION, 'public_runtime': runtime_identity.RUNTIME_VERSION,
                'updated_at': _now()})
    core.state[STATE_KEY] = out
    return out


def _base_failed_gates(metrics: dict[str, Any], autonomous: Any) -> list[str]:
    failures: list[str] = []
    if int(metrics.get('oos_fills') or 0) < int(autonomous.MIN_OOS_FILLS): failures.append('OOS fills')
    if _f(metrics.get('profit_factor')) < float(autonomous.MIN_OOS_PF): failures.append('OOS PF')
    if _f(metrics.get('expectancy_r')) < float(autonomous.MIN_OOS_EV_R): failures.append('OOS EV')
    if _f(metrics.get('max_drawdown_r'), 1e9) > float(autonomous.MAX_OOS_DD_R): failures.append('OOS DD')
    if _f(metrics.get('bootstrap_ci05_r'), -1e9) <= float(autonomous.MIN_BOOTSTRAP_CI05): failures.append('Bootstrap CI05')
    if int(metrics.get('invalid_future_paths') or 0) != 0: failures.append('Invalid future paths')
    if _f(metrics.get('stability')) < float(autonomous.MIN_WF_STABILITY): failures.append('WF stability')
    if _f(metrics.get('profitable_folds')) < float(autonomous.MIN_PROFITABLE_FOLDS): failures.append('Profitable folds')
    if _f(metrics.get('worst_fold_ev'), -1e9) < float(autonomous.MIN_WORST_FOLD_EV): failures.append('Worst fold EV')
    return failures


def _eligible(metrics: dict[str, Any], autonomous: Any) -> tuple[bool, dict[str, Any]]:
    """Only a sole absolute-DD failure can enter the provisional tier."""
    failed = _base_failed_gates(metrics, autonomous)
    fills = int(metrics.get('oos_fills') or 0)
    pf = _f(metrics.get('profit_factor'))
    ev = _f(metrics.get('expectancy_r'))
    ci = _f(metrics.get('bootstrap_ci05_r'), -999.0)
    dd = _f(metrics.get('max_drawdown_r'), 1e9)
    total = _f(metrics.get('total_oos_r'))
    stability = _f(metrics.get('stability'))
    profitable = _f(metrics.get('profitable_folds'))
    worst = _f(metrics.get('worst_fold_ev'), -999.0)
    ratio = total / max(dd, 1e-9) if total > 0 and dd > 0 else 0.0
    checks = {
        'sole_failed_gate_is_oos_dd': failed == ['OOS DD'],
        'fills': fills >= MIN_FILLS,
        'pf': pf >= MIN_PF,
        'ev_r': ev >= MIN_EV_R,
        'ci05_r': ci >= MIN_CI05_R,
        'wf_stability': stability >= MIN_WF_STABILITY,
        'profitable_folds': profitable >= MIN_PROFITABLE_FOLDS,
        'worst_fold_ev_r': worst >= MIN_WORST_FOLD_EV_R,
        'invalid_paths_zero': int(metrics.get('invalid_future_paths') or 0) == 0,
        'dd_hard_ceiling': dd <= MAX_DD_R,
        'return_to_drawdown': ratio >= MIN_RETURN_DD,
    }
    return bool(all(checks.values())), {
        'failed_strict_gates': failed, 'checks': checks, 'return_to_drawdown': ratio,
        'thresholds': {'min_fills': MIN_FILLS, 'min_pf': MIN_PF, 'min_ev_r': MIN_EV_R,
                       'min_ci05_r': MIN_CI05_R, 'min_wf_stability': MIN_WF_STABILITY,
                       'min_profitable_folds': MIN_PROFITABLE_FOLDS,
                       'min_worst_fold_ev_r': MIN_WORST_FOLD_EV_R,
                       'max_dd_r': MAX_DD_R, 'min_return_to_drawdown': MIN_RETURN_DD},
    }


def _latest_run(core: Any, pipeline52: Any) -> str:
    s = _d(core.state.get('v49_stage6_atomic_orchestration'))
    if s.get('run_id'):
        return str(s['run_id'])
    pipeline52._ensure(core)
    con = core.db()
    try:
        row = con.execute(f'''SELECT run_id,MAX(updated_at) FROM {pipeline52.VAULT_TABLE}
                              GROUP BY run_id ORDER BY MAX(updated_at) DESC LIMIT 1''').fetchone()
        return str(row[0]) if row else ''
    finally:
        con.close()


def _rejected_rows(core: Any, autonomous: Any, pipeline52: Any) -> list[dict[str, Any]]:
    run = _latest_run(core, pipeline52)
    if not run:
        return []
    pipeline52._ensure(core)
    con = core.db()
    try:
        rows = con.execute(f'''SELECT rank,finalist_id,direction,genome,development,audit
                               FROM {pipeline52.VAULT_TABLE}
                               WHERE run_id=? AND selected_finalist=1 AND audit IS NOT NULL
                               ORDER BY COALESCE(rank,999999)''', (run,)).fetchall()
    finally:
        con.close()
    out: list[dict[str, Any]] = []
    for rank, fid, direction, genome_raw, dev_raw, audit_raw in rows:
        audit = _j(audit_raw, {})
        metrics = _d(audit.get('metrics'))
        if bool(audit.get('promoted')):
            continue
        ok, rationale = _eligible(metrics, autonomous)
        if not ok:
            continue
        out.append({'rank': int(rank or 0), 'finalist_id': str(fid or ''),
                    'direction': str(direction or ''), 'genome': _j(genome_raw, {}),
                    'development': _j(dev_raw, {}), 'audit': audit,
                    'metrics': metrics, 'rationale': rationale})
    out.sort(key=lambda x: (_f(x['metrics'].get('expectancy_r')), _f(x['metrics'].get('profit_factor')),
                            _f(x['rationale'].get('return_to_drawdown'))), reverse=True)
    return out[:MAX_PROVISIONALS]


def _refit_frozen_package(core: Any, autonomous: Any, row: dict[str, Any]) -> dict[str, Any]:
    genome = dict(row['genome'])
    audit = dict(row['audit'])
    metrics = dict(row['metrics'])
    gate_thresholds = list(audit.get('gate_thresholds') or metrics.get('gate_thresholds') or [])
    threshold = _f(metrics.get('direct_r_threshold'), float('nan'))
    if not math.isfinite(threshold):
        raise RuntimeError('frozen finalist has no persisted direct-R threshold')

    snapshots = autonomous._load_feature_snapshots(core)
    market = autonomous._load_market(core)
    if not snapshots or not market:
        raise RuntimeError('historical snapshots/market unavailable for provisional final refit')
    try:
        ts = snapshots['ts']; x = snapshots['x']
        stride = autonomous._decision_mask(ts, int(genome.get('decision_stride') or 1))
        mask = autonomous._gate_mask(x, gate_thresholds) & stride
        idx = np.where(mask)[0]
        idx = autonomous._sample_evenly(idx, autonomous.FINAL_REFIT_CAP)
        x_fit, y_fit, _ = autonomous._simulate_indices(idx, snapshots, market, genome)
        if len(y_fit) < 180:
            raise RuntimeError(f'provisional refit has only {len(y_fit)} causal fills')
        seed = int(autonomous._hash_payload({'v61': genome}, 8), 16) & 0xFFFFFFFF
        model = autonomous._model(genome, seed)
        model.fit(autonomous._feature_subset_matrix(x_fit, genome), y_fit)
        feature_idx = [autonomous.FEATURE_INDEX[n] for n in genome.get('feature_names', []) if n in autonomous.FEATURE_INDEX]
        train_matrix = x[idx][:, feature_idx] if feature_idx else x[idx]
        if not len(train_matrix):
            raise RuntimeError('provisional refit feature matrix empty')
        metrics.update({
            'schema': SCHEMA,
            'certification_tier': TIER,
            'strict_historical_certified': False,
            'strict_historical_failed_gate': 'OOS DD',
            'selection_after_oos_visibility': True,
            'paper_only': True,
            'forward_confirmation_required': True,
            'historical_oos_rewritten': False,
            'historical_oos_frozen': True,
            'provisional_reason': 'all strict gates passed except absolute OOS DD; admitted only under predeclared V61 high-conviction risk-adjusted paper gate',
            'return_to_drawdown': float(row['rationale']['return_to_drawdown']),
            'v61_gate': row['rationale'],
            'direct_r_threshold': threshold,
            'gate_thresholds': gate_thresholds,
            'feature_median': np.median(train_matrix, axis=0).astype(float).tolist(),
            'feature_q1': np.quantile(train_matrix, .25, axis=0).astype(float).tolist(),
            'feature_q3': np.quantile(train_matrix, .75, axis=0).astype(float).tolist(),
        })
        return {'genome': genome, 'metrics': metrics, 'gate_thresholds': gate_thresholds,
                'model_blob': pickle.dumps(model, pickle.HIGHEST_PROTOCOL)}
    finally:
        try: snapshots.clear()
        except Exception: pass
        try: market.clear()
        except Exception: pass


def _persist_provisional(core: Any, autonomous: Any, pipeline52: Any,
                         row: dict[str, Any], fitted: dict[str, Any]) -> dict[str, Any]:
    genome = dict(fitted['genome']); metrics = dict(fitted['metrics'])
    sid = 'AUTO_PROV_' + autonomous._hash_payload(genome, 12).upper()
    label = autonomous._behavior_label(genome, list(fitted.get('gate_thresholds') or []))
    metrics.update({'strategy_id': sid, 'behavior_label': label, 'rank': int(row.get('rank') or 0)})
    con = core.db()
    try:
        con.execute(f'''INSERT OR REPLACE INTO {autonomous.REGISTRY_TABLE}
            (strategy_id,created_at,status,direction,behavior_label,genome,metrics,model,active)
            VALUES(?,?,?,?,?,?,?,?,1)''',
            (sid, _now(), 'CHAMPION', str(genome.get('direction') or 'UNKNOWN'), label,
             json.dumps(genome, separators=(',', ':'), default=_jd),
             json.dumps(metrics, separators=(',', ':'), ensure_ascii=False, default=_jd), fitted['model_blob']))
        con.commit()
    finally:
        con.close()
    saved = {'strategy_id': sid, 'direction': genome.get('direction'), 'behavior_label': label, **metrics}
    # Reuse V52's durable handoff/vault plumbing, but preserve the historical audit as rejected.
    try:
        pipeline52._attach_champion(core, autonomous,
                                    {'genome': genome, 'metrics': metrics, 'model_blob': fitted['model_blob']}, saved)
    except Exception as exc:
        _state(core, handoff_attach_error=f'{type(exc).__name__}: {exc}')
    return saved


def _existing_provisionals(core: Any, autonomous: Any) -> list[dict[str, Any]]:
    con = core.db()
    try:
        rows = con.execute(f'''SELECT strategy_id,direction,behavior_label,metrics,active FROM {autonomous.REGISTRY_TABLE}
            WHERE status='CHAMPION' AND active=1 ORDER BY created_at,strategy_id''').fetchall()
    finally:
        con.close()
    out = []
    for sid, direction, label, metrics_raw, active in rows:
        metrics = _j(metrics_raw, {})
        if str(metrics.get('certification_tier') or '') == TIER:
            out.append({'strategy_id': str(sid), 'direction': str(direction), 'behavior_label': str(label),
                        'metrics': metrics, 'active': bool(active)})
    return out


def _strict_champions(core: Any, autonomous: Any) -> list[dict[str, Any]]:
    out = []
    for item in list(autonomous._load_registry(core, active_only=True) or []):
        metrics = _d(item.get('metrics'))
        if str(metrics.get('certification_tier') or '') != TIER:
            out.append(item)
    return out


def _worker(core: Any, autonomous: Any, pipeline52: Any) -> None:
    try:
        cp = _d(core.get_state(autonomous.CHECKPOINT_KEY, {}))
        if not ENABLED:
            _state(core, status='DISABLED'); return
        if cp.get('status') != 'COMPLETE':
            _state(core, status='WAITING_HISTORICAL_TERMINAL'); return
        existing = _existing_provisionals(core, autonomous)
        if existing:
            _state(core, status='PROVISIONAL_CURRENT_PAPER_READY', provisionals=existing,
                   provisional_count=len(existing)); return
        if _strict_champions(core, autonomous):
            _state(core, status='STRICT_CHAMPION_EXISTS_NO_EXCEPTION_NEEDED'); return
        rows = _rejected_rows(core, autonomous, pipeline52)
        if not rows:
            _state(core, status='NO_HIGH_CONVICTION_PROVISIONAL_CANDIDATE'); return
        promoted = []
        for row in rows:
            _state(core, status='REFITTING_FROZEN_PROVISIONAL', finalist_rank=row['rank'],
                   finalist_id=row['finalist_id'], rationale=row['rationale'])
            fitted = _refit_frozen_package(core, autonomous, row)
            promoted.append(_persist_provisional(core, autonomous, pipeline52, row, fitted))
        _state(core, status='PROVISIONAL_CURRENT_PAPER_READY', provisionals=promoted,
               provisional_count=len(promoted), historical_strict_champion_count=0)
    except Exception as exc:
        _state(core, status='PROVISIONAL_REFIT_ERROR', error=f'{type(exc).__name__}: {exc}')


def _forward_pnls(core: Any, v56: Any, sid: str) -> list[float]:
    con = core.db()
    try:
        rows = con.execute(f'''SELECT result_r FROM {v56.OBS_TABLE}
            WHERE strategy_id=? AND source='CERTIFIED' AND status='SETTLED' AND filled=1
            ORDER BY decision_ts''', (str(sid),)).fetchall()
    finally:
        con.close()
    return [float(r[0]) for r in rows if r[0] is not None]


def _forward_confirmation(core: Any, autonomous: Any, v56: Any, sid: str) -> dict[str, Any]:
    pnls = _forward_pnls(core, v56, sid)
    if not pnls:
        return {'fills': 0, 'confirmed': False}
    st = v56._champion_forward_stats(core, sid, max(len(pnls), 1))
    ci05 = autonomous._bootstrap_ci05(pnls, int(autonomous._hash_payload({'v61-forward': sid}, 8), 16), reps=400, block=8)
    confirmed = bool(len(pnls) >= FORWARD_CONFIRM_FILLS and st['pf'] >= float(autonomous.MIN_OOS_PF) and
                     st['ev'] >= float(autonomous.MIN_OOS_EV_R) and st['dd'] <= float(autonomous.MAX_OOS_DD_R) and
                     ci05 > float(autonomous.MIN_BOOTSTRAP_CI05))
    return {'fills': len(pnls), 'pf': float(st['pf']), 'ev_r': float(st['ev']), 'dd_r': float(st['dd']),
            'ci05_r': float(ci05), 'confirmed': confirmed}


def _install_forward_guard(core: Any, autonomous: Any, v56: Any) -> None:
    global _BASE_QUARANTINE
    if _BASE_QUARANTINE is not None:
        return
    _BASE_QUARANTINE = v56._refresh_quarantine

    def guarded(c: Any, a: Any) -> dict[str, Any]:
        q = dict(_BASE_QUARANTINE(c, a) or {})
        changed = False
        for item in _existing_provisionals(c, a):
            sid = str(item['strategy_id'])
            st = v56._champion_forward_stats(c, sid)
            if int(st.get('fills') or 0) >= 12 and float(st.get('dd') or 0.0) >= FORWARD_DD_QUARANTINE_R:
                q[sid] = {'active': True, 'at': _now(), 'reason': 'V61 provisional forward drawdown guard', 'stats': st}
                changed = True
            conf = _forward_confirmation(c, a, v56, sid)
            if conf.get('confirmed'):
                con = c.db()
                try:
                    row = con.execute(f'SELECT metrics FROM {a.REGISTRY_TABLE} WHERE strategy_id=?', (sid,)).fetchone()
                    metrics = _j(row[0], {}) if row else {}
                    if not metrics.get('forward_confirmed'):
                        metrics.update({'forward_confirmed': True, 'forward_confirmation': conf,
                                        'certification_tier': 'PROVISIONAL_THEN_FUTURE_ONLY_CONFIRMED',
                                        'strict_historical_certified': False})
                        con.execute(f'UPDATE {a.REGISTRY_TABLE} SET metrics=? WHERE strategy_id=?',
                                    (json.dumps(metrics, separators=(',', ':'), ensure_ascii=False, default=_jd), sid))
                        con.commit(); changed = True
                finally:
                    con.close()
        if changed:
            c.set_state('v56_champion_quarantine', q)
        return q

    v56._refresh_quarantine = guarded


def _install_status(core: Any, autonomous: Any, pipeline: Any) -> None:
    global _BASE_STATUS, _BASE_PIPELINE
    if _BASE_STATUS is None:
        _BASE_STATUS = autonomous.autonomous_status
        def status(c: Any) -> dict[str, Any]:
            z = dict(_BASE_STATUS(c) or {})
            prov = _existing_provisionals(c, autonomous)
            strict = _strict_champions(c, autonomous)
            z.update({'strict_certified_champions': len(strict), 'provisional_paper_champions': len(prov),
                      'provisional_current_paper': bool(prov),
                      'certification_label': ('STRICT_CERTIFIED' if strict else
                                              'PROVISIONAL_CURRENT_PAPER' if prov else 'NO_CERTIFIED_PACKAGE')})
            return z
        autonomous.autonomous_status = status
    if _BASE_PIPELINE is None:
        _BASE_PIPELINE = pipeline.pipeline_status
        def pipe(c: Any) -> dict[str, Any]:
            p = dict(_BASE_PIPELINE(c) or {})
            prov = _existing_provisionals(c, autonomous); strict = _strict_champions(c, autonomous)
            if prov and not strict:
                stages = [dict(x) for x in list(p.get('stages') or [])]
                for x in stages:
                    name = str(x.get('name') or '')
                    if name.startswith('8.'):
                        x['percent'] = 100.0; x['status'] = 'PROVISIONAL_PAPER'
                        x['strict_historical_certified'] = False
                    if name.startswith('9.'):
                        x['percent'] = 100.0; x['status'] = 'CURRENT_PAPER_PROVISIONAL'
                        x['paper_only'] = True
                p['stages'] = stages; p['operational'] = True
                p['v61_provisional_current_paper'] = True
            return p
        pipeline.pipeline_status = pipe


def _inject(html: str) -> str:
    if 'v61-provisional-card' in html:
        return html
    card = '''<section class="card" id="v61-provisional-card"><h2>🟠 V61 風險調整 Provisional Current Paper</h2>
<div id="v61p" class="notice y">讀取 V61 狀態…</div></section>'''
    script = r'''<script id="v61-provisional-js">(function(){async function v61(){const e=document.getElementById('v61p');if(!e)return;try{const r=await fetch('/api/v61/provisional',{cache:'no-store'}),z=await r.json();e.className='notice '+(Number(z.provisional_count||0)>0?'y':'g');const rows=z.provisionals||[];e.innerHTML='<b>'+String(z.status||'—')+'</b><br>Strict 9/9 Champion：<b>'+Number(z.strict_champion_count||0)+'</b>｜Provisional Paper：<b>'+Number(z.provisional_count||0)+'</b><br>'+(rows.length?rows.map(x=>'⚠️ <b>'+String(x.strategy_id)+'</b> '+String(x.direction)+'｜PF '+Number(x.pf||0).toFixed(2)+'｜EV '+Number(x.ev_r||0).toFixed(3)+'R｜DD '+Number(x.dd_r||0).toFixed(2)+'R｜CI05 '+Number(x.ci05_r||0).toFixed(3)+'R｜Return/DD '+Number(x.return_to_drawdown||0).toFixed(2)).join('<br>'):'沒有符合 V61 高可信 sole-DD 例外的策略。')+'<br><span style="color:#ffd56a">Provisional 只准 Paper；歷史 strict OOS 結論不會被改寫。Forward DD ≥ '+Number(z.forward_dd_quarantine_r||0).toFixed(1)+'R 會自動隔離。</span>'}catch(err){e.className='notice r';e.textContent='V61 讀取失敗：'+String(err)}}v61();setInterval(()=>{if(!document.hidden)v61()},12000)})();</script>'''
    if '</main>' in html:
        html = html.replace('</main>', card + '</main>', 1)
    elif '</body>' in html:
        html = html.replace('</body>', card + '</body>', 1)
    else:
        html += card
    return html.replace('</body>', script + '</body>', 1) if '</body>' in html else html + script


def _wrap_dashboard(core: Any, path: str, name: str) -> None:
    route = next((r for r in core.app.router.routes if getattr(r, 'path', None) == path), None)
    old = getattr(route, 'endpoint', None)
    if not callable(old):
        return
    from fastapi.responses import HTMLResponse
    core.app.router.routes = [r for r in core.app.router.routes if getattr(r, 'path', None) != path]
    def endpoint() -> HTMLResponse:
        raw = old(); html = raw.body.decode('utf-8', errors='replace') if hasattr(raw, 'body') else str(raw)
        return HTMLResponse(_inject(html), headers={'Cache-Control': 'no-store, max-age=0',
                                                   'X-ETH-Adaptive-Provisional': VERSION})
    core.app.add_api_route(path, endpoint, methods=['GET'], response_class=HTMLResponse, name=name)


def _api(core: Any, autonomous: Any) -> dict[str, Any]:
    prov = _existing_provisionals(core, autonomous); strict = _strict_champions(core, autonomous)
    rows = []
    for item in prov:
        m = _d(item.get('metrics'))
        rows.append({'strategy_id': item['strategy_id'], 'direction': item['direction'],
                     'pf': _f(m.get('profit_factor')), 'ev_r': _f(m.get('expectancy_r')),
                     'dd_r': _f(m.get('max_drawdown_r')), 'ci05_r': _f(m.get('bootstrap_ci05_r')),
                     'fills': int(m.get('oos_fills') or 0), 'return_to_drawdown': _f(m.get('return_to_drawdown')),
                     'strict_historical_certified': False, 'paper_only': True,
                     'forward_confirmation': m.get('forward_confirmation')})
    s = _d(core.state.get(STATE_KEY))
    return {**s, 'schema': SCHEMA, 'runtime': VERSION, 'enabled': ENABLED,
            'strict_champion_count': len(strict), 'provisional_count': len(rows), 'provisionals': rows,
            'forward_dd_quarantine_r': FORWARD_DD_QUARANTINE_R,
            'rules': {'historical_oos_rewritten': False, 'posthoc_exception_claimed_strict_certified': False,
                      'sole_strict_failure_must_be_oos_dd': True, 'paper_only': True,
                      'future_only_confirmation': True, 'future_peeking_enabled': False}, 'updated_at': _now()}


def install(production: Any, autonomous: Any, pipeline52: Any, pipeline: Any, v56: Any) -> None:
    global _INSTALLED, _WORKER_STARTED
    with _LOCK:
        if _INSTALLED:
            return
        _INSTALLED = True
    core = production.core
    _install_forward_guard(core, autonomous, v56)
    _install_status(core, autonomous, pipeline)

    core.app.router.routes = [r for r in core.app.router.routes if getattr(r, 'path', None) != '/api/v61/provisional']
    core.app.add_api_route('/api/v61/provisional', lambda: _api(core, autonomous), methods=['GET'], name='v61_provisional')
    _wrap_dashboard(core, '/', 'v61_fast_dashboard')
    _wrap_dashboard(core, '/dashboard/full', 'v61_full_dashboard')

    _state(core, status='READY_TO_EVALUATE_PROVISIONAL', enabled=ENABLED,
           strict_oos_thresholds_changed=False, historical_oos_rewritten=False,
           posthoc_exception_claimed_strict_certified=False, paper_only=True,
           future_peeking_enabled=False, replay_reset=False, historical_data_deleted=False)
    if not _WORKER_STARTED:
        _WORKER_STARTED = True
        threading.Thread(target=_worker, args=(core, autonomous, pipeline52),
                         name='v61-provisional-refit', daemon=True).start()
    runtime_identity.stamp(core)
