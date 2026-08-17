from __future__ import annotations

"""Exact dataset/code fingerprint authority for autonomous Stage 6.

V46 made candidate work resumable, but its run fingerprint intentionally sampled the
large arrays.  Sampling is not sufficient to prove bit-identical research inputs after
many deployments.  V47 hashes every byte that can affect Stage-6 decisions/outcomes,
plus the frozen Bitget execution contract and the exact semantic Python modules.

A completed V46 candidate can therefore be resumed only when the complete causal
feature matrix, all Stage-6 OHLC settlement paths, 15m decision closes, research/search
configuration, execution contract and research code are identical.  Any difference
creates a new run id and fails closed against stale candidate reuse.
"""

import hashlib
import importlib
import json
import os
import threading
import time
from pathlib import Path
from typing import Any

import numpy as np

import runtime_identity
import v16_runtime_integrity as runtime_integrity
import v36_bitget_execution_truth as leverage_truth

VERSION = 'V47_DATASET_INTEGRITY_AUTHORITY'
SCHEMA = 47
STATE_KEY = 'v47_dataset_integrity_authority'
CHUNK_BYTES = max(1 << 20, min(32 << 20, int(os.getenv('AUTONOMOUS_V47_HASH_CHUNK_BYTES', str(8 << 20)))))
ROW_CHUNK = max(256, min(16384, int(os.getenv('AUTONOMOUS_V47_HASH_ROW_CHUNK', '4096'))))

SEMANTIC_MODULES = (
    'v30_autonomous_strategy_discovery',
    'v33_autonomous_compute_efficiency',
    'v35_autonomous_feature_integrity',
    'v36_bitget_execution_truth',
    'v43_unified_performance_authority',
    'v44_fixed_research_horizon_authority',
    'v45_autonomous_market_cache_alignment',
    'v46_stage6_throughput_liveness',
    'v47_dataset_integrity_authority',
)

_LOCK = threading.Lock()
_INSTALLED = False
_LAST_MANIFEST: dict[str, Any] = {}


def _json_default(value: Any) -> Any:
    if hasattr(value, 'item'):
        return value.item()
    raise TypeError(type(value).__name__)


def _state(core: Any, **patch: Any) -> dict[str, Any]:
    old = core.state.get(STATE_KEY)
    out = dict(old) if isinstance(old, dict) else {}
    out.update(patch)
    out.update({'schema': SCHEMA, 'runtime': VERSION, 'public_runtime': runtime_identity.RUNTIME_VERSION, 'updated_at': int(time.time())})
    core.state[STATE_KEY] = out
    return out


def _hash_array(value: Any) -> dict[str, Any]:
    arr = np.asarray(value)
    h = hashlib.sha256()
    meta = {'dtype': str(arr.dtype), 'shape': list(arr.shape), 'size': int(arr.size)}
    h.update(json.dumps(meta, sort_keys=True, separators=(',', ':')).encode())
    if arr.size:
        if arr.ndim == 0:
            h.update(np.ascontiguousarray(arr.reshape(1)).tobytes(order='C'))
        elif arr.flags.c_contiguous:
            raw = memoryview(arr).cast('B')
            for off in range(0, len(raw), CHUNK_BYTES):
                h.update(raw[off:off + CHUNK_BYTES])
        else:
            for off in range(0, arr.shape[0], ROW_CHUNK):
                h.update(np.ascontiguousarray(arr[off:off + ROW_CHUNK]).tobytes(order='C'))
    return {**meta, 'sha256': h.hexdigest()}


def _hash_close15(close15: Any) -> dict[str, Any]:
    data = close15 if isinstance(close15, dict) else {}
    items = sorted((int(k), float(v)) for k, v in data.items())
    keys = np.asarray([x[0] for x in items], dtype=np.int64)
    vals = np.asarray([x[1] for x in items], dtype=np.float64)
    h = hashlib.sha256()
    h.update(_hash_array(keys)['sha256'].encode())
    h.update(_hash_array(vals)['sha256'].encode())
    return {
        'count': len(items),
        'first_ts': int(keys[0]) if len(keys) else None,
        'last_ts': int(keys[-1]) if len(keys) else None,
        'sha256': h.hexdigest(),
    }


def _file_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open('rb') as fh:
        while True:
            chunk = fh.read(CHUNK_BYTES)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def _semantic_code_manifest() -> dict[str, str]:
    out: dict[str, str] = {}
    for name in SEMANTIC_MODULES:
        try:
            module = importlib.import_module(name)
            path = Path(str(module.__file__ or '')).resolve()
            out[name] = _file_sha256(path) if path.is_file() else 'MISSING'
        except Exception as exc:
            out[name] = f'ERROR:{type(exc).__name__}'
    return out


def _frozen_execution_contract(core: Any, autonomous: Any) -> dict[str, Any]:
    raw = core.get_state(leverage_truth.FROZEN_KEY, {})
    raw = dict(raw) if isinstance(raw, dict) else {}
    # Only deterministic fields that can influence historical simulation belong in
    # the fingerprint. fetched_at/frozen_at are intentionally excluded.
    keys = (
        'schema', 'ok', 'symbol', 'product_type', 'notional_usdt',
        'contract_max_leverage', 'tier_max_leverage', 'effective_max_leverage',
        'maintenance_margin_rate', 'tier_start_notional', 'tier_end_notional',
        'raw_margin_headroom_fraction', 'conservative_stop_headroom_fraction',
        'headroom_model', 'source', 'research_start_ts', 'research_end_exclusive_ts',
        'purpose', 'not_a_market_feature',
    )
    out = {k: raw.get(k) for k in keys if k in raw}
    out.setdefault('notional_usdt', float(getattr(autonomous, 'PAPER_NOTIONAL_USDT', 0.0)))
    return out


def _full_manifest(core: Any, autonomous: Any, snapshots: dict[str, Any], market: dict[str, Any]) -> dict[str, Any]:
    started = time.monotonic()
    replay = dict(runtime_integrity.replay_progress(core) or {})
    baseline = core.get_state('final_dataset_baseline_v1', {})
    baseline = dict(baseline) if isinstance(baseline, dict) else {}

    arrays = {
        'snapshot_ts': _hash_array(snapshots.get('ts') if snapshots.get('ts') is not None else []),
        'snapshot_x': _hash_array(snapshots.get('x') if snapshots.get('x') is not None else []),
        'snapshot_quality': _hash_array(snapshots.get('quality') if snapshots.get('quality') is not None else []),
        'market_ts5': _hash_array(market.get('ts5') if market.get('ts5') is not None else []),
        'market_o5': _hash_array(market.get('o5') if market.get('o5') is not None else []),
        'market_h5': _hash_array(market.get('h5') if market.get('h5') is not None else []),
        'market_l5': _hash_array(market.get('l5') if market.get('l5') is not None else []),
        'market_c5': _hash_array(market.get('c5') if market.get('c5') is not None else []),
    }
    close15 = _hash_close15(market.get('close15'))
    code = _semantic_code_manifest()
    contract = _frozen_execution_contract(core, autonomous)

    config = {
        'reset_marker': str(getattr(autonomous, 'RESET_MARKER', '')),
        'research_start_ts': int(autonomous.RESEARCH_START_TS),
        'research_end_exclusive_ts': int(autonomous.RESEARCH_END_EXCLUSIVE_TS),
        'settlement_end_exclusive_ts': int(autonomous.SETTLEMENT_END_EXCLUSIVE_TS),
        'population': int(autonomous.POPULATION), 'generations': int(autonomous.GENERATIONS),
        'elites': int(autonomous.ELITES), 'finalists': int(autonomous.FINALISTS),
        'max_champions': int(autonomous.MAX_CHAMPIONS),
        'train_sim_cap': int(autonomous.TRAIN_SIM_CAP), 'cal_sim_cap': int(autonomous.CAL_SIM_CAP),
        'test_sim_cap': int(autonomous.TEST_SIM_CAP), 'final_refit_cap': int(autonomous.FINAL_REFIT_CAP),
        'hold_bars_15m': list(map(int, autonomous.HOLD_BARS_15M)),
        'expire_bars_15m': list(map(int, autonomous.EXPIRE_BARS_15M)),
        'decision_strides': list(map(int, autonomous.DECISION_STRIDES)),
        'final_holdout_pct': float(autonomous.FINAL_HOLDOUT_PCT),
        'min_oos_fills': int(autonomous.MIN_OOS_FILLS), 'min_oos_pf': float(autonomous.MIN_OOS_PF),
        'min_oos_ev_r': float(autonomous.MIN_OOS_EV_R), 'max_oos_dd_r': float(autonomous.MAX_OOS_DD_R),
        'min_wf_stability': float(autonomous.MIN_WF_STABILITY),
        'min_profitable_folds': float(autonomous.MIN_PROFITABLE_FOLDS),
        'min_worst_fold_ev': float(autonomous.MIN_WORST_FOLD_EV),
        'min_bootstrap_ci05': float(autonomous.MIN_BOOTSTRAP_CI05),
        'all_in_cost_bps': float(autonomous.ALL_IN_COST_BPS),
        'paper_notional_usdt': float(autonomous.PAPER_NOTIONAL_USDT),
        'feature_names': list(autonomous.FEATURE_NAMES),
        'market_source5': str(market.get('source5') or ''),
        'market_source15': str(market.get('source15') or ''),
    }
    payload = {
        'schema': SCHEMA,
        'dataset_id': baseline.get('dataset_id'),
        'baseline_clean': bool(baseline.get('clean')),
        'replay_complete': bool(replay.get('complete')),
        'replay_cursor': replay.get('cursor_ts') or replay.get('cursor'),
        'arrays': arrays,
        'close15': close15,
        'code': code,
        'contract': contract,
        'config': config,
    }
    digest = hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(',', ':'), default=_json_default).encode()).hexdigest()
    return {
        **payload,
        'full_sha256': digest,
        'hash_scope': 'EVERY_STAGE6_FEATURE_AND_PRICE_BYTE_PLUS_CLOSE15_CODE_CONFIG_AND_FROZEN_EXECUTION_CONTRACT',
        'sampled_hash_only': False,
        'future_prices_as_features': False,
        'future_5m_role': 'OUTCOME_SETTLEMENT_AFTER_PLAN_FREEZE_ONLY',
        'elapsed_seconds': round(time.monotonic() - started, 3),
        'created_at': int(time.time()),
    }


def install(production: Any, autonomous: Any, throughput: Any) -> None:
    global _INSTALLED, _LAST_MANIFEST
    with _LOCK:
        if _INSTALLED:
            return
        _INSTALLED = True
    core = production.core
    old_run_fingerprint = throughput._run_fingerprint

    def exact_run_fingerprint(c: Any, a: Any, snapshots: dict[str, Any], market: dict[str, Any]) -> str:
        global _LAST_MANIFEST
        legacy = str(old_run_fingerprint(c, a, snapshots, market))
        manifest = _full_manifest(c, a, snapshots, market)
        _LAST_MANIFEST = manifest
        run_id = hashlib.sha256(f"v47|{legacy}|{manifest['full_sha256']}".encode()).hexdigest()[:28]
        previous = c.get_state('v47_last_stage6_manifest', {})
        previous = dict(previous) if isinstance(previous, dict) else {}
        same_as_previous = bool(previous and previous.get('full_sha256') == manifest['full_sha256'])
        c.set_state('v47_last_stage6_manifest', {
            'full_sha256': manifest['full_sha256'], 'run_id': run_id,
            'dataset_id': manifest.get('dataset_id'), 'created_at': manifest.get('created_at'),
            'hash_scope': manifest.get('hash_scope'),
        })
        _state(c, status='VERIFIED_EXACT_STAGE6_INPUT', run_id=run_id,
               full_sha256=manifest['full_sha256'], same_as_previous_manifest=same_as_previous,
               dataset_id=manifest.get('dataset_id'), manifest=manifest,
               stale_candidate_reuse_possible=False,
               exact_resume_requires_full_data_and_code_identity=True)
        return run_id

    throughput._run_fingerprint = exact_run_fingerprint

    if not any(getattr(r, 'path', None) == '/api/v47/dataset-integrity' for r in core.app.router.routes):
        @core.app.get('/api/v47/dataset-integrity')
        def dataset_integrity() -> dict[str, Any]:
            replay = dict(runtime_integrity.replay_progress(core) or {})
            return {
                'schema': SCHEMA, 'runtime': VERSION,
                'state': dict(core.state.get(STATE_KEY) or {}),
                'manifest': dict(_LAST_MANIFEST or {}),
                'replay': replay,
                'rules': {
                    'raw_or_derived_data_deleted_by_v47': False,
                    'learning_samples_modified_by_v47': False,
                    'feature_snapshots_modified_by_v47': False,
                    'market_bars_modified_by_v47': False,
                    'full_stage6_input_hash': True,
                    'close15_fully_hashed': True,
                    'semantic_code_fully_hashed': True,
                    'frozen_execution_contract_hashed': True,
                    'resume_on_any_input_difference': False,
                    'future_peeking_enabled': False,
                },
            }

    # Make the intra-candidate heartbeat visible instead of leaving users staring at
    # an outer 2.08% number while a long 240h candidate is still computing.
    root = next((r for r in list(core.app.router.routes) if getattr(r, 'path', None) == '/'), None)
    old_root = getattr(root, 'endpoint', None)
    if callable(old_root):
        from fastapi.responses import HTMLResponse
        core.app.router.routes = [r for r in core.app.router.routes if getattr(r, 'path', None) != '/']

        @core.app.get('/', response_class=HTMLResponse, name='v47_dataset_integrity_dashboard')
        def dashboard_v47() -> str:
            raw = old_root()
            html = raw.body.decode() if hasattr(raw, 'body') else str(raw)
            card = '''<section class="card"><h2>🧬 Stage 6 真實計算 / 資料完整性</h2><div id="v47integrity" class="notice">讀取 V47 完整資料指紋與 Candidate 心跳…</div></section>'''
            js = r'''<script id="v47-integrity-ui">
async function refreshV47(){try{
 const [a,b]=await Promise.all([fetch('/api/v47/dataset-integrity',{cache:'no-store'}).then(r=>r.json()),fetch('/api/v46/stage6-throughput',{cache:'no-store'}).then(r=>r.json())]);
 const el=document.getElementById('v47integrity');if(!el)return;const s=a.state||{},t=b.state||{},h=t.heartbeat||{};
 const verified=!!s.full_sha256;el.className='notice '+(verified?'g':'y');
 const pathDone=Number(h.paths_completed_current_call||0),pathTotal=Number(h.paths_total_current_call||0),rate=Number(h.paths_per_second_current_call||0);
 el.innerHTML='<b>'+(verified?'EXACT DATASET FINGERPRINT ACTIVE':'等待 Stage 6 建立完整指紋')+'</b>'+
  '<br>V47 full SHA-256：<code>'+String(s.full_sha256||'尚未建立')+'</code>'+
  '<br>Candidate：'+String(h.generation||'—')+'/'+String(h.generations||'—')+' · '+String(h.candidate||'—')+'/'+String(h.population||'—')+
  '<br>目前計算：'+pathDone.toLocaleString()+' / '+pathTotal.toLocaleString()+' paths · '+rate.toFixed(2)+'/s · workers '+String(h.simulation_workers||'—')+
  '<br>Heartbeat：'+String(h.heartbeat_at?new Date(Number(h.heartbeat_at)*1000).toLocaleTimeString('zh-TW',{hour12:false}):'—')+
  '<br>規則：完整資料或程式任何 1 byte 改變 → 不准沿用舊 Candidate；未凍結計畫前禁止讀未來價格。';
}catch(e){const el=document.getElementById('v47integrity');if(el){el.className='notice r';el.textContent='V47 狀態讀取失敗：'+String(e)}}}
refreshV47();setInterval(refreshV47,3000);
</script>'''
            marker = '</div><div class="footer">'
            if marker in html:
                html = html.replace(marker, card + marker, 1)
            else:
                html = html.replace('</body>', card + '</body>', 1) if '</body>' in html else html + card
            return html.replace('</body>', js + '</body>', 1) if '</body>' in html else html + js

    rules = {
        'history_reduced': False, 'features_reduced': False, 'population_reduced': False,
        'generations_reduced': False, 'holding_horizons_reduced': False,
        'oos_rules_changed': False, 'fitness_changed': False,
        'market_or_learning_data_mutated_by_v47': False,
        'full_exact_stage6_input_hash': True,
        'all_feature_matrix_bytes_hashed': True,
        'all_5m_ohlc_bytes_hashed': True,
        'all_15m_decision_closes_hashed': True,
        'semantic_code_hash_in_resume_identity': True,
        'frozen_execution_contract_hash_in_resume_identity': True,
        'any_input_difference_forces_new_candidate_run': True,
        'future_peeking_enabled': False,
    }
    core.state.setdefault('strict_replay', {})['dataset_integrity_authority_v47'] = dict(rules)
    _state(core, status='INSTALLED_WAITING_STAGE6_MANIFEST', rules=rules,
           exact_resume_requires_full_data_and_code_identity=True,
           stale_candidate_reuse_possible=False)
