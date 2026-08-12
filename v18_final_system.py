from __future__ import annotations

import asyncio
import hashlib
import json
import os
import threading
import time
from typing import Any

import execution_v7
import v5_runtime
import v7_runtime
import v8_evolution
import v9_training_store
import v12_clean_baseline
import v13_replay_cursor_integrity as cursor_guard
import v15_data_resilience as resilience
import v16_runtime_integrity as runtime_integrity
import v17_certification_orchestrator as cert17


VERSION = '10.0.0-20260812'
SCHEMA = 1
STATE_KEY = 'v18_final_system_state'
AUDIT_KEY = 'v18_final_dataset_audit'
FAILURE_KEY = 'v18_derived_failure_confirmation'
EXPECTED_ROWS_PER_DECISION = len(v5_runtime.STRATEGIES) * len(v5_runtime.DIRECTIONS)
AUDIT_CONFIRM_COUNT = max(2, min(6, int(os.getenv('FINAL_DERIVED_AUDIT_CONFIRM_COUNT', '3'))))
AUDIT_CONFIRM_SECONDS = max(30, min(900, int(os.getenv('FINAL_DERIVED_AUDIT_CONFIRM_SECONDS', '90'))))
RECERTIFY_MIN_NEW_SAMPLES = max(140, int(os.getenv('FINAL_RECERTIFY_MIN_NEW_SAMPLES', '1400')))
RECERTIFY_SECONDS = max(3600, int(os.getenv('FINAL_RECERTIFY_SECONDS', str(24 * 3600))))

_CERT_LOCK = threading.RLock()
_ORIGINAL_CERT17_TRAIN = cert17.train_v17
_ORIGINAL_CERT17_AUDIT = cert17.audit_derived_dataset


def _load_json(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return dict(raw)
    if isinstance(raw, str):
        try:
            val = json.loads(raw)
            return dict(val) if isinstance(val, dict) else {}
        except Exception:
            return {}
    return {}


def _baseline(core: Any) -> dict[str, Any]:
    raw = core.get_state(v12_clean_baseline.STATE_KEY, None)
    return dict(raw) if isinstance(raw, dict) else {}


def _table_exists(con: Any, name: str) -> bool:
    return bool(con.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)).fetchone())


def _db_snapshot(core: Any) -> dict[str, Any]:
    con = core.db()
    try:
        tables = {str(r[0]) for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
        if 'learning_samples' in tables:
            row = con.execute('SELECT COUNT(*),MIN(ts),MAX(ts),COUNT(DISTINCT ts) FROM learning_samples').fetchone()
            samples = int(row[0] or 0)
            min_ts = int(row[1]) if row[1] is not None else None
            max_ts = int(row[2]) if row[2] is not None else None
            decisions = int(row[3] or 0)
            partial = int(con.execute(
                'SELECT COUNT(*) FROM (SELECT ts,COUNT(*) n FROM learning_samples GROUP BY ts HAVING n<>?)',
                (EXPECTED_ROWS_PER_DECISION,),
            ).fetchone()[0] or 0)
            non_normalized = int(con.execute("SELECT COUNT(*) FROM learning_samples WHERE features <> ('@' || ts)").fetchone()[0] or 0)
        else:
            samples = decisions = partial = non_normalized = 0
            min_ts = max_ts = None
        snapshots = int(con.execute('SELECT COUNT(*) FROM learning_feature_snapshots').fetchone()[0] or 0) if 'learning_feature_snapshots' in tables else 0
        orphan_refs = 0
        if 'learning_samples' in tables and 'learning_feature_snapshots' in tables:
            orphan_refs = int(con.execute('''SELECT COUNT(*) FROM learning_samples ls
                LEFT JOIN learning_feature_snapshots fs ON fs.ts=ls.ts
                WHERE ls.features LIKE '@%' AND fs.ts IS NULL''').fetchone()[0] or 0)
        signal_champions = int(con.execute("SELECT COUNT(*) FROM model_registry WHERE status='CHAMPION' AND direction IN ('LONG','SHORT')").fetchone()[0] or 0) if 'model_registry' in tables else 0
        execution_champions = int(con.execute("SELECT COUNT(*) FROM execution_registry_v7 WHERE status='CHAMPION'").fetchone()[0] or 0) if 'execution_registry_v7' in tables else 0
        signals = int(con.execute('SELECT COUNT(*) FROM signals').fetchone()[0] or 0) if 'signals' in tables else 0
        closed_signals = int(con.execute("SELECT COUNT(*) FROM signals WHERE status='CLOSED'").fetchone()[0] or 0) if 'signals' in tables else 0
        live_samples = int(con.execute('SELECT COUNT(*) FROM live_execution_samples').fetchone()[0] or 0) if 'live_execution_samples' in tables else 0
        ledger_rows = int(con.execute('SELECT COUNT(*) FROM evolution_trade_ledger').fetchone()[0] or 0) if 'evolution_trade_ledger' in tables else 0
        ledger_closed = int(con.execute("SELECT COUNT(*) FROM evolution_trade_ledger WHERE status='CLOSED'").fetchone()[0] or 0) if 'evolution_trade_ledger' in tables else 0
        ledger_net_r = float(con.execute("SELECT COALESCE(SUM(realized_r),0) FROM evolution_trade_ledger WHERE status='CLOSED'").fetchone()[0] or 0) if 'evolution_trade_ledger' in tables else 0.0
        return {
            'tables': sorted(tables), 'learning_samples': samples, 'sample_min_ts': min_ts,
            'sample_max_ts': max_ts, 'decision_timestamps': decisions, 'feature_snapshots': snapshots,
            'partial_decision_timestamps': partial, 'non_normalized_feature_refs': non_normalized,
            'orphan_feature_refs': orphan_refs, 'signal_champions': signal_champions,
            'execution_champions': execution_champions, 'signals': signals, 'closed_signals': closed_signals,
            'live_execution_samples': live_samples, 'ledger_rows': ledger_rows,
            'ledger_closed': ledger_closed, 'ledger_net_r': ledger_net_r,
        }
    finally:
        con.close()


def _restore_schema_markers_if_safe(core: Any, snap: dict[str, Any]) -> list[str]:
    repaired: list[str] = []
    baseline = _baseline(core)
    structurally_clean = bool(
        baseline.get('clean') is True and baseline.get('status') == 'CLEAN' and baseline.get('dataset_id') and
        int(snap.get('learning_samples') or 0) > 0 and
        int(snap.get('partial_decision_timestamps') or 0) == 0 and
        int(snap.get('non_normalized_feature_refs') or 0) == 0 and
        int(snap.get('orphan_feature_refs') or 0) == 0 and
        int(snap.get('feature_snapshots') or 0) == int(snap.get('decision_timestamps') or 0)
    )
    if not structurally_clean:
        return repaired
    markers = (
        ('point_in_time_sample_schema', 6),
        ('replay_cursor_integrity_schema', 2),
        ('final_data_resilience_schema', 1),
    )
    for key, expected in markers:
        current = int(core.get_state(key, 0) or 0)
        if current == expected:
            continue
        core.set_state(key, expected)
        repaired.append(f'{key}:{current}->{expected}')
    return repaired


def _restore_cursor_from_samples(core: Any, snap: dict[str, Any]) -> dict[str, Any]:
    frontier = runtime_integrity._legal_frontier(core)
    legal = int(frontier.get('legal_frontier_ts') or core.START_TS)
    raw = core.get_state(v5_runtime.REPLAY_STATE_KEY, None)
    current = int(raw or core.START_TS)
    max_ts = int(snap.get('sample_max_ts') or 0)
    changed = False
    reason = None
    if int(snap.get('learning_samples') or 0) > 0 and max_ts > 0 and legal > int(core.START_TS):
        target = min(max_ts, legal)
        if max_ts >= legal and int(snap.get('partial_decision_timestamps') or 0) == 0:
            target = legal
        if raw is None or current < target:
            current = target
            core.set_state(v5_runtime.REPLAY_STATE_KEY, current)
            changed = True
            reason = 'persistent replay cursor restored from the newest structurally complete learning decision'
        elif current > legal:
            current = legal
            core.set_state(v5_runtime.REPLAY_STATE_KEY, current)
            changed = True
            reason = 'replay cursor clamped to the latest legally labelable frontier'
    return {'changed': changed, 'reason': reason, 'cursor_ts': current, 'legal_frontier_ts': legal}


def _source_state(core: Any) -> dict[str, Any]:
    state = resilience._load(core)
    # _freeze_sources is deterministic and uses only provider/database capability state.
    # It does not look at future prices or labels. If providers are already settled,
    # this restores a missing in-memory view from persistent source state.
    if not state.get('source_set_frozen'):
        try:
            state = resilience._freeze_sources(core)
        except Exception as exc:
            state = {**state, 'freeze_restore_error': f'{type(exc).__name__}: {exc}'}
    return state


def _archive_unsafe_champions(core: Any, legal_frontier_ts: int) -> dict[str, Any]:
    """Prevent old-version Champions from bypassing the current safety contract.

    Historical samples are preserved. Only registry rows that cannot prove current
    OOS/anti-overfit/untouched-audit requirements are archived and may be recertified.
    """
    con = core.db()
    archived_signal: list[str] = []
    archived_execution: list[str] = []
    try:
        if _table_exists(con, 'model_registry'):
            rows = con.execute("SELECT strategy,direction,version,metrics FROM model_registry WHERE status='CHAMPION'").fetchall()
            for row in rows:
                strategy, direction, version = str(row[0]), str(row[1]), int(row[2])
                meta = _load_json(row[3])
                trained_through = int(meta.get('trained_through_ts') or 0)
                current_contract = bool(
                    direction in ('LONG', 'SHORT') and int(meta.get('schema_version') or 0) >= 4 and
                    meta.get('overfit_guard_passed') is True and
                    int(meta.get('effective_oos_selected_n') or meta.get('selected_n') or 0) >= 60 and
                    float(meta.get('clustered_ev_bootstrap_05') or -9.0) > 0 and
                    (trained_through <= 0 or trained_through <= int(legal_frontier_ts or 0))
                )
                if current_contract:
                    continue
                con.execute("UPDATE model_registry SET status='ARCHIVED' WHERE strategy=? AND direction=? AND version=?", (strategy, direction, version))
                archived_signal.append(f'{strategy}|{direction}|v{version}')
        current_signal_versions: set[tuple[str, str, int]] = set()
        if _table_exists(con, 'model_registry'):
            for row in con.execute("SELECT strategy,direction,version FROM model_registry WHERE status='CHAMPION'").fetchall():
                current_signal_versions.add((str(row[0]), str(row[1]), int(row[2])))
        if _table_exists(con, 'execution_registry_v7'):
            rows = con.execute("SELECT strategy,direction,model_version,version,metrics FROM execution_registry_v7 WHERE status='CHAMPION'").fetchall()
            for row in rows:
                strategy, direction, model_version, version = str(row[0]), str(row[1]), int(row[2]), int(row[3])
                meta = _load_json(row[4])
                method = str(meta.get('validation_method') or '')
                current_contract = bool(
                    (strategy, direction, model_version) in current_signal_versions and
                    meta.get('certified') is True and 'UNTOUCHED_AUDIT' in method and
                    not bool(meta.get('suspicious_metrics')) and
                    float(meta.get('ev_bootstrap_05') or -9.0) > 0 and
                    int(meta.get('oos_fills') or 0) >= execution_v7.MIN_AUDIT_FILLS
                )
                if current_contract:
                    continue
                con.execute("UPDATE execution_registry_v7 SET status='ARCHIVED' WHERE strategy=? AND direction=? AND model_version=? AND version=?", (strategy, direction, model_version, version))
                archived_execution.append(f'{strategy}|{direction}|signal-v{model_version}|exec-v{version}')
        con.commit()
    finally:
        con.close()
    if archived_signal:
        core.set_state('v7_execution_signal_signature', [])
    return {'signal': archived_signal, 'execution': archived_execution}


def _audit_fingerprint(audit: dict[str, Any]) -> str:
    payload = {
        'status': audit.get('status'), 'reason': audit.get('reason'),
        'orphan': audit.get('orphan_feature_refs'), 'partial': audit.get('partial_decision_timestamps'),
        'future': audit.get('future_sample_rows'), 'invalid': audit.get('invalid_strategy_direction_rows'),
        'non_normalized': audit.get('non_normalized_feature_refs'), 'source_provenance': audit.get('source_provenance_ok'),
    }
    return hashlib.sha256(json.dumps(payload, sort_keys=True, ensure_ascii=False).encode('utf-8')).hexdigest()[:20]


def final_audit(core: Any, *, allow_auto_rebuild: bool = True) -> dict[str, Any]:
    snap = _db_snapshot(core)
    marker_repairs = _restore_schema_markers_if_safe(core, snap)
    cursor_restore = _restore_cursor_from_samples(core, snap)
    source = _source_state(core)
    base = _ORIGINAL_CERT17_AUDIT(core, allow_auto_rebuild=False)
    replay = runtime_integrity.replay_progress(core)
    baseline = _baseline(core)
    source_provenance_ok = bool(source.get('source_set_frozen')) or int(snap.get('learning_samples') or 0) == 0
    extra_reasons: list[str] = []
    if not source_provenance_ok:
        extra_reasons.append('historical samples exist but the frozen derivative feature-generation contract cannot be recovered')
    if int(snap.get('learning_samples') or 0) > 0 and int(snap.get('sample_max_ts') or 0) > int(replay.get('legal_frontier_ts') or core.START_TS):
        extra_reasons.append('learning samples extend beyond the current legally labelable frontier')
    baseline_ok = bool(baseline.get('clean') is True and baseline.get('status') == 'CLEAN' and baseline.get('dataset_id'))
    valid = bool(base.get('valid') and source_provenance_ok and baseline_ok and not extra_reasons)
    hard = bool(base.get('hard_failure') or extra_reasons)
    result = {
        **base,
        'runtime': VERSION,
        'schema': SCHEMA,
        'status': 'VALID' if valid else ('FAILED' if hard else str(base.get('status') or 'WAITING')),
        'valid': valid,
        'hard_failure': hard,
        'reason': '; '.join([str(base.get('reason') or '')] + extra_reasons).strip('; '),
        'checked_at': int(time.time()),
        'dataset_id': baseline.get('dataset_id'),
        'baseline_clean': baseline_ok,
        'source_provenance_ok': source_provenance_ok,
        'source_set_frozen': bool(source.get('source_set_frozen')),
        'model_oi_sources': list(source.get('model_oi_sources') or []),
        'model_funding_sources': list(source.get('model_funding_sources') or []),
        'model_enrichment_sources': list(source.get('model_enrichment_sources') or []),
        'oi_mode': source.get('oi_mode'), 'funding_mode': source.get('funding_mode'),
        'enrichment_mode': source.get('enrichment_mode'),
        'marker_repairs': marker_repairs, 'cursor_restore': cursor_restore,
        'db_snapshot': {k: v for k, v in snap.items() if k != 'tables'},
        'replay': replay,
        'automatic_raw_data_deletion': False,
    }

    if valid or not hard:
        core.set_state(FAILURE_KEY, {})
    elif allow_auto_rebuild and baseline_ok:
        fingerprint = _audit_fingerprint(result)
        prev = core.get_state(FAILURE_KEY, None)
        prev = dict(prev) if isinstance(prev, dict) else {}
        now = int(time.time())
        same = prev.get('fingerprint') == fingerprint
        failure = {
            'fingerprint': fingerprint,
            'first_seen_at': int(prev.get('first_seen_at') or now) if same else now,
            'last_seen_at': now,
            'consecutive': int(prev.get('consecutive') or 0) + 1 if same else 1,
            'reason': result.get('reason'),
        }
        core.set_state(FAILURE_KEY, failure)
        confirmed = bool(
            failure['consecutive'] >= AUDIT_CONFIRM_COUNT and
            now - int(failure['first_seen_at']) >= AUDIT_CONFIRM_SECONDS
        )
        result['failure_confirmation'] = failure
        if confirmed:
            cursor_guard._reset_derived_replay(core, '9.0 final derived audit confirmed structural corruption: ' + str(result.get('reason') or 'unknown'))
            core.set_state(cert17.STATE_KEY, {
                'version': VERSION, 'status': 'NOT_STARTED', 'attempts': 0,
                'last_sample_total': 0, 'last_sample_max_ts': None,
                'last_attempt_at': 0, 'last_completed_at': 0, 'results': [],
            })
            core.set_state(STATE_KEY, {
                'version': VERSION, 'status': 'DERIVED_REBUILDING', 'updated_at': now,
                'reason': result.get('reason'), 'raw_market_preserved': True,
                'raw_derivatives_preserved': True, 'clean_dataset_id_preserved': True,
            })
            core.set_state(FAILURE_KEY, {})
            result['status'] = 'AUTO_REBUILDING_DERIVED_ONLY'
            result['valid'] = False
            result['auto_rebuild_applied'] = True
            result['raw_market_preserved'] = True
            result['raw_derivatives_preserved'] = True
            result['dataset_id_preserved'] = True
        else:
            result['status'] = 'HARD_FAILURE_PENDING_CONFIRMATION'
            result['reason'] += f" | destructive derived reset is intentionally delayed until the same failure is confirmed {AUDIT_CONFIRM_COUNT} times across >= {AUDIT_CONFIRM_SECONDS}s"

    core.set_state(AUDIT_KEY, result)
    core.state['final_dataset_audit'] = result
    return result


def _champions(core: Any) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    con = core.db()
    try:
        sig: list[dict[str, Any]] = []
        if _table_exists(con, 'model_registry'):
            for row in con.execute("SELECT strategy,direction,version,created_at,metrics FROM model_registry WHERE status='CHAMPION' ORDER BY strategy,direction,version DESC").fetchall():
                meta = _load_json(row[4])
                sig.append({'strategy': str(row[0]), 'direction': str(row[1]), 'version': int(row[2]), 'created_at': int(row[3]), **meta})
        exe: list[dict[str, Any]] = []
        if _table_exists(con, 'execution_registry_v7'):
            for row in con.execute("SELECT strategy,direction,model_version,version,created_at,metrics,policy FROM execution_registry_v7 WHERE status='CHAMPION' ORDER BY strategy,direction,version DESC").fetchall():
                exe.append({
                    'strategy': str(row[0]), 'direction': str(row[1]), 'model_version': int(row[2]),
                    'execution_version': int(row[3]), 'created_at': int(row[4]),
                    'metrics': _load_json(row[5]), 'policy': _load_json(row[6]),
                })
        return sig, exe
    finally:
        con.close()


def regime_portfolio(core: Any) -> dict[str, Any]:
    signal_rows, execution_rows = _champions(core)
    execution_map = {(x['strategy'], x['direction'], x['model_version']): x for x in execution_rows}
    specialists: dict[str, list[dict[str, Any]]] = {}
    for row in signal_rows:
        key = (row['strategy'], row['direction'], int(row['version']))
        ex = execution_map.get(key)
        if not ex:
            continue
        em = ex.get('metrics') or {}
        if not em.get('certified') or em.get('suspicious_metrics'):
            continue
        signal_ev = float(row.get('expectancy_r') or 0)
        signal_pf = float(row.get('profit_factor') or 0)
        stability = float(row.get('stability') or 0)
        exec_ev = float(em.get('expectancy_r') or 0)
        exec_pf = float(em.get('profit_factor') or 0)
        score = signal_ev * 2.5 + exec_ev * 3.0 + max(0.0, min(signal_pf, 3.0) - 1.0) * .12 + max(0.0, min(exec_pf, 3.0) - 1.0) * .10 + stability * .08
        blocked = set(em.get('blocked_regimes') or [])
        for regime in row.get('allowed_regimes') or []:
            if regime in blocked:
                continue
            specialists.setdefault(str(regime), []).append({
                'strategy': row['strategy'], 'direction': row['direction'],
                'signal_version': row['version'], 'execution_version': ex['execution_version'],
                'signal_pf': signal_pf, 'signal_ev_r': signal_ev, 'execution_pf': exec_pf,
                'execution_ev_r': exec_ev, 'stability': stability,
                'threshold': float(row.get('threshold') or 0), 'score': score,
                'genome_id': row.get('genome_id'), 'policy': ex.get('policy') or {},
            })
    chosen: dict[str, Any] = {}
    for regime, rows in specialists.items():
        rows.sort(key=lambda x: float(x.get('score') or 0), reverse=True)
        chosen[regime] = {'primary': rows[0], 'alternatives': rows[1:4]}
    return {
        'runtime': VERSION, 'router': 'CURRENT_REGIME -> certified Signal specialist -> matching untouched-audit Execution policy',
        'regimes': chosen, 'signal_champions': len(signal_rows), 'execution_champions': len(execution_rows),
        'rules': {
            'single_global_strategy_for_all_markets': False,
            'regime_specific_allowed_sets': True,
            'execution_policy_must_match_signal_model_version': True,
            'blocked_execution_regimes_never_trade': True,
        },
    }


def _final_state(core: Any) -> dict[str, Any]:
    raw = core.get_state(STATE_KEY, None)
    state = dict(raw) if isinstance(raw, dict) else {}
    state.setdefault('version', VERSION)
    state.setdefault('status', 'BOOTING')
    return state


def _certification_due(core: Any, snap: dict[str, Any], force: bool) -> bool:
    if force:
        return True
    state = _final_state(core)
    now = int(time.time())
    total = int(snap.get('learning_samples') or 0)
    max_ts = int(snap.get('sample_max_ts') or 0)
    last_total = int(state.get('last_cert_sample_total') or 0)
    last_max = int(state.get('last_cert_sample_max_ts') or 0)
    last_at = int(state.get('last_cert_completed_at') or 0)
    if state.get('status') in ('BOOTING', 'READY_FOR_SIGNAL_CERTIFICATION', 'SIGNAL_CERTIFICATION_FAILED', 'NO_SIGNAL_MODEL_PASSED_OOS') and last_at <= 0:
        return True
    if total - last_total >= RECERTIFY_MIN_NEW_SAMPLES:
        return True
    if max_ts > last_max and now - last_at >= RECERTIFY_SECONDS:
        return True
    signal_rows, _ = _champions(core)
    if not signal_rows and total > 0 and now - last_at >= 3600:
        return True
    return False


def _execution_needed(core: Any) -> bool:
    signature = [tuple(x) for x in v7_runtime._champion_signature(core)]
    if not signature:
        return False
    _, execution_rows = _champions(core)
    have = {(x['strategy'], x['direction'], int(x['model_version'])) for x in execution_rows}
    return any((str(s), str(d), int(v)) not in have for s, d, v in signature)


def certify_and_execute(core: Any, force: bool = False) -> list[dict[str, Any]]:
    with _CERT_LOCK:
        snap = _db_snapshot(core)
        _restore_schema_markers_if_safe(core, snap)
        _restore_cursor_from_samples(core, snap)
        replay = runtime_integrity.replay_progress(core)
        audit = final_audit(core, allow_auto_rebuild=True)
        pipe = runtime_integrity.certification_pipeline(core)
        now = int(time.time())
        state = _final_state(core)

        if not replay.get('complete'):
            state.update({'version': VERSION, 'status': 'STRICT_REPLAY_ADVANCING', 'reason': pipe.get('reason'), 'updated_at': now})
            core.set_state(STATE_KEY, state)
            return []
        if not audit.get('valid'):
            state.update({'version': VERSION, 'status': str(audit.get('status') or 'WAITING_DATA_AUDIT'), 'reason': audit.get('reason'), 'updated_at': now})
            core.set_state(STATE_KEY, state)
            return []
        if not pipe.get('signal_training_ready'):
            state.update({'version': VERSION, 'status': str(pipe.get('stage') or 'WAITING_CERTIFICATION_GATE'), 'reason': pipe.get('reason'), 'updated_at': now})
            core.set_state(STATE_KEY, state)
            return []

        registry_cleanup = _archive_unsafe_champions(core, int(replay.get('legal_frontier_ts') or core.START_TS))
        snap = _db_snapshot(core)
        if not _certification_due(core, snap, force) and not _execution_needed(core):
            sig, exe = runtime_integrity._champion_counts(core)
            state.update({
                'version': VERSION,
                'status': 'FULLY_OPERATIONAL' if sig > 0 and exe > 0 else 'WAITING_EXECUTION_AUDIT' if sig > 0 else 'NO_SIGNAL_MODEL_PASSED_OOS',
                'reason': 'certification is current for the latest matured sample generation',
                'updated_at': now, 'signal_champions': sig, 'execution_champions': exe,
                'registry_cleanup': registry_cleanup,
            })
            core.set_state(STATE_KEY, state)
            return []

        state.update({
            'version': VERSION, 'status': 'SIGNAL_CERTIFICATION_RUNNING',
            'reason': 'regime-specialist populations are evolving across all strategy directions; only one never-before-seen chronological holdout may certify each lineage',
            'started_at': now, 'updated_at': now, 'sample_total_at_start': int(snap.get('learning_samples') or 0),
            'sample_max_ts_at_start': snap.get('sample_max_ts'), 'registry_cleanup': registry_cleanup,
        })
        core.set_state(STATE_KEY, state)
        core.state.setdefault('learning', {})['phase'] = 'SIGNAL_CERTIFICATION_RUNNING'

        # Force means "run this eligible certification now". The learner itself still
        # refuses to reopen a previously consumed holdout.
        results = list(_ORIGINAL_CERT17_TRAIN(core, True) or [])
        sig_count, _ = runtime_integrity._champion_counts(core)
        execution_results: list[dict[str, Any]] = []
        if sig_count > 0:
            execution_results = list(execution_v7.optimize_all(core, False) or [])
            core.set_state('v7_execution_signal_signature', [list(x) for x in v7_runtime._champion_signature(core)])
            core.set_state('v7_execution_last_attempt_ts', int(time.time()))
            core.state['execution_learning'] = {
                'version': VERSION, 'results': execution_results,
                'registry': v7_runtime._execution_status(core)[:100],
                'updated_at': int(time.time()), 'reason': 'Signal certification completed -> immediate matching Execution walk-forward audit',
            }
        sig_count, exe_count = runtime_integrity._champion_counts(core)
        promoted = [x for x in results if x.get('promoted')]
        rejected = [x for x in results if not x.get('promoted') and x.get('status') not in (
            'WAITING_NEW_UNTOUCHED_HOLDOUT', 'ABSOLUTE_PASS_INCUMBENT_HELD', 'ERROR',
        )]
        waiting = [x for x in results if x.get('status') == 'WAITING_NEW_UNTOUCHED_HOLDOUT']
        incumbent_held = [x for x in results if x.get('status') == 'ABSOLUTE_PASS_INCUMBENT_HELD']
        evolved = sum(int(x.get('candidates_evaluated') or 0) for x in results)
        exec_promoted = [x for x in execution_results if x.get('status') == 'CHAMPION']
        exec_rejected = [x for x in execution_results if x.get('status') not in ('CHAMPION', 'UNCHANGED')]
        if sig_count <= 0:
            status = 'NO_SIGNAL_MODEL_PASSED_OOS'
            reason = f'0 Signal Champions after {evolved} evolved genomes; holdout rejected={len(rejected)}, awaiting genuinely new holdout={len(waiting)}, incumbent-held={len(incumbent_held)}'
        elif exe_count <= 0:
            status = 'WAITING_EXECUTION_AUDIT'
            reason = f'{sig_count} Signal Champion(s) exist, but no Entry/SL/TP policy has passed the untouched execution audit yet'
        else:
            status = 'FULLY_OPERATIONAL'
            reason = f'{sig_count} Signal Champion(s) + {exe_count} matching Execution Champion(s) are certified; live signals still require current regime/risk/re-entry gates'
        completed = int(time.time())
        portfolio = regime_portfolio(core)
        state.update({
            'version': VERSION, 'status': status, 'reason': reason, 'updated_at': completed,
            'last_cert_completed_at': completed, 'last_cert_sample_total': int(snap.get('learning_samples') or 0),
            'last_cert_sample_max_ts': snap.get('sample_max_ts'), 'signal_champions': sig_count,
            'execution_champions': exe_count, 'signal_promoted': len(promoted), 'signal_rejected': len(rejected),
            'signal_waiting_new_holdout': len(waiting), 'signal_incumbent_held': len(incumbent_held),
            'signal_genomes_evaluated': evolved,
            'execution_promoted': len(exec_promoted), 'execution_rejected': len(exec_rejected),
            'signal_results': results, 'execution_results': execution_results,
            'regime_portfolio': portfolio,
        })
        core.set_state(STATE_KEY, state)
        core.state['final_system'] = state
        core.state['v18_pending_notice'] = {
            'at': completed, 'status': status, 'reason': reason,
            'signal_promoted': len(promoted), 'signal_rejected': len(rejected),
            'signal_waiting_new_holdout': len(waiting), 'signal_incumbent_held': len(incumbent_held),
            'signal_genomes_evaluated': evolved,
            'execution_promoted': len(exec_promoted), 'execution_rejected': len(exec_rejected),
            'signal_champions': sig_count, 'execution_champions': exe_count,
        }
        return results


def _authoritative_view(core: Any) -> dict[str, Any]:
    snap = _db_snapshot(core)
    cursor_restore = _restore_cursor_from_samples(core, snap)
    replay = runtime_integrity.replay_progress(core)
    audit = final_audit(core, allow_auto_rebuild=False)
    source = _source_state(core)
    state = _final_state(core)
    sig, exe = runtime_integrity._champion_counts(core)
    discord_state = core.state.get('discord') if isinstance(core.state.get('discord'), dict) else {}
    webhook = bool(os.getenv('DISCORD_WEBHOOK_URL', '') or getattr(core, 'DISCORD_WEBHOOK_URL', ''))
    bot = bool(os.getenv('DISCORD_BOT_TOKEN', '') or getattr(core, 'DISCORD_BOT_TOKEN', ''))
    channel = bool(os.getenv('DISCORD_CHANNEL_ID', '') or getattr(core, 'DISCORD_CHANNEL_ID', ''))
    portfolio = regime_portfolio(core)
    view = {
        'runtime': VERSION, 'status': state.get('status'), 'reason': state.get('reason'),
        'dataset': {'baseline': _baseline(core), 'audit': audit},
        'replay': replay, 'cursor_restore': cursor_restore,
        'samples': {
            'rows': int(snap.get('learning_samples') or 0), 'decision_timestamps': int(snap.get('decision_timestamps') or 0),
            'feature_snapshots': int(snap.get('feature_snapshots') or 0), 'partial_decisions': int(snap.get('partial_decision_timestamps') or 0),
            'min_ts': snap.get('sample_min_ts'), 'max_ts': snap.get('sample_max_ts'),
            'rows_per_decision': EXPECTED_ROWS_PER_DECISION,
            'full_span_model_store': True, 'model_cap_per_strategy_direction': v9_training_store.MODEL_MAX_ROWS,
            'dense_recent_rows': v9_training_store.MODEL_RECENT_ROWS,
        },
        'source_contract': {
            'frozen': bool(source.get('source_set_frozen')), 'oi_mode': source.get('oi_mode'),
            'funding_mode': source.get('funding_mode'), 'enrichment_mode': source.get('enrichment_mode'),
            'oi_sources': list(source.get('model_oi_sources') or []),
            'funding_sources': list(source.get('model_funding_sources') or []),
            'enrichment_sources': list(source.get('model_enrichment_sources') or []),
            'effective_model_start': source.get('effective_model_start'),
        },
        'certification': {
            **state, 'signal_champions': sig, 'execution_champions': exe,
            'strategies': list(v5_runtime.STRATEGIES), 'directions': list(v5_runtime.DIRECTIONS),
            'genomes': [g['id'] for g in v8_evolution.GENOMES],
        },
        'regime_portfolio': portfolio,
        'live_learning': {
            'live_execution_samples': int(snap.get('live_execution_samples') or 0),
            'deployment_ledger_rows': int(snap.get('ledger_rows') or 0),
            'deployment_closed': int(snap.get('ledger_closed') or 0),
            'deployment_net_r': float(snap.get('ledger_net_r') or 0),
            'pending_execution_reaudit_evidence': int(core.get_state('evolution_live_evidence_pending', 0) or 0),
            'direct_live_outcome_can_mutate_signal_label': False,
            'post_exit_review_enabled': True,
        },
        'discord': {
            'configured': bool(webhook or (bot and channel)), 'runtime_state': discord_state,
            'trade_lifecycle_notifications': True, 'champion_notifications': True,
            'daily_learning_report': True, 'post_exit_review_notifications': True,
        },
        'safety_contract': {
            'future_price_features': False,
            'historical_decision_only_uses_information_available_at_decision_time': True,
            'future_5m_bars_are_labels_only_not_features': True,
            'htf_close_time_required': True,
            'overlapping_8h_labels_clustered_for_oos': True,
            'nested_train_calibration_oos': True,
            'execution_untouched_audit_required': True,
            'raw_data_auto_deleted_on_runtime_state_loss': False,
            'single_bad_trade_directly_changes_signal_model': False,
            'live_signal_requires_signal_and_execution_champion': True,
            'no_guaranteed_profit_claim': True,
        },
    }
    lr = core.state.setdefault('learning', {})
    lr['replay_learning_progress'] = replay
    lr['final_dataset_audit'] = audit
    lr['final_system'] = state
    lr['certification_pipeline'] = {
        'stage': state.get('status'), 'reason': state.get('reason'),
        'signal_champions': sig, 'execution_champions': exe,
        'learning_samples': int(snap.get('learning_samples') or 0),
        'replay_complete': bool(replay.get('complete')),
        'signal_training_ready': bool(replay.get('complete') and audit.get('valid')),
    }
    lr['phase'] = str(state.get('status') or lr.get('phase') or 'BOOTING')
    lr['data_resilience'] = source
    lr['derivative_backfill'] = {
        **(lr.get('derivative_backfill') or {}),
        'core_frozen': bool(source.get('source_set_frozen')),
        'frozen_core_oi': list(source.get('model_oi_sources') or []),
        'frozen_core_funding': list(source.get('model_funding_sources') or []),
        'frozen_enrichment': list(source.get('model_enrichment_sources') or []),
    }
    core.state['learning_sample_total'] = int(snap.get('learning_samples') or 0)
    core.state['final_system_view'] = view
    return view


def _final_live_gate(core: Any, original_create: Any, analysis: dict[str, Any], m15: list[dict[str, Any]]):
    view = _authoritative_view(core)
    if view.get('status') != 'FULLY_OPERATIONAL':
        analysis['final_authority_gate'] = {'allowed': False, 'reason': f"final system status is {view.get('status')}"}
        return None
    if not ((view.get('dataset') or {}).get('audit') or {}).get('valid'):
        analysis['final_authority_gate'] = {'allowed': False, 'reason': 'final dataset audit is not valid'}
        return None
    selection = analysis.get('selection') or {}
    model_meta = ((selection.get('model') or {}).get('metrics') or {})
    execution = selection.get('execution') or {}
    exec_meta = execution.get('metrics') or {}
    if model_meta.get('overfit_guard_passed') is not True:
        analysis['final_authority_gate'] = {'allowed': False, 'reason': 'Signal Champion does not prove the final clustered anti-overfit contract'}
        return None
    if not execution.get('certified') or 'UNTOUCHED_AUDIT' not in str(exec_meta.get('validation_method') or ''):
        analysis['final_authority_gate'] = {'allowed': False, 'reason': 'matching Entry/SL/TP Execution Champion has not passed untouched audit'}
        return None
    row = original_create(analysis, m15)
    if not row:
        return None
    payload = row.get('payload') or {}
    baseline = _baseline(core)
    policy = payload.get('execution_policy') or {}
    policy_hash = hashlib.sha256(json.dumps(policy, sort_keys=True, ensure_ascii=False).encode('utf-8')).hexdigest()[:16]
    payload['final_authority'] = {
        'runtime': VERSION, 'dataset_id': baseline.get('dataset_id'),
        'signal_model_version': (selection.get('model') or {}).get('model_version'),
        'execution_version': (payload.get('execution_validation') or {}).get('execution_version'),
        'genome_id': model_meta.get('genome_id'), 'policy_hash': policy_hash,
        'created_at': int(time.time()), 'immutable_model_policy_binding': True,
    }
    con = core.db()
    try:
        con.execute('UPDATE signals SET payload=? WHERE signal_id=?', (json.dumps(payload, ensure_ascii=False), row['signal_id']))
        con.commit()
    finally:
        con.close()
    return v7_runtime._signal_by_id(core, row['signal_id']) or row


async def _send_pending_notice(core: Any) -> None:
    notice = core.state.get('v18_pending_notice')
    if not isinstance(notice, dict) or not notice.get('at'):
        return
    sent = int(core.get_state('v18_last_notice_at', 0) or 0)
    if int(notice['at']) <= sent:
        return
    body = (
        f"狀態 `{notice.get('status')}`\n{notice.get('reason')}\n"
        f"Signal：本輪進化 `{notice.get('signal_genomes_evaluated',0)}` 個 genome｜升級 `{notice.get('signal_promoted',0)}`｜新 holdout 未通過 `{notice.get('signal_rejected',0)}`｜等待新資料 `{notice.get('signal_waiting_new_holdout',0)}`｜舊 Champion 勝出 `{notice.get('signal_incumbent_held',0)}`｜Champion `{notice.get('signal_champions',0)}`\n"
        f"Execution：通過 `{notice.get('execution_promoted',0)}`｜淘汰 `{notice.get('execution_rejected',0)}`｜Champion `{notice.get('execution_champions',0)}`\n"
        '策略只在 development 歷史內演化；每個 lineage 的 sealed holdout 只開一次。所有 Entry/SL/TP 仍需 matching untouched execution audit。'
    )
    ok = await v5_runtime.robust_send_discord(core, '🧠 ETH Final Strategy Certification', body, 0x57F287)
    if ok:
        core.set_state('v18_last_notice_at', int(notice['at']))


async def _final_boot_notice(core: Any) -> None:
    if core.get_state('v18_boot_notice_version') == VERSION:
        return
    view = _authoritative_view(core)
    body = (
        f"Dataset `{((view.get('dataset') or {}).get('baseline') or {}).get('status')}`｜Audit `{((view.get('dataset') or {}).get('audit') or {}).get('status')}`\n"
        f"Replay `{float((view.get('replay') or {}).get('percent') or 0):.2f}%`｜Samples `{int((view.get('samples') or {}).get('rows') or 0):,}`\n"
        'Final Authority 已啟用：SQLite truth recovery、no-lookahead replay、Signal genome OOS、Execution untouched audit、live drift/post-exit learning、單一路徑 fail-closed。'
    )
    if await v5_runtime.robust_send_discord(core, '✅ ETH Adaptive AI 10.0 Hierarchical Final Authority 已啟動', body, 0x3498DB):
        core.set_state('v18_boot_notice_version', VERSION)


def install(core: Any) -> None:
    # Final audit authority. A single transient/memory-state mismatch may never delete
    # replay-derived data. Structural corruption must be repeated and confirmed first.
    cert17.audit_derived_dataset = lambda c, allow_auto_rebuild=True: final_audit(c, allow_auto_rebuild=allow_auto_rebuild)

    # Recover persistent truth before the first worker tick. This directly addresses
    # deployments where in-memory learning state was empty although SQLite was intact.
    initial_snap = _db_snapshot(core)
    _restore_schema_markers_if_safe(core, initial_snap)
    _restore_cursor_from_samples(core, initial_snap)
    initial_replay = runtime_integrity.replay_progress(core)
    cleanup = _archive_unsafe_champions(core, int(initial_replay.get('legal_frontier_ts') or core.START_TS))
    initial_audit = final_audit(core, allow_auto_rebuild=False)
    initial_state = _final_state(core)
    initial_state.update({
        'version': VERSION, 'updated_at': int(time.time()),
        'status': 'READY_FOR_SIGNAL_CERTIFICATION' if initial_replay.get('complete') and initial_audit.get('valid') else str(initial_state.get('status') or 'BOOTING'),
        'reason': 'persistent SQLite state recovered; awaiting/performing final certification' if initial_replay.get('complete') and initial_audit.get('valid') else str(initial_state.get('reason') or 'initializing'),
        'registry_cleanup_on_boot': cleanup,
    })
    core.set_state(STATE_KEY, initial_state)

    # Every legacy automatic/manual Signal training path now resolves to the same final
    # function, and matching Execution certification happens in the same call.
    def final_train(c: Any, force: bool = False):
        return certify_and_execute(c, force)
    cert17.train_v17 = final_train
    v5_runtime.train_v5 = final_train
    core.train_if_due = lambda force=False: certify_and_execute(core, force)

    original_learning = core.learning_tick
    async def final_learning_tick() -> None:
        _authoritative_view(core)
        await original_learning()
        view = _authoritative_view(core)
        if bool((view.get('replay') or {}).get('complete')) and bool(((view.get('dataset') or {}).get('audit') or {}).get('valid')):
            await asyncio.to_thread(certify_and_execute, core, False)
            _authoritative_view(core)
        await _send_pending_notice(core)
    core.learning_tick = final_learning_tick

    # The live signal entrance remains the composed v7/v8/v9/v12/v16 chain, with one
    # final proof check for current Signal anti-overfit + matching Execution audit.
    original_create = core.create_signal
    core.create_signal = lambda analysis, m15: _final_live_gate(core, original_create, analysis, m15)

    strict = core.state.setdefault('strict_replay', {})
    strict['final_authority'] = {
        'runtime': VERSION, 'schema': SCHEMA,
        'sqlite_is_authoritative_after_redeploy': True,
        'single_runtime_state_loss_can_reset_derived_data': False,
        'confirmed_structural_corruption_rebuilds_derived_only': True,
        'raw_market_preserved': True, 'raw_derivatives_preserved': True,
        'clean_dataset_id_preserved': True,
        'all_configured_strategy_directions_are_population_entrypoints': True,
        'signal_genome_evolution': [g['id'] for g in v8_evolution.GENOMES],
        'full_span_training_store': True,
        'model_max_rows_per_strategy_direction': v9_training_store.MODEL_MAX_ROWS,
        'old_history_policy': 'deterministic full-span temporal decimation plus dense recent block',
        'regime_specialist_router': True,
        'execution_policy_evolution': True,
        'execution_untouched_audit_required': True,
        'post_exit_live_evidence_separate_from_signal_labels': True,
        'future_price_features': False,
        'future_5m_path_is_label_only': True,
        'single_composed_live_signal_entrance': True,
        'profit_guarantee': False,
    }
    core.state['runtime_version'] = VERSION
    core.app.version = '10.0.0'
    _authoritative_view(core)

    if not any(getattr(r, 'path', None) == '/api/v18/final-status' for r in core.app.router.routes):
        @core.app.get('/api/v18/final-status')
        def final_status() -> dict[str, Any]:
            return _authoritative_view(core)

    if not any(getattr(r, 'path', None) == '/api/v18/certify' for r in core.app.router.routes):
        @core.app.post('/api/v18/certify')
        async def final_certify() -> dict[str, Any]:
            results = await asyncio.to_thread(certify_and_execute, core, True)
            await _send_pending_notice(core)
            return {'runtime': VERSION, 'results': results, 'status': _authoritative_view(core)}

    # Boot notice is issued by the lifespan worker on the first learning tick. Expose
    # the coroutine for server_v18 tests and for a safe explicit call if needed.
    core.final_boot_notice = lambda: _final_boot_notice(core)
