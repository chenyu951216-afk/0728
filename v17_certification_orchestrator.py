from __future__ import annotations

import json
import os
import time
from typing import Any

import v5_runtime
import v12_clean_baseline
import v13_replay_cursor_integrity as cursor_guard
import v15_data_resilience as resilience
import v16_runtime_integrity as runtime_integrity

VERSION = '8.4.1-20260810'
AUDIT_SCHEMA = 1
STATE_KEY = 'v17_certification_state'
AUDIT_KEY = 'v17_derived_dataset_audit'
MIN_NEW_SAMPLES = max(140, int(os.getenv('SIGNAL_RECERTIFY_MIN_NEW_SAMPLES', '1400')))
RECERTIFY_SECONDS = max(6 * 3600, int(os.getenv('SIGNAL_RECERTIFY_SECONDS', str(24 * 3600))))
EXPECTED_PER_DECISION = len(v5_runtime.STRATEGIES) * len(v5_runtime.DIRECTIONS)


def _load_state(core: Any) -> dict[str, Any]:
    raw = core.get_state(STATE_KEY, None)
    return dict(raw) if isinstance(raw, dict) else {
        'version': VERSION,
        'status': 'NOT_STARTED',
        'attempts': 0,
        'last_sample_total': 0,
        'last_sample_max_ts': None,
        'last_attempt_at': 0,
        'last_completed_at': 0,
        'results': [],
    }


def _save_state(core: Any, state: dict[str, Any]) -> None:
    state['version'] = VERSION
    core.set_state(STATE_KEY, state)
    core.state['certification_orchestrator'] = state


def _baseline(core: Any) -> dict[str, Any]:
    raw = core.get_state(v12_clean_baseline.STATE_KEY, None)
    return dict(raw) if isinstance(raw, dict) else {}


def _sample_signature(core: Any) -> dict[str, Any]:
    con = core.db()
    try:
        row = con.execute('SELECT COUNT(*),MIN(ts),MAX(ts),COUNT(DISTINCT ts) FROM learning_samples').fetchone()
    finally:
        con.close()
    return {
        'total': int(row[0] or 0),
        'min_ts': int(row[1]) if row[1] is not None else None,
        'max_ts': int(row[2]) if row[2] is not None else None,
        'decision_timestamps': int(row[3] or 0),
    }


def audit_derived_dataset(core: Any, *, allow_auto_rebuild: bool = True) -> dict[str, Any]:
    """Verify that cross-version derived learning state is structurally safe.

    This audit never deletes raw market bars, raw derivatives or the CLEAN Dataset ID.
    A hard structural failure may reset only replay-derived samples/certifications so
    the current final runtime can rebuild them deterministically from preserved raw data.
    """
    baseline = _baseline(core)
    replay = runtime_integrity.replay_progress(core)
    frontier = int(replay.get('legal_frontier_ts') or core.START_TS)
    source_state = resilience._load(core)
    effective_start = int(source_state.get('effective_model_start') or core.START_TS)

    con = core.db()
    try:
        tables = {str(r[0]) for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
        if 'learning_samples' not in tables or 'learning_feature_snapshots' not in tables:
            result = {
                'version': VERSION, 'schema': AUDIT_SCHEMA, 'status': 'MISSING_DERIVED_TABLES',
                'valid': False, 'hard_failure': True, 'checked_at': int(time.time()),
                'reason': 'required learning_samples / learning_feature_snapshots tables are missing',
            }
        else:
            row = con.execute('SELECT COUNT(*),MIN(ts),MAX(ts),COUNT(DISTINCT ts) FROM learning_samples').fetchone()
            total = int(row[0] or 0); min_ts = int(row[1]) if row[1] is not None else None; max_ts = int(row[2]) if row[2] is not None else None; decisions = int(row[3] or 0)
            snapshots = int(con.execute('SELECT COUNT(*) FROM learning_feature_snapshots').fetchone()[0] or 0)
            orphan_refs = int(con.execute('''SELECT COUNT(*) FROM learning_samples ls
                LEFT JOIN learning_feature_snapshots fs ON fs.ts=ls.ts
                WHERE ls.features LIKE '@%' AND fs.ts IS NULL''').fetchone()[0] or 0)
            non_refs = int(con.execute("SELECT COUNT(*) FROM learning_samples WHERE features <> ('@' || ts)").fetchone()[0] or 0)
            partial_decisions = int(con.execute('''SELECT COUNT(*) FROM (
                SELECT ts,COUNT(*) n FROM learning_samples GROUP BY ts HAVING n<>?
            )''', (EXPECTED_PER_DECISION,)).fetchone()[0] or 0)
            valid_strategies = tuple(v5_runtime.STRATEGIES); valid_directions = tuple(v5_runtime.DIRECTIONS)
            sp = ','.join('?' for _ in valid_strategies); dp = ','.join('?' for _ in valid_directions)
            invalid_pairs = int(con.execute(
                f'SELECT COUNT(*) FROM learning_samples WHERE strategy NOT IN ({sp}) OR direction NOT IN ({dp})',
                valid_strategies + valid_directions,
            ).fetchone()[0] or 0)
            future_samples = int(con.execute('SELECT COUNT(*) FROM learning_samples WHERE ts>?', (frontier,)).fetchone()[0] or 0) if frontier else 0
            pre_model_samples = int(con.execute('SELECT COUNT(*) FROM learning_samples WHERE ts<?', (effective_start,)).fetchone()[0] or 0) if total else 0
            snapshot_mismatch = abs(snapshots - decisions)
            schema_ok = bool(
                int(core.get_state('point_in_time_sample_schema', 0) or 0) == 6 and
                int(core.get_state('replay_cursor_integrity_schema', 0) or 0) == 2 and
                int(core.get_state('final_data_resilience_schema', 0) or 0) == 1
            )
            baseline_ok = bool(baseline.get('clean') is True and baseline.get('status') == 'CLEAN' and baseline.get('dataset_id'))
            hard_reasons: list[str] = []
            if not schema_ok: hard_reasons.append('derived schema markers do not match schema6/cursor2/resilience1')
            if not baseline_ok: hard_reasons.append('dataset provenance is not CLEAN')
            if orphan_refs: hard_reasons.append(f'{orphan_refs} sample rows have missing feature snapshots')
            if non_refs: hard_reasons.append(f'{non_refs} sample rows do not use normalized @timestamp feature references')
            if partial_decisions: hard_reasons.append(f'{partial_decisions} decision timestamps do not contain exactly {EXPECTED_PER_DECISION} strategy-direction rows')
            if invalid_pairs: hard_reasons.append(f'{invalid_pairs} samples use unknown strategy/direction values')
            if future_samples: hard_reasons.append(f'{future_samples} samples are beyond the latest legally labelable frontier')
            if snapshot_mismatch: hard_reasons.append(f'feature snapshot count differs from unique decision timestamps by {snapshot_mismatch}')
            if pre_model_samples: hard_reasons.append(f'{pre_model_samples} samples precede the frozen model feature start')

            if total == 0 and not replay.get('complete'):
                status = 'REBUILDING'
                valid = False
                hard = False
                reason = 'derived replay is rebuilding from preserved CLEAN raw data'
            elif total == 0 and replay.get('complete'):
                status = 'EMPTY_COMPLETED_REPLAY'
                valid = False
                hard = True
                reason = 'replay claims complete but no learning samples exist'
                hard_reasons.append(reason)
            elif hard_reasons:
                status = 'FAILED'
                valid = False
                hard = True
                reason = '; '.join(hard_reasons)
            else:
                status = 'VALID'
                valid = True
                hard = False
                reason = 'CLEAN schema6 derived samples are structurally consistent and safe to reuse'

            result = {
                'version': VERSION, 'schema': AUDIT_SCHEMA, 'status': status, 'valid': valid,
                'hard_failure': hard, 'checked_at': int(time.time()), 'reason': reason,
                'dataset_id': baseline.get('dataset_id'), 'baseline_clean': baseline_ok,
                'point_in_time_schema': int(core.get_state('point_in_time_sample_schema', 0) or 0),
                'cursor_integrity_schema': int(core.get_state('replay_cursor_integrity_schema', 0) or 0),
                'data_resilience_schema': int(core.get_state('final_data_resilience_schema', 0) or 0),
                'learning_samples': total, 'decision_timestamps': decisions,
                'feature_snapshots': snapshots, 'sample_min_ts': min_ts, 'sample_max_ts': max_ts,
                'legal_frontier_ts': frontier, 'effective_model_start': effective_start,
                'orphan_feature_refs': orphan_refs, 'non_normalized_feature_refs': non_refs,
                'partial_decision_timestamps': partial_decisions, 'invalid_strategy_direction_rows': invalid_pairs,
                'future_sample_rows': future_samples, 'pre_model_start_rows': pre_model_samples,
                'expected_rows_per_decision': EXPECTED_PER_DECISION,
            }
    finally:
        con.close()

    previous = core.get_state(AUDIT_KEY, None)
    already_reset = bool(isinstance(previous, dict) and previous.get('auto_rebuild_applied_for_dataset') == baseline.get('dataset_id'))
    if result.get('hard_failure') and allow_auto_rebuild and not already_reset and baseline.get('clean') is True:
        cursor_guard._reset_derived_replay(core, '8.4.1 derived-data audit failed: ' + str(result.get('reason') or 'unknown'))
        result['status'] = 'AUTO_REBUILDING_DERIVED_ONLY'
        result['valid'] = False
        result['auto_rebuild_applied'] = True
        result['auto_rebuild_applied_for_dataset'] = baseline.get('dataset_id')
        result['raw_market_preserved'] = True
        result['raw_derivatives_preserved'] = True
        result['dataset_id_preserved'] = True
    elif already_reset:
        result['auto_rebuild_applied_for_dataset'] = baseline.get('dataset_id')

    core.set_state(AUDIT_KEY, result)
    core.state['derived_dataset_audit'] = result
    return result


def _preflight_counts(core: Any) -> dict[str, int]:
    con = core.db()
    try:
        out: dict[str, int] = {}
        for strategy in v5_runtime.STRATEGIES:
            for direction in v5_runtime.DIRECTIONS:
                out[f'{strategy}|{direction}'] = int(con.execute(
                    'SELECT COUNT(*) FROM learning_samples WHERE strategy=? AND direction=?',
                    (strategy, direction),
                ).fetchone()[0] or 0)
        return out
    finally:
        con.close()


def _run_detailed_certification(core: Any) -> list[dict[str, Any]]:
    """Run every strategy x direction explicitly so an empty Champion table is diagnosable."""
    con = core.db()
    results: list[dict[str, Any]] = []
    try:
        store = v5_runtime.ModelStore(con)
        learner = v5_runtime.Learner(store)
        for strategy in v5_runtime.STRATEGIES:
            for direction in v5_runtime.DIRECTIONS:
                started = time.monotonic()
                sample_n = int(con.execute(
                    'SELECT COUNT(*) FROM learning_samples WHERE strategy=? AND direction=?',
                    (strategy, direction),
                ).fetchone()[0] or 0)
                try:
                    evaluation = learner.train_strategy_direction(strategy, direction)
                    if evaluation is None:
                        item = {
                            'strategy': strategy, 'direction': direction, 'status': 'NO_EVALUATION_OUTPUT',
                            'sample_rows': sample_n, 'promoted': False,
                            'reason': 'training produced no eligible OOS evaluation; usually insufficient class/fold/selected-signal evidence under current anti-overfit rules',
                        }
                    else:
                        item = dict(evaluation.__dict__)
                        item['status'] = 'PROMOTED' if bool(item.get('promoted')) else 'REJECTED_OOS'
                        item['sample_rows'] = sample_n
                    item['elapsed_seconds'] = round(time.monotonic() - started, 3)
                    results.append(item)
                except Exception as exc:
                    results.append({
                        'strategy': strategy, 'direction': direction, 'status': 'ERROR',
                        'sample_rows': sample_n, 'promoted': False,
                        'reason': f'{type(exc).__name__}: {exc}',
                        'elapsed_seconds': round(time.monotonic() - started, 3),
                    })
    finally:
        con.close()
    return results


def _champion_counts(core: Any) -> tuple[int, int]:
    return runtime_integrity._champion_counts(core)


def _certification_due(core: Any, state: dict[str, Any], signature: dict[str, Any], force: bool) -> bool:
    if force:
        return True
    last_total = int(state.get('last_sample_total') or 0)
    last_max = int(state.get('last_sample_max_ts') or 0)
    now = int(time.time())
    if state.get('status') in ('NOT_STARTED', 'WAITING_FOR_REPLAY', 'WAITING_DATA_AUDIT'):
        return True
    if int(signature.get('total') or 0) - last_total >= MIN_NEW_SAMPLES:
        return True
    if int(signature.get('max_ts') or 0) > last_max and now - int(state.get('last_completed_at') or 0) >= RECERTIFY_SECONDS:
        return True
    return False


def train_v17(core: Any, force: bool = False) -> list[dict[str, Any]]:
    """Final Signal certification authority.

    Replay completion itself is an immediate trigger. Old sample-count/time due flags
    are no longer allowed to strand a complete dataset in READY_FOR_SIGNAL_CERTIFICATION.
    """
    state = _load_state(core)
    replay = runtime_integrity.replay_progress(core)
    pipe = runtime_integrity.certification_pipeline(core)
    audit = audit_derived_dataset(core)
    signature = _sample_signature(core)

    if not replay.get('complete'):
        state.update({'status': 'WAITING_FOR_REPLAY', 'reason': 'Strict Replay has not reached the latest legally labelable decision'})
        _save_state(core, state)
        return []
    if not audit.get('valid'):
        state.update({'status': 'WAITING_DATA_AUDIT', 'reason': audit.get('reason'), 'audit_status': audit.get('status')})
        _save_state(core, state)
        return []
    if not pipe.get('signal_training_ready'):
        state.update({'status': 'WAITING_CERTIFICATION_GATE', 'reason': pipe.get('reason'), 'pipeline_stage': pipe.get('stage')})
        _save_state(core, state)
        return []
    if not _certification_due(core, state, signature, force):
        core.state['certification_orchestrator'] = state
        return []

    now = int(time.time())
    state.update({
        'status': 'SIGNAL_CERTIFICATION_RUNNING', 'reason': 'formal nested OOS certification is running',
        'started_at': now, 'last_attempt_at': now, 'attempts': int(state.get('attempts') or 0) + 1,
        'sample_total_at_start': signature['total'], 'sample_max_ts_at_start': signature['max_ts'],
        'preflight_counts': _preflight_counts(core), 'results': [],
    })
    _save_state(core, state)
    core.state.setdefault('learning', {})['phase'] = 'SIGNAL_CERTIFICATION_RUNNING'

    results = _run_detailed_certification(core)
    sig, exe = _champion_counts(core)
    errors = [x for x in results if x.get('status') == 'ERROR']
    promoted = [x for x in results if x.get('promoted')]
    rejected = [x for x in results if x.get('status') in ('REJECTED_OOS', 'NO_EVALUATION_OUTPUT')]

    if errors and len(errors) == len(results):
        status = 'SIGNAL_CERTIFICATION_FAILED'
        reason = 'every strategy-direction certification attempt raised an error'
    elif sig > 0:
        status = 'SIGNAL_CHAMPION_CERTIFIED'
        reason = f'{sig} Signal Champion(s) certified; Execution walk-forward may proceed'
    else:
        status = 'NO_SIGNAL_MODEL_PASSED_OOS'
        reason = f'0 Champions; {len(rejected)} strategy-direction candidates were rejected by OOS/anti-overfit evidence'

    completed = int(time.time())
    state.update({
        'status': status, 'reason': reason, 'completed_at': completed, 'last_completed_at': completed,
        'last_sample_total': signature['total'], 'last_sample_max_ts': signature['max_ts'],
        'signal_champions': sig, 'execution_champions': exe,
        'promoted_count': len(promoted), 'rejected_count': len(rejected), 'error_count': len(errors),
        'results': results,
    })
    _save_state(core, state)
    core.set_state('last_train_ts_v5', completed)
    core.set_state('v5_last_train_sample_total', int(signature['total']))
    core.state['last_training'] = results
    return results


def install(core: Any) -> None:
    # Final training authority: all automatic and manual paths resolve this symbol.
    v5_runtime.train_v5 = lambda c, force=False: train_v17(c, force)
    core.train_if_due = lambda force=False: train_v17(core, force)

    original_learning = core.learning_tick
    async def learning() -> None:
        await original_learning()
        # The old inner scheduler calls v5_runtime.train_v5 during heavy ticks. If a
        # completed replay was reached on a quiet tick, explicitly trigger once here.
        rp = runtime_integrity.replay_progress(core)
        state = _load_state(core)
        if rp.get('complete') and state.get('status') in ('NOT_STARTED', 'WAITING_FOR_REPLAY', 'WAITING_DATA_AUDIT', 'WAITING_CERTIFICATION_GATE'):
            results = train_v17(core, False)
            if results:
                await v5_runtime._notify_promotions(core, results)
        audit = core.get_state(AUDIT_KEY, None) or audit_derived_dataset(core, allow_auto_rebuild=False)
        cert = _load_state(core)
        lr = core.state.setdefault('learning', {})
        lr['derived_dataset_audit'] = audit
        lr['certification_orchestrator'] = cert
        pipe = runtime_integrity.certification_pipeline(core)
        if cert.get('status') == 'SIGNAL_CERTIFICATION_RUNNING':
            pipe['stage'] = 'SIGNAL_CERTIFICATION_RUNNING'; pipe['reason'] = cert.get('reason')
        elif cert.get('status') == 'SIGNAL_CHAMPION_CERTIFIED':
            pipe['stage'] = 'WAITING_EXECUTION_AUDIT' if int(cert.get('execution_champions') or 0) <= 0 else 'FULLY_OPERATIONAL'
            pipe['reason'] = cert.get('reason')
        elif cert.get('status') == 'NO_SIGNAL_MODEL_PASSED_OOS':
            pipe['stage'] = 'NO_SIGNAL_MODEL_PASSED_OOS'; pipe['reason'] = cert.get('reason')
        elif cert.get('status') in ('SIGNAL_CERTIFICATION_FAILED', 'WAITING_DATA_AUDIT'):
            pipe['stage'] = cert.get('status'); pipe['reason'] = cert.get('reason')
        lr['certification_pipeline'] = pipe
        lr['phase'] = pipe.get('stage') if rp.get('complete') else lr.get('phase')
        lr['blocker'] = None if pipe.get('stage') in ('SIGNAL_CERTIFICATION_RUNNING', 'WAITING_EXECUTION_AUDIT', 'FULLY_OPERATIONAL', 'NO_SIGNAL_MODEL_PASSED_OOS') else pipe.get('reason')
    core.learning_tick = learning

    strict = core.state.setdefault('strict_replay', {})
    strict['certification_orchestrator'] = {
        'runtime': VERSION, 'audit_schema': AUDIT_SCHEMA,
        'replay_completion_triggers_signal_certification_immediately': True,
        'legacy_due_counter_cannot_strand_completed_replay': True,
        'all_strategy_direction_attempts_are_visible': True,
        'derived_cross_version_data_is_audited_before_reuse': True,
        'hard_derived_corruption_auto_rebuilds_derived_only': True,
        'raw_market_preserved_on_derived_rebuild': True,
        'raw_derivatives_preserved_on_derived_rebuild': True,
        'clean_dataset_id_preserved_on_derived_rebuild': True,
        'minimum_new_samples_for_recertification': MIN_NEW_SAMPLES,
        'recertification_seconds': RECERTIFY_SECONDS,
    }
    core.state['runtime_version'] = VERSION
    core.app.version = '8.4.1'

    if not any(getattr(r, 'path', None) == '/api/v17/certification' for r in core.app.router.routes):
        @core.app.get('/api/v17/certification')
        def status() -> dict[str, Any]:
            return {
                'runtime': VERSION,
                'audit': audit_derived_dataset(core, allow_auto_rebuild=False),
                'certification': _load_state(core),
                'pipeline': runtime_integrity.certification_pipeline(core),
                'rules': strict.get('certification_orchestrator', {}),
            }
