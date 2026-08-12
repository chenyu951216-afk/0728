from __future__ import annotations

import asyncio
import bisect
import json
import time
from datetime import datetime
from typing import Any

import execution_v7
import v5_async_runtime
import v5_runtime
import v7_learning_guard
import v7_runtime
import v8_stability
import v10_final_integrity as fin
import v12_clean_baseline
import v15_data_resilience as resilience


VERSION = '10.1.0-20260813'
REPLAY_PROGRESS_SCHEMA = 5
MAINTENANCE_SECONDS = 15 * 60


def _legal_frontier(core: Any) -> dict[str, Any]:
    """Return the newest 15m decision that can legally have a full 8h 5m label.

    Strict replay cannot ever reach the newest market candle because every training
    decision deliberately needs 96 future 5m bars. Completion must therefore be
    measured against the newest *label-matured* decision, not the live market edge.
    """
    m15 = resilience.canonical_bars(core, 'ETH', '15m')
    m5 = resilience.canonical_bars(core, 'ETH', '5m')
    if len(m15) < 134 or len(m5) < 96:
        latest = int(m15[-1]['ts']) if m15 else int(time.time())
        return {
            'ready': False,
            'latest_market_ts': latest,
            'legal_frontier_ts': int(core.START_TS),
            'legal_frontier_index': None,
            'reason': 'insufficient canonical 15m/5m history for a matured strict label',
        }

    ts15 = [int(x['ts']) for x in m15]
    last5 = int(m5[-1]['ts'])
    # future5 begins at decision_close=open+900 and contains timestamps
    # decision_close .. decision_close+95*300. The replay generator also keeps a
    # 33x15m live-edge buffer, so mirror both constraints exactly.
    max_open_from_5m = last5 - 900 - 95 * 300
    max_i_from_5m = bisect.bisect_right(ts15, max_open_from_5m) - 1
    max_i_from_15m = len(m15) - 34  # range(..., len(m15)-33) => last legal i = len-34
    i = min(max_i_from_5m, max_i_from_15m)
    stride = max(1, int(getattr(__import__('v9_final'), 'REPLAY_STRIDE_BARS', 2)))
    while i >= 100 and i % stride:
        i -= 1
    if i < 100:
        return {
            'ready': False,
            'latest_market_ts': int(ts15[-1]),
            'legal_frontier_ts': int(core.START_TS),
            'legal_frontier_index': None,
            'reason': 'no label-matured decision after strict warm-up yet',
        }
    return {
        'ready': True,
        'latest_market_ts': int(ts15[-1]),
        'legal_frontier_ts': int(ts15[i]),
        'legal_frontier_index': int(i),
        'latest_5m_ts': last5,
        'stride_bars': stride,
        'reason': 'newest decision with complete 96x5m post-decision path and 15m safety buffer',
    }


def replay_progress(core: Any) -> dict[str, Any]:
    collection_gate = getattr(core, 'price_collection_gate', None)
    full_collection_enforced = callable(collection_gate)
    if full_collection_enforced:
        collection = dict(collection_gate() or {})
        if not collection.get('ready'):
            return {
                'cursor_ts': int(core.get_state(v5_runtime.REPLAY_STATE_KEY, core.START_TS) or core.START_TS),
                'latest_market_ts': None, 'legal_frontier_ts': None, 'latest_5m_ts': None,
                'percent': 0.0, 'complete': False, 'pending_eligible_decisions': None,
                'processed_decisions': 0, 'total_eligible_decisions': None,
                'learned_decisions': 0, 'remaining_to_legal_frontier_seconds': None,
                'expected_label_maturity_buffer_seconds': 8 * 3600,
                'completion_basis': 'FULL_HISTORY_COLLECTION_THEN_CAUSAL_DECISION_COUNT',
                'frontier_reason': None, 'schema': REPLAY_PROGRESS_SCHEMA,
                'status': 'WAITING_FOR_FULL_HISTORY',
                'reason': 'all required 1D/4H/1H/30M/15M/5M history must meet the frozen coverage contract before the first replay decision',
                'price_collection_percent': float(collection.get('percent') or 0.0),
                'price_collection_blockers': list(collection.get('blockers') or []),
            }
    frontier = _legal_frontier(core)
    cursor = int(core.get_state(v5_runtime.REPLAY_STATE_KEY, core.START_TS) or core.START_TS)
    legal = int(frontier.get('legal_frontier_ts') or core.START_TS)
    latest = int(frontier.get('latest_market_ts') or time.time())
    start = int(core.START_TS)
    learned_decisions = 0
    con = core.db()
    try:
        expected_group = len(v5_runtime.STRATEGIES) * len(v5_runtime.DIRECTIONS)
        learned_decisions = int(con.execute(
            '''SELECT COUNT(*) FROM (
                   SELECT ts FROM learning_samples
                   GROUP BY ts HAVING COUNT(*)=?
               )''', (expected_group,),
        ).fetchone()[0] or 0)
    except Exception:
        learned_decisions = 0
    finally:
        con.close()

    processed = total = None
    if not frontier.get('ready') or legal <= start:
        pct = 0.0
        complete = False
        pending = None
    else:
        m15 = resilience.canonical_bars(core, 'ETH', '15m')
        ts15 = [int(x['ts']) for x in m15]
        end_i = int(frontier['legal_frontier_index'])
        stride = int(frontier.get('stride_bars') or 2)
        # Count causal decision slots, not wall-clock distance from 2020. This makes
        # a recent-only replay incapable of displaying 99% merely because its cursor
        # timestamp happens to be near the live edge.
        first_i = 100
        if full_collection_enforced:
            required_closes: list[int] = []
            for asset, tf, need in (('ETH', '1d', 80), ('ETH', '4h', 100), ('ETH', '1h', 100), ('BTC', '1h', 50)):
                rows = resilience.canonical_bars(core, asset, tf)
                if len(rows) < need:
                    first_i = end_i + 1
                    break
                required_closes.append(int(rows[need - 1]['ts']) + int(core.TIMEFRAME_SECONDS[tf]))
            if required_closes and first_i <= end_i:
                first_open = max(required_closes) - int(core.TIMEFRAME_SECONDS['15m'])
                first_i = max(first_i, bisect.bisect_left(ts15, first_open))
        first_i += (-first_i) % stride
        total = 0 if first_i > end_i else 1 + (end_i - first_i) // stride
        cursor_i = bisect.bisect_right(ts15, min(cursor, legal)) - 1
        processed = 0 if cursor_i < first_i else min(total, 1 + (cursor_i - first_i) // stride)
        pending = max(0, total - processed)
        complete = bool(total > 0 and cursor >= legal and pending == 0)
        pct = 100.0 if complete else 100.0 * processed / max(total, 1)
    return {
        'cursor_ts': cursor,
        'latest_market_ts': latest,
        'legal_frontier_ts': legal,
        'latest_5m_ts': frontier.get('latest_5m_ts'),
        'percent': round(100.0 if complete else min(100.0, pct), 2),
        'complete': bool(complete),
        'pending_eligible_decisions': pending,
        'processed_decisions': processed,
        'total_eligible_decisions': total,
        'learned_decisions': learned_decisions,
        'remaining_to_legal_frontier_seconds': max(0, legal - cursor) if frontier.get('ready') else None,
        'expected_label_maturity_buffer_seconds': max(0, latest - legal) if frontier.get('ready') else None,
        'completion_basis': (
            'FULL_HISTORY_COLLECTION_THEN_CAUSAL_DECISION_COUNT'
            if full_collection_enforced else 'LATEST_LEGALLY_LABELABLE_DECISION_NOT_LIVE_EDGE'
        ),
        'frontier_reason': frontier.get('reason'),
        'schema': REPLAY_PROGRESS_SCHEMA,
        'status': 'COMPLETE' if complete else 'STRICT_REPLAY_ADVANCING',
    }


def _champion_counts(core: Any) -> tuple[int, int]:
    con = core.db()
    try:
        sig = int(con.execute("SELECT COUNT(*) FROM model_registry WHERE status='CHAMPION' AND direction IN ('LONG','SHORT')").fetchone()[0] or 0)
        has_exec = con.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='execution_registry_v7'").fetchone()
        exe = int(con.execute("SELECT COUNT(*) FROM execution_registry_v7 WHERE status='CHAMPION'").fetchone()[0] or 0) if has_exec else 0
        samples = int(con.execute('SELECT COUNT(*) FROM learning_samples').fetchone()[0] or 0)
    finally:
        con.close()
    core.state['learning_sample_total'] = samples
    return sig, exe


def certification_pipeline(core: Any) -> dict[str, Any]:
    replay = replay_progress(core)
    rstate = resilience._load(core)
    gaps = resilience._gap_summary(core)
    marker = core.get_state(v12_clean_baseline.STATE_KEY, None) or {}
    baseline_ok = bool(marker.get('clean') is True and marker.get('status') == 'CLEAN')
    pending = int((gaps.get('counts') or {}).get('PENDING_REPAIR', 0) or 0)
    quarantined = int((gaps.get('counts') or {}).get('QUARANTINED_UNRECOVERABLE', 0) or 0)
    source_ok = bool(rstate.get('source_set_frozen'))
    derivative_ready = resilience.ready_through(core)
    now = int(time.time())
    derivative_ok = bool(derivative_ready is None or int(derivative_ready) >= now - fin.READY_SAFETY_SECONDS)
    gap_ok = bool(pending == 0 and quarantined <= resilience.MAX_QUARANTINED_GAPS)
    signal_ready = bool(replay.get('complete') and baseline_ok and source_ok and derivative_ok and gap_ok)
    sig, exe = _champion_counts(core)

    if not baseline_ok:
        stage, reason = 'BLOCKED_DATASET_PROVENANCE', 'Final Clean Baseline is not CLEAN'
    elif not source_ok:
        stage, reason = 'WAITING_SOURCE_FREEZE', 'full-span derivative source semantics are not frozen yet'
    elif not derivative_ok:
        stage, reason = 'WAITING_DERIVATIVE_CATCHUP', 'selected core derivative history has not caught the latest safe interval'
    elif pending:
        stage, reason = 'WAITING_PRICE_GAP_REPAIR', f'{pending} real price gap(s) are still under repair'
    elif quarantined > resilience.MAX_QUARANTINED_GAPS:
        stage, reason = 'BLOCKED_TOO_MANY_QUARANTINED_GAPS', f'{quarantined} quarantined gaps exceed limit {resilience.MAX_QUARANTINED_GAPS}'
    elif not replay.get('complete'):
        stage, reason = 'STRICT_REPLAY_ADVANCING', f"{replay.get('pending_eligible_decisions') or 0} matured decision(s) remain"
    elif sig <= 0:
        last = core.state.get('last_training') or []
        rejected = [x for x in last if not x.get('promoted')]
        if rejected:
            stage, reason = 'NO_SIGNAL_MODEL_PASSED_OOS', 'replay is complete; latest challengers were correctly rejected by OOS/anti-overfit gates'
        else:
            stage, reason = 'READY_FOR_SIGNAL_CERTIFICATION', 'strict replay is complete and formal Signal training is eligible'
    elif exe <= 0:
        stage, reason = 'WAITING_EXECUTION_AUDIT', 'Signal Champion exists; Entry/SL/TP walk-forward audit has not certified an Execution Champion yet'
    else:
        stage, reason = 'FULLY_OPERATIONAL', 'Signal + Execution Champions exist; live signal creation still requires current market/risk/re-entry gates'

    return {
        'runtime': VERSION,
        'stage': stage,
        'reason': reason,
        'signal_training_ready': signal_ready,
        'replay_complete': bool(replay.get('complete')),
        'replay': replay,
        'baseline_clean': baseline_ok,
        'source_set_frozen': source_ok,
        'derivative_ready': derivative_ok,
        'derivative_ready_through': derivative_ready,
        'pending_price_gaps': pending,
        'quarantined_price_gaps': quarantined,
        'signal_champions': sig,
        'execution_champions': exe,
        'learning_samples': int(core.state.get('learning_sample_total') or 0),
    }


async def _learning_tick_guarded(core: Any) -> None:
    """Matured-frontier scheduler: fast during replay, quiet after catch-up."""
    live_added = await asyncio.to_thread(v7_runtime.ingest_completed_live_samples_v7, core)
    replay = replay_progress(core)
    now = int(time.time())
    last_heavy = int(core.get_state('v7_last_heavy_learning_ts', 0) or 0)
    matured_pending = bool((replay.get('pending_eligible_decisions') or 0) > 0)
    maintenance_due = now - last_heavy >= MAINTENANCE_SECONDS
    heavy = bool(not replay.get('complete') or matured_pending or maintenance_due)
    if heavy:
        await v5_async_runtime.learning_tick_v5_async(core)
        core.set_state('v7_last_heavy_learning_ts', now)
    else:
        lr = core.state.setdefault('learning', {})
        lr['v7_live_execution_samples_added'] = live_added
        lr['v7_heavy_learning_skipped'] = True
        lr['v7_next_check_seconds'] = max(0, MAINTENANCE_SECONDS - (now - last_heavy))

    signature = [list(x) for x in v7_runtime._champion_signature(core)]
    old_signature = core.get_state('v7_execution_signal_signature', []) or []
    last_attempt = int(core.get_state('v7_execution_last_attempt_ts', 0) or 0)
    signature_changed = signature != old_signature
    daily_refresh = now - last_attempt >= 24 * 3600
    need_exec = bool(signature) and (signature_changed or daily_refresh)
    if need_exec:
        results = await asyncio.to_thread(execution_v7.optimize_all, core, bool(daily_refresh and not signature_changed))
        core.state['execution_learning'] = {
            'version': VERSION,
            'results': results,
            'registry': v7_runtime._execution_status(core)[:50],
            'updated_at': datetime.now(core.timezone.utc).isoformat(),
            'throttled': True,
            'reason': 'signal_version_changed' if signature_changed else 'daily_fresh_data_reaudit',
        }
        core.set_state('v7_execution_signal_signature', signature)
        core.set_state('v7_execution_last_attempt_ts', now)
        await v7_runtime._notify_execution_results(core, results)
    elif not signature:
        h = v8_stability._health(core, 'execution_audit')
        if h.get('status') in ('BOOTING', 'OK') and not v7_runtime._execution_status(core):
            h['status'] = 'WAITING_FOR_SIGNAL_CHAMPION'


async def _safe_scan(core: Any) -> dict[str, Any]:
    """Single live signal entrance: all later create_signal guards must execute."""
    bundle = await core.hub.live_bundle()
    core.upsert_live_gate(bundle)
    analysis = core._analysis_from_bundle(bundle)
    now = int(time.time())
    m15 = bundle['eth_15m']
    gate = v7_runtime.reentry_gate(core, analysis, m15) if analysis.get('selection') else {'allowed': False, 'reason': 'no selection'}
    analysis['reentry_gate'] = gate
    analysis['runtime_version'] = VERSION
    con = core.db()
    try:
        con.execute('INSERT INTO snapshots(ts,payload) VALUES(?,?)', (now, json.dumps(analysis, ensure_ascii=False)))
        con.execute('DELETE FROM snapshots WHERE ts<?', (now - 120 * 86400,))
        con.commit()
    finally:
        con.close()

    active = core.latest_signal()
    if active is None and analysis.get('selection', {}).get('tradeable') and gate.get('allowed'):
        # IMPORTANT: do not call create_signal_v7 directly. core.create_signal is the
        # composed final gate: fresh 15m close -> Clean Dataset -> v7 execution/reentry.
        created = core.create_signal(analysis, m15)
        if created:
            val = (created.get('payload') or {}).get('execution_validation') or {}
            await v5_runtime.robust_send_discord(
                core,
                '🆕 ETH 8.4 Point-in-Time 雙認證掛單',
                v7_runtime._summary(core, created) +
                f"\nAudit fills `{int(val.get('audit_fills') or 0)}`｜方法 `{val.get('method')}`\n"
                '只允許最新剛收線的 15m 決策；不追價。',
                0x4C8BF5,
            )
    core.state.update(
        service='OK',
        updated_at=datetime.now(core.timezone.utc).isoformat(),
        error=None,
        scan_count=core.state['scan_count'] + 1,
        analysis=analysis,
        active_signal=core.latest_signal(),
        account_equity_usdt=float(core.get_state('account_equity_usdt', 0) or 0),
    )
    return analysis


def _safe_manual_train(core: Any, force: bool = False) -> list[dict[str, Any]]:
    """Legacy HTTP route may call this name, but cannot bypass modern gates."""
    _ = force
    return list(v5_runtime.train_v5(core) or [])


def install(core: Any) -> None:
    # Completion semantics are a shared authority used by final-integrity training,
    # daily reports and the learning scheduler.
    v5_runtime._replay_progress = replay_progress

    # v7 learning_guard's installed closure resolves this module global at call time.
    v7_learning_guard.learning_tick_guarded = _learning_tick_guarded

    # Same for the v7 scan wrapper captured by stability layers: replace the module
    # global so every scan reaches the composed core.create_signal gate exactly once.
    v7_runtime.scan_v7 = _safe_scan

    # The original FastAPI /api/learning/train route resolves app.train_if_due as a
    # module global. Rebind it to the certified modern training chain.
    core.train_if_due = lambda force=False: _safe_manual_train(core, force)

    # Keep account equity visible even when there is no active signal. The original
    # POST /api/equity/{usdt} resolves set_state dynamically, so this also refreshes
    # in-memory UI state without changing persistent-storage semantics.
    original_set_state = core.set_state
    def synced_set_state(key: str, value: Any) -> None:
        original_set_state(key, value)
        if key == 'account_equity_usdt':
            try:
                core.state['account_equity_usdt'] = float(value)
            except Exception:
                core.state['account_equity_usdt'] = 0.0
    core.set_state = synced_set_state
    core.state['account_equity_usdt'] = float(core.get_state('account_equity_usdt', 0) or 0)

    # Final public-state wrapper. Do not clear samples, models, raw candles or the
    # CLEAN Dataset marker: 8.4 is a runtime/certification fix only.
    original_learning = core.learning_tick
    async def learning() -> None:
        await original_learning()
        lr = core.state.setdefault('learning', {})
        rp = replay_progress(core)
        pipe = certification_pipeline(core)
        lr['replay_learning_progress'] = rp
        lr['certification_pipeline'] = pipe
        lr['model_certification_gate'] = {
            'ready': pipe['signal_training_ready'],
            'reason': pipe['reason'],
            'stage': pipe['stage'],
        }
        lr['legal_label_frontier_ts'] = rp.get('legal_frontier_ts')
        lr['expected_label_maturity_buffer_seconds'] = rp.get('expected_label_maturity_buffer_seconds')
        if rp.get('complete') and lr.get('phase') in ('STRICT_REPLAY_PROBING', 'STRICT_REPLAY_ADVANCING'):
            lr['phase'] = pipe['stage']
            lr['blocker'] = None if pipe['stage'] in ('READY_FOR_SIGNAL_CERTIFICATION', 'WAITING_EXECUTION_AUDIT', 'FULLY_OPERATIONAL', 'NO_SIGNAL_MODEL_PASSED_OOS') else pipe['reason']
    core.learning_tick = learning

    strict = core.state.setdefault('strict_replay', {})
    strict['runtime_integrity'] = {
        'runtime': VERSION,
        'replay_progress_schema': REPLAY_PROGRESS_SCHEMA,
        'completion_is_latest_legally_labelable_decision': True,
        'live_market_edge_is_not_a_replay_completion_target': True,
        'future_label_horizon_seconds': 8 * 3600,
        'matured_frontier_scheduler': True,
        'legacy_manual_training_bypass_closed': True,
        'single_composed_new_signal_entrance': True,
        'clean_dataset_and_fresh_close_gate_cannot_be_bypassed_by_scan': True,
        'runtime_patch_resets_historical_data': False,
    }
    core.state['runtime_version'] = VERSION
    core.app.version = '8.4.0'

    if not any(getattr(r, 'path', None) == '/api/v16/runtime-integrity' for r in core.app.router.routes):
        @core.app.get('/api/v16/runtime-integrity')
        def status() -> dict[str, Any]:
            return {
                'runtime': VERSION,
                'replay': replay_progress(core),
                'pipeline': certification_pipeline(core),
                'account_equity_usdt': float(core.get_state('account_equity_usdt', 0) or 0),
                'rules': strict.get('runtime_integrity', {}),
            }
