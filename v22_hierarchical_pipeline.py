from __future__ import annotations

import asyncio
import hashlib
import json
import os
import time
from typing import Any

import v5_runtime
import v13_replay_cursor_integrity as cursor_integrity
import v15_data_resilience as resilience
import v16_runtime_integrity as runtime_integrity
import v20_historical_signal_evolution as evolution
import v7_runtime
import runtime_identity

VERSION = runtime_identity.RUNTIME_VERSION
FEATURE_SCHEMA = 9
STATE_KEY = 'hierarchical_learning_schema'
EXPECTED_LINEAGES = len(v5_runtime.STRATEGIES) * len(v5_runtime.DIRECTIONS)
COLLECTION_CUTOFF_KEY = 'causal_price_collection_cutoff_ts'
COLLECTION_CONTRACT_KEY = 'causal_price_collection_contract_v1'
PRICE_MIN_COVERAGE_PCT = max(99.0, min(100.0, float(os.getenv('CAUSAL_PRICE_MIN_COVERAGE_PCT', '100.0'))))
PRICE_MAX_MISSING_BARS = max(0, min(500, int(os.getenv('CAUSAL_PRICE_MAX_MISSING_BARS', '0'))))
PRICE_START_TOLERANCE_BARS = max(0, min(12, int(os.getenv('CAUSAL_PRICE_START_TOLERANCE_BARS', '2'))))
PRICE_TAIL_TOLERANCE_BARS = max(1, min(48, int(os.getenv('CAUSAL_PRICE_TAIL_TOLERANCE_BARS', '3'))))

PRICE_GROUPS = (
    ('MACRO_CONTEXT', (('ETH', '1d'), ('ETH', '4h'))),
    ('MARKET_STRUCTURE', (('ETH', '1h'), ('ETH', '30m'), ('BTC', '1h'))),
    ('SHORT_HORIZON_EXECUTION', (('ETH', '15m'), ('ETH', '5m'))),
)


async def _unified_boot_notice(core: Any) -> None:
    key = 'discord_boot_public_runtime'
    if core.get_state(key, '') == VERSION or core.state.get('discord_boot_public_runtime_inflight'):
        return
    # This assignment occurs before the first await, so concurrent asyncio workers
    # cannot both send the same startup embed.
    core.state['discord_boot_public_runtime_inflight'] = True
    try:
        gate = price_collection_gate(core)
        body = (
            f"Runtime `{VERSION}`｜公開版本統一為 `{runtime_identity.DISPLAY_VERSION}`\n"
            f"原始歷史資料 `{float(gate.get('percent') or 0):.2f}%`｜Replay 在資料收齊前固定 `0%`\n"
            '流程：1D/4H 大趨勢 → 1H/30M 結構 → 15M/5M 短線決策 → sealed OOS → untouched execution audit。\n'
            '歷史決策只能看到當時已收盤資料；Entry/SL/TP 凍結後，未來 5M 才依時間順序快轉揭露。\n'
            '舊版名稱只保留為資料庫與 API 相容層，不代表仍在執行舊模型。'
        )
        ok = await v5_runtime.robust_send_discord(
            core,
            f'✅ {runtime_identity.PRODUCT_NAME} {runtime_identity.DISPLAY_VERSION} 已啟動',
            body,
            0x3498DB,
        )
        if ok:
            core.set_state(key, VERSION)
    finally:
        core.state['discord_boot_public_runtime_inflight'] = False


def _pct(value: float) -> float:
    return round(max(0.0, min(100.0, float(value))), 2)


def _collection_cutoff(core: Any) -> int:
    cutoff = int(core.get_state(COLLECTION_CUTOFF_KEY, 0) or 0)
    if cutoff <= int(core.START_TS):
        cutoff = int(time.time())
        core.set_state(COLLECTION_CUTOFF_KEY, cutoff)
    return cutoff


def _series_progress(core: Any, asset: str, tf: str) -> dict[str, Any]:
    sec = int(core.TIMEFRAME_SECONDS[tf])
    start = int(core.START_TS)
    # Freeze the initial collection horizon. Otherwise "collect everything first"
    # can never finish because the live edge moves forward on every status refresh.
    target_end = (_collection_cutoff(core) // sec) * sec - sec
    expected = max(1, (target_end - start) // sec + 1)
    placeholders = ','.join('?' for _ in resilience.PRICE_PRIORITY)
    con = core.db()
    try:
        row = con.execute(
            f'''SELECT COUNT(DISTINCT ts),MIN(ts),MAX(ts) FROM market_bars
                WHERE asset=? AND tf=? AND ts BETWEEN ? AND ?
                  AND source IN ({placeholders})''',
            (asset, tf, start, target_end, *resilience.PRICE_PRIORITY),
        ).fetchone()
    finally:
        con.close()
    unique = int(row[0] or 0) if row else 0
    earliest = int(row[1]) if row and row[1] is not None else None
    latest = int(row[2]) if row and row[2] is not None else None
    if not unique or earliest is None or latest is None:
        return {
            'asset': asset, 'timeframe': tf, 'percent': 0.0, 'bars': 0,
            'expected_bars': expected, 'from': None, 'to': None,
            'target_from': start, 'target_to': target_end, 'gaps_estimate': expected,
            'start_ready': False, 'tail_ready': False, 'coverage_ready': False,
            'history_ready': False, 'required_coverage_pct': PRICE_MIN_COVERAGE_PCT,
            'maximum_missing_bars_before_replay': PRICE_MAX_MISSING_BARS,
        }
    raw_percent = unique / expected * 100.0
    percent = _pct(raw_percent)
    missing_bars = max(0, expected - unique)
    start_ready = earliest <= start + PRICE_START_TOLERANCE_BARS * sec
    tail_ready = latest >= target_end - PRICE_TAIL_TOLERANCE_BARS * sec
    coverage_ready = raw_percent >= PRICE_MIN_COVERAGE_PCT and missing_bars <= PRICE_MAX_MISSING_BARS
    history_ready = bool(start_ready and tail_ready and coverage_ready)
    return {
        'asset': asset, 'timeframe': tf, 'percent': percent, 'bars': unique,
        'expected_bars': expected, 'from': earliest, 'to': latest,
        'target_from': start, 'target_to': target_end,
        'gaps_estimate': missing_bars,
        'density': round(unique / expected, 6),
        'start_ready': start_ready, 'tail_ready': tail_ready,
        'coverage_ready': coverage_ready, 'history_ready': history_ready,
        'required_coverage_pct': PRICE_MIN_COVERAGE_PCT,
        'maximum_missing_bars_before_replay': PRICE_MAX_MISSING_BARS,
    }


def price_foundation(core: Any) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for name, specs in PRICE_GROUPS:
        series = [_series_progress(core, asset, tf) for asset, tf in specs]
        percent = min((float(item['percent']) for item in series), default=0.0)
        ready = bool(series and all(bool(item.get('history_ready')) for item in series))
        out[name] = {
            'status': 'COMPLETE' if ready else 'COLLECTING' if percent > 0 else 'WAITING',
            'percent': _pct(percent), 'series': series,
            'history_ready': ready,
            'rule': 'collect the frozen full-history horizon first; closed exchange candles only; fixed canonical priority; no interpolation',
        }
    return out


def price_collection_gate(core: Any) -> dict[str, Any]:
    foundation = price_foundation(core)
    series = [item for group in foundation.values() for item in group['series']]
    ready = bool(series and all(bool(item.get('history_ready')) for item in series))
    starts = {f"{item['asset']}:{item['timeframe']}": item.get('from') for item in series}
    payload = {
        'schema': FEATURE_SCHEMA,
        'cutoff_ts': _collection_cutoff(core),
        'required_coverage_pct': PRICE_MIN_COVERAGE_PCT,
        'maximum_missing_bars_before_replay': PRICE_MAX_MISSING_BARS,
        'starts': starts,
        'targets': {f"{item['asset']}:{item['timeframe']}": item.get('target_to') for item in series},
    }
    fingerprint = hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(',', ':')).encode()).hexdigest()[:24]
    blockers = [
        {
            'asset': item['asset'], 'timeframe': item['timeframe'],
            'percent': item['percent'], 'from': item.get('from'), 'to': item.get('to'),
            'target_from': item.get('target_from'), 'target_to': item.get('target_to'),
            'gaps_estimate': item.get('gaps_estimate'),
        }
        for item in series if not item.get('history_ready')
    ]
    status = {
        'ready': ready,
        'status': 'READY_FOR_CAUSAL_REPLAY' if ready else 'COLLECTING_FULL_HISTORY_BEFORE_REPLAY',
        'percent': min((float(item['percent']) for item in series), default=0.0),
        'cutoff_ts': _collection_cutoff(core), 'contract_fingerprint': fingerprint,
        'starts': starts, 'blockers': blockers, 'foundation': foundation,
        'future_data_available_to_decision': False,
        'future_5m_after_decision_is_label_only': True,
        'contract': payload,
    }
    core.state['causal_price_collection_gate'] = status
    return status


def _first_collection_gap(core: Any) -> dict[str, Any] | None:
    placeholders = ','.join('?' for _ in resilience.PRICE_PRIORITY)
    for _group, specs in PRICE_GROUPS:
        for asset, tf in specs:
            sec = int(core.TIMEFRAME_SECONDS[tf])
            start = int(core.START_TS)
            target_end = (_collection_cutoff(core) // sec) * sec - sec
            con = core.db()
            try:
                first_last = con.execute(
                    f'''SELECT MIN(ts),MAX(ts) FROM market_bars
                        WHERE asset=? AND tf=? AND ts BETWEEN ? AND ?
                          AND source IN ({placeholders})''',
                    (asset, tf, start, target_end, *resilience.PRICE_PRIORITY),
                ).fetchone()
                earliest = int(first_last[0]) if first_last and first_last[0] is not None else None
                latest = int(first_last[1]) if first_last and first_last[1] is not None else None
                if earliest is None or earliest > start:
                    missing = start
                else:
                    row = con.execute(
                        f'''WITH unique_ts AS (
                                SELECT DISTINCT ts FROM market_bars
                                WHERE asset=? AND tf=? AND ts BETWEEN ? AND ?
                                  AND source IN ({placeholders})
                            ), ordered AS (
                                SELECT ts,LAG(ts) OVER (ORDER BY ts) AS previous_ts FROM unique_ts
                            )
                            SELECT previous_ts+? FROM ordered
                            WHERE previous_ts IS NOT NULL AND ts-previous_ts>?
                            ORDER BY ts LIMIT 1''',
                        (asset, tf, start, target_end, *resilience.PRICE_PRIORITY, sec, sec),
                    ).fetchone()
                    missing = int(row[0]) if row and row[0] is not None else (latest + sec if latest is not None and latest < target_end else None)
            finally:
                con.close()
            if missing is not None and missing <= target_end:
                return {
                    'asset': asset, 'timeframe': tf, 'missing_ts': int(missing),
                    'target_from': start, 'target_to': target_end,
                }
    return None


async def _repair_collection_gap(core: Any, target: dict[str, Any]) -> dict[str, Any]:
    asset = str(target['asset'])
    tf = str(target['timeframe'])
    missing = int(target['missing_ts'])
    sec = int(core.TIMEFRAME_SECONDS[tf])
    end_ts = min(int(target['target_to']), missing + 999 * sec)
    errors: list[str] = []
    attempts: list[dict[str, Any]] = []
    for source in core.hub.history_source_order(tf, end_ts):
        try:
            rows = await core.hub.fetch_history(source, asset, tf, end_ts=end_ts, limit=1000)
            payload = [row.dict() for row in rows if int(core.START_TS) <= int(row.ts) <= int(target['target_to'])]
            added = int(core.insert_bars(source, asset, tf, payload) or 0) if payload else 0
            exact = any(int(row.ts) == missing for row in rows)
            attempts.append({'source': source, 'rows': len(rows), 'added': added, 'exact_missing_bar': exact})
            if exact:
                result = {
                    'status': 'REPAIRED_PAGE', 'asset': asset, 'timeframe': tf,
                    'missing_ts': missing, 'source': source, 'added': added,
                    'attempts': attempts, 'no_interpolation': True,
                }
                core.state['causal_price_collection_repair'] = result
                return result
        except Exception as exc:
            errors.append(f'{source}: {exc}')
            attempts.append({'source': source, 'error': str(exc)[-500:]})
    result = {
        'status': 'STILL_MISSING', 'asset': asset, 'timeframe': tf,
        'missing_ts': missing, 'attempts': attempts, 'errors': errors[-8:],
        'no_interpolation': True,
    }
    core.state['causal_price_collection_repair'] = result
    return result


def _activate_collection_contract(core: Any, gate: dict[str, Any]) -> None:
    candidate = dict(gate.get('contract') or {})
    previous = core.get_state(COLLECTION_CONTRACT_KEY, {})
    previous = dict(previous) if isinstance(previous, dict) else {}
    previous_starts = dict(previous.get('starts') or {})
    candidate_starts = dict(candidate.get('starts') or {})
    if previous_starts and previous_starts != candidate_starts:
        # Raw history was extended behind an already-advanced cursor. Derived rows
        # are disposable; replay them from the beginning so newly discovered older
        # decisions can never be silently omitted.
        cursor_integrity._reset_derived_replay(
            core,
            f'{VERSION} raw full-history start changed behind replay cursor; rebuild every causal decision from the frozen horizon',
        )
    candidate.update({
        'activated_at': int(previous.get('activated_at') or time.time()),
        'contract_fingerprint': gate.get('contract_fingerprint'),
        'raw_history_can_expand_behind_cursor_without_reset': False,
    })
    core.set_state(COLLECTION_CONTRACT_KEY, candidate)


def _install_full_history_replay_gate(core: Any) -> None:
    original_generate = v5_runtime.generate_learning_samples_v5

    def causal_generate(c: Any, batch: int = 500) -> int:
        gate = price_collection_gate(c)
        if not gate.get('ready'):
            learning = c.state.setdefault('learning', {})
            learning['phase'] = 'COLLECTING_FULL_HISTORY_BEFORE_REPLAY'
            learning['replay_price_blocker'] = {
                'blocked': True,
                'state': 'WAITING_FOR_FULL_HISTORY',
                'reason': 'strict point-in-time replay cannot start until every required price timeframe meets the frozen full-history coverage contract',
                'collection_percent': gate.get('percent'),
                'blockers': gate.get('blockers'),
            }
            learning['causal_price_collection_gate'] = gate
            return 0
        _activate_collection_contract(c, gate)
        learning = c.state.setdefault('learning', {})
        if str(learning.get('phase') or '').startswith('COLLECTING_FULL_HISTORY'):
            learning['phase'] = 'STRICT_REPLAY_ADVANCING'
        learning['causal_price_collection_gate'] = gate
        return int(original_generate(c, batch) or 0)

    v5_runtime.generate_learning_samples_v5 = causal_generate
    core.generate_learning_samples = lambda batch=500: causal_generate(core, batch)


def _stage(name: str, percent: float, status: str, evidence: dict[str, Any], blocker: str | None = None) -> dict[str, Any]:
    return {'name': name, 'percent': _pct(percent), 'status': status, 'blocker': blocker, 'evidence': evidence}


def pipeline_status(core: Any) -> dict[str, Any]:
    price = price_foundation(core)
    collection = price_collection_gate(core)
    learning = core.state.get('learning') if isinstance(core.state.get('learning'), dict) else {}
    source = learning.get('data_resilience') if isinstance(learning.get('data_resilience'), dict) else {}
    replay = runtime_integrity.replay_progress(core)
    evo = evolution.evolution_status(core)
    lineages = list(evo.get('latest_lineages') or [])
    terminal_count = sum(1 for row in lineages if str(row.get('status') or '') not in ('RUNNING', 'WAITING'))
    con = core.db()
    try:
        signal_champions = int(con.execute("SELECT COUNT(*) FROM model_registry WHERE status='CHAMPION'").fetchone()[0] or 0)
        tables = {str(row[0]) for row in con.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
        execution_champions = int(con.execute("SELECT COUNT(*) FROM execution_registry_v7 WHERE status='CHAMPION'").fetchone()[0] or 0) if 'execution_registry_v7' in tables else 0
        live_samples = int(con.execute('SELECT COUNT(*) FROM live_execution_samples').fetchone()[0] or 0) if 'live_execution_samples' in tables else 0
    finally:
        con.close()

    derivative_ready = bool(source.get('source_set_frozen'))
    derivative_pct = 100.0 if derivative_ready else 35.0 if source else 0.0
    replay_pct = float(replay.get('percent') or 0.0)
    evo_pct = _pct(terminal_count / max(EXPECTED_LINEAGES, 1) * 100.0) if replay.get('complete') else 0.0
    holdout_opened = sum(1 for row in lineages if int(row.get('holdout_end_ts') or 0) > 0)
    holdout_pct = _pct(holdout_opened / max(EXPECTED_LINEAGES, 1) * 100.0)
    execution_pct = 100.0 if execution_champions > 0 else 0.0
    final_state = core.get_state('v18_final_system_state', {})
    final_state = final_state if isinstance(final_state, dict) else {}

    stages = [
        _stage('1. MACRO_DATA_1D_4H', price['MACRO_CONTEXT']['percent'], price['MACRO_CONTEXT']['status'], price['MACRO_CONTEXT']),
        _stage('2. STRUCTURE_DATA_1H_30M', price['MARKET_STRUCTURE']['percent'], price['MARKET_STRUCTURE']['status'], price['MARKET_STRUCTURE']),
        _stage('3. EXECUTION_DATA_15M_5M', price['SHORT_HORIZON_EXECUTION']['percent'], price['SHORT_HORIZON_EXECUTION']['status'], price['SHORT_HORIZON_EXECUTION']),
        _stage('4. DERIVATIVE_SOURCE_FREEZE', derivative_pct, 'COMPLETE' if derivative_ready else 'WAITING', {
            'oi_sources': list(source.get('model_oi_sources') or []),
            'funding_sources': list(source.get('model_funding_sources') or []),
            'enrichment_sources': list(source.get('model_enrichment_sources') or []),
            'missing_groups_are_generation_wide_masks': True,
        }, None if derivative_ready else 'waiting for provider capability audit; range-limited data will be masked, never backfilled with present values'),
        _stage(
            '5. POINT_IN_TIME_EVENT_REPLAY', replay_pct,
            'COMPLETE' if replay.get('complete') else 'WAITING' if str(replay.get('status') or '').startswith('WAITING') else 'RUNNING',
            replay,
            None if replay.get('complete') else str(
                replay.get('reason') or (learning.get('replay_price_blocker') or {}).get('reason') or
                'advancing only through matured 8h labels'
            ),
        ),
        _stage('6. HIERARCHICAL_DEV_EVOLUTION', evo_pct, 'COMPLETE' if terminal_count >= EXPECTED_LINEAGES else 'WAITING' if not replay.get('complete') else 'RUNNING', {
            'search_order': ['MACRO_REGIME', 'MARKET_STRUCTURE', 'SHORT_HORIZON_SIGNAL'],
            'terminal_lineages': terminal_count, 'expected_lineages': EXPECTED_LINEAGES,
            'candidates_evaluated': sum(int(row.get('candidates_evaluated') or 0) for row in lineages),
        }),
        _stage('7. ONE_TIME_SEALED_OOS', holdout_pct, 'COMPLETE' if holdout_opened >= EXPECTED_LINEAGES else 'WAITING', {
            'opened_lineages': holdout_opened, 'expected_lineages': EXPECTED_LINEAGES,
            'same_failed_holdout_can_be_retried': False, 'lineages': lineages,
        }),
        _stage('8. ENTRY_SL_TP_UNTOUCHED_AUDIT', execution_pct, 'COMPLETE' if execution_champions > 0 else 'WAITING', {
            'signal_champions': signal_champions, 'execution_champions': execution_champions,
            'simulation': 'bar-by-bar after decision close; conservative stop-before-target on ambiguous bars',
        }, None if execution_champions > 0 else 'requires at least one Signal Champion and a matching untouched execution audit'),
    ]
    weights = (10, 10, 10, 10, 25, 15, 5, 15)
    overall = _pct(sum(float(stage['percent']) * weight for stage, weight in zip(stages, weights)) / 100.0)
    active = next((stage for stage in stages if float(stage['percent']) < 99.5), stages[-1])
    operational = bool(signal_champions > 0 and execution_champions > 0 and replay.get('complete'))
    status = {
        'runtime': VERSION, 'feature_schema': FEATURE_SCHEMA, 'overall_percent': overall,
        'active_stage': active['name'], 'operational': operational,
        'final_status': final_state.get('status') or ('FULLY_OPERATIONAL' if operational else 'LEARNING'),
        'final_reason': final_state.get('reason'), 'stages': stages,
        'paper_feedback': {
            'status': 'MONITORING' if operational else 'WAITING_FOR_CERTIFIED_POLICY',
            'live_execution_samples': live_samples,
            'single_trade_can_mutate_signal_model': False,
            'post_exit_tracking': True,
            'reaudit_requires_batched_new_evidence': True,
        },
        'source_policy': {
            'price_priority': list(resilience.PRICE_PRIORITY),
            'coinglass_role': 'aggregated derivative enrichment; never the sole price source',
            'provider_retention_is_capability': True,
            'synthetic_gap_fill': False,
        },
        'price_collection_gate': collection,
        'no_lookahead_contract': {
            'raw_history_may_be_precollected_but_is_not_visible_to_historical_decisions': True,
            'features_use_closed_bars_only': True,
            'higher_timeframe_close_required': True,
            'future_5m_path_is_label_only': True,
            'future_path_revealed_only_after_entry_stop_target_plan_is_frozen': True,
            'future_path_is_processed_in_timestamp_order': True,
            'execution_simulation_is_sequential_after_decision': True,
            'purged_walk_forward_development_only': True,
            'sealed_holdout_opened_once_after_candidate_freeze': True,
        },
        'startup_preflight': core.state.get('startup_preflight') or {},
        'updated_at': int(time.time()),
    }
    core.state['hierarchical_pipeline'] = status
    return status


def _ensure_feature_schema(core: Any) -> None:
    current = int(core.get_state(STATE_KEY, 0) or 0)
    if current >= FEATURE_SCHEMA:
        return
    # Always rewind the derived cursor on this one-time schema migration. A stale
    # cursor with zero surviving samples is just as unsafe as an obviously partial
    # sample table. Raw price/derivative caches and the dataset identity are kept.
    cursor_integrity._reset_derived_replay(
        core,
        f'{VERSION} causal full-history schema: collect the frozen raw horizon before rebuilding every point-in-time decision',
    )
    core.set_state(COLLECTION_CUTOFF_KEY, int(time.time()))
    core.set_state(COLLECTION_CONTRACT_KEY, {})
    state = core.get_state('v18_final_system_state', {})
    state = dict(state) if isinstance(state, dict) else {}
    state.update({'last_cert_completed_at': 0, 'status': 'WAITING_FOR_FULL_HISTORY', 'reason': f'{VERSION} must collect the frozen multi-timeframe price horizon before causal replay and certification'})
    core.set_state('v18_final_system_state', state)
    core.set_state(STATE_KEY, FEATURE_SCHEMA)


def install(core: Any) -> None:
    _ensure_feature_schema(core)
    core.BACKFILL_PLAN = [('ETH', '1d'), ('ETH', '4h'), ('BTC', '1h'), ('ETH', '1h'), ('ETH', '30m'), ('ETH', '15m'), ('ETH', '5m')]
    core.price_collection_gate = lambda: price_collection_gate(core)
    _install_full_history_replay_gate(core)
    # Only the final runtime may own public boot messages. Earlier names remain
    # storage/code compatibility identifiers and are never advertised to users.
    v5_runtime.maybe_boot_notice = lambda _core: _unified_boot_notice(_core)
    v7_runtime.maybe_boot_notice = lambda _core: _unified_boot_notice(_core)
    core.final_boot_notice = lambda: _unified_boot_notice(core)
    def hierarchical_bootstrap_progress(_con: Any = None) -> dict[str, Any]:
        foundation = price_foundation(core)
        return {
            # A prerequisite pipeline is only as complete as its least-covered
            # required group. Averaging 100%, 100%, 0.1% into 66.7% was misleading.
            'overall': round(min((group['percent'] for group in foundation.values()), default=0.0), 2),
            'hierarchical': foundation,
        }

    core.bootstrap_progress = hierarchical_bootstrap_progress
    original_learning = core.learning_tick

    async def hierarchical_learning_tick() -> None:
        await original_learning()
        gate = price_collection_gate(core)
        if not gate.get('ready'):
            target = _first_collection_gap(core)
            if target:
                await _repair_collection_gap(core, target)
        pipeline_status(core)

    core.learning_tick = hierarchical_learning_tick
    runtime_identity.stamp(core)
    core.state.setdefault('strict_replay', {})['hierarchical_pipeline'] = {
        'runtime': VERSION, 'feature_schema': FEATURE_SCHEMA,
        'learning_order': ['1D/4H macro', '1H/30M structure', '15M/5M short-horizon', 'sealed OOS', 'Entry/SL/TP untouched audit'],
        'future_price_features': False, 'same_holdout_reuse': False,
        'live_single_trade_retraining': False,
        'full_history_collection_precedes_initial_replay': True,
        'replay_progress_is_decision_count_not_wall_clock_distance': True,
    }
    pipeline_status(core)

    if not any(getattr(route, 'path', None) == '/api/v22/pipeline' for route in core.app.router.routes):
        @core.app.get('/api/v22/pipeline')
        def hierarchical_pipeline_status() -> dict[str, Any]:
            return pipeline_status(core)

    if not any(getattr(route, 'path', None) == '/api/latest/pipeline' for route in core.app.router.routes):
        @core.app.get('/api/latest/pipeline')
        def latest_pipeline() -> dict[str, Any]:
            return pipeline_status(core)

    if not any(getattr(route, 'path', None) == '/api/latest/champions' for route in core.app.router.routes):
        @core.app.get('/api/latest/champions')
        def latest_champions() -> list[dict[str, Any]]:
            return v5_runtime._all_champions(core)

    if not any(getattr(route, 'path', None) == '/api/latest/execution' for route in core.app.router.routes):
        @core.app.get('/api/latest/execution')
        def latest_execution() -> dict[str, Any]:
            return {'runtime': VERSION, 'registry': v7_runtime._execution_status(core), 'state': core.state.get('execution_learning', {})}

    if not any(getattr(route, 'path', None) == '/api/latest/execution/train' for route in core.app.router.routes):
        @core.app.post('/api/latest/execution/train')
        async def latest_execution_train() -> dict[str, Any]:
            results = await asyncio.to_thread(v7_runtime.execution.optimize_all, core, True)
            core.state['execution_learning'] = {
                'version': VERSION, 'results': results,
                'registry': v7_runtime._execution_status(core)[:50],
                'updated_at': int(time.time()),
            }
            await v7_runtime._notify_execution_results(core, results)
            return {'runtime': VERSION, 'results': results}

    if not any(getattr(route, 'path', None) == '/api/latest/trade-monitor' for route in core.app.router.routes):
        @core.app.get('/api/latest/trade-monitor')
        def latest_trade_monitor() -> dict[str, Any]:
            return {'runtime': VERSION, 'mode': 'GATE_PUBLIC_TRADES_ORDERED', 'state': core.state.get('risk_monitor')}

    if not any(getattr(route, 'path', None) == '/api/latest/final-status' for route in core.app.router.routes):
        @core.app.get('/api/latest/final-status')
        def latest_final_status() -> dict[str, Any]:
            import v18_final_system
            return v18_final_system._authoritative_view(core)
