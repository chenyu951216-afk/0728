from __future__ import annotations

import time
from typing import Any

import v5_runtime
import v13_replay_cursor_integrity as cursor_integrity
import v15_data_resilience as resilience
import v16_runtime_integrity as runtime_integrity
import v20_historical_signal_evolution as evolution

VERSION = '10.0.0-20260812'
FEATURE_SCHEMA = 8
STATE_KEY = 'hierarchical_learning_schema'
EXPECTED_LINEAGES = len(v5_runtime.STRATEGIES) * len(v5_runtime.DIRECTIONS)


def _pct(value: float) -> float:
    return round(max(0.0, min(100.0, float(value))), 2)


def _series_progress(core: Any, asset: str, tf: str) -> dict[str, Any]:
    sec = int(core.TIMEFRAME_SECONDS[tf])
    start = int(core.START_TS)
    target_end = (int(time.time()) // sec) * sec - sec
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
        return {'asset': asset, 'timeframe': tf, 'percent': 0.0, 'bars': 0, 'expected_bars': expected, 'from': None, 'to': None, 'gaps_estimate': expected}
    span_expected = max(1, (latest - earliest) // sec + 1)
    density = unique / span_expected
    historical_span = max(0.0, (latest - start + sec) / max(target_end - start + sec, sec))
    percent = _pct(min(historical_span, unique / expected) * min(1.0, density) * 100.0)
    return {
        'asset': asset, 'timeframe': tf, 'percent': percent, 'bars': unique,
        'expected_bars': expected, 'from': earliest, 'to': latest,
        'gaps_estimate': max(0, span_expected - unique), 'density': round(density, 6),
    }


def price_foundation(core: Any) -> dict[str, Any]:
    groups = (
        ('MACRO_CONTEXT', (('ETH', '1d'), ('ETH', '4h'))),
        ('MARKET_STRUCTURE', (('ETH', '1h'), ('ETH', '30m'), ('BTC', '1h'))),
        ('SHORT_HORIZON_EXECUTION', (('ETH', '15m'), ('ETH', '5m'))),
    )
    out: dict[str, Any] = {}
    for name, specs in groups:
        series = [_series_progress(core, asset, tf) for asset, tf in specs]
        percent = min((float(item['percent']) for item in series), default=0.0)
        out[name] = {
            'status': 'COMPLETE' if percent >= 99.5 else 'RUNNING' if percent > 0 else 'WAITING',
            'percent': _pct(percent), 'series': series,
            'rule': 'closed exchange candles only; canonical priority is fixed per timestamp; no interpolation',
        }
    return out


def _stage(name: str, percent: float, status: str, evidence: dict[str, Any], blocker: str | None = None) -> dict[str, Any]:
    return {'name': name, 'percent': _pct(percent), 'status': status, 'blocker': blocker, 'evidence': evidence}


def pipeline_status(core: Any) -> dict[str, Any]:
    price = price_foundation(core)
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
        _stage('5. POINT_IN_TIME_EVENT_REPLAY', replay_pct, 'COMPLETE' if replay.get('complete') else 'RUNNING', replay, None if replay.get('complete') else str((learning.get('replay_price_blocker') or {}).get('reason') or 'advancing only through matured 8h labels')),
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
        'no_lookahead_contract': {
            'features_use_closed_bars_only': True,
            'higher_timeframe_close_required': True,
            'future_5m_path_is_label_only': True,
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
    con = core.db()
    try:
        tables = {str(row[0]) for row in con.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
        sample_count = int(con.execute('SELECT COUNT(*) FROM learning_samples').fetchone()[0] or 0) if 'learning_samples' in tables else 0
        champion_count = int(con.execute("SELECT COUNT(*) FROM model_registry WHERE status='CHAMPION'").fetchone()[0] or 0) if 'model_registry' in tables else 0
    finally:
        con.close()
    if sample_count or champion_count:
        cursor_integrity._reset_derived_replay(core, '10.0 hierarchical feature schema: rebuild labels from closed macro -> structure -> short-horizon context')
    state = core.get_state('v18_final_system_state', {})
    state = dict(state) if isinstance(state, dict) else {}
    state.update({'last_cert_completed_at': 0, 'status': 'WAITING_FOR_REPLAY', 'reason': '10.0 hierarchical point-in-time replay must complete before new certification'})
    core.set_state('v18_final_system_state', state)
    core.set_state(STATE_KEY, FEATURE_SCHEMA)


def install(core: Any) -> None:
    _ensure_feature_schema(core)
    core.BACKFILL_PLAN = [('ETH', '1d'), ('ETH', '4h'), ('BTC', '1h'), ('ETH', '1h'), ('ETH', '30m'), ('ETH', '15m'), ('ETH', '5m')]
    def hierarchical_bootstrap_progress(_con: Any = None) -> dict[str, Any]:
        foundation = price_foundation(core)
        return {
            'overall': round(sum(group['percent'] for group in foundation.values()) / 3.0, 2),
            'hierarchical': foundation,
        }

    core.bootstrap_progress = hierarchical_bootstrap_progress
    original_learning = core.learning_tick

    async def hierarchical_learning_tick() -> None:
        await original_learning()
        pipeline_status(core)

    core.learning_tick = hierarchical_learning_tick
    core.state['runtime_version'] = VERSION
    core.app.version = '10.0.0'
    core.state.setdefault('strict_replay', {})['hierarchical_pipeline'] = {
        'runtime': VERSION, 'feature_schema': FEATURE_SCHEMA,
        'learning_order': ['1D/4H macro', '1H/30M structure', '15M/5M short-horizon', 'sealed OOS', 'Entry/SL/TP untouched audit'],
        'future_price_features': False, 'same_holdout_reuse': False,
        'live_single_trade_retraining': False,
    }
    pipeline_status(core)

    if not any(getattr(route, 'path', None) == '/api/v22/pipeline' for route in core.app.router.routes):
        @core.app.get('/api/v22/pipeline')
        def hierarchical_pipeline_status() -> dict[str, Any]:
            return pipeline_status(core)
