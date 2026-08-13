from __future__ import annotations

import bisect
import os
import time
from typing import Any

import execution_v7
import v5_runtime
import v7_runtime
import v15_data_resilience as resilience

SCHEMA = 1
FIXED_CUTOFF_KEY = 'causal_price_collection_cutoff_ts'
LIVE_HANDOFF_KEY = 'fixed_horizon_live_handoff'
PAPER_NOTIONAL_USDT = 20000.0
TRADE_ASSET = 'ETH'
TRADE_SYMBOL = 'ETHUSDT'
LIVE_EXCHANGE = 'bitget'


def _cutoff(core: Any) -> int:
    """Return the immutable historical data horizon captured at this deployment.

    v22 already records this timestamp before full-history collection. Reusing that
    persisted value means a slow replay never starts chasing candles that arrived
    after the process began.
    """
    cutoff = int(core.get_state(FIXED_CUTOFF_KEY, 0) or 0)
    if cutoff <= int(core.START_TS):
        cutoff = int(time.time())
        core.set_state(FIXED_CUTOFF_KEY, cutoff)
    return cutoff


def _fixed_legal_frontier(core: Any) -> dict[str, Any]:
    """Newest historical decision whose entire label path existed by boot cutoff.

    The replay horizon is intentionally frozen. Bars arriving while learning or
    certification runs are live-era data and are not appended to this historical
    replay. This prevents an asymptotic moving target while preserving no-lookahead:
    future 5m bars are still used only as post-decision labels.
    """
    m15 = resilience.canonical_bars(core, 'ETH', '15m')
    m5 = resilience.canonical_bars(core, 'ETH', '5m')
    cutoff = _cutoff(core)
    if len(m15) < 134 or len(m5) < 96:
        latest = int(m15[-1]['ts']) if m15 else cutoff
        return {
            'ready': False,
            'latest_market_ts': latest,
            'legal_frontier_ts': int(core.START_TS),
            'legal_frontier_index': None,
            'fixed_replay_cutoff_ts': cutoff,
            'moving_frontier': False,
            'reason': 'insufficient canonical 15m/5m history for a matured strict label inside the fixed boot horizon',
        }

    ts15 = [int(x['ts']) for x in m15]
    ts5 = [int(x['ts']) for x in m5]
    cutoff_5m_i = bisect.bisect_left(ts5, cutoff) - 1
    cutoff_15m_i = bisect.bisect_left(ts15, cutoff) - 1
    if cutoff_5m_i < 95 or cutoff_15m_i < 133:
        return {
            'ready': False,
            'latest_market_ts': int(ts15[-1]),
            'legal_frontier_ts': int(core.START_TS),
            'legal_frontier_index': None,
            'fixed_replay_cutoff_ts': cutoff,
            'moving_frontier': False,
            'reason': 'fixed boot horizon does not yet contain enough mature history',
        }

    last5_at_cutoff = int(ts5[cutoff_5m_i])
    max_open_from_5m = last5_at_cutoff - 900 - 95 * 300
    max_i_from_5m = bisect.bisect_right(ts15, max_open_from_5m) - 1
    # Mirror the historical generator safety buffer, but cap it at the boot horizon.
    max_i_from_15m = cutoff_15m_i - 33
    i = min(max_i_from_5m, max_i_from_15m)
    stride = max(1, int(os.getenv('STRICT_REPLAY_STRIDE_BARS', '2')))
    while i >= 100 and i % stride:
        i -= 1
    if i < 100:
        return {
            'ready': False,
            'latest_market_ts': int(ts15[-1]),
            'legal_frontier_ts': int(core.START_TS),
            'legal_frontier_index': None,
            'fixed_replay_cutoff_ts': cutoff,
            'latest_5m_ts': last5_at_cutoff,
            'moving_frontier': False,
            'reason': 'no label-matured decision after strict warm-up inside fixed boot horizon',
        }
    return {
        'ready': True,
        'latest_market_ts': int(ts15[-1]),
        'legal_frontier_ts': int(ts15[i]),
        'legal_frontier_index': int(i),
        'latest_5m_ts': last5_at_cutoff,
        'stride_bars': stride,
        'fixed_replay_cutoff_ts': cutoff,
        'moving_frontier': False,
        'reason': 'fixed deployment-time replay horizon; complete 96x5m post-decision path existed before boot cutoff',
    }


def _trading_contract() -> dict[str, Any]:
    return {
        'exchange': LIVE_EXCHANGE,
        'asset': TRADE_ASSET,
        'symbol': TRADE_SYMBOL,
        'paper_notional_usdt': PAPER_NOTIONAL_USDT,
        'real_order_notional_usdt': PAPER_NOTIONAL_USDT,
        'leverage_policy': 'MAX_AVAILABLE_AT_ORDER_TIME',
        'signal_source': 'SIGNAL_CHAMPION_ONLY',
        'entry_stop_targets_source': 'EXECUTION_CHAMPION_ONLY',
        'fixed_percent_stop_or_target_allowed': False,
        'historical_no_lookahead': True,
        'historical_future_path_role': 'LABEL_ONLY_AFTER_PLAN_FREEZE',
        'historical_waiting_time_may_be_fast_forwarded': True,
        'other_historical_steps_may_be_skipped': False,
        # This repository currently has no authenticated Bitget order client. Keep
        # actual exchange placement fail-closed instead of pretending an order was sent.
        'authenticated_bitget_order_connector_present': False,
        'real_order_status': 'FAIL_CLOSED_UNTIL_AUTHENTICATED_BITGET_CONNECTOR_EXISTS',
    }


def _lineage_progress(evolution_module: Any, core: Any) -> dict[str, Any]:
    evo = evolution_module.evolution_status(core)
    rows = list(evo.get('latest_lineages') or [])
    expected = max(1, len(v5_runtime.STRATEGIES) * len(v5_runtime.DIRECTIONS))
    terminal = sum(1 for x in rows if str(x.get('status') or '') not in ('RUNNING', 'WAITING', ''))
    opened = sum(1 for x in rows if int(x.get('holdout_end_ts') or 0) > 0)
    candidates = sum(int(x.get('candidates_evaluated') or 0) for x in rows)
    return {
        'percent': round(100.0 * terminal / expected, 2),
        'terminal_lineages': terminal,
        'expected_lineages': expected,
        'sealed_oos_percent': round(100.0 * opened / expected, 2),
        'sealed_oos_opened': opened,
        'candidates_evaluated': candidates,
        'lineages': rows,
    }


def _execution_progress(core: Any) -> dict[str, Any]:
    con = core.db()
    try:
        tables = {str(r[0]) for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
        sig = int(con.execute("SELECT COUNT(*) FROM model_registry WHERE status='CHAMPION'").fetchone()[0] or 0) if 'model_registry' in tables else 0
        if 'execution_registry_v7' in tables:
            rows = con.execute("SELECT strategy,direction,status,created_at,metrics,policy FROM execution_registry_v7 ORDER BY created_at DESC").fetchall()
        else:
            rows = []
    finally:
        con.close()
    parsed = []
    import json
    for r in rows[:100]:
        try:
            metrics = json.loads(r[4]) if isinstance(r[4], str) else (r[4] or {})
            policy = json.loads(r[5]) if isinstance(r[5], str) else (r[5] or {})
        except Exception:
            metrics, policy = {}, {}
        parsed.append({'strategy': r[0], 'direction': r[1], 'status': r[2], 'created_at': r[3], 'metrics': metrics, 'policy': policy})
    champions = sum(1 for x in parsed if x['status'] == 'CHAMPION')
    rejected = sum(1 for x in parsed if x['status'] == 'REJECTED')
    percent = 100.0 if champions > 0 else (65.0 if rows else (10.0 if sig > 0 else 0.0))
    return {'percent': percent, 'signal_champions': sig, 'execution_champions': champions, 'rejected': rejected, 'recent': parsed[:24]}


def install(core: Any, runtime_integrity: Any, final_system: Any, evolution_module: Any) -> None:
    # Freeze the target first, then replace only the frontier calculation. Existing
    # replay generation, causal feature slicing, and label sequencing remain untouched.
    cutoff = _cutoff(core)
    runtime_integrity._legal_frontier = lambda _core: _fixed_legal_frontier(_core)

    previous_due = final_system._certification_due
    def certification_due_after_fixed_replay(core_obj: Any, snap: dict[str, Any], force: bool) -> bool:
        replay = runtime_integrity.replay_progress(core_obj)
        if not replay.get('complete'):
            core_obj.state['fixed_horizon_certification_gate'] = {
                'ready': False,
                'reason': 'finish the immutable deployment-time historical replay before strategy certification',
                'replay_percent': replay.get('percent'),
                'pending': replay.get('pending_eligible_decisions'),
                'fixed_replay_cutoff_ts': cutoff,
            }
            return False
        core_obj.state['fixed_horizon_certification_gate'] = {
            'ready': True,
            'reason': 'fixed historical replay complete; strategy certification may run',
            'fixed_replay_cutoff_ts': cutoff,
        }
        return bool(previous_due(core_obj, snap, force))
    final_system._certification_due = certification_due_after_fixed_replay

    # Paper/signal metadata is fixed to ETH and 20,000 USDT nominal. Entry/SL/TP are
    # still produced only by the certified learned Execution policy.
    original_create = v7_runtime.create_signal_v7
    def create_signal_eth_20k(core_obj: Any, analysis: dict[str, Any], m15: list[dict[str, Any]]):
        out = original_create(core_obj, analysis, m15)
        if not out:
            return out
        signal_id = str(out.get('signal_id') or '')
        if not signal_id:
            return out
        con = core_obj.db()
        try:
            row = con.execute('SELECT payload FROM signals WHERE signal_id=?', (signal_id,)).fetchone()
            if not row:
                return out
            import json
            payload = json.loads(row[0]) if isinstance(row[0], str) else dict(row[0] or {})
            payload['instrument'] = {'exchange': LIVE_EXCHANGE, 'asset': TRADE_ASSET, 'symbol': TRADE_SYMBOL}
            payload['position_contract'] = {'notional_usdt': PAPER_NOTIONAL_USDT, 'leverage_policy': 'MAX_AVAILABLE_AT_ORDER_TIME'}
            payload['learned_execution_required'] = True
            con.execute('UPDATE signals SET payload=? WHERE signal_id=?', (json.dumps(payload, ensure_ascii=False), signal_id))
            con.commit()
        finally:
            con.close()
        return core_obj.latest_signal()
    v7_runtime.create_signal_v7 = create_signal_eth_20k

    core.state['fixed_horizon_runtime'] = {
        'schema': SCHEMA,
        'fixed_replay_cutoff_ts': cutoff,
        'historical_horizon_moves_with_wall_clock': False,
        'post_cutoff_historical_gap_policy': 'INTENTIONALLY_SKIP_AND_HANDOFF_TO_CURRENT_LIVE_AFTER_CERTIFICATION',
        'trading_contract': _trading_contract(),
    }

    if not any(getattr(route, 'path', None) == '/api/latest/progress-detail' for route in core.app.router.routes):
        @core.app.get('/api/latest/progress-detail')
        def progress_detail() -> dict[str, Any]:
            replay = runtime_integrity.replay_progress(core)
            lineages = _lineage_progress(evolution_module, core)
            execution = _execution_progress(core)
            operational = bool(replay.get('complete') and execution.get('signal_champions', 0) > 0 and execution.get('execution_champions', 0) > 0)
            if operational and not core.get_state(LIVE_HANDOFF_KEY, None):
                core.set_state(LIVE_HANDOFF_KEY, {'at': int(time.time()), 'fixed_replay_cutoff_ts': cutoff})
            handoff = core.get_state(LIVE_HANDOFF_KEY, None) or {}
            return {
                'schema': SCHEMA,
                'fixed_replay_cutoff_ts': cutoff,
                'fixed_replay_cutoff_is_immutable': True,
                'replay': replay,
                'signal_certification': lineages,
                'execution_audit': execution,
                'live_handoff': {
                    'percent': 100.0 if operational else 0.0,
                    'ready': operational,
                    'handoff_at': handoff.get('at'),
                    'skip_between_cutoff_and_handoff': True,
                    'reason': 'candles arriving during historical learning/certification are intentionally not replayed; once certified, scanning resumes from the current live market',
                },
                'trading_contract': _trading_contract(),
                'no_lookahead': {
                    'features_only_from_information_available_at_decision': True,
                    'future_bars_visible_before_plan_freeze': False,
                    'future_bars_after_plan_freeze': 'sequential label/simulation only',
                    'fast_forward_allowed_only_for_waiting_for_outcome': True,
                },
            }
