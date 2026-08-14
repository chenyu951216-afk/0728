from __future__ import annotations

import json
import time
from typing import Any

import execution_v7
import v10_final_integrity as final_integrity
import v25_fixed_horizon_runtime as fixed_horizon

# Historical *decisions* run through the whole Taiwan calendar day 2026-08-01.
# The extra 8h15m is label support only: it is never a new historical decision and
# can only be revealed after that decision's complete Entry/SL/TP plan is frozen.
RESEARCH_DECISION_END_EXCLUSIVE_TS = 1785600000  # 2026-08-02 00:00:00 Asia/Taipei
LABEL_SUPPORT_CUTOFF_TS = 1785629700             # 2026-08-02 08:15:00 Asia/Taipei
RESET_MARKER = 'v29_joint_research_reset_20260801_r2'
_INSTALLED = False
_ORIGINAL_EXEC_SAVE: Any | None = None


def prepare_before_server_v19(core: Any) -> None:
    """Freeze collection before v25 captures its closure-local horizon."""
    core.set_state(fixed_horizon.FIXED_CUTOFF_KEY, LABEL_SUPPORT_CUTOFF_TS)
    core.state['v29_fixed_research_window'] = {
        'research_decision_start_ts': 1577836800,
        'research_decision_end_exclusive_ts': RESEARCH_DECISION_END_EXCLUSIVE_TS,
        'label_support_cutoff_ts': LABEL_SUPPORT_CUTOFF_TS,
        'decisions_after_2026_08_01_allowed': False,
        'post_decision_label_support_only': True,
        'raw_cache_after_research_window_may_be_retained': True,
        'reason': 'retain already-downloaded raw cache; cap historical decisions at 2026-08-01 while preserving enough later bars only to settle the final causal trades',
    }


def _deterministic_market_loader(joint: Any, core: Any) -> dict[str, Any]:
    sources = {tf: final_integrity.deterministic_best_source(core, 'ETH', tf) for tf in ('5m', '15m', '30m', '1h')}
    if not all(sources.values()):
        return {}
    con = core.db()
    try:
        rows5 = con.execute('''SELECT ts,l,h,c FROM market_bars
            WHERE source=? AND asset='ETH' AND tf='5m' AND ts>=? AND ts<? ORDER BY ts''',
            (sources['5m'], joint.RESEARCH_START_TS, LABEL_SUPPORT_CUTOFF_TS)).fetchall()
    finally:
        con.close()
    if len(rows5) < 1000:
        return {}
    import numpy as np
    ts5 = np.asarray([int(r[0]) for r in rows5], dtype=np.int64)
    lo5 = np.asarray([float(r[1]) for r in rows5], dtype=float)
    hi5 = np.asarray([float(r[2]) for r in rows5], dtype=float)
    cl5 = np.asarray([float(r[3]) for r in rows5], dtype=float)
    out: dict[str, Any] = {'ts5': ts5, 'lo5': lo5, 'hi5': hi5, 'cl5': cl5, 'sources': sources}
    for tf in ('15m', '30m', '1h'):
        # Context may include only bars whose opens are before the fixed label-support
        # cutoff. _slice_closed_to still enforces close<=decision at every simulation.
        rows = [x for x in core.load_bars('ETH', tf, sources[tf])
                if joint.RESEARCH_START_TS <= int(x['ts']) < LABEL_SUPPORT_CUTOFF_TS]
        out[tf] = rows
        out[f'ts{tf}'] = [int(x['ts']) for x in rows]
    out['index15'] = {int(x['ts']): i for i, x in enumerate(out['15m'])
                      if int(x['ts']) < RESEARCH_DECISION_END_EXCLUSIVE_TS}
    return out


def _install_execution_metadata_bridge(joint: Any) -> None:
    """Make a joint package satisfy the existing live execution contract exactly."""
    global _ORIGINAL_EXEC_SAVE
    if _ORIGINAL_EXEC_SAVE is not None:
        return
    _ORIGINAL_EXEC_SAVE = execution_v7.ExecutionStore.save
    original = _ORIGINAL_EXEC_SAVE

    def save_joint(self: Any, strategy: str, direction: str, model_version: int,
                   policy: dict[str, Any], metrics: dict[str, Any], promote: bool) -> int:
        out = dict(metrics or {})
        if out.get('joint_signal_execution') or out.get('joint_research_schema'):
            out['schema'] = execution_v7.EXECUTION_SCHEMA
            out['certified'] = bool(promote)
            out['validation_method'] = 'JOINT_SIGNAL_ENTRY_SL_TP_DEV_WALK_FORWARD_THEN_ONE_TIME_UNTOUCHED_AUDIT'
            out['execution_validation_included_in_joint_search'] = True
            out['ev_bootstrap_05'] = out.get('clustered_ev_bootstrap_05')
            out['oos_fills'] = int(out.get('oos_fills') or out.get('effective_oos_selected_n') or 0)
            out['win_rate'] = out.get('test_win')
            out['estimated_all_in_cost_bps'] = float(policy.get('all_in_cost_bps', execution_v7.ALL_IN_COST_BPS))
            out.setdefault('blocked_regimes', [])
            out.setdefault('suspicious_metrics', False)
            out['historical_decision_end_exclusive_ts'] = RESEARCH_DECISION_END_EXCLUSIVE_TS
            out['label_support_cutoff_ts'] = LABEL_SUPPORT_CUTOFF_TS
        return original(self, strategy, direction, model_version, policy, out, promote)

    execution_v7.ExecutionStore.save = save_joint


def install(production: Any, joint: Any) -> None:
    global _INSTALLED
    if _INSTALLED:
        return
    _INSTALLED = True
    core = production.core

    # Force one clean replay migration for this final joint architecture even if an
    # earlier v28 draft already wrote its one-time marker. Raw market/derivative cache
    # remains untouched; only replay-derived samples/models/policies are rebuilt.
    joint.RESET_MARKER = RESET_MARKER
    joint.install(production)

    # v28's research decision end is intentionally midnight. Restore the separate
    # label-support horizon immediately; no background worker has started yet.
    core.set_state(fixed_horizon.FIXED_CUTOFF_KEY, LABEL_SUPPORT_CUTOFF_TS)
    joint._load_market = lambda c: _deterministic_market_loader(joint, c)
    _install_execution_metadata_bridge(joint)

    contract = dict(core.state.get('joint_research_contract') or {})
    contract.update({
        'research_decision_start_ts': joint.RESEARCH_START_TS,
        'research_decision_end_exclusive_ts': RESEARCH_DECISION_END_EXCLUSIVE_TS,
        'label_support_cutoff_ts': LABEL_SUPPORT_CUTOFF_TS,
        'raw_download_reused': True,
        'replay_derived_reset_marker': RESET_MARKER,
        'joint_signal_entry_sl_tp': True,
        'different_regimes_and_phases_can_promote_different_strategy_direction_packages': True,
        'future_data_role': 'forbidden before plan freeze; sequential 5m fill/SL/TP outcome only afterwards',
    })
    core.state['joint_research_contract'] = contract

    original_status = joint.joint_status
    def status_with_window(c: Any) -> dict[str, Any]:
        out = dict(original_status(c) or {})
        out['research_decision_end_exclusive_ts'] = RESEARCH_DECISION_END_EXCLUSIVE_TS
        out['label_support_cutoff_ts'] = LABEL_SUPPORT_CUTOFF_TS
        out['raw_download_reused'] = True
        out['replay_reset_marker'] = RESET_MARKER
        return out
    joint.joint_status = status_with_window

    core.state['v29_joint_runtime'] = {
        'installed_at': int(time.time()),
        'research_decision_end_exclusive_ts': RESEARCH_DECISION_END_EXCLUSIVE_TS,
        'label_support_cutoff_ts': LABEL_SUPPORT_CUTOFF_TS,
        'deterministic_market_source_parity': True,
        'execution_live_contract_bridge': True,
        'separate_signal_then_execution_certification': False,
    }
