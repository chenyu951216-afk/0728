from __future__ import annotations

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
_ORIGINAL_PIPELINE_STATUS: Any | None = None
_ORIGINAL_TRADING_CONTRACT: Any | None = None


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


def _install_trading_contract_bridge() -> None:
    global _ORIGINAL_TRADING_CONTRACT
    if _ORIGINAL_TRADING_CONTRACT is not None:
        return
    _ORIGINAL_TRADING_CONTRACT = fixed_horizon._trading_contract
    original = _ORIGINAL_TRADING_CONTRACT

    def joint_contract() -> dict[str, Any]:
        out = dict(original() or {})
        out.update({
            'signal_source': 'JOINT_STRATEGY_PACKAGE_CHAMPION_ONLY',
            'entry_stop_targets_source': 'JOINT_STRATEGY_PACKAGE_CHAMPION_ONLY',
            'joint_signal_entry_sl_tp_learning': True,
            'historical_decision_end_exclusive_ts': RESEARCH_DECISION_END_EXCLUSIVE_TS,
            'label_support_cutoff_ts': LABEL_SUPPORT_CUTOFF_TS,
        })
        return out

    fixed_horizon._trading_contract = joint_contract


def _install_pipeline_bridge(joint: Any, core: Any) -> None:
    """Replace legacy Signal-then-Execution stage semantics with the real joint pipeline."""
    global _ORIGINAL_PIPELINE_STATUS
    if _ORIGINAL_PIPELINE_STATUS is not None:
        return
    import v22_hierarchical_pipeline as pipeline
    _ORIGINAL_PIPELINE_STATUS = pipeline.pipeline_status
    original = _ORIGINAL_PIPELINE_STATUS

    def joint_pipeline_status(c: Any) -> dict[str, Any]:
        base = dict(original(c) or {})
        status = joint.joint_status(c)
        expected = max(1, int(status.get('expected_lineages') or 1))
        terminal = int(status.get('terminal_lineages') or 0)
        promoted = int(status.get('promoted_packages') or 0)
        active = status.get('active') if isinstance(status.get('active'), dict) else {}
        completed_rows = list(status.get('lineages') or [])
        oos_tested = sum(1 for x in completed_rows if str(x.get('status') or '') in ('PROMOTED', 'REJECTED_JOINT_SEALED_OOS'))
        rejected_before_oos = max(0, terminal - oos_tested)

        pct = float(status.get('percent') or 0.0)
        if active and terminal < expected:
            generation = int(active.get('generation') or 0)
            candidate = int(active.get('candidate') or 0)
            population = max(1, int(active.get('population') or 1))
            generations = max(1, int(getattr(joint, 'GENERATIONS', 1)))
            within = min(.995, (generation + min(1.0, candidate / population)) / generations)
            pct = 100.0 * min(expected, terminal + within) / expected

        old_stages = list(base.get('stages') or [])
        stages = old_stages[:5]
        replay_complete = bool(stages and str(stages[-1].get('status') or '') == 'COMPLETE')
        joint_status_text = 'COMPLETE' if terminal >= expected else 'WAITING' if not replay_complete else 'RUNNING'
        stages.extend([
            pipeline._stage('6. JOINT_SIGNAL_ENTRY_SL_TP_EVOLUTION', pct, joint_status_text, {
                'joint_package': True,
                'terminal_lineages': terminal,
                'expected_lineages': expected,
                'promoted_packages': promoted,
                'active': active,
                'rule': 'Signal/regime/phase/Entry/SL/TP/allocations/management evolve and are scored together on chronological development folds',
            }),
            pipeline._stage('7. COMPLETE_PACKAGE_GENERALIZATION_CHECK', 100.0 * terminal / expected,
                            'COMPLETE' if terminal >= expected else 'WAITING' if not replay_complete else 'RUNNING', {
                'terminal_lineages': terminal,
                'complete_packages_tested_on_untouched_oos': oos_tested,
                'rejected_before_opening_oos': rejected_before_oos,
                'expected_lineages': expected,
                'rule': 'the untouched block never tunes the package; after a pass only the estimator is refit on all fixed history while package rules remain frozen',
            }),
            pipeline._stage('8. JOINT_PACKAGE_CERTIFICATION', 100.0 * terminal / expected,
                            'COMPLETE' if terminal >= expected else 'WAITING' if not replay_complete else 'RUNNING', {
                'promoted_packages': promoted,
                'rejected_packages': max(0, terminal - promoted),
                'expected_lineages': expected,
                'live_requires_complete_signal_and_execution_metadata': True,
            }),
        ])
        weights = (10, 10, 10, 10, 25, 20, 10, 5)
        overall = pipeline._pct(sum(float(stage.get('percent') or 0.0) * weight for stage, weight in zip(stages, weights)) / 100.0)
        active_stage = next((stage for stage in stages if float(stage.get('percent') or 0.0) < 99.5), stages[-1])
        operational = bool(promoted > 0 and replay_complete)
        base.update({
            'overall_percent': overall,
            'active_stage': active_stage['name'],
            'operational': operational,
            'final_status': 'FULLY_OPERATIONAL' if operational and terminal >= expected else 'JOINT_RESEARCH_RUNNING' if replay_complete else base.get('final_status'),
            'stages': stages,
            'joint_strategy_research': status,
            'joint_signal_then_execution_separation': False,
        })
        no_lookahead = dict(base.get('no_lookahead_contract') or {})
        no_lookahead.update({
            'joint_signal_and_execution_package': True,
            'historical_decisions_end_before_2026_08_02_taipei': True,
            'future_path_for_execution_only_after_plan_freeze': True,
        })
        base['no_lookahead_contract'] = no_lookahead
        c.state['hierarchical_pipeline'] = base
        return base

    pipeline.pipeline_status = joint_pipeline_status


def _install_dashboard_bridge(production: Any) -> None:
    """Remove legacy duplicate stage labels without touching research behavior."""
    from fastapi.responses import HTMLResponse
    app = production.core.app
    root_route = next((route for route in app.router.routes if getattr(route, 'path', None) == '/'), None)
    if root_route is None or getattr(root_route, 'name', '') == 'joint_v29_dashboard':
        return
    original_endpoint = root_route.endpoint
    app.router.routes = [route for route in app.router.routes if getattr(route, 'path', None) != '/']

    @app.get('/', response_class=HTMLResponse, name='joint_v29_dashboard')
    def joint_v29_dashboard() -> str:
        raw = original_endpoint()
        html = raw.body.decode() if hasattr(raw, 'body') else str(raw)
        overlay = r'''
<script id="joint-v29-dashboard-overlay">
(function(){
  function patchJointLabels(){
    document.querySelectorAll('h2').forEach(function(h){
      if((h.textContent||'').indexOf('Fixed-Horizon Final Authority')>=0){h.textContent='🧭 完整學習進度 / Joint Strategy Research';}
    });
    document.querySelectorAll('.v25name').forEach(function(n){
      var t=(n.textContent||'').trim();
      if(t.indexOf('6. HIERARCHICAL_DEV_EVOLUTION')===0)n.textContent='6. JOINT_SIGNAL_ENTRY_SL_TP_EVOLUTION';
      if(t.indexOf('7. ONE_TIME_SEALED_OOS')===0)n.textContent='7. COMPLETE_PACKAGE_GENERALIZATION_CHECK';
      if(t.indexOf('8. ENTRY_SL_TP_UNTOUCHED_AUDIT')===0)n.textContent='8. JOINT_PACKAGE_CERTIFICATION';
      if(t.indexOf('9. SIGNAL_CERTIFICATION')===0||t.indexOf('10. SEALED_OOS')===0||t.indexOf('11. ENTRY_SL_TP_EXECUTION_AUDIT')===0){
        var box=n.closest('.v25stage');if(box)box.style.display='none';
      }
      if(t.indexOf('12. CURRENT_LIVE_HANDOFF')===0)n.textContent='9. CURRENT_LIVE_HANDOFF';
    });
    document.querySelectorAll('.v25pill').forEach(function(p){
      if((p.textContent||'').indexOf('Entry / SL / TP：')>=0){p.innerHTML='Signal / Entry / SL / TP：<b>JOINT_PACKAGE_CHAMPION_ONLY</b>';}
    });
    document.querySelectorAll('.v25lineage').forEach(function(x){
      var t=x.textContent||'';
      if((t.indexOf('NO_ELIGIBLE')>=0||t.indexOf('INSUFFICIENT')>=0||t.indexOf('NO_JOINT_')>=0)&&t.indexOf('PF 0.00')>=0){
        x.innerHTML=x.innerHTML.replace('PF 0.00 · EV 0.000R','PF — · EV —');
      }
    });
  }
  patchJointLabels();setInterval(patchJointLabels,750);
})();
</script>
'''
        return html.replace('</body>', overlay + '</body>') if '</body>' in html else html + overlay


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
    _install_trading_contract_bridge()

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

    _install_pipeline_bridge(joint, core)
    _install_dashboard_bridge(production)

    core.state['v29_joint_runtime'] = {
        'installed_at': int(time.time()),
        'research_decision_end_exclusive_ts': RESEARCH_DECISION_END_EXCLUSIVE_TS,
        'label_support_cutoff_ts': LABEL_SUPPORT_CUTOFF_TS,
        'deterministic_market_source_parity': True,
        'execution_live_contract_bridge': True,
        'dashboard_joint_stage_semantics': True,
        'pipeline_joint_stage_semantics': True,
        'separate_signal_then_execution_certification': False,
    }
