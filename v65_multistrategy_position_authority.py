from __future__ import annotations

"""V65 final multi-strategy/current-position authority.

Current-time invariant:
- every completed active strategy is evaluated on every live decision when there is no
  position;
- score-qualified provisional finalists are reconciled into the active registry up to
  the configured cap even when one or more provisionals already exist;
- current entry must fail closed until the completed-strategy roster is fully present;
- only after all strategies independently qualify does the V63 arbiter choose the best
  simultaneous signal.

Historical/OOS results, score caps and V56 execution semantics are unchanged.
"""

import time
from typing import Any, Callable

import runtime_identity

VERSION = 'V65_MULTISTRATEGY_POSITION_AUTHORITY'
SCHEMA = 65
STATE_KEY = 'v65_multistrategy_position_authority'

_CONFIGURED = False
_INSTALLED = False


def _now() -> int:
    return int(time.time())


def _d(v: Any) -> dict[str, Any]:
    return dict(v) if isinstance(v, dict) else {}


def _state(core: Any, **patch: Any) -> dict[str, Any]:
    z = _d(core.state.get(STATE_KEY)); z.update(patch)
    z.update({'schema': SCHEMA, 'runtime': VERSION,
              'public_runtime': runtime_identity.RUNTIME_VERSION, 'updated_at': _now()})
    core.state[STATE_KEY] = z
    return z


def _provisional_strategy_id(autonomous: Any, row: dict[str, Any]) -> str:
    """Match V61._persist_provisional's deterministic strategy id without refitting."""
    try:
        genome = dict(row.get('genome') or {})
        return 'AUTO_PROV_' + str(autonomous._hash_payload(genome, 12)).upper()
    except Exception:
        return ''


def _ids(items: list[dict[str, Any]]) -> list[str]:
    return [str(x.get('strategy_id') or x.get('strategy') or '') for x in items
            if str(x.get('strategy_id') or x.get('strategy') or '')]


def configure_worker(v61: Any) -> None:
    """Patch V61 before V63 starts its daemon so strict + all score strategies coexist."""
    global _CONFIGURED
    if _CONFIGURED:
        return
    _CONFIGURED = True

    def coexist_worker(core: Any, autonomous: Any, pipeline52: Any) -> None:
        try:
            cp = _d(core.get_state(autonomous.CHECKPOINT_KEY, {}))
            strict = list(v61._strict_champions(core, autonomous) or [])

            if not bool(getattr(v61, 'ENABLED', True)):
                active = list(autonomous._load_registry(core, active_only=True) or [])
                ready = bool(active)
                v61._state(core, status='DISABLED', historical_strict_champion_count=len(strict),
                           provisional_count=0, current_time_roster_ready=ready)
                _state(core, current_time_roster_ready=ready,
                       completed_current_time_strategy_count=len(active),
                       active_current_time_strategy_count=len(active),
                       expected_current_time_strategy_ids=_ids(active),
                       active_current_time_strategy_ids=_ids(active),
                       missing_current_time_strategy_ids=[],
                       roster_reason='provisional worker disabled; strict/active registry is authoritative')
                return

            if cp.get('status') != 'COMPLETE':
                v61._state(core, status='WAITING_HISTORICAL_TERMINAL')
                _state(core, current_time_roster_ready=False,
                       roster_reason='historical checkpoint is not terminal yet')
                return

            # IMPORTANT: do NOT return merely because one provisional already exists.
            # The old implementation did that and left Current-Time evaluating only that
            # single strategy even when five completed strategies were available.
            rows = list(v61._rejected_rows(core, autonomous, pipeline52) or [])
            max_provisionals = max(0, int(getattr(v61, 'MAX_PROVISIONALS', len(rows) or 0)))
            if max_provisionals:
                rows = rows[:max_provisionals]
            desired_score_ids = [sid for sid in (_provisional_strategy_id(autonomous, r) for r in rows) if sid]

            existing_before = list(v61._existing_provisionals(core, autonomous) or [])
            existing_ids = set(_ids(existing_before))
            newly_promoted: list[dict[str, Any]] = []
            refit_errors: list[dict[str, Any]] = []

            for row in rows:
                wanted_sid = _provisional_strategy_id(autonomous, row)
                if wanted_sid and wanted_sid in existing_ids:
                    continue
                try:
                    v61._state(core, status='REFITTING_FROZEN_PROVISIONAL',
                               finalist_rank=row.get('rank'), finalist_id=row.get('finalist_id'),
                               rationale=row.get('rationale'),
                               historical_strict_champion_count=len(strict),
                               existing_provisional_count=len(existing_ids),
                               target_provisional_count=len(rows))
                    fitted = v61._refit_frozen_package(core, autonomous, row)
                    saved = v61._persist_provisional(core, autonomous, pipeline52, row, fitted)
                    newly_promoted.append(saved)
                    saved_sid = str(saved.get('strategy_id') or wanted_sid)
                    if saved_sid:
                        existing_ids.add(saved_sid)
                except Exception as exc:
                    refit_errors.append({
                        'finalist_id': str(row.get('finalist_id') or ''),
                        'rank': int(row.get('rank') or 0),
                        'strategy_id': wanted_sid,
                        'error': f'{type(exc).__name__}: {exc}',
                    })
                    # One broken candidate must not stop the other completed strategies
                    # from being restored into Current-Time.
                    continue

            existing_after = list(v61._existing_provisionals(core, autonomous) or [])
            strict_after = list(v61._strict_champions(core, autonomous) or [])
            active_registry = list(autonomous._load_registry(core, active_only=True) or [])
            active_ids = set(_ids(active_registry))
            expected_ids = set(_ids(strict_after)) | set(desired_score_ids)
            missing_ids = sorted(expected_ids - active_ids)
            roster_ready = bool(expected_ids) and not missing_ids and not refit_errors

            if roster_ready:
                status = 'PROVISIONAL_CURRENT_PAPER_READY' if existing_after else 'STRICT_ONLY_CURRENT_PAPER_READY'
            elif expected_ids:
                status = 'CURRENT_PAPER_ROSTER_INCOMPLETE'
            else:
                status = 'NO_SCORE_PROVISIONAL_CANDIDATE' if not strict_after else 'STRICT_ONLY_CURRENT_PAPER_READY'
                roster_ready = bool(strict_after)

            v61._state(
                core,
                status=status,
                provisionals=existing_after,
                provisional_count=len(existing_after),
                target_provisional_count=len(rows),
                newly_promoted_count=len(newly_promoted),
                newly_promoted_ids=_ids(newly_promoted),
                historical_strict_champion_count=len(strict_after),
                strict_and_provisional_can_coexist=True,
                current_time_roster_ready=roster_ready,
                expected_current_time_strategy_count=len(expected_ids),
                active_current_time_strategy_count=len(active_ids),
                missing_current_time_strategy_ids=missing_ids,
                refit_errors=refit_errors,
            )
            _state(
                core,
                current_time_roster_ready=roster_ready,
                parallel_current_time_evaluation=True,
                no_single_strategy_short_circuit=True,
                completed_current_time_strategy_count=len(expected_ids),
                active_current_time_strategy_count=len(active_ids),
                expected_current_time_strategy_ids=sorted(expected_ids),
                active_current_time_strategy_ids=sorted(active_ids),
                missing_current_time_strategy_ids=missing_ids,
                provisional_target_count=len(rows),
                provisional_existing_before_count=len(existing_before),
                provisional_active_after_count=len(existing_after),
                provisional_added_count=len(newly_promoted),
                provisional_added_ids=_ids(newly_promoted),
                roster_refit_errors=refit_errors,
                roster_reason=(
                    'all completed strategies are present and may be evaluated together'
                    if roster_ready else
                    'entry remains fail-closed until every completed strategy is present'
                ),
            )
        except Exception as exc:
            v61._state(core, status='PROVISIONAL_REFIT_ERROR', error=f'{type(exc).__name__}: {exc}',
                       current_time_roster_ready=False)
            _state(core, current_time_roster_ready=False,
                   roster_reason='provisional roster reconciliation failed',
                   roster_error=f'{type(exc).__name__}: {exc}')

    v61._worker = coexist_worker


def _preemptive_update(core: Any, v63: Any, v64: Any, base: Callable[[dict[str, Any]], Any]):
    def update(bar: dict[str, Any]) -> Any:
        active = core.latest_signal(); analysis = _d(core.state.get('v63_current_analysis')); sel = _d(analysis.get('selection'))
        if (active and str(active.get('status')) == 'PLANNED' and sel.get('tradeable')
                and sel.get('v63_reversal_authorized')
                and str(sel.get('direction')) != str(active.get('direction'))):
            sid = str(active.get('signal_id'))
            v64._cancel_planned(core, active, 'AUTONOMOUS_OPPOSITE_STRATEGY_REPLACED')
            v63._enqueue(core, sid + ':opposite-cancel', sid, 'CANCEL', 'ETH Paper 原掛單取消',
                         f"原策略：{active.get('strategy')}｜{active.get('direction')}\n"
                         f"原因：反方向策略 {sel.get('strategy')} 的當下分數更高並通過反轉門檻。\n"
                         "原 LIMIT 尚未成交，因此直接取消，不產生虛構損益。", 16753920)
            _state(core, preemptive_planned_cancel=True, cancelled_signal=sid,
                   next_strategy=sel.get('strategy'), same_bar_fake_fill_prevented=True)
            return core.latest_signal()
        return base(bar)
    return update


def install(production: Any, v63: Any, v64: Any) -> None:
    global _INSTALLED
    if _INSTALLED:
        return
    _INSTALLED = True; core = production.core
    core.update_signal_with_bar = _preemptive_update(core, v63, v64, core.update_signal_with_bar)
    prior = _d(core.state.get(STATE_KEY))
    _state(core, status='READY', strict_and_score_strategies_can_coexist=True,
           live_arbiter_compares_all_active_completed_strategies=True,
           parallel_current_time_evaluation=True,
           no_single_strategy_short_circuit=True,
           current_time_roster_ready=bool(prior.get('current_time_roster_ready', False)),
           one_same_direction_position_until_exit=True, opposite_direction_may_reverse=True,
           unfilled_limit_cancelled_before_reversal_bar_processing=True,
           historical_oos_changed=False, score_caps_changed=False,
           execution_semantics_changed=False, future_peeking_enabled=False, paper_only=True)
    role = core.state.get('bootstrap_replica_role')
    if isinstance(role, dict):
        role.update({'final_runtime_overlay': VERSION, 'production_entry': 'server_entry_v65.py',
                     'strict_and_score_strategies_can_coexist': True,
                     'all_completed_strategies_evaluated_in_parallel': True,
                     'partial_current_roster_can_open_new_signal': False,
                     'preemptive_unfilled_opposite_cancel': True,
                     'same_bar_fake_fill_prevented': True,
                     'historical_oos_rewritten_by_v65': False})
    runtime_identity.stamp(core)
