from __future__ import annotations

"""V65 final multi-strategy/current-position authority.

Two lifecycle fixes are intentionally separated from model semantics:
1) a strict Champion no longer prevents additional score-qualified PAPER strategies
   from being preserved, so the live arbiter can compare several genuinely different
   completed strategies;
2) if an opposite strategy wins while the current LIMIT setup is still unfilled, the
   old setup is cancelled before processing another 5m bar.  It cannot fill and then be
   closed seconds later from the same already-known reversal decision.

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


def configure_worker(v61: Any) -> None:
    """Patch V61 before V63 starts its daemon so strict + score tiers may coexist."""
    global _CONFIGURED
    if _CONFIGURED:
        return
    _CONFIGURED = True

    def coexist_worker(core: Any, autonomous: Any, pipeline52: Any) -> None:
        try:
            cp = _d(core.get_state(autonomous.CHECKPOINT_KEY, {}))
            if not bool(getattr(v61, 'ENABLED', True)):
                v61._state(core, status='DISABLED'); return
            if cp.get('status') != 'COMPLETE':
                v61._state(core, status='WAITING_HISTORICAL_TERMINAL'); return
            existing = list(v61._existing_provisionals(core, autonomous) or [])
            strict = list(v61._strict_champions(core, autonomous) or [])
            if existing:
                v61._state(core, status='PROVISIONAL_CURRENT_PAPER_READY', provisionals=existing,
                           provisional_count=len(existing), historical_strict_champion_count=len(strict),
                           strict_and_provisional_can_coexist=True)
                return
            rows = list(v61._rejected_rows(core, autonomous, pipeline52) or [])
            if not rows:
                v61._state(core, status=('STRICT_ONLY_CURRENT_PAPER_READY' if strict else 'NO_SCORE_PROVISIONAL_CANDIDATE'),
                           historical_strict_champion_count=len(strict), provisional_count=0,
                           strict_and_provisional_can_coexist=True)
                return
            promoted = []
            for row in rows:
                v61._state(core, status='REFITTING_FROZEN_PROVISIONAL', finalist_rank=row['rank'],
                           finalist_id=row['finalist_id'], rationale=row['rationale'],
                           historical_strict_champion_count=len(strict))
                fitted = v61._refit_frozen_package(core, autonomous, row)
                promoted.append(v61._persist_provisional(core, autonomous, pipeline52, row, fitted))
            v61._state(core, status='PROVISIONAL_CURRENT_PAPER_READY', provisionals=promoted,
                       provisional_count=len(promoted), historical_strict_champion_count=len(strict),
                       strict_and_provisional_can_coexist=True)
        except Exception as exc:
            v61._state(core, status='PROVISIONAL_REFIT_ERROR', error=f'{type(exc).__name__}: {exc}')

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
    _state(core, status='READY', strict_and_score_strategies_can_coexist=True,
           live_arbiter_compares_all_active_completed_strategies=True,
           one_same_direction_position_until_exit=True, opposite_direction_may_reverse=True,
           unfilled_limit_cancelled_before_reversal_bar_processing=True,
           historical_oos_changed=False, score_caps_changed=False,
           execution_semantics_changed=False, future_peeking_enabled=False, paper_only=True)
    role = core.state.get('bootstrap_replica_role')
    if isinstance(role, dict):
        role.update({'final_runtime_overlay': VERSION, 'production_entry': 'server_entry_v65.py',
                     'strict_and_score_strategies_can_coexist': True,
                     'preemptive_unfilled_opposite_cancel': True,
                     'same_bar_fake_fill_prevented': True,
                     'historical_oos_rewritten_by_v65': False})
    runtime_identity.stamp(core)
