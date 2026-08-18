from __future__ import annotations

"""Recover the tiny terminal-commit gap after a completed Stage-7 OOS run.

V53 is deliberately outside the V47 research semantic fingerprint. It does not change
history, features, candidate evaluation, leverage, OOS thresholds, models or trade
simulation. It only reconciles durable evidence after the research work is already done:

* a COMPLETE checkpoint must outrank the stale last ONE_TIME_COMPLETE_PACKAGE_OOS marker;
* after a process dies in the few instructions between saving every OOS audit/champion
  and writing the terminal checkpoint, V53 can reconstruct that terminal commit from
  the durable strategy vault once no certification Future is active;
* Current Paper still requires a durable certified Champion and terminal historical
  checkpoint before the existing V52 signal gate can open.
"""

import time
from typing import Any

import runtime_identity

VERSION = 'V53_TERMINAL_HANDOFF_RECOVERY'
SCHEMA = 53
STATE_KEY = 'v53_terminal_handoff_recovery'
STALE_TAIL_SECONDS = 90
_RUNNING_STAGES = {'DIRECT_R_AUTONOMOUS_EVOLUTION', 'ONE_TIME_COMPLETE_PACKAGE_OOS'}
_INSTALLED = False


def _now() -> int:
    return int(time.time())


def _dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _state(core: Any, **patch: Any) -> dict[str, Any]:
    raw = _dict(core.state.get(STATE_KEY))
    raw.update(patch)
    raw.update({
        'schema': SCHEMA,
        'runtime': VERSION,
        'public_runtime': runtime_identity.RUNTIME_VERSION,
        'updated_at': _now(),
    })
    core.state[STATE_KEY] = raw
    return raw


def _future_active(transition: Any) -> bool:
    future = getattr(transition, '_CERT_FUTURE', None)
    if future is None:
        return False
    try:
        return not bool(future.done())
    except Exception:
        return True


def _checkpoint_is_authoritative(cp: dict[str, Any], active: dict[str, Any]) -> bool:
    if str(cp.get('status') or '') != 'COMPLETE':
        return False
    stage = str(active.get('stage') or '')
    if stage not in _RUNNING_STAGES:
        return True
    cp_updated = int(cp.get('updated_at') or 0)
    active_updated = int(active.get('updated_at') or 0)
    # A newer active cursor means a later exact run really did start. An older/equal
    # OOS marker is merely the last progress event from the run that already committed.
    if cp_updated > 0 and active_updated > cp_updated:
        return False
    return cp_updated > 0


def _terminal_marker(core: Any, cp: dict[str, Any], active: dict[str, Any], *, recovered: bool) -> None:
    if str(active.get('stage') or '') in _RUNNING_STAGES:
        core.state['v53_terminal_progress_archive'] = dict(active)
    core.state['autonomous_live_progress'] = {
        'stage': 'HISTORICAL_CERTIFICATION_COMPLETE',
        'terminal': True,
        'terminal_checkpoint_status': 'COMPLETE',
        'champions': int(cp.get('champions') or 0),
        'finalists': int(cp.get('finalists') or 0),
        'generation': int(cp.get('generation') or 0) + 1 if cp.get('generation') is not None else None,
        'updated_at': int(cp.get('updated_at') or _now()),
        'v53_tail_recovered': bool(recovered),
    }


def _vault_truth(core: Any, pipeline52: Any, throughput: Any) -> tuple[str, dict[str, int]]:
    run = str(pipeline52._run(core, throughput) or '')
    counts = dict(pipeline52._counts(core, run) if run else pipeline52._counts(core) or {})
    return run, {
        'saved': int(counts.get('saved') or 0),
        'finalists': int(counts.get('finalists') or 0),
        'audited': int(counts.get('audited') or 0),
        'champions': int(counts.get('champions') or 0),
    }


def reconcile(core: Any, autonomous: Any, throughput: Any, pipeline52: Any,
              transition: Any) -> dict[str, Any]:
    cp = _dict(core.get_state(autonomous.CHECKPOINT_KEY, {}))
    active = _dict(core.state.get('autonomous_live_progress'))

    if _checkpoint_is_authoritative(cp, active):
        _terminal_marker(core, cp, active, recovered=bool(cp.get('v53_tail_recovered')))
        return _state(
            core, status='TERMINAL_CHECKPOINT_AUTHORITATIVE',
            terminal=True, tail_recovered=bool(cp.get('v53_tail_recovered')),
            checkpoint=cp,
        )

    run, vault = _vault_truth(core, pipeline52, throughput)
    orch = _dict(core.state.get('v49_stage6_atomic_orchestration'))
    checkpoint_counts = _dict(orch.get('checkpoint_counts'))
    total = max(1, int(getattr(autonomous, 'POPULATION', 48)) * int(getattr(autonomous, 'GENERATIONS', 8)))
    persisted = int(checkpoint_counts.get('persisted') or 0)
    stage = str(active.get('stage') or '')
    age = max(0, _now() - int(active.get('updated_at') or _now()))
    err = orch.get('error') or orch.get('future_error')

    durable_oos_complete = bool(
        stage == 'ONE_TIME_COMPLETE_PACKAGE_OOS' and
        vault['finalists'] > 0 and
        vault['audited'] >= vault['finalists'] and
        persisted >= total and
        not err
    )

    if durable_oos_complete and age >= STALE_TAIL_SECONDS and not _future_active(transition):
        champions = list(autonomous._load_registry(core, active_only=True) or [])
        now = _now()
        recovered_cp = {
            'schema': int(getattr(autonomous, 'SCHEMA', 30)),
            'status': 'COMPLETE',
            'generation': max(0, int(getattr(autonomous, 'GENERATIONS', 8)) - 1),
            'finalists': int(vault['finalists']),
            'champions': int(len(champions)),
            'updated_at': now,
            'v53_tail_recovered': True,
            'v53_run_id': run,
            'recovery_evidence': {
                'persisted_candidates': persisted,
                'expected_candidates': total,
                'vault': vault,
                'certification_future_active': False,
                'orchestration_error': None,
            },
        }
        core.set_state(autonomous.CHECKPOINT_KEY, recovered_cp)
        state_key = str(getattr(autonomous, 'STATE_KEY', 'autonomous_strategy_discovery_v30'))
        core.state[state_key] = {
            'schema': int(getattr(autonomous, 'SCHEMA', 30)),
            'status': 'COMPLETE' if champions else 'COMPLETE_NO_CERTIFIED_PACKAGE',
            'champions': [
                {'strategy_id': x.get('strategy_id'), 'direction': x.get('direction'),
                 'behavior_label': x.get('behavior_label'), **_dict(x.get('metrics'))}
                for x in champions
            ],
            'finalists': int(vault['finalists']),
            'audits': int(vault['audited']),
            'v53_tail_recovered': True,
            'updated_at': now,
        }
        _terminal_marker(core, recovered_cp, active, recovered=True)
        return _state(
            core, status='TERMINAL_COMMIT_RECOVERED_FROM_DURABLE_OOS', terminal=True,
            tail_recovered=True, run_id=run, checkpoint=recovered_cp, vault=vault,
        )

    return _state(
        core, status='WAITING_FOR_AUTHORITATIVE_TERMINAL_COMMIT', terminal=False,
        tail_recovered=False, run_id=run, vault=vault,
        active_stage=stage, active_age_seconds=age,
        certification_future_active=_future_active(transition),
        persisted_candidates=persisted, expected_candidates=total,
        orchestration_error=err,
    )


def install(production: Any, autonomous: Any, throughput: Any, pipeline52: Any,
            transition: Any, pipeline_module: Any) -> None:
    global _INSTALLED
    if _INSTALLED:
        return
    _INSTALLED = True
    core = production.core

    # Reconcile once at boot, then before every public status read. V52's own status
    # functions become correct automatically because the stale running marker is replaced
    # by a terminal marker only when the durable checkpoint/evidence proves completion.
    reconcile(core, autonomous, throughput, pipeline52, transition)

    base_auto_status = autonomous.autonomous_status
    def terminal_auto_status(c: Any) -> dict[str, Any]:
        reconcile(c, autonomous, throughput, pipeline52, transition)
        return dict(base_auto_status(c) or {})
    autonomous.autonomous_status = terminal_auto_status

    base_pipeline_status = pipeline_module.pipeline_status
    def terminal_pipeline_status(c: Any) -> dict[str, Any]:
        reconcile(c, autonomous, throughput, pipeline52, transition)
        return dict(base_pipeline_status(c) or {})
    pipeline_module.pipeline_status = terminal_pipeline_status

    base_create_signal = core.create_signal
    def terminal_create_signal(analysis: dict[str, Any], m15: list[dict[str, Any]]):
        reconcile(core, autonomous, throughput, pipeline52, transition)
        return base_create_signal(analysis, m15)
    core.create_signal = terminal_create_signal

    core.state.setdefault('strict_replay', {})['v53_terminal_handoff_recovery'] = {
        'schema': SCHEMA,
        'research_semantics_changed': False,
        'v47_exact_dataset_identity_changed': False,
        'raw_history_deleted': False,
        'replay_reset': False,
        'candidate_archive_reset': False,
        'oos_thresholds_changed': False,
        'future_peeking_enabled': False,
        'terminal_recovery_requires_all_saved_finalists_audited': True,
        'terminal_recovery_requires_full_candidate_persistence': True,
        'terminal_recovery_requires_no_active_certification_future': True,
        'current_paper_still_requires_v52_terminal_checkpoint_and_champion': True,
    }

    app = core.app
    if not any(getattr(r, 'path', None) == '/api/v53/terminal-handoff' for r in app.router.routes):
        @app.get('/api/v53/terminal-handoff')
        def terminal_handoff_api() -> dict[str, Any]:
            return reconcile(core, autonomous, throughput, pipeline52, transition)

    runtime_identity.stamp(core)
