from __future__ import annotations

"""Crash-safe persistence for autonomous OOS finalists.

A long research run must not lose a finalist that already passed its one-time OOS just
because the process restarts before final diversity ranking. Passing model blobs are
stored INACTIVE immediately; only after the entire one-time audit stage is complete are
diverse top packages atomically activated. Inactive provisional rows can never trade.
"""

import json
import math
import time
from typing import Any

import v5_runtime
import v17_certification_orchestrator as cert17
import v18_final_system as final_system
import v18_operational_guard as operational_guard

_INSTALLED = False


def install(production: Any, autonomous: Any) -> None:
    global _INSTALLED
    if _INSTALLED:
        return
    _INSTALLED = True
    core = production.core

    base_save_audit = autonomous._save_audit
    def durable_save_audit(c: Any, finalist_id: str, genome: dict[str, Any], result: dict[str, Any]) -> None:
        base_save_audit(c, finalist_id, genome, result)
        if not result.get('promoted') or not result.get('model_blob'):
            return
        sid = autonomous._strategy_id(genome); metrics = dict(result.get('metrics') or {}); gate = list(result.get('gate_thresholds') or []); label = autonomous._behavior_label(genome, gate)
        metrics.update({'strategy_id': sid, 'behavior_label': label, 'provisional_oos_pass': True, 'provisional_finalist_id': finalist_id})
        autonomous._ensure_tables(c); con = c.db()
        try:
            con.execute(f'''INSERT OR REPLACE INTO {autonomous.REGISTRY_TABLE}
                (strategy_id,created_at,status,direction,behavior_label,genome,metrics,model,active)
                VALUES(?,?,?,?,?,?,?,?,0)''', (
                sid, int(time.time()), 'PROVISIONAL_OOS_PASS', str(genome['direction']), label,
                json.dumps(genome, separators=(',', ':'), default=autonomous._json_default),
                json.dumps(metrics, ensure_ascii=False, separators=(',', ':'), default=autonomous._json_default),
                result['model_blob'],
            )); con.commit()
        finally:
            con.close()
    autonomous._save_audit = durable_save_audit

    def activate_complete_set(c: Any) -> list[dict[str, Any]]:
        checkpoint = c.get_state(autonomous.CHECKPOINT_KEY, {})
        if not isinstance(checkpoint, dict) or checkpoint.get('status') != 'COMPLETE':
            return autonomous._load_registry(c, active_only=True)
        rows = autonomous._load_registry(c, active_only=False)
        eligible = [x for x in rows if x.get('model_blob') and x.get('status') in ('CHAMPION', 'PROVISIONAL_OOS_PASS')]
        ranked = sorted(eligible, key=lambda x: (
            float((x.get('metrics') or {}).get('expectancy_r') or -99.0) * 5.0
            + math.log(max(float((x.get('metrics') or {}).get('profit_factor') or 1e-9), 1e-9))
            - float((x.get('metrics') or {}).get('max_drawdown_r') or 999.0) * .01
        ), reverse=True)
        selected: list[dict[str, Any]] = []; diversity: dict[tuple[Any, ...], int] = {}
        for x in ranked:
            key = autonomous._diversity_key(x['genome'])
            if diversity.get(key, 0) >= 2:
                continue
            diversity[key] = diversity.get(key, 0) + 1; selected.append(x)
            if len(selected) >= autonomous.MAX_CHAMPIONS:
                break
        con = c.db()
        try:
            con.execute(f'UPDATE {autonomous.REGISTRY_TABLE} SET active=0 WHERE 1=1')
            for rank, x in enumerate(selected, 1):
                metrics = dict(x['metrics']); metrics['rank'] = rank; metrics['provisional_oos_pass'] = False; metrics['activated_after_complete_audit_set'] = True
                con.execute(f'''UPDATE {autonomous.REGISTRY_TABLE}
                    SET active=1,status='CHAMPION',metrics=? WHERE strategy_id=?''',
                    (json.dumps(metrics, ensure_ascii=False, separators=(',', ':'), default=autonomous._json_default), x['strategy_id']))
            con.commit()
        finally:
            con.close()
        active = autonomous._load_registry(c, active_only=True)
        c.state['autonomous_crash_recovery'] = {
            'checkpoint_complete': True,
            'eligible_oos_passes': len(eligible),
            'active_champions': len(active),
            'inactive_nonselected': max(0, len(eligible) - len(active)),
            'inactive_rows_can_trade': False,
            'updated_at': int(time.time()),
        }
        # Repair in-memory status after a restart that skipped already-audited finalists.
        # The SQLite registry is authoritative; inactive provisional rows were never live.
        previous = c.state.get(autonomous.STATE_KEY) or {}
        c.state[autonomous.STATE_KEY] = {
            **(dict(previous) if isinstance(previous, dict) else {}),
            'schema': autonomous.SCHEMA,
            'status': 'COMPLETE' if active else 'COMPLETE_NO_CERTIFIED_PACKAGE',
            'champions': [
                {'strategy_id': x['strategy_id'], 'direction': x['direction'], 'behavior_label': x['behavior_label'], **(x.get('metrics') or {})}
                for x in active
            ],
            'updated_at': int(time.time()),
        }
        return active

    base_certify = final_system.certify_and_execute
    def durable_certify(c: Any, force: bool = False):
        result = base_certify(c, force)
        activate_complete_set(c)
        return result
    final_system.certify_and_execute = durable_certify
    operational_guard.certify_and_execute = durable_certify
    cert17.train_v17 = durable_certify
    v5_runtime.train_v5 = durable_certify
    core.train_if_due = lambda force=False: durable_certify(core, force)

    core.state['autonomous_checkpoint_contract'] = {
        'generation_elites_persisted': True,
        'oos_pass_model_persisted_before_final_ranking': True,
        'provisional_oos_pass_active': False,
        'activation_requires_complete_audit_set': True,
        'restart_cannot_reopen_already_audited_holdout': True,
        'sqlite_registry_repairs_in_memory_status_after_restart': True,
        'updated_at': int(time.time()),
    }
