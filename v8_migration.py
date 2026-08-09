from __future__ import annotations

import json
import time
from typing import Any

import v8_evolution


def install(core: Any) -> None:
    key = 'evolution_migration_schema'
    if int(core.get_state(key, 0) or 0) == v8_evolution.GENOME_SCHEMA:
        return
    now = int(time.time())
    con = core.db()
    # Historical point-in-time samples remain intact. Only deployable artifacts are
    # retired because their model architecture/Execution OOF no longer matches the
    # new Genome search architecture.
    con.execute("UPDATE model_registry SET status='ARCHIVED' WHERE status='CHAMPION'")
    if con.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='execution_registry_v7'").fetchone():
        con.execute("UPDATE execution_registry_v7 SET status='ARCHIVED' WHERE status='CHAMPION'")
    planned = con.execute("SELECT signal_id,payload FROM signals WHERE status='PLANNED'").fetchall()
    for signal_id, raw_payload in planned:
        payload = json.loads(raw_payload) if isinstance(raw_payload, str) else (raw_payload or {})
        payload['superseded_reason'] = 'Evolution upgrade requires a Genome Signal Champion and matching evolving point-in-time Execution OOF audit.'
        con.execute("UPDATE signals SET status='EXPIRED',updated_at=?,payload=? WHERE signal_id=?", (now, json.dumps(payload, ensure_ascii=False), signal_id))
    opened = con.execute("SELECT signal_id,payload FROM signals WHERE status='OPEN'").fetchall()
    for signal_id, raw_payload in opened:
        payload = json.loads(raw_payload) if isinstance(raw_payload, str) else (raw_payload or {})
        payload['legacy_pre_evolution_open_plan'] = True
        payload.setdefault('management', {})['legacy_note'] = 'Original immutable plan remains monitored; excluded from Evolution certification.'
        con.execute('UPDATE signals SET payload=? WHERE signal_id=?', (json.dumps(payload, ensure_ascii=False), signal_id))
    con.commit(); con.close()

    # Force one clean retraining pass from the already-cached point-in-time samples.
    core.set_state('v5_last_train_sample_total', 0)
    core.set_state('last_train_ts_v5', 0)
    core.set_state('v7_execution_signal_signature', [])
    core.set_state('v7_execution_last_attempt_ts', 0)
    core.set_state('evolution_live_evidence_pending', 0)
    core.set_state(key, v8_evolution.GENOME_SCHEMA)
