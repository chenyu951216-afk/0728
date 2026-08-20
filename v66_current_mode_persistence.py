from __future__ import annotations

"""Persistence guard for the V66 Current-Time latch.

The latch survives ordinary process restarts while the historical checkpoint remains
terminal. A genuine semantic reset clears the terminal checkpoint, which explicitly
clears this persisted latch and permits fresh historical research.
"""

import time
from typing import Any

import runtime_identity

VERSION = "V66_CURRENT_TIME_LATCH_PERSISTENCE"
SCHEMA = 661
STATE_KEY = "v66_current_time_latch_persisted"
_INSTALLED = False


def _d(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def install(production: Any, autonomous: Any, v66: Any) -> None:
    global _INSTALLED
    if _INSTALLED:
        return
    _INSTALLED = True
    core = production.core
    original_refresh = v66.refresh_mode

    def persistent_refresh(c: Any, a: Any, *, source: str) -> dict[str, Any]:
        cp = _d(c.get_state(a.CHECKPOINT_KEY, {}))
        terminal = str(cp.get("status") or "") == "COMPLETE"
        saved = _d(c.get_state(STATE_KEY, {}))

        # Restore only while the same logical historical phase is terminal. If V56 or a
        # later semantic reset clears/reopens the checkpoint, stale Current mode cannot
        # block the required rebuild.
        if terminal and bool(saved.get("latched_current_time")):
            current = _d(c.state.get(v66.STATE_KEY))
            current["latched_current_time"] = True
            c.state[v66.STATE_KEY] = current

        result = original_refresh(c, a, source=source)
        payload = {
            "schema": SCHEMA,
            "runtime": VERSION,
            "public_runtime": runtime_identity.RUNTIME_VERSION,
            "latched_current_time": bool(result.get("latched_current_time")) if terminal else False,
            "historical_checkpoint_complete": terminal,
            "historical_checkpoint_updated_at": cp.get("updated_at"),
            "historical_restart_suppressed": bool(result.get("historical_restart_suppressed")) if terminal else False,
            "updated_at": int(time.time()),
        }
        c.set_state(STATE_KEY, payload)
        return result

    # V66 wrappers resolve refresh_mode dynamically, so replacing the module function
    # upgrades scan/learning/API behavior without stacking a second execution wrapper.
    v66.refresh_mode = persistent_refresh
    persistent_refresh(core, autonomous, source="persistence_install")

    role = core.state.get("bootstrap_replica_role")
    if isinstance(role, dict):
        role.update({
            "current_time_latch_persists_restart": True,
            "current_time_latch_clears_on_nonterminal_semantic_reset": True,
            "current_time_latch_persistence_authority": VERSION,
        })
