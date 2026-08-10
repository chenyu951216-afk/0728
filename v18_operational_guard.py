from __future__ import annotations

import threading
import time
from typing import Any

import v5_runtime
import v17_certification_orchestrator as cert17
import v18_final_system as final


VERSION = final.VERSION
VIEW_TTL_SECONDS = 30
_LOCK = threading.RLock()
_CACHE: dict[str, Any] = {'at': 0.0, 'view': None}

_ORIGINAL_VIEW = final._authoritative_view
_ORIGINAL_CERTIFY = final.certify_and_execute
_ORIGINAL_LIVE_GATE = final._final_live_gate


def _invalidate() -> None:
    with _LOCK:
        _CACHE['at'] = 0.0
        _CACHE['view'] = None


def authoritative_view(core: Any, force: bool = False) -> dict[str, Any]:
    now = time.monotonic()
    with _LOCK:
        cached = _CACHE.get('view')
        age = now - float(_CACHE.get('at') or 0.0)
        if not force and isinstance(cached, dict) and age < VIEW_TTL_SECONDS:
            core.state['final_system_view'] = cached
            return cached
    view = _ORIGINAL_VIEW(core)
    with _LOCK:
        _CACHE['view'] = view
        _CACHE['at'] = time.monotonic()
    return view


def certify_and_execute(core: Any, force: bool = False):
    _invalidate()
    try:
        return _ORIGINAL_CERTIFY(core, force)
    finally:
        # Certification/registry changes must be visible immediately, not after TTL.
        try:
            authoritative_view(core, True)
        except Exception:
            _invalidate()


def final_live_gate(core: Any, original_create: Any, analysis: dict[str, Any], m15: list[dict[str, Any]]):
    # A potential new order is rare and safety-critical: force one fresh persistent
    # audit here. The original final gate then reads the fresh cached snapshot.
    authoritative_view(core, True)
    return _ORIGINAL_LIVE_GATE(core, original_create, analysis, m15)


def install(core: Any) -> None:
    final._authoritative_view = authoritative_view
    final.certify_and_execute = certify_and_execute
    final._final_live_gate = final_live_gate

    # Closures installed by v17/v18 resolve these module globals at call time, but
    # direct legacy/manual references are rebound too so there is only one authority.
    cert17.train_v17 = lambda c, force=False: certify_and_execute(c, force)
    v5_runtime.train_v5 = lambda c, force=False: certify_and_execute(c, force)
    core.train_if_due = lambda force=False: certify_and_execute(core, force)

    strict = core.state.setdefault('strict_replay', {})
    strict['final_authority']['heavy_audit_cache_seconds'] = VIEW_TTL_SECONDS
    strict['final_authority']['dashboard_poll_reexecutes_full_dataset_audit'] = False
    strict['final_authority']['potential_live_order_forces_fresh_audit'] = True
    strict['final_authority']['certification_forces_fresh_audit'] = True
    core.state['runtime_version'] = VERSION
    core.app.version = '9.0.0'

    authoritative_view(core, True)
