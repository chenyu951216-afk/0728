from __future__ import annotations

"""V57 live-hook compatibility/runtime authority.

V56 corrected the execution semantics, but its module-level live hooks were wrapped with
core-bound signatures while some production callers still invoke the historical
module-style signatures (core, bundle)/(core, analysis, m15)/(core, bar).  That caused
Market Scan to fail with ``analysis_with_challengers() takes 1 positional argument but
2 were given`` before any live analysis could complete.

This layer does not change historical research, OOS gates, strategy genomes or model
semantics.  It only makes the V56 canonical live hooks accept both supported calling
conventions and explicitly binds the production Core surfaces to those same V56 hooks,
so analysis, signal creation and signal management cannot silently fall back to an old
execution implementation.
"""

import time
from typing import Any, Callable

import runtime_identity

VERSION = 'V57_LIVE_HOOK_RUNTIME_AUTHORITY'
SCHEMA = 57
STATE_KEY = 'v57_live_hook_runtime_authority'
_INSTALLED = False


def _now() -> int:
    return int(time.time())


def _state(core: Any, **patch: Any) -> dict[str, Any]:
    raw = core.state.get(STATE_KEY)
    out = dict(raw) if isinstance(raw, dict) else {}
    out.update(patch)
    out.update({'schema': SCHEMA, 'runtime': VERSION,
                'public_runtime': runtime_identity.RUNTIME_VERSION,
                'updated_at': _now()})
    core.state[STATE_KEY] = out
    return out


def _one_payload(args: tuple[Any, ...], kwargs: dict[str, Any], name: str) -> Any:
    """Accept (payload) or (core, payload), plus the explicit keyword form."""
    if name in kwargs:
        return kwargs[name]
    if len(args) == 1:
        return args[0]
    if len(args) == 2:
        return args[1]
    raise TypeError(f'{name} hook expects ({name}) or (core, {name}); got {len(args)} positional args')


def _two_payloads(args: tuple[Any, ...], kwargs: dict[str, Any], first: str, second: str) -> tuple[Any, Any]:
    """Accept (a,b) or (core,a,b), plus explicit keyword form."""
    if first in kwargs and second in kwargs:
        return kwargs[first], kwargs[second]
    if len(args) == 2:
        return args[0], args[1]
    if len(args) == 3:
        return args[1], args[2]
    raise TypeError(
        f'live hook expects ({first}, {second}) or (core, {first}, {second}); '
        f'got {len(args)} positional args'
    )


def _analysis_adapter(core: Any, v56_analysis: Callable[[dict[str, Any]], dict[str, Any]]) -> Callable[..., dict[str, Any]]:
    def analysis_with_challengers_compat(*args: Any, **kwargs: Any) -> dict[str, Any]:
        bundle = _one_payload(args, kwargs, 'bundle')
        result = v56_analysis(bundle)
        _state(core, last_analysis_ok_at=_now(), last_analysis_error=None,
               analysis_binding='V56_CANONICAL_ANALYSIS')
        return result
    return analysis_with_challengers_compat


def _create_adapter(core: Any, v56_create: Callable[[dict[str, Any], list[dict[str, Any]]], Any]) -> Callable[..., Any]:
    def create_signal_compat(*args: Any, **kwargs: Any) -> Any:
        analysis, m15 = _two_payloads(args, kwargs, 'analysis', 'm15')
        result = v56_create(analysis, m15)
        _state(core, last_create_call_at=_now(), last_create_error=None,
               create_binding='V56_CANONICAL_CREATE')
        return result
    return create_signal_compat


def _update_adapter(core: Any, v56_update: Callable[[dict[str, Any]], Any]) -> Callable[..., Any]:
    def update_signal_compat(*args: Any, **kwargs: Any) -> Any:
        bar = _one_payload(args, kwargs, 'bar')
        result = v56_update(bar)
        _state(core, last_update_call_at=_now(), last_update_error=None,
               update_binding='V56_CANONICAL_5M_MANAGEMENT')
        return result
    return update_signal_compat


def install(production: Any, autonomous: Any, v56: Any) -> None:
    global _INSTALLED
    if _INSTALLED:
        return
    _INSTALLED = True
    core = production.core

    # Capture the already-installed V56 one-core-bound implementations.  These are the
    # semantic authority; V57 only adapts invocation shape and production binding.
    v56_analysis = autonomous._autonomous_analysis
    v56_create = autonomous._autonomous_create_signal
    v56_update = autonomous._autonomous_update_signal

    analysis_compat = _analysis_adapter(core, v56_analysis)
    create_compat = _create_adapter(core, v56_create)
    update_compat = _update_adapter(core, v56_update)

    # Module-style callers may still pass core explicitly.  Core-style callers use the
    # one-core-bound surfaces below.  Both routes terminate in the exact same V56 code.
    autonomous._autonomous_analysis = analysis_compat
    autonomous._autonomous_create_signal = create_compat
    autonomous._autonomous_update_signal = update_compat

    core._analysis_from_bundle = lambda bundle: analysis_compat(bundle)
    core.create_signal = lambda analysis, m15: create_compat(analysis, m15)
    core.update_signal_with_bar = lambda bar: update_compat(bar)

    _state(core, installed=True, status='READY',
           analysis_accepts_core_and_bound_forms=True,
           create_accepts_core_and_bound_forms=True,
           update_accepts_core_and_bound_forms=True,
           core_analysis_bound_to_v56=True,
           core_create_bound_to_v56=True,
           core_update_bound_to_v56=True,
           historical_research_semantics_changed=False,
           oos_rules_changed=False,
           strategy_results_changed=False,
           future_peeking_enabled=False,
           live_execution_semantics=str(getattr(v56, 'VERSION', 'V56_CAUSAL_MULTICHAMPION_ONLINE_LEARNING')))

    role = core.state.get('bootstrap_replica_role')
    if isinstance(role, dict):
        role.update({
            'live_hook_runtime_authority': VERSION,
            'market_scan_signature_compat_fixed': True,
            'core_analysis_bound_to_v56': True,
            'core_create_bound_to_v56': True,
            'core_update_bound_to_v56': True,
            'research_semantics_changed_by_v57': False,
            'v47_exact_identity_changed_by_v57': False,
        })
