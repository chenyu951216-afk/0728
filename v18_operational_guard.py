from __future__ import annotations

import threading
import time
from typing import Any

import v5_runtime
import v12_clean_baseline
import v13_replay_cursor_integrity as cursor_guard
import v15_data_resilience as resilience
import v17_certification_orchestrator as cert17
import v18_final_system as final


VERSION = final.VERSION
VIEW_TTL_SECONDS = 30
SOURCE_PREFLIGHT_KEY = 'v18_source_provenance_preflight'
_LOCK = threading.RLock()
_CACHE: dict[str, Any] = {'at': 0.0, 'view': None}

_ORIGINAL_VIEW = final._authoritative_view
_ORIGINAL_CERTIFY = final.certify_and_execute
_ORIGINAL_LIVE_GATE = final._final_live_gate


def _invalidate() -> None:
    with _LOCK:
        _CACHE['at'] = 0.0
        _CACHE['view'] = None


def _sample_total(core: Any) -> int:
    con = core.db()
    try:
        row = con.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='learning_samples'").fetchone()
        return int(con.execute('SELECT COUNT(*) FROM learning_samples').fetchone()[0] or 0) if row else 0
    finally:
        con.close()


def preflight_source_provenance(core: Any) -> dict[str, Any]:
    """Resolve cross-version frozen-source provenance before final certification.

    Existing replay-derived samples may be reused only if their persistent source
    generation contract is recoverable. If that contract is truly absent, those
    labels cannot be proven comparable to the final generation. In that one case we
    rebuild *derived* learning only; raw candles, derivative cache and CLEAN Dataset
    identity stay untouched.
    """
    total = _sample_total(core)
    baseline = core.get_state(v12_clean_baseline.STATE_KEY, None)
    baseline = dict(baseline) if isinstance(baseline, dict) else {}
    before = resilience._load(core)
    prior = core.get_state(SOURCE_PREFLIGHT_KEY, None)
    prior = dict(prior) if isinstance(prior, dict) else {}
    dataset_id = baseline.get('dataset_id')

    if total <= 0:
        result = {
            'runtime': VERSION, 'status': 'NO_EXISTING_DERIVED_SAMPLES', 'samples': 0,
            'source_set_frozen_before_install': bool(before.get('source_set_frozen')),
            'dataset_id': dataset_id, 'derived_reset': False, 'checked_at': int(time.time()),
        }
        core.set_state(SOURCE_PREFLIGHT_KEY, result)
        return result

    if before.get('source_set_frozen'):
        result = {
            'runtime': VERSION, 'status': 'PERSISTENT_SOURCE_CONTRACT_RECOVERED', 'samples': total,
            'source_set_frozen_before_install': True,
            'oi_sources': list(before.get('model_oi_sources') or []),
            'funding_sources': list(before.get('model_funding_sources') or []),
            'enrichment_sources': list(before.get('model_enrichment_sources') or []),
            'effective_model_start': before.get('effective_model_start'),
            'dataset_id': dataset_id, 'derived_reset': False, 'checked_at': int(time.time()),
        }
        core.set_state(SOURCE_PREFLIGHT_KEY, result)
        return result

    # Do not repeatedly reset the same CLEAN dataset after a successful provenance
    # recovery. The marker itself is persistent and tied to the Dataset ID.
    if prior.get('derived_reset') is True and prior.get('dataset_id') == dataset_id:
        result = {**prior, 'status': 'DERIVED_REBUILD_ALREADY_APPLIED', 'checked_at': int(time.time())}
        core.set_state(SOURCE_PREFLIGHT_KEY, result)
        return result

    # First ask the deterministic v15 capability resolver to recover/freeze a source
    # contract from persistent provider ledgers/cache. If it was merely an in-memory
    # display loss, this succeeds and no samples are touched.
    try:
        recovered = resilience._freeze_sources(core)
    except Exception as exc:
        recovered = {**before, 'preflight_freeze_error': f'{type(exc).__name__}: {exc}'}

    if recovered.get('source_set_frozen'):
        # The old samples were created before this contract was persistently provable.
        # We cannot assert they used the exact same feature availability semantics,
        # so rebuild derived labels once. Raw data is already present, so this is a
        # replay rebuild—not a disk reinstall or multi-exchange redownload.
        cursor_guard._reset_derived_replay(
            core,
            '9.0 source-provenance preflight: existing samples had no persistent frozen derivative contract; rebuild derived labels under one recoverable final contract',
        )
        result = {
            'runtime': VERSION, 'status': 'DERIVED_REBUILD_REQUIRED_AND_APPLIED',
            'samples_discarded': total, 'source_set_frozen_before_install': False,
            'source_set_frozen_after_recovery': True,
            'oi_sources': list(recovered.get('model_oi_sources') or []),
            'funding_sources': list(recovered.get('model_funding_sources') or []),
            'enrichment_sources': list(recovered.get('model_enrichment_sources') or []),
            'effective_model_start': recovered.get('effective_model_start'),
            'dataset_id': dataset_id, 'derived_reset': True,
            'raw_market_preserved': True, 'raw_derivatives_preserved': True,
            'clean_dataset_id_preserved': True, 'checked_at': int(time.time()),
            'reason': 'old derived labels could not prove the same frozen feature-source contract; fail-closed rebuild prevents cross-generation learning contamination',
        }
        core.set_state(SOURCE_PREFLIGHT_KEY, result)
        return result

    # Provider capability state is still not settled. Never certify existing samples
    # under an unknown contract, but also do not repeatedly delete them while waiting.
    result = {
        'runtime': VERSION, 'status': 'WAITING_SOURCE_CONTRACT_RECOVERY', 'samples': total,
        'source_set_frozen_before_install': False, 'source_set_frozen_after_recovery': False,
        'dataset_id': dataset_id, 'derived_reset': False, 'checked_at': int(time.time()),
        'reason': 'existing derived labels are quarantined from certification until one deterministic frozen source contract is recoverable',
    }
    core.set_state(SOURCE_PREFLIGHT_KEY, result)
    return result


def authoritative_view(core: Any, force: bool = False) -> dict[str, Any]:
    now = time.monotonic()
    with _LOCK:
        cached = _CACHE.get('view')
        age = now - float(_CACHE.get('at') or 0.0)
        if not force and isinstance(cached, dict) and age < VIEW_TTL_SECONDS:
            core.state['final_system_view'] = cached
            return cached
    view = _ORIGINAL_VIEW(core)
    preflight = core.get_state(SOURCE_PREFLIGHT_KEY, None)
    if isinstance(preflight, dict):
        view['source_provenance_preflight'] = preflight
        if preflight.get('status') == 'WAITING_SOURCE_CONTRACT_RECOVERY' and int(preflight.get('samples') or 0) > 0:
            view['status'] = 'WAITING_SOURCE_CONTRACT_RECOVERY'
            view['reason'] = preflight.get('reason')
            view.setdefault('dataset', {}).setdefault('audit', {})['valid'] = False
            view['dataset']['audit']['status'] = 'WAITING_SOURCE_CONTRACT_RECOVERY'
            view['dataset']['audit']['reason'] = preflight.get('reason')
    with _LOCK:
        _CACHE['view'] = view
        _CACHE['at'] = time.monotonic()
    return view


def certify_and_execute(core: Any, force: bool = False):
    preflight = core.get_state(SOURCE_PREFLIGHT_KEY, None)
    if isinstance(preflight, dict) and preflight.get('status') == 'WAITING_SOURCE_CONTRACT_RECOVERY' and int(preflight.get('samples') or 0) > 0:
        _invalidate()
        return []
    _invalidate()
    try:
        return _ORIGINAL_CERTIFY(core, force)
    finally:
        try:
            authoritative_view(core, True)
        except Exception:
            _invalidate()


def final_live_gate(core: Any, original_create: Any, analysis: dict[str, Any], m15: list[dict[str, Any]]):
    # A potential new order is rare and safety-critical: force one fresh persistent
    # audit here. Dashboard polling never causes this heavy check.
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
    strict['final_authority']['cross_version_source_provenance_preflight'] = True
    strict['final_authority']['missing_frozen_source_contract_can_be_silently_reused'] = False
    strict['final_authority']['source_provenance_rebuild_deletes_raw_market'] = False
    core.state['runtime_version'] = VERSION
    core.app.version = '9.0.0'

    authoritative_view(core, True)
