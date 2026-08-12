from __future__ import annotations

import os
import time
from typing import Any

import v9_final
import v9_readiness
import runtime_identity


PARITY_VERSION = runtime_identity.RUNTIME_VERSION
MAX_DECISION_AGE_SECONDS = max(30, min(300, int(os.getenv('STRICT_LIVE_DECISION_MAX_AGE_SECONDS', '120'))))


def install(core: Any) -> None:
    original_raw = core._raw_derivatives
    original_create = core.create_signal

    def canonical_live_derivatives(bundle: dict[str, Any]) -> dict[str, float]:
        raw = original_raw(bundle)
        m15 = bundle.get('eth_15m') or []
        decision_close = int(m15[-1]['ts']) + 900 if m15 else int(time.time())
        # Use the exact same historical derivative semantics as Strict Replay.
        # Instantaneous orderbook / 15m OI remain useful operational telemetry, but
        # they are not injected into a model trained on lagged 4h historical fields.
        canonical = v9_final._strict_derivative_extras(core.derivative_history, decision_close)
        canonical['spot_perp_basis_bps'] = 0.0  # no equivalent full-history series yet
        canonical['source_agreement_bps'] = float(raw.get('source_agreement_bps') or 999.0)
        core.state['live_derivative_parity'] = {
            'decision_close_ts': decision_close,
            'model_semantics': 'same strict historical derivative view',
            'raw_live_oi_available': float(raw.get('oi_available') or 0),
            'raw_live_funding_available': float(raw.get('funding_available') or 0),
            'raw_live_book_available': float(raw.get('book_available') or 0),
            'canonical_coverage': float(canonical.get('derivative_coverage') or 0),
            'instantaneous_fields_used_by_signal_model': False,
        }
        return canonical

    def strict_fresh_create(analysis: dict[str, Any], m15: list[dict[str, Any]]):
        if not m15:
            return None
        decision_close = int(m15[-1]['ts']) + 900
        now = int(time.time())
        age = now - decision_close
        fresh = 0 <= age <= MAX_DECISION_AGE_SECONDS
        core.state['strict_live_decision'] = {
            'decision_close_ts': decision_close,
            'checked_at': now,
            'age_seconds': age,
            'max_age_seconds': MAX_DECISION_AGE_SECONDS,
            'fresh': fresh,
            'reference_price': float(m15[-1]['c']),
            'rule': 'new signals only shortly after a newly closed 15m bar; stale recovery waits for next close',
        }
        if not fresh:
            # Existing PLANNED/OPEN positions are managed elsewhere. This gate only
            # prevents a brand-new order from being synthesized from stale features.
            return None
        aligned = dict(analysis)
        aligned['price'] = float(m15[-1]['c'])
        aligned['strict_decision_close_ts'] = decision_close
        aligned['live_ticker_at_scan'] = float(analysis.get('price') or aligned['price'])
        return original_create(aligned, m15)

    core._raw_derivatives = canonical_live_derivatives
    core.create_signal = strict_fresh_create
    # Normalize component-visible version strings as well as the top-level runtime.
    # Their closures read these module globals at request/notification time.
    v9_final.FINAL_VERSION = PARITY_VERSION
    v9_readiness.READINESS_VERSION = PARITY_VERSION
    core.state.setdefault('strict_replay', {})['live_parity'] = {
        'version': PARITY_VERSION,
        'closed_15m_reference_price': True,
        'max_new_signal_age_seconds': MAX_DECISION_AGE_SECONDS,
        'canonical_derivative_features': True,
        'instantaneous_derivative_distribution_shift_blocked': True,
    }
    core.state['runtime_version'] = PARITY_VERSION
    core.state['strict_replay']['runtime'] = PARITY_VERSION
    runtime_identity.stamp(core)

    if not any(getattr(r, 'path', None) == '/api/v9/live-parity' for r in core.app.router.routes):
        @core.app.get('/api/v9/live-parity')
        def live_parity_status() -> dict[str, Any]:
            return {
                'runtime': PARITY_VERSION,
                'decision': core.state.get('strict_live_decision', {}),
                'derivatives': core.state.get('live_derivative_parity', {}),
                'rule': 'historical and live model features share the same close-time and derivative semantics',
            }
