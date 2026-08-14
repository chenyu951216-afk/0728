from __future__ import annotations

import os
import time
from typing import Any

import v22_hierarchical_pipeline as hierarchical

VERSION = 'V37_FRESH_PRICE_BOOTSTRAP'
DEFAULT_REPAIR_PAGES = 6
MAX_REPAIR_PAGES = 12


def _repair_pages_per_tick(core: Any) -> int:
    configured = os.getenv('V37_PRICE_REPAIR_PAGES_PER_TICK', '').strip()
    if configured:
        try:
            return max(1, min(MAX_REPAIR_PAGES, int(configured)))
        except ValueError:
            pass
    legacy = int(getattr(core, 'BACKFILL_PAGES_PER_TICK', DEFAULT_REPAIR_PAGES) or DEFAULT_REPAIR_PAGES)
    # The old scheduler used its pages only for a tail-backfill pass, while the
    # strict full-history gate repaired exactly one 1,000-bar hole per tick.  On a
    # fresh database that made the UI look frozen for a long time.  Reuse at least
    # the configured legacy throughput for the authoritative contiguous repair.
    return max(DEFAULT_REPAIR_PAGES, min(MAX_REPAIR_PAGES, legacy))


def _weighted_foundation_progress(core: Any) -> dict[str, Any]:
    foundation = hierarchical.price_foundation(core)
    series = [item for group in foundation.values() for item in group.get('series', [])]
    expected = sum(max(0, int(item.get('expected_bars') or 0)) for item in series)
    present = sum(
        min(max(0, int(item.get('bars') or 0)), max(0, int(item.get('expected_bars') or 0)))
        for item in series
    )
    downloaded_pct = (present / expected * 100.0) if expected else 0.0
    strict_pct = min((float(item.get('percent') or 0.0) for item in series), default=0.0)
    ready = bool(series and all(bool(item.get('history_ready')) for item in series))
    return {
        'overall': round(max(0.0, min(100.0, downloaded_pct)), 2),
        'downloaded_percent': round(max(0.0, min(100.0, downloaded_pct)), 2),
        'strict_gate_percent': round(max(0.0, min(100.0, strict_pct)), 2),
        'strict_gate_ready': ready,
        'downloaded_bars': present,
        'expected_bars': expected,
        'hierarchical': foundation,
        'progress_semantics': (
            'overall is actual raw bars downloaded across all required series; '
            'strict replay remains blocked until every required series is complete'
        ),
    }


def install(core: Any) -> None:
    if getattr(hierarchical, '_v37_fresh_bootstrap_installed', False):
        return

    original_repair = hierarchical._repair_collection_gap

    async def batched_repair(c: Any, first_target: dict[str, Any]) -> dict[str, Any]:
        max_pages = _repair_pages_per_tick(c)
        target: dict[str, Any] | None = dict(first_target)
        attempts: list[dict[str, Any]] = []
        total_added = 0
        started = time.monotonic()
        previous_key: tuple[str, str, int] | None = None
        stalled = False

        for page in range(max_pages):
            if target is None:
                break
            key = (
                str(target.get('asset') or ''),
                str(target.get('timeframe') or ''),
                int(target.get('missing_ts') or 0),
            )
            if previous_key == key:
                stalled = True
                break
            previous_key = key

            result = dict(await original_repair(c, target) or {})
            attempts.append(result)
            total_added += max(0, int(result.get('added') or 0))

            if str(result.get('status') or '') != 'REPAIRED_PAGE':
                stalled = True
                break

            gate = hierarchical.price_collection_gate(c)
            if gate.get('ready'):
                target = None
                break
            target = hierarchical._first_collection_gap(c)

        gate = hierarchical.price_collection_gate(c)
        next_target = hierarchical._first_collection_gap(c) if not gate.get('ready') else None
        foundation = _weighted_foundation_progress(c)
        status = {
            'runtime': VERSION,
            'status': 'READY_FOR_REPLAY' if gate.get('ready') else ('NO_PROGRESS' if stalled and total_added <= 0 else 'BULK_REPAIRING'),
            'pages_attempted': len(attempts),
            'pages_per_tick': max_pages,
            'bars_added': total_added,
            'elapsed_seconds': round(time.monotonic() - started, 3),
            'next_target': next_target,
            'downloaded_percent': foundation['downloaded_percent'],
            'strict_gate_percent': foundation['strict_gate_percent'],
            'strict_gate_ready': bool(gate.get('ready')),
            'last_attempt': attempts[-1] if attempts else None,
            'no_interpolation': True,
            'replay_started_before_complete_history': False,
        }
        c.state['price_bootstrap_v37'] = status
        # Preserve the legacy key used by the dashboard, but make it describe the
        # whole batch instead of only one 1,000-bar request.
        c.state['causal_price_collection_repair'] = status
        learning = c.state.setdefault('learning', {})
        learning['price_bootstrap_v37'] = status
        if next_target:
            learning['price_backfill_target'] = {
                'asset': next_target.get('asset'),
                'tf': next_target.get('timeframe'),
                'missing_ts': next_target.get('missing_ts'),
            }
        elif gate.get('ready'):
            learning['price_backfill_target'] = None
        return status

    hierarchical._repair_collection_gap = batched_repair
    hierarchical._v37_fresh_bootstrap_installed = True

    # Keep the strict gate unchanged, but stop reporting a misleading 0.00% merely
    # because the last required timeframe has not started yet.  Replay itself still
    # stays at 0% until the strict 100%/zero-gap contract passes.
    core.bootstrap_progress = lambda _con=None: _weighted_foundation_progress(core)

    core.state['fresh_price_bootstrap_v37'] = {
        'runtime': VERSION,
        'installed': True,
        'pages_per_tick': _repair_pages_per_tick(core),
        'strict_coverage_requirement_unchanged': True,
        'required_coverage_pct': hierarchical.PRICE_MIN_COVERAGE_PCT,
        'maximum_missing_bars': hierarchical.PRICE_MAX_MISSING_BARS,
        'no_interpolation': True,
        'purpose': 'fresh-database chronological bulk collection before causal replay',
    }
