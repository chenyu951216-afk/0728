from __future__ import annotations

import asyncio

import v5_runtime


async def learning_tick_v5_async(core) -> None:
    """Run CPU/SQLite-heavy replay and model fitting outside the live event loop.

    The live scanner, health endpoint and Discord polling must stay responsive even
    while 14 direction-specific challenger models are being trained.
    """
    live_added = await asyncio.to_thread(core.ingest_completed_live_samples)
    con = core.db()
    progress = core.bootstrap_progress(con)
    con.close()

    chosen = None
    for asset, tf in core.BACKFILL_PLAN:
        earliest = core._earliest(asset, tf)
        if earliest is None or earliest > core.START_TS + 2 * core.TIMEFRAME_SECONDS[tf]:
            chosen = (asset, tf)
            break

    backfill_result = None
    derivative_result = None
    samples = 0
    training = []

    if chosen:
        backfill_result = await core.backfill_one(*chosen)
    else:
        core.derivative_history.set_db_path(core.DB_PATH)
        derivative_result = await core.derivative_history.backfill_tick(
            core.hub,
            core.START_TS,
            pages=max(1, min(5, core.BACKFILL_PAGES_PER_TICK)),
        )
        samples = await asyncio.to_thread(v5_runtime.generate_learning_samples_v5, core)
        training = await asyncio.to_thread(v5_runtime.train_v5, core)
        if training:
            await v5_runtime._notify_promotions(core, training)

    con = core.db()
    progress = core.bootstrap_progress(con)
    con.close()
    champions = await asyncio.to_thread(v5_runtime._all_champions, core)
    counts = await asyncio.to_thread(v5_runtime._sample_counts, core)
    replay = await asyncio.to_thread(v5_runtime._replay_progress, core)

    core.state['learning'] = {
        'progress': progress,
        'historical_price_coverage': progress.get('overall', 0),
        'replay_learning_progress': replay,
        'backfill': backfill_result,
        'derivatives': core.derivative_history.status(),
        'derivative_backfill': derivative_result,
        'live_samples_added': live_added,
        'v5_samples_added': samples,
        'sample_counts': counts,
        'champions': champions,
        'recent_rejected': [x for x in training if not x.get('promoted')][:12],
        'learning_order': [
            '1D/4H regime',
            '1H/30M structure',
            '15M/5M execution',
            'derivatives',
            'post-exit review',
        ],
        'model_schema_version': 2,
        'training_off_event_loop': True,
    }
    await v5_runtime.maybe_boot_notice(core)
    await v5_runtime.maybe_daily_report(core)


def install_async(core) -> None:
    async def learning_tick_wrapper() -> None:
        await learning_tick_v5_async(core)

    core.learning_tick = learning_tick_wrapper
