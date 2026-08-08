from __future__ import annotations

import asyncio
import time

import v5_runtime
import v6_runtime


async def learning_tick_v5_async(core) -> None:
    """Run replay, signal-model fitting and execution-policy fitting off the live loop."""
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
    execution_results = []

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

        # Execution optimization is deliberately throttled. A newly promoted signal
        # Champion gets evaluated immediately; rejected policies are reconsidered at
        # most every six hours as new samples arrive, never every learning tick.
        last_exec = int(core.get_state('v6_last_exec_opt_ts', 0) or 0)
        if training or time.time() - last_exec >= 6 * 3600:
            execution_results = await asyncio.to_thread(v6_runtime.optimize_execution, core, False)
            core.set_state('v6_last_exec_opt_ts', int(time.time()))
            if execution_results:
                await v6_runtime.notify_execution_results(core, execution_results)

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
        'execution_validation': core.state.get('execution_learning', {}),
        'execution_results_this_tick': execution_results,
        'learning_order': [
            '1D/4H regime',
            '1H/30M structure',
            '15M/5M signal model',
            'derivatives',
            'exact Entry/SL/TP execution OOS',
            'post-exit review',
        ],
        'model_schema_version': 2,
        'execution_schema_version': 1,
        'training_off_event_loop': True,
    }

    # Keep the old v5 notice for backwards diagnostics, then explicitly verify that
    # the new double-certification runtime can deliver to Discord too.
    await v5_runtime.maybe_boot_notice(core)
    if core.get_state('discord_boot_version_v6') != v6_runtime.V6_VERSION:
        ok = await v5_runtime.robust_send_discord(
            core,
            '✅ ETH Adaptive AI v6 已啟動',
            'Signal Champion + Execution Champion 雙層 OOS 驗證已啟用。新的 Entry / SL / TP / 分批 / BE / trailing 必須和歷史未見資料驗證完全一致；未通過 execution OOS 的方向模型不會建立正式交易計畫。',
            0x3498DB,
        )
        if ok:
            core.set_state('discord_boot_version_v6', v6_runtime.V6_VERSION)
    await v5_runtime.maybe_daily_report(core)


def install_async(core) -> None:
    async def learning_tick_wrapper() -> None:
        await learning_tick_v5_async(core)

    core.learning_tick = learning_tick_wrapper
