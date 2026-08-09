from __future__ import annotations

import asyncio
import time

import v5_runtime
import v6_runtime


def _legacy_runtime_allowed(core) -> bool:
    """Return True only when an older runtime is intentionally active.

    Modern v7+ runtimes reuse this module for historical Signal-learning
    orchestration, but legacy v5/v6 boot notices and the v6 execution optimizer
    must never run inside v7, v8, or later versions.
    """
    runtime = str(core.state.get('runtime_version') or '')
    try:
        major = int(runtime.split('.', 1)[0])
    except Exception:
        major = 0
    return major < 7


def _modern_price_backfill_blocker(core) -> tuple[str, str] | None:
    """Return the first legacy price-backfill target that still wants older bars.

    This is diagnostic/scheduling information only. Modern Strict Replay is never
    globally blocked by this target: the replay generator itself owns continuity,
    close-time and minimum-history fail-closed checks for each decision timestamp.
    """
    for asset, tf in core.BACKFILL_PLAN:
        earliest = core._earliest(asset, tf)
        if earliest is None or earliest > core.START_TS + 2 * core.TIMEFRAME_SECONDS[tf]:
            return asset, tf
    return None


async def learning_tick_v5_async(core) -> None:
    """Run historical replay/model fitting without starving learning on one backfill.

    v8+ may continue filling an old/missing price timeframe, but that must not prevent
    independent derivative backfill and Strict Replay from advancing wherever all
    timestamp-local prerequisites are already valid. Missing/gapped timestamps remain
    fail-closed inside the Strict Replay generator; no future data is synthesized.
    """
    live_added = await asyncio.to_thread(core.ingest_completed_live_samples)
    con = core.db()
    progress = core.bootstrap_progress(con)
    con.close()

    chosen = _modern_price_backfill_blocker(core)
    backfill_result = None
    derivative_result = None
    samples = 0
    training = []
    execution_results = []
    legacy_runtime = _legacy_runtime_allowed(core)

    # Keep repairing raw price history when necessary, but under modern runtimes this
    # target is no longer an exclusive branch that can starve all learning forever.
    if chosen:
        backfill_result = await core.backfill_one(*chosen)

    # Legacy v5/v6 keep their historical sequencing. Modern v7+ always attempts the
    # independent derivative pipeline and Strict Replay after any price repair pass.
    if not chosen or not legacy_runtime:
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

        # v6 execution optimization exists only for an intentionally active legacy
        # runtime. Modern runtimes have their own independent point-in-time execution
        # audit and must never run this older optimizer in the background.
        if legacy_runtime:
            last_exec = int(core.get_state('v6_last_exec_opt_ts', 0) or 0)
            if training or time.time() - last_exec >= 6 * 3600:
                execution_results = await asyncio.to_thread(v6_runtime.optimize_execution, core, False)
                core.set_state('v6_last_exec_opt_ts', int(time.time()))
                if execution_results:
                    await v6_runtime.notify_execution_results(core, execution_results)

    # Preserve diagnostics written by Strict Replay / readiness layers before replacing
    # the public learning state. Older code accidentally erased the exact blocker here.
    prior_learning = dict(core.state.get('learning') or {})
    watermark = prior_learning.get('derivative_replay_watermark')
    generation_reset = prior_learning.get('source_generation_reset')

    con = core.db()
    progress = core.bootstrap_progress(con)
    con.close()
    champions = await asyncio.to_thread(v5_runtime._all_champions, core)
    counts = await asyncio.to_thread(v5_runtime._sample_counts, core)
    replay = await asyncio.to_thread(v5_runtime._replay_progress, core)

    multisource = core.state.get('derivative_multisource') or {}
    ready_through = multisource.get('ready_through')
    derivative_errors = list(multisource.get('errors') or [])
    if samples > 0:
        phase = 'STRICT_REPLAY_ADVANCING'
        blocker = None
    elif watermark and watermark.get('blocked'):
        phase = 'WAITING_DERIVATIVE_WATERMARK'
        blocker = watermark.get('reason') or 'historical derivative watermark has not advanced far enough'
    elif chosen:
        phase = 'PRICE_REPAIR_AND_REPLAY_PROBING'
        blocker = f'price history repair still active for {chosen[0]} {chosen[1]}; modern replay continues probing independently'
    else:
        phase = 'STRICT_REPLAY_PROBING'
        blocker = derivative_errors[0] if derivative_errors else None

    core.state['learning'] = {
        'progress': progress,
        'historical_price_coverage': progress.get('overall', 0),
        'replay_learning_progress': replay,
        'phase': phase,
        'blocker': blocker,
        'price_backfill_target': {'asset': chosen[0], 'tf': chosen[1]} if chosen else None,
        'backfill': backfill_result,
        'derivatives': core.derivative_history.status(),
        'derivative_backfill': derivative_result,
        'derivative_multisource': multisource,
        'derivative_ready_through': ready_through,
        'derivative_errors': derivative_errors,
        'derivative_replay_watermark': watermark,
        'source_generation_reset': generation_reset,
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
            'parallel multi-source derivatives',
            'strict point-in-time replay',
            'exact Entry/SL/TP execution OOS',
            'post-exit review',
        ],
        'model_schema_version': 2,
        'execution_schema_version': 1,
        'training_off_event_loop': True,
        'legacy_execution_disabled_under_v7_plus': not legacy_runtime,
        'price_backfill_cannot_starve_modern_replay': not legacy_runtime,
    }

    # A modern process must emit only its own startup notice. Older boot notices stay
    # available solely when those older runtimes are intentionally launched.
    if legacy_runtime:
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
