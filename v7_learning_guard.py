from __future__ import annotations

import asyncio
import time
from datetime import datetime
from typing import Any

import execution_v7
import v5_async_runtime
import v5_runtime
import v7_runtime
import runtime_identity


async def learning_tick_guarded(core: Any) -> None:
    live_added = await asyncio.to_thread(v7_runtime.ingest_completed_live_samples_v7, core)
    replay = v5_runtime._replay_progress(core)
    now = int(time.time())
    last_heavy = int(core.get_state('v7_last_heavy_learning_ts', 0) or 0)
    need_new_label = int(replay.get('latest_market_ts') or 0) - int(replay.get('cursor_ts') or 0) >= 29 * 900
    heavy = not replay.get('complete') or need_new_label or now - last_heavy >= 900
    if heavy:
        await v5_async_runtime.learning_tick_v5_async(core)
        core.set_state('v7_last_heavy_learning_ts', now)
    else:
        core.state.setdefault('learning', {})['v7_live_execution_samples_added'] = live_added
        core.state['learning']['v7_heavy_learning_skipped'] = True
        core.state['learning']['v7_next_check_seconds'] = max(0, 900 - (now - last_heavy))

    signature = [list(x) for x in v7_runtime._champion_signature(core)]
    old_signature = core.get_state('v7_execution_signal_signature', []) or []
    last_attempt = int(core.get_state('v7_execution_last_attempt_ts', 0) or 0)
    signature_changed = signature != old_signature
    daily_refresh = now - last_attempt >= 24 * 3600
    need_exec = bool(signature) and (signature_changed or daily_refresh)
    if need_exec:
        # A new Signal Champion has no matching execution version, so normal mode
        # is enough. If the Signal version is unchanged, force a daily re-audit so
        # newly accumulated market history can actually evolve Entry/SL/TP policy.
        results = await asyncio.to_thread(execution_v7.optimize_all, core, bool(daily_refresh and not signature_changed))
        core.state['execution_learning'] = {
            'version': v7_runtime.V7_VERSION,
            'results': results,
            'registry': v7_runtime._execution_status(core)[:50],
            'updated_at': datetime.now(core.timezone.utc).isoformat(),
            'throttled': True,
            'reason': 'signal_version_changed' if signature_changed else 'daily_fresh_data_reaudit',
        }
        core.set_state('v7_execution_signal_signature', signature)
        core.set_state('v7_execution_last_attempt_ts', now)
        await v7_runtime._notify_execution_results(core, results)


async def boot_notice_ordered_trades(core: Any) -> None:
    if core.get_state('discord_boot_version_v7_trade') == v7_runtime.V7_VERSION:
        return
    ok = await v5_runtime.robust_send_discord(
        core,
        f'✅ {runtime_identity.PRODUCT_NAME} {runtime_identity.DISPLAY_VERSION} 已啟動',
        '舊 Execution PF 已退役。v7 使用 close-time-safe / point-in-time Signal OOF + 獨立 validation/audit；Entry/TP/SL 由 Gate 公開逐筆成交依時間順序監控；止損後有 cooldown + 新結構 reset；實盤 execution 結果獨立保存，不會污染 Signal Model。',
        0x3498DB,
    )
    if ok:
        core.set_state('discord_boot_version_v7_trade', v7_runtime.V7_VERSION)


def install(core: Any) -> None:
    async def tick() -> None:
        await learning_tick_guarded(core)
    core.learning_tick = tick
    v7_runtime.maybe_boot_notice = boot_notice_ordered_trades
