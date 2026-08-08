from __future__ import annotations

import asyncio
import json
import time
from datetime import datetime
from typing import Any

import v5_runtime


def _summary(core: Any, row: dict[str, Any]) -> str:
    sizing = core._notional_for_risk(float(row['entry']), float(row['initial_stop']))
    targets = row.get('targets') or []
    target_text = '｜'.join(
        f"TP{i+1} {float(x.get('price') or 0):,.2f} ({float(x.get('rr') or 0):.2f}R/{x.get('allocation')}%)"
        for i, x in enumerate(targets)
    )
    payload = row.get('payload') or {}
    validation = payload.get('execution_validation') or {}
    ex = ''
    if validation.get('certified'):
        ex = (
            f"\nExecution OOS PF `{float(validation.get('oos_pf') or 0):.2f}`｜"
            f"EV `{float(validation.get('oos_ev_r') or 0):+.3f}R`｜"
            f"fills `{int(validation.get('oos_fills') or 0)}`｜"
            f"成本假設 `{float(validation.get('estimated_all_in_cost_bps') or 0):.1f}bps`"
        )
    elif payload.get('legacy_execution_plan'):
        ex = '\n⚠️ 這是 v6 部署前已 OPEN 的 legacy plan；原計畫保留，不代表通過 execution OOS。'
    notional = sizing.get('notional_usdt')
    return (
        f"`{row['direction']}`｜`{row['strategy']}`｜{row['regime']}/{row['phase']}\n"
        f"機率 `{float(row['probability']):.1%}`｜Entry `{float(row['entry']):,.2f}`｜"
        f"初始 SL `{float(row['initial_stop']):,.2f}`｜目前 SL `{float(row['current_stop']):,.2f}`\n"
        f"{target_text}{ex}\n"
        f"2% 風險名目金額：`{f'{float(notional):,.2f}' if notional is not None else '請先設定帳戶餘額'} USDT`"
    )


async def scan_v6(core: Any) -> dict[str, Any]:
    bundle = await core.hub.live_bundle()
    core.upsert_live_gate(bundle)
    analysis = core._analysis_from_bundle(bundle)
    now = int(time.time())
    con = core.db()
    con.execute('INSERT INTO snapshots(ts,payload) VALUES(?,?)', (now, json.dumps(analysis, ensure_ascii=False)))
    con.execute('DELETE FROM snapshots WHERE ts<?', (now - 120 * 86400,))
    con.commit(); con.close()

    before = core.latest_signal()
    before_copy = json.loads(json.dumps(before, ensure_ascii=False)) if before else None
    last_bar = bundle['eth_15m'][-1]
    core.update_signal_with_bar(last_bar)
    core.post_exit_review(last_bar)
    after_update = core.latest_signal()

    if before_copy:
        if before_copy['status'] == 'PLANNED' and after_update and after_update['signal_id'] == before_copy['signal_id'] and after_update['status'] == 'OPEN':
            await v5_runtime.robust_send_discord(core, '📥 ETH 雙認證訊號已成交', _summary(core, after_update), 0x3498DB)
        if before_copy['status'] == 'OPEN':
            current = v5_runtime._signal_row(core, before_copy['signal_id'])
            if current and current['status'] == 'CLOSED':
                await v5_runtime.robust_send_discord(
                    core,
                    f"✅ ETH 持倉已結束｜{current.get('exit_reason')}",
                    _summary(core, current) + f"\n加權分批＋估計成本後結果 `{float(current.get('realized_r') or 0):+.2f}R`｜出場參考 `{float(current.get('exit_price') or 0):,.2f}`\n系統會再追蹤 24h 做出場後複盤。",
                    0x2ECC71 if float(current.get('realized_r') or 0) >= 0 else 0xE74C3C,
                )
            elif current and current['status'] == 'OPEN':
                old_hits = set((before_copy.get('payload') or {}).get('management', {}).get('hit_targets', []))
                new_hits = set((current.get('payload') or {}).get('management', {}).get('hit_targets', []))
                for idx in sorted(new_hits - old_hits):
                    mgmt = (current.get('payload') or {}).get('management', {})
                    await v5_runtime.robust_send_discord(
                        core,
                        f"🎯 ETH TP{idx+1} 已實現分批",
                        _summary(core, current) + f"\n目前已實現 `{float(mgmt.get('realized_partial_r') or 0):+.3f}R`｜剩餘 `{float(mgmt.get('remaining_fraction') or 0):.0%}`。",
                        0x2ECC71,
                    )

    active = core.latest_signal()
    if active is None:
        created = core.create_signal(analysis, bundle['eth_15m'])
        if created and created['created_at'] >= now - 5:
            await v5_runtime.robust_send_discord(
                core,
                '🆕 ETH Signal + Execution 雙 Champion 掛單',
                _summary(core, created) + '\n此 Entry / SL / TP / allocation / trailing 是和 OOS 驗證同一套 execution policy；不追價。',
                0x4C8BF5,
            )

    active = core.latest_signal()
    core.state.update(
        service='OK',
        updated_at=datetime.now(core.timezone.utc).isoformat(),
        error=None,
        scan_count=core.state['scan_count'] + 1,
        analysis=analysis,
        active_signal=active,
    )
    return analysis


async def scan_worker_v6(core: Any) -> None:
    while True:
        try:
            await scan_v6(core)
            await v5_runtime.poll_discord_commands(core)
        except Exception as exc:
            core.LOG.exception('v6 scan failed')
            core.state.update(service='DEGRADED', error=str(exc))
        await asyncio.sleep(core.SCAN_SECONDS)


def install(core: Any) -> None:
    async def scan_wrapper() -> dict[str, Any]:
        return await scan_v6(core)

    async def worker_wrapper() -> None:
        await scan_worker_v6(core)

    core.scan = scan_wrapper
    core.scan_worker = worker_wrapper
