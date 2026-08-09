from __future__ import annotations

from typing import Any

import v5_runtime
import v7_runtime
import v8_evolution
import v9_final
import v10_final_integrity as fin


VERSION=fin.VERSION


async def final_boot_notice(core: Any)->None:
    key='discord_boot_version_final_integrity'
    if core.get_state(key)==VERSION:
        return
    state=core.get_state(fin.STATE_KEY,{}) or {}
    oi=', '.join(state.get('frozen_core_oi') or []) or '等待下載完成後鎖定'
    funding=', '.join(state.get('frozen_core_funding') or []) or '等待下載完成後鎖定'
    ok=await v5_runtime.robust_send_discord(
        core,
        '🛡️ ETH Adaptive AI 8.1 Final Replay Integrity 已啟動',
        '歷史模擬：時間 T 只能使用 T 當時已收線且已處理完成的資料；30m/1H/4H/1D 未收線 K 禁止進入特徵。\n'
        'Signal label：15m 決策先鎖死，再從 decision close 之後用連續 5m K 逐根揭露成交/SL/TP；成交 5m 棒不會倒推先前 high/low 當獲利。\n'
        f'資料來源：本代 OI `{oi}`｜Funding `{funding}`；來源集合在 replay 前凍結，半途新增 provider 禁止污染同一代。\n'
        '模型：必須完成 full-span Strict Replay 才能認證 Signal Champion；Genome 只在 train/cal 內挑選。\n'
        'Execution：DEV-only 多代進化，Validation 只選最後 elite，下一段 untouched walk-forward 只能認證/淘汰，不能回頭改參數。\n'
        'Live：新單完整記錄 Net R / MFE / MAE / TP-SL 路徑；交易結果先作 deployment evidence，單筆輸贏不能直接改 Signal Model。',
        0x2ECC71,
    )
    if ok:
        core.set_state(key,VERSION)


def install(core: Any)->None:
    # One modern boot-notice owner. The existing promotion notices remain in place.
    v7_runtime.maybe_boot_notice=final_boot_notice
    v8_evolution.EVOLUTION_VERSION=VERSION
    v9_final.FINAL_VERSION=VERSION
    core.state.setdefault('strict_replay',{})['single_modern_boot_notice']=True
