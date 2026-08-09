from __future__ import annotations

from typing import Any

import v5_runtime
import v7_runtime
import v8_evolution


async def evolution_boot_notice(core: Any) -> None:
    key = 'discord_boot_version_evolution'
    if core.get_state(key) == v8_evolution.EVOLUTION_VERSION:
        return
    ok = await v5_runtime.robust_send_discord(
        core,
        '🧬 ETH Adaptive AI Evolution 已啟動',
        f"Runtime `{str(v8_evolution.EVOLUTION_VERSION).replace('-20260809','')}`\n"
        'Signal：每個策略×方向會在 purged point-in-time folds 內競爭 feature subset、模型複雜度、正則化、近期資料權重與 learned threshold。\n'
        'Execution：使用 expanding chronological walk-forward；每一折只能用更早歷史挑 Entry/SL/TP，下一段才是 untouched audit，最後合併所有未見折驗證 PF/EV/CI/DD/近期穩定度。\n'
        'Live：每筆新單完整記錄 Net R、估算 USDT、MFE/MAE、TP/SL 路徑；先做 deployment evidence / drift，累積到安全批次才重做 execution audit。單筆輸贏不會直接改 Signal Model。\n'
        '歷史 K 線、衍生品與 point-in-time 樣本沿用，不需刪除或重抓。',
        0x8E6CEF,
    )
    if ok:
        core.set_state(key, v8_evolution.EVOLUTION_VERSION)


async def notify_signal_promotions(core: Any, training: list[dict[str, Any]]) -> None:
    for item in training:
        if not item.get('promoted'):
            continue
        con = core.db()
        row = con.execute("SELECT version,metrics FROM model_registry WHERE strategy=? AND direction=? AND status='CHAMPION' ORDER BY version DESC LIMIT 1", (item['strategy'], item['direction'])).fetchone()
        con.close()
        if not row:
            continue
        import json
        meta = json.loads(row[1])
        await v5_runtime.robust_send_discord(
            core,
            f"🏆 Signal Champion 進化｜{item['strategy']} {item['direction']} v{int(row[0])}",
            f"Genome `{meta.get('genome_id') or 'legacy'}`｜Feature `{meta.get('feature_mode') or '—'}` ({int(meta.get('feature_count') or 0)}項)｜近期權重半衰期 `{float(meta.get('recency_half_life_days') or 0):.0f}天`\n"
            f"OOS PF `{float(meta.get('profit_factor') or 0):.2f}`｜EV `{float(meta.get('expectancy_r') or 0):+.3f}R`｜勝率 `{float(meta.get('test_win') or 0):.1%}`｜門檻 `{float(meta.get('threshold') or 0):.1%}`\n"
            f"最近 fold EV `{float(meta.get('recent_fold_ev_r') or 0):+.3f}R`｜PF `{float(meta.get('recent_fold_pf') or 0):.2f}`｜DD `{float(meta.get('max_drawdown_r') or 0):.1f}R`｜OOS入選 `{int(meta.get('selected_n') or 0)}`\n"
            'Genome 只能在各 fold 的歷史 train/cal 內挑選；未見 test 不參與挑參。新版本沒有通過乾淨 OOS 就不能取代舊 Champion。',
            0x2ECC71,
        )


async def notify_execution_results(core: Any, results: list[dict[str, Any]]) -> None:
    for x in results:
        if x.get('status') != 'CHAMPION':
            continue
        policy = x.get('policy') or {}
        targets = '/'.join(f"{float(v):.2f}R" for v in policy.get('target_rr') or [])
        alloc = '/'.join(f"{int(v)}%" for v in policy.get('allocations') or [])
        await v5_runtime.robust_send_discord(
            core,
            f"🧭 Execution Champion 進化｜{x['strategy']} {x['direction']} Exec v{int(x.get('execution_version') or 0)}",
            f"Signal model v`{int(x.get('model_version') or 0)}`｜expanding walk-forward point-in-time OOF\n"
            f"Audit folds `{int(x.get('qualified_walkforward_folds') or 0)}`｜合計 fills `{int(x.get('oos_fills') or 0)}`｜fill `{float(x.get('fill_rate') or 0):.1%}`\n"
            f"Aggregate PF `{float(x.get('profit_factor') or 0):.2f}`｜EV `{float(x.get('expectancy_r') or 0):+.3f}R`｜CI05 `{float(x.get('ev_bootstrap_05') or 0):+.3f}R`｜勝率 `{float(x.get('win_rate') or 0):.1%}`\n"
            f"最差 fold EV `{float(x.get('worst_fold_ev_r') or 0):+.3f}R`｜最近 fold EV `{float(x.get('recent_fold_ev_r') or 0):+.3f}R`｜最近 PF `{float(x.get('recent_fold_pf') or 0):.2f}`｜DD `{float(x.get('max_drawdown_r') or 0):.1f}R`\n"
            f"Entry `{float(policy.get('entry_atr') or 0):.3f} ATR`｜Stop `{float(policy.get('stop_atr') or 0):.2f} ATR`｜結構 `{policy.get('structure_mode') or '—'}`\n"
            f"TP `{targets}`｜分批 `{alloc}`｜TP2鎖 `{float(policy.get('lock_after_tp2_r') or 0):.2f}R`｜TP3鎖 `{float(policy.get('lock_after_tp3_r') or 0):.2f}R`\n"
            f"掛單 `{int(policy.get('expire_bars') or 0)}×15m`｜最長持有 `{int(policy.get('max_hold_bars') or 0)}×15m`｜成本 `{float(x.get('estimated_all_in_cost_bps') or 0):.1f}bps`。",
            0x2ECC71,
        )


def install(core: Any) -> None:
    v7_runtime.maybe_boot_notice = evolution_boot_notice
    v5_runtime._notify_promotions = notify_signal_promotions
    v7_runtime._notify_execution_results = notify_execution_results
