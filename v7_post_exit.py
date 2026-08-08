from __future__ import annotations

import json
import time
from typing import Any

import v5_runtime
import v7_runtime

_ORIGINAL_SCAN = v7_runtime.scan_v7
_ORIGINAL_INGEST = v7_runtime.ingest_completed_live_samples_v7


def sync_review_labels(core: Any) -> int:
    con = core.db(); rows = con.execute("SELECT signal_id,review_label FROM signals WHERE status='CLOSED' AND review_label IS NOT NULL").fetchall(); changed = 0
    for signal_id, label in rows:
        cur = con.execute('SELECT review_label FROM live_execution_samples WHERE signal_id=?', (signal_id,)).fetchone()
        if cur and cur[0] != label:
            con.execute('UPDATE live_execution_samples SET review_label=? WHERE signal_id=?', (label, signal_id)); changed += 1
    con.commit(); con.close(); return changed


def ingest_with_review_sync(core: Any) -> int:
    added = _ORIGINAL_INGEST(core); sync_review_labels(core); return added


async def scan_with_post_exit_review(core: Any) -> dict[str, Any]:
    before = set()
    con = core.db()
    for row in con.execute("SELECT signal_id FROM signals WHERE status='CLOSED' AND review_label IS NOT NULL").fetchall(): before.add(str(row[0]))
    con.close()
    result = await _ORIGINAL_SCAN(core)
    src = core._best_source('ETH', '15m')
    bars = core.load_bars('ETH', '15m', src, limit=1) if src else []
    if bars:
        core.post_exit_review(bars[-1])
    sync_review_labels(core)
    con = core.db(); rows = con.execute("SELECT signal_id,strategy,direction,realized_r,review_label,post_mfe_r,post_mae_r,payload FROM signals WHERE status='CLOSED' AND review_label IS NOT NULL ORDER BY exit_ts DESC LIMIT 30").fetchall(); con.close()
    for row in rows:
        sid = str(row[0])
        if sid in before: continue
        payload = json.loads(row[7]) if isinstance(row[7], str) else (row[7] or {})
        if payload.get('v7_review_notified'): continue
        await v5_runtime.robust_send_discord(core, f"🔎 24h 出場複盤｜{row[1]} {row[2]}", f"結果 `{float(row[3] or 0):+.2f}R`｜標籤 `{row[4]}`\n出場後 MFE `{float(row[5] or 0):.2f}R`｜MAE `{float(row[6] or 0):.2f}R`\n這份回饋只用於 execution/drift 診斷與重新驗證，不會因單筆結果直接改 Signal Champion。", 0x9B59B6)
        payload['v7_review_notified'] = int(time.time()); con = core.db(); con.execute('UPDATE signals SET payload=? WHERE signal_id=?', (json.dumps(payload, ensure_ascii=False), sid)); con.commit(); con.close()
    return result


def install(core: Any) -> None:
    v7_runtime.scan_v7 = scan_with_post_exit_review
    v7_runtime.ingest_completed_live_samples_v7 = ingest_with_review_sync
    async def scan_wrapper() -> dict[str, Any]: return await scan_with_post_exit_review(core)
    core.scan = scan_wrapper
