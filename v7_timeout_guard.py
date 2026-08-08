from __future__ import annotations

from typing import Any

import v7_runtime
import v7_trade_monitor as tm

_ORIGINAL_PROCESS = tm.process_trade_event


def process_with_timeout(core: Any, event: dict[str, Any]) -> dict[str, Any] | None:
    row = core.latest_signal()
    if row and row.get('status') == 'OPEN':
        payload = row.get('payload') or {}; policy = payload.get('execution_policy') or {}; max_bars = int(policy.get('max_hold_bars') or 32); deadline = int(row.get('created_at') or 0) + max_bars * 900; event_time = int(float(event.get('time') or 0)); price = float(event.get('price') or 0)
        # Historical execution audit counts max_hold from the original decision,
        # not from a delayed passive fill. Live follows the identical convention.
        if deadline > 0 and event_time >= deadline and price > 0:
            v7_runtime.close_signal_v7(core, row, price, 'TIMEOUT', event_time)
            return None
    return _ORIGINAL_PROCESS(core, event)


def install() -> None:
    tm.process_trade_event = process_with_timeout
