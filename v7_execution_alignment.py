from __future__ import annotations

from typing import Any

import execution_v7

DECISION_TF_SECONDS = 900


def market_data_close_aligned(core: Any) -> dict[str, Any]:
    src5 = core._best_source('ETH', '5m'); src15 = core._best_source('ETH', '15m'); src30 = core._best_source('ETH', '30m'); src1h = core._best_source('ETH', '1h')
    if not (src5 and src15 and src30 and src1h):
        return {}
    m5 = core.load_bars('ETH', '5m', src5); m15 = core.load_bars('ETH', '15m', src15); m30 = core.load_bars('ETH', '30m', src30); h1 = core.load_bars('ETH', '1h', src1h)
    # A 15m decision is made at sample_open+900. HTF eligibility keys are shifted
    # so execution_v7._slice_to only exposes bars closed by that instant.
    return {
        'm5': m5,
        'm15': m15,
        'm30': m30,
        'h1': h1,
        'index15': {int(x['ts']): i for i, x in enumerate(m15)},
        'ts5': [int(x['ts']) for x in m5],
        'ts30': [int(x['ts']) + 1800 - DECISION_TF_SECONDS for x in m30],
        'ts1h': [int(x['ts']) + 3600 - DECISION_TF_SECONDS for x in h1],
    }


def install() -> None:
    execution_v7._market_data = market_data_close_aligned
