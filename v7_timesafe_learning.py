from __future__ import annotations

import bisect
import json
from typing import Any

import v5_runtime

SAMPLE_SCHEMA = 3


def closed_slice(rows: list[dict[str, Any]], tf_seconds: int, decision_close_ts: int, min_bars: int, max_bars: int) -> list[dict[str, Any]] | None:
    """Return only bars whose close time was known at the decision instant.

    Stored exchange candle timestamps are OPEN times. Comparing those directly to a
    15m sample timestamp can accidentally include the final close of an unfinished
    1H/4H/1D candle. We instead index by open_ts + timeframe_seconds.
    """
    if not rows:
        return None
    closes = [int(x['ts']) + int(tf_seconds) for x in rows]
    idx = bisect.bisect_right(closes, int(decision_close_ts))
    if idx < min_bars:
        return None
    return rows[max(0, idx - max_bars):idx]


def model_safe_features(builder: Any, *args: Any, **kwargs: Any) -> dict[str, float]:
    out = dict(builder(*args, **kwargs))
    # Cross-exchange agreement remains a live DATA QUALITY gate. The repository
    # does not have complete point-in-time multi-exchange history back to 2020, so
    # feeding a fabricated historical value to the model would create train/live
    # distribution shift. Keep the model feature constant instead of inventing it.
    if 'source_agreement_bps' in out:
        out['source_agreement_bps'] = 0.0
    return out


def generate_learning_samples_timesafe(core: Any, batch: int = 500) -> int:
    src15 = core._best_source('ETH', '15m'); src1h = core._best_source('ETH', '1h'); src4h = core._best_source('ETH', '4h'); src1d = core._best_source('ETH', '1d'); srcbtc = core._best_source('BTC', '1h')
    if not all((src15, src1h, src4h, src1d, srcbtc)):
        return 0
    m15 = core.load_bars('ETH', '15m', src15); h1 = core.load_bars('ETH', '1h', src1h); h4 = core.load_bars('ETH', '4h', src4h); d1 = core.load_bars('ETH', '1d', src1d); btc = core.load_bars('BTC', '1h', srcbtc)
    if min(map(len, (m15, h1, h4, d1, btc))) < 120:
        return 0
    ts15 = [int(x['ts']) for x in m15]
    last_ts = int(core.get_state(v5_runtime.REPLAY_STATE_KEY, core.START_TS) or core.START_TS)
    start_i = max(100, bisect.bisect_right(ts15, last_ts))
    con = core.db(); store = v5_runtime.ModelStore(con); learner = v5_runtime.Learner(store)
    created = processed = 0; newest = last_ts
    safe_builder = core.build_features
    for i in range(start_i, len(m15) - 25):
        if i % 4:
            continue
        sample_open_ts = ts15[i]
        decision_close_ts = sample_open_ts + core.TIMEFRAME_SECONDS['15m']
        d1s = closed_slice(d1, core.TIMEFRAME_SECONDS['1d'], decision_close_ts, 80, 420)
        h4s = closed_slice(h4, core.TIMEFRAME_SECONDS['4h'], decision_close_ts, 100, 900)
        h1s = closed_slice(h1, core.TIMEFRAME_SECONDS['1h'], decision_close_ts, 100, 1000)
        btcs = closed_slice(btc, core.TIMEFRAME_SECONDS['1h'], decision_close_ts, 50, 500)
        m15s = m15[max(0, i - 500):i + 1]
        if not all((d1s, h4s, h1s, btcs)):
            continue
        regime = v5_runtime.detect_regime(d1s, h4s, h1s)
        extras = core.derivative_history.extras_at(decision_close_ts)
        # Do not fabricate historical source agreement. model_safe_features makes
        # the corresponding predictor constant while data quality still uses live
        # cross-exchange validation outside the ML feature vector.
        extras.pop('source_agreement_bps', None)
        features = model_safe_features(safe_builder, m15s, h1s, btcs, regime, extras)
        priors = v5_runtime.baseline_direction_scores(features, regime)
        quality = max(60.0, (82.0 if src15 == 'gate' else 74.0) * (0.85 + 0.15 * float(extras.get('derivative_coverage', 0.0))))
        for strategy, dirs in priors.items():
            for direction, prior in dirs.items():
                if prior < .12:
                    continue
                success, pnl_r, mfe_r, mae_r = learner.strategy_outcome(m15, i, strategy, direction, 24)
                store.add_sample({'ts': sample_open_ts, 'strategy': strategy, 'direction': direction, 'regime': regime['regime'], 'phase': regime['phase'], 'features': features, 'success': success, 'pnl_r': pnl_r, 'mfe_r': mfe_r, 'mae_r': mae_r, 'source_quality': quality})
                created += 1
        processed += 1; newest = sample_open_ts
        if processed >= batch:
            break
    store.commit(); con.close()
    if newest > last_ts:
        core.set_state(v5_runtime.REPLAY_STATE_KEY, newest)
    return created


def migrate(core: Any) -> None:
    if int(core.get_state('point_in_time_sample_schema', 0) or 0) >= SAMPLE_SCHEMA:
        return
    con = core.db()
    con.execute('DROP TABLE IF EXISTS learning_samples_pre_timesafe_v7_archive')
    con.execute('CREATE TABLE learning_samples_pre_timesafe_v7_archive AS SELECT * FROM learning_samples')
    con.execute('DELETE FROM learning_samples')
    con.execute("UPDATE model_registry SET status='ARCHIVED' WHERE status='CHAMPION'")
    if con.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='execution_registry_v7'").fetchone():
        con.execute("UPDATE execution_registry_v7 SET status='ARCHIVED' WHERE status='CHAMPION'")
    con.commit(); con.close()
    core.set_state(v5_runtime.REPLAY_STATE_KEY, core.START_TS)
    core.set_state('v5_last_train_sample_total', 0)
    core.set_state('last_train_ts_v5', 0)
    core.set_state('v7_execution_signal_signature', [])
    core.set_state('v7_execution_last_attempt_ts', 0)
    core.set_state('point_in_time_sample_schema', SAMPLE_SCHEMA)


def install(core: Any) -> None:
    migrate(core)
    original_core_builder = core.build_features
    def safe_core_builder(*args: Any, **kwargs: Any) -> dict[str, float]:
        return model_safe_features(original_core_builder, *args, **kwargs)
    core.build_features = safe_core_builder
    # Historical generator references the symbol imported into v5_runtime, so it
    # must be replaced there as well; v5_async_runtime calls this function.
    v5_runtime.generate_learning_samples_v5 = lambda c, batch=500: generate_learning_samples_timesafe(c, batch)
