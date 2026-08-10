from __future__ import annotations

import bisect
import statistics
import time
from typing import Any

import execution_v7
import v5_runtime
import v7_timesafe_learning
import v9_final
import v10_final_integrity as final

VERSION = '8.2.3-20260810'
INTEGRITY_SCHEMA = 2
STATE_KEY = 'replay_cursor_integrity_schema'


def _reset_derived_replay(core: Any, reason: str) -> None:
    con = core.db()
    tables = {str(r[0]) for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
    if 'learning_samples' in tables:
        con.execute('DELETE FROM learning_samples')
    if 'learning_feature_snapshots' in tables:
        con.execute('DELETE FROM learning_feature_snapshots')
    if 'model_registry' in tables:
        con.execute("UPDATE model_registry SET status='ARCHIVED' WHERE status='CHAMPION'")
    if 'execution_registry_v7' in tables:
        con.execute("UPDATE execution_registry_v7 SET status='ARCHIVED' WHERE status='CHAMPION'")
    con.commit(); con.close()
    core.set_state(v5_runtime.REPLAY_STATE_KEY, int(core.START_TS))
    core.set_state('v5_last_train_sample_total', 0)
    core.set_state('last_train_ts_v5', 0)
    core.set_state('v7_execution_signal_signature', [])
    core.set_state('v7_execution_last_attempt_ts', 0)
    core.state.setdefault('learning', {})['replay_cursor_integrity_reset'] = {
        'at': int(time.time()),
        'reason': reason,
        'raw_market_preserved': True,
        'raw_derivatives_preserved': True,
        'dataset_provenance_preserved': True,
    }


def _decision_state(*, htf_ready: bool, future_ready: bool, continuity_ready: bool) -> str:
    if not htf_ready:
        return 'WARMUP'
    if not future_ready:
        return 'BLOCK_FUTURE_PATH'
    if not continuity_ready:
        return 'BLOCK_PRICE_GAP'
    return 'READY'


def _build_model_features(core: Any, m15s: list[dict[str, Any]], h1s: list[dict[str, Any]],
                          btcs: list[dict[str, Any]], regime: dict[str, Any],
                          extras: dict[str, Any]) -> dict[str, float]:
    """Build replay features through the callable builder, never through its result."""
    builder = getattr(core, 'build_features', None)
    if not callable(builder):
        raise TypeError(f'core.build_features must be callable, got {type(builder).__name__}')
    return v7_timesafe_learning.model_safe_features(builder, m15s, h1s, btcs, regime, extras)


def _ema20_prefix(m15: list[dict[str, Any]]) -> list[float]:
    """Exact prefix EMA20 values; each index uses only closes <= that index."""
    if not m15:
        return []
    alpha = 2.0 / 21.0
    out: list[float] = []
    value = final.signal.f(m15[0]['c'])
    out.append(value)
    for row in m15[1:]:
        close = final.signal.f(row['c'])
        value = alpha * close + (1.0 - alpha) * value
        out.append(value)
    return out


def _true_ranges(m15: list[dict[str, Any]]) -> list[float]:
    """TR[i] is the causal true range for bar i; TR[0] is unused/zero."""
    out = [0.0] * len(m15)
    for i in range(1, len(m15)):
        cur, prev = m15[i], m15[i - 1]
        high, low, prev_close = final.signal.f(cur['h']), final.signal.f(cur['l']), final.signal.f(prev['c'])
        out[i] = max(high - low, abs(high - prev_close), abs(low - prev_close))
    return out


def _atr14_at(true_ranges: list[float], i: int) -> float:
    if i <= 0:
        return 0.0
    start = max(1, i - 13)
    vals = true_ranges[start:i + 1]
    return sum(vals) / len(vals) if vals else 0.0


def _one_outcome_5m_precomputed(
    m15: list[dict[str, Any]], i15: int, future5: list[dict[str, Any]], strategy: str,
    direction: str, entry_scale: float, stop_atr: float, target_r: float,
    ema20_value: float, atr14_value: float,
) -> tuple[bool, float, float, float]:
    """Exact v10 5m label math with repeated causal EMA/ATR work removed."""
    close = final.signal.f(m15[i15]['c'])
    a = max(float(atr14_value), close * .001)
    sign = 1 if direction == 'LONG' else -1
    base = execution_v7._base_entry_factor(strategy) * float(entry_scale)

    if strategy in ('TREND_PULLBACK', 'RANGE_MEAN_REVERSION'):
        entry = min(close - base * a, ema20_value) if direction == 'LONG' else max(close + base * a, ema20_value)
    elif strategy == 'BREAKOUT_RETEST':
        # strict replay is well past warm-up here, so this is exactly past[-28:-1].
        window = m15[max(0, i15 - 27):i15]
        ph = max((final.signal.f(x['h']) for x in window), default=close)
        pl = min((final.signal.f(x['l']) for x in window), default=close)
        if direction == 'LONG' and ph < close:
            entry = ph
        elif direction == 'SHORT' and pl > close:
            entry = pl
        else:
            entry = close - sign * base * a
    else:
        entry = close - sign * base * a

    risk = max(float(stop_atr) * a, entry * execution_v7.MIN_STOP_PCT)
    stop = entry - sign * risk
    target = entry + sign * float(target_r) * risk
    wait15 = {
        'MOMENTUM_CONTINUATION': 4, 'SQUEEZE_EXPANSION': 5,
        'FAILED_BREAKOUT_REVERSAL': 5, 'LIQUIDITY_SWEEP_REVERSAL': 6,
        'RANGE_MEAN_REVERSION': 6, 'TREND_PULLBACK': 8, 'BREAKOUT_RETEST': 8,
    }.get(strategy, 6)
    wait5 = min(len(future5), wait15 * 3)
    fill = next((j for j, b in enumerate(future5[:wait5]) if final.signal.f(b['l']) <= entry <= final.signal.f(b['h'])), None)
    if fill is None:
        return False, 0.0, 0.0, 0.0

    mfe = mae = 0.0
    last = entry
    for j, bar in enumerate(future5[fill:]):
        low, high, last = final.signal.f(bar['l']), final.signal.f(bar['h']), final.signal.f(bar['c'])
        favorable = (high - entry) / risk if direction == 'LONG' else (entry - low) / risk
        adverse = (entry - low) / risk if direction == 'LONG' else (high - entry) / risk
        mfe, mae = max(mfe, favorable), max(mae, adverse)
        stop_hit = low <= stop if direction == 'LONG' else high >= stop
        if stop_hit:
            return True, -1.0, mfe, mae
        # Same conservative fill-bar rule as v10: no target credit on the fill bar.
        if j == 0:
            continue
        target_hit = high >= target if direction == 'LONG' else low <= target
        if target_hit:
            return True, float(target_r), mfe, mae

    rr = max(-1.0, min(float(target_r), (last - entry) * sign / max(risk, 1e-9)))
    return True, rr, mfe, mae


def _strategy_outcome_5m_precomputed(
    m15: list[dict[str, Any]], i15: int, future5: list[dict[str, Any]], strategy: str,
    direction: str, ema20_value: float, atr14_value: float,
) -> tuple[int, float, float, float]:
    profiles = ((.60,.90,1.15),(.85,1.15,1.40),(1.00,1.40,1.70),(1.20,1.75,2.10),(1.45,2.15,2.65))
    rows = [
        _one_outcome_5m_precomputed(
            m15, i15, future5, strategy, direction, entry_scale, stop_atr, target_r,
            ema20_value, atr14_value,
        )
        for entry_scale, stop_atr, target_r in profiles
    ]
    filled = [x for x in rows if x[0]]
    if not filled:
        return 0, 0.0, 0.0, 0.0
    pnls = [x[1] for x in filled]
    robust = statistics.median(pnls)
    pnl = robust * min(1.0, len(filled) / 3.0)
    positive_ratio = sum(x > .10 for x in pnls) / len(pnls)
    success = int(len(filled) >= 2 and pnl > .10 and positive_ratio >= .60)
    return success, pnl, statistics.median([x[2] for x in filled]), statistics.median([x[3] for x in filled])


def strict_generate_samples(core: Any, batch: int = 500) -> int:
    src15 = final.deterministic_best_source(core, 'ETH', '15m')
    src5 = final.deterministic_best_source(core, 'ETH', '5m')
    src1h = final.deterministic_best_source(core, 'ETH', '1h')
    src4h = final.deterministic_best_source(core, 'ETH', '4h')
    src1d = final.deterministic_best_source(core, 'ETH', '1d')
    srcbtc = final.deterministic_best_source(core, 'BTC', '1h')
    if not all((src15, src5, src1h, src4h, src1d, srcbtc)):
        core.state.setdefault('learning', {})['replay_price_blocker'] = {
            'blocked': True, 'reason': 'required deterministic price source is not available yet'
        }
        return 0

    m15 = core.load_bars('ETH', '15m', src15)
    m5 = core.load_bars('ETH', '5m', src5)
    h1 = core.load_bars('ETH', '1h', src1h)
    h4 = core.load_bars('ETH', '4h', src4h)
    d1 = core.load_bars('ETH', '1d', src1d)
    btc = core.load_bars('BTC', '1h', srcbtc)
    if min(map(len, (m15, m5, h1, h4, d1, btc))) < 120:
        core.state.setdefault('learning', {})['replay_price_blocker'] = {
            'blocked': True, 'reason': 'required price series still lacks minimum warm-up history'
        }
        return 0

    ts15 = [int(x['ts']) for x in m15]
    ts5 = [int(x['ts']) for x in m5]
    ema20 = _ema20_prefix(m15)
    tr15 = _true_ranges(m15)
    last_ts = int(core.get_state(v5_runtime.REPLAY_STATE_KEY, core.START_TS) or core.START_TS)
    start_i = max(100, bisect.bisect_right(ts15, last_ts))
    con = core.db(); store = v5_runtime.ModelStore(con)
    created = examined = warmup_skipped = 0
    newest_committed = last_ts
    blocker: dict[str, Any] | None = None
    last_regime_key: tuple[int, int, int] | None = None
    last_regime: dict[str, Any] | None = None

    for i in range(start_i, len(m15) - 33):
        if i % v9_final.REPLAY_STRIDE_BARS:
            continue
        sample_open = ts15[i]
        decision_close = sample_open + 900
        examined += 1

        d1s = v9_final._closed_slice(d1, 86400, decision_close, 420)
        h4s = v9_final._closed_slice(h4, 14400, decision_close, 900)
        h1s = v9_final._closed_slice(h1, 3600, decision_close, 1000)
        btcs = v9_final._closed_slice(btc, 3600, decision_close, 500)
        m15s = m15[max(0, i - 500):i + 1]
        j5 = bisect.bisect_left(ts5, decision_close)
        future5 = m5[j5:j5 + 96]

        htf_ready = len(d1s) >= 80 and len(h4s) >= 100 and len(h1s) >= 100 and len(btcs) >= 50
        future_ready = bool(len(future5) >= 96 and int(future5[0]['ts']) == decision_close and v9_final._continuous(future5, 300))
        continuity_ready = bool(
            v9_final._continuous(m15s[-min(160, len(m15s)):], 900)
            and v9_final._continuous(h1s[-min(120, len(h1s)):], 3600)
            and v9_final._continuous(h4s[-min(60, len(h4s)):], 14400)
            and v9_final._continuous(d1s[-min(30, len(d1s)):], 86400)
            and v9_final._continuous(btcs[-min(120, len(btcs)):], 3600)
        ) if htf_ready else False

        decision_state = _decision_state(htf_ready=htf_ready, future_ready=future_ready, continuity_ready=continuity_ready)
        if decision_state == 'WARMUP':
            newest_committed = sample_open
            warmup_skipped += 1
            if examined >= batch:
                break
            continue
        if decision_state != 'READY':
            blocker = {
                'blocked': True,
                'at_ts': sample_open,
                'decision_close_ts': decision_close,
                'reason': '5m future path is not complete yet' if decision_state == 'BLOCK_FUTURE_PATH' else 'core price continuity gap is unresolved',
                'state': decision_state,
                'sources': {'15m': src15, '5m': src5, '1h': src1h, '4h': src4h, '1d': src1d, 'btc1h': srcbtc},
            }
            break

        regime_key = (int(d1s[-1]['ts']), int(h4s[-1]['ts']), int(h1s[-1]['ts']))
        if regime_key == last_regime_key and last_regime is not None:
            regime = last_regime
        else:
            regime = v5_runtime.detect_regime(d1s, h4s, h1s)
            last_regime_key, last_regime = regime_key, regime

        extras = final.strict_derivative_extras(core, core.derivative_history, decision_close)
        features = _build_model_features(core, m15s, h1s, btcs, regime, extras)
        quality = max(58.0, 78.0 * (.84 + .16 * float(extras.get('derivative_coverage', 0.0))))
        atr14 = _atr14_at(tr15, i)

        # Compute every strategy/direction outcome before the first SQLite INSERT.
        # This keeps the writer transaction short instead of holding it while doing
        # 70 path simulations for the same historical decision.
        pending: list[dict[str, Any]] = []
        for strategy in final.signal.STRATEGIES:
            for direction in final.signal.DIRECTIONS:
                success, pnl, mfe, mae = _strategy_outcome_5m_precomputed(
                    m15, i, future5, strategy, direction, ema20[i], atr14
                )
                pending.append({
                    'ts': sample_open, 'strategy': strategy, 'direction': direction,
                    'regime': regime['regime'], 'phase': regime['phase'], 'features': features,
                    'success': success, 'pnl_r': pnl, 'mfe_r': mfe, 'mae_r': mae,
                    'source_quality': quality,
                })
        for row in pending:
            store.add_sample(row)
        created += len(pending)
        newest_committed = sample_open

        if examined % 100 == 0:
            learning = core.state.setdefault('learning', {})
            learning['runtime_heartbeat_at'] = int(time.time())
            learning['strict_replay_inflight'] = {
                'examined_decisions': examined,
                'created_strategy_direction_samples': created,
                'cursor_candidate_ts': newest_committed,
                'status': 'RUNNING',
            }
        if examined >= batch:
            break

    store.commit(); con.close()
    if newest_committed > last_ts:
        core.set_state(v5_runtime.REPLAY_STATE_KEY, newest_committed)

    learning = core.state.setdefault('learning', {})
    learning['replay_price_blocker'] = blocker or {'blocked': False}
    learning['strict_replay_last_batch'] = {
        'schema': final.SAMPLE_SCHEMA,
        'cursor_integrity_schema': INTEGRITY_SCHEMA,
        'event_path': '5m_after_frozen_15m_decision',
        'examined_decisions': examined,
        'warmup_skipped': warmup_skipped,
        'created_strategy_direction_samples': created,
        'cursor_advanced_only_through_legal_decisions_or_explicit_warmup': True,
        'unresolved_price_gap_can_never_be_skipped': True,
        'feature_builder_contract_verified': True,
        'outcome_prefix_precompute': 'exact causal EMA20 + ATR14 reused across strategy/profile simulations',
        'outcomes_computed_before_sample_transaction': True,
        'future_usage': 'labels only; never features or parameter selection',
    }
    return created


def install(core: Any) -> None:
    current = int(core.get_state(STATE_KEY, 0) or 0)
    if current < INTEGRITY_SCHEMA:
        _reset_derived_replay(core, '8.2.2 replay integrity: rebuild derived learning after first-real-sample feature-builder fix')
        core.set_state(STATE_KEY, INTEGRITY_SCHEMA)

    final.generate_samples = strict_generate_samples

    original_train = v5_runtime.train_v5
    def price_complete_train(c: Any, *args: Any, **kwargs: Any):
        chosen = None
        for asset, tf in c.BACKFILL_PLAN:
            earliest = c._earliest(asset, tf)
            if earliest is None or earliest > c.START_TS + 2 * c.TIMEFRAME_SECONDS[tf]:
                chosen = (asset, tf)
                break
        if chosen:
            c.state.setdefault('learning', {})['price_history_certification_gate'] = {
                'ready': False,
                'reason': f'price history repair still active for {chosen[0]} {chosen[1]}',
            }
            return []
        c.state.setdefault('learning', {})['price_history_certification_gate'] = {'ready': True}
        return original_train(c, *args, **kwargs)
    v5_runtime.train_v5 = price_complete_train

    core.state.setdefault('strict_replay', {})['cursor_integrity'] = {
        'runtime': VERSION,
        'schema': INTEGRITY_SCHEMA,
        'unresolved_price_gap_can_advance_cursor': False,
        'explicit_warmup_can_advance_cursor': True,
        'formal_training_requires_price_repair_complete': True,
        'feature_builder_contract_verified': True,
        'causal_outcome_precompute': True,
        'outcomes_computed_before_sample_transaction': True,
    }
    core.state['runtime_version'] = VERSION
    core.app.version = '8.2.3'
