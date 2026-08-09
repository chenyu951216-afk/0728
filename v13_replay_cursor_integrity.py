from __future__ import annotations

import bisect
import time
from typing import Any

import execution_v7
import v5_runtime
import v7_timesafe_learning
import v9_final
import v10_final_integrity as final

VERSION = '8.2.1-20260809'
INTEGRITY_SCHEMA = 1
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
    last_ts = int(core.get_state(v5_runtime.REPLAY_STATE_KEY, core.START_TS) or core.START_TS)
    start_i = max(100, bisect.bisect_right(ts15, last_ts))
    con = core.db(); store = v5_runtime.ModelStore(con)
    created = examined = warmup_skipped = 0
    newest_committed = last_ts
    blocker: dict[str, Any] | None = None

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

        state = _decision_state(htf_ready=htf_ready, future_ready=future_ready, continuity_ready=continuity_ready)
        if state == 'WARMUP':
            # Warm-up is an explicit non-tradable historical interval. It may advance
            # the time cursor because there is nothing the final strategy could have
            # legally evaluated before the required higher-timeframe lookback exists.
            newest_committed = sample_open
            warmup_skipped += 1
            if examined >= batch:
                break
            continue
        if state != 'READY':
            blocker = {
                'blocked': True,
                'at_ts': sample_open,
                'decision_close_ts': decision_close,
                'reason': '5m future path is not complete yet' if state == 'BLOCK_FUTURE_PATH' else 'core price continuity gap is unresolved',
                'state': state,
                'sources': {'15m': src15, '5m': src5, '1h': src1h, '4h': src4h, '1d': src1d, 'btc1h': srcbtc},
            }
            break

        regime = v5_runtime.detect_regime(d1s, h4s, h1s)
        extras = final.strict_derivative_extras(core, core.derivative_history, decision_close)
        features = v7_timesafe_learning.model_safe_features(core.build_features(m15s, h1s, btcs, regime, extras))
        quality = max(58.0, 78.0 * (.84 + .16 * float(extras.get('derivative_coverage', 0.0))))
        for strategy in final.signal.STRATEGIES:
            for direction in final.signal.DIRECTIONS:
                success, pnl, mfe, mae = final.strategy_outcome_5m(m15, i, future5, strategy, direction)
                store.add_sample({
                    'ts': sample_open, 'strategy': strategy, 'direction': direction,
                    'regime': regime['regime'], 'phase': regime['phase'], 'features': features,
                    'success': success, 'pnl_r': pnl, 'mfe_r': mfe, 'mae_r': mae,
                    'source_quality': quality,
                })
                created += 1
        newest_committed = sample_open
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
        'future_usage': 'labels only; never features or parameter selection',
    }
    return created


def install(core: Any) -> None:
    current = int(core.get_state(STATE_KEY, 0) or 0)
    if current < INTEGRITY_SCHEMA:
        _reset_derived_replay(core, '8.2.1 replay cursor integrity: old replay could advance across unresolved price gaps')
        core.set_state(STATE_KEY, INTEGRITY_SCHEMA)

    final.generate_samples = strict_generate_samples

    # A formal training pass also requires that no required historical price series is
    # still being repaired. This is an additional certification gate, not a shortcut.
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
    }
    core.state['runtime_version'] = VERSION
    core.app.version = '8.2.1'
