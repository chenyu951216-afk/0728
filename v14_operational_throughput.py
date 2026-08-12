from __future__ import annotations

import math
import os
import sqlite3
import statistics
import time
from typing import Any

import adaptive_v5
import execution_v7
import v5_runtime
import v9_final
import v10_final_integrity as final
import runtime_identity


VERSION = runtime_identity.RUNTIME_VERSION
BUSY_TIMEOUT_MS = max(10000, min(60000, int(os.getenv('SQLITE_BUSY_TIMEOUT_MS', '20000'))))
REPLAY_BATCH_DECISIONS = max(500, min(4000, int(os.getenv('STRICT_REPLAY_BATCH_DECISIONS', '2000'))))
DB_WRITE_RETRIES = 5


def _source_state_snapshot(core: Any) -> dict[str, Any]:
    # The derivative backfill tick completes before replay generation starts. Reusing
    # its in-memory state during one replay batch avoids thousands of system_state
    # reads without changing which provider rows are eligible for any historical T.
    strict = core.state.get('strict_replay') or {}
    cached = strict.get('final_derivative_coverage')
    if isinstance(cached, dict) and isinstance(cached.get('sources'), dict):
        return cached
    return final._load(core)


def _coverage_allows_from_state(state: dict[str, Any], key: str, ts: int) -> bool:
    rec = dict((state.get('sources') or {}).get(key) or {})
    return bool(not rec.get('disabled') and rec.get('last_success_at') and int(rec.get('processed_through') or 0) >= int(ts))


def _group_metric_rows(rows: list[Any], metric: str, upper: int, lower: int, limit_each: int) -> dict[str, list[Any]]:
    out: dict[str, list[Any]] = {}
    for row in rows:
        if str(row['metric']) != metric:
            continue
        ts = int(row['ts'])
        if ts > int(upper) or ts < int(lower):
            continue
        bucket = out.setdefault(str(row['source']), [])
        if len(bucket) < int(limit_each):
            bucket.append(row)
    return out


def fast_strict_derivative_extras(core: Any, history: Any, decision_ts: int) -> dict[str, float]:
    """Same strict derivative semantics as 8.2.2, using one DB query per decision.

    The query may read a superset window, but metric-specific upper timestamps are
    filtered before use. OI/liquidation/book still obey the historical publication
    safety lag; funding is still bounded by the decision clock. Future rows never enter
    a feature.
    """
    decision_ts = int(decision_ts)
    lagged = max(0, decision_ts - int(v9_final.DERIVATIVE_SAFETY_LAG_SECONDS))
    state = _source_state_snapshot(core)
    frozen = set(state.get('frozen_enrichment') or [])
    lower_all = min(lagged - 24 * 3600, decision_ts - 20 * 3600)

    con = history._con()
    try:
        rows = con.execute(
            "SELECT source,metric,ts,value,quality FROM derivative_history "
            "WHERE metric IN ('oi_usd','oi_coin','funding','liq_long_usd','liq_short_usd','book_imbalance') "
            "AND ts<=? AND ts>=? ORDER BY metric,source,ts DESC",
            (decision_ts, lower_all),
        ).fetchall()
    finally:
        con.close()

    oi_changes: list[float] = []
    oi_q: list[float] = []
    for metric in ('oi_usd', 'oi_coin'):
        grouped = _group_metric_rows(rows, metric, lagged, lagged - 24 * 3600, 6)
        for source, series in grouped.items():
            key = 'gate_stats' if source == 'gate' else 'bybit_oi' if source == 'bybit' else 'cg_oi' if source == 'coinglass' else None
            if not key or not _coverage_allows_from_state(state, key, lagged):
                continue
            if source == 'coinglass' and 'cg_oi' not in frozen:
                continue
            if len(series) >= 2:
                newest, oldest = float(series[0]['value']), float(series[-1]['value'])
                if oldest and math.isfinite(newest) and math.isfinite(oldest):
                    oi_changes.append(newest / oldest - 1.0)
                    oi_q.append(float(series[0]['quality']))

    funding_vals: list[float] = []
    funding_q: list[float] = []
    for source, series in _group_metric_rows(rows, 'funding', decision_ts, decision_ts - 20 * 3600, 6).items():
        key = 'funding_bybit' if source == 'bybit' else 'funding_binance' if source == 'binance' else None
        if key and _coverage_allows_from_state(state, key, decision_ts) and series:
            funding_vals.append(float(series[0]['value']))
            funding_q.append(float(series[0]['quality']))

    longs = _group_metric_rows(rows, 'liq_long_usd', lagged, lagged - 12 * 3600, 3)
    shorts = _group_metric_rows(rows, 'liq_short_usd', lagged, lagged - 12 * 3600, 3)
    liq_vals: list[float] = []
    liq_totals: list[float] = []
    liq_q: list[float] = []
    for source in set(longs) & set(shorts):
        key = 'gate_stats' if source == 'gate' else 'cg_liq' if source == 'coinglass' else None
        if not key or not _coverage_allows_from_state(state, key, lagged):
            continue
        if source == 'coinglass' and 'cg_liq' not in frozen:
            continue
        lv = max(0.0, float(longs[source][0]['value']))
        sv = max(0.0, float(shorts[source][0]['value']))
        total = lv + sv
        if total > 0:
            liq_vals.append((sv - lv) / total)
            liq_totals.append(total)
            liq_q.append(min(float(longs[source][0]['quality']), float(shorts[source][0]['quality'])))

    book_vals: list[float] = []
    book_q: list[float] = []
    if 'cg_book' in frozen and _coverage_allows_from_state(state, 'cg_book', lagged):
        for source, series in _group_metric_rows(rows, 'book_imbalance', lagged, lagged - 12 * 3600, 3).items():
            if source == 'coinglass' and series:
                book_vals.append(float(series[0]['value']))
                book_q.append(float(series[0]['quality']))

    available = (bool(oi_changes), bool(funding_vals), bool(liq_vals), bool(book_vals))
    qs = oi_q + funding_q + liq_q + book_q
    return {
        'oi_change': statistics.median(oi_changes) if oi_changes else 0.0,
        'funding': statistics.median(funding_vals) if funding_vals else 0.0,
        'liquidation_imbalance': statistics.median(liq_vals) if liq_vals else 0.0,
        'liquidation_intensity': math.log1p(statistics.median(liq_totals)) / 25.0 if liq_totals else 0.0,
        'book_imbalance': statistics.median(book_vals) if book_vals else 0.0,
        'oi_available': float(bool(oi_changes)),
        'funding_available': float(bool(funding_vals)),
        'liquidation_available': float(bool(liq_vals)),
        'book_available': float(bool(book_vals)),
        'derivative_coverage': sum(available) / 4.0,
        'derivative_quality': statistics.mean(qs) / 100.0 if qs else 0.0,
        'historical_derivative_safety_lag_seconds': float(v9_final.DERIVATIVE_SAFETY_LAG_SECONDS),
    }


def _configure_runtime_connection(con: sqlite3.Connection) -> sqlite3.Connection:
    con.execute(f'PRAGMA busy_timeout={BUSY_TIMEOUT_MS}')
    con.execute('PRAGMA synchronous=NORMAL')
    return con


def _install_connection_timeout(core: Any) -> None:
    original_db = core.db
    original_dcon = core.derivative_history._con

    def tuned_db() -> sqlite3.Connection:
        return _configure_runtime_connection(original_db())

    def tuned_dcon() -> sqlite3.Connection:
        return _configure_runtime_connection(original_dcon())

    core.db = tuned_db
    core.derivative_history._con = tuned_dcon


def _install_light_store_initializers(core: Any) -> None:
    # Ensure every schema/migration exists once, then make hot-path constructors pure
    # connection wrappers. Re-running CREATE TABLE / CREATE INDEX on every scan/replay
    # can contend on SQLite schema locks even when the schema is already correct.
    con = core.db()
    adaptive_v5.ModelStore(con)
    execution_v7.ExecutionStore(con)
    con.close()

    def light_model_init(self: Any, con: sqlite3.Connection) -> None:
        self.con = con

    def light_execution_init(self: Any, con: sqlite3.Connection) -> None:
        self.con = con

    adaptive_v5.ModelStore.__init__ = light_model_init
    execution_v7.ExecutionStore.__init__ = light_execution_init
    v5_runtime.ModelStore = adaptive_v5.ModelStore
    core.ModelStore = adaptive_v5.ModelStore


def _retry_write(fn: Any) -> Any:
    last: Exception | None = None
    for attempt in range(DB_WRITE_RETRIES):
        try:
            return fn()
        except sqlite3.OperationalError as exc:
            last = exc
            msg = str(exc).lower()
            if 'locked' not in msg and 'busy' not in msg:
                raise
            if attempt + 1 >= DB_WRITE_RETRIES:
                raise
            time.sleep(0.04 * (attempt + 1))
    if last:
        raise last
    return None


def _install_batched_live_market_writes(core: Any) -> None:
    def batched_upsert(bundle: dict[str, Any]) -> None:
        values: list[tuple[Any, ...]] = []

        def add(source: str, asset: str, tf: str, row: dict[str, Any]) -> None:
            values.append((
                source, asset, tf, int(row['ts']), float(row['o']), float(row['h']),
                float(row['l']), float(row['c']), float(row.get('v', 0)), float(row.get('qv', 0)),
            ))

        for key, tf in (('eth_1d','1d'),('eth_4h','4h'),('eth_1h','1h'),('eth_30m','30m'),('eth_15m','15m'),('eth_5m','5m'),('btc_1h','1h')):
            asset = 'BTC' if key == 'btc_1h' else 'ETH'
            for row in bundle.get(key, []):
                add(str(row.get('source') or 'gate'), asset, tf, row)
        for row in bundle.get('eth_spot_15m', []):
            add(str(row.get('source') or 'gate') + '_spot', 'ETH_SPOT', '15m', row)
        for source, rows in (bundle.get('validators') or {}).items():
            for row in rows:
                add(str(source), 'ETH', '15m', row)
        if not values:
            return

        def write_once() -> None:
            con = core.db()
            try:
                con.executemany(
                    'INSERT OR IGNORE INTO market_bars(source,asset,tf,ts,o,h,l,c,v,qv) VALUES(?,?,?,?,?,?,?,?,?,?)',
                    values,
                )
                con.commit()
            except Exception:
                con.rollback()
                raise
            finally:
                con.close()

        _retry_write(write_once)

    core.upsert_live_gate = batched_upsert


def _install_larger_replay_batches(core: Any) -> None:
    original_generate = v5_runtime.generate_learning_samples_v5

    def throughput_generate(c: Any, batch: int | None = None) -> int:
        target = REPLAY_BATCH_DECISIONS if batch is None else max(1, int(batch))
        return int(original_generate(c, target) or 0)

    v5_runtime.generate_learning_samples_v5 = throughput_generate
    core.generate_learning_samples = lambda batch=None: throughput_generate(core, batch)


def install(core: Any) -> None:
    _install_connection_timeout(core)
    _install_light_store_initializers(core)
    _install_batched_live_market_writes(core)
    _install_larger_replay_batches(core)

    final.strict_derivative_extras = fast_strict_derivative_extras
    v9_final._strict_derivative_extras = lambda history, ts: fast_strict_derivative_extras(core, history, ts)

    strict = core.state.setdefault('strict_replay', {})
    strict['operational_throughput'] = {
        'runtime': VERSION,
        'replay_batch_decisions': REPLAY_BATCH_DECISIONS,
        'sqlite_busy_timeout_ms': BUSY_TIMEOUT_MS,
        'live_market_write_transactions_per_scan': 1,
        'runtime_store_ddl_repeated': False,
        'derivative_feature_query_mode': 'single_combined_query_per_decision',
        'sample_density_changed': False,
        'no_lookahead_semantics_changed': False,
        'writer_fairness_guard': True,
    }
    core.state['runtime_version'] = VERSION
    runtime_identity.stamp(core)
