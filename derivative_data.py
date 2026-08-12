from __future__ import annotations

import json
import math
import os
import sqlite3
import statistics
import time
from pathlib import Path
from typing import Any

import httpx


def _f(value: Any, default: float = 0.0) -> float:
    try:
        x = float(value)
        return x if math.isfinite(x) else default
    except (TypeError, ValueError):
        return default


def _ts_seconds(value: Any) -> int:
    x = int(_f(value))
    return x // 1000 if x > 20_000_000_000 else x


class DerivativeHistory:
    """Best-effort historical derivatives cache with provenance.

    Coinglass is optional. When it is unavailable, the system still backfills
    exchange-native OI/funding where possible. Missing metrics remain explicitly
    missing and are exposed through availability/coverage features; zeros are not
    treated as proof that a metric was neutral.
    """

    COINGLASS = "https://open-api-v4.coinglass.com/api"

    def __init__(self, db_path: str, coinglass_key: str | None = None, timeout: float = 20.0) -> None:
        self.db_path = db_path
        self.coinglass_key = coinglass_key if coinglass_key is not None else os.getenv("COINGLASS_API_KEY", "")
        self.timeout = timeout

    def set_db_path(self, db_path: str) -> None:
        self.db_path = db_path

    def _con(self) -> sqlite3.Connection:
        path = Path(self.db_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        con = sqlite3.connect(path, timeout=30, check_same_thread=False)
        con.row_factory = sqlite3.Row
        con.execute("PRAGMA journal_mode=WAL")
        con.execute("""CREATE TABLE IF NOT EXISTS derivative_history(
            source TEXT NOT NULL,
            metric TEXT NOT NULL,
            ts INTEGER NOT NULL,
            value REAL NOT NULL,
            quality REAL NOT NULL,
            meta TEXT NOT NULL DEFAULT '{}',
            PRIMARY KEY(source, metric, ts)
        )""")
        con.execute("CREATE INDEX IF NOT EXISTS ix_derivative_metric_ts ON derivative_history(metric, ts)")
        con.execute("""CREATE TABLE IF NOT EXISTS derivative_backfill_state(
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL,
            updated_at INTEGER NOT NULL
        )""")
        con.commit()
        return con

    def ensure_schema(self) -> None:
        self._con().close()

    def _insert(self, source: str, metric: str, rows: list[tuple[int, float, float, dict[str, Any]]]) -> int:
        if not rows:
            return 0
        con = self._con()
        before = con.total_changes
        con.executemany(
            "INSERT OR IGNORE INTO derivative_history(source,metric,ts,value,quality,meta) VALUES(?,?,?,?,?,?)",
            [(source, metric, int(ts), float(value), float(quality), json.dumps(meta, separators=(",", ":")))
             for ts, value, quality, meta in rows if ts > 0 and math.isfinite(float(value))],
        )
        added = con.total_changes - before
        con.commit(); con.close()
        return added

    def _earliest(self, metric: str) -> int | None:
        con = self._con(); row = con.execute("SELECT MIN(ts) FROM derivative_history WHERE metric=?", (metric,)).fetchone(); con.close()
        return int(row[0]) if row and row[0] is not None else None

    def _latest(self, metric: str) -> int | None:
        con = self._con(); row = con.execute("SELECT MAX(ts) FROM derivative_history WHERE metric=?", (metric,)).fetchone(); con.close()
        return int(row[0]) if row and row[0] is not None else None

    def _get_state(self, key: str, default: Any = None) -> Any:
        con = self._con()
        row = con.execute("SELECT value FROM derivative_backfill_state WHERE key=?", (key,)).fetchone()
        con.close()
        if not row:
            return default
        try:
            return json.loads(row[0])
        except Exception:
            return default

    def _set_state(self, key: str, value: Any) -> None:
        con = self._con(); con.execute(
            "INSERT INTO derivative_backfill_state(key,value,updated_at) VALUES(?,?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_at=excluded.updated_at",
            (key, json.dumps(value, ensure_ascii=False), int(time.time())),
        ); con.commit(); con.close()

    async def _cg(self, path: str, params: dict[str, Any]) -> Any:
        if not self.coinglass_key:
            raise RuntimeError("COINGLASS_API_KEY not configured")
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            r = await client.get(self.COINGLASS + path, params=params,
                                 headers={"CG-API-KEY": self.coinglass_key, "Accept": "application/json"})
            r.raise_for_status(); data = r.json()
        if str(data.get("code")) not in ("0", "200", "None"):
            raise RuntimeError(f"Coinglass code={data.get('code')} msg={data.get('msg')}")
        return data.get("data")

    async def _backfill_coinglass_oi(self, start_ts: int, end_ts: int) -> int:
        data = await self._cg("/futures/open-interest/aggregated-history", {
            "symbol": "ETH", "interval": "4h", "limit": 1000, "start_time": start_ts * 1000, "end_time": end_ts * 1000, "unit": "usd",
        })
        rows = [(_ts_seconds(x.get("time")), _f(x.get("close")), 95.0, {"kind": "aggregated_oi_usd"}) for x in (data or [])]
        return self._insert("coinglass", "oi_usd", rows)

    async def _backfill_coinglass_liquidation(self, start_ts: int, end_ts: int) -> int:
        data = await self._cg("/futures/liquidation/aggregated-history", {
            "exchange_list": "Binance,OKX,Bybit,Gate", "symbol": "ETH", "interval": "4h", "limit": 1000,
            "start_time": start_ts * 1000, "end_time": end_ts * 1000,
        })
        long_rows, short_rows = [], []
        for x in data or []:
            ts = _ts_seconds(x.get("time"))
            long_rows.append((ts, _f(x.get("aggregated_long_liquidation_usd")), 92.0, {}))
            short_rows.append((ts, _f(x.get("aggregated_short_liquidation_usd")), 92.0, {}))
        return self._insert("coinglass", "liq_long_usd", long_rows) + self._insert("coinglass", "liq_short_usd", short_rows)

    async def _backfill_coinglass_book(self, start_ts: int, end_ts: int) -> int:
        data = await self._cg("/futures/orderbook/aggregated-ask-bids-history", {
            "exchange_list": "ALL", "symbol": "ETH", "interval": "4h", "limit": 1000,
            "start_time": start_ts * 1000, "end_time": end_ts * 1000, "range": "1",
        })
        rows = []
        for x in data or []:
            bid, ask = _f(x.get("aggregated_bids_usd")), _f(x.get("aggregated_asks_usd"))
            imbalance = (bid - ask) / max(bid + ask, 1e-9)
            rows.append((_ts_seconds(x.get("time")), imbalance, 88.0, {"range_pct": 1}))
        return self._insert("coinglass", "book_imbalance", rows)

    async def _backfill_coinglass_oi_weighted_funding(self, start_ts: int, end_ts: int) -> int:
        data = await self._cg("/futures/funding-rate/oi-weight-history", {
            "symbol": "ETH", "interval": "4h", "limit": 1000,
            "start_time": start_ts * 1000, "end_time": end_ts * 1000,
        })
        rows = [
            (_ts_seconds(x.get("time")), _f(x.get("close")), 94.0,
             {"kind": "oi_weighted_funding"})
            for x in (data or [])
        ]
        return self._insert("coinglass", "oi_weighted_funding", rows)

    async def _backfill_coinglass_taker(self, start_ts: int, end_ts: int) -> int:
        data = await self._cg("/futures/aggregated-taker-buy-sell-volume/history", {
            "exchange_list": "Binance,OKX,Bybit,Gate", "symbol": "ETH",
            "interval": "4h", "limit": 1000, "unit": "usd",
            "start_time": start_ts * 1000, "end_time": end_ts * 1000,
        })
        rows = []
        for x in data or []:
            buy = _f(x.get("aggregated_buy_volume_usd"))
            sell = _f(x.get("aggregated_sell_volume_usd"))
            total = max(buy + sell, 0.0)
            if total > 0:
                rows.append((_ts_seconds(x.get("time")), (buy - sell) / total, 92.0,
                             {"exchanges": "Binance,OKX,Bybit,Gate", "kind": "aggregated_futures_taker_imbalance"}))
        return self._insert("coinglass", "taker_imbalance", rows)

    async def _backfill_coinglass_crowd_ratio(self, start_ts: int, end_ts: int) -> int:
        data = await self._cg("/futures/global-long-short-account-ratio/history", {
            "exchange": "Binance", "symbol": "ETHUSDT", "interval": "4h", "limit": 1000,
            "start_time": start_ts * 1000, "end_time": end_ts * 1000,
        })
        rows = []
        for x in data or []:
            long_pct = _f(x.get("global_account_long_percent"))
            short_pct = _f(x.get("global_account_short_percent"))
            total = long_pct + short_pct
            if total > 0:
                rows.append((_ts_seconds(x.get("time")), (long_pct - short_pct) / total, 90.0,
                             {"exchange": "Binance", "kind": "global_account_skew"}))
        return self._insert("coinglass", "crowd_skew", rows)

    async def _backfill_coinglass_top_position_ratio(self, start_ts: int, end_ts: int) -> int:
        data = await self._cg("/futures/top-long-short-position-ratio/history", {
            "exchange": "Binance", "symbol": "ETHUSDT", "interval": "4h", "limit": 1000,
            "start_time": start_ts * 1000, "end_time": end_ts * 1000,
        })
        rows = []
        for x in data or []:
            long_pct = _f(x.get("top_position_long_percent"))
            short_pct = _f(x.get("top_position_short_percent"))
            total = long_pct + short_pct
            if total > 0:
                rows.append((_ts_seconds(x.get("time")), (long_pct - short_pct) / total, 90.0,
                             {"exchange": "Binance", "kind": "top_position_skew"}))
        return self._insert("coinglass", "top_position_skew", rows)

    async def coinglass_liquidation_heatmap(self, range_name: str = "3d") -> Any:
        """Return a current heatmap snapshot for live risk gating only.

        The endpoint is not a reconstructable point-in-time history. Callers must never
        inject this response into historical samples or an execution backtest.
        """
        return await self._cg("/futures/liquidation/heatmap/model1", {
            "exchange": "Binance", "symbol": "ETHUSDT", "range": range_name,
        })

    async def _backfill_native_oi(self, hub: Any, end_ts: int) -> int:
        rows = await hub.fetch_bybit_oi_history("ETH", "4h", end_ts=end_ts, limit=200)
        return self._insert("bybit", "oi_coin", [(int(x["ts"]), _f(x["oi"]), 82.0, {}) for x in rows])

    async def _backfill_native_funding(self, hub: Any, source: str, end_ts: int) -> int:
        rows = await hub.fetch_funding_history(source, "ETH", end_ts=end_ts, limit=200)
        return self._insert(source, "funding", [(int(x["ts"]), _f(x["funding"]), 84.0, {}) for x in rows])

    async def backfill_tick(self, hub: Any, start_ts: int, pages: int = 2) -> dict[str, Any]:
        self.ensure_schema()
        result = {"coinglass_enabled": bool(self.coinglass_key), "attempted": [], "errors": []}
        now = int(time.time())
        interval = 4 * 3600

        # Coinglass is filled forward from 2020 so the learner never races ahead of
        # derivative history and silently treats a not-yet-downloaded point as neutral.
        if self.coinglass_key:
            for metric, fn in (("oi_usd", self._backfill_coinglass_oi),
                               ("liq_long_usd", self._backfill_coinglass_liquidation),
                               ("book_imbalance", self._backfill_coinglass_book)):
                added = 0
                latest_before = self._latest(metric)
                persisted = int(self._get_state(f"cg_cursor:{metric}", start_ts) or start_ts)
                cursor = max(start_ts, persisted, (latest_before + interval if latest_before is not None else start_ts))
                for _ in range(max(1, pages)):
                    if cursor >= now:
                        break
                    window_end = min(now, cursor + 999 * interval)
                    try:
                        n = await fn(cursor, window_end); added += int(n)
                    except Exception as exc:
                        result["errors"].append(f"{metric}: {exc}")
                        break
                    latest = self._latest(metric)
                    if latest is None or latest < cursor:
                        # Provider has no coverage for this old window. Persist the
                        # next cursor so the following learning tick does not restart.
                        cursor = window_end + interval
                    else:
                        cursor = max(window_end + interval, latest + interval)
                    self._set_state(f"cg_cursor:{metric}", cursor)
                result["attempted"].append({"metric": metric, "added": added, "from": self._earliest(metric), "to": self._latest(metric), "cursor": cursor})

        # Exchange-native sources provide an independent fallback/cross-check. Their
        # retention can be shorter, so they are best-effort and never block learning.
        for key, metric, fn in (
            ("oi:bybit", "oi_coin", lambda end: self._backfill_native_oi(hub, end)),
            ("funding:bybit", "funding", lambda end: self._backfill_native_funding(hub, "bybit", end)),
            ("funding:binance", "funding", lambda end: self._backfill_native_funding(hub, "binance", end)),
        ):
            earliest = self._earliest(metric)
            end = (earliest - 1) if earliest else now
            added = 0
            try:
                for _ in range(max(1, min(pages, 3))):
                    n = await fn(end); added += int(n)
                    new_earliest = self._earliest(metric)
                    if new_earliest is None or new_earliest >= end or new_earliest <= start_ts:
                        break
                    end = new_earliest - 1
            except Exception as exc:
                result["errors"].append(f"{key}: {exc}")
            result["attempted"].append({"metric": key, "added": added, "from": self._earliest(metric), "to": self._latest(metric)})
        self._set_state("last_tick", result)
        return result

    def _latest_values(self, metric: str, ts: int, max_age: int, limit: int = 8) -> list[sqlite3.Row]:
        con = self._con(); rows = con.execute(
            "SELECT source,ts,value,quality FROM derivative_history WHERE metric=? AND ts<=? AND ts>=? ORDER BY ts DESC LIMIT ?",
            (metric, ts, ts - max_age, limit),
        ).fetchall(); con.close(); return rows

    def extras_at(self, ts: int) -> dict[str, float]:
        """Return point-in-time features only from records timestamped <= ts."""
        oi_rows = self._latest_values("oi_usd", ts, 16 * 3600, 4) or self._latest_values("oi_coin", ts, 16 * 3600, 4)
        funding_rows = self._latest_values("funding", ts, 16 * 3600, 12)
        long_rows = self._latest_values("liq_long_usd", ts, 8 * 3600, 2)
        short_rows = self._latest_values("liq_short_usd", ts, 8 * 3600, 2)
        book_rows = self._latest_values("book_imbalance", ts, 8 * 3600, 2)
        oi_funding_rows = self._latest_values("oi_weighted_funding", ts, 16 * 3600, 2)
        taker_rows = self._latest_values("taker_imbalance", ts, 8 * 3600, 2)
        crowd_rows = self._latest_values("crowd_skew", ts, 8 * 3600, 2)
        top_position_rows = self._latest_values("top_position_skew", ts, 8 * 3600, 2)

        oi_change = 0.0
        if len(oi_rows) >= 2 and _f(oi_rows[-1]["value"]):
            newest, oldest = _f(oi_rows[0]["value"]), _f(oi_rows[-1]["value"])
            oi_change = newest / oldest - 1 if oldest else 0.0
        funding = statistics.median([_f(x["value"]) for x in funding_rows]) if funding_rows else 0.0
        long_liq = _f(long_rows[0]["value"]) if long_rows else 0.0
        short_liq = _f(short_rows[0]["value"]) if short_rows else 0.0
        total_liq = long_liq + short_liq
        liq_imbalance = (short_liq - long_liq) / max(total_liq, 1e-9) if total_liq else 0.0
        liq_intensity = math.log1p(total_liq) / 25.0 if total_liq else 0.0
        book = _f(book_rows[0]["value"]) if book_rows else 0.0

        extra_groups = (oi_funding_rows, taker_rows, crowd_rows, top_position_rows)
        available_groups = sum((bool(oi_rows), bool(funding_rows), bool(long_rows and short_rows), bool(book_rows), *(bool(x) for x in extra_groups)))
        coverage = available_groups / 8.0
        quality_values = [float(x["quality"]) for group in (oi_rows[:1], funding_rows[:1], long_rows[:1], short_rows[:1], book_rows[:1], *(x[:1] for x in extra_groups)) for x in group]
        quality = statistics.mean(quality_values) if quality_values else 0.0
        return {
            "oi_change": oi_change,
            "funding": funding,
            "book_imbalance": book,
            "liquidation_imbalance": liq_imbalance,
            "liquidation_intensity": liq_intensity,
            "oi_weighted_funding": _f(oi_funding_rows[0]["value"]) if oi_funding_rows else 0.0,
            "taker_imbalance": _f(taker_rows[0]["value"]) if taker_rows else 0.0,
            "crowd_skew": _f(crowd_rows[0]["value"]) if crowd_rows else 0.0,
            "top_position_skew": _f(top_position_rows[0]["value"]) if top_position_rows else 0.0,
            "oi_available": float(bool(oi_rows)),
            "funding_available": float(bool(funding_rows)),
            "liquidation_available": float(bool(long_rows and short_rows)),
            "book_available": float(bool(book_rows)),
            "oi_weighted_funding_available": float(bool(oi_funding_rows)),
            "taker_available": float(bool(taker_rows)),
            "crowd_available": float(bool(crowd_rows)),
            "top_position_available": float(bool(top_position_rows)),
            "derivative_coverage": coverage,
            "derivative_quality": quality / 100.0,
        }

    def status(self) -> dict[str, Any]:
        con = self._con()
        rows = con.execute("SELECT metric,COUNT(*) n,MIN(ts) mn,MAX(ts) mx,AVG(quality) q FROM derivative_history GROUP BY metric ORDER BY metric").fetchall()
        con.close()
        return {
            "coinglass_enabled": bool(self.coinglass_key),
            "metrics": {r["metric"]: {"count": int(r["n"]), "from": r["mn"], "to": r["mx"], "quality": round(_f(r["q"]), 2)} for r in rows},
        }
