"""ETH_USDT Gate 公開資料掃描器：只產生研究訊號，不下單。"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import math
import os
import re
import sqlite3
import statistics
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import httpx
import uvicorn
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse

PAIR = "ETH_USDT"
API = "https://api.gateio.ws/api/v4"
DB_PATH = os.getenv("DATABASE_PATH", "eth_scanner.db")
PORT = int(os.getenv("PORT", "8080"))
SCAN_SECONDS = max(30, int(os.getenv("SCAN_SECONDS", "60")))
MIN_SCORE = max(50, min(95, int(os.getenv("SIGNAL_MIN_SCORE", "72"))))
DISCORD = os.getenv("DISCORD_WEBHOOK_URL", "")
DISCORD_BOT_TOKEN = os.getenv("DISCORD_BOT_TOKEN", "")
DISCORD_CHANNEL_ID = os.getenv("DISCORD_CHANNEL_ID", "")
DISCORD_ALLOWED_USER_ID = os.getenv("DISCORD_ALLOWED_USER_ID", "")
DISCORD_API = "https://discord.com/api/v10"
TAIPEI = ZoneInfo(os.getenv("TZ", "Asia/Taipei"))
NEW_YORK = ZoneInfo("America/New_York")
LOG = logging.getLogger("eth-scanner")
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(message)s")

state: dict[str, Any] = {
    "status": "啟動中", "updated_at": None, "error": None, "ws": "REST 定時掃描",
    "analysis": {}, "data_quality": 0, "scan_count": 0,
    "position": None, "discord_commands": "未設定 Bot",
}


def db() -> sqlite3.Connection:
    global DB_PATH
    requested = Path(DB_PATH)
    try:
        requested.parent.mkdir(parents=True, exist_ok=True)
        con = sqlite3.connect(requested, timeout=15)
    except (OSError, sqlite3.OperationalError) as exc:
        fallback = Path("/tmp/eth_scanner.db")
        if requested == fallback:
            raise
        LOG.warning("資料庫路徑 %s 無法寫入（%s），暫時改用 %s；重啟後歷史可能遺失",
                    requested, exc, fallback)
        DB_PATH = str(fallback)
        con = sqlite3.connect(fallback, timeout=15)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("""CREATE TABLE IF NOT EXISTS snapshots(
        id INTEGER PRIMARY KEY, ts INTEGER NOT NULL, price REAL NOT NULL,
        score INTEGER NOT NULL, direction TEXT NOT NULL, payload TEXT NOT NULL)""")
    con.execute("CREATE INDEX IF NOT EXISTS ix_snapshots_ts ON snapshots(ts)")
    con.execute("""CREATE TABLE IF NOT EXISTS alerts(
        setup_id TEXT NOT NULL, level INTEGER NOT NULL, ts INTEGER NOT NULL,
        message TEXT NOT NULL, PRIMARY KEY(setup_id, level))""")
    con.execute("""CREATE TABLE IF NOT EXISTS system_state(
        key TEXT PRIMARY KEY, value TEXT NOT NULL, updated_at INTEGER NOT NULL)""")
    con.execute("""CREATE TABLE IF NOT EXISTS positions(
        position_id TEXT PRIMARY KEY, side TEXT NOT NULL, entry_ts INTEGER NOT NULL,
        entry_price REAL NOT NULL, exit_ts INTEGER, exit_price REAL,
        status TEXT NOT NULL, payload TEXT NOT NULL)""")
    con.execute("CREATE INDEX IF NOT EXISTS ix_positions_entry_ts ON positions(entry_ts)")
    con.commit()
    return con


def get_system_state(key: str, default: Any = None) -> Any:
    con = db()
    row = con.execute("SELECT value FROM system_state WHERE key=?", (key,)).fetchone()
    con.close()
    if not row:
        return default
    try:
        return json.loads(row["value"])
    except json.JSONDecodeError:
        return row["value"]


def set_system_state(key: str, value: Any) -> None:
    con = db()
    con.execute("""INSERT INTO system_state(key,value,updated_at) VALUES(?,?,?)
                   ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_at=excluded.updated_at""",
                (key, json.dumps(value, ensure_ascii=False), int(time.time())))
    con.commit()
    con.close()


def active_position() -> dict | None:
    value = get_system_state("active_position")
    return value if isinstance(value, dict) and value.get("status") == "OPEN" else None


def save_position(position: dict) -> None:
    set_system_state("active_position", position if position.get("status") == "OPEN" else None)
    con = db()
    con.execute("""INSERT INTO positions(position_id,side,entry_ts,entry_price,exit_ts,exit_price,status,payload)
                   VALUES(?,?,?,?,?,?,?,?)
                   ON CONFLICT(position_id) DO UPDATE SET exit_ts=excluded.exit_ts,
                   exit_price=excluded.exit_price,status=excluded.status,payload=excluded.payload""",
                (position["position_id"], position["side"], position["entry_ts"],
                 position["entry_price"], position.get("exit_ts"), position.get("exit_price"),
                 position["status"], json.dumps(position, ensure_ascii=False)))
    con.commit()
    con.close()


def f(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except (TypeError, ValueError):
        return default


def candles(raw: list, futures: bool = True) -> list[dict[str, float]]:
    """Gate 回傳為新到舊；只保留已收線。Spot: t,v,c,h,l,o；Futures: t,v,c,h,l,o,sum。"""
    out = []
    for row in raw:
        if isinstance(row, dict):
            out.append({"t": int(f(row.get("t"))), "v": f(row.get("v")), "c": f(row.get("c")),
                        "h": f(row.get("h")), "l": f(row.get("l")), "o": f(row.get("o"))})
        elif len(row) >= 6:
            out.append({"t": int(f(row[0])), "v": f(row[1]), "c": f(row[2]),
                        "h": f(row[3]), "l": f(row[4]), "o": f(row[5])})
    out.sort(key=lambda x: x["t"])
    # REST 最新一根可能仍在形成；無論 w 欄位是否提供，統一排除，防止偷看未收線。
    return out[:-1]


def sma(values: list[float], n: int) -> float:
    return sum(values[-n:]) / min(n, len(values)) if values else 0


def ema(values: list[float], n: int) -> float:
    if not values:
        return 0
    a, value = 2 / (n + 1), values[0]
    for x in values[1:]:
        value = a * x + (1 - a) * value
    return value


def atr(cs: list[dict], n: int = 14) -> float:
    if len(cs) < 2:
        return 0
    tr = [max(x["h"] - x["l"], abs(x["h"] - cs[i-1]["c"]), abs(x["l"] - cs[i-1]["c"]))
          for i, x in enumerate(cs[1:], 1)]
    return sma(tr, n)


def adx(cs: list[dict], n: int = 14) -> float:
    if len(cs) < n + 2:
        return 0
    trs, plus, minus = [], [], []
    for i in range(1, len(cs)):
        up, down = cs[i]["h"] - cs[i-1]["h"], cs[i-1]["l"] - cs[i]["l"]
        trs.append(max(cs[i]["h"]-cs[i]["l"], abs(cs[i]["h"]-cs[i-1]["c"]), abs(cs[i]["l"]-cs[i-1]["c"])))
        plus.append(up if up > down and up > 0 else 0)
        minus.append(down if down > up and down > 0 else 0)
    trn = sum(trs[-n:]) or 1
    p, m = 100 * sum(plus[-n:]) / trn, 100 * sum(minus[-n:]) / trn
    return 100 * abs(p - m) / (p + m or 1)


def pivots(cs: list[dict], span: int = 3) -> tuple[list[dict], list[dict]]:
    highs, lows = [], []
    for i in range(span, len(cs) - span):
        window = cs[i-span:i+span+1]
        if cs[i]["h"] == max(x["h"] for x in window):
            highs.append({"i": i, "t": cs[i]["t"], "p": cs[i]["h"]})
        if cs[i]["l"] == min(x["l"] for x in window):
            lows.append({"i": i, "t": cs[i]["t"], "p": cs[i]["l"]})
    return highs, lows


def structure(cs: list[dict]) -> dict:
    highs, lows = pivots(cs)
    a = atr(cs)
    close = cs[-1]["c"]
    bull_bos = bool(highs and close > highs[-1]["p"] + .05 * a)
    bear_bos = bool(lows and close < lows[-1]["p"] - .05 * a)
    hhhl = len(highs) > 1 and len(lows) > 1 and highs[-1]["p"] > highs[-2]["p"] and lows[-1]["p"] > lows[-2]["p"]
    lllh = len(highs) > 1 and len(lows) > 1 and highs[-1]["p"] < highs[-2]["p"] and lows[-1]["p"] < lows[-2]["p"]
    trend = "BULLISH" if hhhl or bull_bos else "BEARISH" if lllh or bear_bos else "RANGE"
    if adx(cs) >= 28 and trend != "RANGE":
        trend = "STRONG_" + trend
    return {"trend": trend, "bull_bos": bull_bos, "bear_bos": bear_bos,
            "highs": highs[-5:], "lows": lows[-5:], "atr": a, "adx": adx(cs)}


def fvg(cs: list[dict]) -> list[dict]:
    a, result = atr(cs), []
    med = statistics.median([x["v"] for x in cs[-22:-2]] or [1])
    for i in range(max(2, len(cs)-80), len(cs)):
        x, mid, z = cs[i-2], cs[i-1], cs[i]
        body = abs(mid["c"]-mid["o"]) / max(mid["h"]-mid["l"], 1e-9)
        if z["l"] > x["h"] and z["l"]-x["h"] >= .08*a and body >= .5:
            result.append({"side": "bull", "low": x["h"], "high": z["l"], "mid": (x["h"]+z["l"])/2, "t": z["t"], "volume_ok": mid["v"] >= med})
        elif z["h"] < x["l"] and x["l"]-z["h"] >= .08*a and body >= .5:
            result.append({"side": "bear", "low": z["h"], "high": x["l"], "mid": (z["h"]+x["l"])/2, "t": z["t"], "volume_ok": mid["v"] >= med})
    return result[-8:]


def bos_impulse(cs: list[dict], direction: str, span: int = 3) -> dict:
    """找出最近一次真正造成 BOS、且兩端都已確認的 1H 推進波。"""
    highs, lows = pivots(cs, span)
    target_pivots = highs if direction == "LONG" else lows
    origin_pivots = lows if direction == "LONG" else highs
    broken: set[int] = set()
    events: list[dict] = []
    for i in range(22, len(cs)):
        eligible = [p for p in target_pivots if p["i"] + span < i and p["i"] < i and p["t"] not in broken]
        if not eligible:
            continue
        target = eligible[-1]
        a = atr(cs[:i+1])
        candle = cs[i]
        candle_range = max(candle["h"]-candle["l"], 1e-9)
        body_ratio = abs(candle["c"]-candle["o"]) / candle_range
        median_volume = statistics.median([x["v"] for x in cs[max(0, i-20):i]] or [1])
        close_break = (candle["c"] > target["p"] + .10*a if direction == "LONG"
                       else candle["c"] < target["p"] - .10*a)
        candle_color = candle["c"] > candle["o"] if direction == "LONG" else candle["c"] < candle["o"]
        if not (close_break and candle_color and body_ratio >= .55 and candle["v"] >= 1.20*median_volume):
            continue
        origins = [p for p in origin_pivots if p["i"] + span < i and p["i"] < i]
        if not origins:
            continue
        origin = origins[-1]
        events.append({"i": i, "t": candle["t"], "target": target, "origin": origin,
                       "close": candle["c"], "body_ratio": body_ratio,
                       "volume_ratio": candle["v"]/max(median_volume, 1e-9)})
        broken.add(target["t"])
    if not events:
        return {"valid": False, "reason": "找不到符合收盤、實體與量能要求的有效 1H BOS"}
    event = events[-1]
    endpoints = [p for p in target_pivots if p["i"] >= event["i"] and p["i"] + span < len(cs)]
    if not endpoints:
        return {"valid": False, "reason": "BOS 已成立，等待推進終點右側 3 根 1H K 確認",
                "bos_time": event["t"], "bos_level": event["target"]["p"]}
    endpoint = (max(endpoints, key=lambda p: p["p"]) if direction == "LONG"
                else min(endpoints, key=lambda p: p["p"]))
    low = event["origin"]["p"] if direction == "LONG" else endpoint["p"]
    high = endpoint["p"] if direction == "LONG" else event["origin"]["p"]
    origin_broken = cs[-1]["c"] < low if direction == "LONG" else cs[-1]["c"] > high
    if high <= low:
        return {"valid": False, "reason": "BOS 推進波高低點順序無效"}
    if origin_broken:
        return {"valid": False, "reason": "最近 BOS 推進波起點已被 1H 收盤破壞",
                "bos_time": event["t"], "bos_level": event["target"]["p"]}
    return {
        "valid": True, "reason": "有效 BOS 推進波兩端均已確認",
        "low": low, "high": high, "origin_time": event["origin"]["t"],
        "endpoint_time": endpoint["t"], "bos_time": event["t"],
        "bos_level": event["target"]["p"], "bos_close": event["close"],
        "bos_body_ratio": round(event["body_ratio"], 3),
        "bos_volume_ratio": round(event["volume_ratio"], 2),
    }


def period_liquidity(cs: list[dict]) -> list[dict]:
    """從已收 1H K 建立前一日／前一週高低點。"""
    days: dict[str, list[dict]] = {}
    weeks: dict[str, list[dict]] = {}
    for candle in cs:
        dt = datetime.fromtimestamp(candle["t"], NEW_YORK)
        day_key = dt.strftime("%Y-%m-%d")
        iso = dt.isocalendar()
        week_key = f"{iso.year}-{iso.week:02d}"
        days.setdefault(day_key, []).append(candle)
        weeks.setdefault(week_key, []).append(candle)
    result = []
    day_keys, week_keys = sorted(days), sorted(weeks)
    if len(day_keys) >= 2:
        prior = days[day_keys[-2]]
        end_time = max(x["t"] for x in prior)
        result += [{"price": max(x["h"] for x in prior), "type": "PDH（紐約日）",
                    "strength": 4, "time": end_time},
                   {"price": min(x["l"] for x in prior), "type": "PDL（紐約日）",
                    "strength": 4, "time": end_time}]
    if len(week_keys) >= 2:
        prior = weeks[week_keys[-2]]
        end_time = max(x["t"] for x in prior)
        result += [{"price": max(x["h"] for x in prior), "type": "PWH（紐約週）",
                    "strength": 5, "time": end_time},
                   {"price": min(x["l"] for x in prior), "type": "PWL（紐約週）",
                    "strength": 5, "time": end_time}]
    return result


def ict_targets(direction: str, entry_mid: float, stop: float, live_price: float,
                h4: list[dict], h1: list[dict], m15: list[dict]) -> dict:
    """只以真實流動性定義止盈，不使用固定百分比或虛構延伸價。"""
    risk = abs(entry_mid-stop)
    if not risk:
        return {"targets": [], "rr": [], "details": [], "weighted_rr": 0,
                "quality_ok": False, "reason": "進場與止損風險距離無效"}
    h4h, h4l = pivots(h4)
    h1h, h1l = pivots(h1)
    m15h, m15l = pivots(m15)
    chosen_pivots = ((m15h, h1h, h4h) if direction == "LONG" else (m15l, h1l, h4l))
    labels = ("15M Swing", "1H Swing", "4H External Liquidity")
    strengths = (1, 2, 4)
    candidates: list[dict] = []
    for points, label, strength in zip(chosen_pivots, labels, strengths):
        candidates.extend({"price": x["p"], "type": label, "time": x["t"], "strength": strength}
                          for x in points[-12:])
    candidates.extend(period_liquidity(h1))
    one_hour_points = h1h if direction == "LONG" else h1l
    tolerance = min(.10*atr(h1), entry_mid*.0015)
    for i in range(1, len(one_hour_points)):
        a, b = one_hour_points[i-1], one_hour_points[i]
        if abs(a["p"]-b["p"]) <= tolerance:
            candidates.append({"price": (a["p"]+b["p"])/2,
                               "type": "Equal Highs" if direction == "LONG" else "Equal Lows",
                               "strength": 3, "time": b["t"]})
    directional = []
    for item in candidates:
        price = f(item["price"])
        correct_side = (price > max(entry_mid, live_price) if direction == "LONG"
                        else price < min(entry_mid, live_price))
        if not correct_side:
            continue
        created_at = int(item.get("time", 0))
        later = [x for x in m15 if x["t"] > created_at] if created_at else []
        swept = (any(x["h"] > price + .01 for x in later) if direction == "LONG"
                 else any(x["l"] < price - .01 for x in later))
        if swept:
            continue
        rr = abs(price-entry_mid)/risk
        if rr >= 1.0:
            directional.append({**item, "price": price, "rr": rr})
    directional.sort(key=lambda x: x["rr"])
    clustered: list[dict] = []
    cluster_tolerance = max(.10*atr(h1), entry_mid*.0005)
    for item in directional:
        same = next((old for old in clustered
                     if abs(item["price"]-old["price"]) <= cluster_tolerance), None)
        if same:
            if item.get("strength", 0) > same.get("strength", 0):
                clustered[clustered.index(same)] = item
        else:
            clustered.append(item)
    deduped = sorted(clustered, key=lambda x: x["rr"])
    selected = []
    rules = (
        (1.0, 1),
        (1.5, 2),
        (2.0, 2),
        (2.5, 3),
    )
    last_rr = 0.0
    for threshold, min_strength in rules:
        required_rr = max(threshold, last_rr + .35 if selected else threshold)
        match = next((x for x in deduped if x["rr"] >= required_rr and
                      x.get("strength", 0) >= min_strength and x not in selected), None)
        if match:
            selected.append(match)
            last_rr = match["rr"]
    weights = [25, 30, 30, 15][:len(selected)]
    weighted_rr = (sum(x["rr"]*w for x, w in zip(selected, weights))/sum(weights)
                   if weights else 0)
    quality_ok = bool(selected and selected[0]["rr"] >= 1.0 and
                      selected[-1]["rr"] >= 2.5 and weighted_rr >= 2.0)
    reason = ("TP1≥1R、最終目標≥2.5R、加權平均≥2R" if quality_ok else
              f"ICT 目標不足：最終 {selected[-1]['rr']:.2f}R、加權 {weighted_rr:.2f}R"
              if selected else "前方找不到至少 1R 的有效流動性")
    return {
        "targets": [round(x["price"], 2) for x in selected],
        "rr": [round(x["rr"], 2) for x in selected],
        "details": [{"price": round(x["price"], 2), "rr": round(x["rr"], 2),
                     "type": x["type"], "allocation": weights[i]}
                    for i, x in enumerate(selected)],
        "weighted_rr": round(weighted_rr, 2), "quality_ok": quality_ok, "reason": reason,
    }


def setup(h4: list[dict], h1: list[dict], m15: list[dict], m5: list[dict],
          spot: list[dict], ticker: dict, spot_ticker: dict, book: dict) -> dict:
    """結構只讀已收 K；區域距離與通知價格使用當下 Gate ticker。"""
    s4, s1, s15, s5 = structure(h4), structure(h1), structure(m15), structure(m5)
    raw_trend_4h = s4["trend"]
    close_4h = h4[-1]["c"]
    ema20_4h, ema50_4h = ema([x["c"] for x in h4], 20), ema([x["c"] for x in h4], 50)
    ema20_1h, ema50_1h = ema([x["c"] for x in h1], 20), ema([x["c"] for x in h1], 50)
    close_1h = h1[-1]["c"]
    bull_votes = (
        (2 if "BULLISH" in s1["trend"] else 0) +
        (2 if s1["bull_bos"] else 0) +
        (1 if close_1h > ema20_1h else 0) +
        (1 if ema20_1h > ema50_1h else 0)
    )
    bear_votes = (
        (2 if "BEARISH" in s1["trend"] else 0) +
        (2 if s1["bear_bos"] else 0) +
        (1 if close_1h < ema20_1h else 0) +
        (1 if ema20_1h < ema50_1h else 0)
    )
    if bull_votes >= bear_votes + 2:
        effective_trend = "STRONG_BULLISH" if bull_votes >= 5 and s1["adx"] >= 25 else "BULLISH"
    elif bear_votes >= bull_votes + 2:
        effective_trend = "STRONG_BEARISH" if bear_votes >= 5 and s1["adx"] >= 25 else "BEARISH"
    else:
        effective_trend = "TRANSITION"
    direction = ("LONG" if "BULLISH" in effective_trend else
                 "SHORT" if "BEARISH" in effective_trend else "觀察")
    four_h_aligned = ((direction == "LONG" and "BULLISH" in raw_trend_4h) or
                      (direction == "SHORT" and "BEARISH" in raw_trend_4h))
    four_h_conflict = ((direction == "LONG" and "BEARISH" in raw_trend_4h) or
                       (direction == "SHORT" and "BULLISH" in raw_trend_4h))
    context_4h = "同向加分" if four_h_aligned else "反向背景，降低信心但不封鎖" if four_h_conflict else "震盪背景"
    trend_reason = (f"1H 已收 K 投票：多 {bull_votes}／空 {bear_votes}；"
                    f"1H {s1['trend']}、ADX {s1['adx']:.1f}；4H {raw_trend_4h}（{context_4h}）")
    hs, ls = pivots(h1)
    live_price, closed_1h, a1 = f(ticker.get("last"), h1[-1]["c"]), h1[-1]["c"], atr(h1)
    impulse_meta = bos_impulse(h1, direction) if direction in ("LONG", "SHORT") else {
        "valid": False, "reason": "1H 方向尚未明確，未建立 Fibonacci"
    }
    fib_valid = bool(impulse_meta["valid"])
    low = impulse_meta.get("low")
    high = impulse_meta.get("high")
    if fib_valid:
        rng = high-low
        fib = ({k: high-rng*v for k, v in {".618":.618, ".705":.705, ".786":.786}.items()} if direction == "LONG"
               else {k: low+rng*v for k, v in {".618":.618, ".705":.705, ".786":.786}.items()})
        ote_low, ote_high = min(fib[".618"], fib[".786"]), max(fib[".618"], fib[".786"])
    else:
        fib = {".618": None, ".705": None, ".786": None}
        ote_low = ote_high = None
    in_ote = bool(fib_valid and ote_low <= live_price <= ote_high)
    if direction == "LONG" and fib_valid:
        near_ote = ote_high < live_price <= ote_high + .35*a1
        zone_status = "位於 OTE" if in_ote else "等待回撤" if live_price > ote_high else "超深回撤"
        origin_broken = closed_1h < low
    elif direction == "SHORT" and fib_valid:
        near_ote = ote_low - .35*a1 <= live_price < ote_low
        zone_status = "位於 OTE" if in_ote else "等待反彈" if live_price < ote_low else "超深反彈"
        origin_broken = closed_1h > high
    else:
        near_ote, zone_status, origin_broken = False, f"等待有效 BOS 推進波：{impulse_meta['reason']}", False
    gaps = fvg(h1)
    side = "bull" if direction == "LONG" else "bear"
    active_gap = next((g for g in reversed(gaps) if g["side"] == side and
                       g["low"]-.1*a1 <= live_price <= g["high"]+.1*a1), None)
    vol_med = statistics.median([x["v"] for x in m15[-21:-1]] or [1])
    rvol = m15[-1]["v"] / max(vol_med, 1e-9)
    spot_rvol = spot[-1]["v"] / max(statistics.median([x["v"] for x in spot[-21:-1]] or [1]), 1e-9)
    mss = ((direction == "LONG" and (s15["bull_bos"] or "BULLISH" in s15["trend"])) or
           (direction == "SHORT" and (s15["bear_bos"] or "BEARISH" in s15["trend"])))
    trigger_5m = ((direction == "LONG" and (s5["bull_bos"] or "BULLISH" in s5["trend"])) or
                  (direction == "SHORT" and (s5["bear_bos"] or "BEARISH" in s5["trend"])))
    protected = (s15["lows"][-1]["p"] if direction == "LONG" and s15["lows"] else
                 s15["highs"][-1]["p"] if direction == "SHORT" and s15["highs"] else None)
    bids, asks = book.get("bids", []), book.get("asks", [])
    bid_qty = sum(abs(f(x.get("s"))) for x in bids[:20])
    ask_qty = sum(abs(f(x.get("s"))) for x in asks[:20])
    imbalance = (bid_qty-ask_qty) / max(bid_qty+ask_qty, 1)
    best_bid = f(ticker.get("highest_bid"))
    best_ask = f(ticker.get("lowest_ask"))
    spread = best_ask-best_bid if best_ask and best_bid else 0
    volume_ok = rvol >= .85 and spot_rvol >= .7
    score = 0
    score += 18 if direction != "觀察" else 5
    score += 8 if four_h_aligned else 2 if four_h_conflict else 4
    score += 13 if in_ote else 8 if near_ote else 0
    score += 12 if active_gap else 4 if gaps else 0
    score += 12 if mss else 0
    score += 5 if trigger_5m else 0
    score += 10 if volume_ok else 4
    change_24h = f(ticker.get("change_percentage"))
    score += 5 if (direction == "LONG" and change_24h >= 0) or (direction == "SHORT" and change_24h <= 0) else 2
    funding = f(ticker.get("funding_rate"))
    funding_ok = not (direction == "LONG" and funding > .0008) and not (direction == "SHORT" and funding < -.0008)
    score += 7 if funding_ok else 1
    book_ok = (direction == "LONG" and imbalance > -.15) or (direction == "SHORT" and imbalance < .15)
    score += 5 if book_ok else 1
    score = min(100, score)
    buffer = max(.12*atr(m15), live_price*.0005, spread*3)
    if fib_valid:
        entry = [active_gap["low"], active_gap["high"]] if active_gap else [ote_low, ote_high]
    else:
        entry = [None, None]
    if direction == "LONG" and fib_valid:
        local = [x["p"] for x in s15["lows"] if x["p"] < min(entry)]
        stop = min(([low] if low < min(entry) else []) + local + [min(entry)-buffer]) - buffer
    elif direction == "SHORT" and fib_valid:
        local = [x["p"] for x in s15["highs"] if x["p"] > max(entry)]
        stop = max(([high] if high > max(entry) else []) + local + [max(entry)+buffer]) + buffer
    else:
        stop = 0
    entry_mid = sum(entry)/2 if fib_valid else 0
    risk = abs(entry_mid-stop) if stop else 0
    target_plan = (ict_targets(direction, entry_mid, stop, live_price, h4, h1, m15)
                   if fib_valid and direction in ("LONG", "SHORT") else
                   {"targets": [], "rr": [], "details": [], "weighted_rr": 0,
                    "quality_ok": False, "reason": "尚未建立有效 Fibonacci 與風險距離"})
    targets, rr = target_plan["targets"], target_plan["rr"]
    invalid = origin_broken
    invalid_reason = ("1H 收盤跌破推進波起點" if direction == "LONG" and origin_broken else
                      "1H 收盤突破推進波起點" if direction == "SHORT" and origin_broken else "")
    stage = 4 if score >= MIN_SCORE and in_ote and mss and trigger_5m else 3 if in_ote else 2 if near_ote else 1
    if direction == "觀察" or not fib_valid or invalid or zone_status.startswith("超深"):
        stage = 1
    missing = []
    if direction == "觀察": missing.append("1H 方向尚未明確")
    if not fib_valid: missing.append(f"未建立正式 Fibonacci：{impulse_meta['reason']}")
    elif zone_status.startswith("超深"): missing.append(f"OTE {zone_status}，等待新結構")
    elif not in_ote: missing.append("尚未進入 OTE")
    if not active_gap: missing.append("1H FVG 未共振")
    if not mss: missing.append("15M MSS 尚未收 K 確認")
    if not trigger_5m: missing.append("5M 觸發尚未收 K 確認")
    if not volume_ok: missing.append("現貨／合約量能尚未同步")
    if fib_valid and not target_plan["quality_ok"]:
        missing.append(target_plan["reason"])
        stage = min(stage, 3)
    if invalid_reason: missing.insert(0, invalid_reason)
    formal = stage == 4 and not invalid
    if invalid:
        trade_status = "目前不可下單｜結構已失效，等待新 Setup"
    elif not fib_valid:
        trade_status = "目前不可下單｜尚未確認有效 BOS 推進波與正式 Fibonacci"
    elif not target_plan["quality_ok"]:
        trade_status = f"目前不可下單｜{target_plan['reason']}"
    elif zone_status.startswith("超深"):
        trade_status = f"目前不可下單｜OTE {zone_status}，等待新結構"
    elif stage == 1:
        trade_status = "目前不可下單｜僅建立方向與結構觀察"
    elif stage == 2:
        trade_status = "目前不可下單｜接近 OTE，等待價格進入監控區"
    elif stage == 3:
        trade_status = "目前不可下單｜已進監控區，等待 15M／5M 收 K 確認"
    else:
        trade_status = "可以評估下單｜L4 條件已收 K 確認，仍須自行審核風險"
    trade_label = "可以評估下單" if formal else "目前不可下單"
    latest_high = s4["highs"][-1] if s4["highs"] else {"t": 0, "p": max(x["h"] for x in h4[-40:])}
    latest_low = s4["lows"][-1] if s4["lows"] else {"t": 0, "p": min(x["l"] for x in h4[-40:])}
    latest_1h_high = s1["highs"][-1] if s1["highs"] else {"t": 0, "p": max(x["h"] for x in h1[-40:])}
    latest_1h_low = s1["lows"][-1] if s1["lows"] else {"t": 0, "p": min(x["l"] for x in h1[-40:])}
    structure_id = hashlib.sha256(
        f"{direction}:{latest_1h_high['t']}:{latest_1h_high['p']}:{latest_1h_low['t']}:{latest_1h_low['p']}".encode()
    ).hexdigest()[:16]
    setup_id = hashlib.sha256(
        f"{direction}:{round(low,2) if low is not None else 'NO_FIB'}:"
        f"{round(high,2) if high is not None else 'NO_FIB'}:{structure_id}".encode()
    ).hexdigest()[:16]
    return {
        "pair": PAIR, "price": round(live_price, 2), "futures_price": round(live_price, 2),
        "spot_price": round(f(spot_ticker.get("last")), 2), "mark_price": round(f(ticker.get("mark_price")), 2),
        "index_price": round(f(ticker.get("index_price")), 2), "closed_1h": round(closed_1h, 2),
        "closed_15m": round(m15[-1]["c"], 2),
        "direction": direction, "score": score, "threshold": MIN_SCORE, "stage": stage,
        "setup_id": setup_id, "structure_id": structure_id, "market_bias": effective_trend,
        "trend_4h": raw_trend_4h, "raw_trend_4h": raw_trend_4h,
        "trend_1h": s1["trend"], "trend_reason": trend_reason,
        "direction_votes": {"bull": bull_votes, "bear": bear_votes},
        "four_h_context": context_4h,
        "adx_4h": round(s4["adx"], 2), "bos_4h": "向上 BOS" if s4["bull_bos"] else "向下 BOS" if s4["bear_bos"] else "無新 BOS",
        "adx_1h": round(s1["adx"], 2),
        "ema_4h": {"ema20": round(ema20_4h, 2), "ema50": round(ema50_4h, 2)},
        "ema_1h": {"ema20": round(ema20_1h, 2), "ema50": round(ema50_1h, 2)},
        "last_closed": {"4h": h4[-1]["t"], "1h": h1[-1]["t"], "15m": m15[-1]["t"], "5m": m5[-1]["t"]},
        "swing_high_4h": round(latest_high["p"], 2), "swing_low_4h": round(latest_low["p"], 2),
        "fib_valid": fib_valid, "fib_reason": impulse_meta["reason"],
        "bos_impulse": impulse_meta,
        "impulse": [round(low, 2), round(high, 2)] if fib_valid else [],
        "fib": {k: round(v, 2) if v is not None else None for k, v in fib.items()},
        "ote": [round(ote_low, 2), round(ote_high, 2)] if fib_valid else [],
        "zone_status": zone_status, "in_ote": in_ote, "fvg": active_gap, "mss_15m": mss,
        "trigger_5m": trigger_5m, "protected_15m": round(protected, 2) if protected else None,
        "rvol_15m": round(rvol, 2), "spot_rvol": round(spot_rvol, 2),
        "funding_rate": funding, "change_24h": change_24h,
        "high_24h": f(ticker.get("high_24h")), "low_24h": f(ticker.get("low_24h")),
        "oi_contracts": f(ticker.get("total_size")), "oi_eth_estimate": round(f(ticker.get("total_size"))*f(ticker.get("quanto_multiplier")), 2),
        "orderbook_imbalance": round(imbalance, 3), "spread": round(spread, 4),
        "entry": [round(x, 2) if x is not None else None for x in entry], "stop": round(stop, 2),
        "targets": targets, "target_rr": rr, "target_details": target_plan["details"],
        "weighted_rr": target_plan["weighted_rr"], "target_quality_ok": target_plan["quality_ok"],
        "target_reason": target_plan["reason"], "missing": missing,
        "invalid": invalid, "invalid_reason": invalid_reason, "formal": formal,
        "trade_status": trade_status, "trade_label": trade_label,
        "risk_note": "僅供研究，不會自動下單",
    }


async def gate(client: httpx.AsyncClient, path: str, **params: Any) -> Any:
    r = await client.get(API + path, params=params)
    r.raise_for_status()
    return r.json()


def n(x: Any) -> str:
    if x is None:
        return "—"
    return f"{f(x):,.2f}"


def closed_time(ts: int) -> str:
    return datetime.fromtimestamp(ts, TAIPEI).strftime("%m/%d %H:%M")


def discord_fields(result: dict, event_note: str = "") -> tuple[str, list[dict]]:
    details = result.get("target_details", [])
    tps = ("\n".join(f"TP{i+1} `{n(x['price'])}` · `{x['rr']:.2f}R` · "
                     f"{x['type']} · `{x['allocation']}%`" for i, x in enumerate(details))
           if details else "無符合至少 1R 的有效 ICT 流動性目標")
    prices = (f"合約 `{n(result['futures_price'])}`　現貨 `{n(result['spot_price'])}`\n"
              f"標記 `{n(result['mark_price'])}`　指數 `{n(result['index_price'])}`\n"
              f"24H `{result['change_24h']:+.2f}%`｜高 `{n(result['high_24h'])}`｜低 `{n(result['low_24h'])}`")
    structure_text = (f"方向 **{result['direction']}**｜1H 主偏向 **{result.get('market_bias', result['trend_1h'])}**\n"
                      f"4H 輔助 `{result['trend_4h']}`／ADX `{result['adx_4h']}`／{result.get('four_h_context', '—')}｜"
                      f"1H `{result.get('trend_1h', '—')}`／ADX `{result.get('adx_1h', 0)}`\n"
                      f"{result.get('trend_reason', '')}\n{result['bos_4h']}｜4H Swing `{n(result['swing_low_4h'])}–{n(result['swing_high_4h'])}`\n"
                      f"收 K：4H `{closed_time(result['last_closed']['4h'])}`｜1H `{closed_time(result['last_closed']['1h'])}`")
    ote_text = (f"`{n(result['ote'][0])}–{n(result['ote'][1])}`"
                if result.get("fib_valid") and len(result.get("ote", [])) == 2 else "`尚未建立`")
    bos = result.get("bos_impulse", {})
    fib_detail = (f"Fib `{'✅ 正式' if result.get('fib_valid') else '⏳ 無正式價位'}`｜{result.get('fib_reason', '—')}\n"
                  f"BOS Level `{n(bos.get('bos_level'))}`｜BOS Close `{n(bos.get('bos_close'))}`｜"
                  f"Body `{f(bos.get('bos_body_ratio'))*100:.1f}%`｜Volume `{f(bos.get('bos_volume_ratio')):.2f}x`")
    confirm = (f"{fib_detail}\nOTE **{result['zone_status']}** {ote_text}\n"
               f"15M MSS `{'✅' if result['mss_15m'] else '⏳'}`｜5M 觸發 `{'✅' if result['trigger_5m'] else '⏳'}`\n"
               f"合約 RVOL `{result['rvol_15m']}`｜現貨 RVOL `{result['spot_rvol']}`")
    derivatives = (f"資金費率 `{result['funding_rate']*100:.4f}%`｜OI 約 `{n(result['oi_eth_estimate'])} ETH`\n"
                   f"訂單簿失衡 `{result['orderbook_imbalance']:+.3f}`｜Spread `{result['spread']:.4f}`")
    plan = (f"進場 `{n(result['entry'][0])}–{n(result['entry'][1])}`\n"
            f"結構止損 `{n(result['stop']) if result['stop'] else '—'}`\n{tps}\n"
            f"加權報酬 `{result.get('weighted_rr', 0):.2f}R`｜"
            f"{'✅ 合格' if result.get('target_quality_ok') else '❌ 不合格'}")
    missing = "\n".join(f"• {x}" for x in result["missing"]) or "✅ 目前必要條件已齊備"
    description = (f"**階段 L{result['stage']}｜{result.get('trade_label', '目前不可下單')}｜"
                   f"分數 {result['score']}/100**\n{result.get('trade_status', '')}")
    if event_note:
        description += f"\n{event_note}"
    fields = [
        {"name": "💹 掃描當下即時價格", "value": prices, "inline": False},
        {"name": "🧭 已收 K 結構", "value": structure_text, "inline": False},
        {"name": "🧩 OTE／小週期確認", "value": confirm, "inline": False},
        {"name": "📊 衍生品與訂單簿", "value": derivatives, "inline": False},
        {"name": "🎯 參考交易計畫", "value": plan, "inline": False},
        {"name": "⏳ 尚缺條件／風險", "value": missing[:1000], "inline": False},
    ]
    return description, fields


async def send_discord(payload: dict) -> None:
    """Webhook 優先；未設定 Webhook 時改由 Bot 發送到指定頻道。"""
    async with httpx.AsyncClient(timeout=15) as client:
        if DISCORD:
            response = await client.post(DISCORD, json=payload)
        elif DISCORD_BOT_TOKEN and DISCORD_CHANNEL_ID:
            bot_payload = {k: v for k, v in payload.items() if k != "username"}
            response = await client.post(
                f"{DISCORD_API}/channels/{DISCORD_CHANNEL_ID}/messages",
                headers={"Authorization": f"Bot {DISCORD_BOT_TOKEN}"},
                json=bot_payload,
            )
        else:
            return
        response.raise_for_status()


async def emit_alert(result: dict, event_code: str, title: str, color: int,
                     event_note: str = "", identity: str | None = None) -> bool:
    alert_id = identity or result["setup_id"]
    con = db()
    exists = con.execute("SELECT 1 FROM alerts WHERE setup_id=? AND level=?",
                         (alert_id, event_code)).fetchone()
    if exists:
        con.close()
        return False
    description, fields = discord_fields(result, event_note)
    tw = datetime.now(TAIPEI).strftime("%Y-%m-%d %H:%M:%S")
    msg = (f"【{title}】\n台灣時間：{tw}\n即時合約價：{result['futures_price']}\n"
           f"方向：{result['direction']}｜分數：{result['score']}/100｜階段：L{result['stage']}｜"
           f"{result.get('trade_status', '目前不可下單')}\n"
           f"4H：{result['trend_4h']}｜OTE：{result['zone_status']}\n"
           f"{event_note}\n缺少：{'、'.join(result['missing']) or '無'}")
    con.execute("INSERT INTO alerts(setup_id,level,ts,message) VALUES(?,?,?,?)",
                (alert_id, event_code, int(time.time()), msg))
    con.commit()
    con.close()
    payload = {"username": "ETH SMC／ICT Scanner", "embeds": [{
        "title": title, "description": description, "color": color,
        "fields": fields, "timestamp": datetime.now(timezone.utc).isoformat(),
        "footer": {"text": "Gate ETH_USDT｜每 60 秒掃描｜結構只採已收 K｜不會自動下單"},
    }]}
    await send_discord(payload)
    return True


def position_plan(side: str, entry_price: float, live_price: float, h4: list[dict],
                  h1: list[dict], m15: list[dict]) -> tuple[float, dict]:
    s15 = structure(m15)
    buffer = max(.12*atr(m15), entry_price*.0005)
    if side == "LONG":
        lows = [x["p"] for x in s15["lows"] if x["p"] < entry_price]
        stop = (max(lows) if lows else entry_price-1.2*atr(m15))-buffer
    else:
        highs = [x["p"] for x in s15["highs"] if x["p"] > entry_price]
        stop = (min(highs) if highs else entry_price+1.2*atr(m15))+buffer
    return round(stop, 2), ict_targets(side, entry_price, stop, live_price, h4, h1, m15)


def position_metrics(position: dict, current_price: float) -> tuple[float, float]:
    sign = 1 if position["side"] == "LONG" else -1
    pnl_pct = sign*(current_price-position["entry_price"])/position["entry_price"]*100
    initial_risk = abs(position["entry_price"]-position["initial_stop"])
    pnl_r = sign*(current_price-position["entry_price"])/initial_risk if initial_risk else 0
    return pnl_pct, pnl_r


async def emit_position_alert(position: dict, result: dict, event_code: str,
                              title: str, decision: str, color: int,
                              identity: str | None = None) -> bool:
    alert_id = identity or position["position_id"]
    con = db()
    exists = con.execute("SELECT 1 FROM alerts WHERE setup_id=? AND level=?",
                         (alert_id, event_code)).fetchone()
    if exists:
        con.close()
        return False
    current = result["futures_price"]
    pnl_pct, pnl_r = position_metrics(position, current)
    target_lines = "\n".join(
        f"TP{i+1} `{n(x['price'])}` · `{x['rr']:.2f}R` · {x['type']} · `{x['allocation']}%`"
        for i, x in enumerate(position.get("targets", []))
    ) or "建立持倉時沒有合格的未掃 ICT 流動性目標"
    description = (f"**{decision}**\n方向 **{position['side']}**｜進場 `{n(position['entry_price'])}`｜"
                   f"即時 `{n(current)}`\n未實現 `{pnl_pct:+.2f}%`｜`{pnl_r:+.2f}R`")
    fields = [
        {"name": "📌 持倉資料",
         "value": (f"持倉編號 `{position['position_id']}`\n進場時間 "
                   f"`{datetime.fromtimestamp(position['entry_ts'], TAIPEI).strftime('%m/%d %H:%M:%S')}`\n"
                   f"初始結構止損 `{n(position['initial_stop'])}`｜追蹤保護 "
                   f"`{n(position.get('trailing_level'))}`"), "inline": False},
        {"name": "💹 掃描當下價格",
         "value": (f"合約 `{n(result['futures_price'])}`｜現貨 `{n(result['spot_price'])}`\n"
                   f"標記 `{n(result['mark_price'])}`｜指數 `{n(result['index_price'])}`"), "inline": False},
        {"name": "🧭 已收 K 狀態",
         "value": (f"1H `{result['trend_1h']}`｜主方向 `{result['direction']}`\n"
                   f"15M MSS `{'✅' if result['mss_15m'] else '—'}`｜"
                   f"5M 觸發 `{'✅' if result['trigger_5m'] else '—'}`\n"
                   f"訂單簿失衡 `{result['orderbook_imbalance']:+.3f}`"), "inline": False},
        {"name": "🎯 原持倉 ICT 出場計畫", "value": target_lines[:1024], "inline": False},
    ]
    msg = (f"【{title}】\n{decision}\n{position['side']}｜進場 {position['entry_price']}｜"
           f"即時 {current}｜{pnl_pct:+.2f}%｜{pnl_r:+.2f}R")
    con.execute("INSERT INTO alerts(setup_id,level,ts,message) VALUES(?,?,?,?)",
                (alert_id, event_code, int(time.time()), msg))
    con.commit()
    con.close()
    await send_discord({"username": "ETH 持倉風控", "embeds": [{
        "title": title, "description": description, "color": color, "fields": fields,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "footer": {"text": "只提供出場風險提示，不會自動平倉；完整出場後請輸入：已出場"},
    }]})
    return True


async def command_reply(title: str, text: str, color: int = 0x5B8FF9) -> None:
    await send_discord({"username": "ETH 持倉風控", "embeds": [{
        "title": title, "description": text, "color": color,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }]})


async def process_discord_command(content: str, message_id: str, author_id: str,
                                  result: dict, h4: list[dict], h1: list[dict],
                                  m15: list[dict]) -> None:
    normalized = re.sub(r"\s+", "", content.strip())
    current = result["futures_price"]
    position = active_position()
    is_long = normalized.startswith(("已下單多", "已下单多"))
    is_short = normalized.startswith(("已下單空", "已下单空"))
    is_exit = normalized.startswith(("已出場", "已出场"))
    is_status = normalized.startswith(("持倉狀態", "持仓状态"))
    if not any((is_long, is_short, is_exit, is_status)):
        return
    if is_long or is_short:
        if position:
            await command_reply("⚠️ 已有持倉，未建立新持倉",
                                f"目前仍有 `{position['side']}` 持倉。請先輸入 `已出場` 再建立下一筆。",
                                0xF39C12)
            return
        side = "LONG" if is_long else "SHORT"
        match = re.search(r"(\d{3,}(?:\.\d+)?)", normalized)
        entry_price = f(match.group(1)) if match else current
        stop, target_plan = position_plan(side, entry_price, current, h4, h1, m15)
        position_id = hashlib.sha256(f"{message_id}:{side}:{entry_price}".encode()).hexdigest()[:16]
        position = {
            "position_id": position_id, "side": side, "status": "OPEN",
            "entry_ts": int(time.time()), "entry_price": round(entry_price, 2),
            "initial_stop": stop, "trailing_level": None,
            "targets": target_plan["details"], "target_quality_ok": target_plan["quality_ok"],
            "target_reason": target_plan["reason"], "alerts_sent": [],
            "peak_price": current, "command_message_id": message_id, "author_id": author_id,
        }
        save_position(position)
        price_note = "使用你輸入的成交價" if match else "未附成交價，暫以 Gate 掃描即時價記錄"
        await emit_position_alert(position, result, "POSITION_OPEN",
                                  f"📥 已切換持倉監控｜{side}",
                                  f"持倉已登記；{price_note}。系統每 60 秒評估出場風險。",
                                  0x3498DB, identity=position_id)
        return
    if is_exit:
        if not position:
            await command_reply("ℹ️ 目前沒有持倉", "系統已處於一般訊號掃描模式。")
            return
        position["status"] = "CLOSED"
        position["exit_ts"] = int(time.time())
        position["exit_price"] = current
        pnl_pct, pnl_r = position_metrics(position, current)
        await emit_position_alert(position, result, "POSITION_CLOSED",
                                  "✅ 已登記完整出場｜恢復一般掃描",
                                  f"本筆監控結束；參考結果 `{pnl_pct:+.2f}% / {pnl_r:+.2f}R`。",
                                  0x5AD8A6, identity=position["position_id"])
        save_position(position)
        return
    if is_status:
        if not position:
            await command_reply("ℹ️ 持倉狀態", "目前沒有登記中的持倉，系統正在一般訊號掃描模式。")
        else:
            await emit_position_alert(position, result, f"STATUS_{message_id}",
                                      "📋 ETH 持倉狀態", "目前持倉仍在監控中。",
                                      0x5B8FF9, identity=position["position_id"])


async def poll_discord_commands(result: dict, h4: list[dict], h1: list[dict],
                                m15: list[dict]) -> None:
    if not (DISCORD_BOT_TOKEN and DISCORD_CHANNEL_ID):
        state["discord_commands"] = "未設定 Bot"
        return
    headers = {"Authorization": f"Bot {DISCORD_BOT_TOKEN}"}
    cursor = get_system_state("discord_last_message_id")
    params: dict[str, Any] = {"limit": 50}
    if cursor:
        params["after"] = str(cursor)
    async with httpx.AsyncClient(timeout=15) as client:
        response = await client.get(f"{DISCORD_API}/channels/{DISCORD_CHANNEL_ID}/messages",
                                    headers=headers, params=params)
        response.raise_for_status()
        messages = response.json()
    if not cursor:
        if messages:
            set_system_state("discord_last_message_id", max(messages, key=lambda x: int(x["id"]))["id"])
        state["discord_commands"] = "已連線，等待新指令"
        return
    for message in sorted(messages, key=lambda x: int(x["id"])):
        message_id = str(message["id"])
        set_system_state("discord_last_message_id", message_id)
        author = message.get("author", {})
        if author.get("bot") or message.get("webhook_id"):
            continue
        author_id = str(author.get("id", ""))
        if DISCORD_ALLOWED_USER_ID and author_id != DISCORD_ALLOWED_USER_ID:
            continue
        await process_discord_command(message.get("content", ""), message_id, author_id,
                                      result, h4, h1, m15)
    state["discord_commands"] = "已連線，等待新指令"


async def monitor_position(result: dict, h4: list[dict], h1: list[dict],
                           m15: list[dict], m5: list[dict]) -> None:
    position = active_position()
    if not position:
        state["position"] = None
        return
    current, side = result["futures_price"], position["side"]
    position["peak_price"] = (max(position.get("peak_price", current), current) if side == "LONG"
                              else min(position.get("peak_price", current), current))
    s15, s5 = structure(m15), structure(m5)
    if side == "LONG":
        protective = [x["p"] for x in s15["lows"] if position["entry_price"] < x["p"] < current]
        if protective:
            position["trailing_level"] = max(position.get("trailing_level") or 0, max(protective))
    else:
        protective = [x["p"] for x in s15["highs"] if current < x["p"] < position["entry_price"]]
        if protective:
            old = position.get("trailing_level")
            position["trailing_level"] = min(old, min(protective)) if old else min(protective)
    stop_hit = current <= position["initial_stop"] if side == "LONG" else current >= position["initial_stop"]
    trailing = position.get("trailing_level")
    trailing_broken = bool(trailing and (m15[-1]["c"] < trailing if side == "LONG" else m15[-1]["c"] > trailing))
    direction_reversed = result["direction"] == ("SHORT" if side == "LONG" else "LONG")
    opposite_15m = (("BEARISH" in s15["trend"] and s15["adx"] >= 22) if side == "LONG"
                    else ("BULLISH" in s15["trend"] and s15["adx"] >= 22))
    orderflow_warning = ((result["orderbook_imbalance"] < -.35 and "BEARISH" in s5["trend"])
                         if side == "LONG" else
                         (result["orderbook_imbalance"] > .35 and "BULLISH" in s5["trend"]))
    if stop_hit:
        await emit_position_alert(position, result, "STOP_HIT",
                                  "🛑 ETH 結構止損已觸及｜建議完整出場",
                                  "即時價格已觸及建立持倉時的結構止損；建議立即檢查並完整出場。",
                                  0xE74C3C)
    elif direction_reversed:
        await emit_position_alert(position, result, "DIRECTION_REVERSAL",
                                  "⛔ ETH 1H 主方向反轉｜建議退出",
                                  "1H 已收 K 主方向已與持倉相反；建議完整出場或至少大幅減倉。",
                                  0xE74C3C)
    elif trailing_broken:
        await emit_position_alert(position, result, f"TRAIL_BREAK_{trailing}",
                                  "⚠️ ETH 15M 保護結構失守｜建議減倉",
                                  f"15M 已收 K 穿越追蹤保護位 {trailing}；建議減倉並重新審核剩餘部位。",
                                  0xF39C12)
    elif opposite_15m:
        await emit_position_alert(position, result, "OPPOSITE_15M",
                                  "⚠️ ETH 15M 反向結構增強｜收緊風控",
                                  "15M 已收 K 出現強反向結構；建議減倉或將止損推進至保護位置。",
                                  0xF39C12)
    reached = []
    for i, target in enumerate(position.get("targets", [])):
        hit = current >= target["price"] if side == "LONG" else current <= target["price"]
        if hit and f"TP{i+1}" not in position["alerts_sent"]:
            reached.append((i, target))
    for i, target in reached:
        is_final = i == len(position.get("targets", []))-1
        await emit_position_alert(
            position, result, f"TP{i+1}_HIT",
            f"🎯 ETH TP{i+1} 已觸及｜{'建議完整出場' if is_final else '建議分批出場'}",
            (f"已到達 {target['type']}；建議出場 {target['allocation']}%。"
             + ("這是最後一個合格外部流動性目標。" if is_final else "剩餘部位依保護結構管理。")),
            0x5AD8A6,
        )
        position["alerts_sent"].append(f"TP{i+1}")
    if orderflow_warning:
        await emit_position_alert(position, result, "ORDERFLOW_WARNING",
                                  "⚠️ ETH 短線訂單流反向｜提高警戒",
                                  "5M 結構與訂單簿失衡同時反向；目前不是硬出場，但建議密切檢查。",
                                  0xF6BD16)
    save_position(position)
    state["position"] = position


async def notify(result: dict, previous: dict | None) -> None:
    """每個事件只通知一次；結構變化、破壞及階段升級都會帶當下價格。"""
    stage_titles = {
        1: ("🧭 ETH L1 新結構／趨勢觀察", 0x5B8FF9),
        2: ("🔔 ETH L2 接近 OTE", 0xF6BD16),
        3: ("🟠 ETH L3 進入 OTE 監控區", 0xE8684A),
        4: ("✅ ETH L4 交易條件收 K 確認", 0x5AD8A6),
    }
    new_structure = (not previous or previous.get("structure_id") != result["structure_id"] or
                     previous.get("setup_id") != result["setup_id"])
    direction_changed = bool(previous and previous.get("direction") not in (None, "觀察") and
                             previous.get("direction") != result["direction"])
    protected_broken = False
    if previous and previous.get("protected_15m") and previous.get("mss_15m"):
        protected_broken = (
            previous["direction"] == "LONG" and result["closed_15m"] < previous["protected_15m"]
        ) or (
            previous["direction"] == "SHORT" and result["closed_15m"] > previous["protected_15m"]
        )
    if direction_changed or protected_broken or (result["invalid"] and not (previous or {}).get("invalid")):
        reasons = []
        if direction_changed:
            reasons.append(f"4H 已收 K 方向由 {previous['direction']} 轉為 {result['direction']}")
        if protected_broken:
            reasons.append(f"15M 收盤破壞前一保護結構 {previous['protected_15m']}")
        if result["invalid_reason"]:
            reasons.append(result["invalid_reason"])
        old_id = (previous or result).get("setup_id", result["setup_id"])
        await emit_alert(result, "INVALID", "⛔ ETH 結構破壞／Setup 失效｜目前不可下單", 0xE74C3C,
                         "；".join(reasons), old_id)
    if new_structure:
        await emit_alert(result, "NEW_STRUCTURE",
                         f"🆕 ETH L{result['stage']} 已收 K 新結構｜{result['trade_label']}", 0x3498DB,
                         f"新結構編號 `{result['structure_id']}`；舊結構不再沿用。",
                         result["structure_id"])
    previous_stage = int((previous or {}).get("stage", 0))
    if result["stage"] > previous_stage or not previous:
        title, color = stage_titles[result["stage"]]
        await emit_alert(result, f"L{result['stage']}",
                         f"{title}｜{result['trade_label']}", color)
    elif previous and previous.get("formal") and not result["formal"]:
        await emit_alert(result, "CONDITION_LOST", "⚠️ ETH 正式條件已退回｜目前不可下單", 0xF39C12,
                         "原 L4 條件已不再成立，請勿沿用先前進場計畫。")


async def scan() -> None:
    async with httpx.AsyncClient(timeout=20, headers={"Accept": "application/json"}) as client:
        tasks = [
            gate(client, "/futures/usdt/candlesticks", contract=PAIR, interval=interval, limit=limit)
            for interval, limit in (("4h", 300), ("1h", 500), ("15m", 1000), ("5m", 400))
        ]
        tasks += [
            gate(client, "/spot/candlesticks", currency_pair=PAIR, interval="15m", limit=80),
            gate(client, "/futures/usdt/tickers", contract=PAIR),
            gate(client, "/futures/usdt/order_book", contract=PAIR, limit=20),
            gate(client, "/spot/tickers", currency_pair=PAIR),
        ]
        raw = await asyncio.gather(*tasks)
    h4, h1, m15, m5 = [candles(x) for x in raw[:4]]
    spot = candles(raw[4], False)
    ticker = raw[5][0] if isinstance(raw[5], list) else raw[5]
    spot_ticker = raw[7][0] if isinstance(raw[7], list) else raw[7]
    if min(map(len, (h4, h1, m15, m5, spot))) < 30:
        raise RuntimeError("Gate K 線資料不足")
    result = setup(h4, h1, m15, m5, spot, ticker, spot_ticker, raw[6])
    now = int(time.time())
    con = db()
    prior = con.execute("SELECT payload FROM snapshots ORDER BY ts DESC,id DESC LIMIT 1").fetchone()
    previous = json.loads(prior["payload"]) if prior else None
    con.execute("INSERT INTO snapshots(ts,price,score,direction,payload) VALUES(?,?,?,?,?)",
                (now, result["price"], result["score"], result["direction"], json.dumps(result, ensure_ascii=False)))
    con.execute("DELETE FROM snapshots WHERE ts < ?", (now - 90*86400,))
    con.commit(); con.close()
    state.update(status="正常", updated_at=datetime.now(timezone.utc).isoformat(), error=None,
                 analysis=result, data_quality=100, scan_count=state["scan_count"]+1)
    try:
        await poll_discord_commands(result, h4, h1, m15)
    except Exception as exc:
        LOG.exception("Discord 指令讀取失敗")
        state["discord_commands"] = f"讀取失敗：{exc}"
    if active_position():
        await monitor_position(result, h4, h1, m15, m5)
    else:
        state["position"] = None
        await notify(result, previous)


async def worker() -> None:
    while True:
        try:
            await scan()
        except Exception as exc:
            LOG.exception("掃描失敗")
            state.update(status="降級", error=str(exc), data_quality=0)
        await asyncio.sleep(SCAN_SECONDS)


@asynccontextmanager
async def lifespan(_: FastAPI):
    db().close()
    task = asyncio.create_task(worker())
    await asyncio.sleep(0)
    yield
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


app = FastAPI(title="ETH SMC/ICT Scanner", version="2.0.0", lifespan=lifespan)


@app.get("/health")
def health() -> dict:
    return {"ok": state["status"] == "正常", "service": state["status"], "updated_at": state["updated_at"], "error": state["error"]}


@app.get("/api/status")
def status() -> dict:
    return state


@app.post("/api/scan")
async def manual_scan() -> dict:
    await scan()
    return state


@app.get("/api/history")
def history(limit: int = Query(100, ge=1, le=1000)) -> list[dict]:
    con = db()
    rows = con.execute("SELECT ts,price,score,direction,payload FROM snapshots ORDER BY ts DESC LIMIT ?", (limit,)).fetchall()
    con.close()
    return [{"ts": x["ts"], "price": x["price"], "score": x["score"], "direction": x["direction"],
             "detail": json.loads(x["payload"])} for x in rows]


@app.get("/api/alerts")
def alerts(limit: int = Query(50, ge=1, le=200)) -> list[dict]:
    con = db()
    rows = con.execute("SELECT * FROM alerts ORDER BY ts DESC LIMIT ?", (limit,)).fetchall()
    con.close()
    return [dict(x) for x in rows]


@app.get("/api/position")
def position_status() -> dict:
    return {"active": active_position(), "discord_commands": state.get("discord_commands", "未設定")}


@app.get("/api/positions")
def position_history(limit: int = Query(50, ge=1, le=200)) -> list[dict]:
    con = db()
    rows = con.execute("SELECT payload FROM positions ORDER BY entry_ts DESC LIMIT ?", (limit,)).fetchall()
    con.close()
    return [json.loads(x["payload"]) for x in rows]


@app.get("/api/backtest")
async def backtest(days: int = Query(30, ge=7, le=80)) -> dict:
    """以 Gate 真實 1H K 線做無未來資料的趨勢/回撤簡易驗證。"""
    limit = min(days * 24, 1900)
    async with httpx.AsyncClient(timeout=30) as client:
        cs = candles(await gate(client, "/futures/usdt/candlesticks", contract=PAIR, interval="1h", limit=limit))
    trades = []
    for i in range(80, len(cs)-12):
        past = cs[:i+1]
        closes = [x["c"] for x in past]
        a = atr(past)
        trend = 1 if ema(closes, 30) > ema(closes, 60) else -1
        pullback = abs(closes[-1]-ema(closes, 30)) <= .45*a
        if not pullback:
            continue
        entry, stop = closes[-1], closes[-1]-trend*1.2*a
        outcome = None
        for bar in cs[i+1:i+13]:
            if (trend == 1 and bar["l"] <= stop) or (trend == -1 and bar["h"] >= stop):
                outcome = -1.0; break
            if (trend == 1 and bar["h"] >= entry+2*a) or (trend == -1 and bar["l"] <= entry-2*a):
                outcome = round(2/1.2, 2); break
        if outcome is not None:
            trades.append(outcome)
    wins = [x for x in trades if x > 0]
    losses = [x for x in trades if x < 0]
    return {"source": "Gate 真實 ETH_USDT 1H 已收線", "trades": len(trades),
            "win_rate": round(100*len(wins)/len(trades), 2) if trades else 0,
            "average_r": round(sum(trades)/len(trades), 3) if trades else 0,
            "profit_factor": round(sum(wins)/abs(sum(losses)), 2) if losses else None,
            "note": "這是核心趨勢回撤的快速驗證，不代表未來績效。"}


HTML = """<!doctype html><html lang="zh-Hant"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>ETH 脈衝掃描器</title><style>
:root{color-scheme:dark;--bg:#08111f;--card:#101d30;--line:#223754;--cyan:#48d7e8;--gold:#ffca5c;--muted:#91a5bd}
*{box-sizing:border-box}body{margin:0;background:radial-gradient(circle at 80% 0,#172b47,var(--bg) 38%);font:15px system-ui;color:#eef5ff}
main{max-width:1180px;margin:auto;padding:28px}.top{display:flex;justify-content:space-between;align-items:end}h1{margin:0;font-size:30px}.sub,.muted{color:var(--muted)}
.grid{display:grid;grid-template-columns:repeat(4,1fr);gap:14px;margin-top:22px}.card{background:linear-gradient(145deg,#12233a,#0d1929);border:1px solid var(--line);border-radius:14px;padding:18px;box-shadow:0 12px 40px #0004}
.wide{grid-column:span 2}.full{grid-column:1/-1}.label{color:var(--muted);font-size:12px;text-transform:uppercase;letter-spacing:.12em}.big{font:700 28px ui-monospace;margin-top:8px}.cyan{color:var(--cyan)}.gold{color:var(--gold)}
.bar{height:8px;background:#26384d;border-radius:8px;overflow:hidden;margin-top:12px}.bar i{display:block;height:100%;background:linear-gradient(90deg,var(--cyan),var(--gold))}
table{width:100%;border-collapse:collapse;margin-top:9px}td{padding:8px;border-bottom:1px solid var(--line)}td:last-child{text-align:right;font-family:monospace}
button{background:var(--cyan);border:0;border-radius:8px;padding:9px 14px;font-weight:700;cursor:pointer}@media(max-width:760px){.grid{grid-template-columns:1fr 1fr}.wide{grid-column:span 2}}
</style></head><body><main><div class="top"><div><div class="label">GATE · ETH_USDT PERPETUAL</div><h1>ETH 脈衝掃描器</h1><div class="sub">SMC / ICT · 僅訊號，不執行交易</div></div><button onclick="scan()">立即掃描</button></div>
<section class="grid"><div class="card"><div class="label">Gate 合約即時價格</div><div id="price" class="big cyan">—</div><div id="priceDetail" class="muted"></div></div>
<div class="card"><div class="label">方向 / 階段 / 交易判定</div><div id="direction" class="big">—</div><div id="tradeStatus" class="muted"></div></div>
<div class="card"><div class="label">綜合分數</div><div id="score" class="big gold">—</div><div class="bar"><i id="scorebar"></i></div></div>
<div class="card"><div class="label">資料狀態</div><div id="health" class="big">—</div></div>
<div class="card wide"><div class="label">Fibonacci / OTE</div><table id="fib"></table></div>
<div class="card wide"><div class="label">已收 K 結構與即時衍生資料</div><table id="conditions"></table></div>
<div class="card wide"><div class="label">參考計畫</div><table id="plan"></table></div>
<div class="card wide"><div class="label">尚缺條件</div><div id="missing" style="margin-top:14px;line-height:1.8"></div></div>
<div class="card full"><div class="label">Discord 持倉監控模式</div><div id="positionInfo" style="margin-top:12px;line-height:1.8"></div></div>
<div class="card full muted" id="updated">等待第一輪 Gate 資料…</div></section></main>
<script>
const fmt=n=>Number(n).toLocaleString('zh-TW',{maximumFractionDigits:2});const rows=o=>Object.entries(o).map(([k,v])=>`<tr><td>${k}</td><td>${v}</td></tr>`).join('');
async function load(){let s=await fetch('/api/status').then(r=>r.json()),a=s.analysis||{};price.textContent=a.futures_price?`$ ${fmt(a.futures_price)}`:'—';priceDetail.textContent=a.spot_price?`現貨 ${fmt(a.spot_price)} · 標記 ${fmt(a.mark_price)} · 指數 ${fmt(a.index_price)}`:'';
direction.textContent=(a.direction||'—')+' · L'+(a.stage||0)+' · '+(a.trade_label||'—');tradeStatus.textContent=a.trade_status||'';
score.textContent=(a.score??'—')+'/100';scorebar.style.width=(a.score||0)+'%';health.textContent=s.status;
fib.innerHTML=rows({'狀態':a.fib_valid?'正式有效':'尚未建立','原因':a.fib_reason||'—','.618':a.fib?.['.618']??'—','.705':a.fib?.['.705']??'—','.786':a.fib?.['.786']??'—','OTE':a.ote?.length===2?a.ote.join(' ～ '):'—'});
conditions.innerHTML=rows({'1H 主方向 / 偏向':(a.direction||'—')+' / '+(a.market_bias||a.trend_1h||'—'),'4H 輔助背景':(a.trend_4h||'—')+' / '+(a.four_h_context||'—'),'4H Swing / BOS':(a.raw_trend_4h||a.trend_4h||'—')+' / '+(a.bos_4h||'—'),'1H 趨勢':`${a.trend_1h||'—'} / ADX ${a.adx_1h??'—'}`,'1H 多空票數':a.direction_votes?`多 ${a.direction_votes.bull} / 空 ${a.direction_votes.bear}`:'—','方向依據':a.trend_reason||'—','4H EMA20 / 50':a.ema_4h?`${a.ema_4h.ema20} / ${a.ema_4h.ema50}`:'—','4H Swing':a.swing_low_4h&&a.swing_high_4h?`${a.swing_low_4h} ～ ${a.swing_high_4h}`:'—','15M MSS':a.mss_15m?'已收 K 確認':'等待','5M 觸發':a.trigger_5m?'已收 K 確認':'等待','15M / 現貨 RVOL':`${a.rvol_15m??'—'} / ${a.spot_rvol??'—'}`,'資金費率':a.funding_rate!=null?(a.funding_rate*100).toFixed(4)+'%':'—','OI 約當 ETH':a.oi_eth_estimate?fmt(a.oi_eth_estimate):'—','訂單簿失衡 / Spread':`${a.orderbook_imbalance??'—'} / ${a.spread??'—'}`});
let tp=(a.target_details||[]).map((x,i)=>`TP${i+1} ${x.price} (${x.rr}R · ${x.type} · ${x.allocation}%)`).join(' / ');
plan.innerHTML=rows({'Setup 狀態':a.invalid?'已失效':a.zone_status||'—','進場區':a.fib_valid&&a.entry?.length===2?a.entry.join(' ～ '):'尚未提供','結構止損':a.stop||'—','ICT 流動性目標':tp||'無合格目標','加權報酬':`${a.weighted_rr??0}R / ${a.target_quality_ok?'合格':'不合格'}`,'目標判定':a.target_reason||'—','正式訊號':a.formal?'是':'否'});
missing.textContent=(a.missing||[]).join(' · ')||'條件已齊備';
let p=s.position;if(p){let sign=p.side==='LONG'?1:-1,pnl=sign*((a.futures_price||p.entry_price)-p.entry_price)/p.entry_price*100;positionInfo.innerHTML=`<b class="gold">持倉監控中 · ${p.side}</b>　進場 ${p.entry_price}　即時 ${a.futures_price||'—'}　未實現 ${pnl.toFixed(2)}%<br>初始止損 ${p.initial_stop}　追蹤保護 ${p.trailing_level??'尚未形成'}<br><span class="muted">完整出場後請在 Discord 輸入「已出場」</span>`}else{positionInfo.innerHTML=`<span class="cyan">一般訊號掃描模式</span>　Discord 指令：已下單多／已下單空／持倉狀態`}
updated.textContent=`最後更新：${s.updated_at?new Date(s.updated_at).toLocaleString('zh-TW',{timeZone:'Asia/Taipei'}):'—'} · 掃描 ${s.scan_count} 次 · Discord ${s.discord_commands||'未設定'} · ${a.risk_note||''}`;}
async function scan(){await fetch('/api/scan',{method:'POST'});load()}load();setInterval(load,15000);
</script></body></html>"""


@app.get("/", response_class=HTMLResponse)
def dashboard() -> str:
    return HTML


if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=PORT)
