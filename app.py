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
TARGET_R_LEVELS = (0.55, 0.85, 1.15, 1.50)
TARGET_ALLOCATIONS = (40, 30, 20, 10)
ENTRY_PLAN_VERSION = 2
TARGET_PLAN_VERSION = 2
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


def volatility_profile(cs: list[dict]) -> dict:
    """辨識高波動與長影線環境，供掛單位置採取更保守的深回踩。"""
    recent = cs[-40:]
    if len(recent) < 10:
        return {
            "label": "資料不足", "high_volatility": False, "wick_prone": False,
            "deep_retest": False, "atr_pct": 0.0, "wick_spike_ratio": 0.0,
        }
    current_atr = atr(recent)
    close = max(f(recent[-1]["c"]), 1e-9)
    atr_pct = current_atr / close
    wick_spikes = 0
    wick_ratios = []
    for candle in recent[-24:]:
        candle_range = max(f(candle["h"])-f(candle["l"]), 1e-9)
        body_high = max(f(candle["o"]), f(candle["c"]))
        body_low = min(f(candle["o"]), f(candle["c"]))
        wick_ratio = ((f(candle["h"])-body_high) + (body_low-f(candle["l"]))) / candle_range
        wick_ratios.append(wick_ratio)
        if wick_ratio >= .62 and candle_range >= 1.25*max(current_atr, 1e-9):
            wick_spikes += 1
    wick_spike_ratio = wick_spikes / max(len(wick_ratios), 1)
    median_wick = statistics.median(wick_ratios) if wick_ratios else 0
    high_volatility = atr_pct >= .012
    wick_prone = wick_spike_ratio >= .16 or median_wick >= .58
    label = ("高波動＋易插針" if high_volatility and wick_prone else
             "高波動" if high_volatility else
             "易插針" if wick_prone else "一般")
    return {
        "label": label, "high_volatility": high_volatility, "wick_prone": wick_prone,
        "deep_retest": high_volatility or wick_prone,
        "atr_pct": round(atr_pct, 4), "wick_spike_ratio": round(wick_spike_ratio, 3),
    }


def thirty_minute_wall(direction: str, cs: list[dict], reference_zone: list[float],
                       live_price: float, deep_retest: bool = False) -> dict | None:
    """在 30M OTE 附近找已確認的支撐／壓力牆，不拿 5M 回踩當進場價。"""
    if len(cs) < 12 or len(reference_zone) != 2:
        return None
    zone_low, zone_high = sorted(map(f, reference_zone))
    current_atr = max(atr(cs), live_price*.0005)
    cluster_tolerance = max(.18*current_atr, live_price*.0006)
    highs, lows = pivots(cs, span=2)
    points = lows if direction == "LONG" else highs
    directional = [
        p for p in points[-30:]
        if (p["p"] <= live_price if direction == "LONG" else p["p"] >= live_price)
        and zone_low-cluster_tolerance <= p["p"] <= zone_high+cluster_tolerance
    ]
    if not directional:
        return None
    clusters: list[list[dict]] = []
    for point in sorted(directional, key=lambda x: x["p"]):
        cluster = next(
            (items for items in clusters
             if abs(point["p"]-statistics.mean(x["p"] for x in items)) <= cluster_tolerance),
            None,
        )
        if cluster is None:
            clusters.append([point])
        else:
            cluster.append(point)
    walls = []
    for cluster in clusters:
        price = statistics.median(x["p"] for x in cluster)
        touches = len(cluster)
        recency = max(x["t"] for x in cluster)
        inside = zone_low <= price <= zone_high
        walls.append({
            "price": price, "touches": touches, "last_touch": recency,
            "inside_ote": inside,
        })
    if deep_retest:
        chosen = (min(walls, key=lambda x: (x["price"], -x["touches"], -x["last_touch"]))
                  if direction == "LONG" else
                  max(walls, key=lambda x: (x["price"], x["touches"], x["last_touch"])))
    else:
        chosen = max(
            walls,
            key=lambda x: (
                int(x["inside_ote"]), x["touches"], x["last_touch"],
                x["price"] if direction == "LONG" else -x["price"],
            ),
        )
    half_width = max(.10*current_atr, live_price*.00035)
    low, high = chosen["price"]-half_width, chosen["price"]+half_width
    preferred = low if direction == "LONG" and deep_retest else (
        high if direction == "SHORT" and deep_retest else chosen["price"]
    )
    return {
        **chosen,
        "zone": [round(low, 2), round(high, 2)],
        "preferred_entry": round(preferred, 2),
        "type": "30M 支撐牆" if direction == "LONG" else "30M 壓力牆",
        "deep_retest": deep_retest,
    }


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
    """找鄰近真實流動性；門檻配合較近、較容易分批落袋的止盈距離。"""
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
        correct_side = price > entry_mid if direction == "LONG" else price < entry_mid
        if not correct_side:
            continue
        created_at = int(item.get("time", 0))
        later = [x for x in m15 if x["t"] > created_at] if created_at else []
        swept = (any(x["h"] > price + .01 for x in later) if direction == "LONG"
                 else any(x["l"] < price - .01 for x in later))
        if swept:
            continue
        rr = abs(price-entry_mid)/risk
        if rr >= TARGET_R_LEVELS[0]:
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
    rules = tuple(zip(TARGET_R_LEVELS, (1, 1, 2, 2)))
    last_rr = 0.0
    for threshold, min_strength in rules:
        required_rr = max(threshold, last_rr + .20 if selected else threshold)
        match = next((x for x in deduped if x["rr"] >= required_rr and
                      x.get("strength", 0) >= min_strength and x not in selected), None)
        if match:
            selected.append(match)
            last_rr = match["rr"]
    weights = list(TARGET_ALLOCATIONS[:len(selected)])
    weighted_rr = (sum(x["rr"]*w for x, w in zip(selected, weights))/sum(weights)
                   if weights else 0)
    quality_ok = bool(selected and selected[0]["rr"] >= TARGET_R_LEVELS[0] and
                      selected[-1]["rr"] >= TARGET_R_LEVELS[-1] and weighted_rr >= .9)
    reason = ("近端止盈：TP1≥0.55R、最終目標≥1.50R、TP1 優先落袋 40%" if quality_ok else
              f"ICT 目標不足：最終 {selected[-1]['rr']:.2f}R、加權 {weighted_rr:.2f}R"
              if selected else "前方找不到至少 0.55R 的有效流動性，改用近端 R 倍數風控目標")
    return {
        "targets": [round(x["price"], 2) for x in selected],
        "rr": [round(x["rr"], 2) for x in selected],
        "details": [{"price": round(x["price"], 2), "rr": round(x["rr"], 2),
                     "type": x["type"], "allocation": weights[i]}
                    for i, x in enumerate(selected)],
        "weighted_rr": round(weighted_rr, 2), "quality_ok": quality_ok, "reason": reason,
    }


def fib_zone(direction: str, low: float, high: float) -> tuple[dict, list[float]]:
    """依方向建立固定 Fibonacci 與 OTE；主計畫與短波計畫都共用同一規格。"""
    rng = high - low
    fib = ({k: high-rng*v for k, v in {".618": .618, ".705": .705, ".786": .786}.items()}
           if direction == "LONG" else
           {k: low+rng*v for k, v in {".618": .618, ".705": .705, ".786": .786}.items()})
    zone = [min(fib[".618"], fib[".786"]), max(fib[".618"], fib[".786"])]
    return fib, zone


def complete_target_plan(direction: str, entry_mid: float, stop: float,
                         target_plan: dict) -> list[dict]:
    """止盈以近端 R 階梯為上限；只有距離相近時才採用真實流動性價位。"""
    risk = abs(entry_mid-stop)
    if risk <= 0:
        return []
    actual = sorted(target_plan.get("details", []), key=lambda x: f(x.get("rr")))
    used: set[tuple[float, str]] = set()
    result = []
    for index, minimum_rr in enumerate(TARGET_R_LEVELS):
        match = next(
            (x for x in actual
             if minimum_rr <= f(x.get("rr")) <= minimum_rr+.15 and
             (f(x.get("price")), str(x.get("type"))) not in used),
            None,
        )
        if match:
            price, rr, target_type = f(match["price"]), f(match["rr"]), str(match["type"])
            used.add((price, target_type))
        else:
            rr = minimum_rr
            price = entry_mid + risk*rr if direction == "LONG" else entry_mid-risk*rr
            target_type = "R 倍數備援（非流動性目標）"
        result.append({
            "price": round(price, 2), "rr": round(rr, 2),
            "type": target_type, "allocation": TARGET_ALLOCATIONS[index],
        })
    return result


def previous_main_plan(previous: dict | None) -> dict | None:
    """讀取新版主計畫；也兼容部署前只有扁平欄位的舊快照。"""
    if not previous:
        return None
    plan = previous.get("main_plan")
    if isinstance(plan, dict) and plan.get("direction") in ("LONG", "SHORT"):
        return plan
    impulse = previous.get("confirmed_impulse", [])
    zone = previous.get("core_ote") or previous.get("confirmed_ote") or []
    if (previous.get("direction") in ("LONG", "SHORT") and len(impulse) == 2 and
            len(zone) == 2):
        return {
            "id": previous.get("fib_id") or previous.get("setup_id"),
            "direction": previous["direction"], "valid": not previous.get("invalid", False),
            "status": "INVALID" if previous.get("invalid") else "ACTIVE",
            "impulse": impulse, "zone": zone, "entry": previous.get("entry") or zone,
            "stop": previous.get("stop") or None,
            "targets": previous.get("target_details") or [],
            "stage": int(previous.get("stage", 1)),
            "zone_reached": bool(previous.get("in_ote") or int(previous.get("stage", 1)) >= 3),
            "bos_time": (previous.get("bos_impulse") or {}).get("bos_time", 0),
        }
    return None


def anchored_main_direction(bias_direction: str, closed_1h: float,
                            impulses: dict[str, dict],
                            previous_plan: dict | None) -> tuple[str, str, bool]:
    """短線票數不能翻掉主計畫；只有破壞起點且反向 BOS 更新才准換向。"""
    if previous_plan and not previous_plan.get("valid", True):
        old_direction = previous_plan["direction"]
        old_bos_time = int(previous_plan.get("bos_time", 0))
        newer = [(int(meta.get("bos_time", 0)), side)
                 for side, meta in impulses.items()
                 if meta.get("valid") and int(meta.get("bos_time", 0)) > old_bos_time]
        if newer:
            preferred = next((x for x in newer if x[1] == bias_direction), max(newer))
            side = preferred[1]
            return side, f"失效計畫已封存；新的 {side} 1H BOS 已確認，建立新主計畫", side != old_direction
        return "觀察", "舊主計畫已失效；尚無更新的 1H BOS，不臆測反方向", False
    if previous_plan and previous_plan.get("valid", True) and len(previous_plan.get("impulse", [])) == 2:
        old_direction = previous_plan["direction"]
        low, high = map(f, previous_plan["impulse"])
        broken = closed_1h < low if old_direction == "LONG" else closed_1h > high
        if not broken:
            if bias_direction != old_direction and bias_direction in ("LONG", "SHORT"):
                return old_direction, f"短線偏向 {bias_direction}，但主計畫結構尚未失效，維持 {old_direction}", False
            return old_direction, f"沿用未失效的 {old_direction} 主計畫", False
        opposite = "SHORT" if old_direction == "LONG" else "LONG"
        opposite_impulse = impulses[opposite]
        newer_opposite_bos = (
            opposite_impulse.get("valid") and
            int(opposite_impulse.get("bos_time", 0)) > int(previous_plan.get("bos_time", 0))
        )
        if newer_opposite_bos:
            return opposite, f"舊 {old_direction} 起點已破壞，且新的 {opposite} 1H BOS 已確認", True
        return old_direction, f"舊 {old_direction} 起點已破壞；等待反向 1H BOS 建立新主計畫", False
    if bias_direction in ("LONG", "SHORT") and impulses[bias_direction].get("valid"):
        return bias_direction, f"依已確認的 {bias_direction} 1H BOS 建立主計畫", False
    available = [(int(meta.get("bos_time", 0)), side)
                 for side, meta in impulses.items() if meta.get("valid")]
    if available:
        side = max(available)[1]
        return side, f"短線偏向未形成共識；暫以最新已確認的 {side} 1H BOS 建立主計畫", False
    return "觀察", "尚無任一方向的有效 1H BOS 主計畫", False


def make_trade_plan(kind: str, direction: str, low: float, high: float,
                    live_price: float, closed_1h: float, last_bar: dict,
                    score: int, mss: bool, trigger_5m: bool, spread: float,
                    h4: list[dict], h1: list[dict], m15: list[dict],
                    impulse_meta: dict, previous_plan: dict | None = None,
                    m30: list[dict] | None = None,
                    risk_profile: dict | None = None) -> dict:
    """建立可持續的計畫生命週期；價位固定、觸區與最高階段會跨掃描保留。"""
    fib, raw_zone = fib_zone(direction, low, high)
    plan_id = hashlib.sha256(
        f"{kind}:{direction}:{round(low, 2)}:{round(high, 2)}:"
        f"{impulse_meta.get('origin_time')}:{impulse_meta.get('endpoint_time')}".encode()
    ).hexdigest()[:16]
    same_plan = bool(
        previous_plan and (
            previous_plan.get("id") == plan_id or
            (previous_plan.get("direction") == direction and
             [round(f(x), 2) for x in previous_plan.get("impulse", [])] ==
             [round(low, 2), round(high, 2)])
        )
    )
    prior = previous_plan if same_plan else {}
    timeframe = "1H" if kind == "MAIN" else "15M"
    wall_candles = m30 or m15
    profile = risk_profile or volatility_profile(wall_candles)
    retained_wall = (
        prior.get("entry_wall")
        if int(prior.get("entry_plan_version", 0)) == ENTRY_PLAN_VERSION
        and isinstance(prior.get("entry_wall"), dict)
        else None
    )
    if retained_wall:
        retained_zone = list(map(f, retained_wall.get("zone", [])))
        wall_break_buffer = .10*atr(wall_candles)
        retained_broken = (
            len(retained_zone) != 2 or
            (direction == "LONG" and wall_candles[-1]["c"] < retained_zone[0]-wall_break_buffer) or
            (direction == "SHORT" and wall_candles[-1]["c"] > retained_zone[1]+wall_break_buffer)
        )
        if retained_broken:
            retained_wall = None
    entry_wall = retained_wall or thirty_minute_wall(
        direction, wall_candles, raw_zone, live_price, bool(profile.get("deep_retest"))
    )
    wall_confirmed = bool(entry_wall)
    if entry_wall:
        raw_entry_zone = list(map(f, entry_wall["zone"]))
        preferred_entry = f(entry_wall["preferred_entry"])
    else:
        raw_entry_zone = list(map(f, raw_zone))
        preferred_entry = None
    zone = [round(raw_entry_zone[0], 2), round(raw_entry_zone[1], 2)]
    buffer = max((.12*atr(h1) if kind == "MAIN" else .12*atr(m15)),
                 live_price*.0005, spread*3)
    stop = low-buffer if direction == "LONG" else high+buffer
    stop = f(prior.get("stop"), stop) if prior.get("stop") is not None else stop
    entry_mid = preferred_entry if preferred_entry is not None else sum(raw_entry_zone)/2
    liquidity_targets = ict_targets(direction, entry_mid, stop, live_price, h4, h1, m15)
    retain_targets = (
        int(prior.get("target_plan_version", 0)) == TARGET_PLAN_VERSION
        and retained_wall is not None
        and isinstance(prior.get("targets"), list)
        and len(prior["targets"]) == len(TARGET_R_LEVELS)
    )
    targets = (prior["targets"] if retain_targets else
               complete_target_plan(direction, entry_mid, stop, liquidity_targets))
    target_weighted_rr = (
        sum(f(x.get("rr"))*f(x.get("allocation")) for x in targets) /
        max(sum(f(x.get("allocation")) for x in targets), 1)
    )
    target_reason = (
        "近端分批止盈 0.55R／0.85R／1.15R／1.50R；"
        f"真實流動性檢查：{liquidity_targets['reason']}"
    )
    bar_touched = bool(
        wall_confirmed
        and f(last_bar.get("h")) >= raw_entry_zone[0]
        and f(last_bar.get("l")) <= raw_entry_zone[1]
    )
    in_zone = bool(wall_confirmed and raw_entry_zone[0] <= live_price <= raw_entry_zone[1])
    same_wall = bool(
        retained_wall and entry_wall
        and round(f(retained_wall.get("price")), 2) == round(f(entry_wall.get("price")), 2)
    )
    zone_reached = bool((prior.get("zone_reached") if same_wall else False)
                        or in_zone or bar_touched)
    distance = .35*atr(wall_candles)
    if direction == "LONG":
        near_zone = bool(wall_confirmed and
                         raw_entry_zone[1] < live_price <= raw_entry_zone[1]+distance)
        adverse = live_price < raw_entry_zone[0]
        favorable_departure = zone_reached and live_price > raw_entry_zone[1]
        invalid = closed_1h < low if kind == "MAIN" else m15[-1]["c"] < low
    else:
        near_zone = bool(wall_confirmed and
                         raw_entry_zone[0]-distance <= live_price < raw_entry_zone[0])
        adverse = live_price > raw_entry_zone[1]
        favorable_departure = zone_reached and live_price < raw_entry_zone[0]
        invalid = closed_1h > high if kind == "MAIN" else m15[-1]["c"] > high
    passive_limit = bool(
        preferred_entry is not None
        and (preferred_entry < live_price if direction == "LONG" else preferred_entry > live_price)
    )
    ready_now = bool(
        not invalid and wall_confirmed and passive_limit and score >= MIN_SCORE and mss
    )
    current_stage = 4 if ready_now else 3 if zone_reached else 2 if near_zone else 1
    stage = max(int(prior.get("stage", 0)), current_stage)
    if invalid:
        status = "INVALID"
        action = f"停止使用此計畫；{timeframe} 結構起點已被收盤破壞，等待新計畫"
    elif ready_now:
        status = "READY"
        depth_note = "高波動／易插針，優先掛牆位深側；" if profile.get("deep_retest") else ""
        action = (f"{depth_note}可評估在 30M "
                  f"{entry_wall['type']} {zone[0]:.2f}～{zone[1]:.2f} 限價等待回踩；"
                  f"止損 {stop:.2f}，不使用 5M 回踩價追單")
    elif not wall_confirmed:
        status = "WAIT_30M_WALL"
        action = "30M 支撐／壓力牆尚未形成；不使用 5M 回踩價，等待更好的限價位置"
    elif not passive_limit:
        status = "WAIT_WALL_RECLAIM"
        action = "價格已穿到 30M 牆位不利側；此時掛單會立即成交，先等 30M 收復牆位再評估"
    elif stage >= 4:
        status = "COOLDOWN"
        action = "L4 曾確認但目前不在可掛單窗口；只管理既有掛單／持倉，不新增追價單"
    elif zone_reached:
        status = "TRIGGER"
        action = "已回踩 30M 牆位；保持原方向並等待 15M MSS，不用 5M 價位追單"
    elif near_zone:
        status = "APPROACH"
        action = "接近 30M 牆位；只保留牆位限價計畫，不改掛到 5M 回踩價"
    else:
        status = "WAIT"
        action = "保留 30M 牆位限價，等待回踩成交；現在不市價追單"
    if invalid:
        zone_status = "結構已失效"
    elif not wall_confirmed:
        zone_status = "等待 30M 支撐／壓力牆"
    elif in_zone:
        zone_status = "目前位於 30M 牆位掛單區"
    elif favorable_departure:
        zone_status = "觸區後向目標方向離開"
    elif adverse and zone_reached:
        zone_status = "穿越計畫區但尚待結構收盤判定"
    elif near_zone:
        zone_status = "接近計畫區"
    else:
        zone_status = "等待回到計畫區"
    missing = []
    if not wall_confirmed:
        missing.append("30M 支撐／壓力牆尚未確認")
    elif not passive_limit:
        missing.append("價格位於 30M 牆位不利側，禁止送出會立即成交的限價單")
    elif not zone_reached:
        missing.append("價格尚未回踩 30M 牆位（限價可等待，不追價）")
    if not mss:
        missing.append("15M MSS 尚未確認")
    if score < MIN_SCORE:
        missing.append(f"分數 {score} 未達 {MIN_SCORE}")
    return {
        "id": plan_id, "kind": kind, "name": "主計畫" if kind == "MAIN" else "短波延伸計畫",
        "direction": direction, "valid": not invalid, "status": status,
        "stage": stage, "current_stage": current_stage, "ready_now": ready_now,
        "action": action, "impulse": [round(low, 2), round(high, 2)],
        "entry_plan_version": ENTRY_PLAN_VERSION,
        "target_plan_version": TARGET_PLAN_VERSION,
        "bos_time": impulse_meta.get("bos_time", 0),
        "origin_time": impulse_meta.get("origin_time", 0),
        "endpoint_time": impulse_meta.get("endpoint_time", 0),
        "fib": {k: round(v, 2) for k, v in fib.items()},
        "ote_zone": [round(raw_zone[0], 2), round(raw_zone[1], 2)],
        "zone": zone, "entry": zone,
        "preferred_entry": round(preferred_entry, 2) if preferred_entry is not None else None,
        "entry_wall": entry_wall, "wall_confirmed_30m": wall_confirmed,
        "risk_profile": profile,
        "stop": round(stop, 2), "targets": targets,
        "target_quality_ok": bool(targets),
        "target_reason": target_reason,
        "weighted_rr": round(target_weighted_rr, 2),
        "zone_reached": zone_reached,
        "zone_reached_at": ((prior.get("zone_reached_at") if same_wall else None) or
                            (int(time.time()) if zone_reached else None)),
        "in_zone_now": in_zone, "near_zone_now": near_zone,
        "zone_status": zone_status, "missing": missing,
        "cancel_condition": (f"{timeframe} 收盤跌破 {low:.2f}" if direction == "LONG"
                             else f"{timeframe} 收盤突破 {high:.2f}"),
        "created_at": prior.get("created_at") or int(time.time()),
    }


def setup(h4: list[dict], h1: list[dict], m15: list[dict], m5: list[dict],
          spot: list[dict], ticker: dict, spot_ticker: dict, book: dict,
          previous: dict | None = None, m30: list[dict] | None = None) -> dict:
    """結構只讀已收 K；區域距離與通知價格使用當下 Gate ticker。"""
    m30 = m30 or m15
    s4, s1, s15, s5 = structure(h4), structure(h1), structure(m15), structure(m5)
    raw_trend_4h = s4["trend"]
    close_4h = h4[-1]["c"]
    ema20_4h, ema50_4h = ema([x["c"] for x in h4], 20), ema([x["c"] for x in h4], 50)
    ema20_1h, ema50_1h = ema([x["c"] for x in h1], 20), ema([x["c"] for x in h1], 50)
    close_1h = h1[-1]["c"]
    bull_votes = float(
        (2 if "BULLISH" in s1["trend"] else 0) +
        (2 if s1["bull_bos"] else 0) +
        (1 if close_1h > ema20_1h else 0) +
        (1 if ema20_1h > ema50_1h else 0)
    )
    bear_votes = float(
        (2 if "BEARISH" in s1["trend"] else 0) +
        (2 if s1["bear_bos"] else 0) +
        (1 if close_1h < ema20_1h else 0) +
        (1 if ema20_1h < ema50_1h else 0)
    )
    a1 = atr(h1)
    recent_move = (close_1h - h1[-4]["c"]) / max(a1, 1e-9)
    momentum_weight = min(1.5, abs(recent_move) * .75)
    if recent_move > 0:
        bull_votes += momentum_weight
    elif recent_move < 0:
        bear_votes += momentum_weight
    ema20_prior = ema([x["c"] for x in h1[:-3]], 20)
    ema_slope = (ema20_1h - ema20_prior) / max(a1, 1e-9)
    slope_weight = min(.75, abs(ema_slope))
    if ema_slope > 0:
        bull_votes += slope_weight
    elif ema_slope < 0:
        bear_votes += slope_weight
    if "BULLISH" in s15["trend"]:
        bull_votes += .75
    elif "BEARISH" in s15["trend"]:
        bear_votes += .75
    if bull_votes >= bear_votes + 1.25:
        effective_trend = "STRONG_BULLISH" if bull_votes >= 5 and s1["adx"] >= 25 else "BULLISH"
    elif bear_votes >= bull_votes + 1.25:
        effective_trend = "STRONG_BEARISH" if bear_votes >= 5 and s1["adx"] >= 25 else "BEARISH"
    else:
        effective_trend = "TRANSITION"
    bias_direction = ("LONG" if "BULLISH" in effective_trend else
                      "SHORT" if "BEARISH" in effective_trend else "觀察")
    impulses = {
        "LONG": bos_impulse(h1, "LONG"),
        "SHORT": bos_impulse(h1, "SHORT"),
    }
    old_main_plan = previous_main_plan(previous)
    direction, main_direction_reason, main_reversed = anchored_main_direction(
        bias_direction, close_1h, impulses, old_main_plan
    )
    four_h_aligned = ((direction == "LONG" and "BULLISH" in raw_trend_4h) or
                      (direction == "SHORT" and "BEARISH" in raw_trend_4h))
    four_h_conflict = ((direction == "LONG" and "BEARISH" in raw_trend_4h) or
                       (direction == "SHORT" and "BULLISH" in raw_trend_4h))
    context_4h = "同向加分" if four_h_aligned else "反向背景，降低信心但不封鎖" if four_h_conflict else "震盪背景"
    trend_reason = (f"市場偏向分數：多 {bull_votes:.2f}／空 {bear_votes:.2f}；"
                    f"1H {s1['trend']}、近 3H 動能 {recent_move:+.2f} ATR、ADX {s1['adx']:.1f}；"
                    f"15M {s15['trend']}；4H {raw_trend_4h}（{context_4h}）。"
                    f"主計畫：{main_direction_reason}")
    hs, ls = pivots(h1)
    live_price, closed_1h = f(ticker.get("last"), h1[-1]["c"]), h1[-1]["c"]
    impulse_meta = impulses[direction] if direction in ("LONG", "SHORT") else {
        "valid": False, "reason": "1H 方向尚未明確，未建立 Fibonacci"
    }
    fib_valid = bool(impulse_meta["valid"])
    low = impulse_meta.get("low")
    high = impulse_meta.get("high")
    confirmed_low, confirmed_high = low, high
    fib_provisional = False
    extension_price = None
    extension_time = None
    adaptive_origin = None
    adaptive_source = None
    tactical_fib = {".618": None, ".705": None, ".786": None}
    tactical_ote_low = tactical_ote_high = None
    tactical_low = tactical_high = None
    tactical_meta: dict[str, Any] | None = None
    confirmed_fib = {".618": None, ".705": None, ".786": None}
    confirmed_ote_low = confirmed_ote_high = None
    if fib_valid:
        rng = high-low
        fib = ({k: high-rng*v for k, v in {".618":.618, ".705":.705, ".786":.786}.items()} if direction == "LONG"
               else {k: low+rng*v for k, v in {".618":.618, ".705":.705, ".786":.786}.items()})
        ote_low, ote_high = min(fib[".618"], fib[".786"]), max(fib[".618"], fib[".786"])
        confirmed_fib = dict(fib)
        confirmed_ote_low, confirmed_ote_high = ote_low, ote_high
        endpoint_time = int(impulse_meta.get("endpoint_time", 0))
        after_endpoint = [x for x in h1 if x["t"] >= endpoint_time]
        same_confirmed_wave = bool(
            previous and previous.get("direction") == direction and
            previous.get("confirmed_impulse") == [round(confirmed_low, 2), round(confirmed_high, 2)]
        )
        retained_extreme = (previous.get("extension_price") if same_confirmed_wave else None)
        retained_time = (previous.get("extension_time") if same_confirmed_wave else None)
        if direction == "LONG":
            candidates = [(high, endpoint_time), (live_price, int(time.time()))]
            candidates += [(x["h"], x["t"]) for x in after_endpoint]
            if retained_extreme is not None:
                candidates.append((f(retained_extreme), int(retained_time or endpoint_time)))
            extension_price, extension_time = max(candidates, key=lambda x: x[0])
            extension_size = extension_price - high
            if extension_size >= .25*a1:
                fib_provisional = True
        else:
            candidates = [(low, endpoint_time), (live_price, int(time.time()))]
            candidates += [(x["l"], x["t"]) for x in after_endpoint]
            if retained_extreme is not None:
                candidates.append((f(retained_extreme), int(retained_time or endpoint_time)))
            extension_price, extension_time = min(candidates, key=lambda x: x[0])
            extension_size = low - extension_price
            if extension_size >= .25*a1:
                fib_provisional = True
        if fib_provisional:
            m15_highs, m15_lows = pivots(m15)
            if direction == "LONG":
                nested = [p for p in m15_lows
                          if endpoint_time <= p["t"] <= int(extension_time or time.time()) and
                          p["p"] < extension_price]
                candidate_origin = nested[-1]["p"] if nested else confirmed_low
                nested_still_valid = live_price > candidate_origin and m15[-1]["c"] >= candidate_origin
                if extension_price-candidate_origin >= .75*atr(m15) and nested_still_valid:
                    low, high = candidate_origin, extension_price
                    adaptive_origin, adaptive_source = candidate_origin, "15M 延伸子波"
                else:
                    low, high = confirmed_low, extension_price
                    adaptive_origin = confirmed_low
                    adaptive_source = ("1H 原推進波延伸（15M 子波已失效）"
                                       if nested else "1H 原推進波延伸")
            else:
                nested = [p for p in m15_highs
                          if endpoint_time <= p["t"] <= int(extension_time or time.time()) and
                          p["p"] > extension_price]
                candidate_origin = nested[-1]["p"] if nested else confirmed_high
                nested_still_valid = live_price < candidate_origin and m15[-1]["c"] <= candidate_origin
                if candidate_origin-extension_price >= .75*atr(m15) and nested_still_valid:
                    low, high = extension_price, candidate_origin
                    adaptive_origin, adaptive_source = candidate_origin, "15M 延伸子波"
                else:
                    low, high = extension_price, confirmed_high
                    adaptive_origin = confirmed_high
                    adaptive_source = ("1H 原推進波延伸（15M 子波已失效）"
                                       if nested else "1H 原推進波延伸")
            rng = high-low
            fib = ({k: high-rng*v for k, v in {".618":.618, ".705":.705, ".786":.786}.items()} if direction == "LONG"
                   else {k: low+rng*v for k, v in {".618":.618, ".705":.705, ".786":.786}.items()})
            ote_low, ote_high = min(fib[".618"], fib[".786"]), max(fib[".618"], fib[".786"])
            if adaptive_source == "15M 延伸子波":
                tactical_low, tactical_high = low, high
                tactical_fib = dict(fib)
                tactical_ote_low, tactical_ote_high = ote_low, ote_high
                tactical_meta = {
                    "valid": True, "reason": "15M 延伸子波起點與極值仍有效",
                    "low": tactical_low, "high": tactical_high,
                    "origin_time": endpoint_time,
                    "endpoint_time": int(extension_time or time.time()),
                    "bos_time": int(extension_time or time.time()),
                }
            # 延伸波不得覆蓋主計畫。以下所有主狀態、階段與掛單資料一律回到 1H 核心波。
            low, high = confirmed_low, confirmed_high
            fib = dict(confirmed_fib)
            ote_low, ote_high = confirmed_ote_low, confirmed_ote_high
    else:
        fib = {".618": None, ".705": None, ".786": None}
        ote_low = ote_high = None
    in_ote = bool(fib_valid and ote_low <= live_price <= ote_high)
    if direction == "LONG" and fib_valid:
        near_ote = ote_high < live_price <= ote_high + .35*a1
        zone_status = "位於 OTE" if in_ote else "等待回撤" if live_price > ote_high else "超深回撤"
        origin_broken = closed_1h < confirmed_low
    elif direction == "SHORT" and fib_valid:
        near_ote = ote_low - .35*a1 <= live_price < ote_low
        zone_status = "位於 OTE" if in_ote else "等待反彈" if live_price < ote_low else "超深反彈"
        origin_broken = closed_1h > confirmed_high
    else:
        near_ote, zone_status, origin_broken = False, f"等待有效 BOS 推進波：{impulse_meta['reason']}", False
    gaps = fvg(h1)
    side = "bull" if direction == "LONG" else "bear"
    active_gap = next((g for g in reversed(gaps) if fib_valid and g["side"] == side and
                       g["high"] >= ote_low and g["low"] <= ote_high), None)
    vol_med = statistics.median([x["v"] for x in m15[-21:-1]] or [1])
    rvol = m15[-1]["v"] / max(vol_med, 1e-9)
    spot_rvol = spot[-1]["v"] / max(statistics.median([x["v"] for x in spot[-21:-1]] or [1]), 1e-9)
    mss = ((direction == "LONG" and (s15["bull_bos"] or "BULLISH" in s15["trend"])) or
           (direction == "SHORT" and (s15["bear_bos"] or "BEARISH" in s15["trend"])))
    trigger_5m = ((direction == "LONG" and (s5["bull_bos"] or "BULLISH" in s5["trend"])) or
                  (direction == "SHORT" and (s5["bear_bos"] or "BEARISH" in s5["trend"])))
    risk_profile = volatility_profile(m30)
    preview_wall = (
        thirty_minute_wall(
            direction, m30, [confirmed_ote_low, confirmed_ote_high], live_price,
            bool(risk_profile["deep_retest"]),
        )
        if fib_valid and direction in ("LONG", "SHORT") else None
    )
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
    score += 5 if preview_wall else 0
    score += 10 if volume_ok else 4
    change_24h = f(ticker.get("change_percentage"))
    score += 5 if (direction == "LONG" and change_24h >= 0) or (direction == "SHORT" and change_24h <= 0) else 2
    funding = f(ticker.get("funding_rate"))
    funding_ok = not (direction == "LONG" and funding > .0008) and not (direction == "SHORT" and funding < -.0008)
    score += 7 if funding_ok else 1
    book_ok = (direction == "LONG" and imbalance > -.15) or (direction == "SHORT" and imbalance < .15)
    score += 5 if book_ok else 1
    score = min(100, score)
    main_plan = None
    if fib_valid and direction in ("LONG", "SHORT"):
        main_plan = make_trade_plan(
            "MAIN", direction, f(confirmed_low), f(confirmed_high), live_price, closed_1h,
            m30[-1], score, mss, trigger_5m, spread, h4, h1, m15,
            impulse_meta, old_main_plan, m30=m30, risk_profile=risk_profile,
        )
    elif old_main_plan and len(old_main_plan.get("impulse", [])) == 2:
        # 結構失效時仍保留最後主計畫的完整價位供核對，但禁止再使用。
        old_low, old_high = map(f, old_main_plan["impulse"])
        archived_meta = {
            "origin_time": old_main_plan.get("origin_time", 0),
            "endpoint_time": old_main_plan.get("endpoint_time", 0),
            "bos_time": old_main_plan.get("bos_time", 0),
        }
        main_plan = make_trade_plan(
            "MAIN", old_main_plan["direction"], old_low, old_high, live_price, closed_1h,
            m30[-1], score, mss, trigger_5m, spread, h4, h1, m15,
            archived_meta, old_main_plan, m30=m30, risk_profile=risk_profile,
        )
        main_plan.update(
            valid=False, status="INVALID", ready_now=False,
            action="舊主計畫價位僅供核對，已禁止掛單；等待反向 1H BOS 建立新主計畫",
            zone_status="結構已失效",
        )
    old_scalp = ((previous or {}).get("scalp_plan")
                 if isinstance((previous or {}).get("scalp_plan"), dict) else None)
    scalp_plan = None
    if tactical_meta and tactical_low is not None and tactical_high is not None:
        scalp_plan = make_trade_plan(
            "SCALP", direction, f(tactical_low), f(tactical_high), live_price, closed_1h,
            m30[-1], score, mss, trigger_5m, spread, h4, h1, m15,
            tactical_meta, old_scalp, m30=m30, risk_profile=risk_profile,
        )
    elif old_scalp and old_scalp.get("valid") and len(old_scalp.get("impulse", [])) == 2:
        scalp_plan = dict(old_scalp)
        scalp_plan.update(
            valid=False, status="INVALID", ready_now=False,
            action="短波延伸起點已被破壞；停止使用此區間、止損與止盈，主計畫不受影響",
            zone_status="短波已失效",
        )
    active = main_plan or {}
    direction = active.get("direction", direction)
    stage = int(active.get("stage", 1))
    formal = bool(active.get("ready_now"))
    invalid = not bool(active.get("valid")) if active else True
    invalid_reason = (active.get("action", "") if invalid else "")
    entry = active.get("entry", [None, None])
    public_stop = active.get("stop", 0)
    public_target_details = active.get("targets", [])
    public_targets = [x["price"] for x in public_target_details]
    public_target_rr = [x["rr"] for x in public_target_details]
    target_plan = {
        "details": public_target_details, "targets": public_targets, "rr": public_target_rr,
        "weighted_rr": active.get("weighted_rr", 0),
        "quality_ok": active.get("target_quality_ok", False),
        "reason": active.get("target_reason", "尚未建立主計畫"),
    }
    missing = list(active.get("missing", ["尚未建立有效主計畫"]))
    if fib_valid and not active_gap:
        missing.append("1H FVG 未共振（加分條件，不會刪除計畫）")
    if not volume_ok:
        missing.append("現貨／合約量能尚未同步")
    trade_status = active.get("action", "目前不可下單｜等待有效 1H BOS 主計畫")
    trade_label = "可以評估下單" if formal else "目前不可下單"
    order_advice = ("可以評估限價掛單（不是市價追單）" if formal else
                    "保留計畫，等待條件" if active and active.get("valid") else "禁止掛單")
    order_zone = (f"{entry[0]:.2f} ～ {entry[1]:.2f}"
                  if len(entry) == 2 and entry[0] is not None else "尚未建立")
    preferred_entry = active.get("preferred_entry")
    order_reason = active.get("action", "尚未建立主計畫")
    cancel_conditions = active.get(
        "cancel_condition", "等待有效 1H BOS；沒有主計畫時不得自行推測方向"
    )
    latest_high = s4["highs"][-1] if s4["highs"] else {"t": 0, "p": max(x["h"] for x in h4[-40:])}
    latest_low = s4["lows"][-1] if s4["lows"] else {"t": 0, "p": min(x["l"] for x in h4[-40:])}
    latest_1h_high = s1["highs"][-1] if s1["highs"] else {"t": 0, "p": max(x["h"] for x in h1[-40:])}
    latest_1h_low = s1["lows"][-1] if s1["lows"] else {"t": 0, "p": min(x["l"] for x in h1[-40:])}
    structure_id = hashlib.sha256(
        f"{direction}:{latest_1h_high['t']}:{latest_1h_high['p']}:{latest_1h_low['t']}:{latest_1h_low['p']}".encode()
    ).hexdigest()[:16]
    setup_id = hashlib.sha256(
        f"{direction}:{round(confirmed_low,2) if confirmed_low is not None else 'NO_FIB'}:"
        f"{round(confirmed_high,2) if confirmed_high is not None else 'NO_FIB'}:{structure_id}".encode()
    ).hexdigest()[:16]
    fib_id = active.get("id")
    setup_id = fib_id or setup_id
    fib_update_rule = (
        "同方向推進正在延伸：1H 核心 OTE 繼續保留，15M 子波只加開戰術觀察區；端點確認後才正式換波"
        if fib_provisional else
        "目前沿用最近已確認的 1H BOS 推進波；同向延伸只新增戰術觀察區，不會任意取代核心區"
        if fib_valid else
        "等待有效 1H BOS；推進終點形成後，仍須右側 3 根 1H K 收完才建立新 Fib"
    )
    notification_structure_id = hashlib.sha256(
        f"{direction}:{impulse_meta.get('bos_time', 'NO_BOS')}:{fib_id or 'NO_FIB'}".encode()
    ).hexdigest()[:16]
    return {
        "pair": PAIR, "price": round(live_price, 2), "futures_price": round(live_price, 2),
        "spot_price": round(f(spot_ticker.get("last")), 2), "mark_price": round(f(ticker.get("mark_price")), 2),
        "index_price": round(f(ticker.get("index_price")), 2), "closed_1h": round(closed_1h, 2),
        "closed_15m": round(m15[-1]["c"], 2),
        "direction": direction, "score": score, "threshold": MIN_SCORE, "stage": stage,
        "setup_id": setup_id, "structure_id": structure_id, "market_bias": effective_trend,
        "market_bias_direction": bias_direction,
        "main_direction_reason": main_direction_reason,
        "main_reversed": main_reversed,
        "main_plan": main_plan, "scalp_plan": scalp_plan,
        "notification_structure_id": notification_structure_id,
        "trend_4h": raw_trend_4h, "raw_trend_4h": raw_trend_4h,
        "trend_1h": s1["trend"], "trend_reason": trend_reason,
        "direction_votes": {"bull": bull_votes, "bear": bear_votes},
        "four_h_context": context_4h,
        "adx_4h": round(s4["adx"], 2), "bos_4h": "向上 BOS" if s4["bull_bos"] else "向下 BOS" if s4["bear_bos"] else "無新 BOS",
        "adx_1h": round(s1["adx"], 2),
        "ema_4h": {"ema20": round(ema20_4h, 2), "ema50": round(ema50_4h, 2)},
        "ema_1h": {"ema20": round(ema20_1h, 2), "ema50": round(ema50_1h, 2)},
        "last_closed": {"4h": h4[-1]["t"], "1h": h1[-1]["t"], "30m": m30[-1]["t"],
                        "15m": m15[-1]["t"], "5m": m5[-1]["t"]},
        "swing_high_4h": round(latest_high["p"], 2), "swing_low_4h": round(latest_low["p"], 2),
        "fib_valid": bool(main_plan and main_plan.get("valid")),
        "fib_reason": impulse_meta["reason"],
        "fib_id": fib_id, "fib_update_rule": fib_update_rule,
        "fib_provisional": fib_provisional,
        "extension_price": round(extension_price, 2) if extension_price is not None else None,
        "extension_time": extension_time,
        "adaptive_origin": round(adaptive_origin, 2) if adaptive_origin is not None else None,
        "adaptive_source": adaptive_source,
        "bos_impulse": impulse_meta,
        "impulse": [round(low, 2), round(high, 2)] if fib_valid else [],
        "confirmed_impulse": ([round(confirmed_low, 2), round(confirmed_high, 2)]
                              if fib_valid else []),
        "fib": (main_plan.get("fib") if main_plan else
                {k: round(v, 2) if v is not None else None for k, v in fib.items()}),
        "confirmed_fib": {k: round(v, 2) if v is not None else None
                          for k, v in confirmed_fib.items()},
        "ote": main_plan.get("zone", []) if main_plan else [],
        "confirmed_ote": ([round(confirmed_ote_low, 2), round(confirmed_ote_high, 2)]
                          if fib_valid else []),
        "core_ote": ([round(confirmed_ote_low, 2), round(confirmed_ote_high, 2)]
                     if fib_valid else []),
        "tactical_ote": scalp_plan.get("zone", []) if scalp_plan else [],
        "zone_status": active.get("zone_status", zone_status),
        "in_ote": bool(active.get("in_zone_now")), "fvg": active_gap, "mss_15m": mss,
        "trigger_5m": trigger_5m, "protected_15m": round(protected, 2) if protected else None,
        "wall_confirmed_30m": bool(active.get("wall_confirmed_30m")),
        "entry_wall_30m": active.get("entry_wall"),
        "volatility_profile": risk_profile,
        "rvol_15m": round(rvol, 2), "spot_rvol": round(spot_rvol, 2),
        "funding_rate": funding, "change_24h": change_24h,
        "high_24h": f(ticker.get("high_24h")), "low_24h": f(ticker.get("low_24h")),
        "oi_contracts": f(ticker.get("total_size")), "oi_eth_estimate": round(f(ticker.get("total_size"))*f(ticker.get("quanto_multiplier")), 2),
        "orderbook_imbalance": round(imbalance, 3), "spread": round(spread, 4),
        "entry": [round(x, 2) if x is not None else None for x in entry], "stop": public_stop,
        "targets": public_targets, "target_rr": public_target_rr,
        "target_details": public_target_details,
        "weighted_rr": target_plan["weighted_rr"], "target_quality_ok": target_plan["quality_ok"],
        "target_reason": target_plan["reason"], "missing": missing,
        "invalid": invalid, "invalid_reason": invalid_reason, "formal": formal,
        "trade_status": trade_status, "trade_label": trade_label,
        "order_advice": order_advice, "order_zone": order_zone,
        "preferred_entry": preferred_entry, "order_reason": order_reason,
        "cancel_conditions": cancel_conditions,
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
    def plan_text(plan: dict | None) -> str:
        if not plan:
            return "目前沒有有效計畫；等待已收 K 結構建立。"
        details = plan.get("targets", [])
        target_lines = "\n".join(
            f"TP{i+1} `{n(x['price'])}` · `{f(x['rr']):.2f}R` · "
            f"{x['type']} · `{x['allocation']}%`"
            for i, x in enumerate(details)
        ) or "尚無可用止盈"
        zone = plan.get("zone", [])
        zone_text = f"{n(zone[0])}–{n(zone[1])}" if len(zone) == 2 else "—"
        return (
            f"**{plan.get('direction', '—')}｜L{plan.get('stage', 1)}｜"
            f"{plan.get('status', '—')}**\n"
            f"30M 牆位掛單區 `{zone_text}`｜優先 `{n(plan.get('preferred_entry'))}`\n"
            f"止損 `{n(plan.get('stop'))}`｜{plan.get('cancel_condition', '—')}\n"
            f"{target_lines}\n區域：{plan.get('zone_status', '—')}"
        )

    main_plan = result.get("main_plan")
    scalp_plan = result.get("scalp_plan")
    prices = (f"合約 `{n(result['futures_price'])}`　現貨 `{n(result['spot_price'])}`\n"
              f"標記 `{n(result['mark_price'])}`　指數 `{n(result['index_price'])}`\n"
              f"24H `{result['change_24h']:+.2f}%`｜高 `{n(result['high_24h'])}`｜低 `{n(result['low_24h'])}`")
    structure_text = (f"主計畫方向 **{result['direction']}**｜即時市場偏向 "
                      f"**{result.get('market_bias_direction', '觀察')}**"
                      f"（{result.get('market_bias', result['trend_1h'])}）\n"
                      f"{result.get('main_direction_reason', '')}\n"
                      f"4H 輔助 `{result['trend_4h']}`／ADX `{result['adx_4h']}`／{result.get('four_h_context', '—')}｜"
                      f"1H `{result.get('trend_1h', '—')}`／ADX `{result.get('adx_1h', 0)}`\n"
                      f"{result['bos_4h']}｜4H Swing `{n(result['swing_low_4h'])}–{n(result['swing_high_4h'])}`\n"
                      f"收 K：4H `{closed_time(result['last_closed']['4h'])}`｜1H `{closed_time(result['last_closed']['1h'])}`")
    bos = result.get("bos_impulse", {})
    confirm = (f"BOS Level `{n(bos.get('bos_level'))}`｜BOS Close `{n(bos.get('bos_close'))}`｜"
               f"Body `{f(bos.get('bos_body_ratio'))*100:.1f}%`｜Volume `{f(bos.get('bos_volume_ratio')):.2f}x`\n"
               f"30M 牆位 `{'✅' if result['wall_confirmed_30m'] else '⏳'}`｜"
               f"15M MSS `{'✅' if result['mss_15m'] else '⏳'}`｜"
               f"分數 `{result['score']}/{result.get('threshold', MIN_SCORE)}`\n"
               f"波動型態 `{result['volatility_profile']['label']}`｜"
               f"30M ATR `{result['volatility_profile']['atr_pct']*100:.2f}%`\n"
               f"合約 RVOL `{result['rvol_15m']}`｜現貨 RVOL `{result['spot_rvol']}`")
    derivatives = (f"資金費率 `{result['funding_rate']*100:.4f}%`｜OI 約 `{n(result['oi_eth_estimate'])} ETH`\n"
                   f"訂單簿失衡 `{result['orderbook_imbalance']:+.3f}`｜Spread `{result['spread']:.4f}`")
    missing = "\n".join(f"• {x}" for x in result["missing"]) or "✅ 目前必要條件已齊備"
    description = (f"**主計畫 {result['direction']}｜L{result['stage']}｜"
                   f"{result.get('trade_label', '目前不可下單')}**\n"
                   f"📌 **現在要做什麼：{result.get('trade_status', '等待')}**")
    if event_note:
        description += f"\n{event_note}"
    fields = [
        {"name": "💹 掃描當下即時價格", "value": prices, "inline": False},
        {"name": "🧭 主方向與已收 K 結構", "value": structure_text, "inline": False},
        {"name": "🏙️ 主計畫（價位持續保留）", "value": plan_text(main_plan)[:1024], "inline": False},
        {"name": "⚡ 短波延伸計畫（獨立管理）", "value": plan_text(scalp_plan)[:1024], "inline": False},
        {"name": "🧩 觸發條件", "value": confirm, "inline": False},
        {"name": "📊 衍生品與訂單簿", "value": derivatives, "inline": False},
        {"name": "⏳ 尚缺條件／風險", "value": missing[:1000], "inline": False},
    ]
    return description, fields


async def send_discord(payload: dict) -> bool:
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
            return False
        response.raise_for_status()
        return True


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
    con.close()
    payload = {"username": "ETH SMC／ICT Scanner", "embeds": [{
        "title": title, "description": description, "color": color,
        "fields": fields, "timestamp": datetime.now(timezone.utc).isoformat(),
        "footer": {"text": "Gate ETH_USDT｜每 60 秒掃描｜結構只採已收 K｜不會自動下單"},
    }]}
    sent = await send_discord(payload)
    if not sent:
        return False
    con = db()
    con.execute("INSERT OR IGNORE INTO alerts(setup_id,level,ts,message) VALUES(?,?,?,?)",
                (alert_id, event_code, int(time.time()), msg))
    con.commit()
    con.close()
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
    stop = round(stop, 2)
    liquidity = ict_targets(side, entry_price, stop, live_price, h4, h1, m15)
    details = complete_target_plan(side, entry_price, stop, liquidity)
    weighted_rr = (
        sum(x["rr"]*x["allocation"] for x in details) /
        max(sum(x["allocation"] for x in details), 1)
    )
    return stop, {
        **liquidity,
        "details": details,
        "targets": [x["price"] for x in details],
        "rr": [x["rr"] for x in details],
        "weighted_rr": round(weighted_rr, 2),
        "quality_ok": True,
        "reason": "近端分批止盈：0.55R／0.85R／1.15R／1.50R，TP1 優先落袋 40%",
        "target_plan_version": TARGET_PLAN_VERSION,
    }


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
                   f"30M 牆位 `{'✅' if result['wall_confirmed_30m'] else '—'}`｜"
                   f"15M MSS `{'✅' if result['mss_15m'] else '—'}`\n"
                   f"訂單簿失衡 `{result['orderbook_imbalance']:+.3f}`"), "inline": False},
        {"name": "🎯 原持倉 ICT 出場計畫", "value": target_lines[:1024], "inline": False},
    ]
    msg = (f"【{title}】\n{decision}\n{position['side']}｜進場 {position['entry_price']}｜"
           f"即時 {current}｜{pnl_pct:+.2f}%｜{pnl_r:+.2f}R")
    con.close()
    sent = await send_discord({"username": "ETH 持倉風控", "embeds": [{
        "title": title, "description": description, "color": color, "fields": fields,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "footer": {"text": "只提供出場風險提示，不會自動平倉；完整出場後請輸入：已出場"},
    }]})
    if not sent:
        return False
    con = db()
    con.execute("INSERT OR IGNORE INTO alerts(setup_id,level,ts,message) VALUES(?,?,?,?)",
                (alert_id, event_code, int(time.time()), msg))
    con.commit()
    con.close()
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
            "target_plan_version": target_plan["target_plan_version"],
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
    if int(position.get("target_plan_version", 0)) != TARGET_PLAN_VERSION:
        liquidity = ict_targets(
            side, position["entry_price"], position["initial_stop"], current, h4, h1, m15
        )
        position["targets"] = complete_target_plan(
            side, position["entry_price"], position["initial_stop"], liquidity
        )
        position["target_plan_version"] = TARGET_PLAN_VERSION
        position["target_reason"] = (
            "已更新為近端分批止盈：0.55R／0.85R／1.15R／1.50R，TP1 優先落袋 40%"
        )
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
        if i == 0:
            entry = f(position["entry_price"])
            old_trailing = position.get("trailing_level")
            position["trailing_level"] = (
                max(f(old_trailing), entry) if side == "LONG" and old_trailing is not None else
                min(f(old_trailing), entry) if side == "SHORT" and old_trailing is not None else
                entry
            )
        await emit_position_alert(
            position, result, f"TP{i+1}_HIT",
            f"🎯 ETH TP{i+1} 已觸及｜{'建議完整出場' if is_final else '建議分批出場'}",
            (f"已到達 {target['type']}；建議出場 {target['allocation']}%。"
             + ("這是最後一段止盈，建議完整出場。" if is_final else
                "TP1 後剩餘部位保護位推進到進場價。" if i == 0 else
                "剩餘部位依 15M 保護結構管理。")),
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
    current_notice_id = result.get("notification_structure_id", result["structure_id"])
    previous_notice_id = ((previous or {}).get("notification_structure_id") or
                          (previous or {}).get("structure_id"))
    new_structure = not previous or previous_notice_id != current_notice_id
    direction_changed = bool(previous and previous.get("direction") not in (None, "觀察") and
                             previous.get("direction") != result["direction"])
    fib_changed = bool(previous and result.get("fib_valid") and
                       previous.get("fib_id") != result.get("fib_id"))
    current_scalp = result.get("scalp_plan") if isinstance(result.get("scalp_plan"), dict) else None
    previous_scalp = ((previous or {}).get("scalp_plan")
                      if isinstance((previous or {}).get("scalp_plan"), dict) else None)
    adaptive_started = bool(current_scalp and current_scalp.get("valid") and
                            not (previous_scalp or {}).get("valid"))
    subwave_invalidated = bool(
        previous_scalp and previous_scalp.get("valid") and
        (not current_scalp or not current_scalp.get("valid"))
    )
    protected_broken = False
    if (previous and previous.get("fib_valid") and previous.get("in_ote") and
            int(previous.get("stage", 1)) >= 3 and previous.get("protected_15m") and
            previous.get("mss_15m")):
        protected_broken = (
            previous["direction"] == "LONG" and result["closed_15m"] < previous["protected_15m"]
        ) or (
            previous["direction"] == "SHORT" and result["closed_15m"] > previous["protected_15m"]
        )
    actionable_direction_change = bool(direction_changed and (previous or {}).get("fib_valid"))
    became_invalid = bool(result["invalid"] and not (previous or {}).get("invalid") and
                          (previous or {}).get("fib_valid"))
    if actionable_direction_change or became_invalid:
        reasons = []
        if actionable_direction_change:
            reasons.append(f"舊主計畫 {previous['direction']} 已失效，新主計畫為 {result['direction']}")
        if result["invalid_reason"]:
            reasons.append(result["invalid_reason"])
        old_id = (previous or result).get("setup_id", result["setup_id"])
        await emit_alert(result, "INVALID", "⛔ ETH 舊主計畫失效／新方向檢查", 0xE74C3C,
                         "；".join(reasons), old_id)
    if protected_broken and not (actionable_direction_change or became_invalid):
        await emit_alert(
            result, "TRIGGER_RESET", "⚠️ ETH 15M 確認失效｜主計畫價位仍保留",
            0xF39C12,
            f"15M 收盤破壞前一保護結構 `{n(previous['protected_15m'])}`；"
            "只重置小週期確認，不反轉、不刪除主計畫，等待新的 30M 牆位／15M MSS。",
            result.get("setup_id"),
        )
    if new_structure and not (fib_changed or adaptive_started or subwave_invalidated or
                              actionable_direction_change or protected_broken or became_invalid):
        await emit_alert(result, "NEW_STRUCTURE",
                         f"🆕 ETH L{result['stage']} 已收 K 新結構｜{result['trade_label']}", 0x3498DB,
                         f"新結構編號 `{result['structure_id']}`；主計畫仍以計畫 ID `{result['setup_id']}` 管理，不會因短線擺動翻向。",
                         result["structure_id"])
    if fib_changed:
        old_ote = previous.get("ote", [])
        old_text = f"{n(old_ote[0])}–{n(old_ote[1])}" if len(old_ote) == 2 else "尚未建立"
        new_ote = result.get("ote", [])
        new_text = f"{n(new_ote[0])}–{n(new_ote[1])}" if len(new_ote) == 2 else "尚未建立"
        await emit_alert(
            result, "FIB_ROLLOVER", "🔄 ETH Fibonacci 已確認換波｜舊區間停止沿用",
            0x9B59B6, f"舊 OTE `{old_text}` → 新 OTE `{new_text}`；新終點已由右側 3 根 1H K 確認。",
            result["fib_id"],
        )
    elif adaptive_started:
        old_ote = (result.get("main_plan") or {}).get("zone", [])
        old_text = f"{n(old_ote[0])}–{n(old_ote[1])}" if len(old_ote) == 2 else "尚未建立"
        new_ote = current_scalp.get("zone", []) if current_scalp else []
        new_text = f"{n(new_ote[0])}–{n(new_ote[1])}" if len(new_ote) == 2 else "尚未建立"
        await emit_alert(
            result, "ADAPTIVE_EXTENSION", "⚡ ETH 同向推進延伸｜新增戰術觀察區",
            0xF39C12,
            f"主計畫掛單區 `{old_text}` 繼續保留；短波延伸計畫 `{new_text}` 已獨立建立，"
            f"止損 `{n(current_scalp.get('stop'))}` 與 TP1～TP4 已附上。這不是反向換波。",
            current_scalp.get("id") or result["setup_id"],
        )
    if subwave_invalidated and not fib_changed:
        old_ote = previous_scalp.get("zone", [])
        old_text = f"{n(old_ote[0])}–{n(old_ote[1])}" if len(old_ote) == 2 else "尚未建立"
        new_ote = (result.get("main_plan") or {}).get("zone", [])
        new_text = f"{n(new_ote[0])}–{n(new_ote[1])}" if len(new_ote) == 2 else "尚未建立"
        old_impulse = previous_scalp.get("impulse", [])
        old_origin = (old_impulse[0] if previous_scalp.get("direction") == "LONG" else old_impulse[1]
                      if len(old_impulse) == 2 else None)
        await emit_alert(
            result, "SUBWAVE_INVALIDATED", "⛔ ETH 15M 延伸子波失效｜舊止損止盈全部作廢",
            0xE74C3C,
            f"價格已突破子波起點 `{n(old_origin)}`；戰術觀察區 `{old_text}` 不得再使用。"
            f"1H 核心區仍有效；目前高階觀察區 `{new_text}`，等待新的 L4。",
            f"{previous_scalp.get('id') or result['setup_id']}:{n(old_origin)}",
        )
    previous_stage = int((previous or {}).get("stage", 0))
    if result["stage"] > previous_stage or not previous:
        title, color = stage_titles[result["stage"]]
        await emit_alert(result, f"L{result['stage']}",
                         f"{title}｜{result['trade_label']}", color)
    elif previous and previous.get("formal") and not result["formal"]:
        await emit_alert(result, "CONDITION_LOST", "⚠️ ETH 正式條件已退回｜目前不可下單", 0xF39C12,
                         "原 L4 條件已不再成立，請勿沿用先前進場計畫。")
    if current_scalp and current_scalp.get("valid"):
        old_scalp_stage = int((previous_scalp or {}).get("stage", 0))
        if int(current_scalp.get("stage", 1)) > old_scalp_stage:
            await emit_alert(
                result, f"SCALP_L{current_scalp['stage']}",
                f"⚡ ETH 短波延伸 L{current_scalp['stage']}｜{current_scalp['direction']}",
                0x9B59B6, f"現在要做什麼：{current_scalp['action']}",
                current_scalp["id"],
            )


async def scan() -> None:
    async with httpx.AsyncClient(timeout=20, headers={"Accept": "application/json"}) as client:
        tasks = [
            gate(client, "/futures/usdt/candlesticks", contract=PAIR, interval=interval, limit=limit)
            for interval, limit in (
                ("4h", 300), ("1h", 500), ("30m", 800), ("15m", 1000), ("5m", 400)
            )
        ]
        tasks += [
            gate(client, "/spot/candlesticks", currency_pair=PAIR, interval="15m", limit=80),
            gate(client, "/futures/usdt/tickers", contract=PAIR),
            gate(client, "/futures/usdt/order_book", contract=PAIR, limit=20),
            gate(client, "/spot/tickers", currency_pair=PAIR),
        ]
        raw = await asyncio.gather(*tasks)
    h4, h1, m30, m15, m5 = [candles(x) for x in raw[:5]]
    spot = candles(raw[5], False)
    ticker = raw[6][0] if isinstance(raw[6], list) else raw[6]
    spot_ticker = raw[8][0] if isinstance(raw[8], list) else raw[8]
    if min(map(len, (h4, h1, m30, m15, m5, spot))) < 30:
        raise RuntimeError("Gate K 線資料不足")
    now = int(time.time())
    con = db()
    prior = con.execute("SELECT payload FROM snapshots ORDER BY ts DESC,id DESC LIMIT 1").fetchone()
    previous = json.loads(prior["payload"]) if prior else None
    result = setup(
        h4, h1, m15, m5, spot, ticker, spot_ticker, raw[7], previous, m30=m30
    )
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


app = FastAPI(title="ETH Trading Secretary", version="3.1.0", lifespan=lifespan)


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
.grid{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:14px;margin-top:22px}.card{min-width:0;background:linear-gradient(145deg,#12233a,#0d1929);border:1px solid var(--line);border-radius:14px;padding:18px;box-shadow:0 12px 40px #0004}
.wide{grid-column:span 2}.full{grid-column:1/-1}.label{color:var(--muted);font-size:12px;text-transform:uppercase;letter-spacing:.12em}.big{font:700 28px ui-monospace;margin-top:8px}.cyan{color:var(--cyan)}.gold{color:var(--gold)}
.bar{height:8px;background:#26384d;border-radius:8px;overflow:hidden;margin-top:12px}.bar i{display:block;height:100%;background:linear-gradient(90deg,var(--cyan),var(--gold))}
table{width:100%;border-collapse:collapse;margin-top:9px}td{padding:8px;border-bottom:1px solid var(--line);vertical-align:top}td:last-child{text-align:right;font-family:monospace;overflow-wrap:anywhere}.action{margin-top:10px;padding:10px;border-left:3px solid var(--gold);background:#091525;color:#fff;line-height:1.55}
button{background:var(--cyan);border:0;border-radius:8px;padding:9px 14px;font-weight:700;cursor:pointer}@media(max-width:760px){.grid{grid-template-columns:repeat(2,minmax(0,1fr))}.wide{grid-column:span 2}}
</style></head><body><main><div class="top"><div><div class="label">GATE · ETH_USDT PERPETUAL</div><h1>ETH 脈衝掃描器</h1><div class="sub">SMC / ICT · 僅訊號，不執行交易</div></div><button onclick="scan()">立即掃描</button></div>
<section class="grid"><div class="card"><div class="label">Gate 合約即時價格</div><div id="price" class="big cyan">—</div><div id="priceDetail" class="muted"></div></div>
<div class="card"><div class="label">主計畫 / 階段 / 交易判定</div><div id="direction" class="big">—</div><div id="tradeStatus" class="action"></div></div>
<div class="card"><div class="label">綜合分數</div><div id="score" class="big gold">—</div><div class="bar"><i id="scorebar"></i></div></div>
<div class="card"><div class="label">資料狀態</div><div id="health" class="big">—</div></div>
<div class="card wide"><div class="label">🏙️ 主計畫（固定保存至結構失效）</div><table id="mainPlan"></table></div>
<div class="card wide"><div class="label">已收 K 結構與即時衍生資料</div><table id="conditions"></table></div>
<div class="card wide"><div class="label">⚡ 短波延伸計畫（獨立階段）</div><table id="scalpPlan"></table></div>
<div class="card wide"><div class="label">尚缺條件</div><div id="missing" style="margin-top:14px;line-height:1.8"></div></div>
<div class="card full"><div class="label">Discord 持倉監控模式</div><div id="positionInfo" style="margin-top:12px;line-height:1.8"></div></div>
<div class="card full muted" id="updated">等待第一輪 Gate 資料…</div></section></main>
<script>
const fmt=n=>Number(n).toLocaleString('zh-TW',{maximumFractionDigits:2});
const rows=o=>Object.entries(o).map(([k,v])=>`<tr><td>${k}</td><td>${v}</td></tr>`).join('');
const targetText=p=>(p?.targets||[]).map((x,i)=>`TP${i+1} ${x.price}（${x.rr}R · ${x.type} · ${x.allocation}%）`).join('<br>')||'—';
const planRows=(p,emptyText)=>!p?rows({'狀態':emptyText,'現在要做什麼':'等待已收 K 建立計畫'}):rows({
'現在要做什麼':p.action||'—','方向 / 生命週期階段':`${p.direction||'—'} / L${p.stage||1}（目前條件 L${p.current_stage||1}）`,
'計畫狀態':`${p.status||'—'} / ${p.zone_status||'—'}`,'掛單區間（持續保留）':p.zone?.length===2?p.zone.join(' ～ '):'—',
'優先掛單價':p.preferred_entry??'—','結構止損（持續保留）':p.stop??'—','分批止盈（持續保留）':targetText(p),
'取消 / 失效條件':p.cancel_condition||'—','已觸區':p.zone_reached?'是':'否','目前可評估掛單':p.ready_now?'是':'否',
'ICT 目標品質':`${p.weighted_rr??0}R / ${p.target_quality_ok?'合格':'含 R 倍數備援'}`});
async function load(){let s=await fetch('/api/status').then(r=>r.json()),a=s.analysis||{};price.textContent=a.futures_price?`$ ${fmt(a.futures_price)}`:'—';priceDetail.textContent=a.spot_price?`現貨 ${fmt(a.spot_price)} · 標記 ${fmt(a.mark_price)} · 指數 ${fmt(a.index_price)}`:'';
direction.textContent=(a.direction||'—')+' · L'+(a.stage||0);tradeStatus.textContent='現在要做什麼：'+(a.trade_status||'等待');
score.textContent=(a.score??'—')+'/100';scorebar.style.width=(a.score||0)+'%';health.textContent=s.status;
mainPlan.innerHTML=planRows(a.main_plan,'尚未建立有效主計畫');
conditions.innerHTML=rows({'主計畫方向 / 即時市場偏向':(a.direction||'—')+' / '+(a.market_bias_direction||'觀察'),'方向鎖定依據':a.main_direction_reason||'—','4H 輔助背景':(a.trend_4h||'—')+' / '+(a.four_h_context||'—'),'4H Swing / BOS':(a.raw_trend_4h||a.trend_4h||'—')+' / '+(a.bos_4h||'—'),'1H 趨勢':`${a.trend_1h||'—'} / ADX ${a.adx_1h??'—'}`,'1H 多空票數':a.direction_votes?`多 ${Number(a.direction_votes.bull).toFixed(2)} / 空 ${Number(a.direction_votes.bear).toFixed(2)}`:'—','4H EMA20 / 50':a.ema_4h?`${a.ema_4h.ema20} / ${a.ema_4h.ema50}`:'—','30M 支撐／壓力牆':a.wall_confirmed_30m?'已確認，限價等待回踩':'等待成形','30M 波動型態':a.volatility_profile?`${a.volatility_profile.label} / ATR ${(Number(a.volatility_profile.atr_pct)*100).toFixed(2)}%`:'—','15M MSS':a.mss_15m?'已收 K 確認':'等待','15M / 現貨 RVOL':`${a.rvol_15m??'—'} / ${a.spot_rvol??'—'}`,'資金費率':a.funding_rate!=null?(a.funding_rate*100).toFixed(4)+'%':'—','OI 約當 ETH':a.oi_eth_estimate?fmt(a.oi_eth_estimate):'—','訂單簿失衡 / Spread':`${a.orderbook_imbalance??'—'} / ${a.spread??'—'}`});
scalpPlan.innerHTML=planRows(a.scalp_plan,'目前沒有有效延伸短波；主計畫照常保留');
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
