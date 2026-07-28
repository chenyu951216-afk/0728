"""ETH_USDT Gate 公開資料掃描器：只產生研究訊號，不下單。"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import math
import os
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
TAIPEI = ZoneInfo(os.getenv("TZ", "Asia/Taipei"))
LOG = logging.getLogger("eth-scanner")
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(message)s")

state: dict[str, Any] = {
    "status": "啟動中", "updated_at": None, "error": None, "ws": "REST 定時掃描",
    "analysis": {}, "data_quality": 0, "scan_count": 0,
}


def db() -> sqlite3.Connection:
    con = sqlite3.connect(DB_PATH, timeout=15)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("""CREATE TABLE IF NOT EXISTS snapshots(
        id INTEGER PRIMARY KEY, ts INTEGER NOT NULL, price REAL NOT NULL,
        score INTEGER NOT NULL, direction TEXT NOT NULL, payload TEXT NOT NULL)""")
    con.execute("CREATE INDEX IF NOT EXISTS ix_snapshots_ts ON snapshots(ts)")
    con.execute("""CREATE TABLE IF NOT EXISTS alerts(
        setup_id TEXT NOT NULL, level INTEGER NOT NULL, ts INTEGER NOT NULL,
        message TEXT NOT NULL, PRIMARY KEY(setup_id, level))""")
    con.commit()
    return con


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


def setup(h4: list[dict], h1: list[dict], m15: list[dict], m5: list[dict],
          spot: list[dict], ticker: dict, book: dict) -> dict:
    s4, s15 = structure(h4), structure(m15)
    direction = "LONG" if "BULLISH" in s4["trend"] else "SHORT" if "BEARISH" in s4["trend"] else "觀察"
    hs, ls = pivots(h1)
    price, a1 = h1[-1]["c"], atr(h1)
    if direction == "LONG" and hs and ls:
        low = next((x["p"] for x in reversed(ls) if x["i"] < hs[-1]["i"]), ls[-1]["p"])
        high = hs[-1]["p"]
    elif direction == "SHORT" and hs and ls:
        high = next((x["p"] for x in reversed(hs) if x["i"] < ls[-1]["i"]), hs[-1]["p"])
        low = ls[-1]["p"]
    else:
        low, high = min(x["l"] for x in h1[-40:]), max(x["h"] for x in h1[-40:])
    rng = max(high-low, 1e-9)
    fib = ({k: high-rng*v for k, v in {".618":.618, ".705":.705, ".786":.786}.items()} if direction == "LONG"
           else {k: low+rng*v for k, v in {".618":.618, ".705":.705, ".786":.786}.items()})
    ote_low, ote_high = min(fib[".618"], fib[".786"]), max(fib[".618"], fib[".786"])
    in_ote = ote_low <= price <= ote_high
    near_ote = ote_low-.35*a1 <= price <= ote_high+.35*a1
    gaps = fvg(h1)
    side = "bull" if direction == "LONG" else "bear"
    active_gap = next((g for g in reversed(gaps) if g["side"] == side and g["low"]-.1*a1 <= price <= g["high"]+.1*a1), None)
    vol_med = statistics.median([x["v"] for x in m15[-21:-1]] or [1])
    rvol = m15[-1]["v"] / max(vol_med, 1e-9)
    spot_rvol = spot[-1]["v"] / max(statistics.median([x["v"] for x in spot[-21:-1]] or [1]), 1e-9)
    mss = (direction == "LONG" and (s15["bull_bos"] or "BULLISH" in s15["trend"])) or (direction == "SHORT" and (s15["bear_bos"] or "BEARISH" in s15["trend"]))
    bids, asks = book.get("bids", []), book.get("asks", [])
    bid_qty = sum(abs(f(x.get("s"))) for x in bids[:20])
    ask_qty = sum(abs(f(x.get("s"))) for x in asks[:20])
    imbalance = (bid_qty-ask_qty) / max(bid_qty+ask_qty, 1)
    volume_ok = rvol >= .85 and spot_rvol >= .7
    score = 0
    score += 18 if direction != "觀察" else 5
    score += 8 if s4["bull_bos"] or s4["bear_bos"] else 3
    score += 13 if in_ote else 8 if near_ote else 0
    score += 12 if active_gap else 4 if gaps else 0
    score += 15 if mss else 0
    score += 10 if volume_ok else 4
    oi_change = f(ticker.get("change_percentage"))
    score += 7 if (direction == "LONG" and oi_change >= 0) or (direction == "SHORT" and oi_change <= 0) else 3
    funding = f(ticker.get("funding_rate"))
    funding_ok = not (direction == "LONG" and funding > .0008) and not (direction == "SHORT" and funding < -.0008)
    score += 7 if funding_ok else 1
    book_ok = (direction == "LONG" and imbalance > -.15) or (direction == "SHORT" and imbalance < .15)
    score += 5 if book_ok else 1
    score = min(100, score)
    entry = [active_gap["low"], active_gap["high"]] if active_gap else [ote_low, ote_high]
    buffer = max(.12*atr(m15), price*.0005)
    if direction == "LONG":
        local_lows = [x["p"] for x in s15["lows"] if x["p"] < min(entry)]
        stop = min(([low] if low < min(entry) else []) + local_lows + [min(entry)-buffer]) - buffer
        targets = sorted({x["p"] for x in hs if x["p"] > max(entry)})[:4]
    else:
        local_highs = [x["p"] for x in s15["highs"] if x["p"] > max(entry)]
        stop = max(([high] if high > max(entry) else []) + local_highs + [max(entry)+buffer]) + buffer
        targets = sorted({x["p"] for x in ls if x["p"] < min(entry)}, reverse=True)[:4]
    risk = abs(sum(entry)/2-stop)
    rr = [round(abs(x-sum(entry)/2)/risk, 2) for x in targets] if risk else []
    stage = 4 if score >= MIN_SCORE and in_ote and mss else 3 if in_ote else 2 if near_ote else 1
    if direction == "觀察":
        stage = 1
    missing = []
    if direction == "觀察": missing.append("4H 方向尚未明確")
    if not in_ote: missing.append("尚未進入 OTE")
    if not active_gap: missing.append("1H FVG 未共振")
    if not mss: missing.append("15M MSS 尚未確認")
    if not volume_ok: missing.append("量能尚未確認")
    if not rr or max(rr) < 1.5:
        missing.append("前方流動性目標盈虧比不足")
        stage = min(stage, 3)
    return {
        "pair": PAIR, "price": price, "direction": direction, "score": score, "threshold": MIN_SCORE,
        "stage": stage, "trend_4h": s4["trend"], "adx_4h": round(s4["adx"], 2),
        "fib": {k: round(v, 2) for k, v in fib.items()}, "ote": [round(ote_low, 2), round(ote_high, 2)],
        "in_ote": in_ote, "fvg": active_gap, "mss_15m": mss, "rvol_15m": round(rvol, 2),
        "spot_rvol": round(spot_rvol, 2), "funding_rate": funding, "oi_proxy_24h_change": oi_change,
        "orderbook_imbalance": round(imbalance, 3), "entry": [round(x, 2) for x in entry],
        "stop": round(stop, 2), "targets": [round(x, 2) for x in targets], "target_rr": rr,
        "missing": missing, "formal": stage == 4, "risk_note": "僅供研究，不會自動下單",
    }


async def gate(client: httpx.AsyncClient, path: str, **params: Any) -> Any:
    r = await client.get(API + path, params=params)
    r.raise_for_status()
    return r.json()


async def notify(result: dict) -> None:
    setup_id = hashlib.sha256(f"{PAIR}:{result['direction']}:{result['ote']}".encode()).hexdigest()[:16]
    level = result["stage"]
    title = ["", "趨勢建立", "接近 OTE", "進入監控區", "交易條件確認"][level]
    tw = datetime.now(TAIPEI).strftime("%Y-%m-%d %H:%M")
    msg = (f"【ETH ICT {title}】\n台灣時間：{tw}\n方向：{result['direction']}\n"
           f"綜合分數：{result['score']}/100\n4H 趨勢：{result['trend_4h']}\n"
           f"OTE：{result['ote'][0]}～{result['ote'][1]}\n15M MSS：{'是' if result['mss_15m'] else '否'}\n"
           f"參考進場：{result['entry'][0]}～{result['entry'][1]}\n結構止損：{result['stop']}\n"
           f"目標：{result['targets'] or '等待有效流動性目標'}\n缺少條件：{'、'.join(result['missing']) or '無'}\n"
           "風險提示：量化市場訊號僅供研究，不保證獲利，請自行決定是否交易。")
    con = db()
    exists = con.execute("SELECT 1 FROM alerts WHERE setup_id=? AND level=?", (setup_id, level)).fetchone()
    if not exists:
        con.execute("INSERT INTO alerts VALUES(?,?,?,?)", (setup_id, level, int(time.time()), msg))
        con.commit()
        if DISCORD:
            async with httpx.AsyncClient(timeout=10) as client:
                response = await client.post(DISCORD, json={"content": msg})
                response.raise_for_status()
    con.close()


async def scan() -> None:
    async with httpx.AsyncClient(timeout=20, headers={"Accept": "application/json"}) as client:
        tasks = [
            gate(client, "/futures/usdt/candlesticks", contract=PAIR, interval=x, limit=220)
            for x in ("4h", "1h", "15m", "5m")
        ]
        tasks += [
            gate(client, "/spot/candlesticks", currency_pair=PAIR, interval="15m", limit=80),
            gate(client, "/futures/usdt/tickers", contract=PAIR),
            gate(client, "/futures/usdt/order_book", contract=PAIR, limit=20),
        ]
        raw = await asyncio.gather(*tasks)
    h4, h1, m15, m5 = [candles(x) for x in raw[:4]]
    spot = candles(raw[4], False)
    ticker = raw[5][0] if isinstance(raw[5], list) else raw[5]
    if min(map(len, (h4, h1, m15, m5, spot))) < 30:
        raise RuntimeError("Gate K 線資料不足")
    result = setup(h4, h1, m15, m5, spot, ticker, raw[6])
    now = int(time.time())
    con = db()
    con.execute("INSERT INTO snapshots(ts,price,score,direction,payload) VALUES(?,?,?,?,?)",
                (now, result["price"], result["score"], result["direction"], json.dumps(result, ensure_ascii=False)))
    con.execute("DELETE FROM snapshots WHERE ts < ?", (now - 90*86400,))
    con.commit(); con.close()
    state.update(status="正常", updated_at=datetime.now(timezone.utc).isoformat(), error=None,
                 analysis=result, data_quality=100, scan_count=state["scan_count"]+1)
    await notify(result)


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


app = FastAPI(title="ETH SMC/ICT Scanner", version="1.0.0", lifespan=lifespan)


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
<section class="grid"><div class="card"><div class="label">即時價格</div><div id="price" class="big cyan">—</div></div>
<div class="card"><div class="label">方向 / 階段</div><div id="direction" class="big">—</div></div>
<div class="card"><div class="label">綜合分數</div><div id="score" class="big gold">—</div><div class="bar"><i id="scorebar"></i></div></div>
<div class="card"><div class="label">資料狀態</div><div id="health" class="big">—</div></div>
<div class="card wide"><div class="label">Fibonacci / OTE</div><table id="fib"></table></div>
<div class="card wide"><div class="label">訊號條件</div><table id="conditions"></table></div>
<div class="card wide"><div class="label">參考計畫</div><table id="plan"></table></div>
<div class="card wide"><div class="label">尚缺條件</div><div id="missing" style="margin-top:14px;line-height:1.8"></div></div>
<div class="card full muted" id="updated">等待第一輪 Gate 資料…</div></section></main>
<script>
const fmt=n=>Number(n).toLocaleString('zh-TW',{maximumFractionDigits:2});const rows=o=>Object.entries(o).map(([k,v])=>`<tr><td>${k}</td><td>${v}</td></tr>`).join('');
async function load(){let s=await fetch('/api/status').then(r=>r.json()),a=s.analysis||{};price.textContent=a.price?`$ ${fmt(a.price)}`:'—';direction.textContent=(a.direction||'—')+' · L'+(a.stage||0);
score.textContent=(a.score??'—')+'/100';scorebar.style.width=(a.score||0)+'%';health.textContent=s.status;fib.innerHTML=rows({...a.fib,OTE:a.ote?.join(' ～ ')||'—'});
conditions.innerHTML=rows({'4H 趨勢':a.trend_4h||'—','4H ADX':a.adx_4h??'—','15M MSS':a.mss_15m?'已確認':'等待','15M RVOL':a.rvol_15m??'—','現貨 RVOL':a.spot_rvol??'—','訂單簿失衡':a.orderbook_imbalance??'—'});
plan.innerHTML=rows({'進場區':a.entry?.join(' ～ ')||'—','結構止損':a.stop||'—','流動性目標':a.targets?.join(' / ')||'等待','正式訊號':a.formal?'是':'否'});
missing.textContent=(a.missing||[]).join(' · ')||'條件已齊備';updated.textContent=`最後更新：${s.updated_at?new Date(s.updated_at).toLocaleString('zh-TW',{timeZone:'Asia/Taipei'}):'—'} · 掃描 ${s.scan_count} 次 · ${a.risk_note||''}`;}
async function scan(){await fetch('/api/scan',{method:'POST'});load()}load();setInterval(load,15000);
</script></body></html>"""


@app.get("/", response_class=HTMLResponse)
def dashboard() -> str:
    return HTML


if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=PORT)
