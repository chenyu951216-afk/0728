from __future__ import annotations

import asyncio
import math
import statistics
import time
from dataclasses import dataclass, asdict
from typing import Any

import httpx

TIMEFRAME_SECONDS = {"1m": 60, "5m": 300, "15m": 900, "30m": 1800, "1h": 3600, "4h": 14400, "1d": 86400}

@dataclass(slots=True)
class Candle:
    ts: int
    o: float
    h: float
    l: float
    c: float
    v: float = 0.0
    qv: float = 0.0
    source: str = ""
    closed: bool = True
    def dict(self) -> dict[str, Any]: return asdict(self)

def _f(value: Any, default: float = 0.0) -> float:
    try:
        x = float(value); return x if math.isfinite(x) else default
    except (TypeError, ValueError): return default

class SourceError(RuntimeError): pass

class MarketDataHub:
    """Multi-exchange public-data hub. Missing fields are never fabricated."""
    GATE = "https://api.gateio.ws/api/v4"
    BYBIT = "https://api.bybit.com"
    OKX = "https://www.okx.com"
    BINANCE_FUT = "https://fapi.binance.com"
    BINANCE_SPOT = "https://api.binance.com"

    def __init__(self, timeout: float = 18.0) -> None:
        self.timeout = timeout
        self.headers = {"Accept": "application/json", "User-Agent": "eth-adaptive-research/4"}

    async def _json(self, client: httpx.AsyncClient, url: str, params: dict[str, Any] | None = None) -> Any:
        response = await client.get(url, params=params, headers=self.headers); response.raise_for_status(); data = response.json()
        if isinstance(data, dict):
            if data.get("retCode") not in (None, 0): raise SourceError(f"Bybit {data.get('retCode')}: {data.get('retMsg')}")
            if data.get("code") not in (None, "0", 0): raise SourceError(f"source code={data.get('code')} msg={data.get('msg')}")
        return data

    @staticmethod
    def _drop_open(candles: list[Candle], tf: str) -> list[Candle]:
        now = int(time.time()); sec = TIMEFRAME_SECONDS[tf]
        return [x for x in candles if x.ts + sec <= now]

    async def gate_candles(self, client: httpx.AsyncClient, symbol: str, tf: str, *, end_ts: int | None = None, limit: int = 1000, spot: bool = False) -> list[Candle]:
        if spot:
            params: dict[str, Any] = {"currency_pair": symbol, "interval": tf, "limit": min(limit, 1000)}; path = "/spot/candlesticks"
        else:
            params = {"contract": symbol, "interval": tf}; path = "/futures/usdt/candlesticks"
            if end_ts:
                span = TIMEFRAME_SECONDS[tf] * min(limit, 1900); params.update({"from": max(0, end_ts - span), "to": end_ts})
            else: params["limit"] = min(limit, 1900)
        raw = await self._json(client, self.GATE + path, params); result: list[Candle] = []
        for row in raw or []:
            if isinstance(row, dict):
                result.append(Candle(int(_f(row.get("t"))), _f(row.get("o")), _f(row.get("h")), _f(row.get("l")), _f(row.get("c")), _f(row.get("v")), _f(row.get("sum")), "gate"))
            elif len(row) >= 6:
                result.append(Candle(int(_f(row[0])), _f(row[5]), _f(row[3]), _f(row[4]), _f(row[2]), _f(row[6] if len(row) > 6 else row[1]), _f(row[1]), "gate"))
        result.sort(key=lambda x: x.ts); return self._drop_open(result, tf)

    async def bybit_candles(self, client: httpx.AsyncClient, symbol: str, tf: str, *, end_ts: int | None = None, limit: int = 1000, spot: bool = False) -> list[Candle]:
        interval = {"1m":"1","5m":"5","15m":"15","30m":"30","1h":"60","4h":"240","1d":"D"}[tf]
        params: dict[str, Any] = {"category":"spot" if spot else "linear","symbol":symbol,"interval":interval,"limit":min(limit,1000)}
        if end_ts: params["end"] = end_ts * 1000
        data = await self._json(client, self.BYBIT + "/v5/market/kline", params); rows = ((data or {}).get("result") or {}).get("list") or []
        result = [Candle(int(_f(x[0])/1000),_f(x[1]),_f(x[2]),_f(x[3]),_f(x[4]),_f(x[5]),_f(x[6] if len(x)>6 else 0),"bybit") for x in rows]
        result.sort(key=lambda x:x.ts); return self._drop_open(result, tf)

    async def okx_candles(self, client: httpx.AsyncClient, symbol: str, tf: str, *, end_ts: int | None = None, limit: int = 100, spot: bool = False) -> list[Candle]:
        bar={"1m":"1m","5m":"5m","15m":"15m","30m":"30m","1h":"1H","4h":"4H","1d":"1Dutc"}[tf]
        params: dict[str,Any]={"instId":symbol,"bar":bar,"limit":min(limit,100)}
        if end_ts: params["after"]=end_ts*1000
        data=await self._json(client,self.OKX+"/api/v5/market/history-candles",params); rows=(data or {}).get("data") or []
        result=[Candle(int(_f(x[0])/1000),_f(x[1]),_f(x[2]),_f(x[3]),_f(x[4]),_f(x[5]),_f(x[7] if len(x)>7 else 0),"okx",closed=(str(x[8])=="1" if len(x)>8 else True)) for x in rows]
        result=[x for x in result if x.closed]; result.sort(key=lambda x:x.ts); return self._drop_open(result,tf)

    async def binance_candles(self, client: httpx.AsyncClient, symbol: str, tf: str, *, end_ts: int | None = None, limit: int = 1000, spot: bool = False) -> list[Candle]:
        params: dict[str,Any]={"symbol":symbol,"interval":tf,"limit":min(limit,1500)}
        if end_ts: params["endTime"]=end_ts*1000
        base=self.BINANCE_SPOT if spot else self.BINANCE_FUT; path="/api/v3/klines" if spot else "/fapi/v1/klines"
        rows=await self._json(client,base+path,params); result=[Candle(int(_f(x[0])/1000),_f(x[1]),_f(x[2]),_f(x[3]),_f(x[4]),_f(x[5]),_f(x[7] if len(x)>7 else 0),"binance") for x in rows or []]
        result.sort(key=lambda x:x.ts); return self._drop_open(result,tf)

    async def fetch_history(self, source: str, asset: str, tf: str, *, end_ts: int | None = None, limit: int = 1000, spot: bool = False) -> list[Candle]:
        mapping={"gate":"ETH_USDT" if asset=="ETH" else "BTC_USDT","bybit":"ETHUSDT" if asset=="ETH" else "BTCUSDT","okx":f"{asset}-USDT" if spot else f"{asset}-USDT-SWAP","binance":f"{asset}USDT"}
        if source not in mapping: raise ValueError(f"unknown source {source}")
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            return await getattr(self,f"{source}_candles")(client,mapping[source],tf,end_ts=end_ts,limit=limit,spot=spot)

    async def live_bundle(self) -> dict[str,Any]:
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            gate_tasks=[self.gate_candles(client,"ETH_USDT",tf,limit=limit) for tf,limit in (("1d",500),("4h",600),("1h",800),("30m",800),("15m",1000),("5m",1000))]
            gate_tasks += [self.gate_candles(client,"BTC_USDT","1h",limit=500),self.gate_candles(client,"ETH_USDT","15m",limit=200,spot=True),self._json(client,self.GATE+"/futures/usdt/tickers",{"contract":"ETH_USDT"}),self._json(client,self.GATE+"/futures/usdt/order_book",{"contract":"ETH_USDT","limit":50,"with_id":"true"})]
            gate_result=await asyncio.gather(*gate_tasks,return_exceptions=True)
            repaired=list(gate_result); specs=[("ETH","1d",500),("ETH","4h",600),("ETH","1h",800),("ETH","30m",800),("ETH","15m",1000),("ETH","5m",1000),("BTC","1h",500),("ETH","15m",200)]
            for i,(asset,tf,lim) in enumerate(specs):
                if not isinstance(repaired[i],Exception) and repaired[i]: continue
                spot=i==7
                for source in ("bybit","binance","okx"):
                    try:
                        symbol={"bybit":f"{asset}USDT","binance":f"{asset}USDT","okx":f"{asset}-USDT" if spot else f"{asset}-USDT-SWAP"}[source]
                        rows=await getattr(self,f"{source}_candles")(client,symbol,tf,limit=lim,spot=spot)
                        if rows: repaired[i]=rows; break
                    except Exception: continue
            gate_result=repaired
            validators={"bybit":self.bybit_candles(client,"ETHUSDT","15m",limit=100),"okx":self.okx_candles(client,"ETH-USDT-SWAP","15m",limit=100),"binance":self.binance_candles(client,"ETHUSDT","15m",limit=100)}
            validation_raw=await asyncio.gather(*validators.values(),return_exceptions=True)
            derivative_tasks={"bybit_ticker":self._json(client,self.BYBIT+"/v5/market/tickers",{"category":"linear","symbol":"ETHUSDT"}),"bybit_oi":self._json(client,self.BYBIT+"/v5/market/open-interest",{"category":"linear","symbol":"ETHUSDT","intervalTime":"15min","limit":2}),"bybit_funding":self._json(client,self.BYBIT+"/v5/market/funding/history",{"category":"linear","symbol":"ETHUSDT","limit":2}),"binance_oi":self._json(client,self.BINANCE_FUT+"/fapi/v1/openInterest",{"symbol":"ETHUSDT"}),"binance_funding":self._json(client,self.BINANCE_FUT+"/fapi/v1/fundingRate",{"symbol":"ETHUSDT","limit":2}),"okx_oi":self._json(client,self.OKX+"/api/v5/public/open-interest",{"instType":"SWAP","instId":"ETH-USDT-SWAP"}),"okx_funding":self._json(client,self.OKX+"/api/v5/public/funding-rate-history",{"instId":"ETH-USDT-SWAP","limit":2})}
            derivative_raw=await asyncio.gather(*derivative_tasks.values(),return_exceptions=True)
        keys=["eth_1d","eth_4h","eth_1h","eth_30m","eth_15m","eth_5m","btc_1h","eth_spot_15m"]
        bundle={k:([x.dict() for x in v] if not isinstance(v,Exception) else []) for k,v in zip(keys,gate_result[:8])}
        ticker=gate_result[8]
        if isinstance(ticker,Exception): ticker={}
        if isinstance(ticker,list): ticker=ticker[0] if ticker else {}
        bundle["ticker"]=ticker or {}; book=gate_result[9]; bundle["orderbook"]={} if isinstance(book,Exception) else (book or {})
        validations={}; quality={"primary":"gate","sources":{},"errors":[]}; primary_close=bundle["eth_15m"][-1]["c"] if bundle["eth_15m"] else 0.0; diffs=[]
        for name,value in zip(validators,validation_raw):
            if isinstance(value,Exception): quality["sources"][name]={"ok":False,"error":str(value)}; quality["errors"].append(f"{name}: {value}"); continue
            rows=[x.dict() for x in value]; validations[name]=rows; close=rows[-1]["c"] if rows else 0.0; diff_bps=abs(close-primary_close)/primary_close*10000 if close and primary_close else None
            if diff_bps is not None: diffs.append(diff_bps)
            quality["sources"][name]={"ok":bool(rows),"close":close,"diff_bps":diff_bps}
        quality["agreement_median_bps"]=statistics.median(diffs) if diffs else None; quality["agreement_ok"]=bool(diffs and quality["agreement_median_bps"]<=20); quality["score"]=max(0,min(100,55+15*sum(1 for v in quality["sources"].values() if v.get("ok"))+(15 if quality["agreement_ok"] else 0))); bundle["validators"]=validations
        deriv={}
        for name,value in zip(derivative_tasks,derivative_raw):
            if isinstance(value,Exception): quality["errors"].append(f"{name}: {value}"); continue
            deriv[name]=value
        bundle["derivatives_raw"]=deriv; bundle["quality"]=quality; return bundle

    async def fetch_bybit_oi_history(self, asset: str, tf: str, *, end_ts: int | None = None, limit: int = 200) -> list[dict[str,Any]]:
        interval={"5m":"5min","15m":"15min","30m":"30min","1h":"1h","4h":"4h","1d":"1d"}[tf]; params={"category":"linear","symbol":f"{asset}USDT","intervalTime":interval,"limit":min(limit,200)}
        if end_ts: params["endTime"]=end_ts*1000
        async with httpx.AsyncClient(timeout=self.timeout) as client: data=await self._json(client,self.BYBIT+"/v5/market/open-interest",params)
        rows=((data or {}).get("result") or {}).get("list") or []; out=[{"ts":int(_f(x.get("timestamp"))/1000),"oi":_f(x.get("openInterest")),"source":"bybit"} for x in rows]; out.sort(key=lambda x:x["ts"]); return out

    async def fetch_funding_history(self, source: str, asset: str, *, end_ts: int | None = None, limit: int = 200) -> list[dict[str,Any]]:
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            if source=="bybit":
                params={"category":"linear","symbol":f"{asset}USDT","limit":min(limit,200)}
                if end_ts: params["endTime"]=end_ts*1000
                data=await self._json(client,self.BYBIT+"/v5/market/funding/history",params); rows=((data or {}).get("result") or {}).get("list") or []; return sorted([{"ts":int(_f(x.get("fundingRateTimestamp"))/1000),"funding":_f(x.get("fundingRate")),"source":"bybit"} for x in rows],key=lambda x:x["ts"])
            if source=="binance":
                params={"symbol":f"{asset}USDT","limit":min(limit,1000)}
                if end_ts: params["endTime"]=end_ts*1000
                rows=await self._json(client,self.BINANCE_FUT+"/fapi/v1/fundingRate",params); return sorted([{"ts":int(_f(x.get("fundingTime"))/1000),"funding":_f(x.get("fundingRate")),"source":"binance"} for x in rows or []],key=lambda x:x["ts"])
            if source=="gate":
                rows=await self._json(client,self.GATE+"/futures/usdt/funding_rate",{"contract":f"{asset}_USDT","limit":min(limit,1000)}); return sorted([{"ts":int(_f(x.get("t"))),"funding":_f(x.get("r")),"source":"gate"} for x in rows or []],key=lambda x:x["ts"])
        raise ValueError(f"unsupported funding source {source}")
