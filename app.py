"""ETH adaptive short-term research engine.

Top-down learning order: 1D/4H -> 1H/30M -> 15M/5M -> live execution review.
The service produces research/paper signals only. It never sends exchange orders.
"""
from __future__ import annotations
import asyncio
import bisect
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
from adaptive_engine import Learner, ModelStore, STRATEGIES, atr, bootstrap_progress, build_features, choose_strategy, detect_regime, ema, risk_plan
from market_data import MarketDataHub, TIMEFRAME_SECONDS
from derivative_data import DerivativeHistory
LOG = logging.getLogger('eth-adaptive')
logging.basicConfig(level=os.getenv('LOG_LEVEL', 'INFO'), format='%(asctime)s %(levelname)s %(message)s')
TAIPEI = ZoneInfo(os.getenv('TZ', 'Asia/Taipei'))
DB_PATH = os.getenv('DATABASE_PATH', 'eth_adaptive.db')
PORT = int(os.getenv('PORT', '8080'))
SCAN_SECONDS = max(30, int(os.getenv('SCAN_SECONDS', '60')))
LEARNING_SLEEP_SECONDS = max(1, int(os.getenv('LEARNING_SLEEP_SECONDS', '3')))
BACKFILL_PAGES_PER_TICK = max(1, min(12, int(os.getenv('LEARNING_BACKFILL_PAGES_PER_TICK', '5'))))
START_TS = int(os.getenv('LEARNING_START_TS', '1577836800'))
SIGNAL_MIN_PROB = float(os.getenv('SIGNAL_MIN_PROBABILITY', '0.60'))
RISK_PER_TRADE = float(os.getenv('RISK_PER_TRADE', '0.02'))
POST_EXIT_BARS = max(24, int(os.getenv('POST_EXIT_BARS', '96')))
DISCORD_WEBHOOK_URL = os.getenv('DISCORD_WEBHOOK_URL', '')
DISCORD_BOT_TOKEN = os.getenv('DISCORD_BOT_TOKEN', '')
DISCORD_CHANNEL_ID = os.getenv('DISCORD_CHANNEL_ID', '')
DISCORD_ALLOWED_USER_ID = os.getenv('DISCORD_ALLOWED_USER_ID', '')
DISCORD_API = 'https://discord.com/api/v10'
hub = MarketDataHub()
derivative_history = DerivativeHistory(DB_PATH)
state: dict[str, Any] = {'service': 'BOOTING', 'updated_at': None, 'error': None, 'scan_count': 0, 'analysis': {}, 'learning': {}, 'active_signal': None, 'last_training': None, 'discord': 'not configured'}

def db() -> sqlite3.Connection:
    global DB_PATH
    requested = Path(DB_PATH)
    try:
        requested.parent.mkdir(parents=True, exist_ok=True)
        con = sqlite3.connect(requested, timeout=30, check_same_thread=False)
    except (OSError, sqlite3.OperationalError) as exc:
        fallback = Path('/tmp/eth_adaptive.db')
        LOG.warning('database %s unavailable (%s), fallback=%s', requested, exc, fallback)
        DB_PATH = str(fallback)
        con = sqlite3.connect(fallback, timeout=30, check_same_thread=False)
    con.row_factory = sqlite3.Row
    con.execute('PRAGMA journal_mode=WAL'); con.execute('PRAGMA synchronous=NORMAL')
    con.execute('CREATE TABLE IF NOT EXISTS snapshots(id INTEGER PRIMARY KEY, ts INTEGER NOT NULL, payload TEXT NOT NULL)'); con.execute('CREATE INDEX IF NOT EXISTS ix_snapshots_ts ON snapshots(ts)')
    con.execute('CREATE TABLE IF NOT EXISTS system_state(key TEXT PRIMARY KEY, value TEXT NOT NULL, updated_at INTEGER NOT NULL)')
    con.execute('CREATE TABLE IF NOT EXISTS market_bars(source TEXT NOT NULL, asset TEXT NOT NULL, tf TEXT NOT NULL, ts INTEGER NOT NULL,o REAL NOT NULL,h REAL NOT NULL,l REAL NOT NULL,c REAL NOT NULL,v REAL NOT NULL,qv REAL NOT NULL DEFAULT 0,PRIMARY KEY(source,asset,tf,ts))'); con.execute('CREATE INDEX IF NOT EXISTS ix_market_bars_asset_tf_ts ON market_bars(asset,tf,ts)')
    con.execute('CREATE TABLE IF NOT EXISTS source_ledger(ts INTEGER NOT NULL, source TEXT NOT NULL, feature_group TEXT NOT NULL,ok INTEGER NOT NULL, quality REAL NOT NULL, detail TEXT NOT NULL,PRIMARY KEY(ts,source,feature_group))')
    con.execute('CREATE TABLE IF NOT EXISTS signals(signal_id TEXT PRIMARY KEY, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL,status TEXT NOT NULL, strategy TEXT NOT NULL, direction TEXT NOT NULL,regime TEXT NOT NULL, phase TEXT NOT NULL, probability REAL NOT NULL,entry REAL NOT NULL, initial_stop REAL NOT NULL, current_stop REAL NOT NULL,targets TEXT NOT NULL, payload TEXT NOT NULL, filled_at INTEGER, exit_ts INTEGER,exit_price REAL, exit_reason TEXT, realized_r REAL, review_until INTEGER,post_mfe_r REAL NOT NULL DEFAULT 0, post_mae_r REAL NOT NULL DEFAULT 0,review_label TEXT)'); con.execute('CREATE INDEX IF NOT EXISTS ix_signals_status ON signals(status,created_at)')
    con.execute('CREATE TABLE IF NOT EXISTS alerts(id INTEGER PRIMARY KEY, ts INTEGER NOT NULL, signal_id TEXT, kind TEXT NOT NULL, message TEXT NOT NULL)')
    ModelStore(con); con.commit(); return con

def get_state(key: str, default: Any=None) -> Any:
    con = db(); row = con.execute('SELECT value FROM system_state WHERE key=?', (key,)).fetchone(); con.close()
    if not row: return default
    try: return json.loads(row[0])
    except Exception: return default

def set_state(key: str, value: Any) -> None:
    con = db(); con.execute('INSERT INTO system_state(key,value,updated_at) VALUES(?,?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_at=excluded.updated_at', (key, json.dumps(value, ensure_ascii=False), int(time.time()))); con.commit(); con.close()

def insert_bars(source: str, asset: str, tf: str, rows: list[dict[str, Any]]) -> int:
    if not rows: return 0
    con = db(); before = con.total_changes
    con.executemany('INSERT OR IGNORE INTO market_bars(source,asset,tf,ts,o,h,l,c,v,qv) VALUES(?,?,?,?,?,?,?,?,?,?)', [(source, asset, tf, int(x['ts']), float(x['o']), float(x['h']), float(x['l']), float(x['c']), float(x.get('v', 0)), float(x.get('qv', 0))) for x in rows]); added = con.total_changes - before; con.commit(); con.close(); return added

def load_bars(asset: str, tf: str, source: str='gate', limit: int | None=None) -> list[dict[str, Any]]:
    con = db(); sql = 'SELECT ts,o,h,l,c,v,qv FROM market_bars WHERE source=? AND asset=? AND tf=? ORDER BY ts'; params: list[Any] = [source, asset, tf]
    if limit: sql = 'SELECT * FROM (' + sql + ' DESC LIMIT ?) ORDER BY ts'; params.append(limit)
    rows = con.execute(sql, params).fetchall(); con.close(); return [{'ts': r[0], 'o': r[1], 'h': r[2], 'l': r[3], 'c': r[4], 'v': r[5], 'qv': r[6]} for r in rows]

def upsert_live_gate(bundle: dict[str, Any]) -> None:
    for key, tf in (('eth_1d', '1d'), ('eth_4h', '4h'), ('eth_1h', '1h'), ('eth_30m', '30m'), ('eth_15m', '15m'), ('eth_5m', '5m'), ('btc_1h', '1h')):
        asset = 'BTC' if key == 'btc_1h' else 'ETH'; grouped: dict[str, list[dict[str, Any]]] = {}
        for row in bundle.get(key, []): grouped.setdefault(str(row.get('source') or 'gate'), []).append(row)
        for source, rows in grouped.items(): insert_bars(source, asset, tf, rows)
    for source, rows in bundle.get('validators', {}).items(): insert_bars(source, 'ETH', '15m', rows)

def _raw_derivatives(bundle: dict[str, Any]) -> dict[str, float]:
    raw = bundle.get('derivatives_raw', {}); ticker = bundle.get('ticker', {}); funding_values, oi_values = ([], [])
    gate_funding = float(ticker.get('funding_rate') or 0)
    if gate_funding: funding_values.append(gate_funding)
    try: funding_values += [float(x['fundingRate']) for x in raw['bybit_funding']['result']['list'][:1]]
    except Exception: pass
    try: funding_values += [float(x['fundingRate']) for x in raw.get('binance_funding', [])[:1]]
    except Exception: pass
    try: funding_values += [float(x['fundingRate']) for x in raw['okx_funding']['data'][:1]]
    except Exception: pass
    try: oi_values += [float(x['openInterest']) for x in raw['bybit_oi']['result']['list'][:2]]
    except Exception: pass
    oi_change = oi_values[0] / oi_values[1] - 1 if len(oi_values) >= 2 and oi_values[1] else 0.0
    bids = bundle.get('orderbook', {}).get('bids', []); asks = bundle.get('orderbook', {}).get('asks', []); bid_qty = sum((abs(float(x.get('s', 0) if isinstance(x, dict) else x[1])) for x in bids[:30])); ask_qty = sum((abs(float(x.get('s', 0) if isinstance(x, dict) else x[1])) for x in asks[:30])); imbalance = (bid_qty - ask_qty) / max(bid_qty + ask_qty, 1e-09)
    perp = float(ticker.get('last') or 0); spot_rows = bundle.get('eth_spot_15m', []); spot = float(spot_rows[-1]['c']) if spot_rows else 0.0; basis = (perp / spot - 1) * 10000 if perp and spot else 0.0
    oi_available = float(len(oi_values) >= 2); funding_available = float(bool(funding_values)); book_available = float(bool(bids and asks)); available = oi_available + funding_available + book_available
    return {'funding': statistics.median(funding_values) if funding_values else 0.0, 'oi_change': oi_change, 'book_imbalance': imbalance, 'spot_perp_basis_bps': basis, 'liquidation_imbalance': 0.0, 'liquidation_intensity': 0.0, 'oi_available': oi_available, 'funding_available': funding_available, 'liquidation_available': 0.0, 'book_available': book_available, 'derivative_coverage': available / 4.0, 'derivative_quality': available / 3.0 if available else 0.0, 'source_agreement_bps': bundle.get('quality', {}).get('agreement_median_bps') or 999.0}

def _entry_for_strategy(strategy: str, direction: str, live: float, m15: list[dict[str, Any]]) -> float:
    a = max(atr(m15), live * 0.001); e20 = ema([float(x['c']) for x in m15], 20); sign = 1 if direction == 'LONG' else -1
    if strategy == 'TREND_PULLBACK': proposed = min(live - 0.1 * a, e20) if direction == 'LONG' else max(live + 0.1 * a, e20)
    elif strategy == 'BREAKOUT_RETEST':
        window = m15[-24:-1]; proposed = max((float(x['h']) for x in window)) if direction == 'LONG' else min((float(x['l']) for x in window))
    elif strategy == 'LIQUIDITY_SWEEP_REVERSAL': proposed = live - sign * 0.08 * a
    elif strategy == 'SQUEEZE_EXPANSION': proposed = live - sign * 0.14 * a
    else: proposed = e20
    return round(min(proposed, live - 0.03 * a), 2) if direction == 'LONG' else round(max(proposed, live + 0.03 * a), 2)

def _notional_for_risk(entry: float, stop: float) -> dict[str, Any]:
    equity = float(get_state('account_equity_usdt', 0) or 0)
    if equity <= 0 or entry <= 0 or stop <= 0: return {'equity_usdt': equity, 'risk_pct': RISK_PER_TRADE, 'notional_usdt': None, 'reason': 'set account equity first'}
    stop_pct = abs(entry - stop) / entry; return {'equity_usdt': equity, 'risk_pct': RISK_PER_TRADE, 'stop_pct': stop_pct, 'notional_usdt': round(equity * RISK_PER_TRADE / max(stop_pct, 1e-06), 2)}

def latest_signal(statuses: tuple[str, ...]=('PLANNED', 'OPEN')) -> dict[str, Any] | None:
    con = db(); placeholders = ','.join(('?' for _ in statuses)); row = con.execute(f'SELECT * FROM signals WHERE status IN ({placeholders}) ORDER BY created_at DESC LIMIT 1', statuses).fetchone(); con.close()
    if not row: return None
    item = dict(row); item['targets'] = json.loads(item['targets']); item['payload'] = json.loads(item['payload']); return item

def create_signal(analysis: dict[str, Any], m15: list[dict[str, Any]]) -> dict[str, Any] | None:
    selection = analysis['selection']
    if not selection['tradeable'] or selection['probability'] < SIGNAL_MIN_PROB: return None
    current = latest_signal()
    if current: return current
    live = analysis['price']; entry = _entry_for_strategy(selection['strategy'], selection['direction'], live, m15); con = db(); store = ModelStore(con); plan = risk_plan(store, selection['strategy'], analysis['regime']['regime'], selection['direction'], entry, m15); con.close(); signal_id = f"{int(time.time())}-{selection['strategy'][:4]}-{selection['direction'][0]}"
    payload = {'initial_plan': plan, 'selection': selection, 'regime': analysis['regime'], 'features': analysis.get('features', {}), 'data_quality': float((analysis.get('data_quality') or {}).get('score', 0)), 'created_from_snapshot': analysis.get('snapshot_ts'), 'immutable': True, 'management': {'hit_targets': [], 'mfe_r': 0.0, 'mae_r': 0.0, 'trail_reason': None}}
    con = db(); con.execute('INSERT INTO signals(signal_id,created_at,updated_at,status,strategy,direction,regime,phase,probability,entry,initial_stop,current_stop,targets,payload) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)', (signal_id, int(time.time()), int(time.time()), 'PLANNED', selection['strategy'], selection['direction'], analysis['regime']['regime'], analysis['regime']['phase'], selection['probability'], plan['entry'], plan['stop'], plan['stop'], json.dumps(plan['targets']), json.dumps(payload, ensure_ascii=False))); con.commit(); con.close(); return latest_signal()

def _close_signal(row: dict[str, Any], price: float, reason: str, ts: int) -> None:
    entry, stop0 = (float(row['entry']), float(row['initial_stop'])); sign = 1 if row['direction'] == 'LONG' else -1; risk = abs(entry - stop0) or 1e-09; realized_r = (price - entry) * sign / risk; payload = row['payload'] if isinstance(row['payload'], dict) else json.loads(row['payload']); payload.setdefault('management', {})['closed_reason'] = reason
    con = db(); con.execute("UPDATE signals SET status='CLOSED',updated_at=?,exit_ts=?,exit_price=?,exit_reason=?,realized_r=?,review_until=?,payload=? WHERE signal_id=?", (ts, ts, price, reason, realized_r, ts + POST_EXIT_BARS * 900, json.dumps(payload, ensure_ascii=False), row['signal_id'])); con.commit(); con.close()

def update_signal_with_bar(bar: dict[str, Any]) -> dict[str, Any] | None:
    row = latest_signal()
    if not row: return None
    ts = int(bar['ts']); entry, stop0, current_stop = (float(row['entry']), float(row['initial_stop']), float(row['current_stop'])); direction = row['direction']; sign = 1 if direction == 'LONG' else -1; low, high = (float(bar['l']), float(bar['h'])); touched = low <= entry <= high; payload = row['payload']; targets = row['targets']
    if row['status'] == 'PLANNED':
        if not touched:
            if ts - int(row['created_at']) > 16 * 3600:
                con = db(); con.execute("UPDATE signals SET status='EXPIRED',updated_at=? WHERE signal_id=?", (ts, row['signal_id'])); con.commit(); con.close(); return None
            return row
        row['status'] = 'OPEN'; row['filled_at'] = ts; con = db(); con.execute("UPDATE signals SET status='OPEN',filled_at=?,updated_at=? WHERE signal_id=?", (ts, ts, row['signal_id'])); con.commit(); con.close()
    risk = abs(entry - stop0) or 1e-09; favorable = (high - entry) / risk if direction == 'LONG' else (entry - low) / risk; adverse = (entry - low) / risk if direction == 'LONG' else (high - entry) / risk; mgmt = payload.setdefault('management', {}); mgmt['mfe_r'] = max(float(mgmt.get('mfe_r', 0)), favorable); mgmt['mae_r'] = max(float(mgmt.get('mae_r', 0)), adverse); hit_targets = list(mgmt.get('hit_targets', [])); stop_hit = low <= current_stop if direction == 'LONG' else high >= current_stop
    if stop_hit: _close_signal(row, current_stop, 'STOP_OR_TRAIL', ts); return None
    for idx, target in enumerate(targets):
        if idx in hit_targets: continue
        price = float(target['price']); hit = high >= price if direction == 'LONG' else low <= price
        if hit: hit_targets.append(idx); mgmt['last_target_hit'] = idx + 1
    mgmt['hit_targets'] = hit_targets; new_stop = current_stop
    if 0 in hit_targets: new_stop = max(new_stop, entry) if direction == 'LONG' else min(new_stop, entry); mgmt['trail_reason'] = 'TP1 -> breakeven'
    if 1 in hit_targets:
        locked = entry + sign * 0.55 * risk; new_stop = max(new_stop, locked) if direction == 'LONG' else min(new_stop, locked); mgmt['trail_reason'] = 'TP2 -> lock 0.55R'
    if 2 in hit_targets:
        locked = entry + sign * 1.05 * risk; new_stop = max(new_stop, locked) if direction == 'LONG' else min(new_stop, locked); mgmt['trail_reason'] = 'TP3 -> lock 1.05R'
    new_stop = max(new_stop, current_stop) if direction == 'LONG' else min(new_stop, current_stop)
    if len(hit_targets) == len(targets): _close_signal(row, float(targets[-1]['price']), 'ALL_TARGETS', ts); return None
    con = db(); con.execute('UPDATE signals SET updated_at=?,current_stop=?,payload=? WHERE signal_id=?', (ts, new_stop, json.dumps(payload, ensure_ascii=False), row['signal_id'])); con.commit(); con.close(); return latest_signal()

def post_exit_review(bar: dict[str, Any]) -> None:
    ts = int(bar['ts']); con = db(); rows = con.execute("SELECT * FROM signals WHERE status='CLOSED' AND review_label IS NULL AND review_until>=?", (ts,)).fetchall()
    for raw in rows:
        row = dict(raw)
        if not row['exit_ts'] or ts <= row['exit_ts']: continue
        entry, stop0 = (float(row['entry']), float(row['initial_stop'])); risk = abs(entry - stop0) or 1e-09; sign = 1 if row['direction'] == 'LONG' else -1; exit_price = float(row['exit_price']); favorable = (float(bar['h']) - exit_price) * sign / risk if sign > 0 else (exit_price - float(bar['l'])) / risk; adverse = (exit_price - float(bar['l'])) / risk if sign > 0 else (float(bar['h']) - exit_price) / risk; con.execute('UPDATE signals SET post_mfe_r=MAX(post_mfe_r,?),post_mae_r=MAX(post_mae_r,?),updated_at=? WHERE signal_id=?', (favorable, adverse, ts, row['signal_id']))
    expired = con.execute("SELECT * FROM signals WHERE status='CLOSED' AND review_label IS NULL AND review_until<?", (ts,)).fetchall()
    for raw in expired:
        row = dict(raw); realized = float(row['realized_r'] or 0); post_mfe = float(row['post_mfe_r'] or 0)
        if row['exit_reason'] == 'STOP_OR_TRAIL' and post_mfe >= 1.2: label = 'STOP_TOO_TIGHT_CANDIDATE'
        elif realized > 0 and post_mfe >= 1.5: label = 'EARLY_EXIT_RUNNER_OPPORTUNITY'
        elif realized < 0 and post_mfe < 0.5: label = 'VALID_DEFENSIVE_EXIT'
        else: label = 'NEUTRAL'
        con.execute('UPDATE signals SET review_label=?,updated_at=? WHERE signal_id=?', (label, ts, row['signal_id']))
    con.commit(); con.close()

async def send_discord(title: str, body: str, color: int=6000633) -> None:
    if not DISCORD_WEBHOOK_URL and (not (DISCORD_BOT_TOKEN and DISCORD_CHANNEL_ID)): return
    payload = {'embeds': [{'title': title, 'description': body[:4000], 'color': color, 'timestamp': datetime.now(timezone.utc).isoformat(), 'footer': {'text': 'ETH Adaptive Engine | research/paper only'}}]}
    async with httpx.AsyncClient(timeout=15) as client:
        if DISCORD_WEBHOOK_URL: r = await client.post(DISCORD_WEBHOOK_URL, json=payload)
        else: r = await client.post(f'{DISCORD_API}/channels/{DISCORD_CHANNEL_ID}/messages', headers={'Authorization': f'Bot {DISCORD_BOT_TOKEN}'}, json=payload)
        r.raise_for_status()

def _analysis_from_bundle(bundle: dict[str, Any]) -> dict[str, Any]:
    d1, h4, h1, m15, m5, btc = (bundle['eth_1d'], bundle['eth_4h'], bundle['eth_1h'], bundle['eth_15m'], bundle['eth_5m'], bundle['btc_1h'])
    if min(map(len, (d1, h4, h1, m15, m5, btc))) < 40: raise RuntimeError('insufficient closed candles')
    regime = detect_regime(d1, h4, h1); extras = _raw_derivatives(bundle); features = build_features(m15, h1, btc, regime, extras); con = db(); store = ModelStore(con); learner = Learner(store); selection = choose_strategy(store, learner, features, regime, bundle['quality']['score']); con.close(); ticker = bundle['ticker']; price = float(ticker.get('last') or m15[-1]['c'])
    return {'snapshot_ts': int(time.time()), 'price': price, 'regime': regime, 'features': features, 'selection': selection, 'data_quality': bundle['quality'], 'derivatives': extras, 'trade_label': 'LEARNED TRADE' if selection['tradeable'] else 'WAIT / LEARNING', 'rule': 'strategy selection prioritizes certified learned champion models'}

async def scan() -> dict[str, Any]:
    bundle = await hub.live_bundle(); upsert_live_gate(bundle); analysis = _analysis_from_bundle(bundle); now = int(time.time()); con = db(); con.execute('INSERT INTO snapshots(ts,payload) VALUES(?,?)', (now, json.dumps(analysis, ensure_ascii=False))); con.execute('DELETE FROM snapshots WHERE ts<?', (now - 120 * 86400,)); con.commit(); con.close(); last_bar = bundle['eth_15m'][-1]; update_signal_with_bar(last_bar); post_exit_review(last_bar); before = latest_signal()
    if before is None:
        created = create_signal(analysis, bundle['eth_15m'])
        if created and created['created_at'] >= now - 5: await send_discord('ETH learned setup created', f"{created['direction']} | {created['strategy']} | regime {created['regime']}/{created['phase']}\nprobability {created['probability']:.1%} | entry limit {created['entry']} | stop {created['initial_stop']}\nTargets: " + ', '.join((str(x['price']) for x in created['targets'])) + '\nNo market chase; initial plan is immutable.', 5025616)
    active = latest_signal(); state.update(service='OK', updated_at=datetime.now(timezone.utc).isoformat(), error=None, scan_count=state['scan_count'] + 1, analysis=analysis, active_signal=active); return analysis

BACKFILL_PLAN = [('ETH', '1d'), ('ETH', '4h'), ('BTC', '1h'), ('ETH', '1h'), ('ETH', '30m'), ('ETH', '15m'), ('ETH', '5m')]

def _earliest(asset: str, tf: str, source: str | None=None) -> int | None:
    con = db(); row = con.execute('SELECT MIN(ts) FROM market_bars WHERE source=? AND asset=? AND tf=?', (source, asset, tf)).fetchone() if source else con.execute('SELECT MIN(ts) FROM market_bars WHERE asset=? AND tf=?', (asset, tf)).fetchone(); con.close(); return int(row[0]) if row and row[0] is not None else None

async def backfill_one(asset: str, tf: str) -> dict[str, Any]:
    earliest = _earliest(asset, tf); end_ts = earliest - TIMEFRAME_SECONDS[tf] if earliest else int(time.time())
    if end_ts <= START_TS: return {'done': True, 'asset': asset, 'tf': tf, 'added': 0}
    added = 0; source_used = None; errors: list[str] = []; skipped: list[str] = []
    for _ in range(BACKFILL_PAGES_PER_TICK):
        rows = []
        for source in hub.history_source_order(tf, end_ts):
            try:
                rows = await hub.fetch_history(source, asset, tf, end_ts=end_ts, limit=1000)
                if rows:
                    source_used = source
                    break
                skipped.append(f'{source}:empty')
            except Exception as exc:
                msg = str(exc)
                if 'retention window' in msg or 'too long ago' in msg.lower():
                    skipped.append(f'{source}:{msg}')
                else:
                    errors.append(f'{source}:{msg}')
        if not rows: break
        dict_rows = [x.dict() for x in rows if x.ts >= START_TS]
        added += insert_bars(str(source_used), asset, tf, dict_rows)
        oldest = min((x.ts for x in rows))
        if oldest <= START_TS or oldest >= end_ts: break
        end_ts = oldest - TIMEFRAME_SECONDS[tf]
    con = db(); con.execute('INSERT OR REPLACE INTO source_ledger(ts,source,feature_group,ok,quality,detail) VALUES(?,?,?,?,?,?)', (int(time.time()), str(source_used or 'multi-source'), f'backfill:{asset}:{tf}', int(added > 0), 100.0 if added else 0.0, json.dumps({'added': added, 'errors': errors[-5:], 'capability_skips': skipped[-8:]}, ensure_ascii=False))); con.commit(); con.close(); return {'done': end_ts <= START_TS, 'asset': asset, 'tf': tf, 'added': added, 'source': source_used, 'errors': errors[-5:], 'capability_skips': skipped[-8:]}

def _best_source(asset: str, tf: str) -> str | None:
    con = db(); rows = con.execute('SELECT source,COUNT(*) n,MIN(ts) mn,MAX(ts) mx FROM market_bars WHERE asset=? AND tf=? GROUP BY source ORDER BY n DESC', (asset, tf)).fetchall(); con.close()
    if not rows: return None
    full = [r for r in rows if r[2] is not None and r[2] <= START_TS + 7 * 86400]
    if full: full.sort(key=lambda r: (r[0] != 'gate', -int(r[1]))); return str(full[0][0])
    return str(rows[0][0])

def _slice_to(rows: list[dict[str, Any]], timestamps: list[int], ts: int, min_bars: int, max_bars: int) -> list[dict[str, Any]] | None:
    idx = bisect.bisect_right(timestamps, ts)
    if idx < min_bars: return None
    return rows[max(0, idx - max_bars):idx]

def generate_learning_samples(batch: int=450) -> int:
    src15 = _best_source('ETH', '15m'); src1h = _best_source('ETH', '1h'); src4h = _best_source('ETH', '4h'); src1d = _best_source('ETH', '1d'); srcbtc = _best_source('BTC', '1h')
    if not all((src15, src1h, src4h, src1d, srcbtc)): return 0
    m15 = load_bars('ETH', '15m', src15); h1 = load_bars('ETH', '1h', src1h); h4 = load_bars('ETH', '4h', src4h); d1 = load_bars('ETH', '1d', src1d); btc = load_bars('BTC', '1h', srcbtc)
    if min(map(len, (m15, h1, h4, d1, btc))) < 120: return 0
    ts15 = [x['ts'] for x in m15]; ts1 = [x['ts'] for x in h1]; ts4 = [x['ts'] for x in h4]; tsd = [x['ts'] for x in d1]; tsb = [x['ts'] for x in btc]; last_ts = int(get_state('last_learning_sample_ts', START_TS) or START_TS); start_i = max(100, bisect.bisect_right(ts15, last_ts)); con = db(); store = ModelStore(con); learner = Learner(store); created = 0; processed = 0; newest = last_ts
    for i in range(start_i, len(m15) - 25):
        if i % 4: continue
        ts = ts15[i]; d1s = _slice_to(d1, tsd, ts, 80, 420); h4s = _slice_to(h4, ts4, ts, 100, 900); h1s = _slice_to(h1, ts1, ts, 100, 1000); btcs = _slice_to(btc, tsb, ts, 50, 500); m15s = m15[max(0, i - 500):i + 1]
        if not all((d1s, h4s, h1s, btcs)): continue
        regime = detect_regime(d1s, h4s, h1s); historical_extras = derivative_history.extras_at(ts); historical_extras['source_agreement_bps'] = 10.0; features = build_features(m15s, h1s, btcs, regime, historical_extras); from adaptive_engine import baseline_strategy_scores; candidates = baseline_strategy_scores(features, regime)
        for strategy, meta in candidates.items():
            if meta['baseline'] < 0.22: continue
            success, pnl_r, mfe_r, mae_r = learner.outcome(m15, i, meta['direction'], 24); store.add_sample({'ts': ts, 'strategy': strategy, 'direction': meta['direction'], 'regime': regime['regime'], 'phase': regime['phase'], 'features': features, 'success': success, 'pnl_r': pnl_r, 'mfe_r': mfe_r, 'mae_r': mae_r, 'source_quality': max(60.0, (82.0 if src15 == 'gate' else 74.0) * (0.85 + 0.15 * historical_extras.get('derivative_coverage', 0.0)))}); created += 1
        processed += 1; newest = ts
        if processed >= batch: break
    store.commit(); con.close()
    if newest > last_ts: set_state('last_learning_sample_ts', newest)
    return created

def ingest_completed_live_samples() -> int:
    con = db(); rows = con.execute("SELECT * FROM signals WHERE status='CLOSED' AND review_label IS NOT NULL ORDER BY exit_ts").fetchall(); store = ModelStore(con); added = 0
    for raw in rows:
        row = dict(raw); payload = json.loads(row['payload'])
        if payload.get('learning_ingested'): continue
        features = payload.get('features') or {}
        if not features:
            payload['learning_ingested'] = 'SKIPPED_NO_FEATURES'; con.execute('UPDATE signals SET payload=? WHERE signal_id=?', (json.dumps(payload, ensure_ascii=False), row['signal_id'])); continue
        mgmt = payload.get('management') or {}; realized = float(row['realized_r'] or 0); store.add_sample({'ts': int(row['created_at']), 'strategy': row['strategy'], 'direction': row['direction'], 'regime': row['regime'], 'phase': row['phase'], 'features': features, 'success': int(realized > 0.15), 'pnl_r': realized, 'mfe_r': max(float(mgmt.get('mfe_r', 0)), float(row['post_mfe_r'] or 0)), 'mae_r': max(float(mgmt.get('mae_r', 0)), float(row['post_mae_r'] or 0)), 'source_quality': max(55.0, float(payload.get('data_quality', 75.0)))}); payload['learning_ingested'] = int(time.time()); con.execute('UPDATE signals SET payload=? WHERE signal_id=?', (json.dumps(payload, ensure_ascii=False), row['signal_id'])); added += 1
    store.commit(); con.commit(); con.close(); return added

def train_if_due(force: bool=False) -> list[dict[str, Any]]:
    last = int(get_state('last_train_ts', 0) or 0)
    if not force and time.time() - last < 6 * 3600: return []
    con = db(); store = ModelStore(con); learner = Learner(store); results = learner.train_all(); con.close()
    if results: set_state('last_train_ts', int(time.time()))
    out = [x.__dict__ for x in results]; state['last_training'] = out; return out

async def learning_tick() -> None:
    live_added = ingest_completed_live_samples(); con = db(); progress = bootstrap_progress(con); con.close(); chosen = None
    for asset, tf in BACKFILL_PLAN:
        earliest = _earliest(asset, tf)
        if earliest is None or earliest > START_TS + 2 * TIMEFRAME_SECONDS[tf]: chosen = (asset, tf); break
    backfill_result = None; derivative_result = None
    if chosen: backfill_result = await backfill_one(*chosen)
    else:
        derivative_history.set_db_path(DB_PATH); derivative_result = await derivative_history.backfill_tick(hub, START_TS, pages=max(1, min(5, BACKFILL_PAGES_PER_TICK))); samples = generate_learning_samples()
        if samples:
            last_train = int(get_state('last_train_ts', 0) or 0)
            if time.time() - last_train > 45 * 60: train_if_due(force=True)
    con = db(); progress = bootstrap_progress(con); counts = {s: con.execute('SELECT COUNT(*) FROM learning_samples WHERE strategy=?', (s,)).fetchone()[0] for s in STRATEGIES}; champions = {}
    for strategy in STRATEGIES:
        row = con.execute("SELECT version,metrics FROM model_registry WHERE strategy=? AND status='CHAMPION' ORDER BY version DESC LIMIT 1", (strategy,)).fetchone(); champions[strategy] = {'version': row[0], **json.loads(row[1])} if row else None
    con.close(); state['learning'] = {'progress': progress, 'backfill': backfill_result, 'derivatives': derivative_history.status(), 'derivative_backfill': derivative_result, 'live_samples_added': live_added, 'sample_counts': counts, 'champions': champions, 'learning_order': ['1D/4H', '1H/30M', '15M/5M', 'derivatives/live/post-exit']}

async def scan_worker() -> None:
    while True:
        try: await scan()
        except Exception as exc: LOG.exception('scan failed'); state.update(service='DEGRADED', error=str(exc))
        await asyncio.sleep(SCAN_SECONDS)

async def learning_worker() -> None:
    while True:
        try: await learning_tick()
        except Exception as exc: LOG.exception('learning tick failed'); state['learning'] = {**state.get('learning', {}), 'error': str(exc)}
        await asyncio.sleep(LEARNING_SLEEP_SECONDS)

@asynccontextmanager
async def lifespan(_: FastAPI):
    db().close(); scan_task = asyncio.create_task(scan_worker()); learning_task = asyncio.create_task(learning_worker()); await asyncio.sleep(0); yield
    for task in (scan_task, learning_task): task.cancel()
    for task in (scan_task, learning_task):
        try: await task
        except asyncio.CancelledError: pass

app = FastAPI(title='ETH Adaptive Short-Term Engine', version='4.2.0', lifespan=lifespan)
@app.get('/health')
def health() -> dict[str, Any]: return {'ok': state['service'] == 'OK', 'service': state['service'], 'updated_at': state['updated_at'], 'error': state['error']}
@app.get('/api/status')
def status() -> dict[str, Any]: return state
@app.post('/api/scan')
async def manual_scan() -> dict[str, Any]: await scan(); return state
@app.get('/api/learning')
def learning_status() -> dict[str, Any]: return state.get('learning', {})
@app.post('/api/learning/tick')
async def manual_learning_tick() -> dict[str, Any]: await learning_tick(); return state.get('learning', {})
@app.post('/api/learning/train')
def manual_train() -> list[dict[str, Any]]: return train_if_due(force=True)
@app.post('/api/equity/{usdt}')
def set_equity(usdt: float) -> dict[str, Any]:
    if usdt <= 0: raise HTTPException(400, 'equity must be > 0')
    set_state('account_equity_usdt', usdt); return {'equity_usdt': usdt, 'risk_per_trade': RISK_PER_TRADE}
@app.get('/api/history')
def history(limit: int=Query(100, ge=1, le=1000)) -> list[dict[str, Any]]:
    con = db(); rows = con.execute('SELECT ts,payload FROM snapshots ORDER BY ts DESC LIMIT ?', (limit,)).fetchall(); con.close(); return [{'ts': r[0], **json.loads(r[1])} for r in rows]
@app.get('/api/signals')
def signals(limit: int=Query(100, ge=1, le=500)) -> list[dict[str, Any]]:
    con = db(); rows = con.execute('SELECT * FROM signals ORDER BY created_at DESC LIMIT ?', (limit,)).fetchall(); con.close(); out = []
    for raw in rows:
        row = dict(raw); row['targets'] = json.loads(row['targets']); row['payload'] = json.loads(row['payload']); row['position_sizing'] = _notional_for_risk(float(row['entry']), float(row['initial_stop'])); out.append(row)
    return out
@app.get('/api/position')
def position_status() -> dict[str, Any]:
    row = latest_signal(('OPEN',)); return {'active': row, 'position_sizing': _notional_for_risk(row['entry'], row['initial_stop']) if row else None}
@app.post('/api/signal/{signal_id}/fill')
def mark_filled(signal_id: str, price: float | None=None) -> dict[str, Any]:
    con = db(); row = con.execute('SELECT * FROM signals WHERE signal_id=?', (signal_id,)).fetchone()
    if not row: con.close(); raise HTTPException(404, 'signal not found')
    if row['status'] != 'PLANNED': con.close(); raise HTTPException(400, 'signal is not planned')
    fill = float(price or row['entry']); payload = json.loads(row['payload']); payload['manual_fill_price'] = fill; con.execute("UPDATE signals SET status='OPEN',filled_at=?,updated_at=?,payload=? WHERE signal_id=?", (int(time.time()), int(time.time()), json.dumps(payload, ensure_ascii=False), signal_id)); con.commit(); con.close(); return {'ok': True, 'signal_id': signal_id, 'fill_reference': fill}
@app.post('/api/signal/{signal_id}/close')
def mark_closed(signal_id: str, price: float) -> dict[str, Any]:
    con = db(); raw = con.execute('SELECT * FROM signals WHERE signal_id=?', (signal_id,)).fetchone(); con.close()
    if not raw: raise HTTPException(404, 'signal not found')
    row = dict(raw); row['payload'] = json.loads(row['payload']); row['targets'] = json.loads(row['targets'])
    if row['status'] != 'OPEN': raise HTTPException(400, 'signal is not open')
    _close_signal(row, price, 'MANUAL_EXIT', int(time.time())); return {'ok': True, 'signal_id': signal_id, 'exit_price': price}
@app.get('/api/models')
def models() -> dict[str, Any]:
    con = db(); out = {}
    for strategy in STRATEGIES:
        rows = con.execute('SELECT version,status,created_at,metrics FROM model_registry WHERE strategy=? ORDER BY version DESC LIMIT 5', (strategy,)).fetchall(); out[strategy] = [{'version': r[0], 'status': r[1], 'created_at': r[2], **json.loads(r[3])} for r in rows]
    con.close(); return out
@app.get('/api/reviews')
def reviews(limit: int=Query(100, ge=1, le=500)) -> list[dict[str, Any]]:
    con = db(); rows = con.execute("SELECT signal_id,strategy,direction,regime,realized_r,exit_reason,post_mfe_r,post_mae_r,review_label FROM signals WHERE status='CLOSED' ORDER BY exit_ts DESC LIMIT ?", (limit,)).fetchall(); con.close(); return [dict(r) for r in rows]
@app.get('/api/backtest')
def backtest(strategy: str | None=None, limit: int=Query(5000, ge=100, le=20000)) -> dict[str, Any]:
    con = db(); strategies = [strategy] if strategy in STRATEGIES else list(STRATEGIES); result = {}
    for name in strategies:
        rows = con.execute('SELECT pnl_r,success,regime FROM learning_samples WHERE strategy=? ORDER BY ts DESC LIMIT ?', (name, limit)).fetchall(); pnls = [float(x[0]) for x in rows]; wins = [int(x[1]) for x in rows]; gains = sum((max(x, 0) for x in pnls)); losses = sum((max(-x, 0) for x in pnls)); by_regime: dict[str, list[float]] = {}
        for row in rows: by_regime.setdefault(row[2], []).append(float(row[0]))
        result[name] = {'n': len(rows), 'win_rate': sum(wins) / len(wins) if wins else 0, 'expectancy_r': statistics.mean(pnls) if pnls else 0, 'profit_factor': gains / max(losses, 1e-09), 'by_regime': {k: {'n': len(v), 'ev_r': statistics.mean(v)} for k, v in by_regime.items()}}
    con.close(); return result
@app.get('/', response_class=HTMLResponse)
def index() -> str:
    return "<!doctype html><html lang='zh-Hant'><head><meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'><title>ETH Adaptive Engine</title><style>body{font-family:system-ui;background:#0b1020;color:#e8edf7;margin:0;padding:24px}.card{background:#151d33;border:1px solid #2a3555;border-radius:16px;padding:18px;margin:12px 0}pre{white-space:pre-wrap}.ok{color:#67e8a5}.warn{color:#ffd166}</style></head><body><h1>ETH Adaptive Short-Term Engine v4.2</h1><div class='card'><b>Learning order</b><p>1D/4H → 1H/30M → 15M/5M → derivatives/live + post-exit review</p></div><div id='app' class='card'>loading...</div><script>async function tick(){let x=await fetch('/api/status').then(r=>r.json());let a=x.analysis||{},l=x.learning||{},s=x.active_signal;document.getElementById('app').innerHTML=`<b class='${x.service==='OK'?'ok':'warn'}'>${x.service}</b><pre>Regime: ${JSON.stringify(a.regime||{},null,2)}\nSelection: ${JSON.stringify(a.selection||{},null,2)}\nActive: ${JSON.stringify(s||null,null,2)}\nLearning: ${JSON.stringify(l.progress||{},null,2)}\nDerivatives: ${JSON.stringify(l.derivatives||{},null,2)}</pre>`}tick();setInterval(tick,5000)</script></body></html>"
if __name__ == '__main__': uvicorn.run('app:app', host='0.0.0.0', port=PORT)
