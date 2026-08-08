from __future__ import annotations
import json
import math
import pickle
import random
import sqlite3
import statistics
import time
from dataclasses import dataclass
from typing import Any, Iterable
import numpy as np
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.metrics import brier_score_loss, log_loss
REGIMES = ('BULL_MARKUP', 'BULL_PULLBACK', 'BEAR_MARKDOWN', 'BEAR_RALLY', 'RANGE_LOW_VOL', 'RANGE_HIGH_VOL', 'SQUEEZE', 'EXPANSION_UP', 'EXPANSION_DOWN', 'CAPITULATION', 'REBOUND', 'TRANSITION')
STRATEGIES = ('TREND_PULLBACK', 'LIQUIDITY_SWEEP_REVERSAL', 'SQUEEZE_EXPANSION', 'BREAKOUT_RETEST', 'RANGE_MEAN_REVERSION')
FEATURE_NAMES = ('ret_1', 'ret_4', 'ret_16', 'ema20_gap', 'ema50_gap', 'ema20_slope', 'atr_pct', 'atr_rank', 'adx', 'rsi', 'volume_z', 'range_z', 'wick_ratio', 'dist_vwap_atr', 'bos_up', 'bos_down', 'sweep_low', 'sweep_high', 'fvg_up', 'fvg_down', 'btc_ret_4', 'btc_ret_16', 'eth_btc_rel', 'spot_perp_basis_bps', 'funding', 'oi_change', 'book_imbalance', 'liquidation_imbalance', 'liquidation_intensity', 'oi_available', 'funding_available', 'liquidation_available', 'book_available', 'derivative_coverage', 'derivative_quality', 'source_agreement_bps', 'hour_sin', 'hour_cos', 'dow_sin', 'dow_cos', 'macro_code', 'phase_code')

def f(x: Any, default: float=0.0) -> float:
    try:
        y = float(x)
        return y if math.isfinite(y) else default
    except (TypeError, ValueError):
        return default

def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))

def mean(xs: Iterable[float]) -> float:
    values = list(xs)
    return sum(values) / len(values) if values else 0.0

def ema(values: list[float], n: int) -> float:
    if not values:
        return 0.0
    a = 2.0 / (n + 1)
    out = values[0]
    for value in values[1:]:
        out = a * value + (1 - a) * out
    return out

def sma(values: list[float], n: int) -> float:
    return mean(values[-n:]) if values else 0.0

def stdev(values: list[float]) -> float:
    return statistics.pstdev(values) if len(values) > 1 else 0.0

def atr(cs: list[dict], n: int=14) -> float:
    if len(cs) < 2:
        return 0.0
    tr = []
    for i in range(1, len(cs)):
        x, p = (cs[i], cs[i - 1])
        tr.append(max(f(x['h']) - f(x['l']), abs(f(x['h']) - f(p['c'])), abs(f(x['l']) - f(p['c']))))
    return sma(tr, n)

def rsi(cs: list[dict], n: int=14) -> float:
    if len(cs) < n + 1:
        return 50.0
    closes = [f(x['c']) for x in cs]
    diffs = [closes[i] - closes[i - 1] for i in range(len(closes) - n, len(closes))]
    gains = sum((max(x, 0) for x in diffs)) / n
    losses = sum((max(-x, 0) for x in diffs)) / n
    if losses <= 1e-12:
        return 100.0
    rs = gains / losses
    return 100 - 100 / (1 + rs)

def adx(cs: list[dict], n: int=14) -> float:
    if len(cs) < n + 2:
        return 0.0
    trs, plus, minus = ([], [], [])
    for i in range(1, len(cs)):
        up = f(cs[i]['h']) - f(cs[i - 1]['h'])
        down = f(cs[i - 1]['l']) - f(cs[i]['l'])
        trs.append(max(f(cs[i]['h']) - f(cs[i]['l']), abs(f(cs[i]['h']) - f(cs[i - 1]['c'])), abs(f(cs[i]['l']) - f(cs[i - 1]['c']))))
        plus.append(up if up > down and up > 0 else 0.0)
        minus.append(down if down > up and down > 0 else 0.0)
    trn = sum(trs[-n:]) or 1e-09
    p = 100 * sum(plus[-n:]) / trn
    m = 100 * sum(minus[-n:]) / trn
    return 100 * abs(p - m) / max(p + m, 1e-09)

def percentile_rank(values: list[float], x: float) -> float:
    if not values:
        return 0.5
    return sum((v <= x for v in values)) / len(values)

def rolling_vwap(cs: list[dict], n: int=48) -> float:
    rows = cs[-n:]
    denom = sum((max(f(x.get('v')), 0) for x in rows))
    if denom <= 0:
        return sma([f(x['c']) for x in rows], len(rows))
    return sum(((f(x['h']) + f(x['l']) + f(x['c'])) / 3 * max(f(x.get('v')), 0) for x in rows)) / denom

def pivots(cs: list[dict], span: int=3) -> tuple[list[tuple[int, float]], list[tuple[int, float]]]:
    highs, lows = ([], [])
    for i in range(span, len(cs) - span):
        window = cs[i - span:i + span + 1]
        if f(cs[i]['h']) >= max((f(x['h']) for x in window)):
            highs.append((i, f(cs[i]['h'])))
        if f(cs[i]['l']) <= min((f(x['l']) for x in window)):
            lows.append((i, f(cs[i]['l'])))
    return (highs, lows)

def microstructure_flags(cs: list[dict]) -> dict[str, float]:
    if len(cs) < 12:
        return {'bos_up': 0, 'bos_down': 0, 'sweep_low': 0, 'sweep_high': 0, 'fvg_up': 0, 'fvg_down': 0}
    highs, lows = pivots(cs[:-1], 2)
    last = cs[-1]
    a = max(atr(cs), 1e-09)
    prior_high = highs[-1][1] if highs else max((f(x['h']) for x in cs[-12:-1]))
    prior_low = lows[-1][1] if lows else min((f(x['l']) for x in cs[-12:-1]))
    bos_up = float(f(last['c']) > prior_high + 0.05 * a)
    bos_down = float(f(last['c']) < prior_low - 0.05 * a)
    sweep_low = float(f(last['l']) < prior_low - 0.02 * a and f(last['c']) > prior_low)
    sweep_high = float(f(last['h']) > prior_high + 0.02 * a and f(last['c']) < prior_high)
    x, z = (cs[-3], cs[-1])
    fvg_up = float(f(z['l']) > f(x['h']) and f(z['l']) - f(x['h']) >= 0.05 * a)
    fvg_down = float(f(z['h']) < f(x['l']) and f(x['l']) - f(z['h']) >= 0.05 * a)
    return {'bos_up': bos_up, 'bos_down': bos_down, 'sweep_low': sweep_low, 'sweep_high': sweep_high, 'fvg_up': fvg_up, 'fvg_down': fvg_down}

def _trend_state(cs: list[dict]) -> tuple[int, float, float, float]:
    closes = [f(x['c']) for x in cs]
    a = max(atr(cs), 1e-09)
    e20, e50 = (ema(closes, 20), ema(closes, 50))
    prior_e20 = ema(closes[:-4], 20) if len(closes) > 24 else e20
    slope = (e20 - prior_e20) / a
    direction = 1 if closes[-1] > e20 > e50 and slope > 0 else -1 if closes[-1] < e20 < e50 and slope < 0 else 0
    return (direction, adx(cs), a / max(closes[-1], 1e-09), slope)

def detect_regime(d1: list[dict], h4: list[dict], h1: list[dict]) -> dict[str, Any]:
    d_dir, d_adx, d_atr, d_slope = _trend_state(d1)
    h4_dir, h4_adx, h4_atr, h4_slope = _trend_state(h4)
    h1_dir, h1_adx, h1_atr, h1_slope = _trend_state(h1)
    h4_atrs = []
    for i in range(30, len(h4) + 1):
        window = h4[max(0, i - 80):i]
        h4_atrs.append(atr(window) / max(f(window[-1]['c']), 1e-09))
    vol_rank = percentile_rank(h4_atrs[-180:], h4_atr)
    ret_4h = f(h4[-1]['c']) / max(f(h4[-5]['c']), 1e-09) - 1 if len(h4) >= 5 else 0
    ret_1d = f(d1[-1]['c']) / max(f(d1[-4]['c']), 1e-09) - 1 if len(d1) >= 4 else 0
    drawdown_10d = f(d1[-1]['c']) / max(max((f(x['h']) for x in d1[-10:])), 1e-09) - 1
    if vol_rank <= 0.18 and h4_adx < 18:
        regime = 'SQUEEZE'
    elif d_dir > 0 and h4_dir > 0:
        regime = 'BULL_MARKUP'
    elif d_dir > 0 and h4_dir < 0:
        regime = 'BULL_PULLBACK'
    elif d_dir < 0 and h4_dir < 0:
        regime = 'BEAR_MARKDOWN'
    elif d_dir < 0 and h4_dir > 0:
        regime = 'BEAR_RALLY'
    elif ret_4h > 2.2 * h4_atr:
        regime = 'EXPANSION_UP'
    elif ret_4h < -2.2 * h4_atr:
        regime = 'EXPANSION_DOWN'
    elif drawdown_10d < -0.12 and ret_1d < -0.05 and (vol_rank > 0.8):
        regime = 'CAPITULATION'
    elif drawdown_10d < -0.08 and ret_4h > 1.5 * h4_atr:
        regime = 'REBOUND'
    elif h4_adx < 20:
        regime = 'RANGE_HIGH_VOL' if vol_rank > 0.65 else 'RANGE_LOW_VOL'
    else:
        regime = 'TRANSITION'
    if regime in ('BULL_MARKUP', 'BEAR_MARKDOWN') and h1_dir == -h4_dir:
        phase = 'PULLBACK'
    elif vol_rank < 0.2:
        phase = 'COMPRESSION'
    elif vol_rank > 0.8 and abs(ret_4h) > 1.2 * h4_atr:
        phase = 'EXPANSION'
    elif h1_adx >= 28 and h1_dir != 0:
        phase = 'IMPULSE'
    elif h1_adx < 18:
        phase = 'BALANCE'
    else:
        phase = 'TRANSITION'
    return {'regime': regime, 'phase': phase, 'daily_direction': d_dir, 'h4_direction': h4_dir, 'h1_direction': h1_dir, 'daily_adx': round(d_adx, 2), 'h4_adx': round(h4_adx, 2), 'h1_adx': round(h1_adx, 2), 'volatility_rank': round(vol_rank, 4), 'h4_atr_pct': round(h4_atr, 6), 'daily_slope': round(d_slope, 4), 'h4_slope': round(h4_slope, 4), 'h1_slope': round(h1_slope, 4)}

def strategy_affinity(regime: str, phase: str) -> dict[str, float]:
    table = {s: 0.25 for s in STRATEGIES}
    if regime in ('BULL_MARKUP', 'BEAR_MARKDOWN', 'BULL_PULLBACK', 'BEAR_RALLY'):
        table['TREND_PULLBACK'] = 1.0; table['BREAKOUT_RETEST'] = 0.75
    if regime in ('SQUEEZE', 'EXPANSION_UP', 'EXPANSION_DOWN') or phase == 'COMPRESSION':
        table['SQUEEZE_EXPANSION'] = 1.0; table['BREAKOUT_RETEST'] = max(table['BREAKOUT_RETEST'], 0.8)
    if regime in ('RANGE_LOW_VOL', 'RANGE_HIGH_VOL'):
        table['RANGE_MEAN_REVERSION'] = 1.0; table['LIQUIDITY_SWEEP_REVERSAL'] = 0.85
    if regime in ('CAPITULATION', 'REBOUND', 'TRANSITION'):
        table['LIQUIDITY_SWEEP_REVERSAL'] = 0.9
    return table

def _z(values: list[float], x: float) -> float:
    sd = stdev(values)
    return (x - mean(values)) / sd if sd > 1e-12 else 0.0

def build_features(m15: list[dict], h1: list[dict], btc_h1: list[dict], regime: dict[str, Any], extras: dict[str, Any] | None=None) -> dict[str, float]:
    extras = extras or {}
    closes = [f(x['c']) for x in m15]; last = closes[-1]; a = max(atr(m15), 1e-09); e20, e50 = (ema(closes, 20), ema(closes, 50)); prior20 = ema(closes[:-4], 20) if len(closes) > 25 else e20; atr_now = a / max(last, 1e-09)
    atr_hist = []
    for i in range(max(30, len(m15) - 180), len(m15) + 1):
        w = m15[max(0, i - 80):i]
        if len(w) >= 20:
            atr_hist.append(atr(w) / max(f(w[-1]['c']), 1e-09))
    vols = [f(x.get('v')) for x in m15[-60:-1]]; ranges = [(f(x['h']) - f(x['l'])) / max(f(x['c']), 1e-09) for x in m15[-60:-1]]; current_range = (f(m15[-1]['h']) - f(m15[-1]['l'])) / max(last, 1e-09); body_hi = max(f(m15[-1]['o']), f(m15[-1]['c'])); body_lo = min(f(m15[-1]['o']), f(m15[-1]['c'])); wick_ratio = (f(m15[-1]['h']) - body_hi + (body_lo - f(m15[-1]['l']))) / max(f(m15[-1]['h']) - f(m15[-1]['l']), 1e-09); flags = microstructure_flags(m15); vwap = rolling_vwap(m15); btc = [f(x['c']) for x in btc_h1]; eth_h1 = [f(x['c']) for x in h1]
    btc_ret_4 = btc[-1] / max(btc[-5], 1e-09) - 1 if len(btc) >= 5 else 0; btc_ret_16 = btc[-1] / max(btc[-17], 1e-09) - 1 if len(btc) >= 17 else 0; eth_ret_16 = eth_h1[-1] / max(eth_h1[-17], 1e-09) - 1 if len(eth_h1) >= 17 else 0
    ts = int(f(m15[-1].get('ts', m15[-1].get('t', time.time())))); tm = time.gmtime(ts); macro_code = REGIMES.index(regime['regime']) / max(len(REGIMES) - 1, 1); phases = ('PULLBACK', 'COMPRESSION', 'EXPANSION', 'IMPULSE', 'BALANCE', 'TRANSITION'); phase_code = phases.index(regime['phase']) / (len(phases) - 1)
    out = {'ret_1': last / max(closes[-2], 1e-09) - 1, 'ret_4': last / max(closes[-5], 1e-09) - 1, 'ret_16': last / max(closes[-17], 1e-09) - 1, 'ema20_gap': (last - e20) / a, 'ema50_gap': (last - e50) / a, 'ema20_slope': (e20 - prior20) / a, 'atr_pct': atr_now, 'atr_rank': percentile_rank(atr_hist, atr_now), 'adx': adx(m15) / 100, 'rsi': rsi(m15) / 100, 'volume_z': clamp(_z(vols, f(m15[-1].get('v'))), -5, 5), 'range_z': clamp(_z(ranges, current_range), -5, 5), 'wick_ratio': clamp(wick_ratio, 0, 1), 'dist_vwap_atr': (last - vwap) / a, **flags, 'btc_ret_4': btc_ret_4, 'btc_ret_16': btc_ret_16, 'eth_btc_rel': eth_ret_16 - btc_ret_16, 'spot_perp_basis_bps': f(extras.get('spot_perp_basis_bps')), 'funding': f(extras.get('funding')), 'oi_change': f(extras.get('oi_change')), 'book_imbalance': f(extras.get('book_imbalance')), 'liquidation_imbalance': f(extras.get('liquidation_imbalance')), 'liquidation_intensity': f(extras.get('liquidation_intensity')), 'oi_available': f(extras.get('oi_available')), 'funding_available': f(extras.get('funding_available')), 'liquidation_available': f(extras.get('liquidation_available')), 'book_available': f(extras.get('book_available')), 'derivative_coverage': f(extras.get('derivative_coverage')), 'derivative_quality': f(extras.get('derivative_quality')), 'source_agreement_bps': f(extras.get('source_agreement_bps'), 999.0) / 100, 'hour_sin': math.sin(2 * math.pi * tm.tm_hour / 24), 'hour_cos': math.cos(2 * math.pi * tm.tm_hour / 24), 'dow_sin': math.sin(2 * math.pi * tm.tm_wday / 7), 'dow_cos': math.cos(2 * math.pi * tm.tm_wday / 7), 'macro_code': macro_code, 'phase_code': phase_code}
    return {name: f(out.get(name)) for name in FEATURE_NAMES}

def baseline_strategy_scores(features: dict[str, float], regime: dict[str, Any]) -> dict[str, dict[str, Any]]:
    aff = strategy_affinity(regime['regime'], regime['phase']); trend_sign = 1 if regime['h4_direction'] >= 0 else -1
    long_trend = clamp(0.5 + 0.18 * trend_sign * features['ema20_gap'] + 0.12 * trend_sign * features['ema20_slope'] + 0.1 * features['volume_z'] + 0.15 * (features['sweep_low'] if trend_sign > 0 else features['sweep_high']), 0, 1)
    sweep_long = clamp(0.35 + 0.3 * features['sweep_low'] + 0.12 * (0.5 - features['rsi']) + 0.08 * max(-features['book_imbalance'], 0) * features['book_available'] + 0.08 * max(-features['liquidation_imbalance'], 0) * features['liquidation_available'], 0, 1)
    sweep_short = clamp(0.35 + 0.3 * features['sweep_high'] + 0.12 * (features['rsi'] - 0.5) + 0.08 * max(features['book_imbalance'], 0) * features['book_available'] + 0.08 * max(features['liquidation_imbalance'], 0) * features['liquidation_available'], 0, 1)
    squeeze_dir = 1 if features['ret_4'] + features['ema20_slope'] * 0.01 >= 0 else -1; squeeze_score = clamp(0.3 + 0.2 * (1 - features['atr_rank']) + 0.15 * max(features['volume_z'], 0) + 0.2 * max(features['range_z'], 0), 0, 1)
    breakout_long = clamp(0.25 + 0.32 * features['bos_up'] + 0.13 * max(features['volume_z'], 0) + 0.1 * max(features['oi_change'], 0) * features['oi_available'] + 0.04 * features['derivative_coverage'], 0, 1); breakout_short = clamp(0.25 + 0.32 * features['bos_down'] + 0.13 * max(features['volume_z'], 0) + 0.1 * max(features['oi_change'], 0) * features['oi_available'] + 0.04 * features['derivative_coverage'], 0, 1)
    mr_long = clamp(0.3 + 0.16 * max(-features['dist_vwap_atr'], 0) + 0.15 * max(0.45 - features['rsi'], 0) + 0.12 * features['sweep_low'], 0, 1); mr_short = clamp(0.3 + 0.16 * max(features['dist_vwap_atr'], 0) + 0.15 * max(features['rsi'] - 0.55, 0) + 0.12 * features['sweep_high'], 0, 1)
    raw = {'TREND_PULLBACK': ('LONG' if trend_sign > 0 else 'SHORT', long_trend), 'LIQUIDITY_SWEEP_REVERSAL': ('LONG', sweep_long) if sweep_long >= sweep_short else ('SHORT', sweep_short), 'SQUEEZE_EXPANSION': ('LONG' if squeeze_dir > 0 else 'SHORT', squeeze_score), 'BREAKOUT_RETEST': ('LONG', breakout_long) if breakout_long >= breakout_short else ('SHORT', breakout_short), 'RANGE_MEAN_REVERSION': ('LONG', mr_long) if mr_long >= mr_short else ('SHORT', mr_short)}
    return {k: {'direction': d, 'baseline': clamp(s * aff[k], 0, 1), 'affinity': aff[k]} for k, (d, s) in raw.items()}

def _feature_vector(features: dict[str, float]) -> np.ndarray:
    return np.array([features[name] for name in FEATURE_NAMES], dtype=np.float64)

@dataclass
class StrategyEvaluation:
    strategy: str
    train_n: int
    test_n: int
    train_win: float
    test_win: float
    profit_factor: float
    expectancy_r: float
    brier: float
    logloss: float
    max_drawdown_r: float
    stability: float
    promoted: bool
    reason: str

class ModelStore:
    def __init__(self, con: sqlite3.Connection) -> None:
        self.con = con
        self.con.execute('CREATE TABLE IF NOT EXISTS model_registry(\n            strategy TEXT NOT NULL, version INTEGER NOT NULL, status TEXT NOT NULL,\n            created_at INTEGER NOT NULL, metrics TEXT NOT NULL, model BLOB NOT NULL,\n            PRIMARY KEY(strategy,version))')
        self.con.execute('CREATE TABLE IF NOT EXISTS learning_samples(\n            ts INTEGER NOT NULL, strategy TEXT NOT NULL, direction TEXT NOT NULL,\n            regime TEXT NOT NULL, phase TEXT NOT NULL, features TEXT NOT NULL,\n            success INTEGER NOT NULL, pnl_r REAL NOT NULL, mfe_r REAL NOT NULL, mae_r REAL NOT NULL,\n            source_quality REAL NOT NULL, PRIMARY KEY(ts,strategy,direction))')
        self.con.execute('CREATE INDEX IF NOT EXISTS ix_learning_samples_strategy_ts ON learning_samples(strategy,ts)'); self.con.commit()
    def champion(self, strategy: str) -> tuple[Any | None, dict[str, Any]]:
        row = self.con.execute("SELECT model,metrics,version FROM model_registry WHERE strategy=? AND status='CHAMPION' ORDER BY version DESC LIMIT 1", (strategy,)).fetchone()
        if not row:
            return (None, {})
        return (pickle.loads(row[0]), {**json.loads(row[1]), 'version': row[2]})
    def next_version(self, strategy: str) -> int:
        row = self.con.execute('SELECT MAX(version) FROM model_registry WHERE strategy=?', (strategy,)).fetchone(); return int(row[0] or 0) + 1
    def save_challenger(self, strategy: str, model: Any, metrics: dict[str, Any], promote: bool) -> int:
        version = self.next_version(strategy)
        if promote:
            self.con.execute("UPDATE model_registry SET status='ARCHIVED' WHERE strategy=? AND status='CHAMPION'", (strategy,))
        self.con.execute('INSERT INTO model_registry(strategy,version,status,created_at,metrics,model) VALUES(?,?,?,?,?,?)', (strategy, version, 'CHAMPION' if promote else 'REJECTED', int(time.time()), json.dumps(metrics, ensure_ascii=False), sqlite3.Binary(pickle.dumps(model, protocol=pickle.HIGHEST_PROTOCOL)))); self.con.commit(); return version
    def add_sample(self, row: dict[str, Any]) -> None:
        self.con.execute('INSERT OR IGNORE INTO learning_samples(\n            ts,strategy,direction,regime,phase,features,success,pnl_r,mfe_r,mae_r,source_quality)\n            VALUES(?,?,?,?,?,?,?,?,?,?,?)', (row['ts'], row['strategy'], row['direction'], row['regime'], row['phase'], json.dumps(row['features'], separators=(',', ':')), int(row['success']), row['pnl_r'], row['mfe_r'], row['mae_r'], row.get('source_quality', 100.0)))
    def commit(self) -> None:
        self.con.commit()
    def samples(self, strategy: str, limit: int=20000) -> list[dict[str, Any]]:
        rows = self.con.execute('SELECT ts,direction,regime,phase,features,success,pnl_r,mfe_r,mae_r,source_quality\n                                   FROM learning_samples WHERE strategy=? ORDER BY ts DESC LIMIT ?', (strategy, limit)).fetchall(); out = []
        for x in reversed(rows):
            out.append({'ts': x[0], 'direction': x[1], 'regime': x[2], 'phase': x[3], 'features': json.loads(x[4]), 'success': x[5], 'pnl_r': x[6], 'mfe_r': x[7], 'mae_r': x[8], 'source_quality': x[9]})
        return out

class Learner:
    """Regime-aware champion/challenger learner with purged chronological validation."""
    def __init__(self, store: ModelStore, fee_r: float=0.035) -> None:
        self.store = store; self.fee_r = fee_r
    @staticmethod
    def outcome(cs: list[dict], i: int, direction: str, horizon: int=24) -> tuple[int, float, float, float]:
        entry = f(cs[i]['c']); base_atr = max(atr(cs[:i + 1]), entry * 0.001); stop_dist = 1.2 * base_atr; sign = 1 if direction == 'LONG' else -1; mfe = mae = pnl = 0.0; success = 0
        for bar in cs[i + 1:i + 1 + horizon]:
            favorable = (f(bar['h']) - entry) * sign / stop_dist if sign > 0 else (entry - f(bar['l'])) / stop_dist; adverse = (entry - f(bar['l'])) / stop_dist if sign > 0 else (f(bar['h']) - entry) / stop_dist; mfe, mae = (max(mfe, favorable), max(mae, adverse)); hit_stop = adverse >= 1.0; hit_tp = favorable >= 1.25
            if hit_stop and hit_tp:
                pnl, success = (-1.0, 0); break
            if hit_stop:
                pnl, success = (-1.0, 0); break
            if hit_tp:
                pnl, success = (1.25, 1); break
        else:
            last = f(cs[min(len(cs) - 1, i + horizon)]['c']); pnl = clamp((last - entry) * sign / stop_dist, -1.0, 1.25); success = int(pnl > 0.15)
        return (success, pnl, mfe, mae)
    def predict(self, strategy: str, features: dict[str, float], baseline: float) -> tuple[float, dict[str, Any]]:
        model, meta = self.store.champion(strategy)
        if model is None:
            return (baseline, {'mode': 'BASELINE_UNCERTIFIED', 'model_version': None})
        probability = float(model.predict_proba(_feature_vector(features).reshape(1, -1))[0][1]); probability = 0.88 * probability + 0.12 * baseline
        return (clamp(probability, 0.01, 0.99), {'mode': 'LEARNED_CHAMPION', 'model_version': meta.get('version'), 'metrics': meta})
    @staticmethod
    def _max_drawdown(pnls: list[float]) -> float:
        eq = peak = max_dd = 0.0
        for p in pnls:
            eq += p; peak = max(peak, eq); max_dd = max(max_dd, peak - eq)
        return max_dd
    def train_strategy(self, strategy: str, min_train: int=350, min_test: int=140) -> StrategyEvaluation | None:
        """Train with purged expanding walk-forward folds, then fit the deployable model."""
        rows = [x for x in self.store.samples(strategy) if x['source_quality'] >= 55]
        if len(rows) < min_train + min_test + 64:
            return None
        purge = 32; n = len(rows); first_test = max(min_train + purge, int(n * 0.48)); remaining = n - first_test; fold_count = 4 if remaining >= 4 * min_test else 3 if remaining >= 3 * min_test else 2
        if fold_count < 2:
            return None
        fold_size = max(min_test, remaining // fold_count); oos_rows = []; oos_prob = []; fold_stats = []
        for fold in range(fold_count):
            test_start = first_test + fold * fold_size; test_end = n if fold == fold_count - 1 else min(n, test_start + fold_size); train_end = max(0, test_start - purge); train = rows[:train_end]; test = rows[test_start:test_end]
            if len(train) < min_train or len(test) < max(60, min_test // 2):
                continue
            x_train = np.vstack([_feature_vector(r['features']) for r in train]); y_train = np.array([r['success'] for r in train], dtype=int); x_test = np.vstack([_feature_vector(r['features']) for r in test]); y_test = np.array([r['success'] for r in test], dtype=int)
            if len(set(y_train)) < 2 or len(set(y_test)) < 2:
                continue
            weights = np.array([clamp(r['source_quality'] / 100, 0.5, 1.0) for r in train]); model = HistGradientBoostingClassifier(learning_rate=0.045, max_iter=180, max_leaf_nodes=15, min_samples_leaf=28, l2_regularization=1.2, random_state=42 + fold); model.fit(x_train, y_train, sample_weight=weights); probs = model.predict_proba(x_test)[:, 1]; chosen = [(r, p) for r, p in zip(test, probs) if p >= 0.58]
            if len(chosen) < 35:
                chosen = list(zip(test, probs))
            pnls = [f(r['pnl_r']) - self.fee_r for r, _ in chosen]; gains = sum((max(x, 0) for x in pnls)); losses = sum((max(-x, 0) for x in pnls)); fold_stats.append({'pf': gains / max(losses, 1e-09), 'ev': mean(pnls), 'win': mean((1.0 if x > 0 else 0.0 for x in pnls)), 'n': float(len(pnls))}); oos_rows.extend(test); oos_prob.extend((float(x) for x in probs))
        if len(fold_stats) < 2 or len(oos_rows) < min_test:
            return None
        y_oos = np.array([r['success'] for r in oos_rows], dtype=int); p_oos = np.array(oos_prob, dtype=float)
        if len(set(y_oos)) < 2:
            return None
        chosen = [r for r, p in zip(oos_rows, p_oos) if p >= 0.58]
        if len(chosen) < max(80, min_test // 2):
            chosen = oos_rows
        pnls = [f(r['pnl_r']) - self.fee_r for r in chosen]; gains = sum((max(x, 0) for x in pnls)); losses = sum((max(-x, 0) for x in pnls)); pf = gains / max(losses, 1e-09); expectancy = mean(pnls); test_win = mean((1.0 if x > 0 else 0.0 for x in pnls)); train_win = mean((float(r['success']) for r in rows[:first_test - purge])); fold_evs = [x['ev'] for x in fold_stats]; fold_wins = [x['win'] for x in fold_stats]; ev_dispersion = statistics.pstdev(fold_evs) if len(fold_evs) > 1 else 0.0; win_dispersion = statistics.pstdev(fold_wins) if len(fold_wins) > 1 else 0.0; stability = clamp(1.0 - 0.65 * ev_dispersion - 0.55 * win_dispersion, 0, 1); brier = brier_score_loss(y_oos, p_oos); ll = log_loss(y_oos, p_oos, labels=[0, 1]); max_dd = self._max_drawdown(pnls); worst_fold_ev = min(fold_evs); profitable_folds = sum((x['ev'] > 0 for x in fold_stats)) / len(fold_stats); current_model, current_meta = self.store.champion(strategy); improvement_ok = True; old_oos_ev = old_oos_pf = None
        if current_model is not None:
            try:
                old_probs = current_model.predict_proba(np.vstack([_feature_vector(r['features']) for r in oos_rows]))[:, 1]; old_chosen = [r for r, p in zip(oos_rows, old_probs) if p >= 0.58]
                if len(old_chosen) < max(80, min_test // 2):
                    old_chosen = oos_rows
                old_pnls = [f(r['pnl_r']) - self.fee_r for r in old_chosen]; old_gains = sum((max(x, 0) for x in old_pnls)); old_losses = sum((max(-x, 0) for x in old_pnls)); old_oos_ev = mean(old_pnls); old_oos_pf = old_gains / max(old_losses, 1e-09); improvement_ok = expectancy >= old_oos_ev - 0.015 and pf >= old_oos_pf * 0.97
            except Exception:
                improvement_ok = False
        core_ok = pf >= 1.16 and expectancy >= 0.07 and (stability >= 0.78) and (brier <= 0.255) and (max_dd <= 18) and (worst_fold_ev >= -0.08) and (profitable_folds >= 0.6); promoted = core_ok and improvement_ok; reason = 'purged walk-forward OOS guardrails passed' if promoted else f'rejected: PF={pf:.2f}, EV={expectancy:.3f}R, worstFold={worst_fold_ev:.3f}R, profitableFolds={profitable_folds:.0%}, stability={stability:.2f}, brier={brier:.3f}, DD={max_dd:.1f}R'; x_all = np.vstack([_feature_vector(r['features']) for r in rows]); y_all = np.array([r['success'] for r in rows], dtype=int); weights_all = np.array([clamp(r['source_quality'] / 100, 0.5, 1.0) for r in rows]); final_model = HistGradientBoostingClassifier(learning_rate=0.045, max_iter=180, max_leaf_nodes=15, min_samples_leaf=28, l2_regularization=1.2, random_state=99); final_model.fit(x_all, y_all, sample_weight=weights_all); metrics = {'train_n': first_test - purge, 'test_n': len(oos_rows), 'train_win': train_win, 'test_win': test_win, 'profit_factor': pf, 'expectancy_r': expectancy, 'brier': brier, 'logloss': ll, 'max_drawdown_r': max_dd, 'stability': stability, 'worst_fold_ev_r': worst_fold_ev, 'profitable_fold_ratio': profitable_folds, 'folds': fold_stats, 'old_oos_ev_r': old_oos_ev, 'old_oos_pf': old_oos_pf, 'feature_names': FEATURE_NAMES, 'strategy': strategy, 'reason': reason}; self.store.save_challenger(strategy, final_model, metrics, promoted)
        return StrategyEvaluation(strategy, first_test - purge, len(oos_rows), train_win, test_win, pf, expectancy, brier, ll, max_dd, stability, promoted, reason)
    def train_all(self) -> list[StrategyEvaluation]:
        results = []
        for strategy in STRATEGIES:
            item = self.train_strategy(strategy)
            if item:
                results.append(item)
        return results

def learned_risk_profile(store: ModelStore, strategy: str, regime: str, direction: str) -> dict[str, float]:
    rows = [x for x in store.samples(strategy, 8000) if x['regime'] == regime and x['direction'] == direction]; winners = [x for x in rows if x['pnl_r'] > 0]
    if len(rows) < 80 or len(winners) < 35:
        return {'stop_r': 1.0, 'tp1_r': 0.75, 'tp2_r': 1.25, 'tp3_r': 1.9, 'runner_r': 2.8, 'sample_n': len(rows), 'mode': 'ROBUST_PRIOR'}
    maes = sorted((clamp(f(x['mae_r']), 0.05, 3.0) for x in winners)); mfes = sorted((clamp(f(x['mfe_r']), 0.05, 6.0) for x in winners)); q = lambda xs, p: xs[min(len(xs) - 1, max(0, int((len(xs) - 1) * p)))]; stop_r = clamp(q(maes, 0.88) + 0.08, 0.72, 1.45)
    return {'stop_r': stop_r, 'tp1_r': clamp(q(mfes, 0.35), 0.6, 1.0), 'tp2_r': clamp(q(mfes, 0.55), 0.95, 1.65), 'tp3_r': clamp(q(mfes, 0.72), 1.35, 2.5), 'runner_r': clamp(q(mfes, 0.86), 1.9, 4.2), 'sample_n': len(rows), 'mode': 'LEARNED_MAE_MFE'}

def structure_stop(direction: str, entry: float, m15: list[dict], stop_r: float) -> float:
    a = max(atr(m15), entry * 0.001); highs, lows = pivots(m15[-100:], 2)
    if direction == 'LONG':
        candidates = [p for _, p in lows if p < entry]; structural = max(candidates) if candidates else entry - stop_r * a; return min(structural - 0.1 * a, entry - 0.65 * stop_r * a)
    candidates = [p for _, p in highs if p > entry]; structural = min(candidates) if candidates else entry + stop_r * a; return max(structural + 0.1 * a, entry + 0.65 * stop_r * a)

def risk_plan(store: ModelStore, strategy: str, regime: str, direction: str, entry: float, m15: list[dict]) -> dict[str, Any]:
    profile = learned_risk_profile(store, strategy, regime, direction); stop = structure_stop(direction, entry, m15, profile['stop_r']); risk = abs(entry - stop); sign = 1 if direction == 'LONG' else -1; levels = [profile['tp1_r'], profile['tp2_r'], profile['tp3_r'], profile['runner_r']]; allocations = [25, 30, 25, 20]; targets = [{'price': round(entry + sign * risk * rr, 2), 'rr': round(rr, 2), 'allocation': alloc} for rr, alloc in zip(levels, allocations)]
    return {'entry': round(entry, 2), 'stop': round(stop, 2), 'risk': round(risk, 4), 'targets': targets, 'profile': profile, 'management': {'move_to_be_after_tp1': True, 'trail_after_tp2': True, 'never_widen_stop': True, 'initial_plan_immutable': True}}

def choose_strategy(store: ModelStore, learner: Learner, features: dict[str, float], regime: dict[str, Any], data_quality: float) -> dict[str, Any]:
    baselines = baseline_strategy_scores(features, regime); candidates = []
    for strategy, info in baselines.items():
        probability, model_meta = learner.predict(strategy, features, info['baseline']); quality_penalty = clamp(data_quality / 100, 0.55, 1.0); score = probability * info['affinity'] * quality_penalty; candidates.append({'strategy': strategy, 'direction': info['direction'], 'baseline': info['baseline'], 'probability': probability, 'score': score, 'model': model_meta})
    candidates.sort(key=lambda x: x['score'], reverse=True); best = candidates[0]; certified = best['model'].get('mode') == 'LEARNED_CHAMPION'; tradeable = certified and best['probability'] >= 0.6 and (data_quality >= 70) and (best['score'] >= 0.44)
    return {**best, 'certified': certified, 'tradeable': tradeable, 'candidates': candidates, 'reason': 'learned champion + data quality passed' if tradeable else 'not certified / probability or data quality below gate'}

def bootstrap_progress(con: sqlite3.Connection) -> dict[str, Any]:
    con.execute('CREATE TABLE IF NOT EXISTS market_bars(\n        source TEXT NOT NULL, asset TEXT NOT NULL, tf TEXT NOT NULL, ts INTEGER NOT NULL,\n        o REAL NOT NULL,h REAL NOT NULL,l REAL NOT NULL,c REAL NOT NULL,v REAL NOT NULL,qv REAL NOT NULL DEFAULT 0,\n        PRIMARY KEY(source,asset,tf,ts))'); con.execute('CREATE INDEX IF NOT EXISTS ix_market_bars_asset_tf_ts ON market_bars(asset,tf,ts)'); stages = [('MACRO', ('1d', '4h')), ('STRUCTURE', ('1h', '30m')), ('EXECUTION', ('15m', '5m'))]; details = []
    for stage, tfs in stages:
        counts = {}
        for tf in tfs:
            row = con.execute("SELECT COUNT(*),MIN(ts),MAX(ts) FROM market_bars WHERE asset='ETH' AND tf=?", (tf,)).fetchone(); counts[tf] = {'count': int(row[0] or 0), 'from': row[1], 'to': row[2]}
        details.append({'stage': stage, 'timeframes': counts})
    start_2020 = 1577836800; now = int(time.time()); coverage = []
    for item in details:
        for tf, meta in item['timeframes'].items():
            coverage.append(0.0 if meta['from'] is None or meta['to'] is None else clamp((meta['to'] - meta['from']) / max(now - start_2020, 1), 0, 1))
    return {'stages': details, 'overall': round(mean(coverage) * 100, 2)}
