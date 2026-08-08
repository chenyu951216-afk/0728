import sqlite3
import tempfile
import unittest

from derivative_data import DerivativeHistory
from adaptive_engine import (
    Learner,
    ModelStore,
    baseline_strategy_scores,
    build_features,
    choose_strategy,
    detect_regime,
    risk_plan,
)

def candles(count=220, start=100.0, step=0.25, tf=3600, volume=1000.0):
    rows=[]; price=start
    for i in range(count):
        price += step; body=max(abs(step),0.05); o=price-step*0.45; c=price
        rows.append({"t":1_600_000_000+i*tf,"o":o,"h":max(o,c)+body*0.8,"l":min(o,c)-body*0.8,"c":c,"v":volume+(i%17)*7})
    return rows

class RegimeTests(unittest.TestCase):
    def test_top_down_bull_market_is_not_classified_bear(self):
        regime=detect_regime(candles(220,100,1.0,86400),candles(260,100,0.28,14400),candles(320,100,0.08,3600))
        self.assertIn(regime["regime"],{"BULL_MARKUP","EXPANSION_UP"}); self.assertGreaterEqual(regime["daily_direction"],0); self.assertGreaterEqual(regime["h4_direction"],0)
    def test_top_down_bear_market_is_not_classified_bull(self):
        regime=detect_regime(candles(220,500,-1.0,86400),candles(260,500,-0.28,14400),candles(320,500,-0.08,3600))
        self.assertIn(regime["regime"],{"BEAR_MARKDOWN","EXPANSION_DOWN"}); self.assertLessEqual(regime["daily_direction"],0); self.assertLessEqual(regime["h4_direction"],0)

class LearningSafetyTests(unittest.TestCase):
    def test_ambiguous_ohlc_tp_and_stop_is_counted_as_loss(self):
        cs=candles(100,100,0.02,900); i=70; entry=cs[i]["c"]; cs[i+1]={**cs[i+1],"h":entry*1.10,"l":entry*0.90}; success,pnl_r,_,_=Learner.outcome(cs,i,"LONG",horizon=8); self.assertEqual(success,0); self.assertEqual(pnl_r,-1.0)
    def test_uncertified_baseline_cannot_create_tradeable_signal(self):
        con=sqlite3.connect(":memory:"); store=ModelStore(con); learner=Learner(store); features={name:0.0 for name in __import__("adaptive_engine").FEATURE_NAMES}; regime={"regime":"BULL_MARKUP","phase":"IMPULSE","h4_direction":1}; result=choose_strategy(store,learner,features,regime,data_quality=100); self.assertFalse(result["certified"]); self.assertFalse(result["tradeable"])
    def test_regime_affinity_changes_strategy_priority(self):
        features={name:0.0 for name in __import__("adaptive_engine").FEATURE_NAMES}; bull=baseline_strategy_scores(features,{"regime":"BULL_MARKUP","phase":"IMPULSE","h4_direction":1}); rng=baseline_strategy_scores(features,{"regime":"RANGE_LOW_VOL","phase":"BALANCE","h4_direction":0}); self.assertGreater(bull["TREND_PULLBACK"]["affinity"],bull["RANGE_MEAN_REVERSION"]["affinity"]); self.assertGreater(rng["RANGE_MEAN_REVERSION"]["affinity"],rng["TREND_PULLBACK"]["affinity"])

class RiskPlanTests(unittest.TestCase):
    def test_long_plan_never_places_stop_above_entry_and_targets_are_ordered(self):
        con=sqlite3.connect(":memory:"); store=ModelStore(con); m15=candles(180,2500,0.4,900); entry=m15[-1]["c"]; plan=risk_plan(store,"TREND_PULLBACK","BULL_MARKUP","LONG",entry,m15); self.assertLess(plan["stop"],entry); target_prices=[x["price"] for x in plan["targets"]]; self.assertEqual(target_prices,sorted(target_prices)); self.assertTrue(plan["management"]["never_widen_stop"]); self.assertTrue(plan["management"]["initial_plan_immutable"])
    def test_short_plan_never_places_stop_below_entry_and_targets_are_ordered(self):
        con=sqlite3.connect(":memory:"); store=ModelStore(con); m15=candles(180,2500,-0.4,900); entry=m15[-1]["c"]; plan=risk_plan(store,"TREND_PULLBACK","BEAR_MARKDOWN","SHORT",entry,m15); self.assertGreater(plan["stop"],entry); target_prices=[x["price"] for x in plan["targets"]]; self.assertEqual(target_prices,sorted(target_prices,reverse=True))

class DerivativeAvailabilityTests(unittest.TestCase):
    def test_missing_derivatives_are_marked_missing_not_neutral_available(self):
        with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
            extras=DerivativeHistory(tmp.name,coinglass_key="").extras_at(1_800_000_000); self.assertEqual(extras["derivative_coverage"],0.0); self.assertEqual(extras["oi_available"],0.0); self.assertEqual(extras["funding_available"],0.0); self.assertEqual(extras["liquidation_available"],0.0); self.assertEqual(extras["book_available"],0.0)

class DerivativeCursorTests(unittest.IsolatedAsyncioTestCase):
    async def test_empty_coinglass_windows_advance_persistent_cursor(self):
        class EmptyHub:
            async def fetch_bybit_oi_history(self,*args,**kwargs): return []
            async def fetch_funding_history(self,*args,**kwargs): return []
        with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
            history=DerivativeHistory(tmp.name,coinglass_key="test")
            async def empty(*args,**kwargs): return 0
            history._backfill_coinglass_oi=empty
            history._backfill_coinglass_liquidation=empty
            history._backfill_coinglass_book=empty
            start=1_577_836_800
            await history.backfill_tick(EmptyHub(),start,pages=1)
            first=int(history._get_state("cg_cursor:oi_usd",start))
            await history.backfill_tick(EmptyHub(),start,pages=1)
            second=int(history._get_state("cg_cursor:oi_usd",start))
            self.assertGreater(first,start)
            self.assertGreater(second,first)

class FeatureTests(unittest.TestCase):
    def test_feature_builder_is_finite_with_missing_derivatives(self):
        m15=candles(220,2000,0.15,900); h1=candles(220,1900,0.35,3600); btc=candles(220,50000,2.0,3600); regime=detect_regime(candles(220,1000,1.0,86400),candles(220,1000,0.4,14400),h1); features=build_features(m15,h1,btc,regime,extras={}); self.assertTrue(features); self.assertTrue(all(isinstance(v,float) for v in features.values()))

if __name__=="__main__": unittest.main()
