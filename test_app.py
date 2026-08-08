import sqlite3
import tempfile
import unittest

import numpy as np

from adaptive_engine import build_features, detect_regime
from adaptive_v5 import (
    DIRECTIONS, FEATURE_NAMES, Learner, ModelStore, adaptive_entry,
    baseline_direction_scores, choose_strategy, risk_plan,
)
from derivative_data import DerivativeHistory


def candles(count=260, start=100.0, step=0.2, tf=3600, volume=1000.0):
    out=[]; price=start
    for i in range(count):
        price += step
        body=max(abs(step),0.05)
        o=price-step*0.45; c=price
        out.append({'ts':1_600_000_000+i*tf,'t':1_600_000_000+i*tf,'o':o,'h':max(o,c)+body*0.8,'l':min(o,c)-body*0.8,'c':c,'v':volume+(i%13)*11})
    return out


class DummyModel:
    def predict_proba(self, x):
        return np.tile(np.array([[.2,.8]]), (len(x),1))


class RegimeTests(unittest.TestCase):
    def test_bull_and_bear_separation(self):
        bull=detect_regime(candles(220,100,1,86400),candles(260,100,.3,14400),candles(320,100,.08,3600))
        bear=detect_regime(candles(220,500,-1,86400),candles(260,500,-.3,14400),candles(320,500,-.08,3600))
        self.assertGreaterEqual(bull['daily_direction'],0)
        self.assertLessEqual(bear['daily_direction'],0)


class DirectionLearningTests(unittest.TestCase):
    def test_direction_priors_are_independent(self):
        m15=candles(260,2000,.3,900); h1=candles(260,1900,.5,3600); btc=candles(260,50000,3,3600)
        regime=detect_regime(candles(220,1000,1,86400),candles(260,1000,.4,14400),h1)
        priors=baseline_direction_scores(build_features(m15,h1,btc,regime,{}),regime)
        self.assertTrue(priors)
        self.assertEqual(set(next(iter(priors.values())).keys()),set(DIRECTIONS))

    def test_legacy_registry_migrates_direction_column(self):
        con=sqlite3.connect(':memory:')
        con.execute('CREATE TABLE model_registry(strategy TEXT NOT NULL,version INTEGER NOT NULL,status TEXT NOT NULL,created_at INTEGER NOT NULL,metrics TEXT NOT NULL,model BLOB NOT NULL,PRIMARY KEY(strategy,version))')
        ModelStore(con)
        cols={r[1] for r in con.execute('PRAGMA table_info(model_registry)').fetchall()}
        self.assertIn('direction',cols)

    def test_long_short_champions_are_independent(self):
        con=sqlite3.connect(':memory:'); store=ModelStore(con)
        meta={'threshold':.55,'allowed_regimes':['BULL_MARKUP']}
        store.save_challenger('TREND_PULLBACK','LONG',DummyModel(),meta,True)
        store.save_challenger('TREND_PULLBACK','SHORT',DummyModel(),meta,True)
        self.assertIsNotNone(store.champion('TREND_PULLBACK','LONG')[0])
        self.assertIsNotNone(store.champion('TREND_PULLBACK','SHORT')[0])

    def test_certified_tradeable_candidate_not_blocked_by_uncertified_research_candidate(self):
        con=sqlite3.connect(':memory:'); store=ModelStore(con)
        store.save_challenger('TREND_PULLBACK','LONG',DummyModel(),{'threshold':.55,'allowed_regimes':['BULL_MARKUP'],'profit_factor':1.4,'expectancy_r':.12},True)
        features={name:0.0 for name in FEATURE_NAMES}; features['ema20_gap']=.3; features['ema20_slope']=.2
        result=choose_strategy(store,Learner(store),features,{'regime':'BULL_MARKUP','phase':'IMPULSE','h4_direction':1},100)
        self.assertTrue(result['tradeable'])
        self.assertEqual((result['strategy'],result['direction']),('TREND_PULLBACK','LONG'))

    def test_champion_is_blocked_outside_its_profitable_regimes(self):
        con=sqlite3.connect(':memory:'); store=ModelStore(con)
        store.save_challenger('TREND_PULLBACK','LONG',DummyModel(),{'threshold':.55,'allowed_regimes':['BULL_MARKUP']},True)
        features={name:0.0 for name in FEATURE_NAMES
        }
        result=choose_strategy(store,Learner(store),features,{'regime':'RANGE_LOW_VOL','phase':'BALANCE','h4_direction':0},100)
        self.assertFalse(result['tradeable'])


class ExecutionTests(unittest.TestCase):
    def test_same_bar_tp_and_stop_is_conservative_loss(self):
        cs=candles(100,100,.02,900); i=70; entry=cs[i]['c']; cs[i+1]={**cs[i+1],'h':entry*1.10,'l':entry*.90}
        success,pnl,*_=Learner.outcome(cs,i,'LONG',8)
        self.assertEqual((success,pnl),(0,-1.0))

    def test_strategy_unfilled_limit_is_not_cherry_picked_as_winner(self):
        cs=candles(120,100,1.0,900); i=80
        success,pnl,mfe,mae=Learner.strategy_outcome(cs,i,'TREND_PULLBACK','LONG',12)
        self.assertEqual(success,0)
        self.assertEqual(pnl,0.0)
        self.assertEqual((mfe,mae),(0.0,0.0))

    def test_adaptive_entry_is_passive(self):
        con=sqlite3.connect(':memory:'); store=ModelStore(con); m15=candles(220,2000,.2,900); live=m15[-1]['c']
        self.assertLess(adaptive_entry(store,'TREND_PULLBACK','BULL_MARKUP','LONG',live,m15),live)
        self.assertGreater(adaptive_entry(store,'TREND_PULLBACK','BEAR_MARKDOWN','SHORT',live,m15),live)

    def test_risk_plan_order_and_no_widen(self):
        con=sqlite3.connect(':memory:'); store=ModelStore(con); m15=candles(220,2000,.2,900); entry=m15[-1]['c']
        plan=risk_plan(store,'TREND_PULLBACK','BULL_MARKUP','LONG',entry,m15)
        self.assertLess(plan['stop'],entry)
        self.assertEqual([x['price'] for x in plan['targets']],sorted(x['price'] for x in plan['targets']))
        self.assertTrue(plan['management']['never_widen_stop'])


class FeatureAndDerivativeTests(unittest.TestCase):
    def test_feature_builder_finite_when_derivatives_missing(self):
        m15=candles(220,2000,.15,900); h1=candles(220,1900,.35,3600); btc=candles(220,50000,2,3600)
        regime=detect_regime(candles(220,1000,1,86400),candles(220,1000,.4,14400),h1)
        features=build_features(m15,h1,btc,regime,{})
        self.assertTrue(all(isinstance(v,float) and np.isfinite(v) for v in features.values()))

    def test_missing_derivatives_are_explicitly_unavailable(self):
        with tempfile.NamedTemporaryFile(suffix='.db') as tmp:
            extras=DerivativeHistory(tmp.name,coinglass_key='').extras_at(1_800_000_000)
            self.assertEqual(extras['derivative_coverage'],0.0)
            self.assertEqual(extras['oi_available'],0.0)
            self.assertEqual(extras['liquidation_available'],0.0)


class DerivativeCursorTests(unittest.IsolatedAsyncioTestCase):
    async def test_empty_coinglass_windows_advance_cursor(self):
        class EmptyHub:
            async def fetch_bybit_oi_history(self,*args,**kwargs): return []
            async def fetch_funding_history(self,*args,**kwargs): return []
        with tempfile.NamedTemporaryFile(suffix='.db') as tmp:
            history=DerivativeHistory(tmp.name,coinglass_key='test')
            async def empty(*args,**kwargs): return 0
            history._backfill_coinglass_oi=empty; history._backfill_coinglass_liquidation=empty; history._backfill_coinglass_book=empty
            start=1_577_836_800
            await history.backfill_tick(EmptyHub(),start,pages=1); first=int(history._get_state('cg_cursor:oi_usd',start))
            await history.backfill_tick(EmptyHub(),start,pages=1); second=int(history._get_state('cg_cursor:oi_usd',start))
            self.assertGreater(first,start); self.assertGreater(second,first)


if __name__=='__main__': unittest.main()
