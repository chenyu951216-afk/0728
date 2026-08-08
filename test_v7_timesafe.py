import bisect
import unittest

import v7_timesafe_learning as ts
import v7_execution_alignment as ea


class TimeSafeLearningTests(unittest.TestCase):
    def test_higher_timeframe_bar_requires_its_close(self):
        h1=[{'ts':0,'c':1},{'ts':3600,'c':2},{'ts':7200,'c':3}]
        got=ts.closed_slice(h1,3600,4500,1,20)
        self.assertEqual([x['ts'] for x in got],[0])
        got2=ts.closed_slice(h1,3600,7200,1,20)
        self.assertEqual([x['ts'] for x in got2],[0,3600])

    def test_model_source_agreement_is_not_fabricated(self):
        def builder(*args,**kwargs): return {'ret_1':.1,'source_agreement_bps':9.99}
        out=ts.model_safe_features(builder)
        self.assertEqual(out['source_agreement_bps'],0.0)

    def test_execution_eligibility_timestamp_matches_15m_decision_close(self):
        key=3600+3600-ea.DECISION_TF_SECONDS
        self.assertEqual(key,6300)
        self.assertEqual(bisect.bisect_right([key],5400),0)
        self.assertEqual(bisect.bisect_right([key],6300),1)

    def test_continuity_filter_rejects_missing_candle(self):
        good=[{'ts':i*900} for i in range(10)]
        bad=[{'ts':0},{'ts':900},{'ts':2700},{'ts':3600}]
        self.assertTrue(ts.continuous_tail(good,900,10))
        self.assertFalse(ts.continuous_tail(bad,900,4))


if __name__=='__main__': unittest.main()
