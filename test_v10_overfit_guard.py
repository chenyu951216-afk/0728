import unittest

import v10_overfit_guard as guard


class OverfitGuardTests(unittest.TestCase):
    def test_overlapping_signals_count_as_one_effective_observation(self):
        rows=[{'ts':1000+i*1800,'pnl_r':1.0,'success':1} for i in range(8)]
        clustered=guard._cluster_rows(rows)
        self.assertEqual(len(clustered),1)
        stats=guard.clustered_stats(rows,0.0)
        self.assertEqual(stats['raw_n'],8)
        self.assertEqual(stats['effective_n'],1)

    def test_signals_after_outcome_horizon_are_independent(self):
        rows=[{'ts':1000,'pnl_r':1.0,'success':1},{'ts':1000+guard.CLUSTER_SECONDS,'pnl_r':-1.0,'success':0},{'ts':1000+2*guard.CLUSTER_SECONDS,'pnl_r':1.0,'success':1}]
        self.assertEqual(len(guard._cluster_rows(rows)),3)

    def test_bootstrap_lower_bound_is_negative_for_unstable_edge(self):
        pnls=[1.0,-1.0]*20
        self.assertLess(guard._bootstrap_ci05(pnls,123),0.0)

    def test_bootstrap_lower_bound_positive_for_consistent_edge(self):
        pnls=[0.30,0.20,0.25,0.35,0.15]*20
        self.assertGreater(guard._bootstrap_ci05(pnls,123),0.0)


if __name__=='__main__': unittest.main()
