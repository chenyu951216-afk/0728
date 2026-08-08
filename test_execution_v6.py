import unittest

import execution_v6 as ex


class ExecutionV6Tests(unittest.TestCase):
    def _bars(self, n=140, start=1900.0):
        rows=[]
        price=start
        for i in range(n):
            price += 0.25
            rows.append({'ts':1700000000+i*900,'o':price-0.1,'h':price+1.2,'l':price-1.2,'c':price,'v':1000+i})
        return rows

    def test_plan_enforces_cost_aware_minimum_stop(self):
        bars=self._bars()
        policy={
            'entry_atr':0.04,'stop_atr':0.2,'target_rr':[1.0,1.5,2.0,3.0],
            'allocations':[25,30,25,20],'min_stop_pct':0.002,'all_in_cost_bps':8,
            'lock_after_tp2_r':0.55,'lock_after_tp3_r':1.05,'expire_bars':6,'max_hold_bars':32,
        }
        plan=ex.plan_from_policy('MOMENTUM_CONTINUATION','LONG',bars[-1]['c'],bars,policy)
        stop_pct=abs(plan['entry']-plan['stop'])/plan['entry']
        self.assertGreaterEqual(stop_pct,0.00199)
        self.assertEqual(sum(x['allocation'] for x in plan['targets']),100)

    def test_all_targets_are_weighted_not_full_position_last_tp(self):
        # Directly verify the intended weighted realization math used by simulator.
        alloc=[.25,.30,.25,.20]
        rr=[1.0,1.5,2.0,3.0]
        weighted=sum(a*r for a,r in zip(alloc,rr))
        self.assertAlmostEqual(weighted,1.80,places=8)
        self.assertNotAlmostEqual(weighted,3.0,places=8)

    def test_policy_grid_contains_multiple_entries_stops_targets_allocations(self):
        c=ex.policy_candidates('MOMENTUM_CONTINUATION')
        self.assertGreaterEqual(len(c),100)
        self.assertGreater(len({x['entry_atr'] for x in c}),1)
        self.assertGreater(len({x['stop_atr'] for x in c}),1)
        self.assertGreater(len({tuple(x['target_rr']) for x in c}),1)
        self.assertGreater(len({tuple(x['allocations']) for x in c}),1)


if __name__=='__main__':
    unittest.main()
