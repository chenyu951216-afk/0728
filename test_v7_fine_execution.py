import unittest

import v7_fine_execution as fine


class FineExecutionTests(unittest.TestCase):
    def test_stats_exclude_invalid_paths(self):
        rows=[{'invalid_data':True,'filled':False,'pnl_r':0},{'filled':True,'pnl_r':1.0,'cost_r':.1,'stop_pct':.01},{'filled':True,'pnl_r':-1.0,'cost_r':.1,'stop_pct':.01}]
        st=fine.stats_without_data_gaps(rows)
        self.assertEqual(st['opportunities'],2)
        self.assertEqual(st['invalid_data_paths'],1)
        self.assertAlmostEqual(st['valid_path_rate'],2/3)

    def test_five_minute_continuity(self):
        self.assertTrue(fine._continuous([{'ts':0},{'ts':300},{'ts':600}]))
        self.assertFalse(fine._continuous([{'ts':0},{'ts':600}]))


if __name__=='__main__':unittest.main()
