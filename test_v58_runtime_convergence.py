from __future__ import annotations

import time
import unittest

import v58_runtime_convergence as v58


class V58RuntimeConvergenceTests(unittest.TestCase):
    def setUp(self):
        v58._ENDPOINT_CACHE.clear()

    def test_dashboard_governor_clamps_and_pauses_polling(self):
        js = v58._governor_script()
        self.assertIn('v58-dashboard-governor', js)
        self.assertIn('Math.max(V58_MIN', js)
        self.assertIn('document.hidden', js)
        self.assertIn("url.startsWith('/api/')", js)
        self.assertIn(str(v58.DASHBOARD_MIN_POLL_MS), js)

    def test_endpoint_cache_reuses_value_within_ttl(self):
        calls = {'n': 0}

        def fn():
            calls['n'] += 1
            return {'n': calls['n']}

        a = v58._cached_call('/x', fn, 1.0)
        b = v58._cached_call('/x', fn, 1.0)
        self.assertEqual(a, b)
        self.assertEqual(calls['n'], 1)

    def test_endpoint_cache_refreshes_after_ttl(self):
        calls = {'n': 0}

        def fn():
            calls['n'] += 1
            return calls['n']

        self.assertEqual(v58._cached_call('/x', fn, 0.05), 1)
        time.sleep(0.06)
        self.assertEqual(v58._cached_call('/x', fn, 0.05), 2)

    def test_banner_distinguishes_final_runtime_from_research_semantics(self):
        html = v58._runtime_banner()
        self.assertIn('Production Runtime V58', html)
        self.assertIn('研究/回測語意：V56', html)
        self.assertIn('Live hook 對接：V57', html)


if __name__ == '__main__':
    unittest.main()
