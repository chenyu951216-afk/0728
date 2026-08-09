import unittest

import numpy as np

import adaptive_v5 as base
import execution_v7
import v8_evolution as evo
import v8_execution_oof
import v8_execution_walkforward as wf


class EvolutionTests(unittest.TestCase):
    def test_genomes_have_distinct_search_spaces(self):
        ids = [x['id'] for x in evo.GENOMES]
        self.assertEqual(len(ids), len(set(ids)))
        self.assertGreaterEqual(len(ids), 4)
        feature_counts = {len(evo._indices(x)) for x in evo.GENOMES}
        self.assertGreaterEqual(len(feature_counts), 2)
        self.assertTrue(all(len(evo._indices(x)) >= 8 for x in evo.GENOMES))

    def test_genome_model_accepts_full_feature_vector(self):
        genome = evo.GENOMES[2]
        idx = evo._indices(genome)
        rng = np.random.default_rng(7)
        x = rng.normal(size=(180, len(idx)))
        y = np.array([0, 1] * 90)
        model = evo._estimator(genome, 7)
        model.fit(x, y)
        wrapped = evo.GenomeModel(model, idx, genome['id'])
        full = rng.normal(size=(4, len(base.FEATURE_NAMES)))
        probs = wrapped.predict_proba(full)
        self.assertEqual(probs.shape, (4, 2))
        self.assertTrue(np.all(np.isfinite(probs)))

    def test_execution_search_is_broader_and_deterministic(self):
        a = evo.evolving_policy_candidates('MOMENTUM_CONTINUATION')
        b = evo.evolving_policy_candidates('MOMENTUM_CONTINUATION')
        self.assertEqual(a, b)
        self.assertGreaterEqual(len(a), 48 + evo.EXECUTION_RANDOM_CANDIDATES)
        generated = [x for x in a if x.get('search_origin') == 'EVOLUTION_CONTINUOUS_DEV_ONLY']
        self.assertEqual(len(generated), evo.EXECUTION_RANDOM_CANDIDATES)
        self.assertGreater(max(float(x['stop_atr']) for x in generated), 2.20)
        self.assertGreater(max(float(x['target_rr'][-1]) for x in generated), 3.20)
        self.assertGreater(len({tuple(x['target_rr']) for x in generated}), 20)

    def test_execution_oof_patch_is_explicit(self):
        original = execution_v7._signal_oof_opportunities
        try:
            v8_execution_oof.install()
            self.assertIs(execution_v7._signal_oof_opportunities, v8_execution_oof.evolving_signal_oof_opportunities)
        finally:
            execution_v7._signal_oof_opportunities = original

    def test_live_reaudit_requires_batch_not_single_trade(self):
        self.assertGreaterEqual(evo.LIVE_REAUDIT_BATCH, 3)

    def test_walkforward_uses_multiple_later_unseen_blocks(self):
        ranges = wf._walkforward_ranges(292)
        self.assertGreaterEqual(len(ranges), 3)
        self.assertTrue(all(start >= 72 and end > start for start, end in ranges))
        self.assertTrue(all(ranges[i][1] <= ranges[i + 1][0] for i in range(len(ranges) - 1)))
        aggregate_audit_opportunities = sum(end - start for start, end in ranges)
        self.assertGreater(aggregate_audit_opportunities, 100)
        self.assertGreater(aggregate_audit_opportunities, 55)

    def test_walkforward_can_audit_mid_sized_signal_history(self):
        ranges = wf._walkforward_ranges(137)
        self.assertGreaterEqual(len(ranges), wf.MIN_WF_FOLDS)
        self.assertGreaterEqual(sum(end - start for start, end in ranges), 54)

    def test_walkforward_refuses_too_little_evidence(self):
        self.assertEqual(wf._walkforward_ranges(100), [])
        self.assertGreaterEqual(wf.MIN_WF_OPPORTUNITIES, 110)
        self.assertGreaterEqual(wf.MIN_WF_FOLDS, 3)
        self.assertGreaterEqual(wf.MIN_FOLD_AUDIT_FILLS, 6)


if __name__ == '__main__':
    unittest.main()
