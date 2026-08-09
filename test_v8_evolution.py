import unittest

import numpy as np

import adaptive_v5 as base
import execution_v7
import v8_evolution as evo
import v8_execution_oof


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


if __name__ == '__main__':
    unittest.main()
