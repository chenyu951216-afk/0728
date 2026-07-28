import unittest

import app


def sample_candles(count: int = 80, base: float = 108.0) -> list[dict]:
    candles = []
    for i in range(count):
        close = base + ((i % 5) - 2) * 0.2
        candles.append({
            "t": 1_700_000_000 + i * 3600,
            "o": close - 0.1,
            "h": close + 1.2,
            "l": close - 1.2,
            "c": close,
            "v": 100 + i,
        })
    return candles


class MainDirectionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.previous_short = {
            "direction": "SHORT",
            "valid": True,
            "status": "ACTIVE",
            "impulse": [100.0, 120.0],
            "bos_time": 100,
        }

    def test_retracement_bias_cannot_flip_main_plan(self) -> None:
        impulses = {
            "LONG": {"valid": True, "bos_time": 200},
            "SHORT": {"valid": True, "bos_time": 100},
        }
        direction, reason, reversed_plan = app.anchored_main_direction(
            "LONG", 115.0, impulses, self.previous_short
        )
        self.assertEqual(direction, "SHORT")
        self.assertFalse(reversed_plan)
        self.assertIn("維持 SHORT", reason)

    def test_true_reversal_requires_origin_break_and_new_opposite_bos(self) -> None:
        impulses = {
            "LONG": {"valid": True, "bos_time": 200},
            "SHORT": {"valid": False, "bos_time": 100},
        }
        direction, _, reversed_plan = app.anchored_main_direction(
            "LONG", 121.0, impulses, self.previous_short
        )
        self.assertEqual(direction, "LONG")
        self.assertTrue(reversed_plan)

    def test_break_without_opposite_bos_does_not_invent_reverse_plan(self) -> None:
        impulses = {
            "LONG": {"valid": False, "bos_time": 0},
            "SHORT": {"valid": False, "bos_time": 100},
        }
        direction, reason, reversed_plan = app.anchored_main_direction(
            "LONG", 121.0, impulses, self.previous_short
        )
        self.assertEqual(direction, "SHORT")
        self.assertFalse(reversed_plan)
        self.assertIn("等待反向 1H BOS", reason)

    def test_invalid_plan_is_not_resurrected_by_old_historical_bos(self) -> None:
        invalid_previous = {**self.previous_short, "valid": False, "status": "INVALID"}
        impulses = {
            "LONG": {"valid": True, "bos_time": 90},
            "SHORT": {"valid": True, "bos_time": 100},
        }
        direction, reason, reversed_plan = app.anchored_main_direction(
            "LONG", 121.0, impulses, invalid_previous
        )
        self.assertEqual(direction, "觀察")
        self.assertFalse(reversed_plan)
        self.assertIn("尚無更新的 1H BOS", reason)


class PlanLifecycleTests(unittest.TestCase):
    def test_zone_touch_persists_and_can_reach_l4_after_reaction(self) -> None:
        h4 = sample_candles()
        h1 = sample_candles()
        m15 = sample_candles()
        touch_bar = {"h": 107.0, "l": 105.0}
        impulse = {
            "origin_time": 1, "endpoint_time": 2, "bos_time": 2,
        }
        first = app.make_trade_plan(
            "MAIN", "LONG", 100.0, 120.0, 106.0, 108.0,
            touch_bar, 68, False, False, 0.1,
            h4, h1, m15, impulse,
        )
        self.assertEqual(first["stage"], 3)
        self.assertTrue(first["zone_reached"])
        self.assertEqual(len(first["targets"]), 4)
        self.assertGreater(first["stop"], 0)

        second = app.make_trade_plan(
            "MAIN", "LONG", 100.0, 120.0, 108.0, 108.0,
            {"h": 109.0, "l": 107.8}, 80, True, True, 0.1,
            h4, h1, m15, impulse, first,
        )
        self.assertEqual(second["stage"], 4)
        self.assertTrue(second["ready_now"])
        self.assertEqual(second["zone"], first["zone"])
        self.assertEqual(second["stop"], first["stop"])
        self.assertEqual(second["targets"], first["targets"])

    def test_l4_peak_does_not_disappear_when_trigger_cools(self) -> None:
        h4 = sample_candles()
        h1 = sample_candles()
        m15 = sample_candles()
        impulse = {"origin_time": 1, "endpoint_time": 2, "bos_time": 2}
        ready = app.make_trade_plan(
            "MAIN", "LONG", 100.0, 120.0, 106.0, 108.0,
            {"h": 107.0, "l": 105.0}, 80, True, True, 0.1,
            h4, h1, m15, impulse,
        )
        cooled = app.make_trade_plan(
            "MAIN", "LONG", 100.0, 120.0, 110.0, 108.0,
            {"h": 110.5, "l": 109.5}, 60, False, False, 0.1,
            h4, h1, m15, impulse, ready,
        )
        self.assertEqual(cooled["stage"], 4)
        self.assertFalse(cooled["ready_now"])
        self.assertEqual(cooled["status"], "COOLDOWN")
        self.assertIn("不新增追價單", cooled["action"])


if __name__ == "__main__":
    unittest.main()
