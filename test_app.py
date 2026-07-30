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
            "MAIN", "LONG", 100.0, 120.0, 108.0, 108.0,
            touch_bar, 68, False, False, 0.1,
            h4, h1, m15, impulse,
        )
        self.assertEqual(first["stage"], 3)
        self.assertTrue(first["zone_reached"])
        self.assertEqual(len(first["targets"]), 4)
        self.assertGreater(first["stop"], 0)

        second = app.make_trade_plan(
            "MAIN", "LONG", 100.0, 120.0, 108.0, 108.0,
            {"h": 109.0, "l": 107.8}, 80, True, False, 0.1,
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
            "MAIN", "LONG", 100.0, 120.0, 108.0, 108.0,
            {"h": 107.0, "l": 105.0}, 80, True, False, 0.1,
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

    def test_retained_wall_never_becomes_marketable_chase_order(self) -> None:
        h4 = sample_candles()
        h1 = sample_candles()
        m15 = sample_candles()
        impulse = {"origin_time": 1, "endpoint_time": 2, "bos_time": 2}
        ready = app.make_trade_plan(
            "MAIN", "LONG", 100.0, 120.0, 108.0, 108.0,
            {"h": 107.0, "l": 105.0}, 80, True, False, 0.1,
            h4, h1, m15, impulse,
        )
        below_wall = app.make_trade_plan(
            "MAIN", "LONG", 100.0, 120.0, 106.0, 108.0,
            {"h": 106.2, "l": 105.8}, 80, True, False, 0.1,
            h4, h1, m15, impulse, ready,
        )
        self.assertFalse(below_wall["ready_now"])
        self.assertEqual(below_wall["status"], "WAIT_WALL_RECLAIM")
        self.assertIn("立即成交", below_wall["action"])

    def test_plan_waits_when_no_30m_wall_exists(self) -> None:
        rising = []
        for i in range(80):
            close = 100.0 + i * 0.2
            rising.append({
                "t": 1_700_000_000 + i * 1800,
                "o": close - 0.1, "h": close + 0.2,
                "l": close - 0.2, "c": close, "v": 100,
            })
        plan = app.make_trade_plan(
            "MAIN", "LONG", 100.0, 120.0, 116.0, 116.0,
            rising[-1], 90, True, True, 0.1,
            rising, rising, rising,
            {"origin_time": 1, "endpoint_time": 2, "bos_time": 2},
            m30=rising,
        )
        self.assertFalse(plan["wall_confirmed_30m"])
        self.assertFalse(plan["ready_now"])
        self.assertEqual(plan["status"], "WAIT_30M_WALL")
        self.assertIn("不使用 5M 回踩價", plan["action"])


class TargetPlanTests(unittest.TestCase):
    def test_far_liquidity_does_not_push_take_profit_farther(self) -> None:
        target_plan = {
            "details": [
                {"price": 130.0, "rr": 3.0, "type": "4H External Liquidity"},
            ]
        }
        targets = app.complete_target_plan("LONG", 100.0, 90.0, target_plan)
        self.assertEqual([x["rr"] for x in targets], list(app.TARGET_R_LEVELS))
        self.assertEqual([x["allocation"] for x in targets], [40, 30, 20, 10])
        self.assertEqual([x["price"] for x in targets], [105.5, 108.5, 111.5, 115.0])

    def test_nearby_real_liquidity_is_kept(self) -> None:
        target_plan = {
            "details": [
                {"price": 106.0, "rr": 0.6, "type": "15M Swing"},
            ]
        }
        targets = app.complete_target_plan("LONG", 100.0, 90.0, target_plan)
        self.assertEqual(targets[0]["price"], 106.0)
        self.assertEqual(targets[0]["type"], "15M Swing")
        self.assertEqual(targets[1]["rr"], 0.85)


class ThirtyMinuteWallTests(unittest.TestCase):
    def test_high_risk_profile_selects_deeper_support_wall(self) -> None:
        candles = []
        special_lows = {5: 107.0, 10: 107.0, 15: 105.0, 20: 105.0, 25: 107.0}
        for i in range(32):
            candles.append({
                "t": 1_700_000_000 + i * 1800,
                "o": 110.0, "h": 111.0,
                "l": special_lows.get(i, 109.0),
                "c": 110.0, "v": 100,
            })
        regular = app.thirty_minute_wall("LONG", candles, [104.0, 108.0], 110.0)
        defensive = app.thirty_minute_wall(
            "LONG", candles, [104.0, 108.0], 110.0, deep_retest=True
        )
        self.assertIsNotNone(regular)
        self.assertIsNotNone(defensive)
        self.assertEqual(regular["price"], 107.0)
        self.assertEqual(defensive["price"], 105.0)
        self.assertLess(defensive["preferred_entry"], defensive["price"])


if __name__ == "__main__":
    unittest.main()
