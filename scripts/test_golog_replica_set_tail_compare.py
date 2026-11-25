"""Regression tests for strict noise ties and incomplete paired rounds."""

import unittest

from golog_replica_set_tail_compare import contrasts, ENDPOINTS


class PairingTests(unittest.TestCase):
    def test_integer_noise_tie_does_not_qualify(self):
        windows = []
        for epoch in (1, 2):
            for round_number in (1, 2):
                for arm, offset_ns in zip("ABCDE", (0, 1, 3, 8, 9)):
                    latency_us = (100000 + round_number + offset_ns) / 1000
                    tails = {"set_success": {}, "get_success": {}}
                    for op, field in ENDPOINTS:
                        tails[f"{op}_success"][field] = latency_us
                    windows.append({"phase": f"formal-{epoch}", "round": round_number,
                                    "config": {"target_rate": 3000}, "arm": arm,
                                    "get_set_tails": tails, "scope_met": True, "primary_load_met": True})
        effects = contrasts(windows)["3000"]["set_p99_us"]["contrasts"]
        self.assertEqual(effects["B-A"]["median_us"], effects["B-A"]["threshold_us"])
        self.assertFalse(effects["B-A"]["repeats_above_noise"])
        self.assertTrue(effects["E-A"]["repeats_above_noise"])
        self.assertEqual(effects["(D-C)-(B-A)"]["median_us"], .004)
        windows[-1]["primary_load_met"] = False
        effects = contrasts(windows)["3000"]["set_p99_us"]["contrasts"]
        self.assertEqual(effects["E-A"]["valid_pairs"], 3)
        self.assertFalse(effects["E-A"]["repeats_above_noise"])
        self.assertTrue(effects["D-C"]["repeats_above_noise"])


if __name__ == "__main__":
    unittest.main()
