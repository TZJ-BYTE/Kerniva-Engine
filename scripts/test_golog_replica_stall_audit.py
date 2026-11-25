import unittest

from golog_replica_stall_audit import SECOND, THRESHOLD_US, marker_clock_overlap, operation_values, select_controls


def row(second, triggered=True, window="formal-1/A", arm="A", stage="formal-1", epoch=1000):
    return {"second": second, "triggered": triggered, "window": window, "arm": arm, "stage": stage,
            "rate": 6000, "read_percent": 95, "window_start_lo_ns": epoch, "id": f"{window}@{second:02}"}


class StallAuditTests(unittest.TestCase):
    def test_uncertain_clock_half_open_boundary(self):
        start = [100, 110]
        self.assertEqual(marker_clock_overlap(start, [110, SECOND + 99], 0), "inside")
        self.assertEqual(marker_clock_overlap(start, [102, 108], 0), "boundary")
        self.assertEqual(marker_clock_overlap(start, [SECOND + 100, SECOND + 100], 0), "boundary")
        self.assertEqual(marker_clock_overlap(start, [SECOND + 100, SECOND + 100], 1), "boundary")
        self.assertIsNone(marker_clock_overlap(start, [SECOND + 110, SECOND + 110], 0))
        self.assertEqual(marker_clock_overlap(start, [SECOND + 110, SECOND + 110], 1), "inside")
        self.assertIsNone(marker_clock_overlap(start, [95, 99], 0))

    def test_nearest_each_side_and_preserve_absent_side(self):
        group = [row(0), row(1, False), row(2), row(3), row(4, False), row(5)]
        groups = {group[0]["window"]: group, "other": [row(0, False, window="other")]}
        self.assertEqual(select_controls(group[2], groups),
                         {"before": "formal-1/A@01", "after": "formal-1/A@04", "fallback": None})
        self.assertIsNone(select_controls(group[0], groups)["before"])
        self.assertIsNone(select_controls(group[-1], groups)["after"])
        self.assertIsNone(select_controls(group[0], groups)["fallback"])

    def test_all_slow_fallback_nearest_compatible_window_then_nearest_position(self):
        trigger = row(3)
        groups = {"formal-1/A": [trigger],
                  "B": [row(3, False, "B", arm="B")],
                  "later-stage": [row(3, False, "later-stage", stage="formal-2")],
                  "far": [row(3, False, "far", epoch=1200)],
                  "near": [row(2, False, "near", epoch=1050), row(4, False, "near", epoch=1050)]}
        self.assertEqual(select_controls(trigger, groups), {"before": None, "after": None, "fallback": "near@02"})
        del groups["near"], groups["far"]
        self.assertIsNone(select_controls(trigger, groups)["fallback"])

    def test_recover_integer_sums_with_distinct_queue_and_latency_tails(self):
        def hist(sum_ns, p99):
            return {"count": 3, "mean_us": sum_ns / 3000, "max_us": p99,
                    "p50_us": 0, "p95_us": 0, "p99_us": p99, "p99_9_us": p99}
        success, queue = hist(100_000_003, THRESHOLD_US), hist(99_001_003, THRESHOLD_US - 1)
        values = {"offered": 3, "sent": 3, "succeeded": 3, "failures": 0, "success_latency": success,
                  "observed_latency": success, "client_queue_delay": queue}
        outcome = operation_values(values)
        self.assertEqual(outcome["sum_ns"]["success"] - outcome["sum_ns"]["client_queue"], 999_000)
        with self.assertRaises(ValueError):
            operation_values({**values, "failures": 1})


if __name__ == "__main__":
    unittest.main()
