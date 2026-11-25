"""Regression tests for the boundary and request identities used by the stall report."""
import unittest

import golog_stall_trace_report as report


class StallTraceTests(unittest.TestCase):
    def test_bounds_bracket_real_100ms(self):
        indices = [i for i in range(8000) if report.bounds(i)[0] <= report.SLOW <= report.bounds(i)[1]]
        self.assertEqual(len(indices), 1)
        low, high = report.bounds(indices[0])
        self.assertLess(low, report.SLOW)
        self.assertGreater(high, report.SLOW)
        self.assertEqual(report.bounds(indices[0] - 1)[1] + 1, low)

    def test_same_request_majority_and_tie(self):
        rows = [dict(index=i, planned_ns=0, dequeued_ns=q, send_start_ns=q+5, completed_ns=100)
                for i, q in enumerate([40, 50, 60, 70])]
        result = report.breakdown(rows, lambda r: None)
        self.assertEqual(result["queue_strict_majority"], 2)
        self.assertEqual(result["queue_equal_half"], 1)
        self.assertEqual(result["queue_below_half"], 1)
        self.assertEqual(result["traced_immediate_predecessors"], 0)
        self.assertEqual(result["sum_ns"]["total"], sum(result["sum_ns"][k]
                         for k in ("queue","preparation","exchange")))

    def test_reusable_independent_uniform_schedule(self):
        groups, selected = report.schedule(180000,64,6000,42,16,95)
        self.assertEqual(len(selected), 11228)
        self.assertEqual(len({i % 64 for i in selected}), 64)
        self.assertEqual(sum(v["offered"] for v in groups.values()), 180000)
        self.assertEqual(sum(v["uniform"] for v in groups.values()), len(selected))

    def test_clock_fraction_and_zone(self):
        self.assertEqual(report.wall_ns("2020-01-01T00:00:00.123456789Z"), 1577836800123456789)
        self.assertEqual(report.wall_ns("2020-01-01T08:00:00.123456+08:00"), 1577836800123456000)

    def test_external_controls_preserve_topology_and_occurrence(self):
        cases = {"03-A": ("ON",500,[]), "01-A": ("ON",300,[2,4]),
                 "07-A": ("ON",700,[3]), "02-A": ("OFF",499,[3]), "04-E": ("ON",501,[3])}
        external, needed = report.external_controls(cases)
        self.assertEqual(external["03-A"][3], {"case":"01-A","second":2})
        self.assertEqual(needed["01-A"], {2,4})
        missing, _ = report.external_controls({"03-A":cases["03-A"], "02-A":cases["02-A"]})
        self.assertIsNone(missing["03-A"][3])
