import unittest

from golog_kernel_writer_report import candidate_ranges, peak_support, residuals, reverse_batch_candidates, syscall_stamp


class FtraceTimestampTests(unittest.TestCase):
    def setUp(self):
        self.call = {"kind": "fsync", "enter_ns": 5000, "exit_ns": 8000, "enter_resolution_ns": 1000,
                     "exit_resolution_ns": 1000, "result": 0}
        self.batch = {"sync_start_ns": 5499, "sync_end_ns": 7500}

    def test_mixed_boundary_rounding(self):
        self.assertEqual(syscall_stamp(self.call, "enter"), (4500, 5499))
        self.assertEqual(syscall_stamp(self.call, "exit"), (7500, 8499))
        self.assertEqual(candidate_ranges([self.batch], [self.call], (-500, 500)), [{0: (0, 0)}])
        self.assertEqual(candidate_ranges([self.batch], [self.call], (1, 1)), [{}])
        self.assertEqual(candidate_ranges([self.batch], [self.call], (-1, -1)), [{}])

    def test_residuals_include_quantization_without_inventing_elapsed_time(self):
        self.assertEqual(
            residuals(self.batch, self.call, (0, 0)),
            {"user_before_syscall_ns": [0, 0], "user_after_syscall_ns": [0, 0],
             "user_outside_syscall_ns": [0, 0]},
        )

    def test_offset_support_counts_distinct_batches(self):
        rows = [{0: (1, 3), 1: (2, 5)}, {2: (3, 4)}, {3: (1, 1), 4: (8, 9)}]
        self.assertEqual(peak_support(rows), {"support": 2, "batches": 3, "peak_ranges_ns": [(1, 1), (3, 4)]})

    def test_reverse_keeps_other_batches_and_exact_clock_boundaries(self):
        selected = {**self.batch, "id": 9}
        wider = {"id": 4, "sync_start_ns": 4500, "sync_end_ns": 8000}
        outside = {"id": 1, "sync_start_ns": 5500, "sync_end_ns": 7499}
        self.assertEqual(reverse_batch_candidates(self.call, [selected, wider, outside], (-500, 500)), [
            {"batch_id": 4, "feasible_offset_ns": [-500, 500]}, {"batch_id": 9, "feasible_offset_ns": [0, 0]},
        ])
        self.assertEqual(
            residuals({"sync_start_ns": 4000, "sync_end_ns": 9000}, self.call, (0, 0)),
            {"user_before_syscall_ns": [500, 1499], "user_after_syscall_ns": [501, 1500],
             "user_outside_syscall_ns": [1001, 2999]},
        )


if __name__ == "__main__":
    unittest.main()
