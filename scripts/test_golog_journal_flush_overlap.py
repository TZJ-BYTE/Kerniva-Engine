import unittest

from golog_journal_flush_overlap import flush_observations, relate


def event(us, kind, flags="FF", status=0):
    if kind == "issue":
        payload = "0 () 0 + 0 none,0,0 [jbd2/sdd-8]"
    else:
        payload = f"() 18446744073709551615 + 0 none,0,0 [{status}]"
    return f" worker [001] 0.{us:06}: block_rq_{kind}: 8,48 {flags} {payload}\n".encode()


class JournalFlushOverlapTest(unittest.TestCase):
    def test_sentinel_pair_and_nonflush_interleaving(self):
        pairs, uncertain, _ = flush_observations([
            event(6, "issue"), event(7, "issue", "W"), event(8, "complete"),
        ], {"8,48"})
        self.assertFalse(uncertain)
        self.assertEqual(len(pairs), 1)
        self.assertEqual(pairs[0]["interval_ns"], 2000)
        self.assertEqual(pairs[0]["issue_line"], 1)
        self.assertEqual(pairs[0]["complete_line"], 3)
        self.assertEqual(pairs[0]["interleaved_other_issues"], 1)
        episode = dict(out_ns=6000, wakeup_ns=9000, device="8,48", report="test",
                       window="test", batch_id=1, user_sync_ns=9000)
        boundary = relate(episode, pairs, uncertain)
        self.assertFalse(boundary["fully_contained_ff_intervals"])
        self.assertEqual(boundary["all_overlap_coverage_ns"], 2000)
        strict = relate(dict(episode, out_ns=5000), pairs, uncertain)
        self.assertEqual(strict["contained_issue_to_complete_coverage_ns"], 2000)

    def test_concurrency_unmatched_failed_and_tied_events(self):
        pairs, uncertain, counts = flush_observations([
            event(1, "complete"), event(2, "issue"), event(3, "issue"),
            event(4, "complete"), event(5, "complete"), event(6, "issue"),
            event(7, "complete", status=-5), event(8, "issue"), event(8, "complete"),
            event(9, "issue"),
        ], {"8,48"})
        self.assertFalse(pairs)
        self.assertEqual(counts, {"ff_completions": 5, "ff_issues": 5})
        self.assertEqual([row["reason"] for row in uncertain], [
            "completion_without_observed_issue", "overlapping_identical_descriptions",
            "missing_or_failed_completion_status", "unresolved_clock_order",
            "incomplete_at_trace_end",
        ])

    def test_unidentifiable_requeue_on_same_device(self):
        pairs, uncertain, counts = flush_observations([
            event(1, "issue"), b" worker [001] 0.000002: block_rq_requeue: 8,48 FF () 0 + 0\n",
            event(3, "complete"),
        ], {"8,48"})
        self.assertFalse(pairs)
        self.assertEqual(counts["observed_device_requeues"], 1)
        self.assertEqual(uncertain[0]["reason"], "device_requeue_cannot_be_excluded")


if __name__ == "__main__":
    unittest.main()
