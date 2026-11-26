import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from golog_journal_stack_report import (
    COMMIT, classify_stack, journal_transactions, parse_enriched, selected_trace, stack_pairs, summarize_slow,
)


class JournalStackTests(unittest.TestCase):
    def test_stack_pairs_with_prior_switch_on_same_cpu(self):
        switch = {"ns": 1000, "tid": 5, "cpu": 2,
                  "payload": "sched_switch: prev_pid=5 prev_prio=120 prev_state=D ==> next_pid=0"}
        unrelated = {"ns": 1500, "tid": 0, "cpu": 3, "payload": "other"}
        frames = ["trace_event_raw_event_sched_switch", "jbd2_log_wait_commit", "ext4_sync_file"]
        stack = {"ns": 2000, "tid": 5, "cpu": 2, "payload": "<stack trace>", "stack": frames}
        paired, unmatched = stack_pairs([switch, unrelated, stack])
        self.assertEqual(paired[(5, 1000)]["frames"], frames)
        self.assertEqual(unmatched, [])
        self.assertEqual(stack_pairs([switch, {**unrelated, "cpu": 2}, stack]), ({}, [stack]))
        self.assertEqual(stack_pairs([switch, {**stack, "tid": 6}]), ({}, [{**stack, "tid": 6}]))
        separate = {**stack, "selected_interval": 1}
        self.assertEqual(stack_pairs([{**switch, "selected_interval": 0}, separate]), ({}, [separate]))

    def test_classification_requires_ext4_and_a_specific_wait_function(self):
        self.assertEqual(classify_stack(["jbd2_log_wait_commit"]), "ext4_sync_file_not_observed_in_stack")
        self.assertEqual(classify_stack(["ext4_sync_file", "jbd2_log_wait_commit"]), "journal_commit_wait")
        self.assertEqual(classify_stack(["ext4_sync_file", "folio_wait_writeback+0x14"]), "file_writeback_wait")
        self.assertEqual(classify_stack(["ext4_sync_file", "file_write_and_wait_range"]), "ext4_sleep_unclassified")
        self.assertEqual(classify_stack(["ext4_sync_file", "blk_mq_get_tag"]), "block_queue_tag_wait")

    def test_transaction_requires_id_and_all_ordered_stages(self):
        events = [{"device": "8,48", "transaction": 12, "tid": 8, "ns": index, "event": name}
                  for index, name in enumerate(COMMIT)]
        unknown = {**events[-1], "transaction": None}
        self.assertFalse(journal_transactions([*events[:-1], unknown])[0]["complete_ordered_single_tid"])
        self.assertTrue(journal_transactions([*events, unknown])[0]["complete_ordered_single_tid"])
        mixed = [*events[:-1], {**events[-1], "tid": 9}]
        self.assertFalse(journal_transactions(mixed)[0]["complete_ordered_single_tid"])

    def test_streamed_window_retains_target_thread_and_interleaved_cpu_order(self):
        records = [
            "writer-10 [000] ..... 1.000001: sched_switch: prev_pid=10 prev_prio=120 prev_state=D ==> next_pid=0",
            "other-20 [001] ..... 1.000002: sched_wakeup: pid=20",
            "writer-10 [000] ..... 1.000003: <stack trace>",
            " => __traceiter_sched_switch",
            " => ext4_sync_file",
            " => jbd2_log_wait_commit",
            "other-20 [001] ..... 1.000004: sched_wakeup: pid=10",
            "other-20 [001] ..... 1.000005: sched_switch: prev_pid=20 prev_prio=120 prev_state=R ==> next_pid=10",
        ]
        with TemporaryDirectory() as directory:
            path = Path(directory) / "trace.txt"
            path.write_text("# entries-in-buffer/entries-written: 5/8\n" + "\n".join(records) + "\n",
                            encoding="utf-8")
            call = {"tid": 10, "enter_ns": 1_000_000_000, "exit_ns": 1_000_006_000}
            timelines, pairs, unpaired, transactions, unknown, stats = selected_trace(path, [call], "8,48")
            self.assertEqual(list(timelines), [10])
            self.assertEqual([row[1] for row in timelines[10]], ["out", "wake", "in"])
            self.assertEqual(classify_stack(pairs[(10, 1_000_001_000)]["frames"]), "journal_commit_wait")
            self.assertEqual((unpaired, transactions, unknown), ([], [], 0))
            self.assertEqual(stats, {"retained": 5, "written": 8, "absent_from_ring": 3})
            with self.assertRaises(ValueError):
                list(parse_enriched(path))

    def test_aggregate_keeps_unclassified_d_time_distinct(self):
        row = {"scheduler_warnings": {}, "user_sync_ns": 120, "kernel_syscall_ns": 119, "d_state_ns": 95,
               "classified_d_state_ns": {"file_writeback_wait": 70}, "unclassified_d_state_ns": 25,
               "on_cpu_ns": 15, "other_sleep_ns": 0, "wakeup_to_switch_in_ns": 3,
               "runnable_preempted_ns": 4, "unresolved_off_cpu_ns": 2}
        aggregate = summarize_slow([row, {**row, "user_sync_ns": 30}], 100)
        self.assertEqual(aggregate["batches"], 1)
        self.assertEqual(aggregate["classified_d_state_ns"], {"file_writeback_wait": 70})
        self.assertEqual(aggregate["unclassified_d_state_ns"], 25)


if __name__ == "__main__":
    unittest.main()
