import unittest

from golog_journal_stack_report import COMMIT, classify_stack, journal_transactions, stack_pairs


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

    def test_classification_requires_ext4_and_a_specific_wait_function(self):
        self.assertEqual(classify_stack(["jbd2_log_wait_commit"]), "ext4_sync_file_not_observed_in_stack")
        self.assertEqual(classify_stack(["ext4_sync_file", "jbd2_log_wait_commit"]), "journal_commit_wait")
        self.assertEqual(classify_stack(["ext4_sync_file", "folio_wait_writeback+0x14"]), "file_writeback_wait")
        self.assertEqual(classify_stack(["ext4_sync_file", "file_write_and_wait_range"]), "ext4_sleep_unclassified")

    def test_transaction_requires_id_and_all_ordered_stages(self):
        events = [{"device": "8,48", "transaction": 12, "tid": 8, "ns": index, "event": name}
                  for index, name in enumerate(COMMIT)]
        unknown = {**events[-1], "transaction": None}
        self.assertFalse(journal_transactions([*events[:-1], unknown])[0]["complete_ordered_single_tid"])
        self.assertTrue(journal_transactions([*events, unknown])[0]["complete_ordered_single_tid"])
        mixed = [*events[:-1], {**events[-1], "tid": 9}]
        self.assertFalse(journal_transactions(mixed)[0]["complete_ordered_single_tid"])


if __name__ == "__main__":
    unittest.main()
