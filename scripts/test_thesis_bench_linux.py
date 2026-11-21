"""Regression checks for Linux experiment scheduling and optional metrics."""

import copy
import importlib.util
import io
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock


def load_module(name, filename):
    spec = importlib.util.spec_from_file_location(name, Path(__file__).parent / filename)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


runner = load_module("thesis_linux", "thesis-bench-linux.py")
analysis = load_module("thesis_analysis", "thesis-analyze.py")


class LinuxRunnerTests(unittest.TestCase):
    def test_formal_schedule_has_108_balanced_seeded_windows(self):
        options = {"durability": "sync", "keys": 100000, "clients": 64,
                   "hot_percent": 0, "hot_keys": 1000}
        groups = [runner.parse_group("std:95:5000,10000,15000"),
                  runner.parse_group("gnet:80:1250,2500,3750")]
        cases = runner.build_cases(groups, ["start", "candidate", "redis"], 6, 42, options)
        self.assertEqual(len(cases), 108)
        for variant in ("start", "candidate", "redis"):
            positions = [case["position"] for case in cases if case["variant_name"] == variant]
            self.assertEqual([positions.count(position) for position in (1, 2, 3)], [12, 12, 12])
        for case in cases:
            self.assertEqual(case["seed"], 41 + case["round"])

    def test_denominator_uses_client_integer_arrival_count(self):
        case = {"seed": 42, "rate": 10, "keys": 1, "clients": 1, "read_percent": 95}
        measurement = {"offered": 2, "succeeded": 1, "failures": 1,
                       "get": {"offered": 1, "succeeded": 1, "failures": 0},
                       "set": {"offered": 1, "succeeded": 0, "failures": 1},
                       "seed": 42, "target_rate": 10, "keys": 1, "clients": 1,
                       "read_percent": 95, "value_bytes": 32}
        runner.validate_measurement(measurement, case, {"duration_ns": 250_000_000})
        measurement["offered"] = 3
        with self.assertRaises(ValueError):
            runner.validate_measurement(measurement, case, {"duration_ns": 250_000_000})

    def test_resp_bulk_and_nested_arrays_are_complete(self):
        reply = b"*2\r\n$3\r\na\nb\r\n*2\r\n:2\r\n$0\r\n\r\n"
        self.assertEqual(runner.read_resp(io.BytesIO(reply)), ["a\nb", [2, ""]])
        with self.assertRaises(RuntimeError):
            runner.read_resp(io.BytesIO(b"$4\r\nab\r\n"))

    @unittest.skipUnless(sys.platform == "linux", "Linux executable symlinks")
    def test_redis_launch_name_is_preserved(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            target = root / "redis-check-rdb"
            target.write_bytes(b"redis binary")
            server = root / "redis-server"
            server.symlink_to(target)
            benchmark = root / "bench"
            benchmark.write_bytes(b"client binary")
            manifest = root / "manifest.json"
            manifest.write_text(json.dumps({"benchmark": str(benchmark), "variants": [
                {"name": "redis", "engine": "redis", "server": str(server)}]}))
            with mock.patch.object(runner.subprocess, "run", return_value=mock.Mock(stdout="Redis 8.0.5")) as launch:
                _, variants = runner.load_manifest(manifest, None)
            self.assertEqual(launch.call_args.args[0][0], str(server))
            self.assertEqual(variants[0]["server"], str(server))
            self.assertEqual(variants[0]["server_realpath"], str(target))
            self.assertIsNone(variants[0]["library_sha256"])

    def test_summary_and_pairs_preserve_missing_runtime_metrics(self):
        row = {field: "group" for field in analysis.GROUP_FIELDS}
        row.update({metric: 1 for metric in analysis.SUMMARY_METRICS})
        row.update(variant="redis", round=1, seed=42, stable=True, superseded=False, failures=0)
        row["server_go_num_gc_delta"] = None
        row["server_go_pause_ns_delta"] = None
        summary = analysis.summarize([row])
        self.assertIsNone(summary[0]["server_go_num_gc_delta_median"])
        candidate = copy.deepcopy(row)
        candidate.update(variant="candidate", server_go_num_gc_delta=3, server_go_pause_ns_delta=4)
        pairs = analysis.paired_rows([row, candidate])
        self.assertEqual(len(pairs), 1)
        self.assertIsNone(pairs[0]["candidate_over_reference_server_go_num_gc_delta"])

    def test_empty_latency_histograms_export_null_and_sample_counts(self):
        detail = {"offered": 10, "sent": 10, "succeeded": 0, "failures": 10,
                  "success_latency": {"count": 0, "mean_us": 0, "p50_us": 0, "p95_us": 0, "p99_us": 0, "p99_9_us": 0},
                  "client_queue_delay": {"count": 0, "mean_us": 0, "p50_us": 0, "p95_us": 0, "p99_us": 0, "p99_9_us": 0}}
        row = {field: "group" for field in analysis.GROUP_FIELDS}
        row.update(variant="redis", round=1, superseded=False, superseded_reason="", client_sha256="client")
        item = analysis.flatten_window(row, {"planned_second": 0, "get": detail}, "get")
        self.assertIsNone(item["success_latency_p99_us"])
        self.assertIsNone(item["success_latency_p99_9_us"])
        self.assertIsNone(item["client_queue_delay_p99_us"])
        self.assertEqual(item["success_samples"], 0)
        self.assertEqual(item["p99_9_expected_tail_observations"], 0)
        detail["success_latency"].update(count=15000, p99_9_us=12)
        self.assertEqual(analysis.sample_counts(detail)["p99_9_expected_tail_observations"], 15)
        self.assertEqual(analysis.latency_value(detail, "success_latency", "p99_9_us"), 12)

    def test_zero_sample_percentiles_stay_null_in_summary_and_pairs(self):
        row = {field: "group" for field in analysis.GROUP_FIELDS}
        row.update({metric: 1 for metric in analysis.SUMMARY_METRICS})
        row.update(variant="start", round=1, seed=42, stable=False, superseded=False, failures=10,
                   get_success_latency_p99_us=None, get_success_latency_p99_9_us=None)
        candidate = dict(row, variant="candidate")
        summary = analysis.summarize([row, candidate])
        self.assertTrue(all(item["get_success_latency_p99_9_us_median"] is None for item in summary))
        pair = analysis.paired_rows([row, candidate])[0]
        self.assertIsNone(pair["candidate_over_reference_get_success_latency_p99_9_us"])

    def test_changed_process_identity_is_never_signaled(self):
        expected = {"pid": 1234, "start_ticks": 10, "session": 1234, "executable": "/engine"}
        actual = dict(expected, start_ticks=11)
        with mock.patch.object(runner, "process_identity", return_value=actual), mock.patch.object(runner.os, "killpg", create=True) as kill:
            with self.assertRaises(RuntimeError):
                runner.signal_owned(expected, 15)
            kill.assert_not_called()


if __name__ == "__main__":
    unittest.main()
