"""Flatten fixed-rate benchmark evidence and render reproducible figures."""

import argparse
import csv
import hashlib
import json
import statistics
import sys
from collections import defaultdict
from importlib import metadata as package_metadata
from pathlib import Path


def read_json(path):
    return json.loads(path.read_text(encoding="utf-8-sig"))


def write_csv(path, rows):
    if not rows:
        path.write_text("", encoding="utf-8-sig")
        return
    fields = list(dict.fromkeys(key for row in rows for key in row))
    with path.open("w", encoding="utf-8-sig", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def weighted_queue(window):
    operations = [window[operation] for operation in ("get", "set")]
    count = sum(operation["offered"] for operation in operations)
    if not count:
        return 0.0
    return sum(operation["client_queue_delay"]["mean_us"] * operation["offered"] for operation in operations) / count


def queue_trend(windows):
    values = [weighted_queue(window) for window in windows]
    if len(values) < 2:
        return 0.0, 0.0, 0.0, False
    width = max(1, len(values) // 3)
    first = statistics.mean(values[:width])
    last = statistics.mean(values[-width:])
    middle = (len(values) - 1) / 2
    slope = sum((index - middle) * value for index, value in enumerate(values)) / sum((index - middle) ** 2 for index in range(len(values)))
    increases = sum(right > left for left, right in zip(values, values[1:])) / (len(values) - 1)
    rising = last - first > max(1000.0, first * 0.5) and slope > 100 and increases >= 0.65
    return first, last, slope, rising


def latency_value(detail, kind, quantile, scale=1):
    summary = detail[kind]
    fallback_count = detail["succeeded"] if kind == "success_latency" else detail["offered"]
    if summary.get("count", fallback_count) == 0:
        return None
    return summary[quantile] / scale


def sample_counts(detail):
    count = detail["success_latency"].get("count", detail["succeeded"])
    return {"success_samples": count, "p99_9_expected_tail_observations": count * 0.001}


def flatten_window(row, window, operation):
    item = {key: row[key] for key in GROUP_FIELDS + ("variant", "round", "superseded", "superseded_reason", "client_sha256")}
    item.update(planned_second=window["planned_second"], operation=operation)
    detail = window[operation]
    item.update({key: detail[key] for key in ("offered", "sent", "succeeded", "failures")})
    item.update(sample_counts(detail))
    for kind in ("success_latency", "client_queue_delay"):
        for quantile in ("mean_us", "p50_us", "p95_us", "p99_us", "p99_9_us"):
            item[f"{kind}_{quantile}"] = latency_value(detail, kind, quantile)
    return item


def flatten(path):
    result = read_json(path)
    measurement = result["measurement"]
    case = result["case"]
    plan_path = path.parents[3] / "plan.json"
    plan = read_json(plan_path)
    superseded_path = plan_path.parent / "superseded.json"
    first, last, slope, rising = queue_trend(measurement["windows"])
    target = measurement["target_rate"]
    attainment = min(measurement["actual_send_rate"], measurement["actual_success_rate"], measurement["window_send_rate"], measurement["window_success_rate"]) / target
    stable = measurement["failures"] == 0 and attainment >= 0.98 and not rising
    reasons = []
    if measurement["failures"]:
        reasons.append("failures")
    if attainment < 0.98:
        reasons.append("rate_below_98pct")
    if rising:
        reasons.append("sustained_queue_growth")
    samples = [json.loads(line) for line in (path.parent / "process.jsonl").read_text(encoding="utf-8-sig").splitlines() if line]
    resources = result["resources"]
    row = {
        "suite": plan["suite"], "plan_id": result["plan_id"], "runner_sha256": plan.get("runner_sha256", ""),
        "superseded": superseded_path.exists(),
        "superseded_reason": read_json(superseded_path)["reason"] if superseded_path.exists() else "",
        "started_utc": measurement["started_utc"], "completed_utc": result["completed_utc"],
        "variant": result["variant"]["name"], "round": case["round"], "case_id": case["id"],
        "network": case["network"], "durability": case["durability"], "read_percent": case["read_percent"],
        "target_rate": target, "keys": case["keys"], "hot_percent": case["hot_percent"], "hot_keys": case["hot_keys"],
        "clients": case["clients"], "seed": case["seed"], "max_memory_bytes": plan["max_memory_bytes"],
        "duration_seconds": measurement["duration_seconds"], "elapsed_seconds": measurement["elapsed_seconds"],
        "reconnect_backoff_seconds": measurement.get("reconnect_backoff_seconds", 0),
        "drain_seconds": measurement["drain_seconds"], "actual_success_rate": measurement["actual_success_rate"],
        "actual_send_rate": measurement["actual_send_rate"], "window_success_rate": measurement["window_success_rate"],
        "window_send_rate": measurement["window_send_rate"], "attainment_min": attainment,
        "offered": measurement["offered"], "sent": measurement["sent"], "succeeded": measurement["succeeded"],
        "failures": measurement["failures"], "failure_percent": 100 * measurement["failures"] / measurement["offered"],
        "stable": stable, "stability_reason": ";".join(reasons),
        "queue_first_third_mean_us": first, "queue_last_third_mean_us": last,
        "queue_slope_us_per_second": slope, "queue_sustained_growth": rising,
        "cpu_seconds": resources["cpu_seconds"], "cpu_percent_one_core": resources["cpu_percent_one_core"],
        "resource_window_seconds": samples[-1]["seconds"] - samples[0]["seconds"],
        "rss_sampled_peak_bytes": max(sample["working_set_bytes"] for sample in samples),
        "private_sampled_peak_bytes": max((sample["private_bytes"] for sample in samples if sample.get("private_bytes") is not None), default=None),
        "server_go_num_gc_delta": resources["go_num_gc_delta"], "server_go_pause_ns_delta": resources["go_pause_ns_delta"],
        "server_heap_before_bytes": resources["heap_alloc_before_bytes"], "server_heap_after_bytes": resources["heap_alloc_after_bytes"],
        "client_num_gc_delta": measurement["client_num_gc_delta"],
        "client_heap_alloc_bytes": measurement["client_heap_alloc_bytes"],
        "server_sha256": result["variant"]["server_sha256"], "library_sha256": result["variant"].get("library_sha256"),
        "engine": result["variant"].get("engine", "kerniva"),
        "engine_network": result.get("engine_network", case["network"]),
        "method_version": result.get("method_version", "windows-frozen-client-v4"),
        "private_memory_definition": resources.get("private_memory_definition", "Windows PrivateMemorySize64"),
        "client_sha256": result["client_sha256"], "result_path": str(path),
    }
    for operation in ("get", "set"):
        detail = measurement[operation]
        if detail["offered"] != detail["succeeded"] + detail["failures"]:
            raise ValueError(f"{operation.upper()} failure denominator mismatch in {path}")
        row.update({f"{operation}_{key}": value for key, value in sample_counts(detail).items()})
        for field in ("offered", "sent", "succeeded", "failures", "queue_drops", "queue_timeouts", "request_timeouts", "network_errors", "reply_errors", "reconnect_attempts"):
            row[f"{operation}_{field}"] = detail.get(field, 0)
        row[f"{operation}_actual_success_rate"] = detail["succeeded"] / measurement["elapsed_seconds"]
        row[f"{operation}_window_success_rate"] = detail["succeeded_in_window"] / measurement["duration_seconds"]
        for kind in ("success_latency", "observed_latency", "client_queue_delay"):
            for quantile in ("mean_us", "p50_us", "p95_us", "p99_us", "p99_9_us", "max_us"):
                row[f"{operation}_{kind}_{quantile}"] = latency_value(detail, kind, quantile)
    if measurement["offered"] != measurement["succeeded"] + measurement["failures"]:
        raise ValueError(f"Failure denominator mismatch in {path}")
    return row


GROUP_FIELDS = (
    "suite", "plan_id", "network", "durability", "read_percent", "target_rate",
    "keys", "hot_percent", "hot_keys", "clients", "max_memory_bytes", "duration_seconds",
)
SUMMARY_METRICS = (
    "actual_success_rate", "actual_send_rate", "window_success_rate", "window_send_rate",
    "failure_percent", "cpu_percent_one_core", "rss_sampled_peak_bytes", "resource_window_seconds",
    "server_go_num_gc_delta", "server_go_pause_ns_delta",
)
SUMMARY_METRICS += tuple(
    f"{operation}_{rate_kind}"
    for operation in ("get", "set")
    for rate_kind in ("actual_success_rate", "window_success_rate")
)
SUMMARY_METRICS += tuple(
    f"{operation}_success_latency_{quantile}"
    for operation in ("get", "set")
    for quantile in ("p50_us", "p95_us", "p99_us", "p99_9_us")
)
SUMMARY_METRICS += tuple(
    f"{operation}_{metric}"
    for operation in ("get", "set")
    for metric in ("success_samples", "p99_9_expected_tail_observations")
)


def summarize(rows):
    grouped = defaultdict(list)
    for row in rows:
        grouped[tuple(row[field] for field in GROUP_FIELDS) + (row["variant"],)].append(row)
    summary = []
    for key, samples in sorted(grouped.items()):
        item = dict(zip(GROUP_FIELDS + ("variant",), key))
        item["superseded"] = samples[0]["superseded"]
        item.update(rounds=len(samples), stable_rounds=sum(sample["stable"] for sample in samples), all_rounds_stable=all(sample["stable"] for sample in samples))
        for metric in SUMMARY_METRICS:
            values = [sample[metric] for sample in samples if sample[metric] is not None]
            item[f"{metric}_median"] = statistics.median(values) if values else None
            item[f"{metric}_min"] = min(values) if values else None
            item[f"{metric}_max"] = max(values) if values else None
        summary.append(item)
    return summary


def paired_rows(rows):
    grouped = defaultdict(dict)
    for row in rows:
        grouped[tuple(row[field] for field in GROUP_FIELDS) + (row["round"],)][row["variant"]] = row
    pairs = []
    for key, variants in sorted(grouped.items()):
        for reference, name in (("baseline", "final"), ("baseline", "go-only"), ("go-only", "final"),
                                ("start", "candidate"), ("redis", "start"), ("redis", "candidate")):
            if reference not in variants or name not in variants:
                continue
            reference_row, candidate = variants[reference], variants[name]
            if reference_row["seed"] != candidate["seed"]:
                raise ValueError("Paired windows must use the same request seed")
            item = dict(zip(GROUP_FIELDS + ("round",), key))
            item["superseded"] = reference_row["superseded"]
            item.update(reference=reference, candidate=name, both_stable=reference_row["stable"] and candidate["stable"], reference_failures=reference_row["failures"], candidate_failures=candidate["failures"])
            for metric in SUMMARY_METRICS:
                item[f"reference_{metric}"] = reference_row[metric]
                item[f"candidate_{metric}"] = candidate[metric]
                reference_value, candidate_value = reference_row[metric], candidate[metric]
                item[f"candidate_over_reference_{metric}"] = candidate_value / reference_value if reference_value and candidate_value is not None else None
            pairs.append(item)
    return pairs


def histogram_upper(index):
    if index < 512:
        return index
    shift = (index - 512) // 256 + 1
    mantissa = (index - 512) % 256 + 256
    return min(((mantissa + 1) << shift) - 1, (1 << 63) - 1)


def render_figures(rows, output):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    colors = {"baseline": "#b14a3c", "go-only": "#a87a15", "final": "#176a96",
              "start": "#b14a3c", "candidate": "#176a96", "redis": "#a87a15"}
    figures = output / "figures"
    figures.mkdir(exist_ok=True)
    groups = defaultdict(list)
    for row in rows:
        if row["duration_seconds"] >= 30 and not row["superseded"]:
            groups[tuple(row[field] for field in GROUP_FIELDS if field != "target_rate")].append(row)
    for group_key, samples in sorted(groups.items()):
        representative = samples[0]
        suite = representative["suite"]
        network, read, durability = representative["network"], representative["read_percent"], representative["durability"]
        figure_id = hashlib.sha256(json.dumps(group_key).encode()).hexdigest()[:12]
        file_label = f"{suite}-{network}-r{read}-{figure_id}"
        network_label = f"Kerniva {network} comparison group; Redis native TCP" if any(row["engine"] == "redis" for row in samples) else network
        title = f"{suite} / {network_label} / GET {read}% SET {100-read}% / {durability} / 32 B"
        distribution = f"hot {representative['hot_percent']}% of requests to {representative['hot_keys']:,} keys" if representative["hot_percent"] else "uniform key selection"
        configuration = f"keys {representative['keys']:,}, {distribution}, configured budget {representative['max_memory_bytes']/1024**2:g} MiB, clients {representative['clients']}, duration {representative['duration_seconds']:g} s"
        memory_note = "Budget accounting follows each engine: Kerniva payload/cache, Redis allocator. Process RSS is reported separately."
        round_counts = sorted({len({row['round'] for row in samples if row['variant'] == variant and row['target_rate'] == target}) for variant in {row['variant'] for row in samples} for target in {row['target_rate'] for row in samples}})
        if not round_counts or min(round_counts) < 3:
            continue
        figure, axes = plt.subplots(2, 3, figsize=(14, 8.5), constrained_layout=True)
        round_midpoint = (min(row["round"] for row in samples) + max(row["round"] for row in samples)) / 2
        metrics = [("actual_success_rate", "Successful replies/s"), ("get_success_latency_p99_us", "GET success p99 (ms)"), ("set_success_latency_p99_us", "SET success p99 (ms)"), ("failure_percent", "All offered requests failed (%)"), ("get_success_latency_p99_9_us", "GET success p99.9 (ms)"), ("set_success_latency_p99_9_us", "SET success p99.9 (ms)")]
        for axis, (metric, label) in zip(axes.flat, metrics):
            for variant in sorted({row["variant"] for row in samples}):
                variant_rows = [row for row in samples if row["variant"] == variant]
                selected = [row for row in variant_rows if row[metric] is not None]
                variant_label = variant
                if metric.endswith("_p99_9_us"):
                    operation = metric.split("_", 1)[0]
                    counts = [row[f"{operation}_success_samples"] for row in variant_rows]
                    variant_label = (f"{variant}: N {min(counts):,}-{max(counts):,}, "
                                     f"0.1% tail ~{min(counts) * .001:.1f}-{max(counts) * .001:.1f}")
                if not selected:
                    axis.plot([], [], label=variant_label, color=colors.get(variant))
                    continue
                rates = sorted({row["target_rate"] for row in selected})
                scale = 1000 if "latency" in metric else 1
                medians = [statistics.median(row[metric] / scale for row in selected if row["target_rate"] == rate) for rate in rates]
                axis.plot(rates, medians, label=variant_label, color=colors.get(variant), linewidth=1.5, marker="_", markersize=14, markeredgewidth=2)
                for row in selected:
                    jitter = (row["round"] - round_midpoint) * row["target_rate"] * .008
                    axis.scatter(row["target_rate"] + jitter, row[metric] / scale, marker="o" if row["stable"] else "x", s=30, color=colors.get(variant), alpha=.8)
            axis.set_xlabel("Target offered requests/s")
            axis.set_ylabel(label)
            axis.grid(alpha=.2)
            if "latency" in metric:
                axis.set_yscale("log")
            else:
                axis.ticklabel_format(axis="y", useOffset=False, style="plain")
            if metric == "failure_percent":
                axis.set_ylim(bottom=0)
            if metric.endswith("_p99_9_us"):
                axis.legend(fontsize=6.5)
        axes[0, 0].legend()
        figure.suptitle(f"{title}\n{configuration}\n{memory_note}\n"
                       f"Round points ({round_counts}) show tail spread. Lines mark medians. x = load criterion unmet. Planned-send latency includes queue.\n"
                       "p99.9 legends show per-round successful N ranges across displayed rates; N x 0.001 describes tail sampling limits.", fontsize=9)
        figure.savefig(figures / f"{file_label}.png", dpi=160)
        plt.close(figure)

        figure, axes = plt.subplots(2, 2, figsize=(11, 7), constrained_layout=True)
        resources = (("cpu_percent_one_core", "Server CPU (% of one logical CPU)", 1), ("rss_sampled_peak_bytes", "Server RSS peak, 1 s samples (MiB)", 1024**2), ("server_go_num_gc_delta", "Server Go GC cycles in window", 1), ("server_go_pause_ns_delta", "Server Go total GC pause delta (ms)", 1e6))
        for axis, (metric, label, scale) in zip(axes.flat, resources):
            for variant in sorted({row["variant"] for row in samples}):
                selected = [row for row in samples if row["variant"] == variant and row[metric] is not None]
                if not selected:
                    continue
                rates = sorted({row["target_rate"] for row in selected})
                axis.plot(rates, [statistics.median(row[metric] / scale for row in selected if row["target_rate"] == rate) for rate in rates], label=variant, color=colors.get(variant), marker="_", markersize=14, markeredgewidth=2)
                for row in selected:
                    axis.scatter(row["target_rate"], row[metric] / scale, color=colors.get(variant), s=22, alpha=.7)
            axis.set_xlabel("Target offered requests/s")
            axis.set_ylabel(label)
            axis.grid(alpha=.2)
        axes[0, 0].legend()
        figure.suptitle(f"{title}\n{configuration}\n{memory_note}\nResource interval spans start-marker to client exit, including drain and reporting. RSS is a 1 s sampled peak.", fontsize=9)
        figure.savefig(figures / f"resources-{file_label}.png", dpi=160)
        plt.close(figure)

        for target in sorted({row["target_rate"] for row in samples}):
            selected = [row for row in samples if row["target_rate"] == target]
            figure, axes = plt.subplots(1, 2, figsize=(11, 4.5), constrained_layout=True)
            for operation, axis in zip(("get", "set"), axes):
                for variant in sorted({row["variant"] for row in selected}):
                    bins = defaultdict(int)
                    count = 0
                    failures = 0
                    for row in selected:
                        if row["variant"] != variant:
                            continue
                        raw = read_json(Path(row["result_path"]).parent / "histogram.json")
                        snapshot = raw[operation]["success"]
                        count += snapshot["count"]
                        failures += row[f"{operation}_failures"]
                        for entry in snapshot["bins"] or []:
                            bins[entry["index"]] += entry["count"]
                    accumulated = 0
                    x_values, y_values = [], []
                    for index in sorted(bins):
                        accumulated += bins[index]
                        x_values.append(max(histogram_upper(index) / 1e6, 1e-6))
                        y_values.append(accumulated / count if count else 0)
                    label = f"{variant}, N={count:,}, 0.1% tail ~{count * .001:.1f}, failed={failures}"
                    axis.step(x_values, y_values, where="post", label=label, color=colors.get(variant))
                axis.set_xscale("log")
                axis.set_ylim(.9, 1.0005)
                axis.set_xlabel("Planned-send to complete reply (ms)")
                axis.set_ylabel(f"{operation.upper()} success-only tail CDF")
                axis.grid(alpha=.2)
                axis.legend(fontsize=8)
            figure.suptitle(f"{title}, target {target}/s\n{configuration}\n{memory_note}\n"
                           "CDF and N pool successful histograms across rounds. Failed outcomes retain the offered denominator.", fontsize=8)
            figure.savefig(figures / f"cdf-{file_label}-q{target}.png", dpi=160)
            plt.close(figure)

            figure, axes = plt.subplots(2, 3, figsize=(14, 7), constrained_layout=True)
            for row in selected:
                measurement = read_json(Path(row["result_path"]))["measurement"]
                seconds = [window["planned_second"] for window in measurement["windows"]]
                style = ("-", "--", ":")[(row["round"] - 1) % 3]
                label = f"{row['variant']} round {row['round']}"
                for operation, row_axes in zip(("get", "set"), axes):
                    details = [window[operation] for window in measurement["windows"]]
                    values = (
                        [latency_value(detail, "client_queue_delay", "mean_us", 1000) for detail in details],
                        [latency_value(detail, "client_queue_delay", "p99_us", 1000) for detail in details],
                        [100 * detail["succeeded"] / detail["offered"] if detail["offered"] else None for detail in details],
                    )
                    labels = (f"{operation.upper()} queue mean (ms)", f"{operation.upper()} queue p99 (ms)", f"{operation.upper()} successful outcomes (%)")
                    for column, (axis, series, axis_label) in enumerate(zip(row_axes, values, labels)):
                        axis.plot(seconds, series, label=label, color=colors.get(row["variant"]), linestyle=style, alpha=.8, linewidth=1)
                        axis.set_xlabel("Planned-send second")
                        axis.set_ylabel(axis_label)
                        axis.grid(alpha=.2)
                        if column < 2:
                            axis.set_yscale("log")
                        else:
                            axis.set_ylim(-2, 102)
            axes[0, 0].legend(fontsize=7)
            figure.suptitle(f"{title}, target {target}/s\n{configuration}\n{memory_note}\nCohorts are grouped by planned-send second. Replies may complete later. Queue includes reconnect backoff.", fontsize=9)
            figure.savefig(figures / f"timeline-{file_label}-q{target}.png", dpi=160)
            plt.close(figure)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("run_root", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--no-figures", action="store_true")
    arguments = parser.parse_args()
    output = arguments.output or arguments.run_root / "analysis"
    output.mkdir(parents=True, exist_ok=True)
    paths = sorted((arguments.run_root / "measurements").glob("*/cases/*/*/result.json"))
    rows = [flatten(path) for path in paths]
    write_csv(output / "runs.csv", rows)
    execution = []
    for index, row in enumerate(sorted(rows, key=lambda item: item["started_utc"]), start=1):
        item = {key: row[key] for key in ("suite", "plan_id", "case_id", "variant", "round", "started_utc", "completed_utc", "duration_seconds", "superseded", "server_sha256", "library_sha256", "client_sha256", "result_path")}
        item["execution_order"] = index
        execution.append(item)
    write_csv(output / "execution-index.csv", execution)
    summary = summarize(rows)
    write_csv(output / "summary.csv", summary)
    write_csv(output / "formal-summary.csv", [row for row in summary if row["duration_seconds"] >= 30 and row["rounds"] >= 3 and not row["superseded"]])
    write_csv(output / "paired.csv", paired_rows(rows))
    windows = []
    for row in rows:
        measurement = read_json(Path(row["result_path"]))["measurement"]
        for window in measurement["windows"]:
            for operation in ("get", "set"):
                windows.append(flatten_window(row, window, operation))
    write_csv(output / "windows.csv", windows)
    metadata = {
        "analysis_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "run_count": len(rows), "formal_run_count": sum(row["rounds"] for row in summary if row["duration_seconds"] >= 30 and row["rounds"] >= 3 and not row["superseded"]),
        "superseded_run_count": sum(row["superseded"] for row in rows),
        "summary_statistics": "median, minimum and maximum of per-round metrics. Each p99 summary uses per-round p99 values",
        "stable_rule": "zero failures, min(actual send/success and window send/success) >= 98% target, and no sustained queue-growth heuristic",
        "stable_scope": "Load criterion only. Tail spread is reported by per-round minima/maxima, scatter and time series",
        "empty_latency_samples": "Zero-count latency histograms export null quantiles; no successful samples means success latency is undefined",
        "p99_9_sampling": "Successful N and N x 0.001 are reported per round and planned-second cohort. N x 0.001 describes expected observations in the 0.1% tail, without an experiment acceptance threshold",
        "queue_growth_rule": "last-third mean queue exceeds first third by max(1ms,50%), positive linear slope >100us/s, and >=65% increasing adjacent seconds",
        "latency": "planned send to complete reply includes client queue. Success percentiles exclude failures. Failures remain in offered denominator",
        "send_counter": "exchange attempts counted immediately before TCP Write after dialing and deadline setup. Successful replies verify completed requests separately",
        "queue_bound": "virtual per-connection backlog age threshold. Expired unsent requests retain offered denominator and deadline-censored observed latency",
        "payload_validation": "deterministic 32-byte value per key and seed. SET repeats that value and GET checks complete expected bytes to detect wrong keys, loss and corruption",
        "reconnect": "current method uses one-second per-connection minimum redial backoff after failure. Original planned epoch and total offers are retained",
        "cdf": "success histogram bins pooled across matching rounds. Logarithmic quantization upper bound <= 0.390625%",
        "resource_sampling": "start-marker to client-exit interval includes drain/reporting/polling, server CPU delta and one-second sampled RSS peak, INFO before marker and after client exit, colocated client",
        "missing_runtime_metrics": "Unavailable engine-specific metrics remain null and are excluded from metric summaries/plots. Redis has no Go heap/GC or native library hash",
        "linux_resource_sampling": "Linux /proc CPU, RSS and I/O; smaps_rollup private memory is opt-in. INFO only at measurement endpoints; Redis uses native TCP in each Kerniva network comparison group",
    }
    (output / "method.json").write_text(json.dumps(metadata, indent=2), encoding="utf-8")
    analysis_argv = [sys.executable, str(Path(__file__).resolve()), str(arguments.run_root.resolve())]
    if arguments.output:
        analysis_argv.extend(["--output", str(output.resolve())])
    if arguments.no_figures:
        analysis_argv.append("--no-figures")
    try:
        matplotlib_version = package_metadata.version("matplotlib")
    except package_metadata.PackageNotFoundError:
        matplotlib_version = None
    reproduction = {
        "python_executable": sys.executable,
        "python_version": sys.version,
        "matplotlib_version": matplotlib_version,
        "analysis_sha256": metadata["analysis_sha256"],
        "working_directory": str(Path.cwd()),
        "analysis_command_argv": analysis_argv,
        "powershell_command": "& " + " ".join("'" + argument.replace("'", "''") + "'" for argument in analysis_argv),
    }
    (output / "reproduction.json").write_text(json.dumps(reproduction, indent=2), encoding="utf-8")
    if not arguments.no_figures:
        render_figures(rows, output)
    print(json.dumps(metadata, indent=2))


if __name__ == "__main__":
    main()
