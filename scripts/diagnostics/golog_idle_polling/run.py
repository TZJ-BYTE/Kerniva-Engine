#!/usr/bin/env python3
"""Measure allocations per successfully completed unchanged pull on Linux ext4."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import tarfile
import tempfile
import time


HERE = Path(__file__).resolve().parent
FOLLOWER = Path("internal/gologreplica/follower_linux.go")
TEST_INPUT = HERE / "idle_diagnostic_linux_test.go.txt"
TEST_OUTPUT = FOLLOWER.with_name("idle_diagnostic_linux_test.go")

# Apply only in the disposable copy. The first callback parks workers between
# polls. The second observes the result after fetch and status publication.
INSTRUMENTATION = (
    (
        "// Run polls each database independently",
        "var idleDiagnosticBeforePoll func(context.Context, int)\n"
        "var idleDiagnosticPollResult func(int, bool, error)\n\n"
        "// Run polls each database independently",
    ),
    (
        "\t\t\t\tf.pollDatabase(ctx, db)",
        "\t\t\t\tif idleDiagnosticBeforePoll != nil {\n"
        "\t\t\t\t\tidleDiagnosticBeforePoll(ctx, db.number)\n"
        "\t\t\t\t}\n"
        "\t\t\t\tf.pollDatabase(ctx, db)",
    ),
    (
        "\treturn err\n}\n\nfunc (db *replicaDB) publishView",
        "\tif idleDiagnosticPollResult != nil {\n"
        "\t\tidleDiagnosticPollResult(db.number, previous.view != nil && next == previous.view, err)\n"
        "\t}\n"
        "\treturn err\n}\n\nfunc (db *replicaDB) publishView",
    ),
)


def digest(path):
    with path.open("rb") as file:
        return hashlib.file_digest(file, "sha256").hexdigest()


def go(*args, cwd=None, env=None, stdout=None):
    subprocess.run(["go", *args], cwd=cwd, env=env, stdout=stdout, check=True)


def copy_source(args, destination):
    if args.checkout:
        ignore_artifacts = shutil.ignore_patterns(
            ".git", "bin", "build", "dist", "target", "logs", "*.log", "modules",
            "node_modules", "vendor", "tmp", "temp", "__pycache__", "bench-results",
        )

        def ignore(directory, names):
            excluded = ignore_artifacts(directory, names)
            if Path(directory) == args.checkout and "data" in names:
                excluded.add("data")
            return excluded

        shutil.copytree(args.checkout, destination, ignore=ignore, symlinks=True)
        return str(args.checkout)
    destination.mkdir()
    revision = subprocess.check_output(
        ["git", "-C", args.repo, "rev-parse", args.revision], text=True).strip()
    with tempfile.TemporaryFile() as archive:
        subprocess.run(["git", "-C", args.repo, "archive", revision], stdout=archive, check=True)
        archive.seek(0)
        with tarfile.open(fileobj=archive) as snapshot:
            snapshot.extractall(destination, filter="data")
    return revision


def build(args, root):
    src = root / "src"
    source_identity = copy_source(args, src)
    original = digest(src / FOLLOWER)
    follower = (src / FOLLOWER).read_text()
    for before, after in INSTRUMENTATION:
        if follower.count(before) != 1:
            raise ValueError(f"unrecognized follower source near {before!r}")
        follower = follower.replace(before, after)
    (src / FOLLOWER).write_text(follower)
    shutil.copyfile(TEST_INPUT, src / TEST_OUTPUT)
    subprocess.run(["gofmt", "-w", FOLLOWER, TEST_OUTPUT], cwd=src, check=True)
    binary = root / "idle-diagnostic"
    go("test", "-c", "-o", binary, "./internal/gologreplica", cwd=src)
    (root / "build.json").write_text(json.dumps({
        "source": source_identity,
        "go_version": subprocess.check_output(["go", "version"], text=True).strip(),
        "original_follower_sha256": original,
        "instrumented_follower_sha256": digest(src / FOLLOWER),
        "diagnostic_test_sha256": digest(src / TEST_OUTPUT),
        "runner_sha256": digest(Path(__file__)),
        "follower_binary_sha256": digest(binary),
        "source_binary_sha256": digest(args.source_binary or binary),
        "rust_library_sha256": digest(args.library),
        "origin": str(args.origin),
        "origin_result_sha256": digest(args.origin / "result.json"),
    }, indent=2) + "\n")
    return binary


def wait_source(source, log):
    for _ in range(100):
        match = re.search(r"^source_url=(http://[^\s]+)", log.read_text(), re.MULTILINE)
        if match:
            return match[1]
        if source.poll() is not None:
            break
        time.sleep(0.05)
    raise RuntimeError(f"source failed to start, see {log}")


def profile_report(binary, run):
    before, after = run / "alloc.before.pprof", run / "alloc.after.pprof"
    workload = run / "alloc.workload.pprof"
    with workload.open("wb") as output:
        go("tool", "pprof", "-alloc_space", "-proto", "-ignore", "writeIdleDiagnosticProfile",
           "-base", before, binary, after, stdout=output)
    reports = {
        "alloc.workload.top.txt": ["-top", "-nodecount=50"],
        "alloc.workload.cum.txt": ["-top", "-cum", "-nodecount=60"],
        "alloc.bufio.txt": ["-traces", "-focus", "bufio.NewReaderSize"],
    }
    for name, options in reports.items():
        with (run / name).open("wb") as output:
            go("tool", "pprof", "-alloc_space", *options, binary, workload, stdout=output)


def measure(args, root, binary, index, interval, mode):
    run = root / f"{index:02d}-{interval}-{mode}"
    run.mkdir()
    env = {key: value for key, value in os.environ.items()
           if not key.startswith(("KERNIVA_", "IDLE_DIAGNOSTIC_"))}
    env.update(
        KERNIVA_ENGINE_LIBRARY=str(args.library),
        IDLE_DIAGNOSTIC_SOURCE_ROOT=str(root / "primary-data" / "go-log-v1"),
        IDLE_DIAGNOSTIC_FOLLOWER_ROOT=str(root / "b"),
        IDLE_DIAGNOSTIC_INTERVAL=interval,
        IDLE_DIAGNOSTIC_DURATION=args.duration,
    )
    if mode == "profile":
        env.update(IDLE_DIAGNOSTIC_PROFILE_PREFIX=str(run / "alloc"),
                   IDLE_DIAGNOSTIC_PROFILE_RATE="1024")
    source_log = run / "source.log"
    follower_log = run / "follower.log"
    with source_log.open("wb") as source_output, follower_log.open("wb") as follower_output:
        source = subprocess.Popen(
            [args.source_binary or binary, "-test.run", "^TestIdleDiagnosticSource$"],
            env=env, stdout=source_output, stderr=subprocess.STDOUT,
        )
        try:
            env["IDLE_DIAGNOSTIC_UPSTREAM"] = wait_source(source, source_log)
            subprocess.run([binary, "-test.run", "^TestIdleDiagnosticFollower$"], env=env,
                           stdout=follower_output, stderr=subprocess.STDOUT, check=True)
        finally:
            if source.poll() is None:
                source.terminate()
            if source.wait(timeout=10):
                raise RuntimeError(f"source failed, see {source_log}")

    result = json.loads(follower_log.read_text().splitlines()[0])
    counts = re.search(r"source_pulls=(\d+) source_new_connections=(\d+)", source_log.read_text())
    if counts is None:
        raise RuntimeError(f"no source counts, see {source_log}")
    pulls, connections = map(int, counts.groups())
    if pulls != result["completed_no_change"] + result["warmup_no_change"]:
        raise RuntimeError(f"B and source pull counts differ, see {run}")
    result.update(mode=mode, source_pulls=pulls, source_new_connections=connections)
    (run / "result.json").write_text(json.dumps(result, indent=2) + "\n")
    if mode == "profile":
        profile_report(binary, run)
    print(f"{run.name} {result['completed_no_change']} pulls "
          f"{result['alloc_bytes_per_pull']:.0f} B/pull, {connections}/{pulls} new connections",
          flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True,
                        help="new ext4 output directory outside --checkout")
    parser.add_argument("--origin", type=Path, required=True, help="converged ordinary fixture")
    parser.add_argument("--library", type=Path, required=True, help="frozen libkerniva_engine.so")
    parser.add_argument("--repo", type=Path, default=HERE.parents[2], help="Git repository to archive")
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--revision", help="Git revision from --repo")
    source.add_argument("--checkout", type=Path, help="candidate source tree to copy")
    parser.add_argument("--source-binary", type=Path, help="earlier idle-diagnostic binary for the source role")
    parser.add_argument("--duration", default="15s", help="requested interval between worker barriers")
    parser.add_argument("--window", nargs=2, action="append", metavar=("INTERVAL", "MODE"),
                        help="run in this order, with interval 100ms or 1s and mode exact or profile")
    args = parser.parse_args()
    windows = args.window or [("100ms", "exact"), ("1s", "exact"),
                              ("100ms", "profile"), ("1s", "profile")]
    for interval, mode in windows:
        if interval not in {"100ms", "1s"} or mode not in {"exact", "profile"}:
            parser.error("window expects 100ms or 1s, followed by exact or profile")
    for name in ("root", "origin", "library", "repo", "checkout", "source_binary"):
        path = getattr(args, name)
        if path is not None:
            setattr(args, name, path.resolve())
    if args.checkout and args.root.is_relative_to(args.checkout):
        parser.error("--root must be outside --checkout")
    fs = subprocess.check_output(["findmnt", "-n", "-o", "FSTYPE", "-T", args.root.parent],
                                 text=True).strip()
    if fs != "ext4":
        parser.error(f"expected an ext4 output directory, got {fs}")
    origin_result = json.loads((args.origin / "result.json").read_text())
    if not origin_result["passed"]:
        parser.error("origin fixture must have passed")
    args.root.mkdir()
    shutil.copytree(args.origin / "b", args.root / "b")
    shutil.copytree(args.origin / "primary-data", args.root / "primary-data")
    binary = build(args, args.root)
    for index, (interval, mode) in enumerate(windows, 1):
        measure(args, args.root, binary, index, interval, mode)


if __name__ == "__main__":
    main()
