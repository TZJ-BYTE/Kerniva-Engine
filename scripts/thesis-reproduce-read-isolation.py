#!/usr/bin/env python3
"""Reproduce the WSL sync read-isolation A/B with the existing fixed-rate runner."""

import argparse
import csv
import json
import os
from pathlib import Path
import runpy
import shlex
import shutil
import subprocess
import sys
import tempfile

SCRIPTS = Path(__file__).resolve().parent
REPOSITORY = SCRIPTS.parent
RUNNER = SCRIPTS / "thesis-bench-linux.py"
ANALYZER = SCRIPTS / "thesis-analyze.py"
HOLD, COMMITTED = "sync-hold", "sync-committed"


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2, default=str) + "\n", encoding="utf-8")


def capture(*command):
    return subprocess.check_output(command, cwd=REPOSITORY, text=True).strip()


def run(command, log):
    command = [str(argument) for argument in command]
    log.parent.mkdir(parents=True, exist_ok=True)
    print(f"{log.stem} log: {log}", flush=True)
    with log.open("w", encoding="utf-8") as output:
        output.write(f"$ {shlex.join(command)}\n")
        output.flush()
        subprocess.run(command, cwd=REPOSITORY, stdout=output, stderr=subprocess.STDOUT, check=True)


def prepare_artifacts(root, rust_host):
    artifacts = root / "artifacts"
    artifacts.mkdir()
    target = root / "target"
    library = artifacts / "libkerniva_engine.so"
    server = artifacts / "kerniva-server"
    benchmark = artifacts / "kerniva-bench"
    run(["cargo", "build", "--locked", "--release", "--manifest-path", "kerniva_engine/Cargo.toml",
         "--target", rust_host, "--target-dir", target], root / "build-rust.log")
    shutil.copy2(target / rust_host / "release" / library.name, library)
    for package, binary, log in (("./cmd/server", server, "build-server.log"),
                                 ("./scripts/bench", benchmark, "build-bench.log")):
        run(["go", "build", "-trimpath", "-mod=readonly", "-o", binary, package], root / log)
    environment = {"KERNIVA_STORAGE_BACKEND": "rust-memory-sync", "KERNIVA_RUST_MEMORY_SHARDS": "16"}
    manifest = root / "manifest.json"
    write_json(manifest, {"benchmark": benchmark, "variants": [
        {"name": name, "engine": "kerniva", "server": server, "library": library,
         "environment": {**environment, "KERNIVA_RUST_MEMORY_READ_DURING_SYNC": read_during_sync}}
        for name, read_during_sync in ((HOLD, "false"), (COMMITTED, "true"))
    ]})
    return manifest


def measure(root, manifest, rates, rounds, duration, warmup, keys, clients):
    rate_group = ",".join(str(rate) for rate in rates)
    run([sys.executable, RUNNER, "--run-root", root, "--manifest", manifest,
         "--suite", "read-isolation", "--variants", f"{HOLD},{COMMITTED}",
         "--group", f"std:95:{rate_group}", "--durability", "sync", "--retries", 0,
         "--rounds", rounds, "--duration", duration, "--warmup", warmup,
         "--keys", keys, "--clients", clients, "--hot-percent", 100, "--hot-keys", 1,
         "--seed", 42, "--timeout-ms", 10000], root / "runner.log")
    run([sys.executable, ANALYZER, root, "--no-figures", "--pair", f"{HOLD}:{COMMITTED}"],
        root / "analyzer.log")
    with (root / "analysis" / "paired.csv").open(newline="", encoding="utf-8-sig") as pairs:
        return [row for row in csv.DictReader(pairs)
                if (row["reference"], row["candidate"]) == (HOLD, COMMITTED)]


def sustainable(rows, rate, rounds):
    matched = [row for row in rows if int(row["target_rate"]) == rate]
    return (len(matched) == rounds
            and {int(row["round"]) for row in matched} == set(range(1, rounds + 1))
            and all(row["both_stable"] == "True" for row in matched))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-root", type=Path, help="new or empty directory on ext4")
    parser.add_argument("--smoke", action="store_true", help="run small windows to check the procedure")
    args = parser.parse_args()
    if sys.platform != "linux":
        parser.error("this reproducer requires WSL 2 or Linux on ext4")
    if args.run_root is None:
        parent = REPOSITORY / "logs"
        parent.mkdir(exist_ok=True)
        root = Path(tempfile.mkdtemp(prefix="read-isolation-", dir=parent))
    else:
        root = args.run_root.expanduser().resolve()
        root.mkdir(parents=True, exist_ok=True)
        if any(root.iterdir()):
            parser.error(f"run directory must be empty: {root}")
    filesystem_type = runpy.run_path(str(RUNNER))["filesystem_type"]
    if filesystem_type(root) != "ext4":
        parser.error(f"run directory must be on ext4: {root}")

    calibration, formal = root / "calibration", root / "formal"
    result = {"run_root": root, "profile": "smoke" if args.smoke else "formal",
              "calibration_analysis": calibration / "analysis", "formal_analysis": formal / "analysis"}
    print(f"Run directory: {root}", flush=True)
    try:
        rust_version = capture("rustc", "-vV")
        write_json(root / "provenance.json", {
            "repository": REPOSITORY, "run_root": root, "wrapper": Path(__file__).resolve(),
            "runner": RUNNER, "analyzer": ANALYZER, "linux_release": os.uname().release,
            "commit": capture("git", "rev-parse", "HEAD"),
            "dirty": bool(capture("git", "status", "--porcelain")),
            "python": sys.version, "go": capture("go", "version"),
            "rustc": rust_version, "cargo": capture("cargo", "--version"),
        })
        rust_host = next(line.split()[1] for line in rust_version.splitlines() if line.startswith("host: "))
        manifest = prepare_artifacts(root, rust_host)
        rates = (100, 200) if args.smoke else (2000, 5000)
        keys, clients = (128, 4) if args.smoke else (100_000, 64)
        calibration_rounds = 1 if args.smoke else 2
        rows = measure(calibration, manifest, rates, calibration_rounds,
                       2 if args.smoke else 12, 0 if args.smoke else 3, keys, clients)
        passing = [rate for rate in rates if sustainable(rows, rate, calibration_rounds)]
        result["calibration_passing_rates"] = passing
        if not passing:
            result["status"] = "no_sustainable_calibration_rate"
        else:
            selected = max(passing)
            result["selected_rate"] = selected
            formal_rounds = 2 if args.smoke else 4
            rows = measure(formal, manifest, (selected,), formal_rounds,
                           2 if args.smoke else 30, 0 if args.smoke else 5, keys, clients)
            result["stable_pairs"] = sum(row["both_stable"] == "True" for row in rows)
            result["expected_pairs"] = formal_rounds
            result["status"] = "complete" if sustainable(rows, selected, formal_rounds) else "load_not_sustainable"
    except (OSError, subprocess.CalledProcessError) as error:
        result.update(status="execution_failed", error=str(error))
    write_json(root / "result.json", result)
    print(json.dumps(result, indent=2, default=str))
    return {"complete": 0, "execution_failed": 1}.get(result["status"], 2)


if __name__ == "__main__":
    raise SystemExit(main())
