"""Replay the matched log-age experiments using only mirrored evidence."""
import argparse
from hashlib import file_digest
import json
from pathlib import Path

from golog_batch_report import read, require, window


def report(raw):
    manifest = read(raw / "ext4-readback.json")
    require(manifest["driver_exit_code"] == 0, "incomplete ext4 run")
    expected = [[95, 6000, age] for age in (1, 30, 30, 1)] + [[50, 3000, age] for age in (30, 1, 1, 30)]
    required = {"run.json"}
    inputs = ("capture.json", "case.json", "config.json", "client.json", "client-hist.json", "client-trace.jsonl",
              "server-batches.jsonl", "sealed-files.json")
    for position, (read_percent, rate, _) in enumerate(expected, 1):
        required.update(f"{position:02d}-r{read_percent}-q{rate}-A/{name}" for name in inputs)
    required.update(f"recovery-age-{age}/{name}" for age in (1, 30) for name in ("config.json", "recovery.json"))
    require(required <= manifest["raw_sha256"].keys(), "input missing from ext4 checksum manifest")
    for name, expected_digest in manifest["raw_sha256"].items():
        path = (raw / name).resolve()
        require(path.is_relative_to(raw.resolve()), "manifest path outside capture")
        with path.open("rb") as stream:
            require(file_digest(stream, "sha256").hexdigest() == expected_digest, f"hash mismatch {name}")
    run = read(raw / "run.json")
    require(run["driver_exit_code"] == 0 and run["duration"] == 65, "invalid run receipt")
    require(run["plan"] == expected and len(run["windows"]) == len(expected), "unexpected load/age order")
    windows = []
    for position, ((read_percent, rate, age), capture) in enumerate(zip(expected, run["windows"]), 1):
        folder = raw / f"{position:02d}-r{read_percent}-q{rate}-A"
        require(capture == read(folder / "capture.json") and capture["folder"] == folder.name
                and capture["pass"] and not any(capture["clean"].values()), f"{folder.name}: invalid capture")
        config = read(folder / "config.json")
        require(config["go_log_segment_age_seconds"] == age and config["go_log_group_max"] == 8
                and config["persistence_write_mode"] == "strong" and config["persistence_durability"] == "sync"
                and config["network_type"] == "gnet", "incorrect primary settings")
        client = read(folder / "client.json")
        require((client["clients"], client["keys"], client["read_percent"], client["target_rate"],
                 client["duration_seconds"], client["sent"], client["succeeded"], client["failures"])
                == (64, 100000, read_percent, rate, 65, rate * 65, rate * 65, 0), "incorrect client receipt")
        joined = window(folder)
        seals = read(folder / "sealed-files.json")
        case = read(folder / "case.json")
        release = case["client_release_interval"]["start_ns"]
        exit_ns = next(event["monotonic_ns"] for event in case["events"] if event["name"] == "client_exited")
        require(seals["before_release"]["end"]["monotonic_after_ns"] <= release <= exit_ns
                <= seals["after_client"]["start"]["monotonic_before_ns"], "seal census does not enclose client")
        counts = {}
        for db in ("0", "1"):
            before = set(seals["before_release"]["segments"][db])
            after = set(seals["after_client"]["segments"][db])
            require(before <= after, f"sealed segments disappeared in DB {db}")
            counts[db] = len(after - before)
        windows.append({
            "window": folder.name, "load": [read_percent, rate], "age_s": age, "requests": joined["requests"],
            "failures": joined["failures"], "window_success_rate": client["window_success_rate"],
            "drain_s": client["drain_seconds"], "joined_sets": joined["joined_sets"], "p99_us": joined["p99_us"],
            "slow_ge100ms": joined["slow"], "sealed_during_client": counts, "batches": joined["batches"],
            "set_sync_ns": joined["phases_ns"]["sync"], "set_queue_ns": joined["phases_ns"]["batch_queue"],
            "set_writer_lock_ns": joined["phases_ns"]["writer_lock"],
        })
    recovered = {}
    for age in (1, 30):
        folder = raw / f"recovery-age-{age}"
        settings = read(folder / "config.json")
        require(settings["go_log_segment_age_seconds"] == age and settings["persistence_durability"] == "sync",
                f"age {age}: wrong recovery settings")
        data = read(folder / "recovery.json")
        require(data["crash_exit"] == -9 and data["pass"] and not any(data["clean"].values())
                and data["expected_sequence"] == data["recovered_sequence"] == data["resumed_sequence"] - 1
                and data["keys"] and all(v["expected"] == v["recovered"] for v in data["keys"].values()),
                f"age {age}: acknowledged writes did not survive process restart")
        recovered[age] = {"keys": data["keys"], "restored_sequence": data["recovered_sequence"],
                          "resumed_sequence": data["resumed_sequence"]}
    return {"windows": windows, "recovery": recovered, "verified_files": len(manifest["raw_sha256"])}


if __name__ == "__main__":
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument("--raw", type=Path, required=True)
    cli.add_argument("--out", type=Path, required=True)
    args = cli.parse_args()
    result = report(args.raw)
    args.out.write_text(json.dumps(result, indent=2, allow_nan=False) + "\n", encoding="utf-8")
