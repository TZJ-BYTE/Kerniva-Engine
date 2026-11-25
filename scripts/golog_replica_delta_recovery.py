#!/usr/bin/env python3
"""Verify a truncated DB 15 delta and two offline restarts of the production follower."""

import argparse
from contextlib import ExitStack, closing, nullcontext
import gzip
from hashlib import sha256
from http.client import HTTPConnection
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import shutil
import socket
from threading import Event, Lock, Thread
import time
from urllib.parse import parse_qs, urlsplit

from golog_replica_direct_delta import DB, KEYS, VALUE_BYTES, record
from golog_replica_poll_ordinary import filesystem
from golog_replica_snapshot_capacity import (
    MARKER_KEY, Experiment, check, db_dir, digest, get, require, save_result, unpack_manifest,
)

AFTER, LAST = 4, 260
FILE_COUNT = 18
PAYLOAD_BYTES = 67_126_416
CUT_BYTES = 512 << 10
CHUNK_BYTES = 64 << 10
MARKER = "fixed-link-snapshot-capacity-marker-4"
PREPARED_SHA = "68aa8aa837030b5782e87bf845a17109d7d7e8f5b8461f6d33d861cb382a7cd0"
BLOCKED = b"recovery test source unavailable"
FAULT_LABEL, RETRY_LABEL, OFFLINE_LABEL = "b-truncated", "b-blocked-restart", "b-offline-restart"
OLD_PHASES = "partial", "after_truncated", "before_blocked_restart", "blocked_restart"
NEW_PHASES = "published", "offline_restart"


class Relay:
    def __init__(self, source, manifest):
        self.source, self.manifest = source, manifest
        self.requests, self.errors = [], []
        self.cut_reached, self.drop = Event(), Event()
        self.reply_blocked = Event()
        self.lock = Lock()
        self.mode = "fault"

    def switch(self, before, after):
        with self.lock:
            require(self.mode, before, "relay mode")
            self.mode = after
            return time.monotonic_ns()

    def start(self, context):
        relay = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                try:
                    relay.forward(self)
                except Exception as error:
                    relay.errors.append(repr(error))

            def log_message(self, *_args):
                pass

        server = context.enter_context(ThreadingHTTPServer(("127.0.0.1", 0), Handler))
        server.daemon_threads = False
        thread = Thread(target=server.serve_forever, daemon=True)
        thread.start()

        def stop():
            self.drop.set()
            self.reply_blocked.set()
            server.shutdown()
            thread.join()

        context.callback(stop)
        return server.server_port

    def forward(self, handler):
        path = urlsplit(handler.path)
        query = parse_qs(path.query)
        after = int(query["after"][0])
        require((path.path, query.get("db"), query.get("source")),
                ("/pull", [str(DB)], [self.manifest["source"]]), "follower pull identity")
        with self.lock:
            mode = self.mode
            if mode in ("fault", "retry"):
                require(after, AFTER, "pull starts from old cursor")
                self.mode = "blocked" if mode == "fault" else "caught_up"
            elif mode == "caught_up":
                require(after, LAST, "no second successful delta response")
            row = {"mode": mode, "query": query, "forwarded_bytes": 0,
                   "request_ns": time.monotonic_ns(), "upstream_requested": False}
            self.requests.append(row)
        if mode in ("blocked", "offline"):
            require(self.reply_blocked.wait(30), True, "observer verified truncated response")
            handler.send_response(503)
            handler.send_header("Content-Length", str(len(BLOCKED)))
            handler.end_headers()
            handler.wfile.write(BLOCKED)
            row.update(status=503, outcome="blocked", end_ns=time.monotonic_ns())
            return

        with closing(HTTPConnection("127.0.0.1", self.source, timeout=15)) as conn:
            row["upstream_requested"] = True
            conn.request("GET", handler.path)
            with conn.getresponse() as response:
                require(response.status, 200, "source pull status")
                prefix, manifest = unpack_manifest(response)
                row.update(status=response.status, manifest=manifest)
                if mode == "caught_up":
                    require((manifest["db"], manifest["source"], manifest["after"], manifest["last"],
                             manifest.get("files")), (DB, self.manifest["source"], LAST, LAST, None),
                            "caught-up manifest")
                    size = 0
                else:
                    require(manifest, self.manifest, "exact sealed delta manifest")
                    size = PAYLOAD_BYTES
                handler.send_response(200)
                handler.send_header("Content-Type", "application/octet-stream")
                handler.send_header("Content-Length", str(len(prefix) + size))
                handler.end_headers()
                handler.wfile.write(prefix)
                limit = CUT_BYTES if mode == "fault" else size
                while row["forwarded_bytes"] < limit:
                    chunk_size = min(CHUNK_BYTES, limit - row["forwarded_bytes"])
                    chunk = response.read(chunk_size)
                    require(len(chunk), chunk_size, "upstream payload chunk")
                    handler.wfile.write(chunk)
                    row["forwarded_bytes"] += len(chunk)
                if mode == "fault":
                    handler.wfile.flush()
                    row["cut_reached_ns"] = time.monotonic_ns()
                    self.cut_reached.set()
                    require(self.drop.wait(30), True, "observer released partial body")
                    row.update(outcome="truncated", end_ns=time.monotonic_ns())
                    handler.connection.shutdown(socket.SHUT_WR)
                else:
                    require(response.read(1), b"", "source response ended")
                    row.update(outcome="complete", end_ns=time.monotonic_ns())


def stage_inputs(run, origin, prepared, index):
    original_b = origin / "b"
    follower = run.root / "b"
    destination = db_dir(follower, DB)
    destination.parent.mkdir(parents=True)
    shutil.copy2(original_b / "follower-v1/UPSTREAM", destination.parent)
    shutil.copytree(db_dir(original_b, DB), destination)
    before_raw = (destination / "CURRENT").read_bytes()
    require(before_raw, (db_dir(original_b, DB) / "CURRENT").read_bytes(), "copied CURRENT bytes")
    require(json.loads(before_raw), index["before"], "sealed block starting cursor")

    source = prepared / "primary-data/go-log-v1"
    run.storage.mkdir(parents=True)
    shutil.copy2(source / ".replica-source-id", run.storage)
    copied = run.storage / f"db_{DB}"
    shutil.copytree(source / f"db_{DB}", copied)
    verify_files(copied, index["files"])
    return before_raw


def verify_files(directory, files):
    checked = {}
    for name, expected in files.items():
        path = directory / name
        observed = {"bytes": path.stat().st_size, "sha256": digest(path)}
        require(observed, expected, f"sealed file {name}")
        checked[name] = observed
    return checked


def wait_state(run, relay, label, predicate):
    def observe():
        require(relay.errors, [], "relay errors")
        state = run.status(run.b, DB)
        return state if state and predicate(state) else None

    return run.wait(label, observe, 30)


def unavailable(state):
    return state["last_error"] == f"source HTTP 503: {BLOCKED.decode()}"


def verify_state(run, state, expected_cursor, installs, reads, expected_raw, phase):
    current_path = db_dir(run.root / "b", DB) / "CURRENT"
    raw = current_path.read_bytes()
    require(raw, expected_raw, "CURRENT expected raw bytes")
    cursor = json.loads(raw)
    require(cursor, expected_cursor, "durable cursor")
    require((state["ready"], state["source"], state["sequence"], state["installs"], state["snapshot"]),
            (True, cursor["source"], cursor["sequence"], installs, cursor["snapshot"]), "HTTP status")
    require(get(run.b, DB, MARKER_KEY), (200, MARKER.encode()), "original readable marker")
    installed = cursor["sequence"] == LAST
    archive = run.root / f"{phase}-get-values.gz"
    observations = []
    output = gzip.open(archive, "wb", compresslevel=1) if installed else nullcontext()
    with output as payloads:
        for key, expected in reads.items():
            status, value = get(run.b, DB, key)
            require(status, 200 if installed else 404, key)
            row = {"key": key, "status": status}
            if installed:
                checksum = sha256(value).hexdigest()
                require((len(value), checksum), (VALUE_BYTES, expected), f"GET value of {key}")
                row.update(bytes=len(value), sha256=checksum)
                payloads.write(value)
            observations.append(row)
    require(current_path.read_bytes(), raw, "CURRENT stable through GET verification")
    keys = {"count": len(observations), "observations": observations}
    if installed:
        keys["payload_archive"] = {"file": archive.name, "sha256": digest(archive)}
    return {"status": state, "current": cursor, "current_bytes": raw.decode(),
            "current_sha256": sha256(raw).hexdigest(), "marker": {"key": MARKER_KEY, "status": 200, "value": MARKER},
            "new_keys": keys, "verified_ns": time.monotonic_ns()}


def start_b(run, executable, relay_port, label, data):
    trace = run.root / f"{label}-memory.jsonl"
    started_ns = time.monotonic_ns()
    child = run.start(label, executable, "follow", "--root", run.root / "b", "--dbs", str(DB),
                      "--source", f"http://127.0.0.1:{relay_port}", "--listen", f"127.0.0.1:{run.b}",
                      env={**run.env, "KERNIVA_GOLOG_MEMORY_TRACE": str(trace)})
    proc_exe = Path(f"/proc/{child.pid}/exe")
    identity = {"path": str(proc_exe.resolve(strict=True)), "sha256": digest(proc_exe)}
    require(identity, data["follower"], "running B executable identity")
    data[label] = {"pid": child.pid, "started_ns": started_ns, "executable": identity,
                   "trace": trace.name, "log": f"{label}.log"}
    return child, trace


def finish_b(child, trace, evidence, startup, native_library):
    mappings = Path(f"/proc/{child.pid}/maps").read_text().splitlines()
    mapped_paths = {fields[5] for line in mappings if len(fields := line.split(maxsplit=5)) == 6}
    library_path = native_library["path"]
    require(library_path in mapped_paths, True, "B mapped the frozen native library")
    evidence["native_library"] = {"path": library_path, "sha256": digest(Path(library_path))}
    require(evidence["native_library"], native_library, "running B native library identity")
    check.stop(child)
    evidence.update(exit_status=child.returncode, stopped_ns=time.monotonic_ns())
    require(child.returncode, 0, "B graceful shutdown")
    evidence["trace_sha256"] = digest(trace)
    evidence["log_sha256"] = digest(trace.parent / evidence["log"])
    startup_events, install_events = trace_events(trace, startup)
    evidence["startup_events"] = startup_events
    return install_events


def trace_events(trace, startup):
    events = [json.loads(line) for line in trace.read_bytes().splitlines()]
    require(all(event["db"] == DB for event in events), True, "trace belongs to DB 15")
    reload_events = [event for event in events if event["kind"] == "startup"]
    require(all((event["generation"], event["sequence"]) == (startup["generation"], startup["sequence"])
                for event in reload_events), True, "startup trace matches durable generation")
    essential = {"rust_recovery_start", "rust_handle_ready", "rust_rows_restored", "read_view_switched"}
    startup_events = [event for event in reload_events if event["stage"] in essential]
    require([event["stage"] for event in startup_events],
            ["rust_recovery_start", "rust_handle_ready", "rust_rows_restored", "read_view_switched"],
            "one native startup recovery")
    require([event.get("recovery") for event in startup_events[:3]], ["full"] * 3,
            "startup native full replay")
    return startup_events, [event for event in events if event["kind"] != "startup"]


def run_recovery(run, follower, relay, before_raw, files, reads, data):
    before = json.loads(before_raw)
    relay_port = relay.start(run.context)
    data["ports"] = {"source": run.source, "relay": relay_port, "b": run.b}
    child, trace = start_b(run, follower, relay_port, FAULT_LABEL, data)
    require(relay.cut_reached.wait(15), True, "relay forwarded 512 KiB")
    state = wait_state(run, relay, "old view during partial download", lambda state: state["ready"])
    data["partial"] = verify_state(run, state, before, 0, reads, before_raw, "partial")
    relay.drop.set()
    first_file = relay.manifest["files"][0]["name"]
    state = wait_state(run, relay, "B observed truncated body",
                       lambda state: state["last_error"] == f"receive {first_file}: unexpected EOF")
    data["after_truncated"] = verify_state(run, state, before, 0, reads, before_raw, "after_truncated")
    relay.reply_blocked.set()
    state = wait_state(run, relay, "pre-restart retry was blocked", unavailable)
    data["before_blocked_restart"] = verify_state(run, state, before, 0, reads, before_raw, "before_blocked_restart")
    events = finish_b(child, trace, data[FAULT_LABEL], before, data["native_library"])
    data[FAULT_LABEL]["install_events"] = events
    require([(event["kind"], event["stage"], event["sequence"]) for event in events],
            [("delta", "install_start", LAST)], "failed download never reached publication")

    child, trace = start_b(run, follower, relay_port, RETRY_LABEL, data)
    state = wait_state(run, relay, "B recovered old cursor with source blocked", unavailable)
    data["blocked_restart"] = verify_state(run, state, before, 0, reads, before_raw, "blocked_restart")
    data["relay_reopened_ns"] = relay.switch("blocked", "retry")
    state = wait_state(run, relay, "B installed the complete delta",
                       lambda state: state["sequence"] == LAST and not state["last_error"])
    after_raw = (db_dir(run.root / "b", DB) / "CURRENT").read_bytes()
    after = json.loads(after_raw)
    require((after["source"], after["sequence"], after["snapshot"], after["generation"] != before["generation"]),
            (before["source"], LAST, before["snapshot"], True), "new generation cursor")
    data["published"] = verify_state(run, state, after, 1, reads, after_raw, "published")
    data["verified_files"] = verify_files(db_dir(run.root / "b", DB) / after["generation"], files)
    events = finish_b(child, trace, data[RETRY_LABEL], before, data["native_library"])
    data["publication_events"] = publication_events(events, after, data["relay_reopened_ns"])
    data["relay_closed_ns"] = relay.switch("caught_up", "offline")
    return after, after_raw


def publication_events(events, after, reopened_ns):
    require(all((event["kind"], event["generation"], event["sequence"]) ==
                ("delta", after["generation"], LAST) for event in events), True, "one published delta generation")
    publication = [event for event in events if event["stage"] in ("current_published", "read_view_switched")]
    require([event["stage"] for event in publication], ["current_published", "read_view_switched"],
            "one durable publication before view switch")
    require(reopened_ns < publication[0]["monotonic_ns"] < publication[1]["monotonic_ns"],
            True, "monotonic durable and readable event order")
    return publication


def check_relay(requests, errors):
    require(errors, [], "clean relay")
    forwarded = [row for row in requests if row["upstream_requested"] and row["mode"] != "caught_up"]
    require([(row["mode"], row["outcome"], row["forwarded_bytes"]) for row in forwarded],
            [("fault", "truncated", CUT_BYTES), ("retry", "complete", PAYLOAD_BYTES)],
            "one deliberately truncated response and one complete delta")
    blocked = [row for row in requests if row["outcome"] == "blocked"]
    require({row["mode"] for row in blocked}, {"blocked", "offline"}, "old and final offline relay responses")
    require(all((row["status"], row["upstream_requested"], row["forwarded_bytes"]) == (503, False, 0)
                for row in blocked), True, "blocked relay did not contact source")
    return forwarded


def audit_result(folder):
    """Recompute the recovery checks from the small evidence set after a run."""
    folder = Path(folder).resolve(strict=True)
    path = folder / "result.json"
    data = json.loads(path.read_bytes())
    hashes = {path.name: digest(path)}
    require(data["passed"], True, "completed recovery")
    require((data["db"], data["before_sequence"], data["after_sequence"], data["file_count"],
             data["payload_bytes"], data["cut_payload_bytes"], data["key_count"]),
            (DB, AFTER, LAST, FILE_COUNT, PAYLOAD_BYTES, CUT_BYTES, KEYS), "recovery fixture")
    require(PREPARED_SHA in data["artifacts_sha256"].values(), True, "historical sealed fixture index")
    require(data["verified_files"], data["expected_files"], "installed file identities")
    require(data["source_exit_status"], 0, "offline source exit")

    def artifact(name, expected):
        local = (folder / name).resolve(strict=True)
        require((local.parent, local.name), (folder, name), "recovery evidence path")
        observed = digest(local)
        require(observed, expected, f"evidence checksum for {name}")
        hashes[name] = observed
        return local

    artifact("source.log", data["source_log_sha256"])
    before = data["partial"]["current"]
    after = data["published"]["current"]
    require((before["sequence"], after["sequence"], before["source"], after["source"],
             before["snapshot"], after["snapshot"], before["generation"] != after["generation"]),
            (AFTER, LAST, data["manifest"]["source"], data["manifest"]["source"], 0, 0, True), "cursor transition")
    reads = {key: sha256(value.encode()).hexdigest() for key, value in map(record, range(KEYS))}
    require(data["expected_key_sha256"], reads, "recomputed expected GET SHA-256 values")
    for phase in (*OLD_PHASES, *NEW_PHASES):
        row = data[phase]
        installed = phase in NEW_PHASES
        expected_current = data["published" if installed else "partial"]["current_bytes"]
        require((row["current_bytes"], row["current"], row["current_sha256"]),
                (expected_current, json.loads(expected_current), sha256(expected_current.encode()).hexdigest()),
                f"{phase} CURRENT bytes")
        status = row["status"]
        require((status["ready"], status["sequence"], status["source"], status["snapshot"], status["installs"]),
                (True, LAST if installed else AFTER, before["source"], 0, int(phase == "published")),
                f"{phase} status")
        require(row["marker"], {"key": MARKER_KEY, "status": 200, "value": MARKER}, f"{phase} marker")
        keys = row["new_keys"]
        require((keys["count"], [item["key"] for item in keys["observations"]]),
                (KEYS, list(reads)), f"{phase} GET key set")
        require([item["status"] for item in keys["observations"]], [200 if installed else 404] * KEYS,
                f"{phase} GET statuses")
        if installed:
            name = f"{phase}-get-values.gz"
            evidence = keys["payload_archive"]
            require(evidence["file"], name, f"{phase} GET archive name")
            with gzip.open(artifact(name, evidence["sha256"]), "rb") as payloads:
                for item in keys["observations"]:
                    body = payloads.read(VALUE_BYTES)
                    require((len(body), sha256(body).hexdigest(), item["bytes"], item["sha256"]),
                            (VALUE_BYTES, reads[item["key"]], VALUE_BYTES, reads[item["key"]]),
                            f"{phase} raw GET {item['key']}")
                require(payloads.read(1), b"", f"{phase} end of GET archive")

    follower = data["follower"]
    library = data["native_library"]
    require(data["artifacts_sha256"][follower["path"]], follower["sha256"], "configured B binary digest")
    require(data["artifacts_sha256"][library["path"]], library["sha256"], "configured native library digest")
    require(len({data[label]["pid"] for label in (FAULT_LABEL, RETRY_LABEL, OFFLINE_LABEL)}), 3, "three B processes")
    for label, initial in ((FAULT_LABEL, before), (RETRY_LABEL, before), (OFFLINE_LABEL, after)):
        observed = data[label]
        require((observed["executable"], observed["native_library"], observed["exit_status"]),
                (follower, library, 0), f"{label} binary, native library, and exit")
        require((observed["trace"], observed["log"]), (f"{label}-memory.jsonl", f"{label}.log"),
                f"{label} evidence names")
        artifact(observed["log"], observed["log_sha256"])
        trace = artifact(observed["trace"], observed["trace_sha256"])
        startup, events = trace_events(trace, initial)
        require(startup, observed["startup_events"], f"{label} native startup trace")
        for event in (*startup, *events):
            require(observed["started_ns"] <= event["monotonic_ns"] <= observed["stopped_ns"],
                    True, f"{label} event within process lifetime")
        if label == FAULT_LABEL:
            require(events, observed["install_events"], "truncated process trace")
            require([(event["kind"], event["stage"], event["sequence"]) for event in events],
                    [("delta", "install_start", LAST)], "failed download never reached publication")
        elif label == RETRY_LABEL:
            require(publication_events(events, after, data["relay_reopened_ns"]), data["publication_events"],
                    "same-generation durable and readable trace events")
        else:
            require(events, [], "offline process performed no install")
    fault, retry = check_relay(data["relay_requests"], data["relay_errors"])
    require((fault["manifest"], retry["manifest"]), (data["manifest"], data["manifest"]), "identical relay manifests")
    require(fault["cut_reached_ns"] <= data["partial"]["verified_ns"] <= fault["end_ns"]
            <= data["after_truncated"]["verified_ns"], True, "old view verified on both sides of cut")
    require(data[FAULT_LABEL]["stopped_ns"] <= data[RETRY_LABEL]["started_ns"]
            <= data["blocked_restart"]["verified_ns"] <= data["relay_reopened_ns"] <= retry["request_ns"],
            True, "old-view restart while relay remained closed")
    require(data[RETRY_LABEL]["stopped_ns"] <= data["relay_closed_ns"] <= data["source_stopped_ns"]
            <= data[OFFLINE_LABEL]["started_ns"], True, "source stop before offline start")
    require(all(unavailable(data[phase]["status"]) for phase in
                ("before_blocked_restart", "blocked_restart", "offline_restart")), True, "recorded source gate")
    first = data["manifest"]["files"][0]["name"]
    require(data["after_truncated"]["status"]["last_error"], f"receive {first}: unexpected EOF",
            "truncated delta body error")
    return hashes


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--audit", type=Path, help="re-audit an existing recovery result without starting processes")
    parser.add_argument("--root", type=Path, help="existing empty ext4 output directory")
    parser.add_argument("--origin", type=Path, help="stopped ordinary fixture at DB 15 sequence 4")
    parser.add_argument("--prepared", type=Path, help="the historical passed direct-1 sealed block")
    parser.add_argument("--bin", type=Path, help="frozen production source and native library")
    parser.add_argument("--follower", type=Path, help="ordinary follow executable")
    args = parser.parse_args()
    inputs = (args.root, args.origin, args.prepared, args.bin, args.follower)
    if args.audit is not None:
        if any(item is not None for item in inputs):
            parser.error("--audit only takes the existing recovery directory")
        print(json.dumps(audit_result(args.audit), indent=2))
        return
    if any(item is None for item in inputs):
        parser.error("running recovery requires --root, --origin, --prepared, --bin and --follower")
    root, origin, prepared, binary, follower = (path.resolve() for path in inputs)
    require(list(root.iterdir()), [], "empty recovery output directory")
    data = {"db": DB, "before_sequence": AFTER, "after_sequence": LAST, "file_count": FILE_COUNT,
            "payload_bytes": PAYLOAD_BYTES, "cut_payload_bytes": CUT_BYTES, "key_count": KEYS}
    with save_result(root, data), ExitStack() as stack:
        device = filesystem(root)
        require((filesystem(origin), filesystem(prepared)), (device, device), "same ext4 device")
        origin_result, prepared_result = origin / "result.json", prepared / "result.json"
        library = (binary / "libkerniva_engine.so").resolve(strict=True)
        artifacts = (origin_result, prepared_result, binary / "kerniva-replica", follower,
                     library, Path(__file__).resolve())
        data["artifacts_sha256"] = {str(path): digest(path) for path in artifacts}
        require(data["artifacts_sha256"][str(prepared_result)], PREPARED_SHA, "historical sealed block SHA-256")
        prior = json.loads(origin_result.read_bytes())
        index = json.loads(prepared_result.read_bytes())
        require((prior["passed"], index["passed"], index["db"], index["keys"], index["value_bytes"]),
                (True, True, DB, KEYS, VALUE_BYTES), "historical fixture configuration")
        manifest = index["manifest"]
        require((manifest["db"], manifest["after"], manifest["last"], manifest.get("snapshot", 0),
                 manifest.get("rebase", False)), (DB, AFTER, LAST, 0, False), "historical delta span")
        files = index["files"]
        require({item["name"]: item["size"] for item in manifest["files"]},
                {name: item["bytes"] for name, item in files.items()}, "manifest file sizes")
        require((len(files), sum(item["bytes"] for item in files.values())),
                (FILE_COUNT, PAYLOAD_BYTES), "historical sealed file count and payload bytes")
        reads = {key: sha256(value.encode()).hexdigest() for key, value in map(record, range(KEYS))}
        data.update(manifest=manifest, expected_files=files, expected_key_sha256=reads, device=device,
                    follower={"path": str(follower), "sha256": data["artifacts_sha256"][str(follower)]},
                    native_library={"path": str(library), "sha256": data["artifacts_sha256"][str(library)]})
        run = Experiment(root, binary, binary / "kerniva-replica", root / "source-data", stack)
        before_raw = stage_inputs(run, origin, prepared, index)
        relay = Relay(run.source, manifest)
        data.update(relay_requests=relay.requests, relay_errors=relay.errors)
        source = run.start("source", run.replica, "serve", "--root", run.storage,
                           "--listen", f"127.0.0.1:{run.source}")
        run.wait("source is listening", lambda: "listening on" in (root / "source.log").read_text(), 15)
        after, after_raw = run_recovery(run, follower, relay, before_raw, files, reads, data)
        check.stop(source)
        require(source.returncode, 0, "source stopped for second offline restart")
        data["source_stopped_ns"] = time.monotonic_ns()
        data["source_exit_status"] = source.returncode
        data["source_log_sha256"] = digest(root / "source.log")
        child, trace = start_b(run, follower, data["ports"]["relay"], OFFLINE_LABEL, data)
        state = wait_state(run, relay, "B recovered published cursor with source stopped", unavailable)
        data["offline_restart"] = verify_state(run, state, after, 0, reads, after_raw, "offline_restart")
        events = finish_b(child, trace, data[OFFLINE_LABEL], after, data["native_library"])
        require(events, [], "offline restart performed no install")
        check_relay(relay.requests, relay.errors)
        data["passed"] = True
    print(f"Passed DB {DB} truncated delta recovery ({AFTER} -> {LAST}), evidence {root / 'result.json'}", flush=True)


if __name__ == "__main__":
    main()
