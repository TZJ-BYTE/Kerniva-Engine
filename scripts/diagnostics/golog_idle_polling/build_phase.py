#!/usr/bin/env python3
"""Build ordinary or diagnostic replica CLIs from the same source on Linux ext4."""

import argparse
import json
from pathlib import Path
import subprocess

from run import FOLLOWER, HERE, copy_source, digest, go


TRACE = FOLLOWER.with_name("poll_phase_trace_linux.go")
TRACE_INPUT = HERE / "poll_phase_trace_linux.go.txt"
PROBES = (
    ("type replicaDB struct {\n",
     "type replicaDB struct {\n\tphaseTrace *pollPhaseTrace\n\tphaseRequest uint64\n"),
    ("type Follower struct {\n",
     "type Follower struct {\n\tphaseTrace *pollPhaseTrace\n"),
    (
        "\treturn f, nil\n}\n\nfunc (f *Follower) openDatabase",
        "\tf.phaseTrace, err = openPollPhaseTrace(f.byNumber)\n"
        "\tif err != nil {\n\t\tf.Close()\n\t\treturn nil, err\n\t}\n"
        "\treturn f, nil\n}\n\nfunc (f *Follower) openDatabase",
    ),
    (
        "\t\tf.active.Wait()\n\t\tf.trace.close()",
        "\t\tf.active.Wait()\n\t\tf.phaseTrace.close()\n\t\tf.trace.close()",
    ),
    (
        "\t\tnext.trace.record(\"read_view_switched\")",
        "\t\tdb.recordPollPhase(pollPhaseEvent{Stage: \"view_published\", "
        "After: previous.status.Sequence, Last: next.Sequence})\n"
        "\t\tnext.trace.record(\"read_view_switched\")",
    ),
    (
        "\treturn err\n}\n\nfunc (db *replicaDB) publishView",
        "\tif db.phaseTrace != nil {\n"
        "\t\tif err == nil && previous.view != nil && next == previous.view {\n"
        "\t\t\tdb.recordPollPhase(pollPhaseEvent{Stage: \"poll_no_change\", "
        "After: status.Sequence, Last: status.Sequence})\n"
        "\t\t} else if err != nil && ctx.Err() == nil {\n"
        "\t\t\tdb.recordPollPhase(pollPhaseEvent{Stage: \"poll_error\", "
        "After: status.Sequence, Error: err.Error()})\n"
        "\t\t}\n\t}\n"
        "\treturn err\n}\n\nfunc (db *replicaDB) publishView",
    ),
    (
        "\t\tnext.trace.record(\"current_published\")",
        "\t\tdb.recordPollPhase(pollPhaseEvent{Stage: \"current_durable\", Last: next.Sequence})\n"
        "\t\tnext.trace.record(\"current_published\")",
    ),
    (
        "\tresponse, err := f.client.Do(request)",
        "\tdb.beginPollPhase(after)\n"
        "\tresponse, err := f.client.Do(request)",
    ),
    (
        "\tif probeSnapshot && info.Snapshot != 0 {",
        "\tdb.recordPollPhase(pollPhaseEvent{Stage: \"manifest_validated\", After: info.After, "
        "Last: info.Last, Files: len(info.Files), Snapshot: info.Snapshot, Rebase: info.Rebase})\n"
        "\tif probeSnapshot && info.Snapshot != 0 {",
    ),
    (
        "\terr = syncDir(db.directory)\n\tif err == nil && observe != nil {",
        "\terr = syncDir(db.directory)\n\tif err == nil {\n"
        "\t\tdb.recordPollPhase(pollPhaseEvent{Stage: \"current_durable\", Last: meta.Sequence})\n"
        "\t}\n\tif err == nil && observe != nil {",
    ),
)


def apply_probes(code, probes):
    for anchor, replacement in probes:
        if code.count(anchor) != 1:
            raise ValueError(f"unrecognized follower source near {anchor!r}")
        code = code.replace(anchor, replacement)
    return code


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True, help="new build directory on ext4")
    parser.add_argument("--repo", type=Path, default=HERE.parents[2], help="Git repository to archive")
    parser.add_argument("--mode", choices=("ordinary", "coarse", "fine", "narrow"), default="coarse")
    sources = parser.add_mutually_exclusive_group(required=True)
    sources.add_argument("--revision", help="Git revision from --repo")
    sources.add_argument("--checkout", type=Path, help="candidate source tree to copy")
    args = parser.parse_args()
    args.root = args.root.resolve()
    if args.checkout:
        args.checkout = args.checkout.resolve()
        if args.root.is_relative_to(args.checkout):
            parser.error("--root must be outside --checkout")
    filesystem = subprocess.check_output(
        ["findmnt", "-n", "-o", "FSTYPE", "-T", args.root.parent], text=True).strip()
    if filesystem != "ext4":
        parser.error(f"expected an ext4 build directory, got {filesystem}")

    args.root.mkdir()
    source = args.root / "src"
    source_identity = copy_source(args, source)
    path = source / FOLLOWER
    original = digest(path)
    metadata = {}
    if args.mode != "ordinary":
        source_trace = TRACE_INPUT.read_text()
        code = apply_probes(path.read_text(), PROBES)
        if args.mode in ("fine", "narrow"):
            from install_probes import DETAIL_PROBES

            code = apply_probes(code, DETAIL_PROBES)
            source_trace = source_trace.replace('pollPhaseDefaultDetail = "coarse"',
                                                f'pollPhaseDefaultDetail = "{args.mode}"')
        path.write_text(code)
        (source / TRACE).write_text(source_trace)
        subprocess.run(["gofmt", "-w", FOLLOWER, TRACE], cwd=source, check=True)
        metadata.update(instrumented_follower_sha256=digest(path), probe_go_sha256=digest(source / TRACE))

    binary = args.root / "bin" / "kerniva-replica"
    binary.parent.mkdir()
    go("build", "-o", binary, "./cmd/kerniva-replica", cwd=source)
    (args.root / "build.json").write_text(json.dumps({
        "source": source_identity,
        "mode": args.mode,
        "go_version": subprocess.check_output(["go", "version"], text=True).strip(),
        "original_follower_sha256": original,
        "probe_builder_sha256": digest(Path(__file__)),
        "binary_sha256": digest(binary),
        **metadata,
    }, indent=2) + "\n")
    print(binary)


if __name__ == "__main__":
    main()
