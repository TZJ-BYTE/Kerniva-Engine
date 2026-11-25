"""Select one DB/request's install boundaries and sum nonoverlapping spans."""

from golog_replica_snapshot_capacity import require


BOUNDARIES = ("pull_request", "manifest_validated", "current_durable", "view_published")
INSTALL_STAGES = (
    "generation_prepare", "segment_transfer", "response_end", "view_prepare", "generation_dir_sync",
    "database_dir_sync", "pin_source", "current_temp_prepare", "current_rename", "current_final_dir_sync",
)
SEGMENT_STAGES = ("segment_open", "segment_copy", "segment_sync", "segment_close")
TEMP_STAGES = ("current_temp_write", "current_temp_sync", "current_temp_close")
PARENTS = {
    "link_previous": "generation_prepare",
    **dict.fromkeys(SEGMENT_STAGES, "segment_transfer"),
    "delta_recovery": "view_prepare",
    "delta_go_restore": "delta_recovery",
    **dict.fromkeys(TEMP_STAGES, "current_temp_prepare"),
}


def install_events(rows, db, manifest, mode):
    require((rows[-1]["stage"], rows[-1]["dropped"]), ("trace_closed", 0), "trace complete")
    require(rows[-1].get("detail", "coarse"), mode, "tracing mode")
    groups = {}
    request = 0
    for row in rows:
        if row["db"] != db:
            continue
        stage = row["stage"]
        require(stage != "poll_error", True, f"DB {db} poll completes: {row.get('error')}")
        if stage == "pull_request":
            request += 1
            require(row.get("request_id", request), request, "database-local consecutive request IDs")
        require(request > 0, True, "database trace starts with a request")
        require(row.get("request_id", request), request, "event belongs to the active request on DB")
        groups.setdefault(request, []).append(row)

    matches = []
    for group in groups.values():
        selected = [r for r in group if r["stage"] in BOUNDARIES]
        if any(r["stage"] == "view_published" and r["last"] == manifest["last"] for r in selected):
            matches.append((selected, group))
    require(len(matches), 1, "one request installed the full delta")
    selected, group = matches[0]
    require(tuple(row["stage"] for row in selected), BOUNDARIES, "one request's ordered install boundaries")
    request, checked, durable, view = selected
    require((request["after"], checked["after"], checked["last"], checked["files"], checked["snapshot"],
             checked["rebase"], durable["last"], view["after"], view["last"]),
            (manifest["after"], manifest["after"], manifest["last"], len(manifest["files"]), 0, False,
             manifest["last"], manifest["after"], manifest["last"]), "observed complete delta")
    stamps = [row["monotonic_ns"] for row in selected]
    require(stamps, sorted(stamps), "monotonic install boundaries")
    return selected, group


def span_duration(row):
    if "count" in row:
        return row.get("duration_ns", 0)
    return row["monotonic_ns"] - row["start_ns"]


def disjoint(spans, start, end, name):
    cursor = start
    for row in sorted(spans, key=lambda r: r["start_ns"]):
        begin, finish = row["start_ns"], row["monotonic_ns"]
        require(cursor <= begin <= finish <= end, True, f"ordered, contained timing in {name}")
        cursor = finish


def phase_summary(events, rows, files, mode):
    requested, checked, durable, view = (row["monotonic_ns"] for row in events)
    if mode == "coarse":
        return {"stages": {}, "manifest_to_durable_ns": durable - checked,
                "manifest_to_durable_measured_ns": 0, "manifest_to_durable_unattributed_ns": durable - checked,
                "request_to_view_ns": view - requested, "request_to_view_measured_ns": 0,
                "request_to_view_unattributed_ns": view - requested}

    grouped = {}
    for row in rows:
        if "start_ns" in row or "count" in row:
            grouped.setdefault(row["stage"], []).append(row)
    required = (*INSTALL_STAGES, "view_publish", "delta_recovery")
    if mode == "fine":
        required += tuple(PARENTS)
    require(set(grouped), set(required), "expected timed stages")
    for stage in set(required) - set(SEGMENT_STAGES):
        require(len(grouped[stage]), 1, f"one measurement for {stage}")
    if mode == "fine":
        for stage in SEGMENT_STAGES:
            expected = {name: 0 if stage == "segment_open" else file["bytes"] for name, file in files.items()}
            spans = grouped[stage]
            require(len(spans), len(expected), f"{stage} measures each segment")
            require({row["file"]: row.get("bytes", 0) for row in spans}, expected, f"{stage} segment bytes")

    install = [grouped[stage][0] for stage in INSTALL_STAGES]
    published = grouped["view_publish"]
    disjoint(install, checked, durable, "manifest to durability")
    disjoint(published, durable, view, "durability to view publication")
    stages = {}
    for stage, spans in grouped.items():
        parent = PARENTS.get(stage)
        stages[stage] = {
            "count": sum(row.get("count", 1) for row in spans),
            "bytes": sum(row.get("bytes", 0) for row in spans),
            "total_ns": sum(map(span_duration, spans)),
            "max_ns": max(row.get("max_ns", span_duration(row)) for row in spans),
            "parent": parent,
        }
    for parent in set(PARENTS.values()) & grouped.keys():
        container = grouped[parent][0]
        children = [row for stage, spans in grouped.items() if PARENTS.get(stage) == parent for row in spans]
        disjoint([row for row in children if "start_ns" in row], container["start_ns"],
                 container["monotonic_ns"], parent)
        stages[parent]["unattributed_ns"] = stages[parent]["total_ns"] - sum(map(span_duration, children))
        require(stages[parent]["unattributed_ns"] >= 0, True, f"nonnegative {parent} remainder")

    measured = sum(map(span_duration, install))
    measured_to_view = checked - requested + measured + span_duration(published[0])
    return {"stages": stages, "manifest_to_durable_ns": durable - checked,
            "manifest_to_durable_measured_ns": measured, "manifest_to_durable_unattributed_ns": durable - checked - measured,
            "request_to_view_ns": view - requested, "request_to_view_measured_ns": measured_to_view,
            "request_to_view_unattributed_ns": view - requested - measured_to_view}
