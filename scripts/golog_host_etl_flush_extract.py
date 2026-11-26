import argparse
from collections import Counter, defaultdict
import gzip
import json
from pathlib import Path
import re

PATTERN = re.compile(rb'"(Irp|OriginalIrp)":"([^"]+)"')
COMMAND = re.compile(rb'"Command":"0x(?:35|91)"', re.IGNORECASE)
SELECTED = {b'"id":201,' , b'"id":202,', b'"id":203,', b'"id":208,', b'"id":220,', b'"id":258,'}


def main(source, directory, disk):
    direct = defaultdict(list)
    last_init = {}
    sample = {}
    stats = Counter()
    schemas = []
    first = {"header", "summary"}
    metadata = {}
    with source.open("rb") as stream, gzip.open(directory / "disk-flush.jsonl.gz", "wt", compresslevel=3, encoding="utf-8") as output:
        for line in stream:
            if b'"type":"schema"' in line[:24]:
                schemas.append(json.loads(line))
                continue
            if b'"provider":"classic_disk"' not in line[:64]:
                if any(f'"type":"{kind}"'.encode() in line[:24] for kind in first):
                    row = json.loads(line)
                    metadata[row["type"]] = row
                continue
            row = json.loads(line)
            fields = row["fields"]
            if row["opcode"] == 15:
                last_init[fields["Irp"]] = row
                continue
            if row["opcode"] != 14:
                continue
            stats["disk_" + fields["DiskNumber"]] += 1
            start = last_init.pop(fields["Irp"], None)
            if int(fields["DiskNumber"]) != disk:
                continue
            row["init"] = start
            row["response_qpc_ticks_reported"] = int(fields["HighResResponseTime"])
            if start is not None and start["qpc"] <= row["qpc"]:
                row["init_to_complete_qpc_ticks"] = row["qpc"] - start["qpc"]
                stats["target_has_init"] += 1
            direct[fields["Irp"].encode()].append(row["qpc"])
            output.write(json.dumps(row, separators=(",", ":")) + "\n")
            if "disk" not in sample or row["response_qpc_ticks_reported"] > sample["disk"]["response_qpc_ticks_reported"]:
                sample["disk"] = row

    with source.open("rb") as stream, gzip.open(directory / "storport-flush.jsonl.gz", "wt", compresslevel=3, encoding="utf-8") as output:
        for line in stream:
            prefix = line[:85]
            if b'"provider":"storport"' not in prefix or not any(tag in prefix for tag in SELECTED):
                continue
            ids = [value for _, value in PATTERN.findall(line)]
            direct_ids = [value for value in ids if value in direct]
            is_flush = bool(COMMAND.search(line))
            if not direct_ids and not is_flush:
                continue
            row = json.loads(line)
            linked = []
            for identity in set(direct_ids):
                # Keep candidate links rather than claiming identity across IRP reuse.
                linked.extend([identity.decode(), done] for done in direct[identity]
                              if abs(done - row["qpc"]) <= metadata["header"]["perf_frequency"])
            if not linked and not is_flush:
                continue
            row["disk_flush_candidates_irp_qpc_within_1s"] = sorted(linked)
            stats["storport_event_" + str(row["id"])] += 1
            if is_flush:
                names = row["fields"]
                stats["storport_scsi_flush_" + names["Command"] + "_event_" + str(row["id"])] += 1
                sample.setdefault("storport_scsi_" + str(row["id"]), row)
            output.write(json.dumps(row, separators=(",", ":")) + "\n")

    metadata.update(fields=schemas, selected_counts=dict(stats), examples=sample,
                    coordinate="native Windows raw QPC ticks; host perf_counter_ns = floor(qpc * 1e9 / frequency)")
    (directory / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(stats, sort_keys=True))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("source", type=Path)
    parser.add_argument("directory", type=Path)
    parser.add_argument("--disk", type=int, default=1)
    args = parser.parse_args()
    main(args.source, args.directory, args.disk)
