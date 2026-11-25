"""Witness exact Go-log SET frames, concurrent replica visibility, and sampled per-DB lag."""

import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from hashlib import sha256
from http.client import HTTPConnection
import importlib.util
import json
from pathlib import Path
import re
import socket
import struct
from threading import Event, Lock
import time
from urllib.parse import urlencode
import zlib


_spec = importlib.util.spec_from_file_location("live_replica_check", Path(__file__).with_name("golog-replica-check.py"))
check = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(check)
now = time.monotonic_ns
SEGMENT = re.compile(r"log-(\d{20})-(\d{20})\.seg")
POLL = 0.025
SAMPLE = 0.25


def need(condition, message):
    if not condition:
        raise ValueError(message)


def witness(call, *args):
    start = now()
    value = call(*args)
    return {"start_ns": start, "end_ns": now(), "value": value}


def list_segments(folder):
    return {p.name: (int(m[1]), int(m[2])) for p in folder.glob("log-*.seg") if (m := SEGMENT.fullmatch(p.name))}


def frontier(folder):
    files = list_segments(folder)
    try:
        cutoff = int((folder / "CURRENT").read_bytes())
    except FileNotFoundError:
        cutoff = 0
    return {"sealed_or_snapshot": max([cutoff, *(r[1] for r in files.values())]), "snapshot": cutoff}


def mark_sequence(data, filename, key, value):
    """Read actual V3 string PUT records and validate all frame checksums and sequence numbers."""
    bounds = SEGMENT.fullmatch(filename)
    need(bounds and data[:8] == b"KGLLOG01", f"invalid sealed segment {filename}")
    cursor, sequence, found = 8, int(bounds[1]), []
    while cursor < len(data):
        size, crc = struct.unpack_from("<II", data, cursor)
        frame = memoryview(data)[cursor + 8:cursor + 8 + size]
        need(len(frame) == size and zlib.crc32(frame) == crc, f"frame CRC or length {filename}/{sequence}")
        lsn, count = struct.unpack_from("<QI", frame)
        need(lsn == sequence and 0 < count <= (size - 12) // 9, f"frame sequence or count {filename}/{sequence}")
        pos = 12
        for _ in range(count):
            kind, ksize, vsize = struct.unpack_from("<BII", frame, pos)
            pos += 9
            need(kind in (1, 2, 3) and pos + ksize + vsize <= size, "invalid record")
            if frame[pos:pos + ksize] == key.encode():
                v = frame[pos + ksize:pos + ksize + vsize]
                expected = value.encode()
                need(kind == 1 and len(v) == 22 + len(expected) and v[0] == 3 and v[17] in (1, 2)
                     and v[1:9] == b"\0" * 8 and struct.unpack_from("<I", v, 18)[0] == len(expected)
                     and v[22:] == expected, f"unexpected marker value {filename}/{lsn}")
                found.append(lsn)
            pos += ksize + vsize
        need(pos == size, f"trailing record bytes {filename}/{lsn}")
        cursor += size + 8
        sequence += 1
    need(sequence - 1 == int(bounds[2]) and len(found) <= 1, f"segment range or duplicate marker {filename}")
    return found[0] if found else None


def primary(port, db, *words):
    with socket.create_connection(("127.0.0.1", port), timeout=2) as sock, check.RECOVERY.RESP(sock) as c:
        check.require(c.command("SELECT", str(db)), "OK", "marker SELECT")
        return c.command(*words)


def follower_get(port, db, key, value):
    code, body = check.http(port, "/get", db=db, key=key)
    need(code == 404 or (code == 200 and body == value.encode()), f"DB {db} follower returned wrong marker: {code} {body[:80]!r}")
    return {"status": code, "body": body.decode() if code == 200 else None}


def status(port, db, lineage):
    code, body = check.http(port, "/status", db=db)
    s = json.loads(body)
    need(code == 200 and s["ready"] and s["source"] == lineage and not s["last_error"], f"unready or diverged DB {db}: {s}")
    return s


def disk_cursor(root, peer, db, lineage):
    path = root / peer / "follower-v1" / f"db_{db}" / "CURRENT"
    cursor = json.loads(path.read_bytes())
    need(cursor["source"] == lineage and 0 <= cursor["snapshot"] <= cursor["sequence"] and cursor["generation"],
         f"invalid {peer}/DB {db} CURRENT")
    return cursor


def pull_segment(port, db, lineage, filename, local):
    after = int(SEGMENT.fullmatch(filename)[1]) - 1
    query = urlencode({"db": db, "after": after, "source": lineage, "defer_snapshot": 1})
    with closing(HTTPConnection("127.0.0.1", port, timeout=3)) as connection:
        connection.request("GET", "/pull?" + query)
        with connection.getresponse() as response:
            result = {"http_status": response.status, "request": query, "body_bytes_read": 0}
            if response.status == 204 and response.getheader("Kerniva-Snapshot-Required") == "1":
                return result
            need(response.status == 200, f"source returned {response.status} for DB {db} after {after}")
            h = response.read(4)
            need(len(h) == 4, "truncated manifest header")
            n = int.from_bytes(h, "big")
            need(0 < n <= 1 << 20, "invalid manifest size")
            raw = response.read(n)
            need(len(raw) == n, "truncated manifest")
            m = json.loads(raw)
            need(m["version"] == 1 and (m["db"], m["after"], m["source"]) == (db, after, lineage)
                 and m["last"] >= after, "source manifest identity or sequence")
            entries = m.get("files") or []
            result.update(manifest=m, body_bytes_read=4 + n,
                          advertised_body_bytes=4 + n + sum(item["size"] for item in entries))
            if not entries or entries[0]["name"] != filename:
                need(m.get("snapshot", 0) > after, f"available source omitted target segment {filename}")
                return result
            need(entries[0]["size"] == len(local), "source and local segment size differ")
            payload = response.read(len(local))
            need(payload == local, "source and local segment content differ")
            result.update(body_bytes_read=4+n+len(payload), segment_sha256=sha256(payload).hexdigest())
            return result


def derived(raw, root):
    rows, total_pulls, read_bytes = [], 0, 0
    targets = raw["followers"]
    origin = raw["start_ns"]
    disk_probes = raw["schema"] >= 2
    for mark in raw["markers"]:
        db, key, val = mark["db"], mark["key"], mark["value"]
        need(mark["before_primary"]["value"] is None and mark["set"]["value"] == "OK"
             and mark["final_primary"]["value"] == val, "primary marker changed")
        need(mark["set"]["start_ns"] >= origin + round(mark["offset"] * 1e9), "marker preceded scheduled offset")
        expected_key = f"live-observer:{mark['ordinal']}:db:{db}"
        need(key == expected_key and val == "value:" + expected_key, "unexpected marker plan")
        row = {"db": db, "ordinal": mark["ordinal"], "key": key, "sequence": None, "source_to_get_ms": {}}
        segment = mark.get("sealed")
        seq = None
        if segment:
            name = segment["name"]
            need(SEGMENT.fullmatch(name) and mark["set"]["start_ns"] <= segment["end_ns"], "invalid marked seal")
            data = (root / "live-observer-evidence" / f"db-{db}" / name).read_bytes()
            need(sha256(data).hexdigest() == segment["sha256"], "archived sealed segment digest changed")
            seq = mark_sequence(data, name, key, val)
            need(seq is not None and seq == segment["sequence"], "marker frame differs")
            row["sequence"] = seq
        pull = mark.get("pull")
        if pull:
            total_pulls += 1
            resp = pull["value"]
            read_bytes += resp["body_bytes_read"]
            if "segment_sha256" in resp:
                m = resp["manifest"]
                need(m["source"] == raw["lineage"] and m["db"] == db and
                     m["after"] == int(SEGMENT.fullmatch(segment["name"])[1])-1 and m["last"] >= seq and
                     m["files"][0] == {"name":segment["name"],"size":len(data)} and resp["segment_sha256"] == segment["sha256"] and
                     pull["start_ns"] >= segment["start_ns"] and pull["end_ns"] >= pull["start_ns"], "wrong source pull evidence")
                lo = max([mark["set"]["start_ns"], *(h["start_ns"] for h in mark["heads"] if h["value"]["sealed_or_snapshot"] < seq)])
                row["source_available_ns"] = [lo, pull["end_ns"]]
                need(lo <= pull["end_ns"], "inconsistent source availability")
        for peer in targets:
            obs = mark["replicas"][peer]
            verified = obs["first_positive"]
            prior = obs["last_negative"]
            need(prior["value"]["status"] == 404 and verified["value"] == {"status":200,"body":val}
                 and prior["start_ns"] <= prior["end_ns"] <= verified["start_ns"] <= verified["end_ns"]
                 and mark["final_replicas"][peer]["value"] == verified["value"], "incorrect final or transition GET")
            if disk_probes:
                polls = obs["samples"]
                need(len(polls) == obs["requests"] and len(polls) >= 2 and polls[-2:] == [prior,verified]
                     and all(i["value"]["status"] == 404 for i in polls[:-1])
                     and all(a["end_ns"] <= b["start_ns"] for a,b in zip(polls,polls[1:])), "inconsistent raw GET history")
            if "source_available_ns" in row:
                lo, hi = row["source_available_ns"]
                upper = verified["end_ns"] - lo
                need(upper >= 0, "GET preceded earliest possible source availability")
                row["source_to_get_ms"][peer] = [max(0, prior["start_ns"]-hi)/1e6, upper/1e6]
            if disk_probes and seq is not None:
                # A pathname read observes atomic replacement. It cannot date the directory fsync.
                samples = [mark["initial_disk"][peer], *(s["disk"][peer] for s in raw["samples"] if s["db"] == db)]
                samples.sort(key=lambda w: w["end_ns"])
                positive = next((w for w in samples if w["value"]["sequence"] >= seq),None)
                need(positive is not None, "final CURRENT behind marker")
                negatives = [w for w in samples if w["start_ns"] <= positive["end_ns"] and w["value"]["sequence"] < seq]
                negative = max(negatives,key=lambda w:w["start_ns"],default=None)
                bounds = [negative["start_ns"] if negative else None, positive["end_ns"]]
                row.setdefault("disk_current_visible_ns",{})[peer] = {"bounds":bounds,
                       "last_negative":negative,"first_positive":positive}
                if "source_available_ns" in row:
                    lo,hi = row["source_available_ns"]
                    need(bounds[1] >= lo, "CURRENT preceded earliest possible marker availability")
                    row.setdefault("source_to_disk_current_ms",{})[peer] = [
                        None if bounds[0] is None else max(0,bounds[0]-hi)/1e6, (bounds[1]-lo)/1e6]
        row["scope"] = ("exact-source-and-get" if "source_available_ns" in row else
                        "primary-seal" if not targets and seq is not None else "scope-limited")
        row["scope_reason"] = mark.get("limited")
        row["snapshot_publication_observation_overlap"] = []
        # The two cutoff observations bracket publication, not construction or the entire I/O interval.
        for span in raw["snapshot_intervals"]:
            if span["db"] != db:
                continue
            need(span["end_ns"] >= span["start_ns"] and span["cutoff"] > span["previous_cutoff"], "snapshot cutoff observation")
            end = max([mark["set"]["end_ns"], *(v["first_positive"]["end_ns"] for v in mark["replicas"].values())])
            if max(mark["set"]["start_ns"],span["start_ns"]) <= min(end,span["end_ns"]):
                row["snapshot_publication_observation_overlap"].append(span)
        rows.append(row)
    plan = [(i,db) for i in range(len(raw["offsets"])) for db in raw["databases"]]
    need([(m["ordinal"],m["db"]) for m in rows] == plan, "incomplete marker plan")
    lag, last = [], {}
    for stamp in raw["samples"]:
        db = stamp["db"]
        before = stamp["before"]["value"]["sealed_or_snapshot"]
        after = stamp["after"]["value"]["sealed_or_snapshot"]
        need(before <= after and set(stamp["states"]) == set(targets), "source frontier or replica roster changed")
        entries, disk_lag = {}, {}
        for peer, item in stamp["states"].items():
            s = item["value"]
            seq = s["sequence"]
            need(stamp["before"]["end_ns"] <= item["start_ns"] <= item["end_ns"] <= stamp["after"]["start_ns"]
                 and s["ready"] and s["source"] == raw["lineage"] and not s["last_error"]
                 and last.get((db,peer),0) <= seq <= after, f"{peer} DB{db} sequence or source diverged")
            last[db,peer] = seq
            entries[peer] = [max(0,before-seq), after-seq]
            if disk_probes:
                w = stamp["disk"][peer]
                cursor = w["value"]
                pos = cursor["sequence"]
                need(stamp["before"]["end_ns"] <= w["start_ns"] <= w["end_ns"] <= stamp["after"]["start_ns"] and
                     cursor["source"] == raw["lineage"] and 0 <= cursor["snapshot"] <= pos and
                     last.get((db,peer,"disk"),0) <= pos <= after, "on-disk CURRENT diverged")
                last[db,peer,"disk"] = pos
                disk_lag[peer] = [max(0,before-pos),after-pos]
        lag.append({"db":db,"start_ns":stamp["before"]["start_ns"],"end_ns":stamp["after"]["end_ns"],
                    "snapshot_before":stamp["before"]["value"]["snapshot"],"snapshot_after":stamp["after"]["value"]["snapshot"],
                    "sequence_lag_bounds":entries,**({"disk_sequence_lag_bounds":disk_lag} if disk_probes else {})})
    final = {s["db"]: s for s in raw["final"]}
    need(set(final) == set(raw["databases"]), "missing final DB status")
    for db,s in final.items():
        horizon = s["after"]["value"]["sealed_or_snapshot"]
        need(s["before"]["value"]["sealed_or_snapshot"] == horizon and all(
             v["value"]["sequence"] == horizon for v in s["states"].values()), "final sealed cursor not caught up")
        if disk_probes:
            need(set(s["disk"]) == set(targets) and all(v["value"]["sequence"] == horizon for v in s["disk"].values()),
                 "final on-disk cursor not caught up")
        need(all(r["sequence"] is None or r["sequence"] <= horizon for r in rows if r["db"]==db),"final cursor behind marker")
    return {"markers":rows,"sequence_lag_samples":lag,"final_sealed_sequences":{str(db):s["after"]["value"]["sealed_or_snapshot"] for db,s in final.items()},
            "source_pull_requests":total_pulls,"source_pull_response_body_bytes_read":read_bytes,
            "scope_limited_markers":sum(r["scope"]=="scope-limited" for r in rows)}


class LiveObserver:
    def __init__(self, root, storage, primary_port, followers, databases=(0,1), *,
                 source_port=None, offsets=(0.35,), snapshot_intervals=None, timeout=15):
        self.root, self.storage = Path(root), Path(storage)
        self.primary, self.source, self.peers = primary_port, source_port, dict(followers)
        self.dbs, self.offsets = tuple(databases), tuple(offsets)
        self.spans = snapshot_intervals if snapshot_intervals is not None else []
        self.timeout = timeout
        need(self.dbs and len(set(self.dbs))==len(self.dbs) and all(db >= 0 for db in self.dbs), "invalid DB roster")
        need(self.offsets and self.offsets == tuple(sorted(set(self.offsets))) and self.offsets[0] >= 0, "invalid offsets")
        need(not self.peers or self.source is not None, "replica observations require source_port")
        self.stop, self.cancel = Event(), Event()
        self.archive_lock = Lock()
        self.partial = {}
        self.pool = self.receipt = None
        self.files = self.root / "live-observer-evidence"

    def __enter__(self):
        return self

    def start(self):
        need(self.pool is None, "observer already started")
        self.files.mkdir()
        for db in self.dbs:
            (self.files / f"db-{db}").mkdir()
        self.lineage = (self.storage / ".replica-source-id").read_text() if self.source else None
        self.pool = ThreadPoolExecutor(max_workers=1+len(self.offsets)*len(self.dbs))
        self.anchor = now()
        self.entries = [self.pool.submit(self._marker,i,db,offset) for i,offset in enumerate(self.offsets) for db in self.dbs]
        self.sampling = self.pool.submit(self._sample_until_done)
        return self.anchor

    def _sample(self, db):
        folder = self.storage / f"db_{db}"
        before = witness(frontier,folder)
        disk = {k:witness(disk_cursor,self.root,k,db,self.lineage) for k in self.peers}
        states = {k:witness(status,p,db,self.lineage) for k,p in self.peers.items()}
        return {"db":db,"before":before,"disk":disk,"states":states,"after":witness(frontier,folder)}

    def _sample_until_done(self):
        samples = []
        while not self.stop.is_set():
            samples.extend(self._sample(db) for db in self.dbs)
            self.stop.wait(SAMPLE)
        return samples

    def _marker(self, ordinal, db, offset):
        folder = self.storage / f"db_{db}"
        key = f"live-observer:{ordinal}:db:{db}"
        val = "value:" + key
        if self.cancel.wait(max(0,(self.anchor+round(offset*1e9)-now())/1e9)):
            raise RuntimeError("observer cancelled")
        mark = {"db":db,"ordinal":ordinal,"offset":offset,"key":key,"value":val,"heads":[],"replicas":{},"initial_disk":{}}
        self.partial[ordinal,db] = mark
        mark["before_primary"] = witness(primary,self.primary,db,"GET",key)
        need(mark["before_primary"]["value"] is None,"marker already exists")
        for peer,port in self.peers.items():
            mark["initial_disk"][peer] = witness(disk_cursor,self.root,peer,db,self.lineage)
            sample = witness(follower_get,port,db,key,val)
            need(sample["value"]["status"]==404, "marker already in replica")
            mark["replicas"][peer] = {"last_negative":sample,"requests":1,"samples":[sample]}
        visited = set(list_segments(folder))
        prior_cutoff = frontier(folder)["snapshot"]
        mark["set"] = witness(primary,self.primary,db,"SET",key,val)
        need(mark["set"]["value"] == "OK","marker SET failed")
        deadline = time.monotonic()+self.timeout
        while not self.cancel.is_set():
            for peer,port in self.peers.items():
                obs = mark["replicas"][peer]
                if "first_positive" not in obs:
                    w = witness(follower_get,port,db,key,val)
                    obs["requests"] += 1
                    obs["samples"].append(w)
                    obs["first_positive" if w["value"]["status"]==200 else "last_negative"] = w
            if "sealed" not in mark:
                mark["heads"].append(witness(frontier,folder))
                for name in sorted(set(list_segments(folder))-visited):
                    visited.add(name)
                    begin=now()
                    try:
                        local=(folder/name).read_bytes()
                    except FileNotFoundError:
                        continue
                    if key.encode() not in local:
                        continue
                    lsn=mark_sequence(local,name,key,val)
                    if lsn is None:
                        continue
                    mark["sealed"]={"name":name,"sha256":sha256(local).hexdigest(),"sequence":lsn,"start_ns":begin,"end_ns":now()}
                    evidence=self.files/f"db-{db}"/name
                    # Several scheduled markers can occur in the same immutable sealed segment.
                    with self.archive_lock:
                        if not evidence.exists():
                            evidence.write_bytes(local)
                    if self.source:
                        mark["pull"]=witness(pull_segment,self.source,db,self.lineage,name,local)
                        if "segment_sha256" not in mark["pull"]["value"]:
                            mark["limited"]="source returned a snapshot requirement or a covering snapshot"
                    break
            complete = all("first_positive" in x for x in mark["replicas"].values())
            if complete and "sealed" in mark:
                return mark
            if time.monotonic() >= deadline:
                need(complete, f"timed out waiting for DB{db} follower GET: {mark['replicas']}")
                if "sealed" not in mark and mark["heads"][-1]["value"]["snapshot"] > prior_cutoff:
                    mark["limited"]="no surviving sealed segment containing this marker was observed before deadline"
                    return mark
                raise TimeoutError(f"timed out waiting for DB{db} marker seal")
            self.cancel.wait(POLL)
        raise RuntimeError("observer cancelled")

    def finish(self):
        if self.receipt is not None:
            return self.receipt
        need(self.pool is not None,"observer has not started")
        try:
            markers=[job.result() for job in self.entries]
            self.stop.set()
            samples=self.sampling.result()
            deadline=time.monotonic()+self.timeout
            while True:
                final=[self._sample(db) for db in self.dbs]
                samples.extend(final)
                if all(s["before"]["value"]["sealed_or_snapshot"]==s["after"]["value"]["sealed_or_snapshot"] and
                       all(v["value"]["sequence"]==s["after"]["value"]["sealed_or_snapshot"] for v in
                           (*s["states"].values(),*s["disk"].values())) for s in final):
                    break
                if time.monotonic()>=deadline:
                    raise TimeoutError(f"final sealed frontier did not catch up: {final}")
                time.sleep(POLL)
            for m in markers:
                m["final_primary"]=witness(primary,self.primary,m["db"],"GET",m["key"])
                m["final_replicas"]={k:witness(follower_get,p,m["db"],m["key"],m["value"]) for k,p in self.peers.items()}
            raw={"schema":2,"start_ns":self.anchor,"end_ns":now(),"databases":self.dbs,"offsets":self.offsets,
                 "followers":list(self.peers),"lineage":self.lineage,"snapshot_intervals":list(self.spans),
                 "markers":markers,"samples":samples,"final":final}
            path=self.files/"witnesses.json"
            check.RUNNER.write_json(path,raw)
            self.receipt={"schema":2,"passed":True,"evidence":"live-observer-evidence/witnesses.json",
                          "witness_sha256":sha256(path.read_bytes()).hexdigest(),**derived(raw,self.root)}
            check.RUNNER.write_json(self.root/"live-observer.json",self.receipt)
            return self.receipt
        except Exception as err:
            self.cancel.set()
            self.stop.set()
            self.pool.shutdown(wait=True)
            check.RUNNER.write_json(self.files/"incomplete.json",{"markers":list(self.partial.values())})
            check.RUNNER.write_json(self.root/"live-observer.json",
                                    {"schema":1,"passed":False,"error":str(err),"partial":"live-observer-evidence/incomplete.json"})
            raise
        finally:
            self.stop.set()
            self.cancel.set()
            self.pool.shutdown(wait=True)

    def __exit__(self, typ, *_):
        if self.pool:
            if typ is None and self.receipt is None:
                self.finish()
            self.stop.set()
            self.cancel.set()
            self.pool.shutdown(wait=True)


def verify_window(root):
    root=Path(root)
    receipt=json.loads((root/"live-observer.json").read_bytes())
    evidence=root/"live-observer-evidence/witnesses.json"
    data=evidence.read_bytes()
    need(receipt["passed"] and receipt["evidence"]==evidence.relative_to(root).as_posix()
         and receipt["witness_sha256"]==sha256(data).hexdigest(),"raw witness digest differs")
    raw=json.loads(data)
    need(receipt["schema"] == raw["schema"] and raw["schema"] in (1,2), "unexpected observation schema")
    recomputed=derived(raw,root)
    need(all(receipt[k]==v for k,v in recomputed.items()),"offline readback differs")
    return {"window":str(root),"receipt_sha256":sha256((root/"live-observer.json").read_bytes()).hexdigest(),
            "markers":len(recomputed["markers"]),"source_pulls":recomputed["source_pull_requests"],
            "scope_limited_markers":recomputed["scope_limited_markers"],"final_sealed_sequences":recomputed["final_sealed_sequences"]}


if __name__ == "__main__":
    cli=argparse.ArgumentParser(description=__doc__)
    cli.add_argument("--verify",required=True,type=Path)
    print(json.dumps(verify_window(cli.parse_args().verify),indent=2))
