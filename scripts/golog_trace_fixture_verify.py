"""Verify the isolated one-connection RESP fixture from captured bytes and timings."""
import argparse
import hashlib
import json
from collections import Counter
from pathlib import Path
from struct import pack

MASK = (1 << 64) - 1
MS = 1_000_000
SLOW = 100 * MS
TARGET = 50


def check(condition, message):
    if not condition:
        raise ValueError(message)


def mix(number):
    number = (number + 0x9e3779b97f4a7c15) & MASK
    number = ((number ^ (number >> 30)) * 0xbf58476d1ce4e5b9) & MASK
    number = ((number ^ (number >> 27)) * 0x94d049bb133111eb) & MASK
    return number ^ (number >> 31)


def rows(path):
    return [json.loads(line) for line in path.read_bytes().splitlines()]


def spans(row):
    p, q, s, c = [row[f] for f in ("planned_ns", "dequeued_ns", "send_start_ns", "completed_ns")]
    check(p <= q <= s <= c, "timestamps out of order")
    return {"queue_ns": q-p, "prepare_ns": s-q, "exchange_ns": c-s, "total_ns": c-p}


def bucket(ns):
    if ns < 512:
        return ns
    shift = ns.bit_length()-9
    return 512+(shift-1)*256+(ns >> shift)-256


def verify(root, name):
    folder = root/name
    client = json.loads((folder/"client.json").read_bytes())
    hist = json.loads((folder/"client-hist.json").read_bytes())
    server = rows(folder/"server-events.jsonl")
    meta, *kept, end = rows(folder/"client-trace.jsonl")
    delayed = name != "control"
    check(meta["type"] == "meta" and end["type"] == "summary" and meta["started_utc"] == client["started_utc"], "trace epoch")
    check((client["clients"], client["keys"], client["target_rate"], client["read_percent"], client["seed"],
           client["offered"], client["sent"], client["succeeded"], client["failures"]) ==
          (1,1024,100,0,42,300,300,300,0), "fixture configuration and denominator")
    check(meta["stride"] == 16 and meta["slow_threshold_ns"] == SLOW and end["offered"] == 300, "trace configuration")
    check(len(server) == 300 and len({r["client_peer_port"] for r in server}) == 1, "server connection")
    last = 0
    for i,r in enumerate(server):
        key = mix(mix(i ^ 42)) % 1024
        payload = pack("<QQQQ", key, *(mix(key ^ 42 ^ k) for k in (8,16,24)))
        check(r["server_zero_based_index"] == i and r["connection"] == 0 and r["command"] == "SET" and
              r["key"] == f"kerniva:thesis:{key}" and bytes.fromhex(r["value_hex"]) == payload and
              r["valid"] and r["reply"] == "+OK", f"wire data #{i}")
        check(last <= r["request_received_ns"] <= r["reply_send_start_ns"] <= r["reply_send_end_ns"], "server order")
        check(r["injected_delay_ns"] == (250*MS if i == TARGET and delayed else 0), "unexpected injection")
        last = r["reply_send_end_ns"]
    uniform_ids = {i for i in range(300) if mix(i ^ 42 ^ 0xd81b54a32d192ed3) % 16 == 0}
    observed = {r["index"]:r for r in kept}
    check(len(kept) == len(observed) == end["kept"] == end["candidates"] and end["drops"] == 0 and
          uniform_ids <= observed.keys(), "trace loss or duplicate IDs")
    exact = {}
    for i,r in observed.items():
        check(0 <= i < 300 and r["op"] == "SET" and r["worker"] == 0 and r["outcome"] == "success" and
              r["planned_ns"] == i*10*MS and r["uniform"] == (i in uniform_ids), "trace identity")
        timing = spans(r)
        if timing["total_ns"] >= SLOW:
            exact[i] = timing
        else:
            check(i in uniform_ids, "short row outside fixed sample")
    by_second = end["by_worker_second_get_set"][0]
    check(len(by_second) == 3, "planned second coverage")
    for sec,(get,sets) in enumerate(by_second):
        ids = range(100*sec,100*(sec+1))
        n_slow, n_uniform = sum(i in exact for i in ids), sum(i in uniform_ids for i in ids)
        check(get["offered"] == 0 and sets["offered"] == client["windows"][sec]["set"]["offered"] == 100 and
              sets["slow"] == sets["kept_slow"] == n_slow and sets["uniform"] == sets["kept_uniform"] == n_uniform and
              sets["kept"] == sum(i in observed for i in ids) and sets["drops"] == 0, "coverage reconciliation")
    sparse = {b["index"]:b["count"] for b in hist["set"]["success"]["bins"]}
    slow_bins = Counter(bucket(t["total_ns"]) for t in exact.values())
    check(sum(sparse.values()) == 300 and all(slow_bins[k] == v for k,v in sparse.items() if k > bucket(SLOW)) and
          all(v <= sparse.get(k,0) for k,v in slow_bins.items()), "slow traces and success histogram disagree")
    head = exact.get(TARGET)
    next_req = exact.get(TARGET+1)
    server_wait = server[TARGET]["reply_send_start_ns"]-server[TARGET]["request_received_ns"]
    if delayed:
        check(head and next_req and head["exchange_ns"] > 200*MS and head["queue_ns"] < 50*MS and
              next_req["queue_ns"] >= SLOW and next_req["exchange_ns"] < 50*MS and server_wait >= 200*MS,
              "known delayed reply and queued successor were not distinguished")
        check(observed[TARGET]["send_start_ns"] <= observed[TARGET+1]["planned_ns"] <
              observed[TARGET]["completed_ns"] <= observed[TARGET+1]["dequeued_ns"], "predecessor chronology")
    else:
        check(not exact, "control contained natural >=100ms requests")
    inputs = {f:hashlib.sha256((folder/f).read_bytes()).hexdigest() for f in
              ("client.json","client-hist.json","client-trace.jsonl","server-events.jsonl")}
    return {"attempt":name,"success":300,"retained":len(kept),"uniform":len(uniform_ids),
            "exact_slow_count":len(exact),"exact_slow_ids":sorted(exact), "target_index":TARGET,
            "target":head,"successor":next_req,"server_target_hold_ns":server_wait,
            "input_sha256":inputs,"result":"pass"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    result = {"schema_version":1, "population":"isolated RESP fixture",
              "attempts":[verify(args.root, name) for name in ("control","injected-1","injected-2")]}
    args.out.parent.mkdir(parents=True,exist_ok=True)
    args.out.write_bytes((json.dumps(result,indent=2,sort_keys=True)+"\n").encode())


if __name__ == "__main__":
    main()
