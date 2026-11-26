#!/usr/bin/env python3
"""Validate the frozen client tracer with three bounded loopback RESP fixtures."""

import argparse
from contextlib import closing
from datetime import datetime, timezone
import fcntl
from hashlib import file_digest
import json
from pathlib import Path
import re
import socket
import struct
import subprocess
import sys
import time

RATE = 100
SECONDS = 3
SEED = 42
KEYS = 1024
STRIDE = 16
TARGET = 50
INJECTION_NS = 250_000_000
SLOW_NS = 100_000_000
TOTAL = RATE * SECONDS
MASK = (1 << 64) - 1


def digest(path):
    with path.open("rb") as file:
        return file_digest(file, "sha256").hexdigest()


def now():
    return datetime.now(timezone.utc).isoformat()


def write(path, record):
    draft = path.with_suffix(path.suffix + ".tmp")
    with draft.open("x", encoding="utf-8") as file:
        json.dump(record, file, indent=2)
        file.write("\n")
    draft.replace(path)


def require(condition, message):
    if not condition:
        raise ValueError(message)


def mix(value):
    value = (value + 0x9e3779b97f4a7c15) & MASK
    value = ((value ^ (value >> 30)) * 0xbf58476d1ce4e5b9) & MASK
    value = ((value ^ (value >> 27)) * 0x94d049bb133111eb) & MASK
    return value ^ (value >> 31)


def expected(index):
    key = mix(mix(index ^ SEED)) % KEYS
    val = struct.pack("<4Q", key, *(mix(key ^ SEED ^ offset) for offset in (8, 16, 24)))
    return f"kerniva:thesis:{key}".encode(), val


def read_request(stream):
    header = stream.readline(8)
    if not header:
        return None
    require(header == b"*3\r\n", f"unexpected RESP command header {header!r}")
    result = []
    for _ in range(3):
        line = stream.readline(8)
        require(re.fullmatch(rb"\$[0-9]{1,2}\r\n", line), f"unexpected RESP bulk header {line!r}")
        length = int(line[1:-2])
        value = stream.read(length + 2)
        require(len(value) == length + 2 and value.endswith(b"\r\n"), "partial or malformed RESP bulk")
        result.append(value[:-2])
    return result


def new_listener(exclude):
    while True:
        listener = socket.socket()
        listener.bind(("127.0.0.1", 0))
        if listener.getsockname()[1] not in exclude:
            return listener
        listener.close()


def serve(root, injected, exclude):
    with closing(new_listener(exclude)) as listener, (root/"server-events.jsonl").open("x", encoding="utf-8", buffering=1) as log:
        port = listener.getsockname()[1]
        listener.settimeout(12)
        listener.listen(1)
        write(root/"server-ready.json", {"port":port,"started_at":now(),"monotonic_ns":time.monotonic_ns()})
        connection, address = listener.accept()
        with connection, connection.makefile("rb") as stream:
            connection.settimeout(8)
            for index in range(TOTAL):
                request = read_request(stream)
                received = time.monotonic_ns()
                require(request is not None, f"immediate EOF at request {index}")
                command, key, value = request
                want_key, want_value = expected(index)
                valid = command == b"SET" and key == want_key and value == want_value
                delay = INJECTION_NS if injected and index == TARGET else 0
                if delay:
                    time.sleep(delay / 1e9)
                before_send = time.monotonic_ns()
                reply = b"+OK\r\n" if valid else b"-ERR fixture received wrong value\r\n"
                connection.sendall(reply)
                end_send = time.monotonic_ns()
                record = {"server_zero_based_index":index,"connection":0,"client_peer_port":address[1],
                          "request_received_ns":received,"reply_send_start_ns":before_send,"reply_send_end_ns":end_send,
                          "injected_delay_ns":delay,"valid":valid,"command":command.decode("ascii","replace"),
                          "key":key.decode("ascii","replace"),"value_hex":value.hex(),"reply":reply.decode().strip()}
                log.write(json.dumps(record,sort_keys=True)+"\n")
                require(valid, f"incorrect command, key or value at request {index}")
            require(read_request(stream) is None, "unexpected command beyond fixed request count")
    return 0


def read_rows(path):
    return [json.loads(line) for line in path.read_bytes().splitlines()]


def split(row):
    plan, dequeued, send, complete = (row[k] for k in ("planned_ns","dequeued_ns","send_start_ns","completed_ns"))
    require(0 <= plan <= dequeued <= send <= complete, f"non-monotone trace index {row['index']}")
    queue, prepare, exchange = dequeued - plan, send - dequeued, complete - send
    require(queue + prepare + exchange == complete - plan, "timing does not sum")
    return {"index":row["index"],"queue_ns":queue,"prepare_ns":prepare,
            "exchange_ns":exchange,"total_ns":complete-plan}


def analyze(root, injected):
    bench = json.loads((root/"client.json").read_bytes())
    histogram = json.loads((root/"client-hist.json").read_bytes())
    raw = read_rows(root/"client-trace.jsonl")
    meta, summary = raw[0],raw[-1]
    samples = raw[1:-1]
    require(meta["type"]=="meta" and summary["type"]=="summary", "trace is not sealed")
    require((bench["offered"],bench["sent"],bench["succeeded"],bench["failures"])==(TOTAL,TOTAL,TOTAL,0),
             "client did not complete all 300 operations successfully")
    require(bench["get"]["offered"]==0 and bench["set"]["succeeded"]==TOTAL, "wrong operation mix")
    require((meta["clients"],meta["rate"],meta["seed"],meta["stride"],meta["slow_threshold_ns"])==
            (1,RATE,SEED,STRIDE,SLOW_NS), "trace metadata mismatch")
    require(summary["offered"]==TOTAL and summary["candidates"]==summary["kept"]==len(samples) and summary["drops"]==0,
             "trace accounting differs or has drops")
    server = read_rows(root/"server-events.jsonl")
    require(len(server)==TOTAL and [row["server_zero_based_index"] for row in server]==list(range(TOTAL)), "server sequence mismatch")
    require({row["connection"] for row in server}=={0} and len({row["client_peer_port"] for row in server})==1,
            "unexpected connection reuse or second connection")
    require(all(row["valid"] and row["reply"]=="+OK" for row in server), "server input or response failure")
    require([row["server_zero_based_index"] for row in server if row["injected_delay_ns"]]==([TARGET] if injected else []),
            "an unexpected reply was deliberately delayed")
    require(all(r["request_received_ns"]<=r["reply_send_start_ns"]<=r["reply_send_end_ns"] for r in server),
            "server clock order incorrect")
    require(all(a["reply_send_end_ns"]<=b["request_received_ns"] for a,b in zip(server,server[1:])),
            "server saw overlapping commands")
    indices=[r["index"] for r in samples]
    require(indices==sorted(set(indices)) and all(0<=index<TOTAL for index in indices), "duplicate/invalid trace IDs")
    by_index={}
    for row in samples:
        index=row["index"]
        require((row["worker"],row["op"],row["outcome"],row["planned_ns"])==(0,"SET","success",index*(10**9//RATE)),
                f"trace does not match known worker/index/plan {index}")
        by_index[index]=split(row)
    wild={i for i in range(TOTAL) if mix(i ^ SEED ^ 0xd81b54a32d192ed3) % STRIDE==0}
    observed={r["index"] for r in samples if r["uniform"]}
    slow={i for i,row in by_index.items() if row["total_ns"]>=SLOW_NS}
    require(observed==wild and set(by_index)==wild|slow,"deterministic ordinary or all-slow selection differs")
    coverage=summary["by_worker_second_get_set"]
    require(len(coverage)==1 and len(coverage[0])==SECONDS,"coverage dimensions differ")
    for second,(get,st) in enumerate(coverage[0]):
        scoped=lambda indices: sum(i//RATE==second for i in indices)
        require(not any(get.values()) and st=={"offered":RATE,"uniform":scoped(wild),"slow":scoped(slow),
                    "kept_uniform":scoped(wild),"kept_slow":scoped(slow),"kept":scoped(wild|slow),"drops":0},
                f"complete raw per-second counters disagree at {second}")
    hist=histogram["set"]["success"]
    require(hist["count"]==TOTAL and sum(bin["count"] for bin in hist["bins"])==TOTAL,"histogram result count differs")
    selected={}
    if injected:
        known,next_row=by_index[TARGET],by_index[TARGET+1]
        require(known["queue_ns"]<50_000_000 and known["exchange_ns"]>200_000_000, "injected request timing failed")
        require(next_row["queue_ns"]>=SLOW_NS and next_row["exchange_ns"]<50_000_000, "immediate successor timing failed")
        wait=server[TARGET]["reply_send_start_ns"]-server[TARGET]["request_received_ns"]
        require(wait>=INJECTION_NS, "server did not wait 250ms or more")
        selected={"injected":known,"successor":next_row,"server_actual_hold_ns":wait,
                  "noninjected_server_longest_response_ns":max(row["reply_send_start_ns"]-row["request_received_ns"]
                                                              for row in server if not row["injected_delay_ns"])}
    else:
        require(not slow and hist["max_ns"]<SLOW_NS,"control reached the slow threshold")
    return {"offered":TOTAL,"bench_exit":0,"trace_first_and_last":"verified","histogram_max_ns":hist["max_ns"],
            "resampled_ordinary_ids":sorted(wild),"slow_ids":sorted(slow),"slow_parts":[by_index[i] for i in sorted(slow)],
            "kept_drops":summary["drops"],"both_attempts_boundary_check":selected,
            "server_clock":"Linux monotonic_ns; server read boundary is full parsed RESP request",
            "client_clock":"offsets from that trial's Go monotonic offer epoch"}


def run_case(root, binary, injected, ports):
    root.mkdir()
    with (root/"server.stdout").open("xb") as stdout,(root/"server.stderr").open("xb") as stderr:
        argv=[sys.executable,"-B",str(Path(__file__).resolve()),"--serve",str(root)]
        if injected:
            argv.append("--inject")
        for port in ports:
            argv.extend(("--exclude",str(port)))
        server=subprocess.Popen(argv,stdin=subprocess.DEVNULL,stdout=stdout,stderr=stderr,start_new_session=True)
    result={"server_argv":argv,"server_pid":server.pid,"injected":injected,"started_at":now()}
    try:
        channel=root/"server-ready.json"
        cutoff=time.monotonic()+7
        while not channel.is_file() and server.poll() is None and time.monotonic()<cutoff:
            time.sleep(0.01)
        require(channel.is_file(),"fixture server did not listen")
        port=json.loads(channel.read_bytes())["port"]
        ports.add(port)
        client_args=[str(binary),"-mode","mixed","-network","gnet","-durability","sync","-addr",f"127.0.0.1:{port}",
                     "-clients","1","-keys",str(KEYS),"-seed",str(SEED),"-rate",str(RATE),"-read-percent","0",
                     "-duration",f"{SECONDS}s","-timeout","2s","-warmup","0s","-prefill=false",
                     "-histogram-out",str(root/"client-hist.json"),"-request-trace-out",str(root/"client-trace.jsonl"),
                     "-request-trace-stride",str(STRIDE)]
        result.update(port=port,client_argv=client_args)
        with (root/"client.json").open("xb") as stdout,(root/"client.stderr").open("xb") as stderr:
            with subprocess.Popen(client_args,stdin=subprocess.DEVNULL,stdout=stdout,stderr=stderr) as client:
                result["client_pid"]=client.pid
                try:
                    result["client_exit"]=client.wait(timeout=20)
                except subprocess.TimeoutExpired:
                    client.kill()
                    result["client_exit"]=client.wait()
                    raise
        result["server_exit"]=server.wait(timeout=10)
        require(result["client_exit"]==result["server_exit"]==0,"client or fixture process failed")
        result["verification"]=analyze(root,injected)
    except Exception as error:
        result["error"]=f"{type(error).__name__}: {error}"
    finally:
        if server.poll() is None:
            server.terminate()
            try:
                server.wait(timeout=2)
            except subprocess.TimeoutExpired:
                server.kill()
                server.wait()
        result["server_exit"]=server.returncode
        result["ended_at"]=now()
        result["retained_sha256"]={file.name:digest(file) for file in sorted(root.iterdir()) if file.is_file()}
        write(root/"attempt.json",result)
    return result


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--serve",type=Path)
    parser.add_argument("--inject",action="store_true")
    parser.add_argument("--exclude",type=int,action="append",default=[])
    parser.add_argument("--root",type=Path)
    parser.add_argument("--bench",type=Path)
    parser.add_argument("--bench-sha256")
    parser.add_argument("--lock",type=Path)
    parser.add_argument("--preregistration",type=Path)
    parser.add_argument("--preregistration-sha256")
    args=parser.parse_args()
    if args.serve is not None:
        return serve(args.serve,args.inject,set(args.exclude))
    if not all((args.root,args.bench,args.bench_sha256,args.lock,args.preregistration,args.preregistration_sha256)):
        parser.error("requires --root, --bench, --bench-sha256, --lock, --preregistration and --preregistration-sha256")
    root=args.root.resolve()
    binary=args.bench.resolve(strict=True)
    require(digest(args.preregistration)==args.preregistration_sha256,"fixture preregistration changed")
    registration=json.loads(args.preregistration.read_bytes())
    require(registration["bench_sha256"]==args.bench_sha256 and
            registration["decision"]=="GO_ISOLATED_KNOWN_RESPONSE_DELAY_FIXTURE_THREE_RUNS",
            "signed approval does not match fixture")
    require(datetime.now(timezone.utc)<datetime.fromisoformat(registration["deadline"]),"fixture deadline passed")
    require(digest(binary)==args.bench_sha256,"wrong benchmark binary")
    filesystem=subprocess.check_output(["findmnt","-n","-o","FSTYPE","-T",str(root.parent)],text=True).strip()
    require(filesystem=="ext4","fixture evidence requires ext4")
    result={"start":now(),"bench":str(binary),"bench_sha256":digest(binary),"script_sha256":digest(Path(__file__)),
            "plan":["control","injected-1","injected-2"],"fixed_request_index":TARGET,"injection_ns":INJECTION_NS,
            "preregistration_sha256":args.preregistration_sha256,
            "launch_constants":{"connections":1,"duration_s":SECONDS,"rate":RATE,"seed":SEED,"keys":KEYS,
                                "stride":STRIDE,"read_percent":0,"warmup_s":0,"prefill":False},
            "boundary":"successful correct +OK with one deliberate server delay after receiving an entire SET",
            "attempts":[]}
    with args.lock.open("r+b") as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        root.mkdir()
        ports=set()
        write(root/"run.json",result)
        for number,name in enumerate(result["plan"]):
            attempt=run_case(root/name,binary,number!=0,ports)
            result["attempts"].append({"name":name,"receipt_sha256":digest(root/name/"attempt.json"),
                                       "pass":"verification" in attempt,"error":attempt.get("error")})
            write(root/"run.json",result)
        result["passed"]=all(attempt["pass"] for attempt in result["attempts"])
        result["ended_at"]=now()
        write(root/"run.json",result)
    print(json.dumps(result,indent=2))
    return 0 if result["passed"] else 2


if __name__=="__main__":
    sys.exit(main())
