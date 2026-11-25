"""Provide three temporary loopback ACL Redis primaries to a workbench test over stdin/stdout."""

import argparse
import binascii
from contextlib import ExitStack
import fcntl
import json
import os
from pathlib import Path
import secrets
import select
import socket
import subprocess
import sys
import tempfile
import time


def start(args):
    with ExitStack() as stack:
        lock = stack.enter_context(open(args.lock, "a"))
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        root = Path(stack.enter_context(tempfile.TemporaryDirectory(prefix="kerniva-workbench-cluster-")))
        if args.cutoff and time.time() + 130 > args.cutoff:
            raise ValueError("Not enough time before cutoff")
        admin, password = secrets.token_urlsafe(24), secrets.token_urlsafe(24)
        env = {**os.environ, "REDISCLI_AUTH": admin}
        ports = []
        with ExitStack() as reservations:
            while len(ports) < 3:
                port = 21000 + secrets.randbelow(7000)
                try:
                    for n in [port, port + 10000]:
                        s = reservations.enter_context(socket.socket())
                        s.bind(("127.0.0.1", n))
                except OSError:
                    continue
                ports.append(port)
        children = []
        def stop():
            for child in children:
                if child.poll() is None:
                    child.terminate()
            for child in children:
                try:
                    child.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    child.kill()
                    child.wait()
            print(json.dumps({"exits": [c.returncode for c in children]}), flush=True)
        stack.callback(stop)
        for port in ports:
            d = root / str(port)
            d.mkdir(mode=0o700)
            cfg = d / "redis.conf"
            cfg.write_text(f'bind 127.0.0.1\nport {port}\ncluster-enabled yes\ncluster-announce-ip 127.0.0.1\n'
                           'cluster-config-file nodes.conf\nsave ""\nappendonly no\n'
                           f'user default on >{admin} ~* &* +@all\n'
                           f'user viewer on >{password} ~tui:* +@read +ping +info +command +cluster|slots\n'
                           f'user exact on >{password} ~tui:* +@read -scan +ping +info +command +cluster|slots\n')
            cfg.chmod(0o600)
            log = stack.enter_context((d / "server.log").open("wb"))
            children.append(subprocess.Popen(["redis-server", str(cfg)], cwd=d, stdout=log, stderr=log))
        def cli(port, *argv):
            return subprocess.check_output(["redis-cli", "--raw", "-p", str(port), *argv], env=env, text=True,
                                           stderr=subprocess.DEVNULL, timeout=20).strip()
        for port in ports:
            for _ in range(50):
                try:
                    if cli(port, "PING") == "PONG":
                        break
                except subprocess.CalledProcessError:
                    pass
                time.sleep(.08)
            else:
                raise RuntimeError(f"Node did not open {port}")
        creation = cli(ports[0], "--cluster", "create", *[f"127.0.0.1:{p}" for p in ports],
                       "--cluster-replicas", "0", "--cluster-yes")
        if "[OK] All 16384 slots covered" not in creation:
            raise RuntimeError("Incomplete Redis Cluster")
        for _ in range(50):
            if all("cluster_state:ok" in cli(p, "CLUSTER", "INFO") for p in ports):
                break
            time.sleep(.1)
        mapping = {}
        for line in cli(ports[0], "CLUSTER", "NODES").splitlines():
            node, address, *rest = line.split()
            low, high = map(int, rest[-1].split("-"))
            port = int(address.split("@")[0].rsplit(":", 1)[1])
            for i in range(10000):
                key = f"tui:{i}"
                if low <= binascii.crc_hqx(key.encode(), 0) % 16384 <= high:
                    break
            value = f"verified-{port}"
            assert cli(port, "SET", key, value) == "OK"
            mapping[node] = {"port": port, "key": key, "value": value, "slots": [low, high]}
        print(json.dumps({"version": cli(ports[0], "INFO", "server").splitlines()[1], "nodes": mapping,
                          "seed": ports[0], "password": password}), flush=True)
        select.select([sys.stdin], [], [], min(100, max(0, args.cutoff-time.time())) if args.cutoff else 100)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--lock", required=True)
    parser.add_argument("--cutoff", type=float)
    start(parser.parse_args())
