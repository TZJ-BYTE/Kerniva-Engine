"""Exercise deployment through a real local workbench with a disposable Windows Go-log data directory."""

import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing, ExitStack
import json
import os
from pathlib import Path
import socket
import sqlite3
import subprocess
import sys
import tempfile
import time
import uuid

from verify import service

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))
from workbench_operations import JsonApi, Problem
import workbench_redis as redis


def expect_status(status, action, *args):
    try:
        action(*args)
    except Problem as error:
        if error.status == status:
            return str(error)
        raise
    raise AssertionError(f"expected HTTP {status}")


def check_startup(root):
    import winreg
    import workbench_instances as manager
    from types import SimpleNamespace
    instances = manager.Instances(SimpleNamespace(path=root / "test-config"))
    instances.desktop = root / "Kerniva Workbench.exe"
    instances.desktop.touch()
    temporary_key = "Software\\Kerniva-acceptance-" + str(uuid.uuid4())
    original = manager.LOGIN_KEY
    manager.LOGIN_KEY = temporary_key
    try:
        assert not instances.startup()["enabled"]
        assert instances.startup({"enabled": True})["enabled"]
        with winreg.OpenKey(winreg.HKEY_CURRENT_USER, temporary_key, 0, winreg.KEY_ALL_ACCESS) as key:
            assert winreg.QueryValueEx(key, manager.LOGIN_VALUE)[0] == '"' + str(instances.desktop) + '"'
            assert not instances.startup({"enabled": False})["enabled"]
            winreg.SetValueEx(key, manager.LOGIN_VALUE, 0, winreg.REG_SZ, '"C:\\foreign.exe"')
            assert instances.startup()["otherLocation"]
            expect_status(409, instances.startup, {"enabled": False})
            assert winreg.QueryValueEx(key, manager.LOGIN_VALUE)[0] == '"C:\\foreign.exe"'
    finally:
        manager.LOGIN_KEY = original
        winreg.DeleteKey(winreg.HKEY_CURRENT_USER, temporary_key)


def verify(source, bundle):
    if os.name != "nt":
        raise RuntimeError("Windows is required")
    with tempfile.TemporaryDirectory(prefix="Kerniva 单机 ") as temporary, ExitStack() as stack:
        root = Path(temporary)
        config = root / "workspace.json"
        if bundle:
            helper = bundle / "backend/kerniva-workbench-service.exe"
            arguments = ()
        else:
            helper = Path(sys.executable)
            arguments = ("-B", str(ROOT / "scripts/kerniva-workbench.py"), "--engine-bundle", str(source))
        def open_workbench(resources):
            return JsonApi(service(resources, root, helper, config, os.environ.copy(), arguments))
        def resp(command, *words):
            with redis._Client({"host": "127.0.0.1", "port": port, "db": 1}) as client:
                return client.at(client.seed, command, *words)

        ident = str(uuid.uuid4())
        key = {"id": ident}
        def ensure_stopped():
            with ExitStack() as rescue:
                api = open_workbench(rescue)
                if any(r["id"] == ident and r["pid"] for r in api.call("instances")["instances"]):
                    api.call("instances/stop", key, timeout=35)
        stack.callback(ensure_stopped)
        with ExitStack() as first:
            a = open_workbench(first)
            state = a.call("instances")
            assert state["version"] and not state["instances"]
            port = state["suggestedPort"]
            body = {**key, "name": "实例 A", "port": port, "dataDir": str(root / "数据 A"),
                    "engineRoot": str(root / "引擎 A")}
            with socket.socket() as outsider:
                outsider.bind(("127.0.0.1", 0))
                outsider.listen()
                expect_status(409, a.call, "instances/deploy", {**body, "port": outsider.getsockname()[1]})
            foreign = root / "原数据"
            foreign.mkdir()
            (foreign / "keep").write_bytes(b"original")
            expect_status(409, a.call, "instances/deploy", {**body, "dataDir": str(foreign)})
            assert (foreign / "keep").read_bytes() == b"original"
            parallel = open_workbench(first)
            with ThreadPoolExecutor(2) as pool:
                results = list(pool.map(lambda api: api.call("instances/deploy", body, timeout=35), (a, parallel)))
            assert results == [key, key]
            installed, = a.call("instances")["instances"]
            assert installed["state"] == "running" and installed["installed"]
            pid = installed["pid"]
            assert resp("SET", "实例见证", "关闭工作台后仍可读") == b"OK"
            association = a.call("instances/connection", key)
            assert association == a.call("instances/connection", key)
            assert len(a.call("state")["profiles"]) == 1
            for directory in [str(root / "数据 A"), str(root / "数据 A" / "child")]:
                expect_status(409, a.call, "instances/deploy",
                              {**body, "id": str(uuid.uuid4()), "port": a.call("instances")["suggestedPort"],
                               "dataDir": str(root / "另一个数据"), "engineRoot": directory})
            assert "database.log" in installed["logPath"]
            assert a.call("instances/logs", key)["text"]
            expect_status(409, a.call, "instances/discard", key)
        assert resp("GET", "实例见证") == "关闭工作台后仍可读".encode()
        b = open_workbench(stack)
        assert b.call("instances")["instances"][0]["pid"] == pid
        assert b.call("instances/restart", key, timeout=35) == key
        after = b.call("instances")["instances"][0]
        assert after["state"] == "running" and after["pid"] != pid
        assert resp("GET", "实例见证") == "关闭工作台后仍可读".encode()
        b.call("instances/stop", key, timeout=35)
        assert b.call("instances")["instances"][0]["state"] == "stopped"
        data = Path(body["dataDir"])
        assert any((data / "go-log-v1").iterdir())
        original = root / "temporarily unavailable"
        data.rename(original)
        try:
            expect_status(409, b.call, "instances/start", key)
            assert not data.exists()
        finally:
            original.rename(data)
        # Exercise the legacy three-column schema as if reopening the first build.
        db = config.with_name(config.name + ".instances") / "instances.sqlite3"
        with closing(sqlite3.connect(db)) as legacy, legacy:
            legacy.execute("ALTER TABLE instances DROP COLUMN installed")
        assert b.call("instances")["instances"][0]["installed"]
        expect_status(409, b.call, "instances/discard", key)
        other_config, other_stop = root / "foreign.json", root / "foreign.stop"
        options = json.loads(Path(installed["configPath"]).read_bytes())
        options.update(data_dir=str(root / "external database"), log_path=str(root / "external.log"))
        other_config.write_text(json.dumps(options), encoding="utf-8")
        engine = Path(installed["engineDir"])
        env = {key: value for key, value in os.environ.items() if not key.upper().startswith("KERNIVA_")}
        env["KERNIVA_ENGINE_LIBRARY"] = str(engine / "kerniva_engine.dll")
        foreign = subprocess.Popen([str(engine / "kerniva-server.exe"), "-config", str(other_config), "-stop-file",
                                    str(other_stop)], cwd=root, env=env, stdin=subprocess.DEVNULL,
                                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        try:
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline and foreign.poll() is None:
                try:
                    assert resp("SET", "foreign", "external-owner") == b"OK"
                    break
                except (OSError, redis.RedisWorkbenchError):
                    time.sleep(.1)
            assert b.call("instances/stop", key) == key
            expect_status(409, b.call, "instances/start", key)
            assert foreign.poll() is None and resp("GET", "foreign") == b"external-owner"
        finally:
            other_stop.touch()
            assert foreign.wait(timeout=12) == 0
        broken = root / "无法作为引擎目录的文件"
        broken.write_bytes(b"keep")
        abandoned = {"id": str(uuid.uuid4())}
        expect_status(500, b.call, "instances/deploy", {**body, **abandoned, "engineRoot": str(broken),
                      "port": b.call("instances")["suggestedPort"], "dataDir": str(root / "未完成")})
        assert b.call("instances/discard", abandoned) == abandoned
        assert broken.read_bytes() == b"keep" and len(b.call("instances")["instances"]) == 1
        check_startup(root)
        return {"version": state["version"], "pidBefore": pid, "pidAfter": after["pid"],
                "concurrentDeployRequests": 2, "resultingInstances": 1, "afterRestartDB1": "关闭工作台后仍可读",
                "survivedAllWorkbenchExits": True, "independentServerPreserved": True, "failedRegistrationReleased": True,
                "foreignDatabaseReadAfterRejectedStart": "external-owner", "foreignPid": foreign.pid,
                "missingDataDirectoryRejected": True, "legacyRegistrationRetained": True,
                "registrationTest": "isolated HKCU test key, real Windows Run entry unchanged"}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--engine", type=Path, help="exercise the source backend with a packaged engine")
    group.add_argument("--bundle", type=Path, help="exercise a complete unpacked Windows bundle")
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    result = verify(args.engine.resolve() if args.engine else None, args.bundle.resolve() if args.bundle else None)
    args.report.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(args.report)
