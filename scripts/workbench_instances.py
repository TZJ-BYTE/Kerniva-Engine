"""Install a bundled Go-log engine and manage the current user's local instances."""

from contextlib import closing
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import shutil
import socket
import sqlite3
import subprocess
import sys
import tempfile
import time
import uuid

from workbench_operations import Problem
import workbench_redis


BINARIES = ("kerniva-server.exe", "kerniva_engine.dll")
LOGIN_VALUE = "Kerniva Workbench"
LOGIN_KEY = r"Software\Microsoft\Windows\CurrentVersion\Run"


def file_hash(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def identity(value):
    try:
        result = str(uuid.UUID(value))
        if result != value:
            raise ValueError
        return result
    except (AttributeError, TypeError, ValueError):
        raise Problem(400, "实例编号无效。") from None


def selected_path(value):
    if not isinstance(value, str) or not value or any(ord(c) < 32 for c in value):
        raise Problem(400, "请输入本地磁盘上的绝对目录。")
    path = Path(value)
    if not path.is_absolute() or not re.fullmatch(r"[A-Za-z]:", path.drive) or ".." in path.parts:
        raise Problem(400, "目录需要使用本地盘符和绝对路径。")
    if any(part.endswith((".", " ")) or re.search(r'[<>:"|?*]', part) or Path(part).is_reserved()
           for part in path.parts[1:]):
        raise Problem(400, "目录名称包含 Windows 保留字符或设备名。")
    resolved = path.resolve()
    if path == Path(path.anchor) or resolved != path:
        raise Problem(400, "请选择磁盘上的普通子目录，避开盘根和目录链接。")
    return resolved


def overlaps(left, right):
    return left == right or left in right.parents or right in left.parents


def manifest(folder):
    try:
        data = json.loads((folder / "manifest.json").read_bytes())
    except (OSError, ValueError) as error:
        raise Problem(409, "数据库组件清单无法读取。") from error
    arch = {"amd64": "amd64", "x86_64": "amd64", "arm64": "arm64", "aarch64": "arm64"}.get(platform.machine().lower())
    if (not isinstance(data, dict) or data.get("schema_version") != 1 or data.get("product") != "kerniva-engine"
            or data.get("os") != "windows" or data.get("arch") != arch
            or not re.fullmatch(r"[0-9]+(?:\.[0-9]+){0,2}", str(data.get("version")))
            or not isinstance(data.get("files"), dict) or set(data["files"]) != set(BINARIES)):
        raise Problem(409, "数据库组件清单或系统架构不匹配。")
    for name in BINARIES:
        entry = data["files"][name]
        if (not isinstance(entry, dict) or type(entry.get("size")) is not int or entry["size"] < 1
                or not re.fullmatch(r"[0-9a-f]{64}", str(entry.get("sha256")))):
            raise Problem(409, "数据库组件清单缺少文件大小或校验值。")
    return data


def verify_engine(folder, record):
    if file_hash(folder / "manifest.json") != record["manifestSha256"]:
        raise Problem(409, "数据库组件清单已变化。")
    for name, entry in manifest(folder)["files"].items():
        file = folder / name
        if file.stat().st_size != entry["size"] or file_hash(file) != entry["sha256"]:
            raise Problem(409, f"数据库组件校验失败：{name}")


def install_engine(source, record):
    target = Path(record["engineDir"])
    if not target.exists():
        verify_engine(source, record)
        target.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(prefix=".kerniva-", dir=target.parent) as temporary:
            incoming = Path(temporary) / "engine"
            incoming.mkdir()
            for name in (*BINARIES, "manifest.json"):
                shutil.copyfile(source / name, incoming / name)
            verify_engine(incoming, record)
            try:
                incoming.rename(target)
            except FileExistsError:
                pass   # Another instance installed the same immutable version.
    verify_engine(target, record)


def server_args(record):
    return [str(Path(record["engineDir"]) / BINARIES[0]), "-config", record["configPath"],
            "-stop-file", record["stopFile"]]


def owned_processes():
    # Fixed query; command-line values are compared as data in Python.
    script = """
$ErrorActionPreference='Stop'
[Console]::OutputEncoding=[Text.UTF8Encoding]::new($false)
$rows=@(Get-CimInstance Win32_Process -Filter "Name='kerniva-server.exe'" |
    Select-Object ProcessId,ExecutablePath,CommandLine)
ConvertTo-Json -InputObject $rows -Compress
"""
    ps = Path(os.environ["SystemRoot"]) / "System32/WindowsPowerShell/v1.0/powershell.exe"
    try:
        result = subprocess.run([str(ps), "-NoProfile", "-NonInteractive", "-Command", script],
                                capture_output=True, timeout=8, check=True, creationflags=subprocess.CREATE_NO_WINDOW)
        return {(os.path.normcase(row["ExecutablePath"]), row["CommandLine"]): row["ProcessId"]
                for row in json.loads(result.stdout) if row["ExecutablePath"] and row["CommandLine"]}
    except (ValueError, OSError, subprocess.SubprocessError) as error:
        raise Problem(503, "无法核对 Windows 数据库进程的身份，请检查本机 CIM 服务。") from error


def owner_pid(record, processes):
    args = server_args(record)
    return processes.get((os.path.normcase(args[0]), subprocess.list2cmdline(args)))


def healthy(record):
    try:
        with workbench_redis._Client({"host": "127.0.0.1", "port": record["port"], "timeout": 1}) as client:
            info = workbench_redis._info_fields(client, ("127.0.0.1", record["port"]))
        return (info.get("storage_backend") == "go-log" and
                Path(info.get("data_dir", "")) == Path(record["dataDir"]) / "go-log-v1")
    except (ValueError, OSError, workbench_redis.RedisWorkbenchError):
        return False


def available_port(port):
    try:
        with socket.socket() as check:
            check.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
            check.bind(("127.0.0.1", port))
    except OSError:
        raise Problem(409, f"本机端口 {port} 已占用。请选择其他端口。") from None


def install_data(record, installed):
    data = Path(record["dataDir"])
    owner = data / ".kerniva-instance"
    config = Path(record["configPath"])
    if installed and (not owner.is_file() or not config.is_file() or
                      not all((data / "go-log-v1" / f"db_{i}").is_dir() for i in range(16))):
        raise Problem(409, "已部署实例的持久化目录、身份标记或配置缺失。请恢复原数据位置后再启动。")
    data.mkdir(parents=True, exist_ok=True)
    if not owner.exists() and any(data.iterdir()):
        raise Problem(409, "数据目录已有内容。部署新实例需要空目录。")
    if not owner.exists():
        with tempfile.NamedTemporaryFile(dir=data.parent, prefix=".kerniva-owner-", delete=False) as out:
            staged_owner = Path(out.name)
            out.write(record["id"].encode("ascii"))
            out.flush()
            os.fsync(out.fileno())
        try:
            staged_owner.rename(owner)
        except FileExistsError:
            pass
        finally:
            staged_owner.unlink(missing_ok=True)
    if owner.read_bytes() != record["id"].encode("ascii"):
        raise Problem(409, "数据目录属于另一个实例。")
    settings = {"host": "127.0.0.1", "port": record["port"], "network_type": "std", "db_count": 16,
                "storage_backend": "go-log", "persistence_enabled": True, "cold_start_strategy": "load_all",
                "persistence_write_mode": "strong", "persistence_durability": "sync", "go_log_group_max": 8,
                "go_log_segment_age_seconds": 30, "data_dir": str(data), "log_path": record["logPath"]}
    config.parent.mkdir(parents=True, exist_ok=True)
    if not config.exists():
        staged = config.with_suffix(".tmp")
        staged.write_text(json.dumps(settings, ensure_ascii=False, indent=2), encoding="utf-8")
        staged.replace(config)
    if json.loads(config.read_bytes()) != settings:
        raise Problem(409, "磁盘配置与登记的实例不一致，请先核对配置文件。")


def start_instance(record, processes):
    if owner_pid(record, processes):
        if healthy(record):
            return
        raise Problem(409, "已有此实例的进程正在启动或健康检查失败。请查看日志。")
    available_port(record["port"])
    stop = Path(record["stopFile"])
    stop.unlink(missing_ok=True)
    env = {key: value for key, value in os.environ.items() if not key.upper().startswith("KERNIVA_")}
    env["KERNIVA_ENGINE_LIBRARY"] = str(Path(record["engineDir"]) / BINARIES[1])
    # The database logger rotates logPath. Redirect its duplicate stderr stream.
    process = subprocess.Popen(server_args(record), cwd=record["engineDir"], stdin=subprocess.DEVNULL,
                               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env=env, close_fds=True,
                               creationflags=subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.CREATE_NO_WINDOW)
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise Problem(409, f"数据库启动退出（{process.returncode}），请查看实例日志。")
        if healthy(record):
            return
        time.sleep(.1)
    raise Problem(503, "进程已启动，等待健康检查超时。请刷新状态或查看日志。")


def stop_instance(record, processes):
    pid = owner_pid(record, processes)
    if pid is None:
        return
    Path(record["stopFile"]).touch()
    deadline = time.monotonic() + 12
    while time.monotonic() < deadline:
        if owner_pid(record, owned_processes()) != pid:
            return
        time.sleep(.15)
    raise Problem(503, "已请求正常停止，数据库仍在退出。请稍后刷新状态。")


class Instances:
    def __init__(self, config, engine=None):
        self.root = config.path.with_name(config.path.name + ".instances")
        self.db = self.root / "instances.sqlite3"
        self.desktop = (Path(sys.executable).resolve().parent.parent / "Kerniva Workbench.exe"
                        if os.name == "nt" and getattr(sys, "frozen", False) else None)
        self.source = (Path(engine).resolve() if engine else self.desktop.parent / "engine" if self.desktop
                       else Path(__file__).resolve().parents[1] / "bin/engine")

    def bundle(self):
        if os.name != "nt":
            raise Problem(409, "当前版本在 Windows 上提供本机单机部署。")
        if not (self.source / "manifest.json").is_file():
            raise Problem(409, "此工作台未附带 Windows 数据库组件。请使用包含引擎的安装包。")
        return manifest(self.source)

    def records(self, conn):
        return [json.loads(row[0]) for row in conn.execute("SELECT record FROM instances ORDER BY rowid")]

    def database(self):
        self.root.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self.db, timeout=20)
        conn.execute("CREATE TABLE IF NOT EXISTS instances "
                     "(id TEXT PRIMARY KEY, port INTEGER UNIQUE, record TEXT NOT NULL, installed INTEGER NOT NULL DEFAULT 0)")
        if "installed" not in {row[1] for row in conn.execute("PRAGMA table_info(instances)")}:
            with conn:
                conn.execute("BEGIN IMMEDIATE")
                if "installed" not in {row[1] for row in conn.execute("PRAGMA table_info(instances)")}:
                    # Existing registrations may already own durable user data.
                    conn.execute("ALTER TABLE instances ADD COLUMN installed INTEGER NOT NULL DEFAULT 1")
        return conn

    def list(self):
        try:
            version, reason = self.bundle()["version"], ""
        except Problem as error:
            version, reason = None, str(error)
        records = []
        installed = set()
        if os.name == "nt" and self.db.exists():
            with closing(self.database()) as conn:
                records = self.records(conn)
                installed = {row[0] for row in conn.execute("SELECT id FROM instances WHERE installed = 1")}
        processes = owned_processes() if records else {}
        suggested = 16379
        if version:
            used = {r["port"] for r in records}
            try:
                available_port(suggested)
            except Problem:
                used.add(suggested)
            while suggested in used:
                with socket.socket() as probe:
                    probe.bind(("127.0.0.1", 0))
                    suggested = probe.getsockname()[1]
        return {"version": version, "reason": reason, "suggestedPort": suggested, "engineRoot": str(self.root / "engines"),
                "dataRoot": str(self.root / "data"),
                "instances": [{**self.public(r, processes), "installed": r["id"] in installed} for r in records]}

    @staticmethod
    def public(r, processes):
        pid = owner_pid(r, processes)
        status = "running" if pid and healthy(r) else "unhealthy" if pid else "stopped"
        return {k: r[k] for k in ("id", "name", "port", "engineDir", "dataDir", "configPath", "logPath", "version")} | {
            "pid": pid, "state": status}

    def deploy(self, body):
        if body.keys() - {"id", "name", "port", "engineRoot", "dataDir"}:
            raise Problem(400, "部署参数无效。")
        product = self.bundle()
        key = identity(body.get("id"))
        name, port = body.get("name"), body.get("port")
        if (not isinstance(name, str) or not name.strip() or len(name) > 100
                or type(port) is not int or not 1 <= port <= 65535):
            raise Problem(400, "请填写实例名称和 1–65535 之间的端口。")
        digest = file_hash(self.source / "manifest.json")
        engine = selected_path(body.get("engineRoot") or str(self.root / "engines"))
        engine = engine / (product["version"] + "-" + digest[:16])
        data = selected_path(body.get("dataDir") or str(self.root / "data" / key))
        control = self.root / "control" / key
        protected = [self.root / "control", self.source]
        if (overlaps(data, engine) or any(overlaps(path, reserved) for path in (engine, data) for reserved in protected)
                or data == self.root or data in self.root.parents or engine == self.root or engine in self.root.parents):
            raise Problem(409, "数据目录和引擎、控制文件需分开存放。")
        record = dict(id=key, name=name.strip(), port=port, engineDir=str(engine), dataDir=str(data),
                      version=product["version"], manifestSha256=digest, configPath=str(control / "server.json"),
                      logPath=str(control / "database.log"), stopFile=str(control / "stop"))
        with closing(self.database()) as conn:
            with conn:
                conn.execute("BEGIN IMMEDIATE")
                existing = self.find(conn, key, required=False)
                if existing and existing != record:
                    raise Problem(409, "同一部署编号已登记不同参数。")
                if not existing:
                    if data.exists() and (not data.is_dir() or any(data.iterdir())):
                        raise Problem(409, "请选择空的数据目录，现有内容将予以保留。")
                    for other in self.records(conn):
                        used, other_engine = Path(other["dataDir"]), Path(other["engineDir"])
                        if (overlaps(data, used) or overlaps(engine, used) or overlaps(data, other_engine)
                                or (engine != other_engine and overlaps(engine, other_engine))
                                or other["port"] == port):
                            raise Problem(409, "端口或数据目录已由工作台的另一实例使用。")
                    available_port(port)
                    conn.execute("INSERT INTO instances (id, port, record, installed) VALUES (?, ?, ?, 0)",
                                 (key, port, json.dumps(record)))
        return self.action("start", {"id": key})

    @staticmethod
    def find(conn, key, required=True):
        row = conn.execute("SELECT record FROM instances WHERE id=?", (key,)).fetchone()
        if row:
            return json.loads(row[0])
        if required:
            raise Problem(404, "本工作台没有登记此实例。")

    def action(self, action, body):
        if os.name != "nt" or body.keys() != {"id"}:
            raise Problem(400, "实例操作或参数无效。")
        key = identity(body["id"])
        with closing(self.database()) as conn, conn:
            conn.execute("BEGIN IMMEDIATE")
            r = self.find(conn, key)
            if action == "discard":
                if owner_pid(r, owned_processes()) or conn.execute(
                        "SELECT installed FROM instances WHERE id=?", (key,)).fetchone()[0]:
                    raise Problem(409, "只能取消尚未成功部署且没有运行进程的实例。")
                conn.execute("DELETE FROM instances WHERE id=?", (key,))
                return {"id": key}
            # Directory mappings may change between launches, including via junctions.
            for path in (r["dataDir"], r["engineDir"], r["configPath"], r["logPath"], r["stopFile"]):
                selected_path(path)
            if Path(r["configPath"]).parent != self.root / "control" / key:
                raise Problem(409, "实例的控制目录与登记编号不匹配。")
            if action == "open-data":
                data = Path(r["dataDir"])
                marker = data / ".kerniva-instance"
                if not marker.is_file() or marker.read_bytes() != key.encode("ascii"):
                    raise Problem(409, "数据目录缺少此实例的登记标记。")
                os.startfile(str(data))
                return {"id": key}
            if action == "logs":
                path = Path(r["logPath"])
                if not path.is_file():
                    return {"text": ""}
                with path.open("rb") as stream:
                    stream.seek(max(0, stream.seek(0, 2) - 16384))
                    return {"text": stream.read().decode("utf-8", errors="replace")}
            if action == "connection":
                return {k: r[k] for k in ("name", "port")}
            if action not in ("start", "stop", "restart"):
                raise Problem(404, "未知实例操作。")
            processes = owned_processes()
            if action in ("start", "restart"):
                installed = conn.execute("SELECT installed FROM instances WHERE id=?", (key,)).fetchone()[0]
                install_engine(self.source, r)
                install_data(r, installed)
            if action in ("stop", "restart"):
                stop_instance(r, processes)
                processes = owned_processes()
            if action in ("start", "restart"):
                start_instance(r, processes)
                conn.execute("UPDATE instances SET installed=1 WHERE id=?", (key,))
            return {"id": key}

    def startup(self, body=None):
        if not self.desktop or not self.desktop.is_file():
            if body is not None:
                raise Problem(409, "登录启动只适用于已解压或安装的 Windows 桌面程序。")
            return {"available": False}
        import winreg
        command = '"' + str(self.desktop) + '"'
        if body is None:
            try:
                registry = winreg.OpenKey(winreg.HKEY_CURRENT_USER, LOGIN_KEY)
            except FileNotFoundError:
                return {"available": True, "enabled": False, "otherLocation": False}
        else:
            if body.keys() != {"enabled"} or type(body["enabled"]) is not bool:
                raise Problem(400, "登录启动设置无效。")
            registry = winreg.CreateKeyEx(winreg.HKEY_CURRENT_USER, LOGIN_KEY, 0, winreg.KEY_READ | winreg.KEY_SET_VALUE)
        with registry as key:
            try:
                value, _ = winreg.QueryValueEx(key, LOGIN_VALUE)
            except FileNotFoundError:
                value = None
            if body is not None:
                if value is not None and value != command:
                    raise Problem(409, "登录启动项属于其他工作台安装，请先在原安装中关闭。")
                if body["enabled"]:
                    winreg.SetValueEx(key, LOGIN_VALUE, 0, winreg.REG_SZ, command)
                    value = command
                elif value is not None:
                    winreg.DeleteValue(key, LOGIN_VALUE)
                    value = None
            return {"available": True, "enabled": value == command, "otherLocation": bool(value and value != command)}
