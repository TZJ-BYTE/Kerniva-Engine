#!/usr/bin/env python3
"""Kerniva database workbench and interactive shell."""

import argparse
from contextlib import ExitStack, contextmanager
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import ipaddress
import json
import os
from pathlib import Path
import secrets
import signal
import sys
import tempfile
import threading
import time
from urllib.parse import parse_qs, urlsplit
import uuid

import workbench_redis as adapter
from workbench_operations import Attached, Journal, OnlineShell, Problem, action_from_path, controller_url, embedded, operation, source_id


ASSETS = {
    "/": ("index.html", "text/html; charset=utf-8"),
    "/index.html": ("index.html", "text/html; charset=utf-8"),
    "/app.js": ("app.js", "text/javascript; charset=utf-8"),
    "/ops.js": ("ops.js", "text/javascript; charset=utf-8"),
    "/styles.css": ("styles.css", "text/css; charset=utf-8"),
}
UI = Path(__file__).resolve().parent / "workbench-ui"
DEFAULTS = {"pageSize": 50, "previewBytes": 4096, "autoRefresh": False,
            "refreshSeconds": 10, "formatJson": True}
PROFILE_FIELDS = {"id", "name", "host", "port", "mode", "db", "username", "tls", "caFile", "timeout"}
IDLE_SECONDS = 60 * 60
MAX_BODY = 2 * 1024 * 1024


def require(value, message):
    if not value:
        raise Problem(400, message)


def fields(obj, allowed):
    require(isinstance(obj, dict), "需要 JSON 对象")
    require(obj.keys() <= allowed, "请求包含无法识别的字段")


def integer(value, low, high, label):
    require(type(value) is int and low <= value <= high, f"{label}应为 {low} 到 {high} 的整数")
    return value


def flag(value, label):
    require(type(value) is bool, f"{label}需要布尔值")
    return value


def string(value, maximum, label, *, empty=False):
    require(isinstance(value, str) and (empty or value.strip()) and len(value) <= maximum,
            f"{label}为空或过长")
    return value


def settings(data):
    fields(data, DEFAULTS.keys())
    data = {**DEFAULTS, **data}
    return {
        "pageSize": integer(data["pageSize"], 1, 200, "每页条数"),
        "previewBytes": integer(data["previewBytes"], 64, 65536, "预览字节数"),
        "autoRefresh": flag(data["autoRefresh"], "自动刷新"),
        "refreshSeconds": integer(data["refreshSeconds"], 2, 300, "刷新间隔"),
        "formatJson": flag(data["formatJson"], "格式化 JSON"),
    }


def profile(data):
    fields(data, PROFILE_FIELDS)
    host = string(data.get("host"), 253, "主机").strip()
    if host.startswith("[") and host.endswith("]"):
        host = host[1:-1]
    require(host and not any(c.isspace() or ord(c) < 32 or c in "/\\@?#[]" for c in host),
            "请输入主机名或 IP 地址，端口请单独填写")
    if ":" in host:
        try:
            ipaddress.IPv6Address(host)
        except ValueError:
            raise Problem(400, "IPv6 地址无效，端口请单独填写") from None
    mode = data.get("mode", "standalone")
    require(mode in ("standalone", "cluster"), "请选择单机或 Redis Cluster")
    db = integer(data.get("db", 0), 0, 65535, "数据库编号")
    require(mode != "cluster" or db == 0, "Redis Cluster 只支持 DB 0")
    use_tls = flag(data.get("tls", False), "TLS")
    ca_file = string(data.get("caFile", ""), 4096, "CA 文件路径", empty=True).strip() if use_tls else ""
    require(not ca_file or ("\0" not in ca_file and Path(ca_file).is_absolute()), "CA 文件需要使用工作台主机的绝对路径")
    return {"name": string(data.get("name"), 100, "连接名称").strip(),
            "host": host, "port": integer(data.get("port", 6379), 1, 65535, "端口"),
            "mode": mode, "db": db, "username": string(data.get("username", ""), 256, "用户名", empty=True),
            "tls": use_tls, "caFile": ca_file, "timeout": integer(data.get("timeout", 5), 1, 30, "超时")}


def default_path():
    if os.name == "nt":
        base = Path(os.environ.get("APPDATA") or Path.home())
    else:
        base = Path(os.environ.get("XDG_CONFIG_HOME") or Path.home() / ".config")
    return base / "kerniva" / "workbench.json"


class Config:
    """Keep the editable JSON atomically consistent across tabs and local processes."""

    def __init__(self, path):
        self.path = path.expanduser().absolute()
        self.mutex = threading.Lock()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.lock_path = self.path.with_name(self.path.name + ".lock")
        with self.locked():
            if not self.path.exists():
                self.write({"version": 1, "profiles": [], "settings": DEFAULTS})
            self.read()

    @contextmanager
    def locked(self):
        with self.mutex, self.lock_path.open("a+b") as lock:
            if os.name == "nt":
                import msvcrt
                lock.seek(0)
                msvcrt.locking(lock.fileno(), msvcrt.LK_LOCK, 1)
            else:
                import fcntl
                fcntl.flock(lock, fcntl.LOCK_EX)
            try:
                yield
            finally:
                if os.name == "nt":
                    lock.seek(0)
                    msvcrt.locking(lock.fileno(), msvcrt.LK_UNLCK, 1)
                else:
                    fcntl.flock(lock, fcntl.LOCK_UN)

    def read(self):
        try:
            data = json.loads(self.path.read_text(encoding="utf-8-sig"))
            if data["version"] != 1 or not isinstance(data["profiles"], list):
                raise ValueError
            profiles = []
            ids = set()
            for item in data["profiles"]:
                pid = str(uuid.UUID(item["id"]))
                if pid in ids:
                    raise ValueError
                ids.add(pid)
                profiles.append({**profile(item), "id": pid})
            clean = {"version": 1, "profiles": profiles, "settings": settings(data["settings"])}
            if data.get("replicationUrl"):
                clean["replicationUrl"] = controller_url(data["replicationUrl"])
            return clean
        except (KeyError, TypeError, ValueError, AttributeError, Problem) as error:
            raise Problem(409, "配置文件内容无效，请修正工作台 JSON 后重试。") from error

    def write(self, data):
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=self.path.parent,
                                         prefix="." + self.path.name + "-", delete=False) as temp:
            name = temp.name
            try:
                json.dump(data, temp, ensure_ascii=False, indent=2)
                temp.write("\n")
                temp.flush()
                os.fsync(temp.fileno())
            except BaseException:
                temp.close()
                os.unlink(name)
                raise
        try:
            os.replace(name, self.path)
        finally:
            Path(name).unlink(missing_ok=True)


def lookup(data, pid):
    for item in data["profiles"]:
        if item["id"] == pid:
            return item
    raise Problem(404, "该连接已删除，请刷新连接列表。")


class Workbench:
    def __init__(self, config, demo_url=None, managed=None):
        self.config = config
        self.config_id = hashlib.sha256(os.fsencode(os.path.normcase(config.path.resolve()))).hexdigest()
        with config.locked():
            data = config.read()
            if demo_url:
                url = controller_url(demo_url)
                if url != data.get("replicationUrl"):
                    data["replicationUrl"] = url
                    config.write(data)
            self.demo_url = None if managed else data.get("replicationUrl")
        self.controller = managed or (Attached(self.demo_url) if self.demo_url else None)
        self.journal = Journal(config.path.with_name(config.path.name + ".activity.sqlite3"))
        self.action_lock = threading.Lock()
        self.sessions = {}
        self.lock = threading.Lock()

    def expire(self):
        with self.lock:
            now = time.monotonic()
            self.sessions = {k: v for k, v in self.sessions.items() if v["until"] > now}

    def state(self):
        with self.config.locked():
            data = self.config.read()
        return {**data, "demoUrl": (self.demo_url + "/") if self.demo_url else None,
                "apiVersion": 2, "configId": self.config_id, "replicationAvailable": bool(self.controller),
                "replicationLabel": self.controller.label if self.controller else None}

    def replication_status(self):
        if not self.controller:
            raise Problem(409, "当前工作台未绑定复制实例。")
        try:
            return self.controller.status()
        except (OSError, ValueError, RuntimeError) as error:
            raise Problem(503, f"复制实例暂不可用：{error}") from None

    def replication_action(self, action, body):
        path, data, message = operation(action, body)
        with self.action_lock:
            src = source_id(self.replication_status())
            try:
                result = self.controller.change(path, data)
            except (OSError, ValueError, RuntimeError) as error:
                raise Problem(503, f"没有收到操作确认，请先查看复制状态。{error}") from None
            if src:
                try:
                    self.journal.record(src, message)
                except Problem:
                    result["warning"] = "操作已生效，但未能写入活动记录。"
            else:
                result["warning"] = "操作已生效，但当时无法确定复制来源，未记入活动记录。"
            return result

    def replication_activity(self, page=1):
        src = source_id(self.replication_status())
        if not src:
            raise Problem(503, "暂时无法确定复制来源，不能读取该实例的活动记录。")
        return self.journal.page(src, page)

    def update(self, action, body):
        with self.config.locked():
            data = self.config.read()
            if action == "settings":
                fields(body, {"settings"})
                change = body.get("settings")
                fields(change, DEFAULTS.keys())
                result = {"settings": settings({**data["settings"], **change})}
                data.update(result)
            elif action == "profile/save":
                fields(body, {"profile"})
                raw = body.get("profile")
                item = profile(raw)
                if raw.get("id"):
                    old = lookup(data, raw["id"])
                    old.update(item)
                    item = old
                else:
                    item["id"] = str(uuid.uuid4())
                    data["profiles"].append(item)
                result = {"profile": item}
            else:
                fields(body, {"id"})
                data["profiles"].remove(lookup(data, body.get("id")))
                result = {"ok": True}
            self.config.write(data)
            if action.startswith("profile/"):
                with self.lock:
                    changed = item["id"] if action == "profile/save" else body["id"]
                    self.sessions = {k: v for k, v in self.sessions.items() if
                                     v["profile"]["id"] != changed or
                                     (action == "profile/save" and v["profile"] == item)}
            return result

    def connect(self, body):
        fields(body, {"id", "password"})
        password = string(body.get("password", ""), 16384, "密码", empty=True)
        with self.config.locked():
            item = lookup(self.config.read(), body.get("id"))
        info = database_call(adapter.probe, item, password)
        with self.config.locked():
            if lookup(self.config.read(), item["id"]) != item:
                raise Problem(409, "连接参数已变化，请使用新参数重新连接。")
            token = secrets.token_urlsafe(32)
            with self.lock:
                self.sessions[token] = {"profile": item, "password": password,
                                        "until": time.monotonic() + IDLE_SECONDS}
        return {"session": token, "info": info}

    def perform(self, action, body):
        if action.startswith("replication/"):
            verb, args = action_from_path(action[len("replication/"):], body)
            return self.replication_action(verb, args)
        if action in ("settings", "profile/save", "profile/delete"):
            return self.update(action, body)
        if action == "connect":
            return self.connect(body)
        if action not in ("scan", "key", "status", "disconnect"):
            raise Problem(404, "未知工作台操作")
        token = string(body.get("session"), 128, "会话")
        if action == "disconnect":
            fields(body, {"session"})
            with self.lock:
                self.sessions.pop(token, None)
            return {"ok": True}
        with self.config.locked():
            data = self.config.read()
            with self.lock:
                session = self.sessions.get(token)
                if (not session or session["until"] <= time.monotonic() or
                        session["profile"] not in data["profiles"]):
                    self.sessions.pop(token, None)
                    raise Problem(401, "连接已断开或配置已变更，请重新连接。")
                session["until"] = time.monotonic() + IDLE_SECONDS
        if action == "status":
            fields(body, {"session"})
            result = database_call(adapter.status, session["profile"], session["password"])
            return result
        fields(body, {"session", "cursor", "count", "node", "pattern"} if action == "scan"
               else {"session", "cursor", "count", "key_b64", "previewBytes"})
        req = {k: v for k, v in body.items() if k != "session"}
        req["count"] = integer(req.get("count", data["settings"]["pageSize"]), 1, 200, "页大小")
        if action == "scan":
            req["pattern"] = string(req.get("pattern", "*"), 4096, "键匹配", empty=True)
            if req.get("node") is not None:
                string(req["node"], 1024, "节点", empty=True)
        else:
            string(req.get("key_b64"), MAX_BODY, "键", empty=True)
            req["previewBytes"] = integer(req.get("previewBytes", data["settings"]["previewBytes"]),
                                          64, 65536, "预览字节数")
        if req.get("cursor") is not None:
            string(req["cursor"], MAX_BODY, "游标", empty=True)
        return database_call(adapter.scan if action == "scan" else adapter.inspect_key,
                             session["profile"], session["password"], req)


def database_call(func, item, password, *args):
    try:
        return func({**item, "password": password}, *args)
    except (adapter.RedisWorkbenchError, OSError, ValueError) as error:
        msg = str(error)
        if password:
            msg = msg.replace(password, "••••")
        raise Problem(422, msg[:1500] or "数据库请求失败") from None


class Handler(BaseHTTPRequestHandler):
    def setup(self):
        super().setup()
        self.connection.settimeout(10)

    def reply(self, status, kind, body):
        self.send_response(status)
        self.send_header("Content-Type", kind)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Referrer-Policy", "no-referrer")
        self.send_header("Content-Security-Policy", "default-src 'self'; connect-src 'self'; "
                         "frame-ancestors 'none'; base-uri 'none'; form-action 'none'")
        self.end_headers()
        self.wfile.write(body)

    def serve(self, post):
        try:
            host = self.headers.get("Host")
            port = self.server.server_port
            if self.headers.get_all("Host") != [host] or host not in (f"127.0.0.1:{port}", f"localhost:{port}"):
                raise Problem(403, "请使用本地工作台地址")
            if post:
                if (self.headers.get("Origin", "http://" + host) != "http://" + host or
                        self.headers.get("Sec-Fetch-Site", "same-origin") not in ("same-origin", "none")):
                    raise Problem(403, "仅接受来自当前工作台的请求")
                if self.headers.get_content_type() != "application/json":
                    raise Problem(415, "需要 application/json")
                length = int(self.headers.get("Content-Length", "0"))
                require(1 <= length <= MAX_BODY and not self.headers.get("Transfer-Encoding"), "请求大小无效")
                body = json.loads(self.rfile.read(length))
                require(isinstance(body, dict), "需要 JSON 对象")
                if not self.path.startswith("/api/"):
                    raise Problem(404, "接口不存在")
                payload = self.server.app.perform(self.path[5:], body)
            elif self.path == "/api/state":
                payload = self.server.app.state()
            elif self.path == "/api/replication/status":
                payload = self.server.app.replication_status()
            elif self.path == "/api/replication/experiments":
                payload = json.loads((UI / "replication-experiments.json").read_bytes())
            elif urlsplit(self.path).path == "/api/replication/activity":
                query = parse_qs(urlsplit(self.path).query, keep_blank_values=True)
                require(query.keys() <= {"page"} and len(query.get("page", ["1"])) == 1, "页码无效")
                page = int(query.get("page", ["1"])[0])
                require(page >= 1, "页码无效")
                payload = self.server.app.replication_activity(page)
            elif self.path in ASSETS:
                name, kind = ASSETS[self.path]
                self.reply(200, kind, (UI / name).read_bytes())
                return
            else:
                raise Problem(404, "页面不存在")
            status = 200
        except Problem as error:
            status, payload = error.status, {"error": str(error)}
        except (ValueError, UnicodeError, RecursionError):
            status, payload = 400, {"error": "请求格式无效"}
        except OSError:
            status, payload = 500, {"error": "工作台本地文件读写失败，请检查配置目录和权限。"}
        self.reply(status, "application/json; charset=utf-8", json.dumps(payload).encode())

    def do_GET(self):
        self.serve(False)

    def do_POST(self):
        self.serve(True)

    def log_message(self, *_args):
        pass


class Server(ThreadingHTTPServer):
    def service_actions(self):
        self.app.expire()


def main():
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument("--shell", action="store_true", help="Start the interactive workbench terminal.")
    cli.add_argument("--url", help="Attach the terminal to an existing local workbench origin.")
    cli.add_argument("--port", type=int, default=18782, help="Local HTTP port (0 selects an available port).")
    cli.add_argument("--config", type=Path, default=default_path(), help="Location of editable workbench JSON.")
    controllers = cli.add_mutually_exclusive_group()
    controllers.add_argument("--demo-url", help="Attach an existing replication controller and remember its address.")
    controllers.add_argument("--with-demo", action="store_true", help="Own a new Linux replication environment in this process.")
    cli.add_argument("--bin-dir", help="Linux binary directory for --with-demo (in WSL on Windows).")
    cli.add_argument("--wsl-distro", help="WSL distribution for the Windows managed controller.")
    cli.add_argument("--wsl-user", help="WSL user for the Windows managed controller.")
    args = cli.parse_args()
    if not 0 <= args.port <= 65535:
        cli.error("port must be between 0 and 65535")
    if args.url and (not args.shell or args.with_demo or args.demo_url):
        cli.error("--url attaches a shell to an existing workbench and takes no local controller options")
    if (args.wsl_distro or args.wsl_user) and (not args.with_demo or os.name != "nt"):
        cli.error("WSL options apply to --with-demo on Windows")
    try:
        with ExitStack() as stack:
            if args.shell:
                from workbench_shell import run
                online = OnlineShell(args.url or f"http://127.0.0.1:{args.port}")
                if args.url:
                    if online.state().get("apiVersion") != 2:
                        raise Problem(409, "目标不是支持交互终端的 Kerniva 工作台。")
                    return run(online)
                if not args.with_demo and not args.demo_url:
                    try:
                        remote = online.state()
                    except Problem:
                        remote = {}
                    config_id = hashlib.sha256(os.fsencode(os.path.normcase(args.config.expanduser().resolve()))).hexdigest()
                    if remote.get("apiVersion") == 2 and remote.get("configId") == config_id:
                        return run(online)
            config = Config(args.config)
            if not args.shell:
                server = stack.enter_context(Server(("127.0.0.1", args.port), Handler))
            if args.with_demo and sys.platform.startswith("linux"):
                def terminate(_signum, _frame):
                    raise KeyboardInterrupt
                previous = signal.signal(signal.SIGTERM, terminate)
                stack.callback(signal.signal, signal.SIGTERM, previous)
            managed = None
            if args.with_demo:
                if os.name == "nt":
                    from workbench_wsl import wsl_managed
                    env = wsl_managed(args.bin_dir, args.wsl_distro, args.wsl_user)
                else:
                    env = embedded(Path(args.bin_dir) if args.bin_dir else UI.parent.parent / "bin")
                managed = stack.enter_context(env)
            app = Workbench(config, args.demo_url, managed)
            if args.shell:
                return run(app)
            server.app = app
            print(f"Open http://127.0.0.1:{server.server_port}/\nConfig: {config.path}", flush=True)
            server.serve_forever()
    except KeyboardInterrupt:
        pass
    except (OSError, RuntimeError, Problem) as error:
        cli.exit(1, f"Workbench: {error}\n")


if __name__ == "__main__":
    sys.exit(main())
