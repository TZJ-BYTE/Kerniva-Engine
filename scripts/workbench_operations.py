"""Managed replication, its durable activity journal and a client for the local workbench API."""

from contextlib import closing, contextmanager, ExitStack
import importlib.util
import json
from http.client import HTTPException
from pathlib import Path
import socket
import sqlite3
import sys
import tempfile
import time
from types import SimpleNamespace
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener


class Problem(Exception):
    def __init__(self, status, message):
        super().__init__(message)
        self.status = status


def controller_url(value):
    try:
        url = urlsplit(value)
        if (url.scheme not in ("http", "https") or not url.hostname or url.username is not None
                or url.password is not None or url.query or url.fragment or url.port == 0):
            raise ValueError
    except (ValueError, TypeError, AttributeError):
        raise Problem(400, "复制管理地址必须是无凭据的 HTTP(S) 基础地址。") from None
    return value.rstrip("/")


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, *args):
        return None


class JsonApi:
    def __init__(self, base):
        self.url = controller_url(base)
        self.opener = build_opener(ProxyHandler({}), NoRedirect())

    def call(self, path, body=None, timeout=12):
        data = None if body is None else json.dumps(body).encode()
        request = Request(self.url + "/api/" + path, data=data, headers={"Content-Type": "application/json"})
        try:
            with self.opener.open(request, timeout=timeout) as response:
                raw = response.read(32 * 1024 * 1024 + 1)
                if len(raw) > 32 * 1024 * 1024:
                    raise Problem(502, "服务返回的数据超过读取上限")
                result = json.loads(raw)
                if not isinstance(result, dict):
                    raise ValueError
                return result
        except HTTPError as error:
            try:
                msg = json.loads(error.read(8192)).get("error", "")
            except (ValueError, AttributeError):
                msg = ""
            finally:
                error.close()
            raise Problem(error.code if 400 <= error.code <= 599 else 502,
                          str(msg)[:1000] or f"服务返回 HTTP {error.code}") from None
        except (HTTPException, URLError, OSError, ValueError) as error:
            raise Problem(503, f"服务连接失败或响应无效：{error}") from None


def operation(action, body):
    if not isinstance(body, dict):
        raise Problem(400, "需要 JSON 对象")
    if action == "mark" and body.keys() == {"db", "value"}:
        db, value = body["db"], body["value"]
        if type(db) is int and db in (0, 1) and isinstance(value, str) and 0 < len(value.encode()) <= 256:
            return "marker", {"db": db, "value": value}, f"DB {db} 写入标记 · {value}"
    if action in ("pause", "resume") and body.keys() == {"replica"} and body["replica"] in ("a", "b"):
        name = body["replica"]
        label = "暂停" if action == "pause" else "恢复"
        return f"replicas/{name}/{action}", {}, f"{label}副本 {name.upper()}"
    raise Problem(400, "操作参数无效。副本为 A/B，标记支持 DB 0/1 与 1–256 个 UTF-8 字节。")


def action_from_path(path, body):
    if path == "marker":
        return "mark", body
    parts = path.split("/")
    if len(parts) == 3 and parts[0] == "replicas" and parts[1] in ("a", "b") and parts[2] in ("pause", "resume"):
        if body:
            raise Problem(400, "副本控制需要空的 JSON 对象")
        return parts[2], {"replica": parts[1]}
    raise Problem(404, "复制操作不存在")


class Attached:
    def __init__(self, url):
        self.api = JsonApi(url)
        self.label = self.api.url

    def status(self):
        result = self.api.call("status")
        if not all(isinstance(result.get(k), dict) for k in ("source", "primary", "replicas")):
            raise Problem(502, "该地址未返回 Kerniva 复制管理状态")
        return result

    def change(self, path, body):
        result = self.api.call(path, body, timeout=25)
        if result.get("ok") is not True:
            raise Problem(502, "管理端没有确认操作成功，请先查看状态。")
        return result


class Embedded:
    def __init__(self, demo, root):
        self.demo = demo
        self.label = str(root)

    def status(self):
        with self.demo.lock:
            return self.demo.status()

    def change(self, path, body):
        with self.demo.lock:
            if self.demo.closed:
                raise Problem(503, "复制服务正在关闭")
            if path == "marker":
                self.demo.write_marker(body["db"], body["value"])
            else:
                _, replica, action = path.split("/")
                self.demo.set_paused(replica, action == "pause")
        return {"ok": True}


@contextmanager
def embedded(bin_dir):
    if not sys.platform.startswith("linux"):
        raise Problem(400, "托管的 Go-log 进程需要 Linux。可在 WSL 中运行 bash kerniva workbench --with-demo。")
    names = ("kerniva-server", "kerniva-replica", "libkerniva_engine.so")
    binaries = [(bin_dir / name).resolve() for name in names]
    if not all(p.is_file() for p in binaries):
        raise Problem(400, f"请先构建 Linux 组件或用 --bin-dir 指定包含 {', '.join(names)} 的目录。")
    spec = importlib.util.spec_from_file_location("kerniva_demo", Path(__file__).with_name("kerniva-replica-demo.py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    root = Path(tempfile.mkdtemp(prefix="kerniva-workbench-demo-"))
    with ExitStack() as stack:
        reservations = [stack.enter_context(socket.socket()) for _ in range(4)]
        for reservation in reservations:
            reservation.bind(("127.0.0.1", 0))
        args = SimpleNamespace(server_bin=binaries[0], replica_bin=binaries[1], library=binaries[2], workbench_url="")
        demo = module.Demo(root, args, reservations)
        stack.callback(demo.close)
        demo.start()
        yield Embedded(demo, root)


class Journal:
    def __init__(self, filename):
        self.filename = filename

    @contextmanager
    def open(self):
        try:
            with closing(sqlite3.connect(self.filename, timeout=5)) as db, db:
                db.execute("CREATE TABLE IF NOT EXISTS activity (id INTEGER PRIMARY KEY, source TEXT NOT NULL, "
                           "time_ms INTEGER NOT NULL, message TEXT NOT NULL)")
                db.execute("CREATE INDEX IF NOT EXISTS by_source ON activity(source,id)")
                yield db
        except sqlite3.Error as error:
            raise Problem(503, "无法读写工作台操作记录，请检查配置目录。") from error

    def record(self, source, message):
        with self.open() as db:
            db.execute("INSERT INTO activity (source,time_ms,message) VALUES (?,?,?)",
                       (source, time.time_ns() // 1000000, message))

    def page(self, source, page):
        with self.open() as db:
            db.execute("BEGIN")
            total, = db.execute("SELECT count(*) FROM activity WHERE source=?", (source,)).fetchone()
            pages = max(1, (total + 4) // 5)
            page = min(pages, max(1, page))
            rows = db.execute("SELECT id,time_ms,message FROM activity WHERE source=? ORDER BY id DESC LIMIT 5 OFFSET ?",
                              (source, (page - 1) * 5))
            return {"total": total, "page": page, "pages": pages, "items": [
                {"id": row[0], "when": row[1], "message": row[2], "source": source} for row in rows]}


def source_id(status):
    dbs = status.get("source", {}).get("dbs", {})
    ids = {d["source"] for d in dbs.values() if isinstance(d, dict) and isinstance(d.get("source"), str) and d["source"]}
    return ids.pop() if len(ids) == 1 else None


class OnlineShell:
    """Expose the same operations as the in-process service to a terminal session."""

    def __init__(self, url):
        self.api = JsonApi(url)
        if urlsplit(self.api.url).hostname not in ("127.0.0.1", "localhost", "::1"):
            raise Problem(400, "终端的 --url 指向本地工作台；远程数据库地址保存在连接档案。")

    def state(self):
        return self.api.call("state", timeout=3)

    def perform(self, action, body):
        return self.api.call(action, body, timeout=40)

    def replication_status(self):
        return self.api.call("replication/status")

    def replication_activity(self, page=1):
        return self.api.call(f"replication/activity?page={int(page)}")

    def replication_action(self, action, body):
        path, data, _ = operation(action, body)
        return self.api.call("replication/" + path, data, timeout=40)
