"""Mouse and keyboard frontend for the same service used by the workbench shell."""

import asyncio
import base64
import sys
import time
import unicodedata

from textual import on, work
from rich.text import Text
from textual.app import App, ComposeResult
from textual.containers import Horizontal, Vertical, VerticalScroll
from textual.screen import ModalScreen
from textual.theme import Theme
from textual.widgets import (
    Button, Checkbox, Footer, Input, Label, OptionList, Select, Static, TabbedContent, TabPane,
)

from workbench_operations import Problem
from workbench_shell import _display_text, _format_json, pin_key_label


def plain(value):
    """Keep terminal controls, bidi overrides and markup in database values inert."""
    return "".join(c if c == "\n" or not unicodedata.category(c).startswith("C") else ascii(c)[1:-1]
                   for c in str(value))


def display_value(part):
    if not isinstance(part, dict):
        return plain(part)
    value = part.get("text")
    result = plain(value) if _display_text(value) and "\r" not in value else "Base64 " + part.get("b64", "")
    return result + (" …" if part.get("truncated") else "")


def key_details(page, format_json):
    if page.get("unsupported"):
        return plain(page["unsupported"])
    ttl = page.get("ttl_ms")
    timing = "无过期时间" if ttl == -1 else "键不存在" if ttl == -2 else f"TTL {ttl} ms"
    lines = [f"{plain(page['type'])}  ·  {timing}", ""]
    if "value" in page:
        part = page["value"]
        lines.extend([f"偏移 {page['offset']} / {page['length']} 字节", ""])
        if (format_json and part.get("text") and _display_text(part["text"])
                and "\r" not in part["text"] and not part.get("truncated")):
            lines.append(plain(_format_json(part["text"])))
        else:
            lines.append(display_value(part) or "（空值）")
    for row in page.get("items", []):
        if "fields" in row:
            lines.append(plain(row["id"]))
            lines.extend(f"  {display_value(pair['field'])}  {display_value(pair['value'])}" for pair in row["fields"])
        else:
            lines.append("   ".join(display_value(value) for value in row.values()))
    return "\n".join(lines)


class Form(ModalScreen):
    BINDINGS = [("escape", "cancel", "取消")]

    def __init__(self, title, fields, submit="保存"):
        super().__init__()
        self.title_text, self.fields, self.submit = title, fields, submit

    def compose(self) -> ComposeResult:
        with Vertical(id="dialog"):
            yield Label(self.title_text, classes="heading", markup=False)
            with VerticalScroll(id="fields"):
                for key, title, value, kind in self.fields:
                    if kind == "toggle":
                        yield Checkbox(title, value=bool(value), id=key)
                    else:
                        yield Label(title)
                        if isinstance(kind, list):
                            yield Select(kind, value=value, allow_blank=False, id=key, compact=True)
                        else:
                            yield Input(str(value), password=kind == "password", id=key,
                                        type="integer" if kind == "number" else "text", compact=True)
            yield Static("", id="form-error", markup=False)
            with Horizontal(classes="buttons"):
                yield Button(self.submit, id="accept", variant="primary", compact=True)
                yield Button("取消", id="cancel", compact=True)

    def action_cancel(self):
        self.dismiss(None)

    @on(Button.Pressed, "#cancel")
    def cancel(self):
        self.action_cancel()

    @on(Input.Submitted)
    @on(Button.Pressed, "#accept")
    def accept(self):
        result = {}
        for key, title, _, kind in self.fields:
            widget = self.query_one("#" + key)
            value = widget.value
            if kind == "number":
                try:
                    value = int(value)
                except ValueError:
                    self.query_one("#form-error", Static).update(title + "需要整数")
                    widget.focus()
                    return
            result[key] = value
            if kind == "password":
                widget.value = ""
        self.dismiss(result)


class PinList(ModalScreen):
    BINDINGS = [("escape", "cancel", "返回")]

    def __init__(self, state):
        super().__init__()
        self.pins = state["pins"]
        self.names = {profile["id"]: profile["name"] for profile in state["profiles"]}

    def compose(self) -> ComposeResult:
        with Vertical(id="pin-dialog"):
            yield Label("常用键", classes="heading")
            if self.pins:
                with VerticalScroll(id="pin-list"):
                    for i, pin in enumerate(self.pins):
                        with Horizontal(classes="buttons pin-row"):
                            label = f"{self.names.get(pin['profileId'], '连接已删除')}  ·  DB {pin['db']}"
                            title = Text(plain(f"{label}   {pin_key_label(pin['key_b64'])}"))
                            yield Button(title, id=f"open-{i}", classes="pin-location", compact=True)
                            yield Button("移除", id=f"remove-{i}", compact=True)
            else:
                yield Static("打开一个键后，点击「固定」即可在这里找到它。", markup=False)
            with Horizontal(classes="buttons"):
                yield Button("返回", id="pin-close", compact=True)

    def action_cancel(self):
        self.dismiss(None)

    @on(Button.Pressed)
    def choose(self, event):
        event.stop()
        if event.button.id == "pin-close":
            self.action_cancel()
        else:
            action, index = event.button.id.split("-")
            self.dismiss((action, self.pins[int(index)]["id"]))


class WorkbenchTUI(App):
    TITLE = "Kerniva"
    ENABLE_COMMAND_PALETTE = False
    BINDINGS = [("ctrl+r", "refresh", "刷新"), ("ctrl+q", "quit", "退出")]
    CSS = """
    Screen { background: $surface; }
    #connection { height: 1; background: $panel; color: $text; padding: 0 1; }
    TabbedContent, ContentSwitcher, TabPane { height: 1fr; }
    TabPane { padding: 0 1; }
    .buttons { height: 3; align-vertical: middle; }
    .buttons Button { min-width: 6; width: auto; margin-right: 1; }
    .buttons Input, .buttons Select { width: 1fr; min-width: 5; }
    .heading { text-style: bold; color: $accent; height: auto; margin-top: 1; }
    #profiles, #keys { height: 1fr; }
    #split, #list-side, #value-side, .scroll-body { height: 1fr; }
    #list-side { width: 42%; }
    #value-side { width: 1fr; padding-left: 1; }
    #split.narrow { layout: vertical; }
    #split.narrow > Vertical { width: 100%; padding: 0; }
    #notice { height: auto; max-height: 3; padding: 0 1; color: $text-muted; }
    #notice.error, #form-error { color: $error; }
    Form, PinList { align: center middle; }
    #pin-dialog { width: 80; max-width: 95%; height: auto; max-height: 90%; padding: 0 1;
                  border: round $accent; background: $panel; }
    #pin-list { height: auto; max-height: 55vh; }
    .pin-row { height: auto; min-height: 2; }
    .pin-row .pin-location { width: 1fr; height: auto; }
    #dialog { width: 60; max-width: 95%; height: auto; max-height: 90%; padding: 0 1;
              border: round $accent; background: $panel; }
    #fields { height: auto; max-height: 65vh; }
    #fields Label { margin-top: 1; }
    #form-error { height: auto; }
    """

    def __init__(self, backend):
        super().__init__()
        self.register_theme(Theme("kerniva", primary="#b96e50", accent="#dfab88", foreground="#efe8dd",
                                  background="#201e1c", surface="#252320", panel="#332f2b"))
        self.theme = "kerniva"
        self.backend = backend
        self.session = None
        self.profile = None
        self.active_db = None
        self.saved = []
        self.settings = {}
        self.replica_available = False
        self.key_rows = []
        self.pages = {}
        self.info_nodes = []
        self.activity_page = 1
        self.pending = False
        self.quit_requested = False
        self.next_refresh = 0

    def compose(self) -> ComposeResult:
        yield Static("Kerniva", id="connection", markup=False)
        with TabbedContent(initial="connections", id="views"):
            with TabPane("连接", id="connections"):
                yield Label("保存的连接", classes="heading")
                yield OptionList(id="profiles", markup=False)
                with Horizontal(classes="buttons"):
                    yield Button("新建连接", id="add", compact=True)
                    yield Button("编辑", id="edit", compact=True)
                    yield Button("断开", id="disconnect", compact=True)
            with TabPane("键浏览", id="browse"):
                with Horizontal(classes="buttons"):
                    yield Input("*", placeholder="匹配模式", id="pattern", compact=True)
                    yield Button("扫描", id="scan", compact=True)
                    yield Button("选库", id="database", compact=True)
                    yield Button("节点", id="node", compact=True)
                    yield Button("常用键", id="pins", compact=True)
                with Horizontal(id="split"):
                    with Vertical(id="list-side"):
                        yield Label("键", id="keys-title", classes="heading")
                        yield OptionList(id="keys", markup=False)
                        with Horizontal(classes="buttons"):
                            yield Button("上一页", id="list-prev", compact=True)
                            yield Button("下一页", id="list-next", compact=True)
                    with Vertical(id="value-side"):
                        with Horizontal(classes="buttons"):
                            yield Input(placeholder="完整键名", id="exact", compact=True)
                            yield Button("打开", id="open-key", compact=True)
                            yield Button("固定", id="pin", compact=True, disabled=True)
                        with VerticalScroll(classes="scroll-body"):
                            yield Static("选择一个键查看内容", id="detail", markup=False)
                        with Horizontal(classes="buttons"):
                            yield Button("上一页", id="value-prev", compact=True)
                            yield Button("下一页", id="value-next", compact=True)
            with TabPane("运行状况", id="health"):
                with VerticalScroll(classes="scroll-body"):
                    yield Static("", id="metrics", markup=False)
            with TabPane("复制", id="replication"):
                with Horizontal(classes="buttons"):
                    yield Button("标记", id="mark", compact=True)
                    yield Button("暂停", id="pause", compact=True)
                    yield Button("恢复", id="resume", compact=True)
                    yield Button("记录", id="activity", compact=True)
                with VerticalScroll(classes="scroll-body"):
                    yield Static("", id="topology", markup=False)
                with Horizontal(id="journal-pages", classes="buttons"):
                    yield Button("上一页", id="journal-prev", compact=True)
                    yield Button("下一页", id="journal-next", compact=True)
                    yield Button("返回状态", id="replica-status", compact=True)
            with TabPane("设置", id="preferences"):
                with VerticalScroll(classes="scroll-body"):
                    yield Static("", id="settings-summary", markup=False)
                    yield Button("修改设置", id="configure", compact=True)
        yield Static("", id="notice", markup=False)
        yield Footer()

    def on_resize(self, event):
        if self.query("#split"):
            self.query_one("#split").set_class(event.size.width < 66, "narrow")

    def on_mount(self):
        self.query_one("#split").set_class(self.size.width < 66, "narrow")
        self.query_one("#journal-pages").display = False
        self.sync_session()
        self.set_interval(1, self.auto_refresh)
        self.start("reload")

    def auto_refresh(self):
        if (self.settings.get("autoRefresh") and self.session and not self.pending
                and self.view == "browse" and time.monotonic() >= self.next_refresh):
            self.start("refresh-page")

    @property
    def view(self):
        return self.query_one("#views", TabbedContent).active

    def action_refresh(self):
        self.start("refresh")

    def action_quit(self):
        if not self.pending:
            self.exit()
            return
        # Let an in-flight connect hand its token to run() for cleanup.
        self.quit_requested = True
        if isinstance(self.screen, (Form, PinList)):
            self.screen.dismiss(None)
        else:
            self.notice("当前操作完成后退出")

    def notice(self, message="", error=False):
        notice = self.query_one("#notice", Static)
        notice.set_class(error, "error")
        notice.update(plain(message))

    def sync_session(self):
        label = "尚未连接" if not self.session else (
            f"{self.profile['name']}  ·  {self.profile['host']}:{self.profile['port']}  ·  DB {self.active_db}")
        self.query_one("#connection", Static).update(" Kerniva   /   " + plain(label))
        self.query_one("#disconnect", Button).disabled = not self.session
        for selector in ("scan", "open-key", "database"):
            self.query_one("#" + selector, Button).disabled = not self.session
        self.query_one("#node", Button).disabled = len(self.info_nodes) <= 1
        self.reset_pages()

    def reset_pages(self):
        self.pages.clear()
        self.key_rows = []
        self.query_one("#keys", OptionList).clear_options()
        self.query_one("#keys-title", Label).update("键")
        self.query_one("#detail", Static).update("选择一个键查看内容")
        self.page_buttons()

    def page_buttons(self):
        self.query_one("#pin", Button).disabled = not self.session or "key" not in self.pages
        for section, prefix in (("scan", "list"), ("key", "value")):
            page = self.pages.get(section)
            self.query_one("#" + prefix + "-prev", Button).disabled = not page or len(page["cursors"]) <= 1
            self.query_one("#" + prefix + "-next", Button).disabled = not page or not page["next"]

    def show_settings(self):
        s = self.settings
        self.query_one("#settings-summary", Static).update(
            f"每页条数    {s['pageSize']}\n预览字节    {s['previewBytes']}\n"
            f"格式化 JSON    {'开' if s['formatJson'] else '关'}\n"
            f"自动刷新    {'开' if s['autoRefresh'] else '关'}\n刷新间隔    {s['refreshSeconds']} 秒")

    async def reload(self):
        state = await asyncio.to_thread(self.backend.state)
        self.saved = state["profiles"]
        self.settings = state["settings"]
        self.replica_available = state["replicationAvailable"]
        options = self.query_one("#profiles", OptionList)
        previous = options.highlighted or 0
        options.clear_options()
        options.add_options([Text(plain(f"{p['name']}    {p['host']}:{p['port']}   "
                                       f"{'Cluster' if p['mode'] == 'cluster' else '单机'} · DB {p['db']}"))
                             for p in self.saved])
        if self.saved:
            options.highlighted = min(previous, len(self.saved) - 1)
        self.query_one("#edit", Button).disabled = not self.saved
        for selector in ("mark", "pause", "resume", "activity"):
            self.query_one("#" + selector, Button).disabled = not self.replica_available
        self.show_settings()

    def changed(self, profile=None, session=None, info=None, db=None):
        self.profile, self.session = profile, session
        self.active_db = db if db is not None else profile["db"] if profile else None
        self.info_nodes = (info or {}).get("nodes", [])
        self.node = ""
        self.sync_session()

    async def call(self, action, body=None):
        if not self.session:
            raise ValueError("请先在「连接」选择数据库。")
        try:
            return await asyncio.to_thread(self.backend.perform, action, {"session": self.session, **(body or {})})
        except Problem as error:
            if error.status == 401:
                self.changed()
            raise

    async def authenticate(self, p):
        body = {"id": p["id"]}
        self.notice("正在连接 " + p["name"])
        if not p.get("username"):
            try:
                result = await asyncio.to_thread(self.backend.perform, "connect", body)
            except Problem as error:
                if "NOAUTH" not in str(error).upper():
                    raise
            else:
                return result
        answer = await self.push_screen_wait(Form("连接 " + plain(p["name"]), [
            ("password", "数据库密码", "", "password")], "连接"))
        if answer is not None:
            return await asyncio.to_thread(self.backend.perform, "connect", {**body, **answer})
        self.notice()
        return None

    async def connect_profile(self, index):
        p = self.saved[index]
        result = await self.authenticate(p)
        if result:
            await self.connected(p, result, p["db"])
            await self.scan()
            self.notice()

    async def connected(self, p, result, db):
        old = self.session
        self.changed(p, result["session"], result["info"], db)
        if old:
            await asyncio.to_thread(self.backend.perform, "disconnect", {"session": old})
        self.query_one("#views", TabbedContent).active = "browse"

    async def pins(self):
        state = await asyncio.to_thread(self.backend.state)
        choice = await self.push_screen_wait(PinList(state))
        if choice is None:
            return
        action, ident = choice
        if action == "remove":
            result = await asyncio.to_thread(self.backend.perform, "pins/remove", {"id": ident})
            self.notice("已移除常用键。" if result.get("removed") else "此常用键已不存在。")
            return
        state = await asyncio.to_thread(self.backend.state)
        pin = next((p for p in state["pins"] if p["id"] == ident), None)
        profile = next((p for p in state["profiles"] if pin and p["id"] == pin["profileId"]), None)
        if not profile:
            raise ValueError("该连接或常用键已删除。")
        body = {"key_b64": pin["key_b64"]}
        if self.profile and self.profile["id"] == profile["id"]:
            try:
                if self.active_db != pin["db"]:
                    result = await self.call("select-db", {"db": pin["db"]})
                    self.active_db = result["db"]
                    self.sync_session()
                await self.page("key", body)
                return
            except Problem as error:
                if error.status != 401:
                    raise
        result = await self.authenticate(profile)
        if result is None:
            return
        session = result["session"]
        try:
            if profile["db"] != pin["db"]:
                await asyncio.to_thread(self.backend.perform, "select-db", {"session": session, "db": pin["db"]})
        except Exception:
            await asyncio.to_thread(self.backend.perform, "disconnect", {"session": session})
            raise
        await self.connected(profile, result, pin["db"])
        await self.page("key", body)
        self.notice()

    async def edit_profile(self, edit):
        index = self.query_one("#profiles", OptionList).highlighted
        p = self.saved[index] if edit and index is not None else {}
        fields = [
            ("name", "名称", p.get("name", ""), "text"),
            ("host", "主机", p.get("host", "127.0.0.1"), "text"),
            ("port", "端口", p.get("port", 6379), "number"),
            ("mode", "拓扑", p.get("mode", "standalone"), [("单机", "standalone"), ("Redis Cluster", "cluster")]),
            ("db", "默认数据库", p.get("db", 0), "number"),
            ("username", "ACL 用户名", p.get("username", ""), "text"),
            ("tls", "TLS", p.get("tls", False), "toggle"),
            ("caFile", "CA 文件", p.get("caFile", ""), "text"),
            ("timeout", "连接超时（秒）", p.get("timeout", 5), "number"),
        ]
        answer = await self.push_screen_wait(Form("编辑连接" if p else "新建连接", fields))
        if answer is not None:
            saved = await asyncio.to_thread(self.backend.perform, "profile/save", {"profile": {**p, **answer}})
            if self.profile and self.profile["id"] == saved["profile"]["id"] and self.profile != saved["profile"]:
                self.changed()
            await self.reload()
            self.query_one("#profiles", OptionList).highlighted = next(
                i for i, profile in enumerate(self.saved) if profile["id"] == saved["profile"]["id"])

    async def scan(self):
        body = {"pattern": self.query_one("#pattern", Input).value}
        if self.node:
            body["node"] = self.node
        await self.page("scan", body)

    async def page(self, kind, request, cursors=None):
        cursors = cursors or [None]
        params = dict(request)
        if cursors[-1]:
            params["cursor"] = cursors[-1]
        if kind == "key" and self.pages.get(kind, {}).get("request") != request:
            self.pages.pop(kind, None)
            self.query_one("#detail", Static).update("正在读取键…")
            self.page_buttons()
        try:
            result = await self.call(kind, params)
        except (Problem, OSError, ValueError) as error:
            if kind == "key":
                self.pages.pop(kind, None)
                self.query_one("#detail", Static).update(plain(str(error)))
                self.page_buttons()
            raise
        self.pages[kind] = {"request": request, "cursors": cursors, "next": result.get("cursor")}
        if kind == "scan":
            self.key_rows = result["items"]
            keys = self.query_one("#keys", OptionList)
            keys.clear_options()
            keys.add_options([Text(plain(row["key"]) or "（空键）") if row.get("key") is not None
                              else Text("Base64 " + row["key_b64"]) for row in self.key_rows])
            title = (f"扫描受限 · {plain(result['unsupported'])}" if result.get("unsupported") else
                     f"键 · 第 {len(cursors)} 页 · {len(self.key_rows)} 项")
            self.query_one("#keys-title", Label).update(Text(title))
        else:
            key = base64.b64decode(request["key_b64"])
            try:
                name = plain(key.decode()) or "（空键）"
            except UnicodeDecodeError:
                name = "Base64 " + request["key_b64"]
            self.query_one("#detail", Static).update(f"{name}   ·   第 {len(cursors)} 页\n" +
                                                     key_details(result, self.settings["formatJson"]))
        self.page_buttons()

    async def select_database(self):
        if self.profile["mode"] == "cluster":
            self.notice("Redis Cluster 使用 DB 0")
            return
        choices = [(f"DB {n}", n) for n in sorted(set(range(16)) | {self.active_db})]
        answer = await self.push_screen_wait(Form("选择逻辑库", [
            ("choice", "数据库", self.active_db, choices),
            ("custom", "其他库号（可选）", "", "text")], "切换"))
        if answer is not None:
            db = int(answer["custom"]) if answer["custom"].strip() else answer["choice"]
            result = await self.call("select-db", {"db": db})
            self.active_db = result["db"]
            self.sync_session()
            await self.scan()

    async def configure(self):
        s = self.settings
        fields = [(key, label, s[key], kind) for key, label, kind in (
            ("pageSize", "每页条数", "number"), ("previewBytes", "预览字节", "number"),
            ("formatJson", "格式化 JSON", "toggle"), ("autoRefresh", "自动刷新", "toggle"),
            ("refreshSeconds", "刷新间隔（秒）", "number"))]
        answer = await self.push_screen_wait(Form("工作台设置", fields))
        if answer is not None:
            result = await asyncio.to_thread(self.backend.perform, "settings", {"settings": answer})
            self.settings = result["settings"]
            self.show_settings()
            self.reset_pages()

    async def replica(self, action):
        target = self.query_one("#topology", Static)
        paging = self.query_one("#journal-pages")
        journal = action in ("activity", "journal-prev", "journal-next")
        paging.display = journal and self.replica_available
        if not self.replica_available:
            target.update("工作台尚未绑定受管复制实例。")
            return
        if journal:
            page = {"activity": 1, "journal-prev": self.activity_page - 1, "journal-next": self.activity_page + 1}[action]
            result = await asyncio.to_thread(self.backend.replication_activity, page)
            self.activity_page = result["page"]
            self.query_one("#journal-prev", Button).disabled = self.activity_page <= 1
            self.query_one("#journal-next", Button).disabled = self.activity_page >= result["pages"]
            rows = [f"第 {result['page']} / {result['pages']} 页", ""]
            for item in result["items"]:
                when = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(item["when"] / 1000))
                rows.append(f"{when}   {item['message']}")
            target.update(plain("\n".join(rows) if result["items"] else "暂无记录"))
            return
        if action in ("mark", "pause", "resume"):
            fields = ([("db", "目标库", 0, [("DB 0", 0), ("DB 1", 1)]), ("value", "标记值", "", "text")]
                      if action == "mark" else [("replica", "目标副本", "b", [("副本 A", "a"), ("副本 B", "b")])])
            label = {"mark": "写入标记", "pause": "暂停副本", "resume": "恢复副本"}[action]
            answer = await self.push_screen_wait(Form(label, fields, label))
            if answer is None:
                return
            response = await asyncio.to_thread(self.backend.replication_action, action, answer)
            self.notice(response.get("warning") or label + "已确认")
        state = await asyncio.to_thread(self.backend.replication_status)
        lines = []
        for name, node in [("主库", state["primary"]), ("来源", state["source"]), *state["replicas"].items()]:
            status = "已暂停" if node.get("paused") else "就绪" if node.get("ready") else "未就绪"
            lines.append(f"{name.upper()}   {status}   :{node.get('port', '—')}")
            for db, row in node.get("dbs", {}).items():
                lines.append(f"  DB {db}    {row.get('sequence', '—')}    {row.get('value', '—')}")
        target.update(plain("\n".join(lines)))

    async def refresh_view(self):
        if self.view in ("connections", "preferences"):
            await self.reload()
        elif self.view == "browse" and self.session:
            for kind in ("scan", "key"):
                if kind in self.pages:
                    page = self.pages[kind]
                    await self.page(kind, page["request"], page["cursors"])
                elif kind == "scan" and "key" not in self.pages:
                    await self.scan()
        elif self.view == "replication":
            await self.replica("status")
        elif self.view == "health":
            target = self.query_one("#metrics", Static)
            target.update("")
            if self.session:
                state = await self.call("status")
                lines = []
                for node in state["nodes"]:
                    lines.append(f"{node['label']}   {node.get('server') or '数据库'}")
                    for key, name in (("role", "角色"), ("keys", "键数"), ("connectedClients", "客户端"),
                                      ("usedMemoryBytes", "内存（字节）"), ("opsPerSecond", "操作/秒"),
                                      ("sequence", "提交序号"), ("error", "错误")):
                        if node.get(key) is not None:
                            lines.append(f"  {name}    {node[key]}")
                    lines.append("")
                target.update(plain("\n".join(lines)))
            else:
                target.update("请先选择连接。")

    @on(Button.Pressed)
    def button(self, event):
        self.start(event.button.id)

    @on(OptionList.OptionSelected)
    def selected(self, event):
        self.start(event.option_list.id, event.option_index)

    @on(Input.Submitted)
    def submitted(self, event):
        self.start("scan" if event.input.id == "pattern" else "open-key")

    @on(TabbedContent.TabActivated)
    def switch(self, event):
        if self.settings and event.pane.id in ("replication", "health", "preferences"):
            self.start("refresh")

    def start(self, action, index=None):
        if self.pending:
            return
        self.pending = True
        self.execute(action, index)

    @work
    async def execute(self, action, index):
        previous = self.view
        try:
            self.notice()
            if action == "reload":
                await self.reload()
                self.query_one("#profiles" if self.saved else "#add").focus()
            elif action == "profiles":
                await self.connect_profile(index)
            elif action in ("refresh", "refresh-page"):
                await self.refresh_view()
            elif action == "scan":
                await self.scan()
            elif action in ("add", "edit"):
                await self.edit_profile(action == "edit")
            elif action == "disconnect":
                await self.call("disconnect")
                self.changed()
            elif action == "database":
                await self.select_database()
            elif action == "pins":
                await self.pins()
            elif action == "pin":
                await self.call("pins/add", {"profileId": self.profile["id"], "db": self.active_db,
                                             "key_b64": self.pages["key"]["request"]["key_b64"]})
                self.notice("已固定到常用键。")
            elif action == "node":
                options = [("所有主节点", "")] + [(Text(plain(n["label"])), n["id"]) for n in self.info_nodes]
                answer = await self.push_screen_wait(Form("扫描节点", [("node", "节点", self.node, options)], "选择"))
                if answer is not None:
                    self.node = answer["node"]
                    await self.scan()
            elif action in ("open-key", "keys"):
                key = (self.key_rows[index]["key_b64"] if action == "keys" else
                       base64.b64encode(self.query_one("#exact", Input).value.encode()).decode())
                await self.page("key", {"key_b64": key})
            elif action in ("list-next", "list-prev", "value-next", "value-prev"):
                kind = "scan" if action.startswith("list") else "key"
                page = self.pages[kind]
                cursors = page["cursors"] + [page["next"]] if action.endswith("next") else page["cursors"][:-1]
                await self.page(kind, page["request"], cursors)
            elif action == "configure":
                await self.configure()
            elif action in ("pause", "resume", "mark", "activity", "journal-prev", "journal-next", "replica-status"):
                await self.replica(action)
        except (Problem, OSError, ValueError) as error:
            self.notice(str(error), error=True)
        finally:
            self.next_refresh = time.monotonic() + self.settings.get("refreshSeconds", 10)
            self.pending = False
            if self.quit_requested:
                self.exit()
            elif self.query("#views") and self.view != previous and self.view in ("health", "replication", "preferences"):
                self.call_later(self.start, "refresh")


def run(backend):
    client = WorkbenchTUI(backend)
    try:
        client.run()
    finally:
        if client.session:
            try:
                backend.perform("disconnect", {"session": client.session})
            except (Problem, OSError) as error:
                print(f"关闭会话时工作台返回错误：{plain(error)}", file=sys.stderr)
    return 0
