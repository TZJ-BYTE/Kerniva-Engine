"""Interactive terminal for the shared database workbench."""

import argparse
import base64
import getpass
import json
import re
import shlex
import sys
import unicodedata
import warnings


COMMANDS = {
    "/help": "列出工作台命令",
    "/profiles": "列出已保存的连接",
    "/add": '添加连接。/add "名称" 主机 [--port 6379] [--db 0] [--mode cluster] [--username 用户] [--tls] [--ca-file 路径] [--timeout 5]',
    "/connect": "选择已保存的连接。/connect [--ask-password] [编号、名称或 ID]",
    "/disconnect": "断开当前连接",
    "/status": "显示当前连接和数据库状态",
    "/scan": "扫描键。/scan [匹配模式]",
    "/next": "查看下一页键或值",
    "/prev": "查看上一页键或值",
    "/get": "输入 /get 完整键名，键名可含空格。单独执行 /get 读取空键",
    "/get-b64": "输入 /get-b64 Base64，读取二进制键",
    "/db": "切换当前会话的逻辑库。/db 库号",
    "/pin": "固定当前打开的键",
    "/pins": "列出常用键及其 ID",
    "/pin-open": "按 ID 打开常用键。/pin-open ID",
    "/unpin": "按 ID 移除常用键。/unpin ID",
    "/settings": "显示或保存设置。/settings [pageSize|previewBytes|formatJson|autoRefresh|refreshSeconds 值]",
    "/replica": "复制演示。/replica status | activity [页码] | pause a | resume b | mark <db> <value>",
    "/exit": "退出工作台",
}
BOOLEAN_SETTINGS = {"formatJson", "autoRefresh"}
INTEGER_SETTINGS = {"pageSize", "previewBytes", "refreshSeconds"}
JSON_TOKENS = re.compile(r'"(?:[^"\\]|\\.)*"|[^\s{}\[\],:]+|[{}\[\],:]')


class ShellError(ValueError):
    """A command could not be parsed or completed."""


class CommandParser(argparse.ArgumentParser):
    def error(self, message):
        raise ShellError(f"{message}。用法：{COMMANDS['/add']}")


def _split_command(line):
    parts = re.split(r"[ \t]", line.lstrip(), maxsplit=1)
    return parts[0], parts[1] if len(parts) == 2 else ""


def _display_text(value):
    if value is None:
        return False
    return all(char in "\n\r\t" or not unicodedata.category(char).startswith("C") for char in value)


def _json(data):
    raw = json.dumps(data, ensure_ascii=False, indent=2)
    return "".join(char if _display_text(char) else ascii(char)[1:-1] for char in raw)


def pin_key_label(encoded):
    raw = base64.b64decode(encoded, validate=True)
    if not raw:
        return "（空键）"
    try:
        label = raw.decode("utf-8")
    except UnicodeDecodeError:
        return "Base64 " + encoded
    return label if label.isprintable() else "Base64 " + encoded


def _format_json(raw):
    try:
        parsed = json.loads(raw, parse_int=str, parse_float=str)
    except (ValueError, RecursionError):
        return raw
    if not isinstance(parsed, (list, dict)):
        return raw
    tokens = JSON_TOKENS.findall(raw)
    output = []
    depth = 0
    for index, token in enumerate(tokens):
        if token in ("}", "]"):
            depth -= 1
            if tokens[index - 1] not in ("{", "["):
                output.append("\n" + "  " * min(depth, 100))
        output.append(": " if token == ":" else token)
        if token in ("{", "["):
            depth += 1
        if token == "," or (token in ("{", "[") and tokens[index + 1] not in ("}", "]")):
            output.append("\n" + "  " * min(depth, 100))
    return "".join(output)


def _password():
    with warnings.catch_warnings():
        warnings.simplefilter("error", getpass.GetPassWarning)
        try:
            return getpass.getpass("数据库密码（不回显）: ")
        except (getpass.GetPassWarning, EOFError, OSError) as error:
            raise ShellError("无法安全读取密码，请从交互式终端运行。") from error


class Shell:
    def __init__(self, app, stdout, password_prompt, choose=None):
        self.app = app
        self.stdout = stdout
        self.password_prompt = password_prompt
        self.choose = choose
        self.session = None
        self.profile = None
        self.active_db = None
        self.browse = None

    def write(self, value):
        print(value, file=self.stdout, flush=True)

    def show_status(self, replica=False):
        state = self.app.replication_status() if replica else self.perform("status")
        if not self.stdout.isatty():
            self.write(_json(state))
            return
        if replica:
            for name, node in [("主库", state["primary"]), ("来源", state["source"]),
                               *[(f"副本 {k.upper()}", v) for k, v in state["replicas"].items()]]:
                health = "已暂停" if node.get("paused") else "就绪" if node.get("ready") else "未就绪"
                positions = "  ".join(f"DB {db}={reading.get('sequence')}" for db, reading in node["dbs"].items()
                                      if reading.get("sequence") is not None)
                self.write(f"{name}  {health}  port={node['port']}  {positions}")
            return
        self.write(f"{state.get('server') or '数据库'} · DB {state['db']}")
        for node in state["nodes"]:
            cells = [_json(node["label"])]
            for field, title in [("role", "角色"), ("keys", "键"), ("usedMemoryBytes", "字节"),
                                 ("connectedClients", "客户端"), ("opsPerSecond", "操作/秒"), ("error", "错误")]:
                if node.get(field) is not None:
                    cells.append(f"{title}={_json(node[field])}")
            self.write("  ".join(cells))

    def profiles(self):
        return self.app.state()["profiles"]

    def show_profiles(self, profiles):
        for number, profile in enumerate(profiles, 1):
            self.write(f"#{number}  {_json(profile['name'])}  {_json(profile['host'])}:{profile['port']}"
                       f"  DB {profile['db']}  {profile['id']}")
        if not profiles:
            self.write("还没有连接，可以用 /add 添加。")

    def profile_named(self, name, profiles):
        match = next((item for item in profiles if item["id"] == name), None)
        if match is not None:
            return match
        index = re.fullmatch(r"(#?)([0-9]+)", name)
        matches = [] if index and index[1] else [item for item in profiles if item["name"] == name]
        if len(matches) > 1:
            raise ShellError("存在同名连接，请使用 /profiles 中的编号或 ID。")
        if matches:
            return matches[0]
        if index:
            number = int(index[2])
            if 1 <= number <= len(profiles):
                return profiles[number - 1]
            raise ShellError("连接编号超出范围，请运行 /profiles。")
        raise ShellError("未找到连接，请运行 /profiles 查看连接，或运行 /add 添加连接。")

    def add(self, argument):
        parser = CommandParser(prog="/add", add_help=False, allow_abbrev=False)
        parser.add_argument("name")
        parser.add_argument("host")
        parser.add_argument("--port", type=int, default=6379)
        parser.add_argument("--mode", choices=("standalone", "cluster"), default="standalone")
        parser.add_argument("--db", type=int, default=0)
        parser.add_argument("--username", default="")
        parser.add_argument("--tls", action="store_true")
        parser.add_argument("--ca-file", dest="caFile", default="")
        parser.add_argument("--timeout", type=int, default=5)
        profile = vars(parser.parse_args(shlex.split(argument)))
        saved = self.app.perform("profile/save", {"profile": profile})["profile"]
        self.write(f"已添加连接 {_json(saved['name'])}（{saved['id']}）")

    def connect(self, argument):
        argument = argument.strip()
        option, name = _split_command(argument)
        ask_password = option == "--ask-password"
        if ask_password:
            argument = name.strip()
        profiles = self.profiles()
        if not argument:
            self.show_profiles(profiles)
            if not profiles:
                return
            if self.choose is None:
                self.write("运行 /connect #编号、/connect 名称或 /connect ID。")
                return
            argument = self.choose("连接编号、名称或 ID（回车取消）> ").strip()
            if not argument:
                return
            if re.fullmatch(r"[0-9]+", argument):
                argument = "#" + argument
        profile = self.profile_named(argument, profiles)
        result = self.authenticate(profile, ask_password)
        self.use_session(profile, result["session"], profile["db"])

    def authenticate(self, profile, ask_password=False):
        body = {"id": profile["id"]}
        if ask_password or profile.get("username"):
            body["password"] = self.password_prompt()
        try:
            return self.app.perform("connect", body)
        except Exception as error:
            if "password" in body or "NOAUTH" not in str(error).upper():
                raise
            self.write("数据库要求认证。")
            return self.app.perform("connect", {**body, "password": self.password_prompt()})

    def use_session(self, profile, session, db):
        old = self.session
        self.session = session
        self.profile = profile
        self.active_db = db
        self.browse = None
        self.write(f"已连接 {_json(profile['name'])}（{profile['host']}:{profile['port']}，DB {db}）")
        if old is not None and old != self.session:
            self.app.perform("disconnect", {"session": old})

    def disconnect(self):
        if self.session is not None:
            self.app.perform("disconnect", {"session": self.session})
        self.session = None
        self.profile = None
        self.active_db = None
        self.browse = None

    def perform(self, action, body=None):
        if self.session is None:
            raise ShellError("当前未连接，请运行 /connect。")
        try:
            return self.app.perform(action, {"session": self.session, **(body or {})})
        except Exception as error:
            if getattr(error, "status", None) == 401:
                self.session = None
                self.profile = None
                self.active_db = None
                self.browse = None
            raise

    def render_page(self, action, data, index):
        self.write(f"第 {index + 1} 页")
        if data.get("unsupported"):
            self.write("读取受限 " + _json(data["unsupported"]))
            return
        if action == "scan":
            if data.get("node"):
                self.write("节点 " + _json(data["node"]))
            if not data["items"]:
                self.write("此页没有匹配键。")
            for item in data["items"]:
                if _display_text(item["key"]) and item["key"].isprintable():
                    self.write(_json(item["key"]))
                else:
                    self.write("/get-b64 " + item["key_b64"] if item["key_b64"] else '""')
            return
        if data.get("type") == "none":
            self.write("键不存在或已过期。")
            return
        self.write(f"类型 {data['type']}，TTL（毫秒） {_json(data.get('ttl_ms'))}")
        for item in data["items"]:
            self.write(_json(item))
        value = data.get("value")
        if value is not None:
            self.write(f"字节偏移 {data['offset']}，总长度 {data['length']}")
            content = value["text"]
            if not _display_text(content) or "\r" in content:
                self.write("Base64 " + value["b64"])
            elif content:
                if not value["truncated"] and self.app.state()["settings"]["formatJson"]:
                    content = _format_json(content)
                self.write(content)
            else:
                self.write("（空值）")
            if value["truncated"]:
                self.write("还有后续字节，运行 /next 读取。")

    def start_page(self, action, body):
        self.browse = None
        data = self.perform(action, body)
        self.render_page(action, data, 0)
        self.browse = (action, body, [None], 0, data.get("cursor"))

    def select_db(self, db):
        result = self.perform("select-db", {"db": db})
        self.active_db = result["db"]
        self.browse = None
        self.write(f"当前逻辑库 DB {self.active_db}")

    def list_pins(self):
        state = self.app.state()
        names = {p["id"]: p["name"] for p in state["profiles"]}
        for pin in state["pins"]:
            name = names.get(pin["profileId"], "连接已删除")
            self.write(f"{pin['id']}  {_json(name)}  DB {pin['db']}  {_json(pin_key_label(pin['key_b64']))}")
        if not state["pins"]:
            self.write("没有常用键。打开键后可用 /pin 固定。")

    def open_pin(self, ident):
        state = self.app.state()
        pin = next((p for p in state["pins"] if p["id"] == ident), None)
        profile = next((p for p in state["profiles"] if pin and p["id"] == pin["profileId"]), None)
        if not profile:
            raise ShellError("该连接或常用键已删除。")
        body = {"key_b64": pin["key_b64"]}
        if self.profile and self.profile["id"] == profile["id"]:
            try:
                if self.active_db != pin["db"]:
                    self.select_db(pin["db"])
                self.start_page("key", body)
                return
            except Exception as error:
                if getattr(error, "status", None) != 401:
                    raise
        session = self.authenticate(profile)["session"]
        try:
            if pin["db"] != profile["db"]:
                self.app.perform("select-db", {"session": session, "db": pin["db"]})
        except Exception:
            self.app.perform("disconnect", {"session": session})
            raise
        self.use_session(profile, session, pin["db"])
        self.start_page("key", body)

    def turn_page(self, step):
        if self.browse is None:
            raise ShellError("请先运行 /scan 或 /get。")
        action, body, cursors, index, following = self.browse
        if step > 0:
            if not following:
                raise ShellError("已到最后一页。")
            index += 1
            cursors = cursors[:index] + [following]
        else:
            if index == 0:
                raise ShellError("已到第一页。")
            index -= 1
        cursor = cursors[index]
        request = {**body, "cursor": cursor} if cursor is not None else body
        data = self.perform(action, request)
        self.render_page(action, data, index)
        self.browse = (action, body, cursors, index, data.get("cursor"))

    def settings(self, argument):
        if not argument.strip():
            self.write(_json(self.app.state()["settings"]))
            self.write("自动刷新和刷新间隔应用于图形界面。")
            return
        name, raw = _split_command(argument)
        raw = raw.strip()
        if not raw:
            raise ShellError(COMMANDS["/settings"])
        if name in BOOLEAN_SETTINGS and raw.lower() in ("true", "false", "on", "off"):
            value = raw.lower() in ("true", "on")
        elif name in INTEGER_SETTINGS:
            value = int(raw)
        else:
            raise ShellError(COMMANDS["/settings"] + "，布尔值使用 on/off 或 true/false。")
        result = self.app.perform("settings", {"settings": {name: value}})
        if name in {"pageSize", "previewBytes"}:
            self.browse = None
        self.write(_json(result["settings"]))

    def replica(self, argument):
        command, tail = _split_command(argument)
        if command == "status" and not tail.strip():
            self.show_status(replica=True)
        elif command == "activity":
            self.write(_json(self.app.replication_activity(int(tail.strip() or "1"))))
        elif command in ("pause", "resume") and tail.strip().lower() in ("a", "b"):
            self.write(_json(self.app.replication_action(command, {"replica": tail.strip().lower()})))
        elif command == "mark":
            db, value = _split_command(tail)
            if not value:
                raise ShellError(COMMANDS["/replica"])
            self.write(_json(self.app.replication_action("mark", {"db": int(db), "value": value})))
        else:
            raise ShellError(COMMANDS["/replica"])

    def execute(self, command, argument):
        if command not in COMMANDS:
            raise ShellError("未知命令，请运行 /help。")
        if command in {"/help", "/profiles", "/disconnect", "/status", "/next", "/prev", "/exit",
                       "/pins", "/pin"} and argument.strip():
            raise ShellError(COMMANDS[command])
        if command == "/help":
            for name, guide in COMMANDS.items():
                self.write(f"{name:<13} {guide}")
        elif command == "/profiles":
            profiles = self.profiles()
            if self.stdout.isatty():
                self.show_profiles(profiles)
            else:
                self.write(_json(profiles))
        elif command == "/add":
            self.add(argument)
        elif command == "/connect":
            self.connect(argument)
        elif command == "/disconnect":
            self.disconnect()
            self.write("当前未连接。")
        elif command == "/status":
            if self.session is None:
                self.write("当前未连接。")
            else:
                self.show_status()
        elif command == "/scan":
            self.start_page("scan", {"pattern": argument or "*"})
        elif command in ("/next", "/prev"):
            self.turn_page(1 if command == "/next" else -1)
        elif command in ("/get", "/get-b64"):
            key = argument if command == "/get-b64" else base64.b64encode(argument.encode()).decode()
            self.start_page("key", {"key_b64": key})
        elif command == "/db":
            if not re.fullmatch(r"[0-9]{1,5}", argument.strip()):
                raise ShellError(COMMANDS[command])
            self.select_db(int(argument))
        elif command == "/pins":
            self.list_pins()
        elif command == "/pin":
            if not self.browse or self.browse[0] != "key":
                raise ShellError("请先用 /get 或 /get-b64 打开要固定的键。")
            result = self.perform("pins/add", {"profileId": self.profile["id"], "db": self.active_db,
                                               "key_b64": self.browse[1]["key_b64"]})
            self.write("已固定 " + result["pin"]["id"])
        elif command in ("/pin-open", "/unpin"):
            ident = argument.strip()
            if not ident:
                raise ShellError(COMMANDS[command])
            if command == "/pin-open":
                self.open_pin(ident)
            else:
                result = self.app.perform("pins/remove", {"id": ident})
                self.write("已移除常用键。" if result.get("removed") else "未找到此常用键。")
        elif command == "/settings":
            self.settings(argument)
        elif command == "/replica":
            self.replica(argument)
        elif command == "/exit":
            return False
        return True


def _readline():
    try:
        import readline
    except ImportError:
        return None
    readline.set_auto_history(False)
    def complete(prefix, index):
        matches = [name for name in COMMANDS if name.startswith(prefix)]
        return matches[index] if index < len(matches) else None

    readline.set_completer(complete)
    readline.set_completer_delims(" \t")
    readline.parse_and_bind("bind ^I rl_complete" if "libedit" in (readline.__doc__ or "") else "tab: complete")
    return readline


def run(app, *, stdin=None, stdout=None, password_prompt=None):
    """Run until /exit or EOF. Stream and password overrides support scripted checks."""
    stdin = sys.stdin if stdin is None else stdin
    stdout = sys.stdout if stdout is None else stdout
    interactive = stdin.isatty() and stdout.isatty()
    native_prompt = stdin is sys.stdin and stdout is sys.stdout and interactive
    readline = _readline() if native_prompt else None

    def read_line(prompt):
        if native_prompt:
            return input(prompt)
        if interactive:
            stdout.write(prompt)
            stdout.flush()
        line = stdin.readline()
        if not line:
            raise EOFError
        return line.removesuffix("\n").removesuffix("\r")

    shell = Shell(app, stdout, _password if password_prompt is None else password_prompt,
                  read_line if interactive else None)
    shell.write("Kerniva 工作台 · /help 查看命令 · /exit 退出")
    try:
        while True:
            try:
                name = shell.profile["name"] if shell.profile else None
                prompt = f"kerniva[{_json(name)}]> " if name is not None else "kerniva> "
                line = read_line(prompt)
                command, argument = _split_command(line)
                if not command:
                    continue
                if not shell.execute(command, argument):
                    break
                if readline is not None and command not in ("/connect", "/replica"):
                    if not readline.get_current_history_length() or readline.get_history_item(
                            readline.get_current_history_length()) != line:
                        readline.add_history(line)
            except EOFError:
                break
            except KeyboardInterrupt:
                shell.write("\n已取消当前命令。")
            except Exception as error:
                if not isinstance(error, (ValueError, OSError)) and not hasattr(error, "status"):
                    raise
                shell.write("错误 " + _json(str(error)))
    finally:
        try:
            shell.disconnect()
        except Exception as error:
            if not isinstance(error, (ValueError, OSError)) and not hasattr(error, "status"):
                raise
            shell.write("断开连接时出错 " + _json(str(error)))
    return 0
