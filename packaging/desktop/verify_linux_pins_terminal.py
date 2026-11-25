"""Drive the packaged mouse terminal in a private tmux PTY, inspecting its rendered screen."""

import argparse
import base64
from contextlib import ExitStack
import json
import os
from pathlib import Path
import shlex
import subprocess
import sys
import tempfile
import time
import unicodedata

from verify import database, service

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
from test_workbench_integration import API, require
from workbench_redis import _Client


def columns(text):
    return sum(2 if unicodedata.east_asian_width(char) in ("W", "F") else 1
               for char in text if not unicodedata.combining(char))


class Terminal:
    def __init__(self, root):
        self.socket = root / "tmux.sock"
        self.actions = []
        self.screen = ""

    def run(self, *args, check=True):
        return subprocess.run(["tmux", "-S", str(self.socket), *args], text=True, encoding="utf-8",
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=6, check=check).stdout

    def wait(self, text, line_filter=None):
        deadline = time.monotonic() + 12
        while time.monotonic() < deadline:
            self.screen = self.run("capture-pane", "-p")
            for row, line in enumerate(self.screen.splitlines()):
                if text in line and (not line_filter or line_filter in line):
                    return row + 1, line
            time.sleep(.055)
        raise RuntimeError("Terminal did not display " + text + "\n" + self.screen)

    def click(self, text, line_filter=None):
        # Wait for Textual's button activation and screen transition before the next mouse action.
        time.sleep(.35)
        row, line = self.wait(text, line_filter)
        column = columns(line[:line.index(text)]) + max(1, columns(text) // 2)
        self.run("send-keys", "-l", f"\x1b[<0;{column};{row}M\x1b[<0;{column};{row}m")
        self.actions.append({"label": text, "line": line_filter, "x": column, "y": row})


def verify(bundle, server, evidence):
    with tempfile.TemporaryDirectory(prefix="kerniva-tui-pins-") as tmp, ExitStack() as stack:
        root = Path(tmp)
        initial, port = database(stack, root, server)
        initial.close()
        helper = bundle / "backend/kerniva-workbench-service"
        clean = {k: v for k, v in os.environ.items() if k not in ("PYTHONPATH", "PYTHONHOME", "VIRTUAL_ENV")}
        clean.update(PATH="/usr/bin:/bin", HOME=tmp, XDG_CONFIG_HOME=tmp)
        origin = service(stack, root, helper, root / "profiles.json", clean)
        api = API(origin)
        profile = api("profile/save", {"profile": {"name": "Linux", "host": "127.0.0.1", "port": port,
                                                  "mode": "standalone", "db": 0}})["profile"]
        pid = profile["id"]
        api("settings", {"settings": {"pageSize": 1}})
        token = api("connect", {"id": pid})["session"]
        for db in (0, 1):
            api("select-db", {"session": token, "db": db})
            with _Client({"host": "127.0.0.1", "port": port, "db": db}) as client:
                for key, value in [(b"demo:marker", f"current-DB{db}"), (b"", "empty-live"), (b"\xff", "binary-live")]:
                    client.at(client.seed, "SET", key, value)
                client.at(client.seed, "RPUSH", "list", "head-page", "tail-page")
            for key in ((b"demo:marker",) if db == 0 else (b"demo:marker", b"list", b"", b"\xff")):
                api("pins/add", {"profileId": pid, "session": token, "db": db,
                                 "key_b64": base64.b64encode(key).decode()})
        api("select-db", {"session": token, "db": 0})
        with _Client({"host": "127.0.0.1", "port": port, "db": 1}) as client:
            client.at(client.seed, "SET", "demo:marker", "latest-Linux-DB1")
        terminal = Terminal(root)
        config = root / "tmux.conf"
        config.write_text("set -g status off\nset -g remain-on-exit on\n", encoding="utf-8")
        cmd = ["env", "-u", "PYTHONPATH", "-u", "VIRTUAL_ENV", "-u", "PYTHONHOME", "TERM=xterm-256color",
               "PATH=/usr/bin:/bin", "LANG=C.UTF-8", "HOME=" + tmp, "XDG_CONFIG_HOME=" + tmp,
               str(bundle / "kerniva-tui"), "--url", origin]
        stack.callback(terminal.run, "kill-server", check=False)
        terminal.run("-f", str(config), "new-session", "-d", "-x", "116", "-y", "40", shlex.join(cmd))
        terminal.wait("保存的连接")
        terminal.click("键浏览")
        terminal.click("常用键")
        terminal.wait("Base64")
        (evidence / "terminal-pins.txt").write_text(terminal.screen, encoding="utf-8")
        terminal.click("demo:marker", "DB 1")
        terminal.wait("latest-Linux-DB1")
        (evidence / "terminal-live-db1.txt").write_text(terminal.screen, encoding="utf-8")
        require(api("key", {"session": token, "key_b64": "ZGVtbzptYXJrZXI="})["value"]["text"] == "current-DB0",
                "TUI changed the independently connected DB")
        for label, expected in [("Base64", "binary-live"), ("（空键）", "empty-live"), ("list", "head-page")]:
            terminal.click("常用键")
            terminal.click(label, "DB 1")
            terminal.wait(expected)
        row, line = terminal.wait("下一页")
        # The value-side button is the second “下一页” on the wide layout.
        position = line.rfind("下一页")
        column = columns(line[:position]) + 2
        terminal.run("send-keys", "-l", f"\x1b[<0;{column};{row}M\x1b[<0;{column};{row}m")
        terminal.actions.append({"label": "值 · 下一页", "x": column, "y": row})
        terminal.wait("tail-page")
        (evidence / "terminal-page-2.txt").write_text(terminal.screen, encoding="utf-8")
        terminal.click("常用键")
        terminal.click("移除", "Base64")
        terminal.wait("已移除常用键")
        require(all(pin["key_b64"] != "/w==" for pin in api("state")["pins"]), "Mouse removal did not persist")
        terminal.run("send-keys", "C-q")
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            exit_status = terminal.run("display-message", "-p", "#{pane_dead} #{pane_dead_status}").strip()
            if exit_status == "1 0":
                break
            time.sleep(.08)
        else:
            raise RuntimeError("Packaged terminal did not quit cleanly: " + exit_status)
        return {"passed": True, "launcher": str(bundle / "kerniva-tui"), "terminal": "tmux PTY",
                "mouse": "SGR down/up", "clicks": terminal.actions, "sameNameDBs": [0, 1],
                "newValue": "latest-Linux-DB1", "otherSessionValue": "current-DB0",
                "rawNames": ["", "/w=="], "listPage2": "tail-page", "removed": "/w==", "exit": 0}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bundle-dir", type=Path, required=True)
    parser.add_argument("--server", type=Path, required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    args = parser.parse_args()
    args.evidence.mkdir(parents=True, exist_ok=False)
    try:
        result = verify(args.bundle_dir.resolve(strict=True), args.server.resolve(strict=True), args.evidence)
    except Exception as error:
        result = {"passed": False, "error": str(error)}
    report = json.dumps(result, ensure_ascii=False, indent=2)
    (args.evidence / "result.json").write_text(report + "\n", encoding="utf-8")
    print(report)
    return 0 if result["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
