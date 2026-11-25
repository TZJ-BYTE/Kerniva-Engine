"""Exercise the unpacked GTK workbench on a private X11 display with a real database."""

import argparse
from contextlib import ExitStack, closing
import json
import os
from pathlib import Path
import subprocess
import tempfile
import time

import gi
from Xlib import display, protocol

gi.require_version("Atspi", "2.0")
from gi.repository import Atspi, GLib

from verify import database, seed


def controls(node, depth=0):
    if depth > 35:
        return
    try:
        role, name = node.get_role_name(), node.get_name() or ""
        if not name and role in ("text", "static", "paragraph", "section"):
            try:
                name = Atspi.Text.get_text(node, 0, -1) or ""
            except GLib.Error:
                pass
        yield role, name, node
        for index in range(min(node.get_child_count(), 200)):
            child = node.get_child_at_index(index)
            if child is not None:
                yield from controls(child, depth + 1)
    except GLib.Error:
        # A navigation can replace the document during enumeration.
        return


class Window:
    def __init__(self, process):
        self.process = process
        self.actions = []

    def find(self, label, predicate):
        deadline = time.monotonic() + 12
        desktop = Atspi.get_desktop(0)
        while time.monotonic() < deadline and self.process.poll() is None:
            for index in range(desktop.get_child_count()):
                app = desktop.get_child_at_index(index)
                if app is None or app.get_process_id() != self.process.pid:
                    continue
                for role, name, control in controls(app):
                    if predicate(role, name):
                        return control
            time.sleep(.1)
        raise RuntimeError(f"Control did not appear: {label} (exit {self.process.poll()})")

    def xid(self):
        found = subprocess.check_output(["xdotool", "search", "--onlyvisible", "--pid",
                                         str(self.process.pid)], text=True).splitlines()
        return int(found[-1])

    def click(self, role, name):
        control = self.find(name, lambda r, n: (r, n) == (role, name))
        if not control.get_n_actions() or not control.do_action(0):
            raise RuntimeError(f"Could not activate {name}")
        self.actions.append("click " + name)

    def fill(self, label, value):
        control = self.find(label, lambda r, n: r in ("entry", "spin button") and n.startswith(label))
        subprocess.run(["xdotool", "windowfocus", "--sync", str(self.xid())], check=True, timeout=5)
        if not Atspi.Component.grab_focus(control):
            raise RuntimeError(f"Could not focus {label}")
        subprocess.run(["xdotool", "key", "--clearmodifiers", "ctrl+a"], check=True, timeout=5)
        subprocess.run(["xdotool", "type", "--clearmodifiers", "--delay", "9", str(value)],
                       check=True, timeout=5)
        self.actions.append("fill " + label)

    def close(self):
        with closing(display.Display()) as connection:
            window = connection.create_resource_object("window", self.xid())
            event = protocol.event.ClientMessage(window=window, client_type=connection.intern_atom("WM_PROTOCOLS"),
                        data=(32, [connection.intern_atom("WM_DELETE_WINDOW"), 0, 0, 0, 0]))
            window.send_event(event)
            connection.sync()
        if self.process.wait(timeout=12) != 0:
            raise RuntimeError(f"Native window exited with {self.process.returncode}")


def verify(binary, server, log, root):
    key, expected = "native:中文", "界面读值-826"
    with ExitStack() as stack:
        connection, port = database(stack, root, server)
        seed(connection, key.encode(), expected.encode())
        config = root / "native-settings.json"
        start = time.monotonic()
        proc = subprocess.Popen([str(binary), "--config", str(config)], cwd=root,
                                stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT)
        try:
            window = Window(proc)
            window.click("link", "新建连接")
            ready_ms = round((time.monotonic() - start) * 1000)
            window.fill("连接名称", "Linux GUI acceptance")
            window.fill("主机", "127.0.0.1")
            window.fill("端口", port)
            window.fill("默认逻辑库", 1)
            window.click("button", "保存配置")
            window.click("button", "连接")
            window.click("link", "浏览数据")
            window.click("button", "扫描")
            window.click("link", key)
            window.find(expected, lambda role, name: expected in name)
            window.actions.append("read " + expected)
            children = []
            for path in Path(f"/proc/{proc.pid}/task").glob("*/children"):
                for pid in path.read_text().split():
                    child = Path("/proc") / pid
                    if (child / "cmdline").exists() and os.fsencode(config) in (child / "cmdline").read_bytes().split(b"\0"):
                        children.append(child)
            if not children:
                raise RuntimeError("Window has no management service for its private config")
            rss = next(line.split()[1] for line in Path(f"/proc/{proc.pid}/status").read_text().splitlines()
                       if line.startswith("VmRSS:"))
            window.close()
            deadline = time.monotonic() + 8
            while any(child.exists() for child in children) and time.monotonic() < deadline:
                time.sleep(.1)
            if any(child.exists() for child in children):
                raise RuntimeError("Owned service remained alive after WM_DELETE_WINDOW")
            return {"passed": True, "ready_ms": ready_ms, "desktop_process_rss_kib": int(rss),
                    "db": 1, "key": key, "observed_value": expected, "actions": window.actions,
                    "window_exit": proc.returncode, "owned_services_closed": len(children)}
        finally:
            if proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=6)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bundle-dir", required=True, type=Path)
    parser.add_argument("--server", required=True, type=Path)
    parser.add_argument("--report", required=True, type=Path)
    args = parser.parse_args()
    binary = (args.bundle_dir / "kerniva-workbench").resolve(strict=True)
    args.report.parent.mkdir(parents=True, exist_ok=True)
    Atspi.init()
    with tempfile.TemporaryDirectory() as root, args.report.with_suffix(".log").open("wb") as log:
        result = verify(binary, args.server.resolve(strict=True), log, Path(root))
    encoded = json.dumps(result, ensure_ascii=False, indent=2) + "\n"
    args.report.write_text(encoded, encoding="utf-8")
    print(encoded, end="")


if __name__ == "__main__":
    main()
