"""Run mouse/keyboard flows against an isolated workbench HTTP API and native database."""

import asyncio
from contextlib import ExitStack
import importlib.util
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest

from workbench_operations import OnlineShell, Problem
from workbench_redis import _Client

try:
    from workbench_tui import WorkbenchTUI
    from textual.widgets import Button, Input, OptionList, Select, Static, TabbedContent
except ModuleNotFoundError as error:
    if error.name != "textual":
        raise
    WorkbenchTUI = None


HERE = Path(__file__).resolve().parent
SERVER = HERE.parent / "bin" / ("kerniva-server.exe" if os.name == "nt" else "kerniva-server")


class Demo:
    label = "测试拓扑"

    def __init__(self):
        self.paused = False
        self.marker = ""

    def status(self):
        node = {"ready": True, "port": 1234,
                "dbs": {"1": {"source": "test-source", "sequence": 3, "value": self.marker}}}
        return {"primary": node, "source": node, "replicas": {"a": node, "b": {**node, "paused": self.paused}}}

    def change(self, path, data):
        if path == "marker":
            self.marker = data["value"]
        elif path == "replicas/b/pause":
            self.paused = True
        elif path == "replicas/b/resume":
            self.paused = False
        else:
            raise AssertionError(path)
        return {"ok": True}


@unittest.skipUnless(WorkbenchTUI and SERVER.is_file(), "install the TUI dependency and build kerniva-server")
class WorkbenchTerminalTests(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        stack = ExitStack()
        cls.addClassCleanup(stack.close)
        root = Path(stack.enter_context(tempfile.TemporaryDirectory()))
        cls.root = root
        with socket.socket() as reservation:
            reservation.bind(("127.0.0.1", 0))
            db_port = reservation.getsockname()[1]
        (root / "db.yaml").write_text(
            f'host: "127.0.0.1"\nport: {db_port}\nnetwork_type: "std"\ndb_count: 2\n'
            'persistence_enabled: false\nlog_level: "info"\nlog_path: "native.log"\n', encoding="utf-8")
        log = stack.enter_context((root / "server.log").open("wb"))
        process = subprocess.Popen([str(SERVER), "-config", "db.yaml", "-stop-file", "server.stop"],
                                   cwd=root, stdout=log, stderr=subprocess.STDOUT)

        def stop():
            (root / "server.stop").touch()
            try:
                process.wait(timeout=8)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()

        stack.callback(stop)
        until = time.monotonic() + 10
        while True:
            try:
                with socket.create_connection(("127.0.0.1", db_port), timeout=.2):
                    break
            except OSError:
                if process.poll() is not None or time.monotonic() >= until:
                    raise RuntimeError((root / "server.log").read_text(errors="replace"))
                time.sleep(.05)
        for db in (0, 1):
            with _Client({"port": db_port, "db": db}) as client:
                client.at(client.seed, "SET", "product", f"catalog-{db}")
                client.at(client.seed, "HSET", "book", "title", "Kerniva")
                client.at(client.seed, "RPUSH", "letters", "A", "B", "C")
        spec = importlib.util.spec_from_file_location("workbench_http", HERE / "kerniva-workbench.py")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        cls.demo = Demo()
        cls.service = module.Workbench(module.Config(root / "workbench.json"), managed=cls.demo)
        server = stack.enter_context(module.Server(("127.0.0.1", 0), module.Handler))
        server.app = cls.service
        cls.url = f"http://127.0.0.1:{server.server_port}"
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        stack.callback(thread.join)
        stack.callback(server.shutdown)
        cls.backend = OnlineShell(cls.url)
        cls.pid = cls.backend.perform("profile/save", {"profile": {
            "name": "Terminal QA", "host": "127.0.0.1", "port": db_port}})["profile"]["id"]

    async def test_click_connect_scan_page_switch_db_operate_and_resize(self):
        self.backend.perform("settings", {"settings": {"pageSize": 1}})
        tui = WorkbenchTUI(self.backend)
        async with tui.run_test(size=(96, 32)) as pilot:
            async def ready():
                for _ in range(120):
                    await pilot.pause(.02)
                    if not tui.pending:
                        return
                self.fail("UI operation did not complete")

            async def click(selector, offset=(1, 0)):
                widget = tui.screen.query_one(selector)
                widget.scroll_visible(animate=False)
                await pilot.pause()
                await pilot.click(selector, offset=offset)
                await pilot.pause()

            await ready()
            await click("#profiles", (3, 1))
            await ready()
            self.assertEqual(tui.view, "browse")
            self.assertEqual(tui.active_db, 0)
            visited = []
            while True:
                visited.extend(row["key"] for row in tui.key_rows)
                if tui.query_one("#list-next", Button).disabled:
                    break
                await click("#list-next")
                await ready()
            self.assertEqual(set(visited), {"letters", "book", "product"})

            tui.query_one("#pattern", Input).value = "product"
            await click("#scan")
            await ready()
            await click("#keys", (3, 1))
            await ready()
            self.assertIn("catalog-0", str(tui.query_one("#detail", Static).content))
            await click("#database")
            await click("#choice")
            overlay = tui.screen.query_one("#choice SelectOverlay")
            await pilot.click(offset=(overlay.content_region.x + 2, overlay.content_region.y + 1))
            await pilot.pause()
            self.assertEqual(tui.screen.query_one("#choice", Select).value, 1)
            await click("#accept")
            await ready()
            self.assertEqual(tui.active_db, 1)
            await click("#keys", (3, 1))
            await ready()
            self.assertIn("catalog-1", str(tui.query_one("#detail", Static).content))
            self.assertEqual(self.backend.state()["profiles"][0]["db"], 0)
            other = self.backend.perform("connect", {"id": self.pid})["session"]
            try:
                self.assertEqual(self.backend.perform("status", {"session": other})["db"], 0)
                self.assertEqual(self.backend.perform("status", {"session": tui.session})["db"], 1)
            finally:
                self.backend.perform("disconnect", {"session": other})
            with self.assertRaises(Problem):
                self.backend.perform("select-db", {"session": tui.session, "db": 100})
            self.assertEqual(self.backend.perform("status", {"session": tui.session})["db"], 1)

            tui.query_one("#exact", Input).value = "letters"
            await click("#open-key")
            await ready()
            self.assertIn("A", str(tui.query_one("#detail", Static).content))
            await click("#value-next")
            await ready()
            self.assertIn("B", str(tui.query_one("#detail", Static).content))
            await click("#value-prev")
            await ready()
            self.assertIn("A", str(tui.query_one("#detail", Static).content))
            tui.query_one("#exact", Input).value = "book"
            tui.query_one("#exact").focus()
            await pilot.press("enter")
            await ready()
            self.assertIn("title   Kerniva", str(tui.query_one("#detail", Static).content))
            await pilot.resize_terminal(49, 26)
            await pilot.pause()
            self.assertTrue(tui.query_one("#split").has_class("narrow"))
            await pilot.resize_terminal(96, 32)
            await pilot.pause()
            await click("#--content-tab-health")
            await ready()
            self.assertIn("键数", str(tui.query_one("#metrics", Static).content))

            await click("#--content-tab-replication")
            await ready()
            await click("#pause")
            await click("#accept")
            await ready()
            self.assertTrue(self.demo.paused)
            await click("#resume")
            await click("#accept")
            await ready()
            self.assertFalse(self.demo.paused)
            await click("#mark")
            tui.screen.query_one("#value", Input).value = "terminal-marker"
            await click("#accept")
            await ready()
            self.assertEqual(self.demo.marker, "terminal-marker")
            await click("#activity")
            await ready()
            self.assertIn("terminal-marker", str(tui.query_one("#topology", Static).content))
            await click("#--content-tab-preferences")
            await ready()
            await click("#configure")
            tui.screen.query_one("#pageSize", Input).value = "7"
            await click("#accept")
            await ready()
            self.assertEqual(self.backend.state()["settings"]["pageSize"], 7)
            await click("#--content-tab-connections")
            await click("#disconnect")
            await ready()
            self.assertIsNone(tui.session)
            self.assertEqual(len(self.service.sessions), 0)

    @unittest.skipUnless(os.name == "nt" and importlib.util.find_spec("winpty"), "optional Windows ConPTY smoke (pywinpty)")
    async def test_windows_conpty_mouse_read_and_console_restoration(self):
        from winpty import Backend, PtyProcess

        self.backend.perform("settings", {"settings": {"pageSize": 50}})
        # Derive coordinates from the same terminal dimensions as the real child.
        reference = WorkbenchTUI(self.backend)
        async with reference.run_test(size=(96, 32)) as pilot:
            await pilot.pause()
            profiles = reference.query_one("#profiles").content_region
            reference.query_one("#views", TabbedContent).active = "browse"
            await pilot.pause()
            keys = reference.query_one("#keys").content_region

        probe = self.root / "console_probe.py"
        probe.write_text(
            "import ctypes, json, subprocess, sys\nfrom pathlib import Path\n"
            "from ctypes import wintypes\n"
            "k = ctypes.windll.kernel32\n"
            "def modes():\n"
            "    values = []\n"
            "    for stream in (-10, -11):\n"
            "        mode = wintypes.DWORD()\n"
            "        assert k.GetConsoleMode(k.GetStdHandle(stream), ctypes.byref(mode))\n"
            "        values.append(mode.value)\n"
            "    return values\n"
            "receipt = Path(__file__).with_suffix('.json')\n"
            "if sys.argv[1] == 'before':\n"
            "    receipt.write_text(json.dumps(modes()))\n"
            "    sys.exit(0)\n"
            "if sys.argv[1] == 'after':\n"
            "    before = json.loads(receipt.read_text()); rc = int(sys.argv[2])\n"
            "else:\n"
            "    before = modes(); rc = subprocess.run(sys.argv[1:]).returncode\n"
            "print('KERNIVA_CONSOLE_RESTORED' if rc == 0 and modes() == before else "
            "f'CONSOLE_FAILURE {rc} {before} {modes()}', flush=True)\n", encoding="utf-8")
        launcher = os.environ.get("KERNIVA_TUI_LAUNCHER")
        if launcher:
            # Measure on both sides of CALL in one interactive shell. Starting cmd.exe
            # itself changes ENABLE_MOUSE_INPUT, even for an empty batch file.
            batch = self.root / "run-terminal.cmd"
            batch.write_text('@echo off\n"%QA_PYTHON%" console_probe.py before\n'
                             'call "%KERNIVA_TUI_LAUNCHER%" --url "%QA_URL%"\n'
                             '"%QA_PYTHON%" console_probe.py after %errorlevel%\n', encoding="ascii")
            command = [os.environ["ComSpec"], "/d", "/c", batch.name]
        else:
            program = os.environ.get("KERNIVA_TUI_EXECUTABLE")
            entrypoint = ([program] if program else [sys.executable, "-X", "utf8", "-B",
                                                     str(HERE / "kerniva-workbench.py")]) + ["--tui"]
            command = [sys.executable, str(probe), *entrypoint, "--url", self.url]
        proc = PtyProcess.spawn(command, cwd=str(self.root),
                                env={**os.environ, "QA_PYTHON": sys.executable, "QA_URL": self.url},
                                dimensions=(32, 96), backend=Backend.ConPTY)
        chunks = []

        def reader():
            try:
                while True:
                    chunks.append(proc.read(16384))
            except (EOFError, OSError):
                pass

        reader_thread = threading.Thread(target=reader, daemon=True)
        reader_thread.start()

        async def wait_for(condition, description):
            for _ in range(150):
                if condition():
                    return
                await asyncio.sleep(.1)
            self.fail(f"{description}. Last terminal output {ascii(''.join(chunks)[-1600:])}")

        async def click(x, y):
            proc.write(f"\x1b[<0;{x + 1};{y + 1}M")
            await asyncio.sleep(.08)
            proc.write(f"\x1b[<0;{x + 1};{y + 1}m")

        try:
            await wait_for(lambda: "Terminal QA" in "".join(chunks), "Profile did not render")
            await click(profiles.x + 2, profiles.y)
            await wait_for(lambda: len(self.service.sessions) == 1, "Physical click did not connect")
            token = next(iter(self.service.sessions))
            result = self.backend.perform("scan", {"session": token, "pattern": "*"})
            names = [row["key"] for row in result["items"]]
            await wait_for(lambda: "product" in "".join(chunks), "Keys did not render")
            await click(keys.x + 2, keys.y + names.index("product"))
            await wait_for(lambda: "catalog-0" in "".join(chunks), "Clicked key value did not render")
            await asyncio.to_thread(proc.setwinsize, 26, 52)
            proc.write("\x11")
            await wait_for(lambda: "KERNIVA_CONSOLE_RESTORED" in "".join(chunks), "ConPTY modes/exit code mismatch")
            self.assertFalse(self.service.sessions)
        finally:
            proc.close(force=True)
            reader_thread.join(timeout=3)


if __name__ == "__main__":
    unittest.main()
