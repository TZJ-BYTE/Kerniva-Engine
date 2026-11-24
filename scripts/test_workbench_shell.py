import base64
from io import StringIO
import unittest
from unittest.mock import patch
import warnings

import workbench_shell


class AppProblem(Exception):
    def __init__(self, status, message):
        super().__init__(message)
        self.status = status


class App:
    def __init__(self):
        self.a = {"id": "p1", "name": "My Redis", "host": "localhost", "port": 6379, "db": 0}
        self.b = {"id": "p2", "name": "Restricted", "host": "localhost", "port": 6380, "db": 1}
        self.calls = []
        self.settings = {"pageSize": 50, "previewBytes": 4096, "formatJson": True}

    def state(self):
        return {"profiles": [self.a, self.b], "settings": self.settings}

    def perform(self, action, body):
        self.calls.append((action, body))
        if action == "connect":
            if body["id"] == "p2" and not body.get("password"):
                raise AppProblem(422, "NOAUTH Authentication required")
            if body.get("password") == "wrong":
                raise AppProblem(422, "WRONGPASS invalid credentials")
            return {"session": body["id"] + "-token"}
        if action == "profile/save":
            return {"profile": {**body["profile"], "id": "new"}}
        if action == "settings":
            self.settings.update(body["settings"])
            return {"settings": self.settings}
        if action == "status":
            return {"server": "Test Redis", "db": 0, "nodes": [{"label": "localhost"}]}
        if action == "scan":
            return {"items": [
                {"key": "key with spaces", "key_b64": "a2V5IHdpdGggc3BhY2Vz"},
                {"key": None, "key_b64": "/wA="},
            ], "cursor": "page2" if "cursor" not in body else None}
        if action == "key":
            value = '{"value":9007199254740993,"value":1e+9}'
            return {"items": [], "type": "string", "offset": 0, "length": len(value), "ttl_ms": -1,
                    "value": {"text": value, "truncated": False}}
        return {"ok": True}

    def replication_status(self):
        return {"primary": {"ready": True}}

    def replication_activity(self, page):
        return {"page": page, "items": []}

    def replication_action(self, action, body):
        self.calls.append((action, body))
        return {"ok": True}


class ShellTests(unittest.TestCase):
    def run_commands(self, commands, passwords=()):
        app, stdout = App(), StringIO()
        answers = iter(passwords)
        exit_code = workbench_shell.run(app, stdin=StringIO(commands), stdout=stdout,
                                        password_prompt=lambda: next(answers))
        self.assertEqual(exit_code, 0)
        return app, stdout.getvalue()

    def test_failed_switch_leaves_previous_session_and_secrets_are_not_printed(self):
        app, output = self.run_commands(
            "/connect My Redis\n/connect p2\n/status\n/connect --ask-password p2\n/status\n/exit\n",
            ["wrong", "good-password"],
        )
        self.assertEqual(app.calls, [
            ("connect", {"id": "p1"}), ("connect", {"id": "p2"}),
            ("connect", {"id": "p2", "password": "wrong"}), ("status", {"session": "p1-token"}),
            ("connect", {"id": "p2", "password": "good-password"}), ("disconnect", {"session": "p1-token"}),
            ("status", {"session": "p2-token"}), ("disconnect", {"session": "p2-token"}),
        ])
        self.assertIn("数据库要求认证", output)
        self.assertIn("WRONGPASS", output)
        self.assertNotIn("good-password", output)
        self.assertNotIn("p2-token", output)

    def test_numbered_picker_and_scriptable_number(self):
        app, output = self.run_commands("/connect\n/connect 1\n/connect #5\n/status\n/exit\n")
        self.assertEqual(app.calls, [
            ("connect", {"id": "p1"}), ("status", {"session": "p1-token"}),
            ("disconnect", {"session": "p1-token"}),
        ])
        self.assertIn('#1  "My Redis"', output)
        self.assertIn("连接编号超出范围", output)

        class Terminal(StringIO):
            def isatty(self):
                return True

        app, output = App(), Terminal()
        commands = Terminal("/profiles\n/connect\n\n/connect --ask-password\n2\n/status\n/exit\n")
        self.assertEqual(workbench_shell.run(app, stdin=commands, stdout=output,
                                             password_prompt=lambda: "secure"), 0)
        self.assertEqual(app.calls, [
            ("connect", {"id": "p2", "password": "secure"}), ("status", {"session": "p2-token"}),
            ("disconnect", {"session": "p2-token"}),
        ])
        self.assertIn('#2  "Restricted"', output.getvalue())
        self.assertIn("连接编号、名称或 ID（回车取消）", output.getvalue())

    def test_numbered_picker_preserves_bare_numeric_profile_name(self):
        app = App()
        app.a["name"] = "2"
        stdout = StringIO()
        workbench_shell.run(app, stdin=StringIO("/connect 2\n/connect #2\n/exit\n"), stdout=stdout,
                            password_prompt=lambda: "secure")
        self.assertEqual([body["id"] for action, body in app.calls if action == "connect"], ["p1", "p2", "p2"])

    def test_exact_binary_and_empty_keys_and_page_history(self):
        app, output = self.run_commands(
            "/connect My Redis\n/scan key *\n/next\n/prev\n/get  key with spaces \n/get-b64 /wA=\n/get\n"
            "/settings formatJson off\n/get regular\n/exit\n",
        )
        scan = [body for action, body in app.calls if action == "scan"]
        self.assertEqual([request.get("cursor") for request in scan], [None, "page2", None])
        self.assertEqual([request["pattern"] for request in scan], ["key *"] * 3)
        self.assertEqual([base64.b64decode(body["key_b64"]) for action, body in app.calls if action == "key"],
                         [b" key with spaces ", b"\xff\x00", b"", b"regular"])
        self.assertIn("/get-b64 /wA=", output)
        self.assertIn('"value": 9007199254740993,\n  "value": 1e+9', output)
        self.assertIn('{"value":9007199254740993,"value":1e+9}', output)

    def test_shared_profile_settings_and_replication_actions(self):
        app, output = self.run_commands(
            '/add "Cluster profile" 127.0.0.1 --mode cluster --tls --ca-file "/some CA.pem"\n'
            "/settings pageSize 10\n/replica status\n/replica activity 2\n/replica pause a\n"
            "/replica resume b\n/replica mark 1 example marker value\n",
        )
        self.assertEqual(app.calls[0][1]["profile"]["caFile"], "/some CA.pem")
        self.assertTrue(app.calls[0][1]["profile"]["tls"])
        self.assertEqual(app.settings["pageSize"], 10)
        self.assertEqual(app.calls[-3:], [
            ("pause", {"replica": "a"}), ("resume", {"replica": "b"}),
            ("mark", {"db": 1, "value": "example marker value"}),
        ])
        self.assertIn('"ready": true', output)
        self.assertIn('"page": 2', output)

    def test_password_never_falls_back_to_echo(self):
        def getpass_fallback(_prompt):
            warnings.warn("no terminal echo control", workbench_shell.getpass.GetPassWarning)
            self.fail("getpass continued after the no-echo warning")

        with patch("workbench_shell.getpass.getpass", getpass_fallback):
            with self.assertRaises(workbench_shell.ShellError):
                workbench_shell._password()


if __name__ == "__main__":
    unittest.main()
