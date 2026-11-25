"""Check saved key positions with two real services and a fresh database."""

import argparse
import base64
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
import json
from pathlib import Path
import sys
from tempfile import TemporaryDirectory

from test_workbench_integration import API, ports_closed, require
from test_workbench_navigation import journey, workbench
from workbench_redis import _Client

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "packaging" / "desktop"))
from verify import database


def encoded(key):
    return base64.b64encode(key).decode("ascii")


def add(api, session, profile, db, key):
    return api("pins/add", {"session": session, "profileId": profile, "db": db, "key_b64": encoded(key)})["pin"]


def exercise(args, root, evidence):
    config = root / "profiles.json"
    config.write_text(json.dumps({"version": 1, "profiles": [], "settings": {}}), encoding="utf-8")
    records = {}
    with ExitStack() as stack:
        probe, port = database(stack, root, args.server.resolve())
        probe.close()
        for db in [0, 1]:
            with _Client({"host": "127.0.0.1", "port": port, "db": db}) as client:
                for key, value in [(b"demo:marker", f"live-db{db}"), (b"", "empty-name-value"),
                                   (b"\xff", "binary-ff-value"), (b"\xfe", "binary-fe-value")]:
                    require(client.at(client.seed, "SET", key, value) == b"OK", "SET failed")
                require(client.at(client.seed, "RPUSH", "demo:list", "first-page", "second-page") == 2, "RPUSH failed")
        with workbench(args, root) as origin, workbench(args, root) as other:
            first, second = API(origin), API(other)
            require(first("state")["pins"] == [], "Legacy configuration was not loaded")
            profile = first("profile/save", {"profile": {"name": "本机演示", "host": "127.0.0.1", "port": port,
                                                        "mode": "standalone", "db": 0}})["profile"]
            pid = profile["id"]
            first("settings", {"settings": {"pageSize": 1}})
            s1 = first("connect", {"id": pid})["session"]
            s2 = second("connect", {"id": pid})["session"]
            second("select-db", {"session": s2, "db": 1})
            with ThreadPoolExecutor(max_workers=2) as pool:
                a = pool.submit(add, first, s1, pid, 0, b"\xff")
                b = pool.submit(add, second, s2, pid, 1, b"\xfe")
                p1, p2 = a.result(), b.result()
            require({p["id"] for p in first("state")["pins"]} == {p1["id"], p2["id"]}, "Lost concurrent add")
            second("select-db", {"session": s2, "db": 0})
            with ThreadPoolExecutor(max_workers=2) as pool:
                a = pool.submit(add, first, s1, pid, 0, b"")
                b = pool.submit(add, second, s2, pid, 0, b"")
                require(a.result() == b.result(), "Concurrent add created duplicate position")
                empty = a.result()
            changed = {"session": s2, "profileId": pid, "db": 1, "key_b64": ""}
            second("pins/add", changed, status=409)
            second("pins/add", {**changed, "session": s1}, status=401)
            second("pins/add", {**changed, "db": 0, "key_b64": "??"}, status=400)
            require(first("pins/add", {"session": s1, "profileId": pid, "db": 0, "key_b64": "/x=="})["pin"] == p1,
                    "Equivalent key bytes were recorded twice")
            require(second("pins/remove", {"id": empty["id"]})["removed"], "Valid removal unreported")
            require(not first("pins/remove", {"id": empty["id"]})["removed"], "Stale removal claimed success")
            add(first, s1, pid, 0, b"")
            first("profile/save", {"profile": {**profile, "name": "演示连接"}})
            second("key", {"session": s2, "key_b64": ""}, status=401)
            require(second("key", {"session": second("connect", {"id": pid})["session"], "key_b64": ""})["value"]["text"] == "empty-name-value",
                    "Removing the pin changed the database")
            records.update(concurrentIndependentAdds=2, concurrentSamePosition=1, staleRemoval=False,
                           invalidSession=401, staleDatabase=409, malformedKey=400, renamedProfile=pid)
        with workbench(args, root) as origin:
            api = API(origin)
            require(len(api("state")["pins"]) == 3, "Pins did not survive service and database session restart")
            if args.browser:
                records["browser"] = journey(args, {"origin": origin, "profileId": pid, "evidence": str(evidence),
                                                    "dbPort": port, "pins": True})
            pins = api("state")["pins"]
            stored = json.loads(config.read_text(encoding="utf-8"))
            require(all(set(pin) == {"id", "db", "key_b64", "profileId"} for pin in stored["pins"]),
                    "Unexpected information saved in pin record")
            require(not any(text in config.read_text(encoding="utf-8") for text in
                            ["live-db", "empty-name-value", "binary-", "first-page", s1, s2]), "Configuration contains session or value")
            require(api("profile/delete", {"id": pid})["removedPins"] == len(pins) and not api("state")["pins"],
                    "Profile removal left dangling positions")
            records.update(noPersistedValuesOrTokens=True, cascadedPositions=len(pins))
    require(ports_closed([port, int(origin.rsplit(":", 1)[1]), int(other.rsplit(":", 1)[1])]), "Fixture port survived")
    return {**records, "passed": True, "cleanShutdown": True}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server", type=Path, required=True)
    parser.add_argument("--service", type=Path)
    parser.add_argument("--browser", type=Path)
    parser.add_argument("--evidence", type=Path, required=True)
    args = parser.parse_args()
    evidence = args.evidence.resolve()
    evidence.mkdir(parents=True, exist_ok=False)
    try:
        with TemporaryDirectory(prefix="kerniva-pins-") as root:
            report = exercise(args, Path(root), evidence)
    except Exception as error:
        report = {"passed": False, "error": str(error)}
    result = json.dumps(report, ensure_ascii=False, indent=2)
    (evidence / "result.json").write_text(result + "\n", encoding="utf-8")
    print(result)
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
