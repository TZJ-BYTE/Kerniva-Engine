#!/usr/bin/env python3
"""Open a packaged POSIX terminal, observe its screen and verify terminal restoration."""

import argparse
import fcntl
import json
import os
from pathlib import Path
import pty
import select
import struct
import subprocess
import tempfile
import termios
import time


def verify(launcher):
    with tempfile.TemporaryDirectory(prefix="kerniva-terminal-") as tmp:
        master, slave = pty.openpty()
        try:
            fcntl.ioctl(slave, termios.TIOCSWINSZ, struct.pack("HHHH", 36, 110, 0, 0))
            initial = termios.tcgetattr(slave)
            env = {k: v for k, v in os.environ.items() if k not in {"PYTHONHOME", "PYTHONPATH", "VIRTUAL_ENV"}}
            env.update(TERM="xterm-256color", PATH="/usr/bin:/bin", HOME=tmp, XDG_CONFIG_HOME=tmp)
            process = subprocess.Popen([str(launcher), "--config", str(Path(tmp) / "test.json")],
                                       stdin=slave, stdout=slave, stderr=slave, cwd=tmp, env=env,
                                       start_new_session=True)
            seen = bytearray()
            opened = False
            try:
                deadline = time.monotonic() + 30
                while process.poll() is None and time.monotonic() < deadline:
                    if select.select([master], [], [], .1)[0]:
                        seen.extend(os.read(master, 65536))
                    if not opened and b"Kerniva" in seen and "保存的连接".encode() in seen and b"\x1b[?1006h" in seen:
                        opened = True
                        os.write(master, b"\x11")  # The product's Ctrl+Q binding.
                if not opened or process.poll() != 0:
                    raise RuntimeError(f"terminal startup or Ctrl+Q failed (exit={process.poll()}), "
                                       f"recent PTY output: {bytes(seen[-1500:])!r}")
                if termios.tcgetattr(slave) != initial:
                    raise RuntimeError("packaged terminal did not restore the original PTY attributes")
            finally:
                if process.poll() is None:
                    process.kill()
                process.wait()
        finally:
            os.close(master)
            os.close(slave)
    return {"launcher": launcher.name, "rendered": True, "sgr_mouse_enabled": True,
            "ctrl_q_exit": 0, "terminal_attributes_restored": True}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("launcher", type=Path, help="unpacked kerniva-tui executable")
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    report = json.dumps(verify(args.launcher.resolve(strict=True)), indent=2) + "\n"
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(report, encoding="utf-8")
    print(report, end="")


if __name__ == "__main__":
    main()
