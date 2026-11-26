#!/usr/bin/env python3
"""Build the desktop application and terminal for the current operating system."""

import argparse
import hashlib
from importlib import metadata
import json
import os
from pathlib import Path
import platform
import plistlib
import re
import shutil
import struct
import subprocess
import sys
import sysconfig
import tarfile
import tempfile
from uuid import uuid4
import zipfile


ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = ROOT / "scripts"
DESKTOP = ROOT / "desktop"
SERVICE = "kerniva-workbench-service"
APP_NAME = "Kerniva Workbench"
WINDOWS = sys.platform == "win32"
MAC = sys.platform == "darwin"


def build_backend(work):
    command = [
        sys.executable, "-m", "PyInstaller", "--noconfirm", "--onedir", "--console",
        "--noupx", "--name", SERVICE, "--distpath", str(work / "frozen"),
        "--workpath", str(work / "pyinstaller"), "--specpath", str(work),
        "--paths", str(SCRIPTS), "--hidden-import", "workbench_tui",
        "--collect-all", "textual",
    ]
    # The WSL worker and the Linux demo load these scripts by their filesystem path.
    for source in ("workbench-ui", "replica-demo-ui", "workbench_wsl.py",
                   "workbench_operations.py", "kerniva-replica-demo.py"):
        target = "." if source.endswith(".py") else source
        command.extend(["--add-data", f"{SCRIPTS / source}:{target}"])
    command.append(str(SCRIPTS / "kerniva-workbench.py"))
    subprocess.run(command, check=True, cwd=ROOT)
    return work / "frozen" / SERVICE


def copy_notices(directory, package_names, go_binary):
    directory.mkdir()
    for name in package_names:
        package = metadata.distribution(name)
        for item in package.files or []:
            path = Path(item)
            if ".dist-info" in str(path) and path.name.upper().startswith(("LICENSE", "COPYING", "NOTICE")):
                shutil.copyfile(package.locate_file(item), directory / f"{name}-{path.name}")
    # Upstream toolchains and distribution packages install their notices in different locations.
    doc = Path("/usr/share/doc")
    version = f"python{sys.version_info.major}.{sys.version_info.minor}"
    candidates = [Path(sys.base_prefix) / "LICENSE.txt", Path(sysconfig.get_path("stdlib")) / "LICENSE.txt"]
    candidates.extend(doc / pkg / "copyright" for pkg in (version, "lib" + version, "lib" + version + "-stdlib",
                                                          "lib" + version + "-minimal"))
    copy_runtime_notice(candidates, directory / "Python-LICENSE.txt")
    build_info = subprocess.check_output(["go", "version", "-m", str(go_binary)], cwd=DESKTOP, text=True)
    (directory / "Go-build-info.txt").write_text(build_info.replace(str(go_binary), go_binary.name, 1), encoding="utf-8")
    modules = [line.split()[1] for line in build_info.splitlines() if line.split()[:1] == ["dep"]]
    if modules:
        locations = subprocess.check_output(
            ["go", "list", "-m", "-f", "{{.Path}}\t{{.Dir}}", *modules], cwd=DESKTOP, text=True)
        for row in locations.splitlines():
            name, folder = row.split("\t")
            if not folder:
                raise RuntimeError(f"Missing source for packaged dependency {name}")
            for notice in sorted(Path(folder).iterdir()):
                if notice.is_file() and notice.name.upper().startswith(("LICENSE", "COPYING", "NOTICE")):
                    shutil.copyfile(notice, directory / f"{name.replace('/', '_')}-{notice.name}")
    goroot, goversion = subprocess.check_output(["go", "env", "GOROOT", "GOVERSION"], cwd=DESKTOP, text=True).splitlines()
    candidates = [Path(goroot) / "LICENSE"]
    release = re.match(r"go(\d+\.\d+)", goversion)
    if release:
        candidates.extend(doc / f"golang-{release[1]}-{part}" / "copyright" for part in ("go", "src"))
    copy_runtime_notice(candidates, directory / "Go-LICENSE.txt")


def copy_runtime_notice(candidates, destination):
    for path in candidates:
        if path.is_file():
            shutil.copyfile(path, destination)
            return
    raise FileNotFoundError(f"Runtime notice {destination.name} was not found in {candidates}")


def sha256(path):
    with path.open("rb") as data:
        return hashlib.file_digest(data, "sha256").hexdigest()


def windows_machine(path):
    with path.open("rb") as binary:
        if binary.read(2) != b"MZ":
            raise ValueError(f"{path} is not a Windows PE binary")
        binary.seek(0x3C)
        offset, = struct.unpack("<I", binary.read(4))
        binary.seek(offset)
        if binary.read(4) != b"PE\x00\x00":
            raise ValueError(f"{path} has no Windows PE header")
        return struct.unpack("<H", binary.read(2))[0]


def stage_windows_engine(work, bundle, version, arch):
    folder = bundle / "engine"
    folder.mkdir()
    server = folder / "kerniva-server.exe"
    library = folder / "kerniva_engine.dll"
    cargo = subprocess.run(
        ["cargo", "build", "--locked", "--release", "--lib", "--manifest-path",
         str(ROOT / "kerniva_engine/Cargo.toml"), "--target-dir", str(work / "cargo"),
         "--message-format=json-render-diagnostics"],
        check=True, cwd=ROOT, stdout=subprocess.PIPE, text=True,
    )
    artifacts = [Path(filename) for row in map(json.loads, cargo.stdout.splitlines())
                 if row["reason"] == "compiler-artifact" and row["target"]["name"] == "kerniva_engine"
                 for filename in row["filenames"] if Path(filename).suffix.lower() == ".dll"]
    if len(artifacts) != 1:
        raise RuntimeError("Rust build did not produce one Kerniva native library")
    shutil.copyfile(artifacts[0], library)
    subprocess.run(["go", "build", "-trimpath", "-o", str(server), "./cmd/server"], check=True, cwd=ROOT)
    machine = {"amd64": 0x8664, "arm64": 0xAA64}[arch]
    if any(windows_machine(path) != machine for path in (server, library)):
        raise RuntimeError(f"Windows engine binaries must both target {arch}")
    manifest = {
        "schema_version": 1, "product": "kerniva-engine", "version": version, "os": "windows", "arch": arch,
        "capabilities": {"go_persistence_switches": True, "startup_error_file": True},
        "files": {path.name: {"sha256": sha256(path), "size": path.stat().st_size} for path in (server, library)},
    }
    (folder / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")


def nsis_literal(path):
    return str(path).replace("$", "$$").replace('"', '$\"')


def windows_installer(bundle, target, version):
    if shutil.which("makensis") is None:
        raise RuntimeError("NSIS makensis is required for --windows-installer")
    # Keep deletion bounded to the files shipped by this installer.
    paths = sorted(bundle.rglob("*"))
    files = [path.relative_to(bundle) for path in paths if path.is_file()]
    dirs = sorted((path.relative_to(bundle) for path in paths if path.is_dir()),
                  key=lambda path: len(path.parts), reverse=True)
    include = bundle.parent / "uninstall-files.nsh"
    include.write_text(
        "".join(f'  !insertmacro RemovePayload "$INSTDIR\\{nsis_literal(path)}"\n' for path in files)
        + "".join(f'  RMDir "$INSTDIR\\{nsis_literal(path)}"\n' for path in dirs),
        encoding="utf-8-sig",
    )
    subprocess.run(["makensis", "-V2", f"-DAPP_VERSION={version}", f"-DBUNDLE_DIR={bundle}",
                    f"-DINSTALL_BUILD_ID={uuid4().hex}",
                    f"-DINSTALLER_OUT={target}", f"-DUNINSTALL_FILES={include}",
                    str(Path(__file__).with_name("windows-installer.nsi"))], check=True, cwd=ROOT)


def stage(work, version):
    bundle = work / (f"{APP_NAME}.app" if MAC else APP_NAME)
    assets = bundle / "Contents/Resources" if MAC else bundle
    programs = bundle / "Contents/MacOS" if MAC else bundle
    programs.mkdir(parents=True)
    assets.mkdir(parents=True, exist_ok=True)
    desktop_name = f"{APP_NAME}.exe" if WINDOWS else "kerniva-workbench"
    frontend = programs / desktop_name
    ldflags = "-s -w -H windowsgui" if WINDOWS else "-s -w"
    env = os.environ.copy()
    tags = "production"
    if sys.platform.startswith("linux"):
        tags += ",purego"
        env["CGO_ENABLED"] = "0"
    subprocess.run(["go", "build", "-tags", tags, "-trimpath", "-ldflags", ldflags,
                    "-o", str(frontend), "."], check=True, cwd=DESKTOP, env=env)
    shutil.copytree(build_backend(work), assets / "backend", symlinks=True)
    shutil.copyfile(Path(__file__).with_name("启动说明.txt"), assets / "启动说明.txt")

    if WINDOWS:
        (bundle / "Kerniva-TUI.cmd").write_bytes(
            b'@echo off\r\n"%~dp0backend\\kerniva-workbench-service.exe" --tui %*\r\nexit /b %errorlevel%\r\n')
    else:
        service = "../Resources/backend" if MAC else "backend"
        launcher = programs / "kerniva-tui"
        launcher.write_text('#!/bin/sh\nroot=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd) || exit 1\n'
                            f'exec "$root/{service}/{SERVICE}" --tui "$@"\n', encoding="utf-8")
        launcher.chmod(0o755)
    copy_notices(assets / "notices", ["PyInstaller", "textual", "rich", "markdown-it-py", "mdurl",
                 "mdit-py-plugins", "linkify-it-py", "pygments", "platformdirs", "typing-extensions"], frontend)
    if MAC:
        with (bundle / "Contents/Info.plist").open("wb") as output:
            plistlib.dump({"CFBundleIdentifier": "engine.kerniva.workbench", "CFBundlePackageType": "APPL",
                          "CFBundleExecutable": desktop_name, "CFBundleName": APP_NAME,
                          "CFBundleVersion": version, "CFBundleShortVersionString": version,
                          "NSHighResolutionCapable": True}, output)
        subprocess.run(["codesign", "--force", "--sign", "-", str(bundle)], check=True)
    return bundle


def archive(bundle, target):
    if MAC:
        subprocess.run(["ditto", "-c", "-k", "--sequesterRsrc", "--keepParent", str(bundle), str(target)], check=True)
    elif WINDOWS:
        with zipfile.ZipFile(target, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as output:
            for path in sorted(bundle.rglob("*")):
                if path.is_file():
                    output.write(path, path.relative_to(bundle.parent))
    else:
        with tarfile.open(target, "w:gz") as output:
            output.add(bundle, arcname=bundle.name, recursive=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=ROOT / "dist")
    parser.add_argument("--version", default="0.1.0", help="numeric package version, e.g. 0.1.0")
    parser.add_argument("--windows-installer", action="store_true",
                        help="include native engine binaries and also build the offline Windows installer")
    args = parser.parse_args()
    if args.windows_installer and not WINDOWS:
        parser.error("--windows-installer requires native Windows")
    if not (WINDOWS or MAC or sys.platform.startswith("linux")):
        parser.error("build on native Windows, Linux or macOS")
    if not (1 <= len(args.version.split(".")) <= 3 and all(p.isascii() and p.isdigit() for p in args.version.split("."))):
        parser.error("package version must have one to three numeric components")
    requested_arch = os.environ.get("GOARCH", "").strip()
    host_arch = subprocess.check_output(["go", "env", "GOHOSTARCH"], cwd=DESKTOP, text=True).strip()
    arch = {"amd64": "amd64", "x86_64": "amd64", "arm64": "arm64", "aarch64": "arm64"}.get(platform.machine().lower())
    if (not arch or host_arch != arch or (requested_arch and requested_arch != arch)
            or os.environ.get("GOOS", "") not in ("", "windows" if WINDOWS else "darwin" if MAC else "linux")):
        parser.error("run with Python and Go for the same native x64 or arm64 architecture")
    os_name = "windows" if WINDOWS else "macos" if MAC else "linux"
    extension = "zip" if WINDOWS or MAC else "tar.gz"
    name = f"Kerniva-Workbench-{args.version}-{os_name}-{arch}.{extension}"
    output = args.output_dir.resolve()
    output.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".kerniva-package-", dir=output) as temp:
        work = Path(temp)
        bundle = stage(work, args.version)
        result = work / name
        artifacts = [result]
        if args.windows_installer:
            stage_windows_engine(work, bundle, args.version, arch)
            setup = work / f"Kerniva-Workbench-{args.version}-windows-{arch}-setup.exe"
            windows_installer(bundle, setup, args.version)
            helper = work / f"Kerniva-Service-{args.version}-windows-{arch}.ps1"
            shutil.copyfile(Path(__file__).with_name("windows-service.ps1"), helper)
            artifacts.extend((setup, helper))
        archive(bundle, result)
        for artifact in artifacts:
            sha = sha256(artifact)
            target = output / artifact.name
            artifact.replace(target)
            checksum = work / (artifact.name + ".sha256")
            checksum.write_text(f"{sha}  {artifact.name}\n", encoding="ascii")
            checksum.replace(output / checksum.name)
            print(f"{target}\n{sha}  {target.stat().st_size:,} bytes")


if __name__ == "__main__":
    main()
