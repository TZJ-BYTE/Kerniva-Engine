#!/usr/bin/env bash
# Run in WSL after the Windows recorder publishes host.ready.
set -euo pipefail
if (( $# < 3 )); then
    echo "usage: bash golog_host_disk_guest.sh CAPTURE_DIR WINDOWS_PYTHON COMMAND [ARG ...]" >&2
    exit 2
fi
capture=${1%/}
windows_python=$2
shift 2
if [[ ! -f "$capture/host.ready" || $(<"$capture/host.ready") != "${capture##*/}" || -e "$capture/guest.done" ]]; then
    echo "Windows recorder is not ready for this capture: $capture" >&2
    exit 1
fi
clock_script=$(dirname -- "${BASH_SOURCE[0]}")/golog_host_clock.py
mkdir -- "$capture/guest-run"
trap 'printf "%s\n" "$?" > "$capture/guest.done"' EXIT
python3 -B "$clock_script" --windows-python "$windows_python" \
    --output "$capture/guest-run/clocks.json" -- "$@" 2>&1 | tee -- "$capture/guest-run/run.log"
