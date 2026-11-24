#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
cd "$ROOT"
for tool in go cargo cc python3 redis-server findmnt; do
  command -v "$tool" >/dev/null || { printf 'Required tool unavailable %s\n' "$tool" >&2; exit 1; }
done
if [[ "$(findmnt -n -o FSTYPE -T "$ROOT")" != ext4 ]]; then
  printf 'Run from a workspace on Linux ext4.\n' >&2
  exit 1
fi

mkdir -p "$ROOT/logs"
run_dir="$(mktemp -d "$ROOT/logs/golog-unattended-XXXXXXXX")"
printf 'Evidence directory %s\n' "$run_dir"
{
  date -Iseconds
  uname -a
  git rev-parse HEAD
  git status --short
  go version
  rustc --version
  redis-server --version
  findmnt -n -o TARGET,FSTYPE -T "$ROOT"
} > "$run_dir/environment.txt"

bash scripts/test.sh -race 2>&1 | tee "$run_dir/verification.log"
go build -mod=readonly -trimpath -o "$run_dir/kerniva-bench" ./scripts/bench
comparison_status=0
python3 scripts/thesis-golog-bench.py \
  --run-root "$run_dir/comparison" --server "$ROOT/bin/kerniva-server" \
  --library "$ROOT/bin/libkerniva_engine.so" --client "$run_dir/kerniva-bench" \
  --maintainer "$ROOT/bin/kerniva-maintainer" "$@" 2>&1 | tee "$run_dir/comparison.log" || comparison_status=$?

for arg in "$@"; do
  if [[ "$arg" == --plan-only ]]; then exit "$comparison_status"; fi
done
python3 -B scripts/golog-recovery-bench.py --output-dir "$run_dir/recovery" 2>&1 | tee "$run_dir/recovery.log"
exit "$comparison_status"
