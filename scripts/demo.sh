#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
build_only=false
skip_build=false
case "${1:-}" in
  --build-only) build_only=true; shift ;;
  --skip-build) skip_build=true; shift ;;
esac
if [[ "$skip_build" == false ]]; then
  mkdir -p bin
  go build -trimpath -o bin/kerniva-bench ./scripts/bench
  go build -trimpath -o bin/kerniva-demo ./cmd/demo
fi
if [[ "$build_only" == true ]]; then exit 0; fi
exec "$ROOT/bin/kerniva-demo" -bench "$ROOT/bin/kerniva-bench" "$@"
