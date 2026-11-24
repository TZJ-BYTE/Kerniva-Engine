#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
cargo test --manifest-path kerniva_engine/Cargo.toml
bash scripts/build.sh
case "$(uname -s)" in
  Darwin) export KERNIVA_ENGINE_LIBRARY="$ROOT/bin/libkerniva_engine.dylib" ;;
  *) export KERNIVA_ENGINE_LIBRARY="$ROOT/bin/libkerniva_engine.so" ;;
esac
export KERNIVA_MAINTAINER="$ROOT/bin/kerniva-maintainer"
go test ./... -count=1 -timeout=180s "$@"
