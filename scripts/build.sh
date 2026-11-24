#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
mkdir -p bin
cargo build --release --manifest-path kerniva_engine/Cargo.toml
kernel="$(uname -s)"
case "$kernel" in
  Darwin) library=libkerniva_engine.dylib ;;
  *) library=libkerniva_engine.so ;;
esac
cargo_target="${CARGO_TARGET_DIR:-kerniva_engine/target}"
if [[ -n "${CARGO_BUILD_TARGET:-}" ]]; then cargo_target="$cargo_target/$CARGO_BUILD_TARGET"; fi
cp "$cargo_target/release/$library" bin/
cp "$cargo_target/release/kerniva-maintainer" bin/
go build -trimpath -o bin/kerniva-server ./cmd/server
go build -trimpath -o bin/kerniva-client ./cmd/client
if [[ "$kernel" == Linux ]]; then
  go build -trimpath -o bin/kerniva-replica ./cmd/kerniva-replica
fi
