#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

rm -rf -- "$ROOT/bin" "$ROOT/build" "$ROOT/dist"
echo "Clean Complete."

