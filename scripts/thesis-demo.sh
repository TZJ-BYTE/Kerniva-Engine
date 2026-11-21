#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
database_port=16379
http_port=18080
data_dir="$ROOT/data/demo-data"
results_dir="$ROOT/bench-results"
skip_build=false

usage() {
  cat <<'HELP'
bash scripts/thesis-demo.sh [options]
  --db-port PORT       Database loopback port, default 16379
  --http-port PORT     Demo loopback port, default 18080
  --data-dir PATH      Persistent demo data, default data/demo-data
  --results-dir PATH   Benchmark history, default bench-results
  --skip-build         Reuse existing bin artifacts
  --help               Show this help
Ctrl+C stops this launch's processes and retains data and logs.
HELP
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --db-port|--http-port|--data-dir|--results-dir)
      if [[ $# -lt 2 ]]; then usage; exit 2; fi
      case "$1" in
        --db-port) database_port="$2" ;;
        --http-port) http_port="$2" ;;
        --data-dir) data_dir="$2" ;;
        --results-dir) results_dir="$2" ;;
      esac
      shift 2
      ;;
    --skip-build) skip_build=true; shift ;;
    --help) usage; exit 0 ;;
    *) usage; exit 2 ;;
  esac
done

if ! command -v python3 >/dev/null; then
  printf 'Required tool unavailable python3\n' >&2
  exit 1
fi
python3 - "$database_port" "$http_port" <<'PY'
import socket, sys
ports = [int(value) for value in sys.argv[1:]]
if len(set(ports)) != 2 or any(port < 1 or port > 65535 for port in ports):
    raise SystemExit("Choose two different ports within 1-65535")
listeners = []
try:
    for port in ports:
        listener = socket.socket()
        listeners.append(listener)
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind(("127.0.0.1", port))
finally:
    for listener in listeners:
        listener.close()
PY

mkdir -p "$data_dir" "$results_dir" "$ROOT/logs"
data_dir="$(cd "$data_dir" && pwd -P)"
results_dir="$(cd "$results_dir" && pwd -P)"
run_dir="$(mktemp -d "$ROOT/logs/thesis-demo-XXXXXX")"
stop_file="$run_dir/server.stop"
server_pid=""
demo_pid=""

stop_child() {
  local child="$1"
  if [[ -z "$child" ]]; then return; fi
  for attempt in {1..100}; do
    if ! kill -0 "$child" 2>/dev/null; then
      wait "$child" 2>/dev/null || true
      return
    fi
    sleep 0.05
  done
  kill -TERM "$child" 2>/dev/null || true
  for attempt in {1..100}; do
    if ! kill -0 "$child" 2>/dev/null; then
      wait "$child" 2>/dev/null || true
      return
    fi
    sleep 0.05
  done
  kill -KILL "$child" 2>/dev/null || true
  wait "$child" 2>/dev/null || true
}

cleanup() {
  trap - EXIT INT TERM
  if [[ -n "$demo_pid" ]]; then
    kill -TERM "$demo_pid" 2>/dev/null || true
    stop_child "$demo_pid"
  fi
  if [[ -n "$server_pid" ]]; then
    printf 'stop\n' > "$stop_file"
    stop_child "$server_pid"
  fi
  printf 'Data retained %s\nLogs retained %s\n' "$data_dir" "$run_dir"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

if [[ "$skip_build" == false ]]; then
  if ! command -v go >/dev/null; then
    printf 'Required tool unavailable go\n' >&2
    exit 1
  fi
  if ! command -v cargo >/dev/null && [[ -f "$HOME/.cargo/env" ]]; then
    source "$HOME/.cargo/env"
  fi
  bash scripts/build.sh
  bash scripts/demo.sh --build-only
fi
python3 - "$run_dir/config.json" "$database_port" "$data_dir" "$run_dir/server.log" <<'PY'
import json, sys
config = {
    "host": "127.0.0.1", "port": int(sys.argv[2]), "network_type": "std",
    "db_count": 1, "max_memory": 268435456, "max_memory_policy": "noeviction",
    "persistence_enabled": True, "persistence_type": "kerniva", "data_dir": sys.argv[3],
    "cold_start_strategy": "lazy_load", "persistence_write_mode": "strong",
    "persistence_durability": "sync", "pprof_enabled": False,
    "log_path": sys.argv[4], "log_level": "info",
}
with open(sys.argv[1], "w", encoding="utf-8") as handle:
    json.dump(config, handle, ensure_ascii=False, indent=2)
PY

# Explicit child settings keep inherited server overrides from changing the demo mode.
env KERNIVA_HOST=127.0.0.1 KERNIVA_PORT="$database_port" KERNIVA_NETWORK_TYPE=std \
  KERNIVA_DB_COUNT=1 KERNIVA_MAX_MEMORY=268435456 KERNIVA_MAX_MEMORY_POLICY=noeviction \
  KERNIVA_PERSISTENCE_ENABLED=true KERNIVA_PERSISTENCE_TYPE=kerniva KERNIVA_DATA_DIR="$data_dir" \
  KERNIVA_COLD_START_STRATEGY=lazy_load KERNIVA_PERSISTENCE_WRITE_MODE=strong \
  KERNIVA_PERSISTENCE_DURABILITY=sync KERNIVA_PPROF_ENABLED=false KERNIVA_LOG_LEVEL=info \
  KERNIVA_LOG_PATH="$run_dir/server.log" KERNIVA_ENGINE_LIBRARY="$ROOT/bin/libkerniva_engine.so" \
  "$ROOT/bin/kerniva-server" -config "$run_dir/config.json" -stop-file "$stop_file" \
  > "$run_dir/server.stdout.log" 2> "$run_dir/server.stderr.log" &
server_pid=$!
bash scripts/demo.sh --skip-build -listen "127.0.0.1:$http_port" -addr "127.0.0.1:$database_port" \
  -results-dir "$results_dir" > "$run_dir/demo.stdout.log" 2> "$run_dir/demo.stderr.log" &
demo_pid=$!

python3 - "$database_port" "$http_port" "$server_pid" "$demo_pid" <<'PY'
import json, os, socket, sys, time, urllib.request
db_port, http_port, server_pid, demo_pid = map(int, sys.argv[1:])
for attempt in range(200):
    try:
        os.kill(server_pid, 0)
        os.kill(demo_pid, 0)
        with socket.create_connection(("127.0.0.1", db_port), timeout=.1):
            pass
        with urllib.request.urlopen(f"http://127.0.0.1:{http_port}/api/status", timeout=.2) as response:
            state = json.load(response)
            if state["database"] == f"127.0.0.1:{db_port}":
                break
    except ProcessLookupError:
        raise SystemExit("Demo process exited during startup, inspect this launch's logs")
    except (OSError, ValueError):
        time.sleep(.05)
else:
    raise SystemExit("Demo startup timed out, inspect this launch's logs")
PY
printf 'Demo http://127.0.0.1:%s\nDatabase 127.0.0.1:%s std strong sync\nHistory %s\nLogs %s\nCtrl+C stops this launch and retains data.\n' \
  "$http_port" "$database_port" "$results_dir" "$run_dir"
set +e
wait -n "$server_pid" "$demo_pid"
status=$?
set -e
exit "$status"
