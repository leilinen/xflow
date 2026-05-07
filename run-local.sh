#!/usr/bin/env bash
set -euo pipefail

DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$DIR"

# Load .env if present
if [ -f "$DIR/.env" ]; then
    set -a
    source "$DIR/.env"
    set +a
fi

LOGS_DIR="$DIR/logs"
API_PID_FILE="$LOGS_DIR/api.pid"
WORKER_PID_FILE="$LOGS_DIR/worker.pid"
API_LOG="$LOGS_DIR/api.log"
WORKER_LOG="$LOGS_DIR/worker.log"
CONFIG="${XFLOW_CONFIG:-config.yaml}"
BIN="$DIR/target/release/xflow"

ensure_dirs() {
    mkdir -p "$LOGS_DIR" data
}

build() {
    echo "Building xflow (release)..."
    cargo build --release 2>&1
}

is_running() {
    local pid_file="$1"
    if [ -f "$pid_file" ]; then
        local pid
        pid=$(cat "$pid_file")
        if kill -0 "$pid" 2>/dev/null; then
            return 0
        fi
        rm -f "$pid_file"
    fi
    return 1
}

start_process() {
    local name="$1" pid_file="$2" log_file="$3" subcmd="$4"
    if is_running "$pid_file"; then
        echo "$name already running (PID $(cat "$pid_file"))"
        return
    fi
    echo "Starting $name..."
    "$BIN" $subcmd --config "$CONFIG" >> "$log_file" 2>&1 &
    local pid=$!
    echo "$pid" > "$pid_file"
    sleep 1
    if kill -0 "$pid" 2>/dev/null; then
        echo "$name started (PID $pid, log: $log_file)"
    else
        echo "ERROR: $name failed to start. Check $log_file"
        rm -f "$pid_file"
        return 1
    fi
}

stop_process() {
    local name="$1" pid_file="$2"
    if ! is_running "$pid_file"; then
        echo "$name not running"
        return
    fi
    local pid
    pid=$(cat "$pid_file")
    echo "Stopping $name (PID $pid)..."
    kill "$pid" 2>/dev/null || true
    # Wait up to 10s for graceful shutdown
    for _ in $(seq 1 10); do
        if ! kill -0 "$pid" 2>/dev/null; then
            break
        fi
        sleep 1
    done
    if kill -0 "$pid" 2>/dev/null; then
        echo "Force killing $name..."
        kill -9 "$pid" 2>/dev/null || true
    fi
    rm -f "$pid_file"
    echo "$name stopped"
}

cmd_start() {
    ensure_dirs
    build
    echo ""
    start_process "api" "$API_PID_FILE" "$API_LOG" "serve"
    start_process "worker" "$WORKER_PID_FILE" "$WORKER_LOG" "worker"
    echo ""
    echo "xFlow is running. Use '$0 status' or '$0 logs' to monitor."
}

cmd_stop() {
    stop_process "worker" "$WORKER_PID_FILE"
    stop_process "api" "$API_PID_FILE"
}

cmd_status() {
    local running=0
    if is_running "$API_PID_FILE"; then
        echo "api: running (PID $(cat "$API_PID_FILE"))"
        running=$((running + 1))
    else
        echo "api: stopped"
    fi
    if is_running "$WORKER_PID_FILE"; then
        echo "worker: running (PID $(cat "$WORKER_PID_FILE"))"
        running=$((running + 1))
    else
        echo "worker: stopped"
    fi
    if [ "$running" -eq 2 ]; then
        # Quick health check
        if curl -sf http://127.0.0.1:8000/health >/dev/null 2>&1; then
            echo "health: OK"
        else
            echo "health: no response (api may still be starting)"
        fi
    fi
}

cmd_logs() {
    local target="${1:-all}"
    case "$target" in
        api)    tail -f "$API_LOG" ;;
        worker) tail -f "$WORKER_LOG" ;;
        all)    tail -f "$API_LOG" "$WORKER_LOG" ;;
        *)      echo "Usage: $0 logs [api|worker|all]"; exit 1 ;;
    esac
}

cmd_restart() {
    cmd_stop
    sleep 2
    cmd_start
}

usage() {
    echo "Usage: $0 {start|stop|restart|status|logs [api|worker|all]}"
    echo ""
    echo "  start    Build and start api + worker"
    echo "  stop     Stop all processes"
    echo "  restart  Stop and start"
    echo "  status   Show process status and health"
    echo "  logs     Tail logs (api, worker, or all)"
    echo ""
    echo "Environment:"
    echo "  XFLOW_CONFIG  Config file path (default: config.yaml)"
}

case "${1:-}" in
    start)   cmd_start ;;
    stop)    cmd_stop ;;
    restart) cmd_restart ;;
    status)  cmd_status ;;
    logs)    cmd_logs "${2:-all}" ;;
    *)       usage ;;
esac
