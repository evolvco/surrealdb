#!/bin/bash
# CDC local test harness
# Usage: ./test.sh [start|stop|restart|logs|rebuild]

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
CDC_BIN="/tmp/cdc-connector"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose.yaml"
SURREAL_PID_FILE="/tmp/surreal-cdc-test.pid"
CDC_PID_FILE="/tmp/cdc-connector.pid"

start_infra() {
    echo "Starting PD + TiKV..."
    docker compose -f "$COMPOSE_FILE" up -d
    echo "Waiting for PD to be ready..."
    until curl -sf http://localhost:2379/pd/api/v1/members > /dev/null 2>&1; do
        sleep 1
    done
    echo "PD ready."
    sleep 2
}

build_connector() {
    echo "Building cdc-connector..."
    (cd "$SCRIPT_DIR/.." && go build -o "$CDC_BIN" ./cmd/cdc-connector)
}

start_surreal() {
    echo "Starting SurrealDB..."
    cargo run --manifest-path "$REPO_ROOT/Cargo.toml" --features storage-tikv -- \
        start --log=debug --user root --pass root --bind 0.0.0.0:8000 tikv://pd:2379 \
        > /tmp/surreal-cdc-test.log 2>&1 &
    echo $! > "$SURREAL_PID_FILE"
    echo "SurrealDB starting (pid $(cat $SURREAL_PID_FILE)), waiting..."
    sleep 5
}

start_connector() {
    build_connector
    echo "Starting cdc-connector..."
    CDC_PD_ENDPOINTS=localhost:2379 \
    CDC_SURREAL_INGEST_URL=http://localhost:8000/cdc/ingest \
    CDC_SURREAL_USERNAME=root \
    CDC_SURREAL_PASSWORD=root \
    CDC_HEARTBEAT_INTERVAL=5s \
    "$CDC_BIN" > /tmp/cdc-connector.log 2>&1 &
    echo $! > "$CDC_PID_FILE"
    echo "cdc-connector started (pid $(cat $CDC_PID_FILE))"
}

stop_all() {
    echo "Stopping..."
    if [ -f "$CDC_PID_FILE" ]; then
        kill "$(cat $CDC_PID_FILE)" 2>/dev/null || true
        rm -f "$CDC_PID_FILE"
    fi
    if [ -f "$SURREAL_PID_FILE" ]; then
        kill "$(cat $SURREAL_PID_FILE)" 2>/dev/null || true
        rm -f "$SURREAL_PID_FILE"
    fi
    docker compose -f "$COMPOSE_FILE" down
    echo "Stopped."
}

show_logs() {
    echo "=== SurrealDB (last 20 lines) ==="
    tail -20 /tmp/surreal-cdc-test.log 2>/dev/null || echo "(no logs)"
    echo ""
    echo "=== cdc-connector (last 20 lines) ==="
    tail -20 /tmp/cdc-connector.log 2>/dev/null || echo "(no logs)"
    echo ""
    echo "=== Docker (PD + TiKV) ==="
    docker compose -f "$COMPOSE_FILE" logs --tail=10 2>/dev/null || echo "(not running)"
}

case "${1:-start}" in
    start)
        start_infra
        start_surreal
        start_connector
        echo ""
        echo "All running! Test with:"
        echo "  surreal sql --conn http://localhost:8000 --user root --pass root --ns test --db test"
        echo ""
        echo "  LIVE SELECT * FROM person;"
        echo "  CREATE person:test SET name = 'hello';"
        echo ""
        echo "Logs: $0 logs"
        echo "Stop: $0 stop"
        ;;
    stop)
        stop_all
        ;;
    restart)
        stop_all
        sleep 2
        start_infra
        start_surreal
        start_connector
        ;;
    logs)
        show_logs
        ;;
    rebuild)
        if [ -f "$CDC_PID_FILE" ]; then
            kill "$(cat $CDC_PID_FILE)" 2>/dev/null || true
        fi
        start_connector
        ;;
    *)
        echo "Usage: $0 [start|stop|restart|logs|rebuild]"
        exit 1
        ;;
esac
