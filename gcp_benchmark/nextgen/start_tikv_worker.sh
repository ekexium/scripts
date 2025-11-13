#!/bin/bash
#
# TiKV Worker Startup Script for TiDB-X Deployment
# This script is deployed to the TiKV Worker VM at /data/tikv-worker/start_tikv_worker.sh
#
# Usage:
#   ./start_tikv_worker.sh [pd-endpoint]
#
# Example:
#   ./start_tikv_worker.sh http://nextgen-s3-pd-0:2379
#

set -e

# Configuration
TIKV_WORKER_DIR="/data/tikv-worker"
TIKV_WORKER_BIN="$TIKV_WORKER_DIR/bin/tikv-worker"
TIKV_WORKER_CONFIG="$TIKV_WORKER_DIR/conf/tikv_worker.toml"
TIKV_WORKER_LOG="$TIKV_WORKER_DIR/logs/tikv_worker.log"
TIKV_WORKER_PID="$TIKV_WORKER_DIR/tikv_worker.pid"
TIKV_WORKER_ADDR="0.0.0.0:19000"

# PD endpoint (can be passed as argument or detected from config)
PD_ENDPOINTS="${1:-http://{{PD_HOST}}:2379}"

# Check if TiKV Worker is already running
if [[ -f "$TIKV_WORKER_PID" ]]; then
    OLD_PID=$(cat "$TIKV_WORKER_PID")
    if ps -p "$OLD_PID" > /dev/null 2>&1; then
        echo "TiKV Worker is already running with PID: $OLD_PID"
        echo "To restart, first stop it: kill $OLD_PID"
        exit 1
    else
        echo "Removing stale PID file..."
        rm -f "$TIKV_WORKER_PID"
    fi
fi

# Verify binary exists
if [[ ! -f "$TIKV_WORKER_BIN" ]]; then
    echo "Error: TiKV Worker binary not found at: $TIKV_WORKER_BIN"
    exit 1
fi

# Verify config exists
if [[ ! -f "$TIKV_WORKER_CONFIG" ]]; then
    echo "Error: TiKV Worker config not found at: $TIKV_WORKER_CONFIG"
    exit 1
fi

# Create necessary directories
mkdir -p "$TIKV_WORKER_DIR/logs"
mkdir -p "$TIKV_WORKER_DIR/schemas"

# Change to TiKV Worker directory
cd "$TIKV_WORKER_DIR"

echo "========================================"
echo "Starting TiKV Worker"
echo "========================================"
echo "Binary:       $TIKV_WORKER_BIN"
echo "Config:       $TIKV_WORKER_CONFIG"
echo "Address:      $TIKV_WORKER_ADDR"
echo "PD Endpoints: $PD_ENDPOINTS"
echo "Log File:     $TIKV_WORKER_LOG"
echo "========================================"

# Start TiKV Worker in background
nohup "$TIKV_WORKER_BIN" \
  --addr="$TIKV_WORKER_ADDR" \
  --pd-endpoints="$PD_ENDPOINTS" \
  --config="$TIKV_WORKER_CONFIG" \
  --log-file="$TIKV_WORKER_LOG" \
  > "$TIKV_WORKER_DIR/logs/stdout.log" 2>&1 &

# Save PID
TIKV_WORKER_PID_NUM=$!
echo "$TIKV_WORKER_PID_NUM" > "$TIKV_WORKER_PID"

echo ""
echo "✓ TiKV Worker started successfully"
echo "  PID: $TIKV_WORKER_PID_NUM"
echo "  PID file: $TIKV_WORKER_PID"
echo ""
echo "To check status:"
echo "  curl http://localhost:19000/metrics"
echo ""
echo "To view logs:"
echo "  tail -f $TIKV_WORKER_LOG"
echo ""
echo "To stop:"
echo "  kill $TIKV_WORKER_PID_NUM"

# Wait a moment and check if process is still running
sleep 2
if ps -p "$TIKV_WORKER_PID_NUM" > /dev/null 2>&1; then
    echo ""
    echo "✓ TiKV Worker is running"
else
    echo ""
    echo "✗ TiKV Worker failed to start"
    echo "Check logs at: $TIKV_WORKER_LOG"
    exit 1
fi
