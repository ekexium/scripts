#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
DEPLOY_SCRIPT="${SCRIPT_DIR}/deploy_nextgen_cluster.sh"
LUA_SCRIPT="${SCRIPT_DIR}/rowid_range_scan.lua"

CLUSTER_NAME="nextgen-s3"
TIDB_PORT="4000"
DB_NAME="sbtest"
MYSQL_USER="root"
MYSQL_PASSWORD=""

PREPARE_THREADS="8"
RUN_THREADS="64"
RUN_TIME="600"
REPORT_INTERVAL="5"

TABLE_SIZE="8000000"
SPLIT_REGIONS="2048"
RANGE_COUNT="128"
RANGE_WIDTH="80"
ROWID_STRIDE="60000"

WRITER_THREADS="8"
WRITER_ROWS_PER_EVENT="50"
WRITER_FLUSH_EVERY="1000"
SCAN_TAIL_WINDOW="2000000"
SCAN_TAIL_EXTRA="8000000"

TIDB_LOG_KEYWORDS="fell|range exceeds|validate|coverage"

DO_DEPLOY="false"
DO_CLEANUP="false"

usage() {
  cat <<'EOF'
Usage: run_rowid_remote_cop_bench.sh [options]

Single-profile benchmark for TiDB/CSE remote coprocessor rowid multi-range scan.

Options:
  --deploy                 Deploy cluster before benchmark.
                           Deploy uses: --small-region --stress-remote-cop.
  --cleanup                Run sysbench cleanup after benchmark.
  --cluster NAME           Cluster name (default: nextgen-s3).
  --time SEC               Benchmark duration seconds (default: 600).
  --help                   Show help.

Environment overrides:
  TIDB_HOST                Default: <cluster>-tidb-0
  TIKV_WORKER_HOST         Default: <cluster>-tikv-worker
  MYSQL_PASSWORD           Empty by default
  DEPLOY_EXTRA_ARGS        Extra args appended to deploy script

Examples:
  ./run_rowid_remote_cop_bench.sh
  ./run_rowid_remote_cop_bench.sh --deploy --cluster nextgen-s3
  DEPLOY_EXTRA_ARGS="--skip-patches" ./run_rowid_remote_cop_bench.sh --deploy
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --deploy)
      DO_DEPLOY="true"
      shift
      ;;
    --cleanup)
      DO_CLEANUP="true"
      shift
      ;;
    --cluster)
      CLUSTER_NAME="$2"
      shift 2
      ;;
    --time)
      RUN_TIME="$2"
      shift 2
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage
      exit 1
      ;;
  esac
done

TIDB_HOST="${TIDB_HOST:-${CLUSTER_NAME}-tidb-0}"
TIKV_WORKER_HOST="${TIKV_WORKER_HOST:-${CLUSTER_NAME}-tikv-worker}"

if [[ ! -f "${LUA_SCRIPT}" ]]; then
  echo "Missing Lua script: ${LUA_SCRIPT}" >&2
  exit 1
fi

if ! command -v sysbench >/dev/null 2>&1; then
  echo "sysbench is required but not found." >&2
  exit 1
fi

if ! command -v mysql >/dev/null 2>&1; then
  echo "mysql client is required but not found." >&2
  exit 1
fi

if [[ "${DO_DEPLOY}" == "true" ]]; then
  if [[ ! -x "${DEPLOY_SCRIPT}" ]]; then
    echo "Deploy script missing or not executable: ${DEPLOY_SCRIPT}" >&2
    exit 1
  fi

  deploy_args=("-n" "${CLUSTER_NAME}" "--small-region" "--stress-remote-cop")
  if [[ -n "${DEPLOY_EXTRA_ARGS:-}" ]]; then
    read -r -a extra_parts <<< "${DEPLOY_EXTRA_ARGS}"
    deploy_args+=("${extra_parts[@]}")
  fi

  echo "== Deploy cluster (${CLUSTER_NAME}) with small-region + stress-remote-cop =="
  "${DEPLOY_SCRIPT}" "${deploy_args[@]}"
fi

mysql_base=(mysql -h "${TIDB_HOST}" -P "${TIDB_PORT}" -u "${MYSQL_USER}")
if [[ -n "${MYSQL_PASSWORD}" ]]; then
  mysql_base+=(-p"${MYSQL_PASSWORD}")
fi

echo "== Ensure benchmark database exists =="
"${mysql_base[@]}" -e "CREATE DATABASE IF NOT EXISTS ${DB_NAME};"

common_args=(
  "${LUA_SCRIPT}"
  --db-driver=mysql
  --mysql-host="${TIDB_HOST}"
  --mysql-port="${TIDB_PORT}"
  --mysql-user="${MYSQL_USER}"
  --mysql-db="${DB_NAME}"
  --table_name=sbtest_rowid_scan
  --table_size="${TABLE_SIZE}"
  --split_regions="${SPLIT_REGIONS}"
  --range_count="${RANGE_COUNT}"
  --range_width="${RANGE_WIDTH}"
  --rowid_stride="${ROWID_STRIDE}"
  --writer_threads="${WRITER_THREADS}"
  --writer_rows_per_event="${WRITER_ROWS_PER_EVENT}"
  --writer_flush_every="${WRITER_FLUSH_EVERY}"
  --scan_tail_window="${SCAN_TAIL_WINDOW}"
  --scan_tail_extra="${SCAN_TAIL_EXTRA}"
)
if [[ -n "${MYSQL_PASSWORD}" ]]; then
  common_args+=(--mysql-password="${MYSQL_PASSWORD}")
fi

prepare_log="/tmp/rowid_remote_cop_prepare_${CLUSTER_NAME}.log"
run_log="/tmp/rowid_remote_cop_run_${CLUSTER_NAME}.log"

echo "== Prepare data (log: ${prepare_log}) =="
sysbench "${common_args[@]}" --threads="${PREPARE_THREADS}" prepare | tee "${prepare_log}"

echo "== Run benchmark (log: ${run_log}) =="
sysbench "${common_args[@]}" \
  --threads="${RUN_THREADS}" \
  --time="${RUN_TIME}" \
  --report-interval="${REPORT_INTERVAL}" \
  run | tee "${run_log}"

echo "== Grep TiDB logs for signals (keywords: ${TIDB_LOG_KEYWORDS}) =="
ssh -o StrictHostKeyChecking=no "tidb@${TIDB_HOST}" \
  "grep -inE '${TIDB_LOG_KEYWORDS}' /data/${CLUSTER_NAME}/deploy/tidb-4000/log/tidb.log | tail -n 50" || true

if [[ "${DO_CLEANUP}" == "true" ]]; then
  echo "== Cleanup data =="
  sysbench "${common_args[@]}" cleanup
fi

echo "== Check remote cop hit log on tikv-worker =="
ssh -o StrictHostKeyChecking=no "tidb@${TIKV_WORKER_HOST}" \
  "grep -n 'finished remote coprocessor' /data/tikv-worker/logs/tikv_worker.log | tail -n 20" || true

echo "Done."
