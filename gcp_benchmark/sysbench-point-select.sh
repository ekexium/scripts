#!/bin/bash
# run_benchmark.sh
# This script uses the sysbench point select fixed-rate benchmark to evaluate P99 latency.
# It collects CPU profiles via TiDB's pprof endpoint during each run.
# It toggles specific TiDB features using system variables to measure performance impact.
#
# In addition, the script performs a preparation phase where:
#   1. It creates the "sbtest" database if it does not already exist.
#   2. It runs the sysbench prepare command to create the necessary tables and insert data.
#
# Features to test (each will be individually disabled):
#   tidb_use_plan_baselines
#   tidb_plan_cache_invalidation_on_fresh_stats
#   tidb_enable_stmt_summary
#   tidb_enable_collect_execution_info
#   tidb_enable_resource_control

set -euo pipefail

#######################
# Configuration Parameters
#######################

# TiDB connection and pprof settings (adjust as needed)
CLUSTER_NAME=${CLUSTER_NAME:-"eke-bench"}
TIDB_HOST=${TIDB_HOST:-"${CLUSTER_NAME}-tidb-0"}
TIDB_PORT=${TIDB_PORT:-4000}
TIDB_USER=${TIDB_USER:-"root"}
TIDB_PASSWORD=${TIDB_PASSWORD:-""}
# TiDB pprof endpoint port (usually 10080)
TIDB_PPROF_PORT=${TIDB_PPROF_PORT:-10080}

# Sysbench settings (ensure sysbench is installed and that the test script path is valid)
# This example uses the oltp_point_select test.
SYSBENCH_PATH=${SYSBENCH_PATH:-"sysbench"}
SYSBENCH_TEST_SCRIPT=${SYSBENCH_TEST_SCRIPT:-"oltp_point_select"}
DATABASE=${DATABASE:-"sbtest"}
THREADS=${THREADS:-64}
RUNTIME=${RUNTIME:-180}         # Duration of the run phase in seconds
RATE=${RATE:-65000}             # Fixed QPS rate
# Duration to collect CPU profile. It is suggested to be equal or lower than RUNTIME (in seconds).
PROFILE_DURATION=${RUNTIME}

# Table settings for sysbench (used during prepare and run phases)
TABLES=${TABLES:-1}          # Number of tables used in the test
TABLE_SIZE=${TABLE_SIZE:-1000000}  # Number of rows per table

# Additional sysbench options to reduce output and only report P99, etc.
EXTRA_OPTS=${EXTRA_OPTS:="--report-interval=10 --percentile=99 --mysql-ignore-errors=all --forced-shutdown=60 --rand-type=uniform --ignore-queue-full"}

# Directory for storing test results and profiles
RESULTS_DIR="results_$(date +%Y%m%d_%H%M%S)"
mkdir -p "${RESULTS_DIR}"

# List of feature variables to test
FEATURES=(
  "tidb_use_plan_baselines"
  "tidb_plan_cache_invalidation_on_fresh_stats"
  "tidb_enable_stmt_summary"
  "tidb_enable_collect_execution_info"
  "tidb_enable_resource_control"
)

#######################
# Helper Functions
#######################

# Execute a SQL statement using the mysql client
function exec_mysql() {
  local sql="$1"
  echo ">>> Executing: $sql"
  mysql -h "$TIDB_HOST" -P "$TIDB_PORT" -u "$TIDB_USER" -e "$sql"
}

# Set the state of a given feature variable (state: "on" or "off")
function set_feature_state() {
  local feature="$1"
  local state="$2"  # expected values: "on" or "off"
  exec_mysql "SET GLOBAL ${feature}=${state};"
}

# Restore all features to the "on" state
function restore_all_features() {
  echo ">>> Restoring all features to ON state..."
  local feature
  for feature in "${FEATURES[@]}"; do
    set_feature_state "$feature" "on"
  done
}

# Prepare the test environment
# 1. Create the database (if it does not exist)
# 2. Run the sysbench prepare phase to create tables and insert test data,
#    using specified TABLES and TABLE_SIZE.
function prepare_database() {
  echo ">>> Checking if database '${DATABASE}' exists and creating it if necessary..."
  exec_mysql "CREATE DATABASE IF NOT EXISTS ${DATABASE};"

  echo ">>> Running sysbench 'prepare' phase..."
  ${SYSBENCH_PATH} ${SYSBENCH_TEST_SCRIPT} \
    --mysql-db=${DATABASE} \
    --mysql-host=${TIDB_HOST} \
    --mysql-port=${TIDB_PORT} \
    --mysql-user=${TIDB_USER} \
    --mysql-password=${TIDB_PASSWORD} \
    --tables=${TABLES} \
    --table-size=${TABLE_SIZE} \
    prepare
}

# Run the sysbench benchmark and collect a CPU profile concurrently.
# This function uses the sysbench "run" command.
function run_benchmark() {
  local test_name="$1"
  local test_dir="$2"
  echo "==== Starting benchmark: [${test_name}] ===="
  local sysbench_log="${test_dir}/${test_name}_sysbench.log"
  local cpu_profile_file="${test_dir}/${test_name}_cpuprofile.pprof"

  # Construct the sysbench command using the target database.
  # Now we also pass --tables and --table-size so that the test script
  # correctly understands how many tables and rows to operate on.
  SYSBENCH_CMD="${SYSBENCH_PATH} ${SYSBENCH_TEST_SCRIPT} \
    --threads=${THREADS} \
    --time=${RUNTIME} \
    --rate=${RATE} \
    --db-driver=mysql \
    --mysql-db=${DATABASE} \
    --mysql-host=${TIDB_HOST} \
    --mysql-port=${TIDB_PORT} \
    --mysql-user=${TIDB_USER} \
    --mysql-password=${TIDB_PASSWORD} \
    ${EXTRA_OPTS} \
    --tables=${TABLES} \
    --table-size=${TABLE_SIZE} \
    run"

  echo ">>> Sysbench command: ${SYSBENCH_CMD}"

  # Start CPU profile collection in the background.
  echo ">>> Starting CPU profile collection for ${PROFILE_DURATION} seconds..."
  curl -s -o "${cpu_profile_file}" "http://${TIDB_HOST}:${TIDB_PPROF_PORT}/debug/pprof/profile?seconds=${PROFILE_DURATION}" &
  CPU_PROFILE_PID=$!

  # Run the sysbench benchmark, logging output to a file.
  echo ">>> Running sysbench benchmark. Output will be logged to ${sysbench_log}"
  ${SYSBENCH_CMD} | tee "${sysbench_log}"

  # Wait for the CPU profile collection to complete.
  wait "${CPU_PROFILE_PID}"
  echo "==== Finished benchmark: [${test_name}] ===="
  echo "   Sysbench log: ${sysbench_log}"
  echo "   CPU profile: ${cpu_profile_file}"
}

# Collect diagnostic data from the cluster using tiup diag collect.
function collect_diagnostics() {
  echo ">>> Collecting diagnostic data for cluster [${CLUSTER_NAME}] ..."
  tiup diag collect "${CLUSTER_NAME}" -f "${METRICS_FROM}" -t "${METRICS_TO}" -o "${RESULTS_DIR}/diag_data"
  echo ">>> Diagnostic data collected at: ${RESULTS_DIR}/diag_data"
}

#######################
# Benchmark Workflow
#######################

METRICS_FROM=$(date -uIseconds)

# Preparation Phase:
# Ensure the target database and tables exist by creating the database and running sysbench prepare.
prepare_database

echo ">>> Preparation complete. Waiting 5 seconds to ensure all settings take effect..."
sleep 5

# Restore all features to "on" state as the baseline, then run the baseline benchmark.
restore_all_features
echo ">>> Running baseline benchmark with all features ON"
run_benchmark "baseline_all_on" "${RESULTS_DIR}"

# Test each feature individually by turning it off.
for feature in "${FEATURES[@]}"; do
  echo "----------------------------------------------"
  echo "Testing: Disabling ${feature}"
  # First, restore all features then disable the current feature.
  restore_all_features
  set_feature_state "${feature}" "off"
  echo ">>> Waiting 5 seconds to ensure settings are applied..."
  sleep 5
  
  run_benchmark "feature_${feature}_off" "${RESULTS_DIR}"
done

# Optionally, run a benchmark with all features turned off.
echo "----------------------------------------------"
echo "Testing: Disabling ALL features"
for feature in "${FEATURES[@]}"; do
  set_feature_state "${feature}" "off"
done
echo ">>> Waiting 5 seconds to ensure settings are applied..."
sleep 5
run_benchmark "all_features_off" "${RESULTS_DIR}"

echo "=============================================="
echo "All tests completed. Results are stored in directory: ${RESULTS_DIR}"

METRICS_TO=$(date -uIseconds)

echo "=============================================="
echo "P99 Summary of sysbench benchmarks:"
for log in "${RESULTS_DIR}"/*_sysbench.log; do
    test_name=$(basename "${log}" _sysbench.log)
    p99_line=$(grep -Ei 'P99|99th' "${log}" | head -n 1)
    if [ -z "${p99_line}" ]; then
      p99_line="No P99 metric found."
    fi
    echo "${test_name}: ${p99_line}"
done
echo "=============================================="

echo ">>> Running sysbench 'cleanup' phase to drop existing tables..."
${SYSBENCH_PATH} ${SYSBENCH_TEST_SCRIPT} \
    --mysql-db=${DATABASE} \
    --mysql-host=${TIDB_HOST} \
    --mysql-port=${TIDB_PORT} \
    --mysql-user=${TIDB_USER} \
    --mysql-password=${TIDB_PASSWORD} \
    cleanup

collect_diagnostics
