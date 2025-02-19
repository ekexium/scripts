#!/bin/bash
# run_benchmark.sh
# This script uses the sysbench point select fixed-rate benchmark to evaluate P99 latency.
# It collects CPU and block profiles via TiDB's pprof endpoint during each run.
# It toggles specific TiDB features using system variables to measure performance impact.
#
# In addition, the script performs a preparation phase where:
#   1. It creates the "sbtest" database if it does not already exist.
#   2. It runs the sysbench prepare command to create the necessary tables and insert data.
#
# Features to test (each will be individually toggled):
#   tidb_use_plan_baselines
#   tidb_plan_cache_invalidation_on_fresh_stats
#   tidb_enable_stmt_summary
#   tidb_enable_collect_execution_info
#   tidb_enable_resource_control
#   tidb_txn_assertion_level            (example: "fast" vs "off")
#   tidb_guarantee_linearizability      (example: "on" vs "off")

set -euo pipefail

#######################
# Configuration Parameters
#######################

# TiDB connection and pprof settings (adjust as needed)
CLUSTER_NAME=${CLUSTER_NAME:-"test-zq"}
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
# Duration to collect profiles. It is suggested to be equal or lower than RUNTIME (in seconds).
PROFILE_DURATION=${RUNTIME}

# Table settings for sysbench (used during prepare and run phases)
TABLES=${TABLES:-1}           # Number of tables used in the test
TABLE_SIZE=${TABLE_SIZE:-10000000}  # Number of rows per table

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
  "tidb_txn_assertion_level"
  "tidb_guarantee_linearizability"
)

# Define baseline and test states (you may adjust these values as needed)
declare -A BASELINE_VALUES=(
  ["tidb_use_plan_baselines"]="on"
  ["tidb_plan_cache_invalidation_on_fresh_stats"]="on"
  ["tidb_enable_stmt_summary"]="on"
  ["tidb_enable_collect_execution_info"]="on"
  ["tidb_enable_resource_control"]="on"
  ["tidb_txn_assertion_level"]="fast"
  ["tidb_guarantee_linearizability"]="on"
)

declare -A TEST_VALUES=(
  ["tidb_use_plan_baselines"]="off"
  ["tidb_plan_cache_invalidation_on_fresh_stats"]="off"
  ["tidb_enable_stmt_summary"]="off"
  ["tidb_enable_collect_execution_info"]="off"
  ["tidb_enable_resource_control"]="off"
  ["tidb_txn_assertion_level"]="off"
  ["tidb_guarantee_linearizability"]="off"
)

#######################
# Helper Functions
#######################

# Execute a SQL statement using the mysql client.
function exec_mysql() {
  local sql="$1"
  echo ">>> Executing: $sql"
  mysql -h "$TIDB_HOST" -P "$TIDB_PORT" -u "$TIDB_USER" -e "$sql"
}

# Set the state of a given feature variable (state can now be any value as defined).
function set_feature_state() {
  local feature="$1"
  local state="$2"  # expected value (can be "on"/"off" or other values as needed)
  exec_mysql "SET GLOBAL ${feature}=${state};"
}

# Restore all features to their baseline states.
function restore_all_features() {
  echo ">>> Restoring all features to baseline states..."
  local feature
  for feature in "${FEATURES[@]}"; do
    set_feature_state "$feature" "${BASELINE_VALUES[$feature]}"
  done
}

# Prepare the test environment:
# 1. Create the database (if it does not exist).
# 2. Run the sysbench prepare phase to create tables and insert test data.
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

# Run the sysbench benchmark and concurrently collect CPU and block profiles.
function run_benchmark() {
  local test_name="$1"
  local test_dir="$2"
  echo "==== Starting benchmark: [${test_name}] ===="
  local sysbench_log="${test_dir}/${test_name}_sysbench.log"
  local cpu_profile_file="${test_dir}/${test_name}_cpu.pprof"
  local block_profile_file="${test_dir}/${test_name}_block.pprof"

  # Construct the sysbench command.
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

  # Start block profile collection in the background.
  echo ">>> Starting block profile collection for ${PROFILE_DURATION} seconds..."
  curl -s -o "${block_profile_file}" "http://${TIDB_HOST}:${TIDB_PPROF_PORT}/debug/pprof/block?seconds=${PROFILE_DURATION}" &
  BLOCK_PROFILE_PID=$!

  # Run the sysbench benchmark, logging output.
  echo ">>> Running sysbench benchmark. Output will be logged to ${sysbench_log}"
  ${SYSBENCH_CMD} | tee "${sysbench_log}"

  # Wait for both profile collections to complete.
  wait "${CPU_PROFILE_PID}"
  wait "${BLOCK_PROFILE_PID}"

  echo "==== Finished benchmark: [${test_name}] ===="
  echo "   Sysbench log: ${sysbench_log}"
  echo "   CPU profile: ${cpu_profile_file}"
  echo "   Block profile: ${block_profile_file}"
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

# Record the start time for diagnostics collection.
METRICS_FROM=$(date -uIseconds)

# Preparation Phase:
prepare_database
echo ">>> Preparation complete. Waiting 60 seconds to ensure all settings take effect..."
sleep 60

# --- Warmup Phase ---
echo "=============================================="
echo ">>> Running warmup phase (this run is for warming up, results will be discarded)"
run_benchmark "warmup" "${RESULTS_DIR}"
echo ">>> Warmup phase complete. Waiting 60 seconds before starting tests..."
sleep 60

# Baseline Benchmark (all features in baseline state)
restore_all_features
echo ">>> Running baseline benchmark with all features in baseline state"
run_benchmark "baseline_all_on" "${RESULTS_DIR}"

# Test each feature individually by applying its test state.
for feature in "${FEATURES[@]}"; do
  echo "----------------------------------------------"
  echo "Testing feature: Setting ${feature} from baseline [${BASELINE_VALUES[$feature]}] to test state [${TEST_VALUES[$feature]}]"
  restore_all_features
  set_feature_state "${feature}" "${TEST_VALUES[$feature]}"
  echo ">>> Waiting 60 seconds..."
  sleep 60
  
  run_benchmark "feature_${feature}_test" "${RESULTS_DIR}"
done

# Optionally, run a benchmark with all features set to their test states.
echo "----------------------------------------------"
echo "Testing: Setting ALL features to their test states"
for feature in "${FEATURES[@]}"; do
  set_feature_state "${feature}" "${TEST_VALUES[$feature]}"
done
echo ">>> Waiting 60 seconds..."
sleep 60
run_benchmark "all_features_test" "${RESULTS_DIR}"

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