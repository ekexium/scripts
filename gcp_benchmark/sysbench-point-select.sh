#!/bin/bash
# run_benchmark.sh
# This script uses sysbench point select fixed-rate benchmark to evaluate the P99 latency.
# It also collects CPU profiles via TiDB's pprof endpoint during each run.
# Additionally, the script toggles specific TiDB features (via system variables) to measure the performance impact.
#
# Features to test (each can be disabled and the impact is measured):
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
TIDB_HOST=${TIDB_HOST:-"eke-bench-tidb-0"}
TIDB_PORT=${TIDB_PORT:-4000}
TIDB_USER=${TIDB_USER:-"root"}
TIDB_PASSWORD=${TIDB_PASSWORD:-""}
# TiDB pprof endpoint port (usually 10080)
TIDB_PPROF_PORT=${TIDB_PPROF_PORT:-10080}

# Sysbench settings (ensure sysbench is installed and that the test script path is valid)
# This example uses the oltp_point_select.lua test script. Adjust SYSBENCH_TEST_SCRIPT as appropriate.
SYSBENCH_PATH=${SYSBENCH_PATH:-"sysbench"}
SYSBENCH_TEST_SCRIPT=${SYSBENCH_TEST_SCRIPT:-"oltp_point_select"}
THREADS=${THREADS:-16}
RUNTIME=${RUNTIME:-60}         # Duration of the benchmark in seconds
RATE=${RATE:-1000}             # Fixed QPS rate
# Duration to collect CPU profile. It is suggested to be equal or less than RUNTIME (in seconds).
PROFILE_DURATION=${PROFILE_DURATION:-60}

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
  mysql -h "$TIDB_HOST" -P "$TIDB_PORT" -u "$TIDB_USER" -p"$TIDB_PASSWORD" -e "$sql"
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
  for feature in "${FEATURES[@]}"; do
    set_feature_state "$feature" "on"
  done
}

# Run the sysbench benchmark and collect a CPU profile concurrently
function run_benchmark() {
  local test_name="$1"
  local test_dir="$2"
  echo "==== Starting benchmark: [${test_name}] ===="
  local sysbench_log="${test_dir}/${test_name}_sysbench.log"
  local cpu_profile_file="${test_dir}/${test_name}_cpuprofile.pprof"

  # Construct the sysbench command
  SYSBENCH_CMD="${SYSBENCH_PATH} ${SYSBENCH_TEST_SCRIPT} \
    --threads=${THREADS} \
    --time=${RUNTIME} \
    --rate=${RATE} \
    --db-driver=mysql \
    --mysql-host=${TIDB_HOST} \
    --mysql-port=${TIDB_PORT} \
    --mysql-user=${TIDB_USER} \
    --mysql-password=${TIDB_PASSWORD} \
    run"

  echo ">>> Sysbench command: ${SYSBENCH_CMD}"

  # Start CPU profile collection in the background
  echo ">>> Starting CPU profile collection for ${PROFILE_DURATION} seconds..."
  curl -s -o "${cpu_profile_file}" "http://${TIDB_HOST}:${TIDB_PPROF_PORT}/debug/pprof/profile?seconds=${PROFILE_DURATION}" &
  CPU_PROFILE_PID=$!

  # Run the sysbench benchmark, directing output to a log file
  echo ">>> Running sysbench benchmark. Output will be logged to ${sysbench_log}"
  ${SYSBENCH_CMD} | tee "${sysbench_log}"

  # Wait for the CPU profile collection to complete
  wait "${CPU_PROFILE_PID}"
  echo "==== Finished benchmark: [${test_name}] ===="
  echo "   Sysbench log: ${sysbench_log}"
  echo "   CPU profile: ${cpu_profile_file}"
}

#######################
# Benchmark Workflow
#######################

# Restore all features to "on" state as the baseline, then run the baseline benchmark
restore_all_features
echo "Waiting 5 seconds to ensure all settings take effect..."
sleep 5

echo ">>> Running baseline benchmark with all features ON"
run_benchmark "baseline_all_on" "${RESULTS_DIR}"

# Test each feature individually by turning it off
for feature in "${FEATURES[@]}"; do
  echo "----------------------------------------------"
  echo "Testing: Disabling ${feature}"
  # First, restore all features then disable the current feature
  restore_all_features
  set_feature_state "${feature}" "off"
  echo "Waiting 5 seconds to ensure settings are applied..."
  sleep 5
  
  run_benchmark "feature_${feature}_off" "${RESULTS_DIR}"
done

# Optionally, run a benchmark with all features turned off
echo "----------------------------------------------"
echo "Testing: Disabling ALL features"
for feature in "${FEATURES[@]}"; do
  set_feature_state "${feature}" "off"
done
echo "Waiting 5 seconds to ensure settings are applied..."
sleep 5
run_benchmark "all_features_off" "${RESULTS_DIR}"

echo "=============================================="
echo "All tests completed. Results are stored in directory: ${RESULTS_DIR}"