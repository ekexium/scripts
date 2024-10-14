#!/bin/bash

# Usage: run with a large enough workload, otherwise metrics cannot reflect the actual performance.

# Check if the required parameter is provided
if [ $# -ne 1 ]; then
    echo "Error: Missing required parameter."
    echo "Usage: $0 <row_limit>"
    exit 1
fi

# Assign the parameter to a variable
ROW_LIMIT=$1

# Validate that the input is a positive integer
if ! [[ "$ROW_LIMIT" =~ ^[0-9]+$ ]] || [ "$ROW_LIMIT" -eq 0 ]; then
    echo "Error: Please provide a positive integer for row_limit."
    exit 1
fi

# MySQL connection details
MYSQL_CMD="mysql --comments --host bench-pdml-tidb-0 --port 4000 -uroot --database ycsb_1e8_non_clustered"

# Prometheus server URL
PROMETHEUS_URL="http://bench-pdml-load:9090"

# Prometheus queries
TIDB_CPU_QUERY="avg(rate(process_cpu_seconds_total{job=\"tidb\"}[30s])) * 100"
AVG_TIKV_CPU_QUERY="avg(rate(process_cpu_seconds_total{job=\"tikv\"}[30s])) * 100"
MAX_TIKV_CPU_QUERY="max(rate(process_cpu_seconds_total{job=\"tikv\"}[30s])) * 100"
TIDB_MEMORY_QUERY="max(sum((process_resident_memory_bytes{job=~\"tidb\"})) by (instance)) / (1024 * 1024 * 1024)" # to GiB

# Function to execute SQL, measure time, profile, and collect Prometheus metrics
execute_sql_and_profile() {
    local sql="$1"
    local operation_name="$2"
    echo "Executing: $sql"

    # Start profiling
    # curl http://bench-pdml-tidb-0:10080/debug/pprof/profile?seconds=30 > "${operation_name}_cpu.pprof" &
    # PROFILE_PID=$!

    # Record start time in RFC3339 format
    start_time=$(date +%s)
    start_time_iso=$(date -u +"%Y-%m-%dT%H:%M:%SZ" -d @"$start_time")

    # Execute SQL
    $MYSQL_CMD -e "$sql"

    # Record end time in RFC3339 format
    end_time=$(date +%s)
    end_time_iso=$(date -u +"%Y-%m-%dT%H:%M:%SZ" -d @"$end_time")

    # Wait for profiling to complete (30 seconds by default)
    # wait $PROFILE_PID

    execution_time=$((end_time - start_time))
    echo "Execution time: $execution_time seconds"

    # Collect Prometheus metrics
    echo "Collecting Prometheus metrics for ${operation_name}..."

    # Function to query Prometheus
    query_prometheus() {
        local query="$1"
        local start="$2"
        local end="$3"
        local step="15s"

        curl -s -G "$PROMETHEUS_URL/api/v1/query_range" \
            --data-urlencode "query=$query" \
            --data-urlencode "start=$start" \
            --data-urlencode "end=$end" \
            --data-urlencode "step=$step"
    }

    # Fetch TiDB CPU average
    tidb_cpu_response=$(query_prometheus "$TIDB_CPU_QUERY" "$start_time_iso" "$end_time_iso")
    tidb_cpu_avg=$(echo "$tidb_cpu_response" | jq -r '.data.result[0].values | map(.[1]|tonumber) | add / length')

    # Fetch TiKV CPU average
    tikv_cpu_response=$(query_prometheus "$AVG_TIKV_CPU_QUERY" "$start_time_iso" "$end_time_iso")
    tikv_cpu_avg=$(echo "$tikv_cpu_response" | jq -r '.data.result[0].values | map(.[1]|tonumber) | add / length')
    tikv_max_cpu_response=$(query_prometheus "$MAX_TIKV_CPU_QUERY" "$start_time_iso" "$end_time_iso")
    tikv_max_cpu_avg=$(echo "$tikv_max_cpu_response" | jq -r '.data.result[0].values | map(.[1]|tonumber) | add / length')

    # Fetch TiDB Memory max
    tidb_memory_response=$(query_prometheus "$TIDB_MEMORY_QUERY" "$start_time_iso" "$end_time_iso")
    tidb_memory_max=$(echo "$tidb_memory_response" | jq -r '.data.result[0].values | map(.[1]|tonumber) | max')

    # Save metrics to a file
    echo "TiDB CPU Avg: ${tidb_cpu_avg}%" >> "${operation_name}_metrics.txt"
    echo "TiKV CPU Avg: ${tikv_cpu_avg}%" >> "${operation_name}_metrics.txt"
    echo "TiKV CPU Max: ${tikv_max_cpu_avg}%" >> "${operation_name}_metrics.txt"
    echo "TiDB Memory Max: ${tidb_memory_max} MB" >> "${operation_name}_metrics.txt"

    echo "Metrics saved to ${operation_name}_metrics.txt"
    echo "-------------------------"
}

# Initialization
echo "Start the benchmark. Initializing..."
$MYSQL_CMD -e "drop table if exists usertable_2"
$MYSQL_CMD -e "create table usertable_2 like usertable"
$MYSQL_CMD -e "set @@global.tidb_mem_quota_query=64<<30" # 64 GiB
echo "Initialization complete."
echo "-------------------------"

echo "Sleeping for 10 seconds"
sleep 10

# DML 1
execute_sql_and_profile "insert /*+ SET_VAR(tidb_dml_type=bulk) */ into usertable_2 select * from usertable where _tidb_rowid < $ROW_LIMIT;" "dml1_insert"

# Sleep for 5 minutes
echo "Sleeping for 5 minutes..."
sleep 300

# DML 2
execute_sql_and_profile "update /*+ SET_VAR(tidb_dml_type=bulk) */ usertable_2 set field0 = LEFT(CONCAT(' ', FIELD0), 100)" "dml2_update"

# Sleep for 5 minutes
echo "Sleeping for 5 minutes..."
sleep 300

# DML 3
execute_sql_and_profile "delete /*+ SET_VAR(tidb_dml_type=bulk) */ from usertable_2" "dml3_delete"

echo "All operations completed."