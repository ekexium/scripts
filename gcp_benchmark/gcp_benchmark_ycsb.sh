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
    local concurrency="$3"
    echo "Executing: $sql (flush_concurrency: $concurrency)"

    # Record start time in RFC3339 format
    start_time=$(date +%s)
    start_time_iso=$(date -u +"%Y-%m-%dT%H:%M:%SZ" -d @"$start_time")

    # Execute SQL and get flush_wait_ms in the same session
    result=$($MYSQL_CMD <<EOF
SET GLOBAL tidb_pipelined_flush_concurrency = $concurrency;
$sql;
SELECT @@tidb_last_txn_info;
EOF
)

    # Extract the flush_wait_ms value
    flush_wait_ms=$(echo "$result" | grep -o '"flush_wait_ms":[0-9]*' | awk -F':' '{print $2}')

    # Convert milliseconds to seconds
    flush_wait_seconds=$(awk "BEGIN {printf \"%.3f\", $flush_wait_ms / 1000}")

    # Record end time in RFC3339 format
    end_time=$(date +%s)
    end_time_iso=$(date -u +"%Y-%m-%dT%H:%M:%SZ" -d @"$end_time")

    execution_time=$((end_time - start_time))
    echo "Execution time: $execution_time seconds"
    echo "Flush wait time: $flush_wait_seconds seconds"

    # Collect Prometheus metrics
    echo "Collecting Prometheus metrics for ${operation_name}_concurrency_${concurrency}..."

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

    # Fetch metrics
    tidb_cpu_response=$(query_prometheus "$TIDB_CPU_QUERY" "$start_time_iso" "$end_time_iso")
    tidb_cpu_avg=$(echo "$tidb_cpu_response" | jq -r '.data.result[0].values | map(.[1]|tonumber) | add / length')

    tikv_cpu_response=$(query_prometheus "$AVG_TIKV_CPU_QUERY" "$start_time_iso" "$end_time_iso")
    tikv_cpu_avg=$(echo "$tikv_cpu_response" | jq -r '.data.result[0].values | map(.[1]|tonumber) | add / length')
    tikv_max_cpu_response=$(query_prometheus "$MAX_TIKV_CPU_QUERY" "$start_time_iso" "$end_time_iso")
    tikv_max_cpu_avg=$(echo "$tikv_max_cpu_response" | jq -r '.data.result[0].values | map(.[1]|tonumber) | add / length')

    tidb_memory_response=$(query_prometheus "$TIDB_MEMORY_QUERY" "$start_time_iso" "$end_time_iso")
    tidb_memory_max=$(echo "$tidb_memory_response" | jq -r '.data.result[0].values | map(.[1]|tonumber) | max')

    # Save metrics to a file
    {
        echo "Concurrency: ${concurrency}"
        echo "Execution Time: ${execution_time} seconds"
        echo "Flush Wait Time: ${flush_wait_seconds} seconds"
        echo "TiDB CPU Avg: ${tidb_cpu_avg}%"
        echo "TiKV CPU Avg: ${tikv_cpu_avg}%"
        echo "TiKV CPU Max: ${tikv_max_cpu_avg}%"
        echo "TiDB Memory Max: ${tidb_memory_max} GB"
        echo "-------------------------"
    } >> "${operation_name}_metrics.txt"

    echo "Metrics saved to ${operation_name}_metrics.txt"
    echo "-------------------------"
}

# Function to run benchmark with specific concurrency
run_benchmark() {
    local concurrency=$1
    
    echo "Running benchmark with flush_concurrency = $concurrency"
    
    # Initialization
    echo "Initializing..."
    $MYSQL_CMD -e "drop table if exists usertable_2"
    $MYSQL_CMD -e "set global tidb_shard_allocate_step=32;"
    $MYSQL_CMD -e "set global tidb_scatter_region = ON" # for v7.5
    $MYSQL_CMD -e "set global tidb_scatter_region = 'table'" # for v8.4
    $MYSQL_CMD -e "set global tidb_pipelined_flush_concurrency = $concurrency;"
    $MYSQL_CMD -e "set @@global.tidb_mem_quota_query=64<<30" # 64 GiB
    $MYSQL_CMD -e 'CREATE TABLE `usertable_2` (
      `YCSB_KEY` varchar(64) NOT NULL,
      `FIELD0` varchar(100) DEFAULT NULL,
      `FIELD1` varchar(100) DEFAULT NULL,
      `FIELD2` varchar(100) DEFAULT NULL,
      `FIELD3` varchar(100) DEFAULT NULL,
      `FIELD4` varchar(100) DEFAULT NULL,
      `FIELD5` varchar(100) DEFAULT NULL,
      `FIELD6` varchar(100) DEFAULT NULL,
      `FIELD7` varchar(100) DEFAULT NULL,
      `FIELD8` varchar(100) DEFAULT NULL,
      `FIELD9` varchar(100) DEFAULT NULL,
      PRIMARY KEY (`YCSB_KEY`) /*T![clustered_index] NONCLUSTERED */
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin SHARD_ROW_ID_BITS=8 PRE_SPLIT_REGIONS=8'
    $MYSQL_CMD -e "SPLIT TABLE usertable_2 INDEX \`PRIMARY\` BETWEEN (\"user10\") AND (\"user99\") REGIONS 89;"
    sleep 10
    echo "Initialization complete."
    echo "-------------------------"

    echo "Sleeping for 10 seconds"
    sleep 10

    # DML 1
    execute_sql_and_profile "insert /*+ SET_VAR(tidb_dml_type=bulk) */ into usertable_2 select * from usertable where _tidb_rowid < $ROW_LIMIT;" "dml1_insert" "$concurrency"

    # Sleep for 5 minutes
    echo "Sleeping for 5 minutes..."
    sleep 300

    # DML 2
    execute_sql_and_profile "update /*+ SET_VAR(tidb_dml_type=bulk) */ usertable_2 set field0 = LEFT(CONCAT(' ', FIELD0), 100)" "dml2_update" "$concurrency"

    # Sleep for 5 minutes
    echo "Sleeping for 5 minutes..."
    sleep 300

    # DML 3
    execute_sql_and_profile "delete /*+ SET_VAR(tidb_dml_type=bulk) */ from usertable_2" "dml3_delete" "$concurrency"
}

# Define concurrency values to test
CONCURRENCY_VALUES=(2 4 8 16 32 128 512)

# Main loop to test different concurrency values
for concurrency in "${CONCURRENCY_VALUES[@]}"; do
    echo "Testing with flush_concurrency = $concurrency"
    run_benchmark "$concurrency"
    echo "Completed testing for flush_concurrency = $concurrency"
    echo "Sleeping for 5 minutes before next test..."
    sleep 300
done

echo "All tests completed."