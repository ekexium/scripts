# TiDB TTL Benchmark Tool

A high-frequency stress testing tool for TiDB's TTL (Time to Live) functionality.

## Features

- **High-frequency testing**: Designed for quick results (default 60 seconds)
- **Concurrent operations**: Configurable write and read threads
- **Real-time monitoring**: Live statistics every 2 seconds
- **TTL effectiveness tracking**: Monitors data count changes over time
- **Multiple read patterns**: Point select, range select, and aggregate queries
- **Batch operations**: Optimized for high throughput

## Requirements

```bash
pip install pymysql
```

## Quick Start

### Basic Usage

```bash
# Run with default settings (60 seconds, 20 write threads, 10 read threads)
python tidb_ttl_benchmark.py --host 127.0.0.1 --port 4000

# Run with custom duration and threads
python tidb_ttl_benchmark.py --host 127.0.0.1 --duration 120 --write-threads 30 --read-threads 15

# Run with different TTL interval and job frequency
python tidb_ttl_benchmark.py --host 127.0.0.1 --ttl-interval "30 SECOND" --ttl-job-interval "20s"
```

### Command Line Options

```
--host              TiDB host address (default: 127.0.0.1)
--port              TiDB port (default: 4000)
--user              Database username (default: root)
--password          Database password (default: empty)
--database          Database name (default: ttl_benchmark)

--duration          Benchmark duration in seconds (default: 60)
--write-threads     Number of write threads (default: 20)
--read-threads      Number of read threads (default: 10)
--writes-per-second Target writes per second (default: 1000)
--ttl-interval      TTL expiration interval (default: "5 MINUTE")
--ttl-job-interval  TTL job execution interval (default: "1m")
```

## How It Works

### 1. Setup Phase
- Creates database and test table with TTL configuration
- Generates initial test data (10,000 records by default)
- Some initial data is created with varying ages to test TTL effectiveness

### 2. Benchmark Phase
- **Write workers**: Continuously insert new records in batches
- **Read workers**: Perform various read operations (point select, range queries, aggregations)
- **Statistics reporter**: Prints real-time performance metrics every 2 seconds

### 3. TTL Configuration
The tool creates tables with the following TTL settings:
```sql
CREATE TABLE ttl_test_table (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    data_field VARCHAR(1024),
    random_int INT,
    random_float DOUBLE,
    status TINYINT DEFAULT 1,
    INDEX idx_created_at (created_at),
    INDEX idx_random_int (random_int)
) TTL = `created_at` + INTERVAL 5 MINUTE
  TTL_ENABLE = ON
  TTL_JOB_INTERVAL = '1m'
```

### 4. Monitoring Output
During execution, you'll see real-time statistics:
```
[  12.0s] Writes:     6000 ( 500.0 TPS) | Reads:    300 ( 25.0 TPS) | Data:    15234 | Errors: W0/R0 | Latency: W2.1ms/R1.8ms
```

### 5. Final Report
After completion, the tool generates:
- JSON report file with detailed metrics
- Console summary with TTL effectiveness analysis
- Data count timeline showing TTL impact

## Example Output

```
TiDB TTL Benchmark Starting...
Database: 127.0.0.1:4000/ttl_benchmark
Duration: 60 seconds
Threads: 20 write, 10 read
Target: 1000 writes/second
TTL: 5 MINUTE expiration
--------------------------------------------------------------------------------

[   2.0s] Writes:     2000 (1000.0 TPS) | Reads:    100 ( 50.0 TPS) | Data:    12000 | Errors: W0/R0 | Latency: W1.5ms/R2.1ms
[   4.0s] Writes:     4000 (1000.0 TPS) | Reads:    200 ( 50.0 TPS) | Data:    14000 | Errors: W0/R0 | Latency: W1.6ms/R2.0ms
...

================================================================================
TiDB TTL Benchmark Results Summary
================================================================================
Duration:           60.0 seconds
Write Operations:   60,000 (1000.0 TPS)
Read Operations:    3,000 (50.0 TPS)
Write Latency:      1.52 ms
Read Latency:       2.03 ms
Errors:             Write: 0, Read: 0

TTL Effectiveness:
Initial Data Count: 10,000
Final Data Count:   15,234
Peak Data Count:    25,678
Net Change:         +5,234
TTL Working:        Yes
Data Stabilized:    No
================================================================================
```

## TTL Effectiveness Analysis

The tool analyzes TTL effectiveness by tracking:
- **Initial Count**: Data count at start
- **Final Count**: Data count at end
- **Peak Count**: Maximum data count during test
- **Net Change**: Final - Initial count
- **TTL Working**: Whether TTL successfully deleted some data
- **Data Stabilized**: Whether data count stabilized (within 10% of initial)

**Note**: With very short TTL intervals (like 10 seconds or 1 minute), you may see "Table doesn't exist" errors during the test. This is actually a **positive sign** that TTL is working aggressively and cleaning up expired data very quickly!

## Performance Tuning Tips

1. **For higher write throughput**: Increase `--write-threads` and `--writes-per-second`
2. **For TTL testing**: Use shorter `--ttl-interval` (e.g., "1 MINUTE") for faster results
3. **For load testing**: Increase `--duration` and monitor system resources
4. **For latency testing**: Reduce thread counts to minimize contention

## Troubleshooting

- **Connection errors**: Check TiDB host, port, and credentials
- **Permission errors**: Ensure user has CREATE DATABASE privileges
- **High latency**: Reduce concurrent threads or check TiDB cluster resources
- **TTL not working**: Verify TiDB version supports TTL and check TTL job status

## Files Generated

- `ttl_benchmark_report_<timestamp>.json`: Detailed benchmark results
- Log output to console with real-time statistics
