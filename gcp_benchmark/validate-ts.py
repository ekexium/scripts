#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import concurrent.futures
import json
import random
import signal
import sys
import time
import threading  # Added for thread synchronization

import mysql.connector

class TiDBFutureTSTest:
    def __init__(self, args):
        self.host = args.host
        self.port = args.port
        self.user = args.user
        self.password = args.password
        self.database = args.database
        self.table_name = args.table_name
        self.rows = args.rows
        self.region_count = args.region_count
        self.duration = args.duration
        self.concurrency_levels = args.concurrency_levels
        self.future_ts = args.future_ts
        self.cooldown = args.cooldown
        self.test_results = {}
        self.conn = None
        self.cursor = None
        self.counter_lock = threading.Lock()  # Lock for thread safety

    def connect(self):
        """Establish connection to TiDB"""
        retry_count = 0
        max_retries = 3
        retry_interval = 2  # seconds
        
        while retry_count < max_retries:
            try:
                self.conn = mysql.connector.connect(
                    host=self.host,
                    port=self.port,
                    user=self.user,
                    password=self.password,
                    database=self.database,
                    connection_timeout=10,
                    get_warnings=True
                )
                self.cursor = self.conn.cursor()
                print(f"Connected to TiDB: {self.host}:{self.port}")
                return True
            except mysql.connector.Error as err:
                retry_count += 1
                if retry_count < max_retries:
                    print(f"Connection attempt {retry_count} failed: {err}. Retrying in {retry_interval} seconds...")
                    time.sleep(retry_interval)
                    retry_interval *= 2  # Exponential backoff
                else:
                    print(f"Failed to connect to TiDB after {max_retries} attempts: {err}")
                    return False
            except Exception as e:
                print(f"Unexpected error connecting to TiDB: {e}")
                return False

    def close(self):
        """Close database connection safely"""
        try:
            # Attempt to consume any unread results
            if self.cursor:
                try:
                    while self.cursor.nextset():
                        pass
                except:
                    pass
                self.cursor.close()
                
            if self.conn:
                try:
                    self.conn.commit()  # Commit any pending transactions
                except:
                    pass
                    
                # If there are still unread results, we need to handle them
                try:
                    self.conn.cmd_reset_connection()  # Reset the connection state
                except:
                    pass
                    
                self.conn.close()
                
            print("Database connection closed")
        except Exception as e:
            print(f"Error during connection cleanup: {e}")
            # Last resort - try to force close if regular close fails
            try:
                if self.conn and self.conn.is_connected():
                    self.conn.close()
            except:
                pass

    def split_table(self):
        """Split table into multiple regions safely"""
        try:
            print(f"Splitting table into {self.region_count} regions...")
            
            # Use a separate connection for SPLIT operations to isolate any result handling issues
            split_conn = mysql.connector.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database
            )
            split_cursor = split_conn.cursor()
            
            try:
                # Execute the SPLIT TABLE command
                split_cursor.execute(f"SPLIT TABLE {self.table_name} BETWEEN (0) AND ({self.rows}) REGIONS {self.region_count}")
                
                # Make sure to consume all results
                while split_cursor.nextset():
                    pass
                    
                # Verify the split worked by checking region count
                split_cursor.execute(f"SHOW TABLE {self.table_name} REGIONS")
                regions = split_cursor.fetchall()
                region_count = len(regions)
                print(f"Table {self.table_name} now has {region_count} regions")
                
                return region_count
                
            finally:
                # Clean up the split connection resources
                try:
                    split_cursor.close()
                except:
                    pass
                try:
                    split_conn.close()
                except:
                    pass
                    
        except Exception as e:
            print(f"Failed to split table: {e}")
            return 0

    def setup_table(self):
        """Create test table and insert data"""
        try:
            # Drop existing table if any
            self.cursor.execute(f"DROP TABLE IF EXISTS {self.table_name}")
            
            # Create test table
            print(f"Creating test table {self.table_name}...")
            self.cursor.execute(f"""
                CREATE TABLE {self.table_name} (
                    id BIGINT PRIMARY KEY,
                    value VARCHAR(100),
                    region_key INT
                )
            """)

            # Insert test data with optimized batch size to reduce memory pressure
            print(f"Inserting {self.rows} rows of test data...")
            batch_size = 5000  # Smaller batch size to reduce memory usage
            progress_step = max(1, self.rows // 20)  # Show progress every 5%
            
            for i in range(0, self.rows, batch_size):
                values = []
                end = min(i + batch_size, self.rows)
                for j in range(i, end):
                    region_key = j % self.region_count
                    values.append(f"({j}, 'value_{j}', {region_key})")
                
                sql = f"INSERT INTO {self.table_name} (id, value, region_key) VALUES {','.join(values)}"
                self.cursor.execute(sql)
                self.conn.commit()
                
                # Show progress at regular intervals
                if i % progress_step < batch_size:
                    progress = min(100, round((end / self.rows) * 100))
                    print(f"Inserted {end:,} / {self.rows:,} rows ({progress}%)")

            # Split the table using a separate method
            region_count = self.split_table()
            if region_count == 0:
                print("Warning: Table splitting may have failed")

            # Add an index on region_key to ensure cross-region scan
            self.cursor.execute(f"CREATE INDEX idx_region_key ON {self.table_name} (region_key)")
            self.conn.commit()
            
            return True
        except Exception as e:
            print(f"Failed to set up test table: {e}")
            # Try to consume any unread results on error
            try:
                while self.cursor and self.cursor.nextset():
                    pass
            except:
                pass
            return False

    def run_client(self, client_id, queries_counter):
        """Run a single client thread for testing"""
        conn = None
        cursor = None
        try:
            # Create an independent connection
            print(f"Client {client_id}: Connecting to TiDB...")
            conn = mysql.connector.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database,
                connection_timeout=300  # Increase connection timeout
            )
            cursor = conn.cursor()
            print(f"Client {client_id}: Connected successfully")
            
            start_time = time.time()
            query_count = 0
            error_count = 0
            records_scanned = 0
            
            # First, test if the table exists and is accessible
            try:
                test_query = f"SELECT COUNT(*) FROM {self.table_name} LIMIT 1"
                cursor.execute(test_query)
                test_result = cursor.fetchone()
                print(f"Client {client_id}: Table verification successful. Found {test_result[0]} total rows.")
            except Exception as e:
                print(f"Client {client_id}: ERROR - Cannot access test table: {e}")
                raise
            
            # Execute the main test queries
            while time.time() - start_time < self.duration:
                try:
                    # Use fixed future timestamp (current time + fixed milliseconds)
                    future_ts = f"NOW() + INTERVAL {self.future_ts} MILLISECOND"
                    
                    # Randomly select region_key range, ensuring at least 1 cross-region
                    min_region = random.randint(0, self.region_count - 2)
                    max_region = random.randint(min_region + 1, self.region_count - 1)
                    
                    # Execute cross-region query
                    query = f"""
                    SELECT COUNT(*) FROM {self.table_name} AS OF TIMESTAMP {future_ts}
                    WHERE region_key BETWEEN {min_region} AND {max_region}
                    """
                    
                    # Log the first few queries for debugging
                    if query_count < 2:
                        print(f"Client {client_id}: Executing query: {query}")
                    
                    cursor.execute(query)
                    result = cursor.fetchone()
                    
                    # Log the first few results for debugging
                    if query_count < 2:
                        print(f"Client {client_id}: Query result: {result}")
                    
                    # Accumulate scanned records
                    if result and result[0]:
                        records_scanned += result[0]
                    
                    query_count += 1
                    
                    # Progress reporting
                    if query_count % 100 == 0:
                        elapsed = time.time() - start_time
                        print(f"Client {client_id}: Progress: {query_count} queries in {elapsed:.1f}s ({query_count/elapsed:.1f} qps)")
                        
                except Exception as e:
                    error_count += 1
                    if error_count % 10 == 1 or error_count < 5:  # Print more errors at the start
                        print(f"Client {client_id} query failed ({error_count} times): {e}")
                        print(f"Query was: {query}")
                    
                    # Try to reconnect if connection is lost
                    if conn and not conn.is_connected():
                        try:
                            if cursor:
                                cursor.close()
                            if conn:
                                conn.close()
                                
                            print(f"Client {client_id}: Reconnecting to database...")
                            conn = mysql.connector.connect(
                                host=self.host,
                                port=self.port,
                                user=self.user,
                                password=self.password,
                                database=self.database,
                                connection_timeout=300
                            )
                            cursor = conn.cursor()
                            print(f"Client {client_id} reconnected")
                        except Exception as conn_err:
                            print(f"Client {client_id} reconnection failed: {conn_err}")
                    
                    time.sleep(0.1)  # Avoid immediate retry after failure

            # Thread-safe update of shared counter
            local_result = {
                "queries": query_count, 
                "errors": error_count,
                "records_scanned": records_scanned
            }
            with self.counter_lock:
                queries_counter.append(local_result)
                
            print(f"Client {client_id} completed: {query_count} queries executed, {error_count} failures, {records_scanned:,} records scanned")
            
        except Exception as e:
            print(f"Client {client_id} failed with fatal error: {e}")
            # Try to add empty result to avoid crashes in result processing
            with self.counter_lock:
                queries_counter.append({"queries": 0, "errors": 1, "records_scanned": 0})
        finally:
            if cursor:
                try:
                    cursor.close()
                except:
                    pass
            if conn:
                try:
                    conn.close()
                except:
                    pass

    def run_test(self, concurrency):
        """Run test with specified concurrency"""
        print(f"\nStarting test with concurrency {concurrency}...")
        
        # Initialize the thread-safe counter
        queries_counter = []
        
        # Limit max concurrent connections to avoid overwhelming the database
        max_workers = min(concurrency, 100)  # Cap at 100 connections max
        if max_workers < concurrency:
            print(f"Warning: Limiting worker pool to {max_workers} (requested: {concurrency})")
            print(f"Will run {concurrency} virtual clients distributed across {max_workers} worker threads")
        
        # Start concurrent clients
        client_pool = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)
        client_futures = []
        
        start_time = time.time()
        active_clients = 0
        
        try:
            # Submit client tasks
            for i in range(concurrency):
                future = client_pool.submit(self.run_client, i, queries_counter)
                client_futures.append(future)
                active_clients += 1
                
                # Report progress for large concurrency values
                if concurrency > 20 and i > 0 and i % 20 == 0:
                    print(f"Started {i} of {concurrency} clients...")
            
            # Wait for clients to complete with progress reporting
            completed = 0
            for future in concurrent.futures.as_completed(client_futures):
                try:
                    future.result()
                    completed += 1
                    if concurrency > 20 and completed % 20 == 0:
                        print(f"Completed {completed} of {concurrency} clients...")
                except Exception as e:
                    print(f"Client execution error: {e}")
                    active_clients -= 1
        finally:
            # Ensure thread pool is closed
            print(f"Waiting for all {active_clients} active clients to complete...")
            client_pool.shutdown(wait=True)
        
        end_time = time.time()
        actual_duration = end_time - start_time
        
        # Collect and return results
        total_queries = sum(item["queries"] for item in queries_counter)
        total_errors = sum(item["errors"] for item in queries_counter)
        total_records = sum(item["records_scanned"] for item in queries_counter)
        qps = total_queries / actual_duration if actual_duration > 0 else 0
        records_per_sec = total_records / actual_duration if actual_duration > 0 else 0
        
        result = {
            "concurrency": concurrency,
            "duration": actual_duration,
            "total_queries": total_queries,
            "total_errors": total_errors,
            "total_records_scanned": total_records,
            "qps": qps,
            "records_per_sec": records_per_sec,
            "error_rate": (total_errors / total_queries if total_queries > 0 else 0)
        }
        
        self.test_results[concurrency] = result
        
        print(f"Test results for concurrency {concurrency}:")
        print(f"  Actual test duration: {actual_duration:.2f} seconds")
        print(f"  Total queries: {total_queries}")
        print(f"  Total errors: {total_errors}")
        print(f"  Total records scanned: {total_records:,}")
        print(f"  QPS: {qps:.2f}")
        print(f"  Records scan rate: {records_per_sec:.2f} records/sec")
            
        # System cooldown between tests
        print(f"Waiting for system cooldown, {self.cooldown} seconds...")
        time.sleep(self.cooldown)
        
        return result

    def run_all_tests(self):
        """Run all concurrency level tests"""
        for concurrency in self.concurrency_levels:
            self.run_test(concurrency)
        
        self.generate_report()

    def generate_report(self):
        """Generate test report"""
        if not self.test_results:
            print("No test results available for report")
            return
            
        print("\n===== TEST REPORT =====")
        print(f"Test table: {self.table_name}")
        print(f"Row count: {self.rows}")
        print(f"Test duration per concurrency: {self.duration} seconds")
        print(f"Region count: {self.region_count}")
        print(f"Future timestamp: {self.future_ts} milliseconds\n")
        
        # Create results table
        headers = ["Concurrency", "Total Queries", "Total Errors", "QPS", "Records/sec", "Error Rate(%)"]
        rows = []
        
        for concurrency, result in sorted(self.test_results.items()):
            rows.append([
                concurrency,
                f"{result['total_queries']:,}",
                f"{result['total_errors']:,}",
                f"{result['qps']:.2f}",
                f"{result['records_per_sec']:.2f}",
                f"{result['error_rate']*100:.2f}%"
            ])
        
        # Print results table
        print("Test Results:")
        print("-" * 80)
        print(f"{headers[0]:<10} {headers[1]:<15} {headers[2]:<15} {headers[3]:<10} {headers[4]:<12} {headers[5]:<15}")
        print("-" * 80)
        for row in rows:
            print(f"{row[0]:<10} {row[1]:<15} {row[2]:<15} {row[3]:<10} {row[4]:<12} {row[5]:<15}")
        print("-" * 80)
        
        # Save raw results
        try:
            with open("future_ts_test_results.json", "w") as f:
                json.dump(self.test_results, f, indent=2)
            print("Test results saved to future_ts_test_results.json")
        except Exception as e:
            print(f"Failed to save test results: {e}")

def parse_arguments():
    parser = argparse.ArgumentParser(description='TiDB Future TS Read Test')
    parser.add_argument('--host', default='127.0.0.1', help='TiDB host')
    parser.add_argument('--port', type=int, default=4000, help='TiDB port')
    parser.add_argument('--user', default='root', help='TiDB username')
    parser.add_argument('--password', default='', help='TiDB password')
    parser.add_argument('--database', default='test', help='Database name')
    parser.add_argument('--table-name', default='future_ts_test', help='Test table name')
    parser.add_argument('--rows', type=int, default=1000000, help='Number of rows in test table')
    parser.add_argument('--region-count', type=int, default=10, help='Number of regions to split table into')
    parser.add_argument('--duration', type=int, default=60, help='Duration of each test (seconds)')
    parser.add_argument('--concurrency-levels', type=int, nargs='+', default=[1, 5, 10, 20, 50, 100], 
                        help='Concurrency levels to test')
    parser.add_argument('--future-ts', type=int, default=1000, help='Fixed future timestamp in milliseconds')
    parser.add_argument('--cooldown', type=int, default=60, help='Cooldown time between tests (seconds)')

    return parser.parse_args()

def signal_handler(sig, frame):
    print('Test interrupted')
    sys.exit(0)

def main():
    signal.signal(signal.SIGINT, signal_handler)
    args = parse_arguments()
    
    test = TiDBFutureTSTest(args)
    if test.connect():
        # Ask whether to refresh the test table
        setup = input("Re-create test table (y/n)? [y]: ").lower() or 'y'
        if setup == 'y':
            if not test.setup_table():
                print("Failed to create test table, test aborted")
                test.close()
                sys.exit(1)
        
        test.run_all_tests()
        test.close()
    else:
        sys.exit(1)

if __name__ == '__main__':
    main()
