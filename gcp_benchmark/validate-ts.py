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
        self.verbose = args.verbose
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
                    get_warnings=True,
                    autocommit=True  # Ensure queries are not in transaction
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
                database=self.database,
                autocommit=True  # Ensure queries are not in transaction
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
                    id INT PRIMARY KEY
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
                    values.append(f"({j})")
                
                sql = f"INSERT INTO {self.table_name} (id) VALUES {','.join(values)}"
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
            # Only show connection messages in verbose mode
            if self.verbose:
                print(f"Client {client_id}: Connecting to TiDB...")
                
            conn = mysql.connector.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database,
                connection_timeout=300,
                autocommit=True  # Ensure queries are not in transaction
            )
            cursor = conn.cursor()
            
            if self.verbose:
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
                
                # Only client 0 reports table verification in non-verbose mode
                if self.verbose or client_id == 0:
                    print(f"Client {client_id}: Table verification successful. Found {test_result[0]} total rows.")
            except Exception as e:
                print(f"Client {client_id}: ERROR - Cannot access test table: {e}")
                raise
            
            # Execute the main test queries
            while time.time() - start_time < self.duration:
                try:
                    # Use fixed future timestamp (current time + fixed milliseconds)
                    # Convert milliseconds to microseconds (1 millisecond = 1000 microseconds)
                    microseconds = self.future_ts * 1000
                    future_ts = f"NOW() + INTERVAL {microseconds} MICROSECOND"
                    
                    # Simple COUNT(*) query with future timestamp
                    query = f"SELECT COUNT(*) FROM {self.table_name} AS OF TIMESTAMP {future_ts}"
                    
                    cursor.execute(query)
                    result = cursor.fetchone()
                    
                    # Count records found
                    record_count = result[0] if result else 0
                    records_scanned += record_count
                    
                    query_count += 1
                    
                    # Progress reporting (further reduced frequency and only for client 0 unless verbose)
                    if self.verbose:
                        if query_count % 1000 == 0 and client_id < 3:
                            elapsed = time.time() - start_time
                            print(f"Client {client_id}: Progress: {query_count} queries in {elapsed:.1f}s ({query_count/elapsed:.1f} qps)")
                    elif client_id == 0 and query_count % 5000 == 0:
                        elapsed = time.time() - start_time
                        print(f"Progress: {query_count} queries in {elapsed:.1f}s ({query_count/elapsed:.1f} qps)")
                        
                except Exception as e:
                    error_count += 1
                    # Only report the first error and then every 100th error in non-verbose mode
                    if self.verbose and (error_count == 1 or (error_count <= 10 and error_count % 5 == 0) or error_count % 100 == 0):
                        print(f"Client {client_id}: Query failed ({error_count} times): {str(e)}...")
                    elif not self.verbose and (error_count == 1 or error_count % 500 == 0):
                        print(f"Client {client_id}: Query failed ({error_count} times): {str(e)}...")
                    
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
                                connection_timeout=300,
                                autocommit=True  # Ensure queries are not in transaction
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
                
            print(f"Client {client_id} completed: {query_count} queries, {error_count} errors, {records_scanned:,} records")
            
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
                
                # Report progress for large concurrency values (less frequently)
                if concurrency > 50 and i > 0 and i % 50 == 0:
                    print(f"Started {i} of {concurrency} clients...")
            
            # Wait for clients to complete with progress reporting
            completed = 0
            for future in concurrent.futures.as_completed(client_futures):
                try:
                    future.result()
                    completed += 1
                    if concurrency > 50 and completed % 50 == 0:
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
        
        # Calculate total attempts (including errors) for QPS
        total_attempts = total_queries + total_errors
        
        # Calculate QPS based on all attempts (including errors)
        attempts_per_sec = total_attempts / actual_duration if actual_duration > 0 else 0
        
        # Calculate successful QPS (may be 0 or very low with future timestamp)
        successful_qps = total_queries / actual_duration if actual_duration > 0 else 0
        
        # Calculate records scan rate
        records_per_sec = total_records / actual_duration if actual_duration > 0 else 0
        
        # Calculate error rate
        error_rate = total_errors / total_attempts if total_attempts > 0 else 0
        
        result = {
            "concurrency": concurrency,
            "duration": actual_duration,
            "total_attempts": total_attempts,
            "total_queries": total_queries,
            "total_errors": total_errors,
            "total_records_scanned": total_records,
            "attempts_per_sec": attempts_per_sec,
            "successful_qps": successful_qps,
            "records_per_sec": records_per_sec,
            "error_rate": error_rate
        }
        
        self.test_results[concurrency] = result
        
        print(f"Test results for concurrency {concurrency}:")
        print(f"  Actual test duration: {actual_duration:.2f} seconds")
        print(f"  Total query attempts: {total_attempts}")
        print(f"  Successful queries: {total_queries}")
        print(f"  Failed queries: {total_errors}")
        print(f"  Total records scanned: {total_records:,}")
        print(f"  Attempts/sec: {attempts_per_sec:.2f}")
        if total_queries > 0:
            print(f"  Successful QPS: {successful_qps:.2f}")
        print(f"  Error rate: {error_rate*100:.2f}%")
            
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
        headers = ["Concurrency", "Total Attempts", "Success", "Errors", "Attempts/sec", "Error Rate(%)"]
        rows = []
        
        for concurrency, result in sorted(self.test_results.items()):
            rows.append([
                concurrency,
                f"{result['total_attempts']:,}",
                f"{result['total_queries']:,}",
                f"{result['total_errors']:,}",
                f"{result['attempts_per_sec']:.2f}",
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
    parser.add_argument('--verbose', action='store_true', help='Enable verbose logging')

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
