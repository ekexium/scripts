#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import concurrent.futures
import json
import random
import signal
import sys
import time

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

    def connect(self):
        """Establish connection to TiDB"""
        try:
            self.conn = mysql.connector.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database
            )
            self.cursor = self.conn.cursor()
            print(f"Connected to TiDB: {self.host}:{self.port}")
            return True
        except Exception as e:
            print(f"Failed to connect to TiDB: {e}")
            return False

    def close(self):
        """Close database connection"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        print("Database connection closed")

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

            # Insert test data
            print(f"Inserting {self.rows} rows of test data...")
            batch_size = 10000
            for i in range(0, self.rows, batch_size):
                values = []
                end = min(i + batch_size, self.rows)
                for j in range(i, end):
                    region_key = j % self.region_count
                    values.append(f"({j}, 'value_{j}', {region_key})")
                
                sql = f"INSERT INTO {self.table_name} (id, value, region_key) VALUES {','.join(values)}"
                self.cursor.execute(sql)
                self.conn.commit()
                print(f"Inserted {end} / {self.rows} rows")

            # Split table into multiple regions using the correct syntax
            print(f"Splitting table into {self.region_count} regions...")
            # SPLIT TABLE syntax: SPLIT TABLE table_name BETWEEN (lower_value) AND (upper_value) REGIONS region_num
            # Use a single split command to evenly split the table
            self.cursor.execute(f"SPLIT TABLE {self.table_name} BETWEEN (0) AND ({self.rows}) REGIONS {self.region_count}")
                
            # Verify region count
            self.cursor.execute(f"SHOW TABLE {self.table_name} REGIONS")
            regions = self.cursor.fetchall()
            print(f"Table {self.table_name} now has {len(regions)} regions")

            # Add an index on region_key to ensure cross-region scan
            self.cursor.execute(f"CREATE INDEX idx_region_key ON {self.table_name} (region_key)")
            self.conn.commit()
            
            return True
        except Exception as e:
            print(f"Failed to set up test table: {e}")
            return False

    def run_client(self, client_id, queries_counter):
        """Run a single client thread for testing"""
        conn = None
        cursor = None
        try:
            # Create an independent connection
            conn = mysql.connector.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database,
                connection_timeout=300  # Increase connection timeout
            )
            cursor = conn.cursor()
            
            start_time = time.time()
            query_count = 0
            error_count = 0
            records_scanned = 0
            
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
                    
                    cursor.execute(query)
                    result = cursor.fetchone()
                    
                    # Accumulate scanned records
                    if result and result[0]:
                        records_scanned += result[0]
                    
                    query_count += 1
                except Exception as e:
                    error_count += 1
                    if error_count % 10 == 1:  # Only print every 10th error to avoid excessive logs
                        print(f"Client {client_id} query failed ({error_count} times): {e}")
                    
                    # Try to reconnect if connection is lost
                    if conn and not conn.is_connected():
                        try:
                            if cursor:
                                cursor.close()
                            if conn:
                                conn.close()
                                
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

            queries_counter.append({
                "queries": query_count, 
                "errors": error_count,
                "records_scanned": records_scanned
            })
            print(f"Client {client_id} completed: {query_count} queries executed, {error_count} failures, {records_scanned:,} records scanned")
            
        except Exception as e:
            print(f"Client {client_id} failed: {e}")
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
        queries_counter = []
        
        # Start concurrent clients
        client_pool = concurrent.futures.ThreadPoolExecutor(max_workers=concurrency)
        client_futures = []
        
        try:
            for i in range(concurrency):
                future = client_pool.submit(self.run_client, i, queries_counter)
                client_futures.append(future)
            
            # Wait for clients to complete
            for future in concurrent.futures.as_completed(client_futures):
                try:
                    future.result()
                except Exception as e:
                    print(f"Client execution error: {e}")
        finally:
            # Ensure thread pool is closed
            client_pool.shutdown(wait=True)
        
        # Collect and return results
        total_queries = sum(item["queries"] for item in queries_counter)
        total_errors = sum(item["errors"] for item in queries_counter)
        total_records = sum(item["records_scanned"] for item in queries_counter)
        qps = total_queries / self.duration if self.duration > 0 else 0
        records_per_sec = total_records / self.duration if self.duration > 0 else 0
        
        result = {
            "concurrency": concurrency,
            "duration": self.duration,
            "total_queries": total_queries,
            "total_errors": total_errors,
            "total_records_scanned": total_records,
            "qps": qps,
            "records_per_sec": records_per_sec,
            "error_rate": (total_errors / total_queries if total_queries > 0 else 0)
        }
        
        self.test_results[concurrency] = result
        
        print(f"Test results for concurrency {concurrency}:")
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
