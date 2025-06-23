#!/usr/bin/env python3
"""
TiDB TTL Benchmark Script

High-frequency stress testing for TiDB TTL (Time to Live) functionality.
Features:
1. Create TTL-enabled test tables
2. High-frequency concurrent writes
3. Concurrent reads with various patterns
4. Real-time performance metrics
5. TTL effectiveness monitoring

Author: AI Assistant
Version: 1.0
"""

import argparse
import json
import logging
import random
import sys
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any

try:
    import pymysql
except ImportError as e:
    print(f"Missing required packages: {e}")
    print("Please run: pip install pymysql")
    sys.exit(1)


@dataclass
class DatabaseConfig:
    """Database connection configuration"""
    host: str = "127.0.0.1"
    port: int = 4000
    user: str = "root"
    password: str = ""
    database: str = "ttl_benchmark"
    charset: str = "utf8mb4"


@dataclass
class TTLConfig:
    """TTL configuration"""
    ttl_column: str = "created_at"
    ttl_interval: str = "5 MINUTE"  # Short interval for quick results
    ttl_job_interval: str = "1h"    # TTL job execution interval (format: 1h, 30m, 24h)
    ttl_enable: bool = True


@dataclass
class BenchmarkConfig:
    """Benchmark configuration"""
    table_name: str = "ttl_test_table"
    table_count: int = 1
    initial_data_count: int = 10000  # Smaller initial dataset
    record_size: int = 128           # Small records for async commit testing
    tiny_record_size: int = 32       # Very small records for single-row transactions
    
    # High-frequency settings
    duration: int = 60               # Short test duration (1 minute)
    write_threads: int = 20          # More write threads
    read_threads: int = 10           # More read threads
    write_batch_size: int = 50       # Smaller batches for higher frequency
    writes_per_second: int = 1000    # Target writes per second

    # Mixed workload settings
    update_ratio: float = 0.3        # 30% of writes are updates
    small_write_ratio: float = 0.2   # 20% of writes are small operations
    single_row_ratio: float = 0.1    # 10% of writes are single-row transactions (for async commit)
    
    # Monitoring
    stats_interval: int = 2          # Report stats every 2 seconds
    
    ttl: TTLConfig = None
    
    def __post_init__(self):
        if self.ttl is None:
            self.ttl = TTLConfig()


@dataclass
class BenchmarkStats:
    """Benchmark statistics"""
    start_time: float = 0
    end_time: float = 0
    
    total_writes: int = 0
    total_reads: int = 0
    total_inserts: int = 0
    total_updates: int = 0
    total_single_row_ops: int = 0
    write_errors: int = 0
    read_errors: int = 0
    
    write_latency_sum: float = 0
    write_latency_count: int = 0
    read_latency_sum: float = 0
    read_latency_count: int = 0
    
    ttl_deleted_rows: int = 0
    data_count_history: List[Tuple[float, int]] = None
    
    def __post_init__(self):
        if self.data_count_history is None:
            self.data_count_history = []
    
    @property
    def duration(self) -> float:
        return self.end_time - self.start_time if self.end_time > self.start_time else 0
    
    @property
    def write_tps(self) -> float:
        return self.total_writes / self.duration if self.duration > 0 else 0
    
    @property
    def read_tps(self) -> float:
        return self.total_reads / self.duration if self.duration > 0 else 0
    
    @property
    def avg_write_latency_ms(self) -> float:
        if self.write_latency_count > 0:
            return (self.write_latency_sum / self.write_latency_count) * 1000
        return 0
    
    @property
    def avg_read_latency_ms(self) -> float:
        if self.read_latency_count > 0:
            return (self.read_latency_sum / self.read_latency_count) * 1000
        return 0


class TTLBenchmark:
    """TiDB TTL Benchmark main class"""

    def __init__(self):
        self.config = BenchmarkConfig()
        self.db_config = DatabaseConfig()
        self.stats = BenchmarkStats()
        self.stop_event = threading.Event()
        self.logger = self._setup_logging()
    
    def _setup_logging(self) -> logging.Logger:
        """Setup logging"""
        logger = logging.getLogger('ttl_benchmark')
        logger.setLevel(logging.INFO)
        
        handler = logging.StreamHandler()
        handler.setLevel(logging.INFO)
        
        formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        
        return logger
    
    def _create_connection(self) -> pymysql.Connection:
        """Create database connection"""
        return pymysql.connect(
            host=self.db_config.host,
            port=self.db_config.port,
            user=self.db_config.user,
            password=self.db_config.password,
            database=self.db_config.database,
            charset=self.db_config.charset,
            autocommit=True
        )
    
    def _execute_sql(self, sql: str, params: tuple = None, fetch: bool = False) -> Any:
        """Execute SQL statement"""
        conn = self._create_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(sql, params)
                if fetch:
                    return cursor.fetchall()
                return cursor.rowcount
        finally:
            conn.close()
    
    def setup_database(self):
        """Setup database and tables"""
        self.logger.info("Setting up database...")
        
        # Create database if not exists
        try:
            temp_conn = pymysql.connect(
                host=self.db_config.host,
                port=self.db_config.port,
                user=self.db_config.user,
                password=self.db_config.password,
                charset=self.db_config.charset,
                autocommit=True
            )
            
            with temp_conn.cursor() as cursor:
                cursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.db_config.database}")
                self.logger.info(f"Database {self.db_config.database} created")
            
            temp_conn.close()
        except Exception as e:
            self.logger.error(f"Failed to create database: {e}")
            raise
        
        # Create test tables
        self._create_test_tables()
    
    def _create_test_tables(self):
        """Create test tables with TTL"""
        self.logger.info("Creating test tables...")
        
        for i in range(self.config.table_count):
            table_name = f"{self.config.table_name}_{i}" if self.config.table_count > 1 else self.config.table_name
            
            # Drop existing table
            drop_sql = f"DROP TABLE IF EXISTS {table_name}"
            self._execute_sql(drop_sql)
            
            # Create table with TTL
            ttl_enable_val = 'ON' if self.config.ttl.ttl_enable else 'OFF'
            create_sql = f"""
            CREATE TABLE {table_name} (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                {self.config.ttl.ttl_column} TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                data_field VARCHAR(1024),
                random_int INT,
                random_float DOUBLE,
                status TINYINT DEFAULT 1,
                INDEX idx_created_at ({self.config.ttl.ttl_column}),
                INDEX idx_random_int (random_int)
            ) TTL = `{self.config.ttl.ttl_column}` + INTERVAL {self.config.ttl.ttl_interval}
              TTL_ENABLE = '{ttl_enable_val}'
              TTL_JOB_INTERVAL = '{self.config.ttl.ttl_job_interval}'
            """
            
            self._execute_sql(create_sql)
            self.logger.info(f"Table {table_name} created with TTL: {self.config.ttl.ttl_interval}")
    
    def _generate_random_string(self, size: int) -> str:
        """Generate random string of specified size"""
        import string
        chars = string.ascii_letters + string.digits
        return ''.join(random.choice(chars) for _ in range(size))
    
    def generate_initial_data(self):
        """Generate initial test data"""
        self.logger.info(f"Generating {self.config.initial_data_count} initial records...")
        
        for i in range(self.config.table_count):
            table_name = f"{self.config.table_name}_{i}" if self.config.table_count > 1 else self.config.table_name
            self._populate_table(table_name, self.config.initial_data_count)
        
        self.logger.info("Initial data generation completed")
    
    def _populate_table(self, table_name: str, count: int):
        """Populate table with test data"""
        batch_size = self.config.write_batch_size
        batches = (count + batch_size - 1) // batch_size
        
        for batch in range(batches):
            start_idx = batch * batch_size
            end_idx = min(start_idx + batch_size, count)
            batch_count = end_idx - start_idx
            
            # Generate batch data with varying timestamps
            data_rows = []
            for _ in range(batch_count):
                # Create data with different ages (some already expired)
                age_seconds = random.randint(0, 600)  # 0-10 minutes old
                created_time = datetime.now() - timedelta(seconds=age_seconds)
                
                data_content = self._generate_random_string(self.config.record_size)
                
                data_rows.append((
                    created_time,
                    data_content,
                    random.randint(1, 100000),
                    random.uniform(0, 1000),
                    random.randint(0, 1)
                ))
            
            # Batch insert
            sql = f"""
            INSERT INTO {table_name} 
            ({self.config.ttl.ttl_column}, data_field, random_int, random_float, status) 
            VALUES (%s, %s, %s, %s, %s)
            """
            
            conn = self._create_connection()
            try:
                with conn.cursor() as cursor:
                    cursor.executemany(sql, data_rows)
            finally:
                conn.close()

    def run_benchmark(self):
        """Run the benchmark"""
        self.logger.info("Starting TiDB TTL benchmark...")

        # Setup database and generate initial data
        self.setup_database()
        self.generate_initial_data()

        # Start statistics reporting thread
        stats_thread = threading.Thread(target=self._stats_reporter, daemon=True)
        stats_thread.start()

        # Record start time
        self.stats.start_time = time.time()

        # Start benchmark threads
        with ThreadPoolExecutor(max_workers=self.config.write_threads + self.config.read_threads) as executor:
            futures = []

            # Start write workers
            for i in range(self.config.write_threads):
                future = executor.submit(self._write_worker, i)
                futures.append(future)

            # Start read workers
            for i in range(self.config.read_threads):
                future = executor.submit(self._read_worker, i)
                futures.append(future)

            # Wait for benchmark completion
            try:
                start_time = time.time()
                while time.time() - start_time < self.config.duration and not self.stop_event.is_set():
                    time.sleep(0.1)

                self.logger.info("Benchmark duration reached, stopping...")
                self.stop_event.set()

                # Wait for all threads to complete
                for future in as_completed(futures, timeout=10):
                    try:
                        future.result()
                    except Exception as e:
                        self.logger.error(f"Worker thread error: {e}")

            except KeyboardInterrupt:
                self.logger.info("Benchmark interrupted by user")
                self.stop_event.set()

        # Record end time
        self.stats.end_time = time.time()

        # Generate final report
        self._generate_report()

    def _write_worker(self, worker_id: int):
        """Write worker thread"""
        self.logger.info(f"Write worker {worker_id} started")

        # Calculate sleep time to achieve target writes per second
        target_interval = 1.0 / (self.config.writes_per_second / self.config.write_threads)

        while not self.stop_event.is_set():
            try:
                # Select random table
                table_idx = random.randint(0, self.config.table_count - 1)
                table_name = f"{self.config.table_name}_{table_idx}" if self.config.table_count > 1 else self.config.table_name

                # Perform mixed write operations
                self._mixed_write_operations(table_name)

                # Sleep to control write frequency
                time.sleep(target_interval)

            except Exception as e:
                self.logger.error(f"Write worker {worker_id} error: {e}")
                self.stats.write_errors += 1
                time.sleep(0.1)

        self.logger.info(f"Write worker {worker_id} stopped")

    def _mixed_write_operations(self, table_name: str):
        """Perform mixed write operations (INSERT/UPDATE)"""
        # Decide operation type based on ratios
        operation_type = self._choose_write_operation()

        if operation_type == 'single_row':
            self._perform_single_row_operation(table_name)
        elif operation_type == 'update':
            self._perform_updates(table_name)
        elif operation_type == 'small_insert':
            self._perform_small_inserts(table_name)
        else:  # batch_insert
            self._batch_write(table_name)

    def _choose_write_operation(self) -> str:
        """Choose write operation type based on configured ratios"""
        rand = random.random()

        if rand < self.config.single_row_ratio:
            return 'single_row'
        elif rand < self.config.single_row_ratio + self.config.update_ratio:
            return 'update'
        elif rand < self.config.single_row_ratio + self.config.update_ratio + self.config.small_write_ratio:
            return 'small_insert'
        else:
            return 'batch_insert'

    def _perform_single_row_operation(self, table_name: str):
        """Perform single-row transaction (optimal for async commit)"""
        operation = random.choice(['insert', 'update'])

        start_time = time.time()
        conn = self._create_connection()
        try:
            with conn.cursor() as cursor:
                if operation == 'insert':
                    # Single tiny record insert
                    tiny_data = self._generate_random_string(self.config.tiny_record_size)
                    sql = f"""
                    INSERT INTO {table_name}
                    ({self.config.ttl.ttl_column}, data_field, random_int, random_float, status)
                    VALUES (%s, %s, %s, %s, %s)
                    """
                    cursor.execute(sql, (
                        datetime.now(),
                        tiny_data,
                        random.randint(1, 100000),
                        random.uniform(0, 1000),
                        random.randint(0, 1)
                    ))
                else:  # update
                    # Single row update by ID
                    tiny_data = self._generate_random_string(self.config.tiny_record_size)
                    random_id = random.randint(1, 50000)
                    sql = f"""
                    UPDATE {table_name}
                    SET data_field = %s, random_float = %s
                    WHERE random_int = %s
                    LIMIT 1
                    """
                    cursor.execute(sql, (tiny_data, random.uniform(0, 1000), random_id))

            # Update statistics
            latency = time.time() - start_time
            self.stats.total_writes += 1
            self.stats.total_single_row_ops += 1
            if operation == 'insert':
                self.stats.total_inserts += 1
            else:
                self.stats.total_updates += 1
            self.stats.write_latency_sum += latency
            self.stats.write_latency_count += 1

        except Exception as e:
            self.stats.write_errors += 1
            raise e
        finally:
            conn.close()

    def _perform_updates(self, table_name: str):
        """Perform UPDATE operations on existing records"""
        update_count = random.randint(1, 10)  # Small batch of updates

        start_time = time.time()
        conn = self._create_connection()
        try:
            with conn.cursor() as cursor:
                for _ in range(update_count):
                    # Update random existing record
                    new_data = self._generate_random_string(self.config.record_size // 2)
                    new_status = random.randint(0, 1)
                    new_float = random.uniform(0, 1000)

                    # Update by random_int range to affect multiple rows potentially
                    random_range_start = random.randint(1, 50000)
                    random_range_end = random_range_start + random.randint(1, 100)

                    sql = f"""
                    UPDATE {table_name}
                    SET data_field = %s, status = %s, random_float = %s
                    WHERE random_int BETWEEN %s AND %s
                    LIMIT 5
                    """

                    cursor.execute(sql, (new_data, new_status, new_float,
                                       random_range_start, random_range_end))

            # Update statistics
            latency = time.time() - start_time
            self.stats.total_writes += update_count
            self.stats.total_updates += update_count
            self.stats.write_latency_sum += latency
            self.stats.write_latency_count += 1

        except Exception as e:
            self.stats.write_errors += 1
            raise e
        finally:
            conn.close()

    def _perform_small_inserts(self, table_name: str):
        """Perform small INSERT operations"""
        insert_count = random.randint(1, 5)  # Very small batch
        data_rows = []

        for _ in range(insert_count):
            data_content = self._generate_random_string(self.config.record_size // 2)  # Medium-sized records
            data_rows.append((
                datetime.now(),
                data_content,
                random.randint(1, 100000),
                random.uniform(0, 1000),
                random.randint(0, 1)
            ))

        sql = f"""
        INSERT INTO {table_name}
        ({self.config.ttl.ttl_column}, data_field, random_int, random_float, status)
        VALUES (%s, %s, %s, %s, %s)
        """

        start_time = time.time()
        conn = self._create_connection()
        try:
            with conn.cursor() as cursor:
                cursor.executemany(sql, data_rows)

            # Update statistics
            latency = time.time() - start_time
            self.stats.total_writes += insert_count
            self.stats.total_inserts += insert_count
            self.stats.write_latency_sum += latency
            self.stats.write_latency_count += 1

        except Exception as e:
            self.stats.write_errors += 1
            raise e
        finally:
            conn.close()

    def _batch_write(self, table_name: str):
        """Perform batch write operation"""
        batch_size = self.config.write_batch_size
        data_rows = []

        for _ in range(batch_size):
            data_content = self._generate_random_string(self.config.record_size)
            data_rows.append((
                datetime.now(),
                data_content,
                random.randint(1, 100000),
                random.uniform(0, 1000),
                random.randint(0, 1)
            ))

        sql = f"""
        INSERT INTO {table_name}
        ({self.config.ttl.ttl_column}, data_field, random_int, random_float, status)
        VALUES (%s, %s, %s, %s, %s)
        """

        start_time = time.time()
        conn = self._create_connection()
        try:
            with conn.cursor() as cursor:
                cursor.executemany(sql, data_rows)

            # Update statistics
            latency = time.time() - start_time
            self.stats.total_writes += batch_size
            self.stats.total_inserts += batch_size
            self.stats.write_latency_sum += latency
            self.stats.write_latency_count += 1

        except Exception as e:
            self.stats.write_errors += 1
            raise e
        finally:
            conn.close()

    def _read_worker(self, worker_id: int):
        """Read worker thread"""
        self.logger.info(f"Read worker {worker_id} started")

        while not self.stop_event.is_set():
            try:
                # Select random table
                table_idx = random.randint(0, self.config.table_count - 1)
                table_name = f"{self.config.table_name}_{table_idx}" if self.config.table_count > 1 else self.config.table_name

                # Perform read operation
                self._perform_read(table_name)

                # Short sleep
                time.sleep(0.01)

            except Exception as e:
                self.logger.error(f"Read worker {worker_id} error: {e}")
                self.stats.read_errors += 1
                time.sleep(0.1)

        self.logger.info(f"Read worker {worker_id} stopped")

    def _perform_read(self, table_name: str):
        """Perform read operation"""
        # Random read pattern
        read_type = random.choice(['point_select', 'range_select', 'count'])

        start_time = time.time()
        conn = self._create_connection()
        try:
            with conn.cursor() as cursor:
                if read_type == 'point_select':
                    # Point query by random_int
                    random_val = random.randint(1, 100000)
                    sql = f"SELECT * FROM {table_name} WHERE random_int = %s LIMIT 10"
                    cursor.execute(sql, (random_val,))
                    cursor.fetchall()

                elif read_type == 'range_select':
                    # Range query by timestamp
                    start_time_val = datetime.now() - timedelta(minutes=10)
                    sql = f"SELECT * FROM {table_name} WHERE {self.config.ttl.ttl_column} > %s LIMIT 50"
                    cursor.execute(sql, (start_time_val,))
                    cursor.fetchall()

                elif read_type == 'count':
                    # Count query
                    sql = f"SELECT COUNT(*) FROM {table_name} WHERE status = 1"
                    cursor.execute(sql)
                    cursor.fetchall()

            # Update statistics
            latency = time.time() - start_time
            self.stats.total_reads += 1
            self.stats.read_latency_sum += latency
            self.stats.read_latency_count += 1

        finally:
            conn.close()

    def _stats_reporter(self):
        """Statistics reporting thread"""
        self.logger.info("Statistics reporter started")

        while not self.stop_event.is_set():
            try:
                # Collect current data count
                self._collect_data_count()

                # Print current statistics
                self._print_current_stats()

                # Sleep until next report
                time.sleep(self.config.stats_interval)

            except Exception as e:
                self.logger.error(f"Stats reporter error: {e}")
                time.sleep(self.config.stats_interval)

        self.logger.info("Statistics reporter stopped")

    def _collect_data_count(self):
        """Collect current data count"""
        total_count = 0

        for i in range(self.config.table_count):
            table_name = f"{self.config.table_name}_{i}" if self.config.table_count > 1 else self.config.table_name

            try:
                sql = f"SELECT COUNT(*) FROM {table_name}"
                result = self._execute_sql(sql, fetch=True)
                if result:
                    total_count += result[0][0]
            except Exception as e:
                self.logger.error(f"Failed to count table {table_name}: {e}")

        # Record data count sample
        self.stats.data_count_history.append((time.time(), total_count))

        # Keep only recent samples
        if len(self.stats.data_count_history) > 100:
            self.stats.data_count_history = self.stats.data_count_history[-100:]

    def _print_current_stats(self):
        """Print current statistics"""
        if self.stats.start_time == 0:
            return

        elapsed = time.time() - self.stats.start_time
        current_write_tps = self.stats.total_writes / elapsed if elapsed > 0 else 0
        current_read_tps = self.stats.total_reads / elapsed if elapsed > 0 else 0

        # Get current data count
        current_data_count = 0
        if self.stats.data_count_history:
            current_data_count = self.stats.data_count_history[-1][1]

        print(f"[{elapsed:6.1f}s] "
              f"Writes: {self.stats.total_writes:8d} ({current_write_tps:7.1f} TPS) | "
              f"I:{self.stats.total_inserts:6d}/U:{self.stats.total_updates:6d}/S:{self.stats.total_single_row_ops:6d} | "
              f"Reads: {self.stats.total_reads:7d} ({current_read_tps:6.1f} TPS) | "
              f"Data: {current_data_count:8d} | "
              f"Errors: W{self.stats.write_errors}/R{self.stats.read_errors} | "
              f"Latency: W{self.stats.avg_write_latency_ms:.1f}ms/R{self.stats.avg_read_latency_ms:.1f}ms")

    def _generate_report(self):
        """Generate final benchmark report"""
        self.logger.info("Generating benchmark report...")

        # Calculate TTL effectiveness
        ttl_effectiveness = self._calculate_ttl_effectiveness()

        report = {
            "benchmark_config": asdict(self.config),
            "database_config": asdict(self.db_config),
            "results": {
                "duration_seconds": self.stats.duration,
                "total_writes": self.stats.total_writes,
                "total_inserts": self.stats.total_inserts,
                "total_updates": self.stats.total_updates,
                "total_single_row_ops": self.stats.total_single_row_ops,
                "total_reads": self.stats.total_reads,
                "write_tps": self.stats.write_tps,
                "read_tps": self.stats.read_tps,
                "avg_write_latency_ms": self.stats.avg_write_latency_ms,
                "avg_read_latency_ms": self.stats.avg_read_latency_ms,
                "write_errors": self.stats.write_errors,
                "read_errors": self.stats.read_errors,
                "ttl_effectiveness": ttl_effectiveness,
            },
            "data_count_timeline": self.stats.data_count_history,
            "timestamp": datetime.now().isoformat()
        }

        # Save report to file
        report_file = f"ttl_benchmark_report_{int(time.time())}.json"
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)

        self.logger.info(f"Report saved to: {report_file}")

        # Print summary
        self._print_summary(report)

    def _calculate_ttl_effectiveness(self) -> Dict[str, Any]:
        """Calculate TTL effectiveness metrics"""
        if len(self.stats.data_count_history) < 2:
            return {"status": "insufficient_data"}

        initial_count = self.stats.data_count_history[0][1]
        final_count = self.stats.data_count_history[-1][1]
        max_count = max(sample[1] for sample in self.stats.data_count_history)

        return {
            "initial_count": initial_count,
            "final_count": final_count,
            "max_count": max_count,
            "net_change": final_count - initial_count,
            "peak_growth": max_count - initial_count,
            "ttl_working": final_count < max_count,
            "data_stabilized": abs(final_count - initial_count) < (initial_count * 0.1)
        }

    def _print_summary(self, report: Dict[str, Any]):
        """Print benchmark summary"""
        results = report["results"]
        ttl_eff = results["ttl_effectiveness"]

        print("\n" + "="*80)
        print("TiDB TTL Benchmark Results Summary")
        print("="*80)
        print(f"Duration:           {results['duration_seconds']:.1f} seconds")
        print(f"Write Operations:   {results['total_writes']:,} ({results['write_tps']:.1f} TPS)")
        print(f"  - Inserts:        {results['total_inserts']:,}")
        print(f"  - Updates:        {results['total_updates']:,}")
        print(f"  - Single-row ops: {results['total_single_row_ops']:,} (async commit candidates)")
        print(f"Read Operations:    {results['total_reads']:,} ({results['read_tps']:.1f} TPS)")
        print(f"Write Latency:      {results['avg_write_latency_ms']:.2f} ms")
        print(f"Read Latency:       {results['avg_read_latency_ms']:.2f} ms")
        print(f"Errors:             Write: {results['write_errors']}, Read: {results['read_errors']}")

        if ttl_eff.get("status") != "insufficient_data":
            print(f"\nTTL Effectiveness:")
            print(f"Initial Data Count: {ttl_eff['initial_count']:,}")
            print(f"Final Data Count:   {ttl_eff['final_count']:,}")
            print(f"Peak Data Count:    {ttl_eff['max_count']:,}")
            print(f"Net Change:         {ttl_eff['net_change']:+,}")
            print(f"TTL Working:        {'Yes' if ttl_eff['ttl_working'] else 'No'}")
            print(f"Data Stabilized:    {'Yes' if ttl_eff['data_stabilized'] else 'No'}")

        print("="*80)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="TiDB TTL High-Frequency Benchmark")

    parser.add_argument("--host", default="127.0.0.1", help="TiDB host address")
    parser.add_argument("--port", type=int, default=4000, help="TiDB port")
    parser.add_argument("--user", "-u", default="root", help="Database username")
    parser.add_argument("--password", "-p", default="", help="Database password")
    parser.add_argument("--database", "-d", default="ttl_benchmark", help="Database name")

    parser.add_argument("--duration", type=int, default=60, help="Benchmark duration in seconds")
    parser.add_argument("--write-threads", type=int, default=20, help="Number of write threads")
    parser.add_argument("--read-threads", type=int, default=10, help="Number of read threads")
    parser.add_argument("--writes-per-second", type=int, default=1000, help="Target writes per second")
    parser.add_argument("--ttl-interval", default="5 MINUTE", help="TTL expiration interval")
    parser.add_argument("--ttl-job-interval", default="1h", help="TTL job execution interval (format: 1h, 30m, 24h)")

    # Mixed workload parameters
    parser.add_argument("--update-ratio", type=float, default=0.3, help="Ratio of UPDATE operations (0.0-1.0)")
    parser.add_argument("--small-write-ratio", type=float, default=0.2, help="Ratio of small INSERT operations (0.0-1.0)")
    parser.add_argument("--single-row-ratio", type=float, default=0.1, help="Ratio of single-row transactions for async commit (0.0-1.0)")

    args = parser.parse_args()

    try:
        # Create benchmark instance
        benchmark = TTLBenchmark()

        # Update database configuration
        benchmark.db_config.host = args.host
        benchmark.db_config.port = args.port
        benchmark.db_config.user = args.user
        benchmark.db_config.password = args.password
        benchmark.db_config.database = args.database

        # Update benchmark configuration
        benchmark.config.duration = args.duration
        benchmark.config.write_threads = args.write_threads
        benchmark.config.read_threads = args.read_threads
        benchmark.config.writes_per_second = args.writes_per_second
        benchmark.config.ttl.ttl_interval = args.ttl_interval
        if args.ttl_job_interval:
            benchmark.config.ttl.ttl_job_interval = args.ttl_job_interval

        # Update mixed workload configuration
        benchmark.config.update_ratio = args.update_ratio
        benchmark.config.small_write_ratio = args.small_write_ratio
        benchmark.config.single_row_ratio = args.single_row_ratio

        # Print configuration
        print("TiDB TTL Benchmark Starting...")
        print(f"Database: {benchmark.db_config.host}:{benchmark.db_config.port}/{benchmark.db_config.database}")
        print(f"Duration: {benchmark.config.duration} seconds")
        print(f"Threads: {benchmark.config.write_threads} write, {benchmark.config.read_threads} read")
        print(f"Target: {benchmark.config.writes_per_second} writes/second")
        print(f"TTL: {benchmark.config.ttl.ttl_interval} expiration")
        print(f"Mixed Workload: {benchmark.config.update_ratio*100:.0f}% updates, {benchmark.config.small_write_ratio*100:.0f}% small inserts")
        print("-" * 80)

        # Run benchmark
        benchmark.run_benchmark()

    except KeyboardInterrupt:
        print("\nBenchmark interrupted by user")
    except Exception as e:
        print(f"Benchmark failed: {e}")
        sys.exit(1)


