import json
import subprocess
import pymysql
import csv
import time
from tests import tests
from datetime import datetime
import tzlocal


def create_db_connection(host_name, port_number, user_name, user_password, db_name):
    connection = pymysql.connect(
        host=host_name,
        port=int(port_number),
        user=user_name,
        password=user_password,
        database=db_name,
        autocommit=True,
    )
    return connection


def replace_stmt_place_holder(stmt, workload, limit):
    stmt = stmt.replace("{target_table}", workload.target_table)
    stmt = stmt.replace("{source_table}", workload.source_table)
    stmt = stmt.replace("{limit}", str(limit))
    stmt = stmt.replace("{column}", str(workload.column))
    return stmt


def get_flush_metrics(conn):
    with conn.cursor() as cursor:
        # Query the @@tidb_last_txn_info variable
        cursor.execute("SELECT @@tidb_last_txn_info")
        result = cursor.fetchone()[0]

        # Parse the JSON string
        txn_info = json.loads(result)

        # Extract the flush_wait_ms value
        flush_wait_ms = txn_info.get("flush_wait_ms")
        dump_ms = txn_info.get("dump_ms")

        return flush_wait_ms / 1000.0, dump_ms / 1000.0 if dump_ms is not None else 0


def execute_statement_with_hint_option(
    connection, statement, workload, limit, use_hint, interval,
):
    """
    Return the execution time and flush wait time of the statement, in seconds
    """
    statement = replace_stmt_place_holder(
        statement, workload, limit=limit,
    )
    if use_hint:
        parts = statement.split(" ", 1)
        if len(parts) > 1:
            statement = parts[0] + " /*+ SET_VAR(tidb_dml_type=bulk) */ " + parts[1]
    time.sleep(interval)
    print(datetime.now(tzlocal.get_localzone()))
    print(statement)
    start_time = time.time()
    with connection.cursor() as cursor:
        cursor.execute(statement)
    exec_time = time.time() - start_time
    flush_wait_time, dump_time = get_flush_metrics(connection)
    return exec_time, flush_wait_time, dump_time


def execute_init_statements(
    connection,
    init_statements,
    workload,
    limit,
    min_flush_keys,
    min_flush_mem_size,
    force_flush_size,
    max_chunk_size,
):
    with connection.cursor() as cursor:
        cursor.execute("set @@tidb_mem_quota_query=5000000000")
        if min_flush_keys is not None:
            cursor.execute(f"set @@tidb_min_flush_keys={min_flush_keys}")
        if min_flush_mem_size is not None:
            cursor.execute(f"set @@tidb_min_flush_mem_size={min_flush_mem_size}")
        if force_flush_size is not None:
            cursor.execute(
                f"set @@tidb_force_flush_mem_size_threshold={force_flush_size}"
            )
        if max_chunk_size is not None:
            cursor.execute(f"set @@tidb_max_chunk_size={max_chunk_size}")
        for statement in init_statements:
            formatted_statement = replace_stmt_place_holder(
                statement,
                workload,
                limit=limit,
            )
            print("initializing: " + formatted_statement)
            cursor.execute(formatted_statement)


def init_target_table(connection, table_initialization_statements):
    for statement in table_initialization_statements:
        with connection.cursor() as cursor:
            print("initializing:", statement)
            cursor.execute(statement)


def prepare_sysbench_data(host, port, db_name, limit):
    sysbench_command = (
        f"sysbench --db-driver=mysql --mysql-db={db_name} --mysql-host={host} --mysql-port={port} "
        f"--mysql-user=root --tables=1 --table-size={limit} oltp_read_write prepare"
    )
    print("running: " + sysbench_command)
    subprocess.run(sysbench_command, shell=True)


def table_exists_and_row_count(connection, table_name):
    with connection.cursor() as cursor:
        cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
        table_exists = cursor.fetchone() is not None
        if table_exists:
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            row_count = cursor.fetchone()[0]
        else:
            row_count = 0
    return table_exists, row_count


def dbgen(template_file, num_rows):
    subprocess.run("rm -rf out_dir", shell=True)
    command = f"dbgen -i {template_file} -o out_dir -N {num_rows} -R 500000 -r 500000"
    subprocess.run(command, shell=True)

class Workload:
    def __init__(self, name, source_table, target_table, column):
        self.name = name
        self.source_table = source_table
        self.target_table = target_table
        self.column = column

def main(
    host,
    port,
    user_name,
    user_password,
    db_name,
    limit,
    interval,
    workload,
):
    table_initialization_statements = [
        f"DROP TABLE IF EXISTS {workload.target_table}",
        f"CREATE TABLE {workload.target_table} LIKE {workload.source_table}",
    ]
    # min_flush_keys, min_flush_mem_size, force_flush_mem_size_threshold, max_chunk_size
    configs = [
        [100, 0, None, 1024],
        [1000, 0, None, 1024],
        [10000, 0, None, 1024],
    ]
    use_bulk = [True]

    connection = create_db_connection(host, port, user_name, user_password, db_name)

    # init source table
    table_exists, row_count = table_exists_and_row_count(connection, workload.source_table)
    if not table_exists or row_count < limit:
        with connection.cursor() as cursor:
            cursor.execute(f"DROP TABLE IF EXISTS {workload.source_table}")
        match workload.name:
            case "sysbench":
                prepare_sysbench_data(host, port, db_name, limit)
            case "digiplus":
                dbgen("digiplus.template", limit)
                print("data files are generated. Manually import them and retry")
                exit(0)
                # manual import using lightning
                pass
            case _:
                raise ValueError(f"Unknown schema: {workload.name}")

    else:
        print(
            f"Table '{workload.source_table}' already exists and has {row_count} rows,",
            f"which is >= {limit}. No need to prepare sysbench data.",
        )

    init_target_table(connection, table_initialization_statements)

    test_results = []

    for test in tests:
        for (
            min_flush_keys,
            min_flush_mem_size,
            force_flush_threshold,
            max_chunk_size,
        ) in configs:
            for bulk in use_bulk:
                print()
                print(
                    (
                        f"Running test: {test['alias']}, "
                        f"min_flush_keys={min_flush_keys}, "
                        f"min_flush_mem_size={min_flush_mem_size}, "
                        f"force_flush_threshold={force_flush_threshold}, "
                        f"max_chunk_size={max_chunk_size}, "
                        f"use_bulk={bulk}"
                    )
                )
                execute_init_statements(
                    connection,
                    test["init"],
                    workload,
                    limit,
                    min_flush_keys,
                    min_flush_mem_size,
                    force_flush_threshold,
                    max_chunk_size,
                )
                latency, flush_wait, dump_time = execute_statement_with_hint_option(
                    connection,
                    test["statement"],
                    workload,
                    limit,
                    use_hint=bulk,
                    interval=interval,
                )
                print(
                    f"Execution time: {latency:.2f} seconds; "
                    f"Flush wait: {flush_wait:.2f} seconds; "
                    f"Dump time: {dump_time:.2f} seconds"
                )
                row = [
                    test["alias"],
                    min_flush_keys,
                    min_flush_mem_size,
                    force_flush_threshold,
                    max_chunk_size,
                    bulk,
                    workload.target_table,
                    f"{latency:.2f}",
                    f"{flush_wait:.2f}",
                    f"{dump_time:.2f}"
                ]
                test_results.append(row)

    with open("test_results.csv", "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        header = [
            "SQL",
            "min flush keys",
            "min flush size",
            "force flush size",
            "max chunk size",
            "bulk",
            "table",
            "latency",
            "flush wait",
            "dump time"
        ]
        writer.writerow(header)
        for result in test_results:
            writer.writerow(result)

    print("Test finishes. Output to test_results.csv")


# =============================================================================
# sysbench is totally automatic
# digiplus needs manual data import.

sysbench = Workload("sysbench", "sbtest1", "t", "k")
digiplus = Workload("digiplus", "digiplus", "t", "id")

main(
    "192.168.180.11",
    4005,
    "root",
    "",
    db_name="test",
    limit=1000000,
    interval=60,
    workload=digiplus
)
