import json
import subprocess
import pymysql
import csv
import time
from tests import tests


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


def replace_stmt_place_holder(stmt, target_table, source_table, limit):
    stmt = stmt.replace("{target_table}", target_table)
    stmt = stmt.replace("{source_table}", source_table)
    stmt = stmt.replace("{limit}", str(limit))
    return stmt


def get_flush_wait_ms(conn):
    with conn.cursor() as cursor:
        # Query the @@tidb_last_txn_info variable
        cursor.execute("SELECT @@tidb_last_txn_info")
        result = cursor.fetchone()[0]

        # Parse the JSON string
        txn_info = json.loads(result)

        # Extract the flush_wait_ms value
        flush_wait_ms = txn_info.get("flush_wait_ms")

        return flush_wait_ms


def execute_statement_with_hint_option(
    connection, statement, target_table, source_table, limit, use_hint, interval
):
    """
    Return the execution time and flush wait time of the statement, in seconds
    """
    statement = replace_stmt_place_holder(
        statement, target_table=target_table, source_table=source_table, limit=limit
    )
    if use_hint:
        parts = statement.split(" ", 1)
        if len(parts) > 1:
            statement = parts[0] + " /*+ SET_VAR(tidb_dml_type=bulk) */ " + parts[1]
    time.sleep(interval)
    print()
    print(statement)
    start_time = time.time()
    with connection.cursor() as cursor:
        cursor.execute(statement)
    exec_time = time.time() - start_time
    flush_wait_time = get_flush_wait_ms(connection) / 1000.0
    print(
        f"Execution time: {exec_time:.2f} seconds; Flush wait: {flush_wait_time:.2f} seconds"
    )
    return exec_time, flush_wait_time


def execute_init_statements(
    connection, init_statements, target_table, source_table, limit
):
    with connection.cursor() as cursor:
        cursor.execute("set @@tidb_mem_quota_query=5000000000")
        for statement in init_statements:
            formatted_statement = replace_stmt_place_holder(
                statement, target_table=target_table, source_table=source_table, limit=limit
            )
            cursor.execute(formatted_statement)
    


def init_target_tables(connection, table_initialization_statements):
    for statement in table_initialization_statements:
        with connection.cursor() as cursor:
            cursor.execute(statement)


def prepare_sysbench_data(host, port, db_name, limit):
    sysbench_command = f"sysbench --db-driver=mysql --mysql-db={db_name} --mysql-host={host} --mysql-port={port} --mysql-user=root --tables=1 --table-size={limit} oltp_read_write prepare"
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


def main(
    host,
    port,
    user_name,
    user_password,
    db_name,
    limit,
    interval,
    source_table,
    target_tables,
    skip_standard=True,
):
    table_initialization_statements = [
        """ DROP TABLE IF EXISTS target_table""",
        f"CREATE TABLE `target_table` LIKE {source_table}",
    ]

    connection = create_db_connection(host, port, user_name, user_password, db_name)

    # init source table
    table_exists, row_count = table_exists_and_row_count(connection, source_table)
    if not table_exists or row_count < limit:
        with connection.cursor() as cursor:
            cursor.execute(f"DROP TABLE IF EXISTS {source_table}")
        prepare_sysbench_data(host, port, db_name, limit)
    else:
        print(
            f"Table '{source_table}' already exists and has {row_count} rows, which is >= {limit}. No need to prepare sysbench data."
        )

    init_target_tables(connection, table_initialization_statements)

    test_results = []

    for test in tests:
        row = [test["alias"]]
        for table_name in target_tables:
            if not skip_standard:
                execute_init_statements(
                    connection, test["init"], table_name, source_table, limit
                )
                latency_standard, flush_wait_standard = (
                    execute_statement_with_hint_option(
                        connection,
                        test["statement"],
                        table_name,
                        source_table,
                        limit,
                        use_hint=False,
                        interval=interval,
                    )
                )
                row.extend(
                    [
                        f"{latency_standard:.2f}",
                        f"{flush_wait_standard/1000:.2f}",
                    ]
                )
            execute_init_statements(
                connection, test["init"], table_name, source_table, limit
            )
            latency_bulk, flush_wait_bulk = execute_statement_with_hint_option(
                connection,
                test["statement"],
                table_name,
                source_table,
                limit,
                use_hint=True,
                interval=interval,
            )
            row.extend(
                [
                    f"{latency_bulk:.2f}",
                    f"{flush_wait_bulk/1000:.2f}",
                ]
            )
        test_results.append(row)

    with open("test_results.csv", "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        header = ["SQL Statement"]
        for table_name in target_tables:
            if not skip_standard:
                header.extend(
                    [
                        f"standard - latency - {table_name}",
                        f"standard - flush wait - {table_name},",
                    ]
                )
            header.extend(
                [
                    f"bulk - latency - {table_name}",
                    f"bulk - flush wait - {table_name}",
                ]
            )
        writer.writerow(header)
        for result in test_results:
            writer.writerow(result)

    print("Test finishes. Output to test_results.csv")


# =============================================================================

main(
    "192.168.180.11",
    4003,
    "root",
    "",
    db_name="test",
    limit=10000000,
    interval=60,
    source_table="sbtest1",
    target_tables=["target_table"],
)
