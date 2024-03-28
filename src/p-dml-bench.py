import pymysql
import csv
import time

def create_db_connection(host_name, port_number, user_name, user_password, db_name):
    connection = pymysql.connect(
        host=host_name,
        port=int(port_number),
        user=user_name,
        password=user_password,
        database=db_name,
        autocommit=True
    )
    return connection

def execute_statement_with_hint_option(connection, statement, table_name, limit, use_hint, interval):
    statement = statement.replace("{table_name}", table_name)
    statement = statement.replace("{limit}", str(limit))
    if use_hint:
        parts = statement.split(' ', 1)
        if len(parts) > 1:
            statement = parts[0] + ' /*+ SET_VAR(tidb_dml_type=bulk) */ ' + parts[1]
    time.sleep(interval)
    print(statement)
    start_time = time.time()
    with connection.cursor() as cursor:
        cursor.execute(statement)
    exec_time = time.time() - start_time
    print(f"Execution time: {exec_time:.2f} seconds")
    return exec_time

def execute_init_statements(connection, init_statements, table_name, limit):
    for statement in init_statements:
        formatted_statement = statement.replace("{table_name}", table_name)
        formatted_statement = formatted_statement.replace("{limit}", str(limit))
        with connection.cursor() as cursor:
            cursor.execute(formatted_statement)

def initialize_tables(connection, table_initialization_statements):
    for statement in table_initialization_statements:
        with connection.cursor() as cursor:
            cursor.execute(statement)

tests = [
    {
        "alias": "simple insert",
        "init": [
            "DELETE FROM {table_name}",
        ],
        "statement": "INSERT INTO {table_name} SELECT * FROM sbtest_3 LIMIT {limit}",
    },
    {
        "alias": "insert ignore",
        "init": [
            "DELETE FROM {table_name}",
        ],
        "statement": "INSERT IGNORE INTO {table_name} SELECT * FROM sbtest_3 LIMIT {limit}",
    },
    {
        "alias": "insert on duplicate",
        "init": [
            "DELETE FROM {table_name}",
        ],
        "statement": "INSERT INTO {table_name} SELECT * from sbtest_3 limit {limit} ON DUPLICATE KEY UPDATE id={table_name}.id;",
    },
    {
        "alias": "replace empty",
        "init": [
            "DELETE FROM {table_name}",
        ],
        "statement": "REPLACE INTO {table_name} SELECT * from sbtest_3 limit {limit};",
    },
    {
        "alias": "update",
        "init": [
            "DELETE FROM {table_name}",
            "INSERT INTO {table_name} SELECT * from sbtest_3 limit {limit};",
        ],
        "statement": "UPDATE {table_name} SET k=k+1",
    },
    {
        "alias": "delete",
        "init": [
            "DELETE FROM {table_name}",
            "INSERT INTO {table_name} SELECT * from sbtest_3 limit {limit};",
        ],
        "statement": "DELETE FROM {table_name}",
    },
]

target_tables = ["sbtest_3_2", "sbtest_3_3"]

table_initialization_statements = [
    """ DROP TABLE IF EXISTS sbtest_3_2""",
    """
CREATE TABLE `sbtest_3_2` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `k` int(11) NOT NULL DEFAULT '0',
  `c` char(120) NOT NULL DEFAULT '',
  `pad` char(60) NOT NULL DEFAULT '',
  KEY `id` (`id`),
  KEY `k_1` (`k`)
)
    """,
    """ DROP TABLE IF EXISTS sbtest_3_3""",
    """
CREATE TABLE `sbtest_3_3` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `k` int(11) NOT NULL DEFAULT '0',
  `c` char(120) NOT NULL DEFAULT '',
  `pad` char(60) NOT NULL DEFAULT '',
  UNIQUE KEY `id` (`id`),
  KEY `k_1` (`k`)
) 
    """
]

def main(host_name, port_number, user_name, user_password, db_name, limit, interval):
    connection = create_db_connection(host_name, port_number, user_name, user_password, db_name)
    
    initialize_tables(connection, table_initialization_statements)

    test_results = []

    for test in tests:
        row = [test["alias"]]
        for table_name in target_tables:
            execute_init_statements(connection, test["init"], table_name, limit)
            exec_time_no_hint = execute_statement_with_hint_option(connection, test["statement"], table_name, limit, False, interval)
            execute_init_statements(connection, test["init"], table_name, limit)
            exec_time_with_hint = execute_statement_with_hint_option(connection, test["statement"], table_name, limit, True, interval)
            row.extend([f"{exec_time_no_hint:.2f}", f"{exec_time_with_hint:.2f}"])
        test_results.append(row)

    with open('test_results.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        header = ["SQL Statement"]
        for table_name in target_tables:
            header.extend([f"No Hint - {table_name}", f"With Hint - {table_name}"])
        writer.writerow(header)
        for result in test_results:
            writer.writerow(result)

    print("测试完成，结果已输出到 test_results.csv")

main('127.0.0.1', 4000, 'root', '', 'test1', limit=100000, interval=30)
