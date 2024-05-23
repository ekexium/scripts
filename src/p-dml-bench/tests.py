tests = [
    {
        "alias": "simple insert",
        "init": [
            "TRUNCATE {target_table}",
        ],
        "statement": "INSERT INTO {target_table} SELECT * FROM {source_table} LIMIT {limit}",
    },
    {
        "alias": "insert ignore",
        "init": [
            "TRUNCATE {target_table}",
        ],
        "statement": "INSERT IGNORE INTO {target_table} SELECT * FROM {source_table} LIMIT {limit}",
    },
    {
        "alias": "insert on duplicate",
        "init": [
            "TRUNCATE {target_table}",
        ],
        "statement": "INSERT INTO {target_table} SELECT * from {source_table} limit {limit} ON DUPLICATE KEY UPDATE id={target_table}.id;",
    },
    {
        "alias": "replace empty",
        "init": [
            "TRUNCATE {target_table}",
        ],
        "statement": "REPLACE INTO {target_table} SELECT * from {source_table} limit {limit};",
    },
    {
        "alias": "update",
        "init": [
            "TRUNCATE {target_table}",
            "/*prepare data*/ INSERT /*+ SET_VAR(tidb_dml_type=bulk) */ INTO {target_table} SELECT * from {source_table} limit {limit};",
        ],
        "statement": "UPDATE {target_table} SET {column}={column}-1",
    },
    {
        "alias": "delete",
        "init": [
            "TRUNCATE {target_table}",
            "/*prepare data*/ INSERT /*+ SET_VAR(tidb_dml_type=bulk) */ INTO {target_table} SELECT * from {source_table} limit {limit};",
        ],
        "statement": "DELETE FROM {target_table}",
    },
]

