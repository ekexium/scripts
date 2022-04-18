/// load SQLs from a file. Execute them in a large SQL.
use dmlddl::Result;
use sqlx::mysql::MySqlPoolOptions;
use sqlx::Executor;
use std::fs::File;
use std::io::BufRead;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<()> {
    let pool = MySqlPoolOptions::new()
        .max_connections(500)
        .connect("mysql://root@172.16.5.181:4000/test")
        .await?;
    let pool = Arc::new(pool);

    let mut conn = pool.acquire().await?;
    conn.execute("use credit_card").await?;
    conn.execute("drop table if exists T_CUSTOMER").await?;

    let schema = std::fs::read_to_string("insert/CREDIT_CARD.T_CUSTOMER-schema.sql")?;
    conn.execute(schema.as_str()).await?;
    let sql_file = File::open("insert/CREDIT_CARD.T_CUSTOMER.1.sql")?;
    let lines = std::io::BufReader::new(sql_file).lines();
    conn.execute("begin").await?;
    let mut sql = String::new();
    for line in lines {
        let line = line?;
        if line.starts_with("INSERT") {
            println!("{}", sql);
            conn.execute(sql.as_str()).await?;
            sql.clear();
        }
        sql.push_str(&line);
    }
    println!("{}", sql);
    conn.execute(sql.as_str()).await?;
    conn.execute("commit").await?;
    Ok(())
}
