use std::sync::Arc;
use std::time::Duration;

use dmlddl::workload::create_table;
use dmlddl::workload::ddl_worker;
use dmlddl::workload::dml_worker;
use dmlddl::Result;
use log::LevelFilter;
use sqlx::mysql::MySqlPoolOptions;
use sqlx::Executor;
use tokio::sync::broadcast::channel;

#[tokio::main]

async fn main() -> Result<()> {
    simple_logging::log_to_file("dmlddl.log", LevelFilter::Info)?;
    let pool = MySqlPoolOptions::new()
        .max_connections(32)
        .connect("mysql://root@127.0.0.1:4000/test")
        .await?;
    let pool = Arc::new(pool);
    let mut conn1 = pool.acquire().await?;
    let mut conn2 = pool.acquire().await?;

    // init
    conn1.execute("use test").await?;
    create_table(&mut conn1).await?;
    conn1
        .execute("set @@tidb_txn_assertion_level=strict")
        .await?; // ensure assertion is supported
    conn1.execute("set @@tidb_general_log=1").await?; // ensure partition is supported
    let (tx, rx1) = channel(1);
    let rx2 = tx.subscribe();
    let h1 = tokio::spawn(async move { dml_worker(&mut conn1, rx1).await });
    let h2 = tokio::spawn(async move { ddl_worker(&mut conn2, rx2).await });
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(60 * 60 * 24)).await;
        tx.send(()).unwrap();
    });

    h1.await.unwrap()?;
    h2.await.unwrap()?;
    Ok(())
}
