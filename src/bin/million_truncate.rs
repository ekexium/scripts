use std::time::Duration;

use dmlddl::Result;
use log::info;
use sqlx::mysql::MySqlPoolOptions;
use sqlx::{Executor, MySql, Pool};
use tokio::time;

const MAX_CONNECTIONS: u32 = 100;
const TRUNCATE_COUNT: u32 = 5_000;
const LOG_INTERVAL: u32 = 1000;
const GC_CHECK_INTERVAL: Duration = Duration::from_secs(1);
const MAX_WAIT_TIME: Duration = Duration::from_secs(60 * 60);

#[tokio::main]
async fn main() -> Result<()> {
    setup_logger();

    let pool = setup_database_connection().await?;
    let _ = pool.execute("set @@global.tidb_gc_run_interval=1m").await;
    let _ = pool.execute("set @@global.tidb_gc_life_time=1m").await;
    let _ = pool.execute("set @@global.tidb_gc_concurrency=6").await;

    create_table(&pool).await?;

    wait_for_gc(&pool).await?;

    info!("Disabling GC");
    pool.execute("SET @@global.tidb_gc_enable=FALSE").await?;

    perform_truncations(&pool, TRUNCATE_COUNT).await?;
    // pass gc life time
    time::sleep(Duration::from_secs(60)).await;

    let num_ranges = query_gc_delete_range(&pool).await?;
    info!("Number of ranges to delete: {}", num_ranges);

    info!("Re-enabling GC");
    pool.execute("SET @@global.tidb_gc_enable=TRUE").await?;
    wait_for_gc(&pool).await?;

    info!("Delete range finished");
    Ok(())
}

fn setup_logger() {
    simple_logger::SimpleLogger::new()
        .with_utc_timestamps()
        .with_colors(true)
        .with_level(log::LevelFilter::Info)
        .init()
        .unwrap();
}

async fn setup_database_connection() -> Result<Pool<MySql>> {
    MySqlPoolOptions::new()
        .max_connections(MAX_CONNECTIONS)
        .connect("mysql://root@127.0.0.1:4000/test")
        .await
        .map_err(|e| dmlddl::error::MyError::SqlxError { sqlx: e })
}

async fn create_table(pool: &Pool<MySql>) -> Result<()> {
    pool.execute("CREATE TABLE IF NOT EXISTS t(a INT PRIMARY KEY, b INT)")
        .await?;
    Ok(())
}

async fn perform_truncations(pool: &Pool<MySql>, count: u32) -> Result<()> {
    for i in 0..count {
        if i % LOG_INTERVAL == 0 {
            info!("Truncated {} times", i);
        }
        pool.execute("TRUNCATE TABLE t").await?;
    }
    Ok(())
}

async fn wait_for_gc(pool: &Pool<MySql>) -> Result<()> {
    let start = std::time::Instant::now();
    info!("Waiting for current GC to finish");
    pool.execute("SET @@global.tidb_gc_enable=TRUE").await?;

    while query_gc_delete_range(pool).await? > 0 {
        if start.elapsed() > MAX_WAIT_TIME {
            return Err(dmlddl::error::MyError::StringError(
                "Timeout waiting for GC".to_string(),
            ));
        }
        time::sleep(GC_CHECK_INTERVAL).await;
    }
    Ok(())
}

async fn query_gc_delete_range(pool: &Pool<MySql>) -> Result<i64> {
    sqlx::query_scalar("SELECT COUNT(*) FROM mysql.gc_delete_range")
        .fetch_one(pool)
        .await
        .map_err(|e| dmlddl::error::MyError::SqlxError { sqlx: e })
}
