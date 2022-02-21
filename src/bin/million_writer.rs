// write a million rows.
// the i-th row: <i 2*i>

use dmlddl::Result;
use log::LevelFilter;
use sqlx::mysql::MySqlPoolOptions;
use sqlx::Executor;
use std::sync::atomic::AtomicU32;
use std::sync::Arc;

const COUNTER: AtomicU32 = AtomicU32::new(0);

#[tokio::main]
async fn main() -> Result<()> {
    simple_logging::log_to_file("dmlddl.log", LevelFilter::Info)?;
    let pool = MySqlPoolOptions::new()
        .max_connections(500)
        .connect("mysql://root@127.0.0.1:4000/test")
        .await?;
    let pool = Arc::new(pool);

    let mut conn = pool.acquire().await?;
    conn.execute("use test").await?;
    conn.execute("drop table if exists test").await?;
    conn.execute("create table t(a int primary key, b int)")
        .await?;
    drop(conn);

    let mut handles = Vec::new();
    let max = 10_000;
    for _ in 0..32 {
        let mut conn = pool.acquire().await?;
        handles.push(tokio::spawn(async move {
            let x = COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if x >= max {
                return;
            }
            conn.execute("begin").await.expect("begin failed");
            for i in 0..1000 {
                conn.execute(
                    format!(
                        "insert into t values({}, {})",
                        x * 1000 + i,
                        (x * 1000 + i) * 2
                    )
                    .as_str(),
                )
                .await
                .expect("insert failed");
            }
            conn.execute("commit").await.expect("commit failed");
        }));
    }
    Ok(())
}
