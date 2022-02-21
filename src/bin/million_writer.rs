// write a million rows.
// the i-th row: <i 2*i>

use dmlddl::Result;
use futures::stream;
use futures::StreamExt;
use log::LevelFilter;
use sqlx::mysql::MySqlPoolOptions;
use sqlx::Executor;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<()> {
    simple_logging::log_to_file("dmlddl.log", LevelFilter::Info)?;
    let pool = MySqlPoolOptions::new()
        .max_connections(32)
        .connect("mysql://root@127.0.0.1:4000/test")
        .await?;
    let pool = Arc::new(pool);

    let mut conn = pool.acquire().await?;
    conn.execute("use test").await?;
    conn.execute("drop table if exists test").await?;
    conn.execute("create table t(a int, b int)").await?;
    drop(conn);

    let keys = 0..10_0;
    stream::iter(keys)
        .for_each_concurrent(32, |i| {
            let pool = pool.clone();
            async move {
                pool.clone()
                    .execute(format!("insert into t values({}, {})", i, 2 * i).as_str())
                    .await
                    .expect("insert fail");
            }
        })
        .await;
    Ok(())
}
