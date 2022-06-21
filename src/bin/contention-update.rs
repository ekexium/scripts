//! To produce many rollback records to benchmark the effect of collapsing rollbacks.
//! Write transactions keeps updating a set of keys that reside in different regions.
//! Read transactions read these keys.
//!
//! We check:
//! (1) there are a lot of rollback records in MVCC (if we use a patched TiKV that doesn't collapse rollbacks)
//! (2) the read performance degrades as the number of rollback records increases.
use dmlddl::Result;
use futures::future::join_all;
use rand::{Rng, SeedableRng};
use sqlx::mysql::MySqlPoolOptions;
use sqlx::pool::PoolConnection;
use sqlx::{query, Executor};
use tokio;

#[tokio::main]
async fn main() -> Result<()> {
    let pool = MySqlPoolOptions::new()
        .max_connections(20)
        .connect("mysql://root@127.0.0.1:4000/test")
        .await?;
    let mut conn = pool.acquire().await?;

    // import data
    conn.execute("drop table if exists t").await?;
    conn.execute(
        "create table t (id int, v int, primary key (id
    ));",
    )
    .await?;
    conn.execute(
        format!(
            "insert into t values {}",
            (1..100)
                .map(|x| format!("({}, {})", x, 0))
                .collect::<Vec<_>>()
                .join(",")
        )
        .as_str(),
    )
    .await?;

    let mut handles = vec![];
    for _ in 0..15 {
        let c = pool.acquire().await?;
        handles.push(tokio::spawn(async move {
            worker(c).await;
        }));
    }
    join_all(handles).await;

    Ok(())
}

async fn worker(mut c: PoolConnection<sqlx::mysql::MySql>) {
    let mut rng = rand::rngs::SmallRng::from_entropy();
    for _ in 0..10 {
        let _ = c.execute(query("begin")).await;
        for _ in 0..10 {
            let _ = c
                .execute(
                    query("update t set v = v + 1 where id = {}")
                        .bind(rng.gen_range::<i32, _>(0..100)),
                )
                .await;
        }
        let _ = c.execute(query("commit")).await;
    }
}
