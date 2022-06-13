//! to reproduce https://github.com/pingcap/tidb/issues/25659, https://github.com/pingcap/tidb/issues/33393
//!
use dmlddl::Result;
use sqlx::mysql::MySqlPoolOptions;
use sqlx::{query, Executor};

#[tokio::main]
async fn main() -> Result<()> {
    let pool = MySqlPoolOptions::new()
        .connect("mysql://root@172.16.5.181:4000/test")
        .await?;
    let mut conn = pool.acquire().await?;
    conn.execute("drop table if exists t").await?;
    conn.execute("create table t (pk int, id int, v int, primary key (pk), unique key i1(id));")
        .await?;
    // conn.execute("insert into t values (1,1);").await?;
    let mut v = 1;
    loop {
        v += 1;
        conn.execute(query("select * from t use index(primary) where id = 1")).await?;
        conn.execute(query("begin pessimistic")).await?;
        conn.execute(query("update t set v = ? where id = 1;").bind(v))
            .await?;
        conn.execute(query("commit")).await?;
    }
}
