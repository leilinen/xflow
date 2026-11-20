use sqlx::postgres::PgPoolOptions;
use std::ops::Deref;
use std::time::{SystemTime, UNIX_EPOCH};
use tempfile::{tempdir, TempDir};
use xflow::storage::db;

pub struct TestDatabase {
    _directory: TempDir,
    pool: sqlx::PgPool,
    database_url: String,
    schema: String,
}

impl TestDatabase {
    pub async fn new(max_connections: u32) -> Self {
        let database_url = std::env::var("TEST_DATABASE_URL")
            .expect("TEST_DATABASE_URL must point to an isolated PostgreSQL test database");
        let admin_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&database_url)
            .await
            .expect("connect to PostgreSQL test database");
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock before Unix epoch")
            .as_nanos();
        let schema = format!("test_{}_{}", std::process::id(), suffix);
        sqlx::query(&format!(r#"CREATE SCHEMA "{schema}""#))
            .execute(&admin_pool)
            .await
            .expect("create isolated test schema");
        admin_pool.close().await;

        let schema_for_connections = schema.clone();
        let pool = PgPoolOptions::new()
            .max_connections(max_connections)
            .after_connect(move |connection, _metadata| {
                let schema = schema_for_connections.clone();
                Box::pin(async move {
                    sqlx::query(&format!(r#"SET search_path TO "{schema}", public"#))
                        .execute(connection)
                        .await?;
                    Ok(())
                })
            })
            .connect(&database_url)
            .await
            .expect("connect to isolated test schema");
        db::init_db(&pool)
            .await
            .expect("initialize isolated test schema");

        Self {
            _directory: tempdir().expect("create test temp directory"),
            pool,
            database_url,
            schema,
        }
    }

    #[allow(dead_code)]
    pub fn path(&self) -> &std::path::Path {
        self._directory.path()
    }

    pub fn pg_pool(&self) -> &sqlx::PgPool {
        &self.pool
    }
}

impl Deref for TestDatabase {
    type Target = sqlx::PgPool;

    fn deref(&self) -> &Self::Target {
        &self.pool
    }
}

impl Drop for TestDatabase {
    fn drop(&mut self) {
        let pool = self.pool.clone();
        let database_url = self.database_url.clone();
        let schema = self.schema.clone();
        let cleanup = std::thread::spawn(move || {
            let runtime = tokio::runtime::Runtime::new().expect("create cleanup runtime");
            runtime.block_on(async move {
                let admin_pool = PgPoolOptions::new()
                    .max_connections(1)
                    .connect(&database_url)
                    .await
                    .expect("reconnect to clean up test schema");
                sqlx::query("SET statement_timeout TO '5s'")
                    .execute(&admin_pool)
                    .await
                    .expect("bound test schema cleanup");
                sqlx::query(&format!(r#"DROP SCHEMA "{schema}" CASCADE"#))
                    .execute(&admin_pool)
                    .await
                    .expect("drop isolated test schema");
                admin_pool.close().await;
                // Pool shutdown is cleanup, not test behavior. Never let a
                // leaked checkout turn the CI process into an unbounded wait;
                // dropping the final handle still closes remaining sockets.
                let _ = tokio::time::timeout(std::time::Duration::from_secs(2), pool.close()).await;
            });
        });
        if cleanup.join().is_err() && !std::thread::panicking() {
            panic!("test schema cleanup thread panicked");
        }
    }
}
