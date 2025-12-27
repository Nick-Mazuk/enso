use turso::{Builder, Connection};

pub async fn new_test_database_connection() -> Result<Connection, turso::Error> {
    let database = Builder::new_local(":memory:").build().await?;
    let connection = database.connect()?;

    connection
        .execute_batch(include_str!("../../sql/schema.sql"))
        .await?;
    return Ok(connection);
}
