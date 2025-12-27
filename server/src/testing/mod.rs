use turso::{Builder, Database};

pub async fn create_test_db() -> Result<Database, turso::Error> {
    let db = Builder::new_local(":memory:").build().await?;
    let conn = db.connect()?;

    conn.execute(include_str!("../../sql/schema.sql"), ())
        .await?;
    return Ok(db);
}
