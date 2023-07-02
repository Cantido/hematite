use hematite::db::{Database, ExpectedRevision};
use std::path::PathBuf;
use cloudevents::event::Event;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _ = std::fs::remove_file("stream.db");
    let mut db = Database::new(&PathBuf::from("stream.db")).await?;

    db.insert(&mut Event::default(), ExpectedRevision::Any).await
}
