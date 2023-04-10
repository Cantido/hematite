use hematite::db::{Database, ExpectedRevision};
use std::path::PathBuf;
use cloudevents::event::Event;

fn main() -> anyhow::Result<()> {
    let _ = std::fs::remove_file("stream.db");
    let mut db = Database::new(&PathBuf::from("stream.db"))?;

    db.insert(&Event::default(), ExpectedRevision::Any)
}
