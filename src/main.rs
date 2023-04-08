use hematite::db::Database;
use std::path::PathBuf;
use cloudevents::event::Event;

fn main() -> std::io::Result<()> {
    let _ = std::fs::remove_file("stream.db");
    let mut db = Database::new(&PathBuf::from("stream.db"))?;

    db.insert(&Event::default())
}
