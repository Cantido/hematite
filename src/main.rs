use hematite::db::Database;
use std::path::PathBuf;

fn main() -> std::io::Result<()> {
    let _ = std::fs::remove_file("stream.db");
    let mut db = Database::new(&PathBuf::from("stream.db"))?;
    let event = b"Hello, world!";

    db.insert(event)
}
