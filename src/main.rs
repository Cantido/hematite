use std::{fs::File, collections::BTreeMap};

use hematite::db;

fn main() -> std::io::Result<()> {
    std::fs::remove_file("stream.db")?;
    let mut file = File::options().append(true).create(true).open("stream.db")?;
    let mut row_count = 0;
    let mut primary_index = BTreeMap::new();
    let event = b"Hello, world!";


    db::insert(&mut file, &mut row_count, &mut primary_index, event)
}
