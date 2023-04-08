use criterion::{black_box, criterion_group, criterion_main, Criterion};
use std::path::PathBuf;

use hematite::db::Database;

fn write_bench(c: &mut Criterion) {
    let _ = std::fs::remove_file("stream.db");

    let mut db = Database::new(&PathBuf::from("stream.db")).expect("Could not intialize DB");

    c.bench_function("write event", |b| b.iter(|| db.insert(black_box(b"Hello world"))));

    let _ = std::fs::remove_file("stream.db");
}

criterion_group!(benches, write_bench);
criterion_main!(benches);
