use cloudevents::event::Event;
use criterion::{criterion_group, criterion_main, Criterion};
use std::path::PathBuf;

use hematite::db::{Database, ExpectedRevision};

fn write_bench(c: &mut Criterion) {
    let _ = std::fs::remove_file("stream.db");

    let mut db = Database::new(&PathBuf::from("stream.db"));
    db.start().unwrap();

    c.bench_function("write event", |b| {
        b.iter(|| db.insert(Event::default(), ExpectedRevision::Any))
    });

    let _ = std::fs::remove_file("stream.db");
}

criterion_group!(benches, write_bench);
criterion_main!(benches);
