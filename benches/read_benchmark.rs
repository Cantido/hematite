use cloudevents::event::Event;
use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use rand::prelude::*;
use std::path::PathBuf;

use hematite::db::{Database, ExpectedRevision};

fn write_bench(c: &mut Criterion) {
    let _ = std::fs::remove_file("stream.db");

    let mut db = Database::new(&PathBuf::from("stream.db"));
    db.start().unwrap();

    for _n in 1..100_000 {
        let event = Event::default();
        db.insert(event, ExpectedRevision::Any)
            .expect("Could not insert value into DB");
    }

    let mut rng = thread_rng();

    c.bench_function("read event", |b| {
        b.iter_batched(
            || rng.gen_range(0..99_999),
            |rownum| {
                db.query(rownum)
                    .expect("Row not found")
                    .expect("Failed to read DB")
            },
            BatchSize::SmallInput,
        )
    });

    let _ = std::fs::remove_file("stream.db");
}

criterion_group!(benches, write_bench);
criterion_main!(benches);
