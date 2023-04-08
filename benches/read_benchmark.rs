use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion};
use std::path::PathBuf;
use rand::prelude::*;
use cloudevents::event::Event;

use hematite::db::Database;

fn write_bench(c: &mut Criterion) {
    let _ = std::fs::remove_file("stream.db");

    let mut db = Database::new(&PathBuf::from("stream.db")).expect("Could not intialize DB");

    for _n in 1..100_000 {
      let event = Event::default();
      db.insert(black_box(&event)).expect("Could not insert value into DB");
    }

    let mut rng = thread_rng();

    c.bench_function("read event", |b| b.iter_batched(
        || rng.gen_range(0..99_999),
        |rownum| db.query(rownum).expect("Row not found").expect("Failed to read DB"),
        BatchSize::SmallInput
    ));

    let _ = std::fs::remove_file("stream.db");
}

criterion_group!(benches, write_bench);
criterion_main!(benches);
