use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion};
use std::{fs::File, collections::BTreeMap};
use rand::prelude::*;

use hematite::db;

fn write_bench(c: &mut Criterion) {
    let _ = std::fs::remove_file("stream.db");
    let mut file = File::options().append(true).create(true).open("stream.db").unwrap();
    let mut rowcount = 0;
    let mut primary_index = BTreeMap::new();

    for _n in 1..100_000 {
      let data: [u8; 32] = random();
      db::insert(&mut file, &mut rowcount, &mut primary_index, black_box(&data)).unwrap();
    }

    let mut rng = thread_rng();

    c.bench_function("read event", |b| b.iter_batched(
        || rng.gen_range(0..99_999),
        |rownum| db::query(&mut file, &primary_index, rownum),
        BatchSize::SmallInput
    ));

    let _ = std::fs::remove_file("stream.db");
}

criterion_group!(benches, write_bench);
criterion_main!(benches);
