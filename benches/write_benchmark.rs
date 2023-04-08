use criterion::{black_box, criterion_group, criterion_main, Criterion};
use std::{fs::File, collections::BTreeMap};

use hematite::db;

fn write_bench(c: &mut Criterion) {
    let _ = std::fs::remove_file("stream.db");
    let mut file = File::options().append(true).create(true).open("stream.db").unwrap();
    let mut rowcount = 0;
    let mut primary_index = BTreeMap::new();

    c.bench_function("write event", |b| b.iter(|| db::insert(&mut file, &mut rowcount, &mut primary_index, black_box(b"Hello world"))));

    let _ = std::fs::remove_file("stream.db");
}

criterion_group!(benches, write_bench);
criterion_main!(benches);
