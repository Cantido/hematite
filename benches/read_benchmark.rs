use cloudevents::event::Event;
use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use tempfile::tempdir;

use hematite::db::{Database, ExpectedRevision};

fn read_bench(c: &mut Criterion) {
    let runtime =
        tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    let dir = tempdir().unwrap();
    let mut db = Database::new(dir.path());
    runtime
        .block_on(async {
            for _n in 1..100_000 {
                let event = Event::default();
                db.append(vec![event], ExpectedRevision::Any).await
                    .expect("Could not insert value into DB");
            }
        });

    c.bench_function("read event", |b| {
        b
        .to_async(&runtime)
        .iter_batched(
            || db.clone(),
            |db| async move {
                db.query(50_000, 1).await.expect("Failed to read DB");
            },
            BatchSize::SmallInput,
        )
    });
}

criterion_group!(benches, read_bench);
criterion_main!(benches);
