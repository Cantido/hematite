use cloudevents::event::Event;
use criterion::{criterion_group, criterion_main, Criterion};
use tempfile::tempdir;

use hematite::db::{Database, ExpectedRevision};

fn write_bench(c: &mut Criterion) {
    let runtime =
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();

    c.bench_function("write event", |b| {
        b.to_async(&runtime).iter(|| async {
            let dir = tempdir().unwrap();
            let mut db = Database::new(dir.path());
            db.start().await.unwrap();
            db.append(vec![Event::default()], ExpectedRevision::Any).await.unwrap();
        })
    });
}

criterion_group!(benches, write_bench);
criterion_main!(benches);
