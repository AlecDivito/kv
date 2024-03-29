use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use kvs::{KvStore, KvsEngine, SledKvsEngine};
use rand::prelude::*;
use tempfile::TempDir;

fn set_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("set_bench");
    group.bench_function("kvs", |b| {
        b.iter_batched(
            || {
                let temp_dir = TempDir::new().unwrap();
                (KvStore::restore(temp_dir.as_ref()).unwrap(), temp_dir)
            },
            |(store, _temp_dir)| {
                for i in 1..(1 << 12) {
                    store
                        .set(format!("key{}", i).into_bytes(), b"value".to_vec())
                        .unwrap();
                }
            },
            BatchSize::SmallInput,
        )
    });
    group.bench_function("sled", |b| {
        b.iter_batched(
            || {
                let temp_dir = TempDir::new().unwrap();
                (SledKvsEngine::restore(temp_dir.as_ref()).unwrap(), temp_dir)
            },
            |(db, _temp_dir)| {
                for i in 1..(1 << 12) {
                    db.set(format!("key{}", i).into_bytes(), b"value".to_vec())
                        .unwrap();
                }
            },
            BatchSize::SmallInput,
        )
    });
    group.finish();
}

fn get_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("get_bench");
    for i in &vec![6, 8, 10, 12 /*, 16, 20*/] {
        group.bench_with_input(format!("kvs_{}", i), i, |b, i| {
            let temp_dir = TempDir::new().unwrap();
            let store = KvStore::restore(temp_dir.as_ref()).unwrap();
            for key_i in 1..(1 << i) {
                store
                    .set(format!("key{}", key_i).into_bytes(), b"value".to_vec())
                    .unwrap();
            }
            let mut rng = SmallRng::from_seed([0; 32]);
            b.iter(|| {
                store
                    .get(format!("key{}", rng.gen_range(1..1 << i)).as_bytes())
                    .unwrap();
            })
        });
    }
    for i in &vec![6, 8, 10, 12 /*, 16, 20*/] {
        group.bench_with_input(format!("sled_{}", i), i, |b, i| {
            let temp_dir = TempDir::new().unwrap();
            let db = SledKvsEngine::restore(temp_dir.as_ref()).unwrap();
            for key_i in 1..(1 << i) {
                db.set(format!("key{}", key_i).into_bytes(), b"value".to_vec())
                    .unwrap();
            }
            let mut rng = SmallRng::from_seed([0; 32]);
            b.iter(|| {
                db.get(format!("key{}", rng.gen_range(1..1 << i)).as_bytes())
                    .unwrap();
            })
        });
    }
    group.finish();
}

criterion_group!(benches, set_bench, get_bench);
criterion_main!(benches);
