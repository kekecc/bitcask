use bitcask::{options::BitcaskOptions, storage::Bitcask};
use criterion::{criterion_group, criterion_main, Criterion};
use fake::{faker::lorem::en::Sentence, Fake};
use rand::random;

fn bench(c: &mut Criterion) {
    let ops = BitcaskOptions::default();
    let bitcask = Bitcask::open(ops).unwrap();

    let key = Sentence(32..64);
    let value = Sentence(4000..4001);

    let insert_keys: Vec<String> = (0..10000)
        .into_iter()
        .map(|_| {
            let k = key.fake::<String>();
            assert!(bitcask.put(k.clone(), value.fake::<String>()).is_ok());
            k
        })
        .collect();

    c.bench_function("get", |b| {
        b.iter_batched(
            || &insert_keys[random::<usize>() % 10000],
            |k| {
                assert!(bitcask.get(k).is_ok());
            },
            criterion::BatchSize::SmallInput,
        );
    });

    c.bench_function("put", |b| {
        b.iter_batched(
            || (key.fake::<String>(), value.fake::<String>()),
            |(k, v)| {
                assert!(bitcask.put(k, v).is_ok());
            },
            criterion::BatchSize::SmallInput,
        );
    });

    c.bench_function("del", |b| {
        b.iter_batched(
            || insert_keys[random::<usize>() % 10000].clone(),
            |k| assert!(bitcask.delete(k.as_bytes()).is_ok()),
            criterion::BatchSize::SmallInput,
        );
    });
}

criterion_group!(benches, bench);
criterion_main!(benches);
