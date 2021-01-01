use anyhow;
use lmdb;
use std::{env, fs, time};

fn main() -> Result<(), anyhow::Error> {
    let path = env::args()
        .skip(1)
        .next()
        .expect("Must provide a destination path as an arg");

    fs::create_dir_all(&path)?;

    let env = lmdb::Environment::new()
        .set_map_size(100 * 1024 * 1024 * 1024)
        .open(std::path::Path::new(path.as_str()))?;

    let db = env.create_db(None, lmdb::DatabaseFlags::default())?;

    let mut txn = env.begin_rw_txn()?;

    let mut cursor = txn.open_rw_cursor(db)?;

    let overall_start = time::Instant::now();
    let mut start = time::Instant::now();
    let mut count = 0_u64;
    let sample_period = 1_000_000;

    let total_inserted = 100_000_000;

    for i in 0_u64..total_inserted {
        if count == sample_period {
            count = 0;
            let duration = start.elapsed();
            println!(
                "{}: inserted {} in {:?} ({}/s)",
                i,
                sample_period,
                duration,
                sample_period as f64 / duration.as_secs_f64()
            );
            start = time::Instant::now();
        };

        // INTEGER_KEY w/ native byte order didn't seem to help performance, so using big-endian
        // so APPEND will work, and be portable across architectures
        // APPEND only possible because we're writing in order
        cursor.put(&i.to_be_bytes(), &i.to_be_bytes(), lmdb::WriteFlags::APPEND)?;

        count += 1;
    }

    let overall_elapsed = overall_start.elapsed();
    println!(
        "Overall: inserted {} in {:?} ({}/s)",
        total_inserted,
        overall_elapsed,
        total_inserted as f64 / overall_elapsed.as_secs_f64()
    );

    Ok(())
}
