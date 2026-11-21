use std::hint::black_box;
use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use kerniva_engine::{Engine, Options, SyncPolicy, WriteOp};

struct TempDirectory(PathBuf);

impl Drop for TempDirectory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn percentile(samples: &mut [Duration], percent: usize) -> u128 {
    samples.sort_unstable();
    samples[(samples.len() - 1) * percent / 100].as_micros()
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let arguments: Vec<String> = std::env::args().collect();
    let operations = arguments
        .get(1)
        .map_or(Ok(10000), |value| value.parse::<usize>())?;
    let value_bytes = arguments
        .get(2)
        .map_or(Ok(256), |value| value.parse::<usize>())?;
    let batch_size = arguments
        .get(3)
        .map_or(Ok(1), |value| value.parse::<usize>())?;
    let policy_name = arguments.get(4).map_or("none", String::as_str);
    if operations == 0 || batch_size == 0 {
        return Err("operations and batch size must be positive".into());
    }
    let policy = match policy_name {
        "none" => SyncPolicy::None,
        "flush" => SyncPolicy::Flush,
        "sync" => SyncPolicy::SyncData,
        _ => return Err("sync policy must be none, flush, or sync".into()),
    };
    let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let directory = TempDirectory(
        std::env::temp_dir().join(format!("kerniva_benchmark_{}_{nonce}", std::process::id())),
    );
    let mut options = Options::new(&directory.0);
    options.sync_policy = policy;
    let engine = Engine::open(options)?;
    let value = vec![b'x'; value_bytes];
    let mut commit_times = Vec::new();
    let write_started = Instant::now();
    for start in (0..operations).step_by(batch_size) {
        let end = start.saturating_add(batch_size).min(operations);
        let commit_started = Instant::now();
        if batch_size == 1 {
            engine.put(format!("key-{start:09}").as_bytes(), &value)?;
        } else {
            let batch: Vec<_> = (start..end)
                .map(|index| WriteOp::put(format!("key-{index:09}").into_bytes(), value.clone()))
                .collect();
            engine.write_batch(&batch)?;
        }
        commit_times.push(commit_started.elapsed());
    }
    let write_seconds = write_started.elapsed().as_secs_f64();
    let write_p50 = percentile(&mut commit_times, 50);
    let write_p95 = percentile(&mut commit_times, 95);
    let write_p99 = percentile(&mut commit_times, 99);
    let read_operations = operations.min(10000);
    let mut read_times = Vec::with_capacity(read_operations);
    let read_started = Instant::now();
    for index in 0..read_operations {
        let started = Instant::now();
        let result = engine.get(format!("key-{index:09}").as_bytes())?;
        let result = result.ok_or("benchmark key disappeared")?;
        if result != value {
            return Err("benchmark value mismatch".into());
        }
        black_box(result);
        read_times.push(started.elapsed());
    }
    let read_seconds = read_started.elapsed().as_secs_f64();
    let read_p95 = percentile(&mut read_times, 95);
    let checkpoint_started = Instant::now();
    engine.checkpoint()?;
    let checkpoint_ms = checkpoint_started.elapsed().as_secs_f64() * 1000.0;
    let stats = engine.stats();
    println!(
        "{{\"operations\":{operations},\"value_bytes\":{value_bytes},\"batch_size\":{batch_size},\"sync_policy\":\"{policy_name}\",\"write_seconds\":{write_seconds:.6},\"write_ops_per_second\":{:.0},\"commit_p50_us\":{write_p50},\"commit_p95_us\":{write_p95},\"commit_p99_us\":{write_p99},\"read_operations\":{read_operations},\"read_ops_per_second\":{:.0},\"read_p95_us\":{read_p95},\"checkpoint_ms\":{checkpoint_ms:.3},\"segment_count\":{},\"write_timing_includes_checkpoint\":false}}",
        operations as f64 / write_seconds,
        read_operations as f64 / read_seconds,
        stats.segment_count,
    );
    drop(engine);
    Ok(())
}
