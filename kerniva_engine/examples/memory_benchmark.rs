use std::fmt::Write as _;
use std::hint::black_box;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Instant;

use kerniva_engine::{
    DurableMemoryEngine, DurableMemoryError, DurableReadMode, DurableWalStats, MemoryBatchOp,
    MemoryEngine, MemoryError, MemoryStats, MemoryWriteOptions,
};

struct Config {
    shards: usize,
    threads: usize,
    keys: usize,
    operations: usize,
    warmup_per_thread: usize,
    read_percent: usize,
    hot_percent: usize,
    hot_keys: usize,
    value_bytes: usize,
    sample_every: usize,
    seed: u64,
    max_payload_bytes: Option<usize>,
    durable_directory: Option<PathBuf>,
    durable_read_mode: Option<DurableReadMode>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            shards: 64,
            threads: 16,
            keys: 100_000,
            operations: 1_000_000,
            warmup_per_thread: 2_000,
            read_percent: 95,
            hot_percent: 0,
            hot_keys: 1,
            value_bytes: 32,
            sample_every: 16,
            seed: 42,
            max_payload_bytes: None,
            durable_directory: None,
            durable_read_mode: None,
        }
    }
}

fn parse_config() -> Result<Option<Config>, String> {
    let mut config = Config::default();
    let mut arguments = std::env::args().skip(1);
    while let Some(name) = arguments.next() {
        if name == "--help" || name == "-h" {
            println!(
                "memory_benchmark [--shards N] [--threads N] [--keys N] [--operations N] \
                [--warmup-per-thread N] [--read-percent 0..100] [--hot-percent 0..100] \
                [--hot-keys N] [--value-bytes N] [--sample-every N] [--seed N] \
                [--max-payload-bytes N] [--durable-directory EXISTING_EMPTY_DIRECTORY] \
                [--durable-read-mode hold|committed]"
            );
            return Ok(None);
        }
        let raw = arguments
            .next()
            .ok_or_else(|| format!("missing value for {name}"))?;
        if name == "--durable-directory" {
            if config.durable_directory.is_some() {
                return Err("--durable-directory may be specified only once".into());
            }
            config.durable_directory = Some(PathBuf::from(raw));
            continue;
        }
        if name == "--durable-read-mode" {
            if config.durable_read_mode.is_some() {
                return Err("--durable-read-mode may be specified only once".into());
            }
            config.durable_read_mode = Some(match raw.as_str() {
                "hold" => DurableReadMode::HoldAffectedShards,
                "committed" => DurableReadMode::ReadCommitted,
                _ => return Err("--durable-read-mode must be hold or committed".into()),
            });
            continue;
        }
        if name == "--seed" {
            config.seed = raw.parse().map_err(|_| format!("invalid seed: {raw}"))?;
            continue;
        }
        let value: usize = raw
            .parse()
            .map_err(|_| format!("invalid value for {name}: {raw}"))?;
        match name.as_str() {
            "--shards" => config.shards = value,
            "--threads" => config.threads = value,
            "--keys" => config.keys = value,
            "--operations" => config.operations = value,
            "--warmup-per-thread" => config.warmup_per_thread = value,
            "--read-percent" => config.read_percent = value,
            "--hot-percent" => config.hot_percent = value,
            "--hot-keys" => config.hot_keys = value,
            "--value-bytes" => config.value_bytes = value,
            "--sample-every" => config.sample_every = value,
            "--max-payload-bytes" => config.max_payload_bytes = Some(value),
            _ => return Err(format!("unknown option: {name}")),
        }
    }
    if config.shards == 0 || config.threads == 0 || config.keys == 0 || config.sample_every == 0 {
        return Err("shards, threads, keys, and sample-every must be positive".into());
    }
    if config.operations < config.threads {
        return Err("operations must be at least the thread count".into());
    }
    if config.read_percent > 100 || config.hot_percent > 100 {
        return Err("read-percent and hot-percent must be at most 100".into());
    }
    if config.hot_keys == 0 || config.hot_keys > config.keys {
        return Err("hot-keys must be between 1 and keys".into());
    }
    if config.durable_read_mode.is_some() && config.durable_directory.is_none() {
        return Err("--durable-read-mode requires --durable-directory".into());
    }
    Ok(Some(config))
}

struct Random(u64);

impl Random {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
}

#[derive(Default)]
struct OperationStats {
    offered: usize,
    succeeded: usize,
    missing: usize,
    errors: usize,
    first_error: Option<String>,
    samples_ns: Vec<u64>,
}

impl OperationStats {
    fn reserve(&mut self, samples: usize) -> Result<(), String> {
        self.samples_ns
            .try_reserve_exact(samples)
            .map_err(|error| error.to_string())
    }

    fn merge(&mut self, mut other: Self) {
        self.offered += other.offered;
        self.succeeded += other.succeeded;
        self.missing += other.missing;
        self.errors += other.errors;
        self.samples_ns.append(&mut other.samples_ns);
        if self.first_error.is_none() {
            self.first_error = other.first_error;
        }
    }

    fn failures(&self) -> usize {
        self.missing + self.errors
    }
}

#[derive(Default)]
struct WorkloadStats {
    get: OperationStats,
    put: OperationStats,
}

// Generic dispatch keeps the default in-memory workload monomorphized rather
// than introducing a virtual or enum branch in every timed native operation.
trait BenchmarkStore: Send + Sync + 'static {
    type Error: std::fmt::Debug;

    fn get(&self, key: &[u8]) -> Result<Option<Arc<[u8]>>, Self::Error>;
    fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Self::Error>;
    fn stats(&self) -> Result<MemoryStats, Self::Error>;

    fn wal_stats(&self) -> Result<Option<DurableWalStats>, Self::Error> {
        Ok(None)
    }

    fn preload(&self, keys: &[[u8; 8]], value: &[u8]) -> Result<(), String> {
        for (index, key) in keys.iter().enumerate() {
            self.put(key, value)
                .map_err(|error| format!("preload key {index}: {error:?}"))?;
        }
        Ok(())
    }
}

impl BenchmarkStore for MemoryEngine {
    type Error = MemoryError;

    #[inline]
    fn get(&self, key: &[u8]) -> Result<Option<Arc<[u8]>>, Self::Error> {
        MemoryEngine::get(self, key)
    }

    #[inline]
    fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        MemoryEngine::put(self, key, value)
    }

    fn stats(&self) -> Result<MemoryStats, Self::Error> {
        MemoryEngine::stats(self)
    }
}

impl BenchmarkStore for DurableMemoryEngine {
    type Error = DurableMemoryError;

    #[inline]
    fn get(&self, key: &[u8]) -> Result<Option<Arc<[u8]>>, Self::Error> {
        DurableMemoryEngine::get(self, key)
    }

    #[inline]
    fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        DurableMemoryEngine::put(self, key, value)
    }

    fn stats(&self) -> Result<MemoryStats, Self::Error> {
        DurableMemoryEngine::stats(self)
    }

    fn wal_stats(&self) -> Result<Option<DurableWalStats>, Self::Error> {
        DurableMemoryEngine::wal_stats(self).map(Some)
    }

    fn preload(&self, keys: &[[u8; 8]], value: &[u8]) -> Result<(), String> {
        for (index, chunk) in keys.chunks(256).enumerate() {
            let operations: Vec<_> = chunk
                .iter()
                .map(|key| MemoryBatchOp::Put {
                    key,
                    value,
                    options: MemoryWriteOptions::default(),
                })
                .collect();
            if !self
                .apply_batch_at(&operations, 0)
                .map_err(|error| format!("preload batch {index}: {error:?}"))?
            {
                return Err(format!("unconditional preload batch {index} was rejected"));
            }
        }
        Ok(())
    }
}

fn empty_durable_directory(directory: &Path) -> Result<PathBuf, String> {
    let metadata = std::fs::symlink_metadata(directory)
        .map_err(|error| format!("durable directory: {error}"))?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err("--durable-directory must name an existing non-symlink directory".into());
    }
    let directory = std::fs::canonicalize(directory)
        .map_err(|error| format!("resolve durable directory: {error}"))?;
    if std::fs::read_dir(&directory)
        .map_err(|error| format!("read durable directory: {error}"))?
        .next()
        .transpose()
        .map_err(|error| format!("read durable directory entry: {error}"))?
        .is_some()
    {
        return Err(
            "--durable-directory must be empty; existing files are never removed or reused".into(),
        );
    }
    Ok(directory)
}

fn wal_stats_json(stats: DurableWalStats) -> String {
    format!(
        "{{\"log_bytes\":{},\"commits\":{},\"failed_commits\":{},\"sync_attempts\":{},\"sync_elapsed_ns\":{},\"requires_reopen\":{}}}",
        stats.log_bytes,
        stats.commits,
        stats.failed_commits,
        stats.sync_attempts,
        stats.sync_elapsed_ns,
        stats.requires_reopen,
    )
}

fn durability_fields(
    before: Option<DurableWalStats>,
    after: Option<DurableWalStats>,
    directory: Option<&Path>,
    read_mode: DurableReadMode,
) -> Result<String, String> {
    let (Some(before), Some(after), Some(directory)) = (before, after, directory) else {
        return Ok(String::new());
    };
    let delta = DurableWalStats {
        log_bytes: after
            .log_bytes
            .checked_sub(before.log_bytes)
            .ok_or("WAL byte counter decreased")?,
        commits: after
            .commits
            .checked_sub(before.commits)
            .ok_or("WAL commit counter decreased")?,
        failed_commits: after
            .failed_commits
            .checked_sub(before.failed_commits)
            .ok_or("WAL failure counter decreased")?,
        sync_attempts: after
            .sync_attempts
            .checked_sub(before.sync_attempts)
            .ok_or("WAL sync counter decreased")?,
        sync_elapsed_ns: after
            .sync_elapsed_ns
            .checked_sub(before.sync_elapsed_ns)
            .ok_or("WAL sync time decreased")?,
        requires_reopen: after.requires_reopen,
    };
    let read_mode = match read_mode {
        DurableReadMode::HoldAffectedShards => "hold",
        DurableReadMode::ReadCommitted => "committed",
    };
    let mut fields = format!(
        ",\"durability\":\"sync\",\"durable_directory\":{},\"durable_read_mode\":\"{read_mode}\",\"preload_batch_size\":256",
        json_string(&directory.to_string_lossy()),
    );
    fields.push_str(
        ",\"wal\":{\"scope\":\"measured workload only; excludes preload and warmup\",\"before\":",
    );
    fields.push_str(&wal_stats_json(before));
    fields.push_str(",\"after\":");
    fields.push_str(&wal_stats_json(after));
    fields.push_str(",\"delta\":");
    fields.push_str(&wal_stats_json(delta));
    fields.push('}');
    Ok(fields)
}

struct Inputs<E: BenchmarkStore> {
    engine: E,
    keys: Vec<[u8; 8]>,
    value: Vec<u8>,
    config: Config,
}

fn run_workload<E: BenchmarkStore>(
    inputs: &Inputs<E>,
    operations: usize,
    measured: bool,
    random: &mut Random,
    sample_offset: usize,
    stats: &mut WorkloadStats,
) {
    let config = &inputs.config;
    for index in 0..operations {
        let reading = random.next() % 100 < config.read_percent as u64;
        let hot = config.hot_percent != 0 && random.next() % 100 < config.hot_percent as u64;
        let key_limit = if hot { config.hot_keys } else { config.keys };
        let key = &inputs.keys[(random.next() % key_limit as u64) as usize];
        let operation = if reading {
            &mut stats.get
        } else {
            &mut stats.put
        };
        let sampled = measured && index % config.sample_every == sample_offset;
        let started = sampled.then(Instant::now);
        let result = if reading {
            inputs
                .engine
                .get(key)
                .map(|value| black_box(value).is_some())
        } else {
            inputs.engine.put(key, &inputs.value).map(|()| true)
        };
        let elapsed_ns = started.map(|time| time.elapsed().as_nanos().min(u64::MAX as u128) as u64);
        operation.offered += 1;
        match result {
            Ok(true) => {
                operation.succeeded += 1;
                if let Some(elapsed_ns) = elapsed_ns {
                    operation.samples_ns.push(elapsed_ns);
                }
            }
            Ok(false) => operation.missing += 1,
            Err(error) => {
                operation.errors += 1;
                if operation.first_error.is_none() {
                    operation.first_error = Some(format!("{error:?}"));
                }
            }
        }
    }
}

fn json_string(text: &str) -> String {
    let mut escaped = String::from("\"");
    for character in text.chars() {
        match character {
            '"' => escaped.push_str("\\\""),
            '\\' => escaped.push_str("\\\\"),
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            '\t' => escaped.push_str("\\t"),
            character if character < '\u{20}' => {
                let _ = write!(escaped, "\\u{:04x}", character as u32);
            }
            character => escaped.push(character),
        }
    }
    escaped.push('"');
    escaped
}

fn quantile(samples: &[u64], numerator: usize, denominator: usize) -> String {
    if samples.is_empty() {
        return "null".into();
    }
    let count = samples.len();
    let rank = (count / denominator) * numerator
        + ((count % denominator) * numerator).div_ceil(denominator);
    samples[rank - 1].to_string()
}

fn operation_json(stats: &mut OperationStats) -> String {
    stats.samples_ns.sort_unstable();
    let first_error = stats
        .first_error
        .as_deref()
        .map_or_else(|| "null".into(), json_string);
    format!(
        "{{\"offered\":{},\"succeeded\":{},\"failures\":{},\"missing\":{},\"engine_errors\":{},\"first_error\":{},\"latency_samples\":{},\"p99_9_expected_tail_samples\":{:.6},\"p50_ns\":{},\"p95_ns\":{},\"p99_ns\":{},\"p99_9_ns\":{}}}",
        stats.offered,
        stats.succeeded,
        stats.failures(),
        stats.missing,
        stats.errors,
        first_error,
        stats.samples_ns.len(),
        stats.samples_ns.len() as f64 * 0.001,
        quantile(&stats.samples_ns, 50, 100),
        quantile(&stats.samples_ns, 95, 100),
        quantile(&stats.samples_ns, 99, 100),
        quantile(&stats.samples_ns, 999, 1000),
    )
}

fn run(config: Config) -> Result<bool, String> {
    if let Some(directory) = &config.durable_directory {
        let directory = empty_durable_directory(directory)?;
        let engine = DurableMemoryEngine::open_with_read_mode(
            &directory,
            config.shards,
            config.max_payload_bytes,
            config.durable_read_mode.unwrap_or_default(),
        )
        .map_err(|error| format!("durable engine initialization: {error:?}"))?;
        let initial = engine
            .stats()
            .map_err(|error| format!("empty durable stats: {error:?}"))?;
        if initial.keys != 0 || initial.payload_bytes != 0 {
            return Err("durable directory changed before open; refusing recovered data".into());
        }
        run_engine(config, engine)
    } else {
        let engine = MemoryEngine::new(config.shards, config.max_payload_bytes)
            .map_err(|error| format!("engine initialization: {error:?}"))?;
        run_engine(config, engine)
    }
}

fn run_engine<E: BenchmarkStore>(config: Config, engine: E) -> Result<bool, String> {
    let mut keys = Vec::new();
    keys.try_reserve_exact(config.keys)
        .map_err(|error| error.to_string())?;
    for index in 0..config.keys {
        keys.push((index as u64).to_le_bytes());
    }
    let mut value = Vec::new();
    value
        .try_reserve_exact(config.value_bytes)
        .map_err(|error| error.to_string())?;
    value.resize(config.value_bytes, b'v');
    let preload_started = Instant::now();
    engine.preload(&keys, &value)?;
    let preload_seconds = preload_started.elapsed().as_secs_f64();
    let before = engine
        .stats()
        .map_err(|error| format!("initial stats: {error:?}"))?;
    let mut workers = Vec::new();
    let mut reserved_sample_bytes = 0;
    for index in 0..config.threads {
        let operations = config.operations / config.threads
            + usize::from(index < config.operations % config.threads);
        let capacity = operations.div_ceil(config.sample_every);
        let mut measured = WorkloadStats::default();
        measured.get.reserve(capacity)?;
        measured.put.reserve(capacity)?;
        reserved_sample_bytes += (measured.get.samples_ns.capacity()
            + measured.put.samples_ns.capacity())
            * std::mem::size_of::<u64>();
        workers.push((operations, measured));
    }
    let inputs = Arc::new(Inputs {
        engine,
        keys,
        value,
        config,
    });
    let ready = Arc::new(Barrier::new(inputs.config.threads + 1));
    let start = Arc::new(Barrier::new(inputs.config.threads + 1));
    let finished = Arc::new(Barrier::new(inputs.config.threads + 1));
    let mut handles = Vec::new();
    for (index, (operations, mut measured)) in workers.into_iter().enumerate() {
        let inputs = Arc::clone(&inputs);
        let ready = Arc::clone(&ready);
        let start = Arc::clone(&start);
        let finished = Arc::clone(&finished);
        handles.push(thread::spawn(move || {
            let seed = inputs
                .config
                .seed
                .wrapping_add(0x9e3779b97f4a7c15_u64.wrapping_mul(index as u64 + 1));
            let mut random = Random(seed.max(1));
            let offset = (random.next() % inputs.config.sample_every as u64) as usize;
            let mut warmup = WorkloadStats::default();
            let warmed = catch_unwind(AssertUnwindSafe(|| {
                run_workload(
                    &inputs,
                    inputs.config.warmup_per_thread,
                    false,
                    &mut random,
                    offset,
                    &mut warmup,
                );
            }));
            ready.wait();
            start.wait();
            let outcome = warmed.and_then(|()| {
                catch_unwind(AssertUnwindSafe(|| {
                    run_workload(
                        &inputs,
                        operations,
                        true,
                        &mut random,
                        offset,
                        &mut measured,
                    );
                }))
            });
            finished.wait();
            outcome
                .map(|()| (warmup, measured))
                .map_err(|_| "benchmark worker panicked")
        }));
    }
    ready.wait();
    let wal_before = inputs.engine.wal_stats();
    let measured_started = Instant::now();
    start.wait();
    finished.wait();
    let elapsed_seconds = measured_started.elapsed().as_secs_f64();
    let wal_after = inputs.engine.wal_stats();
    let mut measured = WorkloadStats::default();
    let mut warmup = WorkloadStats::default();
    for handle in handles {
        let (worker_warmup, worker_measured) =
            handle.join().map_err(|_| "benchmark worker panicked")??;
        warmup.get.merge(worker_warmup.get);
        warmup.put.merge(worker_warmup.put);
        measured.get.merge(worker_measured.get);
        measured.put.merge(worker_measured.put);
    }
    let wal_before = wal_before.map_err(|error| format!("initial WAL stats: {error:?}"))?;
    let wal_after = wal_after.map_err(|error| format!("final WAL stats: {error:?}"))?;
    let durability = durability_fields(
        wal_before,
        wal_after,
        inputs.config.durable_directory.as_deref(),
        inputs.config.durable_read_mode.unwrap_or_default(),
    )?;
    let mut validation_failures = 0;
    let mut validation_first_error = None;
    for key in &inputs.keys {
        match inputs.engine.get(key) {
            Ok(Some(value)) if value.as_ref() == inputs.value.as_slice() => {}
            result => {
                validation_failures += 1;
                if validation_first_error.is_none() {
                    validation_first_error = Some(format!("{result:?}"));
                }
            }
        }
    }
    let after = inputs
        .engine
        .stats()
        .map_err(|error| format!("final stats: {error:?}"))?;
    let failures = measured.get.failures() + measured.put.failures();
    let warmup_failures = warmup.get.failures() + warmup.put.failures();
    let succeeded = measured.get.succeeded + measured.put.succeeded;
    let valid = failures == 0
        && warmup_failures == 0
        && validation_failures == 0
        && measured.get.offered + measured.put.offered == inputs.config.operations
        && before.keys == inputs.config.keys
        && after.keys == inputs.config.keys
        && before.payload_bytes == after.payload_bytes
        && wal_after.is_none_or(|stats| !stats.requires_reopen);
    let config = &inputs.config;
    let mode = if config.durable_directory.is_some() {
        "native_durable_memory_closed_loop"
    } else {
        "native_memory_closed_loop"
    };
    let budget = config
        .max_payload_bytes
        .map_or_else(|| "null".into(), |value| value.to_string());
    let validation_error = validation_first_error
        .as_deref()
        .map_or_else(|| "null".into(), json_string);
    println!(
        "{{\"schema_version\":1,\"status\":\"{}\",\"mode\":{},\"crate_version\":\"{}\",\"shards_requested\":{},\"shards_actual\":{},\"threads\":{},\"keys\":{},\"key_bytes\":8,\"value_bytes\":{},\"operations\":{},\"warmup_per_thread\":{},\"warmup_failures\":{},\"read_percent\":{},\"hot_percent\":{},\"hot_keys\":{},\"seed\":{},\"rng\":\"per_thread_xorshift64\",\"sample_every\":{},\"sampling\":\"systematic total-operation index with seeded per-thread phase\",\"latency_origin\":\"engine call including returned Arc release; successful sampled operations only\",\"throughput_scope\":\"full worker loop including key selection and local counters\",\"elapsed_seconds\":{:.9},\"attempted_ops_per_second\":{:.3},\"successful_ops_per_second\":{:.3},\"succeeded\":{},\"failures\":{},\"preload_seconds\":{:.9},\"payload_bytes_before\":{},\"payload_bytes_after\":{},\"max_payload_bytes\":{},\"harness_key_catalog_bytes\":{},\"harness_sample_capacity_bytes\":{},\"validation_keys\":{},\"validation_failures\":{},\"validation_first_error\":{},\"get\":{},\"put\":{}{}}}",
        if valid { "passed" } else { "failed" },
        json_string(mode),
        env!("CARGO_PKG_VERSION"),
        config.shards,
        after.shards,
        config.threads,
        config.keys,
        config.value_bytes,
        config.operations,
        config.warmup_per_thread,
        warmup_failures,
        config.read_percent,
        config.hot_percent,
        config.hot_keys,
        config.seed,
        config.sample_every,
        elapsed_seconds,
        config.operations as f64 / elapsed_seconds,
        succeeded as f64 / elapsed_seconds,
        succeeded,
        failures,
        preload_seconds,
        before.payload_bytes,
        after.payload_bytes,
        budget,
        inputs.keys.capacity() * std::mem::size_of::<[u8; 8]>(),
        reserved_sample_bytes,
        config.keys,
        validation_failures,
        validation_error,
        operation_json(&mut measured.get),
        operation_json(&mut measured.put),
        durability,
    );
    Ok(valid)
}

fn main() {
    let outcome = parse_config().and_then(|config| config.map_or(Ok(true), run));
    match outcome {
        Ok(true) => {}
        Ok(false) => std::process::exit(1),
        Err(error) => {
            println!(
                "{{\"schema_version\":1,\"status\":\"error\",\"error\":{}}}",
                json_string(&error)
            );
            std::process::exit(2);
        }
    }
}
