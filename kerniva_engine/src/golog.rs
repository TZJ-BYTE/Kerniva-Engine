mod delta;
mod format;

use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

use crate::checkpoint::{atomic_replace, sync_directory};
use crate::directory_lock::{DirectoryLock, acquire_named};
use crate::error::{EngineError, Result};
use format::{State, corrupt, next_sequence, read_snapshot, replay_log, write_snapshot};

pub(crate) use delta::{Delta, GoLogRange, MAX_DELTA_RANGES};

const CURRENT: &str = "CURRENT";
const ACTIVE: &str = "active.log";
const RECOVERY_LOCK: &str = ".recovery.lock";
const MAINTENANCE_LOCK: &str = ".maintenance.lock";
const SNAPSHOT_MAINTENANCE: &str = ".snapshot-maintenance";
const MAX_RETAINED_SEGMENTS: u64 = 16;
const MAX_RETAINED_BYTES: u64 = 64 << 20;

pub(crate) struct Recovery {
    pub state: State,
    pub last_sequence: u64,
    pub active_end: u64,
}

pub(crate) fn recover(directory: &Path) -> Result<Recovery> {
    let _guard = lock_recovery(directory)?;
    let cutoff = load_cutoff(directory)?;
    let mut state = load_state(directory, cutoff)?;
    let segments = sealed_segments(directory, None)?;
    let last = replay_sealed(&segments, cutoff, &mut state)?;
    let active = replay_log(&directory.join(ACTIVE), last, cutoff, true, &mut state)?;
    Ok(Recovery {
        state,
        last_sequence: active.sequence,
        active_end: active.offset,
    })
}

#[derive(Debug, Clone, Copy)]
pub struct MaintenanceOptions {
    pub min_sealed_bytes: u64,
    pub max_age: Duration,
}

impl Default for MaintenanceOptions {
    fn default() -> Self {
        Self {
            min_sealed_bytes: 16 << 20,
            max_age: Duration::from_secs(300),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct MaintenanceOutcome {
    pub cutoff: u64,
    pub snapshot: bool,
    pub reclaimed_segments: usize,
    pub reclaimed_bytes: u64,
}

pub struct Maintainer {
    root: PathBuf,
    options: MaintenanceOptions,
    retain_covered_segments: usize,
    _lease: std::sync::Arc<DirectoryLock>,
}

impl Maintainer {
    pub fn open(root: PathBuf, options: MaintenanceOptions) -> Result<Self> {
        Self::open_with_retention(root, options, 0)
    }

    /// Retain up to 16 contiguous covered segments, limited to 64 MiB per database.
    pub fn open_with_retention(
        root: PathBuf,
        options: MaintenanceOptions,
        retain_covered_segments: u64,
    ) -> Result<Self> {
        if retain_covered_segments > MAX_RETAINED_SEGMENTS {
            return Err(EngineError::InvalidInput(format!(
                "--retain-covered-segments must be between 0 and {MAX_RETAINED_SEGMENTS}"
            )));
        }
        if !fs::metadata(&root)?.is_dir() {
            return Err(EngineError::InvalidInput(
                "Go-log root must be a directory".into(),
            ));
        }
        let lease = acquire_named(&root, MAINTENANCE_LOCK)?;
        Ok(Self {
            root,
            options,
            retain_covered_segments: retain_covered_segments as usize,
            _lease: lease,
        })
    }

    pub fn run_pass(
        &self,
        force: bool,
    ) -> Result<Vec<(PathBuf, Result<Option<MaintenanceOutcome>>)>> {
        // Go updates this byte under the same file lock before serving with new
        // settings. Keep the lock until all publications and removals are done.
        let policy = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(self.root.join(SNAPSHOT_MAINTENANCE))?;
        policy.lock()?;
        let mut value = Vec::new();
        (&policy).take(2).read_to_end(&mut value)?;
        match value.as_slice() {
            b"" | b"1" => {}
            b"0" => return Ok(Vec::new()),
            _ => return Err(corrupt("invalid snapshot maintenance policy")),
        }
        let mut directories = Vec::new();
        for entry in fs::read_dir(&self.root)? {
            let entry = entry?;
            let name = entry.file_name();
            if entry.file_type()?.is_dir()
                && name
                    .to_str()
                    .and_then(|name| name.strip_prefix("db_"))
                    .is_some_and(|number| number.parse::<usize>().is_ok())
            {
                directories.push(entry.path());
            }
        }
        directories.sort_unstable();
        Ok(directories
            .into_iter()
            .map(|directory| {
                let outcome = maintain_database(
                    &directory,
                    self.options,
                    self.retain_covered_segments,
                    force,
                );
                (directory, outcome)
            })
            .collect())
    }
}

#[derive(Debug)]
struct Sealed {
    path: PathBuf,
    first: u64,
    last: u64,
    bytes: u64,
    age: Duration,
}

fn sealed_segments(
    directory: &Path,
    mut snapshots: Option<&mut Vec<(u64, PathBuf)>>,
) -> Result<Vec<Sealed>> {
    let mut segments = Vec::new();
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(range) = name
            .to_str()
            .and_then(|name| name.strip_prefix("log-"))
            .and_then(|name| name.strip_suffix(".seg"))
        else {
            if let Some(snapshots) = snapshots.as_deref_mut()
                && let Some(sequence) = name
                    .to_str()
                    .and_then(|name| name.strip_prefix("snapshot-"))
                    .and_then(|name| name.strip_suffix(".bin"))
                    .and_then(|text| parse_sequence_name(text).ok())
                && entry.file_type()?.is_file()
            {
                snapshots.push((sequence, entry.path()));
            }
            continue;
        };
        let (first, last) = range
            .split_once('-')
            .ok_or_else(|| corrupt("invalid sealed log name"))?;
        let first = parse_sequence_name(first)?;
        let last = parse_sequence_name(last)?;
        if first > last || !entry.file_type()?.is_file() {
            return Err(corrupt("invalid sealed log range or file type"));
        }
        let metadata = entry.metadata()?;
        let age = metadata
            .modified()
            .ok()
            .and_then(|time| time.elapsed().ok())
            .unwrap_or_default();
        segments.push(Sealed {
            path: entry.path(),
            first,
            last,
            bytes: metadata.len(),
            age,
        });
    }
    segments.sort_unstable_by_key(|segment| (segment.first, segment.last));
    Ok(segments)
}

fn parse_sequence_name(text: &str) -> Result<u64> {
    if text.len() != 20 || !text.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(corrupt("invalid sequence in filename"));
    }
    text.parse()
        .ok()
        .filter(|&number| number > 0)
        .ok_or_else(|| corrupt("invalid sequence in filename"))
}

fn replay_sealed(segments: &[Sealed], cutoff: u64, state: &mut State) -> Result<u64> {
    let mut sequence = cutoff;
    for segment in segments.iter().filter(|segment| segment.last > cutoff) {
        let expected = next_sequence(sequence)?;
        // The first unpruned segment can overlap the snapshot cutoff.
        if segment.first > expected || (segment.first < expected && sequence != cutoff) {
            return Err(corrupt(format!(
                "expected sealed sequence {expected}, found range {}..{}",
                segment.first, segment.last
            )));
        }
        let end = replay_log(&segment.path, segment.first - 1, cutoff, false, state)?;
        if end.sequence != segment.last {
            return Err(corrupt(format!(
                "sealed log {} ends at {}, expected {}",
                segment.path.display(),
                end.sequence,
                segment.last
            )));
        }
        sequence = end.sequence;
    }
    Ok(sequence)
}

fn load_cutoff(directory: &Path) -> Result<u64> {
    let file = match File::open(directory.join(CURRENT)) {
        Ok(file) => file,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(err) => return Err(err.into()),
    };
    let mut text = String::new();
    file.take(22).read_to_string(&mut text)?;
    if text.len() > 21 {
        return Err(corrupt("invalid snapshot cutoff length in CURRENT"));
    }
    let text = text.strip_suffix('\n').unwrap_or(&text);
    if text.is_empty() || text.starts_with('0') || !text.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(corrupt("invalid snapshot cutoff in CURRENT"));
    }
    text.parse()
        .ok()
        .filter(|&number| number > 0)
        .ok_or_else(|| corrupt("invalid snapshot cutoff in CURRENT"))
}

fn snapshot_path(directory: &Path, sequence: u64) -> PathBuf {
    directory.join(format!("snapshot-{sequence:020}.bin"))
}

fn load_state(directory: &Path, cutoff: u64) -> Result<State> {
    if cutoff == 0 {
        Ok(State::new())
    } else {
        read_snapshot(&snapshot_path(directory, cutoff), cutoff)
    }
}

fn lock_recovery(directory: &Path) -> Result<File> {
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(directory.join(RECOVERY_LOCK))?;
    lock.lock()?;
    Ok(lock)
}

fn maintain_database(
    directory: &Path,
    options: MaintenanceOptions,
    retain_covered_segments: usize,
    force: bool,
) -> Result<Option<MaintenanceOutcome>> {
    let cutoff = load_cutoff(directory)?;
    let mut snapshots = Vec::new();
    let segments = sealed_segments(directory, Some(&mut snapshots))?;
    let mut new_bytes = 0_u64;
    let mut oldest = Duration::ZERO;
    for pending in segments.iter().filter(|segment| segment.last > cutoff) {
        new_bytes = new_bytes.saturating_add(pending.bytes);
        oldest = oldest.max(pending.age);
    }
    let publish = new_bytes != 0
        && (force || new_bytes >= options.min_sealed_bytes || oldest >= options.max_age);
    let mut reclaimable = Vec::new();
    if !publish {
        reclaimable = reclaimable_segments(&segments, cutoff, retain_covered_segments);
        if reclaimable.is_empty() && !snapshots.iter().any(|(sequence, _)| *sequence < cutoff) {
            return Ok(None);
        }
    }

    let mut state = load_state(directory, cutoff)?;
    let next_cutoff = if publish {
        let next_cutoff = replay_sealed(&segments, cutoff, &mut state)?;
        let path = snapshot_path(directory, next_cutoff);
        let temp = directory.join(".snapshot.tmp");
        write_snapshot(&temp, next_cutoff, &state)?;
        atomic_replace(&temp, &path)?;
        sync_directory(directory)?;
        #[cfg(test)]
        tests::maintenance_barrier("snapshot-synced");
        next_cutoff
    } else {
        cutoff
    };
    drop(state);

    let _guard = lock_recovery(directory)?;
    if publish {
        publish_cutoff(directory, next_cutoff)?;
        #[cfg(test)]
        tests::maintenance_barrier("current-synced");
        reclaimable = reclaimable_segments(&segments, next_cutoff, retain_covered_segments);
    } else {
        // A prior pass may have exposed CURRENT before its directory sync
        // completed. Make this generation durable before resuming cleanup.
        sync_directory(directory)?;
    }
    let mut result = MaintenanceOutcome {
        cutoff: next_cutoff,
        snapshot: publish,
        reclaimed_segments: 0,
        reclaimed_bytes: 0,
    };
    for segment in reclaimable {
        fs::remove_file(&segment.path)?;
        result.reclaimed_segments += 1;
        result.reclaimed_bytes = result.reclaimed_bytes.saturating_add(segment.bytes);
        #[cfg(test)]
        if result.reclaimed_segments == 1 {
            tests::maintenance_barrier("first-reclaim");
        }
    }
    for (sequence, path) in snapshots {
        if sequence < next_cutoff {
            fs::remove_file(path)?;
        }
    }
    sync_directory(directory)?;
    Ok(Some(result))
}

fn publish_cutoff(directory: &Path, cutoff: u64) -> Result<()> {
    let temp = directory.join(".CURRENT.tmp");
    let mut file = File::create(&temp)?;
    writeln!(file, "{cutoff}")?;
    file.sync_all()?;
    drop(file);
    atomic_replace(&temp, &directory.join(CURRENT))?;
    sync_directory(directory)?;
    Ok(())
}

fn reclaimable_segments(segments: &[Sealed], cutoff: u64, retain: usize) -> Vec<&Sealed> {
    let mut covered: Vec<_> = segments
        .iter()
        .filter(|segment| segment.last <= cutoff)
        .collect();
    // Only a continuous suffix ending at the published cutoff can help a follower.
    let mut expected_last = cutoff;
    let mut budget = MAX_RETAINED_BYTES;
    let mut kept = 0;
    for segment in covered.iter().rev().take(retain) {
        if segment.last != expected_last
            || segment.bytes <= format::LOG_MAGIC.len() as u64
            || segment.bytes > budget
        {
            break;
        }
        expected_last = segment.first - 1;
        budget -= segment.bytes;
        kept += 1;
    }
    covered.truncate(covered.len() - kept);
    covered
}

#[cfg(test)]
mod tests;
