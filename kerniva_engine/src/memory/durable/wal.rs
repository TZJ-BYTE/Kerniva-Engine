use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::mem::size_of;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use crate::EngineError;
use crate::batch::WriteOp;
use crate::checkpoint::sync_directory;
use crate::record::{
    RECORD_HEADER_LEN, ReadStatus, Record, RecordKind, parse_batch, read_exact_at, read_record,
};
use crate::segment::{open_active_segment, truncate_segment};

use super::super::MemoryValue;
use super::super::batch::PreparedBatch;
use super::{DurableMemoryError, DurableWalStats};

pub(super) const WAL_NAME: &str = "memory-v1.wal";
const HEADER: [u8; 8] = *b"KMWAL001";
const PUT_METADATA_LEN: usize = 16;

pub(super) enum RecoveredEdit {
    Put(Vec<u8>, MemoryValue),
    Delete(Vec<u8>),
}

pub(super) struct Wal {
    path: PathBuf,
    file: File,
    pub(super) version: u64,
    pub(super) stats: DurableWalStats,
    #[cfg(test)]
    pub(super) failure: Option<InjectedFailure>,
    #[cfg(test)]
    pub(super) pause: Option<SyncPause>,
}

#[cfg(test)]
#[derive(Default)]
pub(super) struct InjectedFailure {
    pub(super) partial_write: Option<usize>,
    pub(super) sync: bool,
    pub(super) rollback: bool,
}

#[cfg(test)]
pub(super) struct SyncPause {
    pub(super) entered: std::sync::mpsc::Sender<()>,
    pub(super) release: std::sync::mpsc::Receiver<()>,
}

impl Wal {
    pub(super) fn open(
        directory: &Path,
        mut replay: impl FnMut(RecoveredEdit),
    ) -> Result<Self, DurableMemoryError> {
        let path = directory.join(WAL_NAME);
        let file = match File::open(&path) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                let pending = directory.join("memory-v1.wal.initializing");
                let mut created = OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .open(&pending)?;
                created.write_all(&HEADER)?;
                created.sync_all()?;
                drop(created);
                fs::rename(pending, &path)?;
                File::open(&path)?
            }
            Err(error) => return Err(error.into()),
        };

        let mut header = [0; HEADER.len()];
        match read_exact_at(&file, &mut header, 0) {
            Ok(()) if header == HEADER => {}
            Ok(()) => return Err(corruption("unsupported memory WAL header")),
            Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => {
                return Err(corruption("incomplete memory WAL header"));
            }
            Err(error) => return Err(error.into()),
        }

        let mut offset = HEADER.len() as u64;
        let mut version = 0;
        loop {
            match read_record(&file, offset)? {
                (ReadStatus::EndOfFile, _) => break,
                (ReadStatus::TruncatedTail, _) => {
                    truncate_segment(&path, offset)?;
                    break;
                }
                (ReadStatus::Complete, Some(record)) => {
                    let (next_version, edits) = decode_edits(&record, offset, version)?;
                    for edit in edits {
                        replay(edit);
                    }
                    offset = record.next_offset;
                    version = next_version;
                }
                (ReadStatus::Complete, None) => return Err(corruption("missing complete frame")),
            }
        }
        drop(file);
        // A complete frame from an unacknowledged write can be accepted during
        // replay. Sync it before making the recovered view available.
        let file = open_active_segment(&path)?;
        file.sync_all()?;
        sync_directory(directory)?;
        // The caller may have just created the directory and its parents. On
        // Unix, each new directory entry must also reach its parent's disk
        // state before we can acknowledge writes to the WAL.
        #[cfg(unix)]
        for parent in directory.canonicalize()?.ancestors().skip(1) {
            sync_directory(parent)?;
        }

        Ok(Self {
            file,
            path,
            version,
            stats: DurableWalStats {
                log_bytes: offset,
                ..DurableWalStats::default()
            },
            #[cfg(test)]
            failure: None,
            #[cfg(test)]
            pause: None,
        })
    }

    pub(super) fn append(
        &mut self,
        encoded: &[u8],
        version: u64,
    ) -> Result<(), DurableMemoryError> {
        let next_offset = self
            .stats
            .log_bytes
            .checked_add(encoded.len() as u64)
            .ok_or_else(|| EngineError::InvalidInput("memory WAL offset overflow".into()))?;
        #[cfg(test)]
        let failure = self.failure.take();
        #[cfg(test)]
        let io_result = match failure.as_ref().and_then(|failure| failure.partial_write) {
            Some(length) => self
                .file
                .write_all(&encoded[..length.min(encoded.len())])
                .and(Err(io::Error::other("injected partial WAL write"))),
            None => self.file.write_all(encoded),
        };
        #[cfg(not(test))]
        let io_result = self.file.write_all(encoded);
        let result = io_result.and_then(|()| {
            let entered = Instant::now();
            self.stats.sync_attempts = self.stats.sync_attempts.saturating_add(1);
            #[cfg(test)]
            let result = self.test_sync(failure.as_ref().is_some_and(|failure| failure.sync));
            #[cfg(not(test))]
            let result = self.file.sync_all();
            self.stats.sync_elapsed_ns = self
                .stats
                .sync_elapsed_ns
                .saturating_add(entered.elapsed().as_nanos());
            result
        });
        if let Err(write) = result {
            self.stats.failed_commits = self.stats.failed_commits.saturating_add(1);
            #[cfg(test)]
            let rollback = if failure.is_some_and(|failure| failure.rollback) {
                Err(EngineError::Io(io::Error::other(
                    "injected WAL rollback failure",
                )))
            } else {
                truncate_segment(&self.path, self.stats.log_bytes)
            };
            #[cfg(not(test))]
            let rollback = truncate_segment(&self.path, self.stats.log_bytes);
            if let Err(rollback) = rollback {
                self.stats.requires_reopen = true;
                return Err(DurableMemoryError::UncertainCommit { write, rollback });
            }
            return Err(write.into());
        }

        self.stats.log_bytes = next_offset;
        self.stats.commits = self.stats.commits.saturating_add(1);
        self.version = version;
        Ok(())
    }

    #[cfg(test)]
    fn test_sync(&mut self, fail: bool) -> io::Result<()> {
        if let Some(pause) = self.pause.take() {
            pause.entered.send(()).map_err(io::Error::other)?;
            pause.release.recv().map_err(io::Error::other)?;
        }
        if fail {
            return Err(io::Error::other("injected WAL sync failure"));
        }
        self.file.sync_all()
    }
}

pub(super) fn encode(
    prepared: &PreparedBatch<'_, '_>,
    version: u64,
) -> Result<Vec<u8>, DurableMemoryError> {
    let mut changes = Vec::new();
    for shard in &prepared.shards {
        changes
            .try_reserve(shard.changes.len())
            .map_err(|_| super::super::MemoryError::CapacityOverflow)?;
        for (key, pending) in &shard.changes {
            let edit = if let Some(entry) = pending {
                let mut body = Vec::new();
                let len = entry
                    .bytes
                    .len()
                    .checked_add(PUT_METADATA_LEN)
                    .ok_or(super::super::MemoryError::CapacityOverflow)?;
                crate::record::validate_lengths(key.len(), len)?;
                body.try_reserve_exact(len)
                    .map_err(|_| super::super::MemoryError::CapacityOverflow)?;
                body.extend_from_slice(&entry.version.to_le_bytes());
                body.extend_from_slice(&entry.expires_at_ms.unwrap_or_default().to_le_bytes());
                body.extend_from_slice(entry.bytes);
                WriteOp::Put(key.to_vec(), body)
            } else {
                WriteOp::Delete(key.to_vec())
            };
            changes.push(edit);
        }
    }

    let batch = Record::batch(&changes)?;
    let mut value = Vec::new();
    let len = batch
        .value
        .len()
        .checked_add(size_of::<u64>())
        .ok_or(super::super::MemoryError::CapacityOverflow)?;
    crate::record::validate_lengths(0, len)?;
    value
        .try_reserve_exact(len)
        .map_err(|_| super::super::MemoryError::CapacityOverflow)?;
    value.extend_from_slice(&version.to_le_bytes());
    value.extend_from_slice(&batch.value);
    let frame = Record {
        kind: RecordKind::Put,
        key: Vec::new(),
        value,
    };
    Ok(frame.encode())
}

fn decode_edits(
    record: &crate::record::ParsedRecord,
    offset: u64,
    previous_version: u64,
) -> Result<(u64, Vec<RecoveredEdit>), DurableMemoryError> {
    if record.kind != RecordKind::Put || !record.key.is_empty() || record.value.len() < 8 {
        return Err(corruption("invalid memory WAL commit frame"));
    }
    let version = u64::from_le_bytes(record.value[..8].try_into().unwrap());
    if version < previous_version {
        return Err(corruption("memory WAL version moved backwards"));
    }
    let entries = parse_batch(&record.value[8..], offset + RECORD_HEADER_LEN as u64 + 8)?;
    let mut edits = Vec::with_capacity(entries.len());
    for (_, entry) in entries {
        let edit = match entry.kind {
            RecordKind::Put if entry.value.len() >= PUT_METADATA_LEN => {
                let token = u64::from_le_bytes(entry.value[..8].try_into().unwrap());
                if token <= previous_version || token > version {
                    return Err(corruption("memory WAL value version outside commit range"));
                }
                let deadline =
                    u64::from_le_bytes(entry.value[8..PUT_METADATA_LEN].try_into().unwrap());
                RecoveredEdit::Put(
                    entry.key,
                    MemoryValue {
                        value: Arc::from(&entry.value[PUT_METADATA_LEN..]),
                        version: token,
                        expires_at_ms: (deadline != 0).then_some(deadline),
                    },
                )
            }
            RecordKind::Delete => RecoveredEdit::Delete(entry.key),
            _ => return Err(corruption("invalid memory WAL operation")),
        };
        edits.push(edit);
    }
    Ok((version, edits))
}

fn corruption(message: &str) -> DurableMemoryError {
    EngineError::Corruption(message.into()).into()
}
