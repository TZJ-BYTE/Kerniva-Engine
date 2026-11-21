use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::path::Path;

use crate::error::{EngineError, Result};
use crate::record::{
    ParsedRecord, RECORD_HEADER_LEN, ReadStatus, RecordKind, ValueLocation, parse_batch,
    read_record,
};

#[derive(Debug, Clone, Copy)]
pub struct ScanSummary {
    pub valid_end: u64,
    pub truncated_tail: bool,
}

pub fn list_segment_ids(segments_dir: &Path) -> Result<Vec<u64>> {
    let mut ids = Vec::new();
    for entry in std::fs::read_dir(segments_dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("seg") {
            continue;
        }
        let Some(stem) = path.file_stem().and_then(|name| name.to_str()) else {
            continue;
        };
        let Ok(id) = stem.parse::<u64>() else {
            continue;
        };
        ids.push(id);
    }
    ids.sort_unstable();
    Ok(ids)
}

pub fn open_active_segment(path: &Path) -> Result<File> {
    Ok(OpenOptions::new()
        .create(true)
        .read(true)
        .append(true)
        .open(path)?)
}

pub fn scan_segment(
    path: &Path,
    segment_id: u64,
    start_offset: u64,
    index: &mut BTreeMap<Vec<u8>, ValueLocation>,
) -> Result<ScanSummary> {
    let file = File::open(path)?;

    let mut offset = start_offset;
    let mut truncated_tail = false;
    loop {
        let (status, parsed) = read_record(&file, offset)?;
        match status {
            ReadStatus::EndOfFile => {
                break;
            }
            ReadStatus::TruncatedTail => {
                truncated_tail = true;
                break;
            }
            ReadStatus::Complete => {
                let record = parsed.expect("complete status must carry a record");
                let next_offset = record.next_offset;
                apply_record(index, segment_id, offset, record)?;
                offset = next_offset;
            }
        }
    }

    Ok(ScanSummary {
        valid_end: offset,
        truncated_tail,
    })
}

pub fn apply_record(
    index: &mut BTreeMap<Vec<u8>, ValueLocation>,
    segment_id: u64,
    offset: u64,
    record: ParsedRecord,
) -> Result<()> {
    match record.kind {
        RecordKind::Put => {
            let value_offset = offset + RECORD_HEADER_LEN as u64 + record.key.len() as u64;
            index.insert(
                record.key,
                ValueLocation {
                    segment_id,
                    record_offset: offset,
                    value_offset,
                    value_len: record.value.len() as u32,
                },
            );
        }
        RecordKind::Delete => {
            index.remove(&record.key);
        }
        RecordKind::Clear => {
            index.clear();
        }
        RecordKind::Batch => {
            let entries = parse_batch(&record.value, offset + RECORD_HEADER_LEN as u64)?;
            for (entry_offset, entry) in entries {
                apply_record(index, segment_id, entry_offset, entry)?;
            }
        }
    }
    Ok(())
}

pub fn truncate_segment(path: &Path, valid_end: u64) -> Result<()> {
    let file = OpenOptions::new().write(true).open(path)?;
    file.set_len(valid_end)?;
    file.sync_all()?;
    Ok(())
}

pub fn read_value(file: &File, location: &ValueLocation, key: &[u8]) -> Result<Vec<u8>> {
    let (status, record) = read_record(file, location.record_offset)?;
    let record = record
        .filter(|record| {
            status == ReadStatus::Complete
                && record.kind == RecordKind::Put
                && record.key == key
                && record.value.len() == location.value_len as usize
                && location.value_offset
                    == location.record_offset + RECORD_HEADER_LEN as u64 + key.len() as u64
        })
        .ok_or_else(|| EngineError::Corruption("index points to an invalid value record".into()))?;
    Ok(record.value)
}
