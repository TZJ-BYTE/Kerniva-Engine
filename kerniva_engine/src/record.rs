use std::fs::File;
use std::io;

use crate::batch::WriteOp;
use crate::error::{EngineError, Result};

const RECORD_MAGIC: [u8; 4] = *b"RDG2";
const RECORD_VERSION: u8 = 1;
pub const RECORD_HEADER_LEN: usize = 20;
pub const MAX_KEY_BYTES: usize = u32::MAX as usize;
pub const MAX_VALUE_BYTES: usize = u32::MAX as usize;
// Every operation needs a record header inside the u32-sized batch payload.
pub const MAX_BATCH_OPERATIONS: usize = (MAX_VALUE_BYTES - 4) / RECORD_HEADER_LEN;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum RecordKind {
    Put = 1,
    Delete = 2,
    Batch = 3,
    Clear = 4,
}

impl RecordKind {
    fn from_byte(value: u8) -> Result<Self> {
        match value {
            1 => Ok(Self::Put),
            2 => Ok(Self::Delete),
            3 => Ok(Self::Batch),
            4 => Ok(Self::Clear),
            _ => Err(EngineError::Corruption(format!(
                "unknown record kind: {value}"
            ))),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ValueLocation {
    pub segment_id: u64,
    pub record_offset: u64,
    pub value_offset: u64,
    pub value_len: u32,
}

#[derive(Debug, Clone)]
pub struct Record {
    pub kind: RecordKind,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct ParsedRecord {
    pub kind: RecordKind,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub next_offset: u64,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ReadStatus {
    Complete,
    EndOfFile,
    TruncatedTail,
}

impl Record {
    pub fn put(key: &[u8], value: &[u8]) -> Self {
        Self {
            kind: RecordKind::Put,
            key: key.to_vec(),
            value: value.to_vec(),
        }
    }

    pub fn delete(key: &[u8]) -> Self {
        Self {
            kind: RecordKind::Delete,
            key: key.to_vec(),
            value: Vec::new(),
        }
    }

    pub fn clear() -> Self {
        Self {
            kind: RecordKind::Clear,
            key: Vec::new(),
            value: Vec::new(),
        }
    }

    pub fn batch(operations: &[WriteOp]) -> Result<Self> {
        if operations.len() > MAX_BATCH_OPERATIONS {
            return Err(EngineError::InvalidInput(
                "batch has too many operations".into(),
            ));
        }
        let mut payload_size = 4_usize;
        for operation in operations {
            let (key, value) = match operation {
                WriteOp::Put(key, value) => (key.as_slice(), value.as_slice()),
                WriteOp::Delete(key) => (key.as_slice(), &[][..]),
            };
            validate_lengths(key.len(), value.len())?;
            payload_size = payload_size
                .checked_add(RECORD_HEADER_LEN + key.len() + value.len())
                .ok_or_else(|| EngineError::InvalidInput("batch length overflow".into()))?;
        }
        validate_lengths(0, payload_size)?;
        let mut value = Vec::new();
        value
            .try_reserve_exact(payload_size)
            .map_err(|err| EngineError::InvalidInput(err.to_string()))?;
        value.extend_from_slice(&(operations.len() as u32).to_le_bytes());
        for operation in operations {
            let record = match operation {
                WriteOp::Put(key, value) => Self::put(key, value),
                WriteOp::Delete(key) => Self::delete(key),
            };
            value.extend_from_slice(&record.encode());
        }
        Ok(Self {
            kind: RecordKind::Batch,
            key: Vec::new(),
            value,
        })
    }

    pub fn validate(&self) -> Result<()> {
        validate_lengths(self.key.len(), self.value.len())?;
        if self.kind == RecordKind::Clear && (!self.key.is_empty() || !self.value.is_empty()) {
            return Err(EngineError::InvalidInput(
                "clear record must be empty".into(),
            ));
        }
        Ok(())
    }

    pub fn encoded_len(&self) -> usize {
        RECORD_HEADER_LEN + self.key.len() + self.value.len()
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(self.encoded_len());
        buf.extend_from_slice(&RECORD_MAGIC);
        buf.push(RECORD_VERSION);
        buf.push(self.kind as u8);
        buf.extend_from_slice(&[0_u8; 2]);
        buf.extend_from_slice(&(self.key.len() as u32).to_le_bytes());
        buf.extend_from_slice(&(self.value.len() as u32).to_le_bytes());
        buf.extend_from_slice(&checksum(self.kind, &self.key, &self.value).to_le_bytes());
        buf.extend_from_slice(&self.key);
        buf.extend_from_slice(&self.value);
        buf
    }
}

pub fn validate_lengths(key_len: usize, value_len: usize) -> Result<()> {
    if key_len > MAX_KEY_BYTES || value_len > MAX_VALUE_BYTES {
        return Err(EngineError::InvalidInput(
            "record exceeds key/value size limit".into(),
        ));
    }
    Ok(())
}

/// Explicit offsets keep cloned handles independent of their shared file cursor.
pub fn read_exact_at(file: &File, mut buffer: &mut [u8], mut offset: u64) -> io::Result<()> {
    while !buffer.is_empty() {
        #[cfg(windows)]
        let result = std::os::windows::fs::FileExt::seek_read(file, buffer, offset);
        #[cfg(unix)]
        let result = std::os::unix::fs::FileExt::read_at(file, buffer, offset);
        match result {
            Ok(0) => return Err(io::Error::from(io::ErrorKind::UnexpectedEof)),
            Ok(count) => {
                offset += count as u64;
                buffer = &mut buffer[count..];
            }
            Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
            Err(err) => return Err(err),
        }
    }
    Ok(())
}

pub fn read_record(file: &File, offset: u64) -> Result<(ReadStatus, Option<ParsedRecord>)> {
    let file_len = file.metadata()?.len();
    if offset == file_len {
        return Ok((ReadStatus::EndOfFile, None));
    }
    if offset > file_len {
        return Err(EngineError::Corruption(
            "record offset exceeds segment length".into(),
        ));
    }
    if file_len - offset < RECORD_HEADER_LEN as u64 {
        return Ok((ReadStatus::TruncatedTail, None));
    }
    let mut header = [0_u8; RECORD_HEADER_LEN];
    read_exact_at(file, &mut header, offset)?;
    let (kind, key_len, value_len, expected_checksum) = parse_header(&header, offset)?;
    let record_len = RECORD_HEADER_LEN as u64 + key_len as u64 + value_len as u64;
    if record_len > file_len - offset {
        return Ok((ReadStatus::TruncatedTail, None));
    }
    let mut key = Vec::new();
    key.try_reserve_exact(key_len)
        .map_err(|err| EngineError::Corruption(err.to_string()))?;
    key.resize(key_len, 0);
    read_exact_at(file, &mut key, offset + RECORD_HEADER_LEN as u64)?;
    let mut value = Vec::new();
    value
        .try_reserve_exact(value_len)
        .map_err(|err| EngineError::Corruption(err.to_string()))?;
    value.resize(value_len, 0);
    read_exact_at(
        file,
        &mut value,
        offset + RECORD_HEADER_LEN as u64 + key_len as u64,
    )?;
    if checksum(kind, &key, &value) != expected_checksum {
        return Err(EngineError::Corruption(format!(
            "record checksum mismatch at offset {offset}"
        )));
    }
    Ok((
        ReadStatus::Complete,
        Some(ParsedRecord {
            kind,
            key,
            value,
            next_offset: offset + record_len,
        }),
    ))
}

pub fn parse_batch(payload: &[u8], value_offset: u64) -> Result<Vec<(u64, ParsedRecord)>> {
    if payload.len() < 4 {
        return Err(EngineError::Corruption(
            "batch operation count is missing".into(),
        ));
    }
    let count = u32::from_le_bytes(payload[..4].try_into().unwrap()) as usize;
    if count > MAX_BATCH_OPERATIONS || count > (payload.len() - 4) / RECORD_HEADER_LEN {
        return Err(EngineError::Corruption(
            "invalid batch operation count".into(),
        ));
    }
    let mut entries = Vec::with_capacity(count);
    let mut position = 4;
    for _ in 0..count {
        let end = position + RECORD_HEADER_LEN;
        if end > payload.len() {
            return Err(EngineError::Corruption("incomplete batch header".into()));
        }
        let offset = value_offset + position as u64;
        let header: &[u8; RECORD_HEADER_LEN] = payload[position..end].try_into().unwrap();
        let (kind, key_len, value_len, expected) = parse_header(header, offset)?;
        let next = end
            .checked_add(key_len + value_len)
            .filter(|next| *next <= payload.len())
            .ok_or_else(|| EngineError::Corruption("incomplete batch payload".into()))?;
        if !matches!(kind, RecordKind::Put | RecordKind::Delete) {
            return Err(EngineError::Corruption("invalid batch entry kind".into()));
        }
        let key = payload[end..end + key_len].to_vec();
        let value = payload[end + key_len..next].to_vec();
        if checksum(kind, &key, &value) != expected {
            return Err(EngineError::Corruption(
                "batch entry checksum mismatch".into(),
            ));
        }
        entries.push((
            offset,
            ParsedRecord {
                kind,
                key,
                value,
                next_offset: value_offset + next as u64,
            },
        ));
        position = next;
    }
    if position != payload.len() {
        return Err(EngineError::Corruption(
            "unexpected bytes after batch".into(),
        ));
    }
    Ok(entries)
}

fn parse_header(
    header: &[u8; RECORD_HEADER_LEN],
    offset: u64,
) -> Result<(RecordKind, usize, usize, u32)> {
    if header[..4] != RECORD_MAGIC || header[4] != RECORD_VERSION || header[6..8] != [0, 0] {
        return Err(EngineError::Corruption(format!(
            "invalid record header at offset {offset}"
        )));
    }
    let kind = RecordKind::from_byte(header[5])?;
    let key_len = u32::from_le_bytes(header[8..12].try_into().unwrap()) as usize;
    let value_len = u32::from_le_bytes(header[12..16].try_into().unwrap()) as usize;
    validate_lengths(key_len, value_len).map_err(|err| EngineError::Corruption(err.to_string()))?;
    if (kind == RecordKind::Delete && value_len != 0)
        || (kind == RecordKind::Batch && key_len != 0)
        || (kind == RecordKind::Clear && (key_len != 0 || value_len != 0))
    {
        return Err(EngineError::Corruption(
            "invalid record field lengths".into(),
        ));
    }
    Ok((
        kind,
        key_len,
        value_len,
        u32::from_le_bytes(header[16..20].try_into().unwrap()),
    ))
}

pub(crate) fn checksum_bytes(bytes: &[u8]) -> u32 {
    bytes.iter().fold(2_166_136_261_u32, |hash, byte| {
        hash.wrapping_mul(16_777_619) ^ u32::from(*byte)
    })
}

fn checksum(kind: RecordKind, key: &[u8], value: &[u8]) -> u32 {
    let mut hash = 2_166_136_261_u32.wrapping_mul(16_777_619) ^ (kind as u8 as u32);
    for byte in key.iter().chain(value) {
        hash = hash.wrapping_mul(16_777_619) ^ u32::from(*byte);
    }
    hash
}
