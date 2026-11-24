use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufReader, BufWriter, Cursor, Read, Write};
use std::path::Path;

use crate::error::{EngineError, Result};

pub(super) const LOG_MAGIC: &[u8; 8] = b"KGLLOG01";
const SNAPSHOT_MAGIC: &[u8; 8] = b"KGLSNP01";
const FRAME_HEADER_LEN: u64 = 8;
const MAX_PAYLOAD: usize = 256 << 20;
const CRC_TABLE: [u32; 256] = crc_table();

pub(crate) type State = BTreeMap<Vec<u8>, Vec<u8>>;

pub(crate) struct LogEnd {
    pub sequence: u64,
    pub offset: u64,
}

pub(super) enum LogRecord<'a> {
    Put(&'a [u8], &'a [u8]),
    Delete(&'a [u8]),
    Reset,
}

pub(crate) fn replay_log(
    path: &Path,
    previous: u64,
    cutoff: u64,
    allow_tail: bool,
    state: &mut State,
) -> Result<LogEnd> {
    replay_log_records(path, previous, cutoff, allow_tail, |record| match record {
        LogRecord::Put(key, value) => {
            state.insert(key.to_vec(), value.to_vec());
        }
        LogRecord::Delete(key) => {
            state.remove(key);
        }
        LogRecord::Reset => state.clear(),
    })
}

pub(super) fn replay_log_records(
    path: &Path,
    previous: u64,
    cutoff: u64,
    allow_tail: bool,
    mut apply: impl FnMut(LogRecord<'_>),
) -> Result<LogEnd> {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(err) if allow_tail && err.kind() == std::io::ErrorKind::NotFound => {
            return Ok(LogEnd {
                sequence: previous,
                offset: 0,
            });
        }
        Err(err) => return Err(err.into()),
    };
    let len = file.metadata()?.len();
    let mut reader = BufReader::new(file);
    if len < 8 {
        let mut prefix = [0; 8];
        reader.read_exact(&mut prefix[..len as usize])?;
        if allow_tail && prefix[..len as usize] == LOG_MAGIC[..len as usize] {
            return Ok(LogEnd {
                sequence: previous,
                offset: 0,
            });
        }
        return Err(corrupt("invalid partial log header"));
    }
    if &read_array::<8>(&mut reader)? != LOG_MAGIC {
        return Err(corrupt("invalid log magic"));
    }

    let mut offset = 8;
    let mut sequence = previous;
    let mut payload = Vec::new();
    while offset < len {
        let remaining = len - offset;
        if remaining < FRAME_HEADER_LEN {
            if allow_tail {
                break;
            }
            return Err(corrupt("truncated sealed frame header"));
        }
        let payload_len = read_u32(&mut reader)? as usize;
        let expected_crc = read_u32(&mut reader)?;
        if !(21..=MAX_PAYLOAD).contains(&payload_len) {
            return Err(corrupt("invalid log frame size"));
        }
        let frame_len = FRAME_HEADER_LEN + payload_len as u64;
        if remaining < frame_len {
            if allow_tail {
                break;
            }
            return Err(corrupt("truncated sealed frame payload"));
        }
        read_payload(&mut reader, &mut payload, payload_len, expected_crc)?;
        let expected_sequence = next_sequence(sequence)?;
        apply_batch(&payload, expected_sequence, cutoff, &mut apply)?;
        sequence = expected_sequence;
        offset += frame_len;
    }
    Ok(LogEnd { sequence, offset })
}

fn apply_batch(
    payload: &[u8],
    expected_sequence: u64,
    cutoff: u64,
    apply: &mut impl FnMut(LogRecord<'_>),
) -> Result<()> {
    let mut cursor = Cursor::new(payload);
    let sequence = read_u64(&mut cursor)?;
    if sequence != expected_sequence {
        return Err(corrupt(format!(
            "expected commit sequence {expected_sequence}, found {sequence}"
        )));
    }
    let count = read_u32(&mut cursor)? as usize;
    if count == 0 || count > (payload.len() - 12) / 9 {
        return Err(corrupt("invalid log batch count"));
    }
    for _ in 0..count {
        let kind = read_array::<1>(&mut cursor)?[0];
        let (key, value) = read_entry(&mut cursor)?;
        let record = match kind {
            1 => LogRecord::Put(key, value),
            2 if value.is_empty() => LogRecord::Delete(key),
            3 if count == 1 && key.is_empty() && value.is_empty() => LogRecord::Reset,
            _ => return Err(corrupt("invalid log record")),
        };
        if sequence > cutoff {
            apply(record);
        }
    }
    if cursor.position() != payload.len() as u64 {
        return Err(corrupt("trailing bytes in log batch"));
    }
    Ok(())
}

pub(crate) fn next_sequence(sequence: u64) -> Result<u64> {
    sequence
        .checked_add(1)
        .ok_or_else(|| corrupt("commit sequence exhausted"))
}

pub(crate) fn read_snapshot(path: &Path, cutoff: u64) -> Result<State> {
    let file = File::open(path)?;
    let len = file.metadata()?.len();
    let mut reader = BufReader::new(file);
    let header = read_array::<24>(&mut reader)?;
    if &header[..8] != SNAPSHOT_MAGIC || read_u32(&mut reader)? != crc32(&header) {
        return Err(corrupt("snapshot header checksum or magic mismatch"));
    }
    let mut fields = Cursor::new(&header[8..]);
    let sequence = read_u64(&mut fields)?;
    let count = read_u64(&mut fields)?;
    let mut remaining = len - 28;
    if sequence != cutoff || count > remaining / 16 {
        return Err(corrupt("invalid snapshot sequence or entry count"));
    }
    let mut state = State::new();
    let mut payload = Vec::new();
    for _ in 0..count {
        if remaining < FRAME_HEADER_LEN {
            return Err(corrupt("truncated snapshot frame header"));
        }
        let payload_len = read_u32(&mut reader)? as usize;
        let expected_crc = read_u32(&mut reader)?;
        let frame_len = FRAME_HEADER_LEN + payload_len as u64;
        if !(8..=MAX_PAYLOAD).contains(&payload_len) || remaining < frame_len {
            return Err(corrupt("invalid snapshot frame size"));
        }
        read_payload(&mut reader, &mut payload, payload_len, expected_crc)?;
        let mut cursor = Cursor::new(payload.as_slice());
        let (key, value) = read_entry(&mut cursor)?;
        if cursor.position() != payload_len as u64
            || state.insert(key.to_vec(), value.to_vec()).is_some()
        {
            return Err(corrupt("invalid or duplicate snapshot entry"));
        }
        remaining -= frame_len;
    }
    if remaining != 0 {
        return Err(corrupt("trailing bytes in snapshot"));
    }
    Ok(state)
}

pub(crate) fn write_snapshot(path: &Path, cutoff: u64, state: &State) -> Result<()> {
    let mut writer = BufWriter::new(File::create(path)?);
    let mut header = [0_u8; 24];
    header[..8].copy_from_slice(SNAPSHOT_MAGIC);
    header[8..16].copy_from_slice(&cutoff.to_le_bytes());
    header[16..24].copy_from_slice(&(state.len() as u64).to_le_bytes());
    writer.write_all(&header)?;
    writer.write_all(&crc32(&header).to_le_bytes())?;
    for (key, value) in state {
        let key_len = (key.len() as u32).to_le_bytes();
        let value_len = (value.len() as u32).to_le_bytes();
        let len = 8 + key.len() + value.len();
        let crc = crc_update(!0, &key_len);
        let crc = crc_update(crc, &value_len);
        let crc = crc_update(crc, key);
        let crc = !crc_update(crc, value);
        writer.write_all(&(len as u32).to_le_bytes())?;
        writer.write_all(&crc.to_le_bytes())?;
        writer.write_all(&key_len)?;
        writer.write_all(&value_len)?;
        writer.write_all(key)?;
        writer.write_all(value)?;
    }
    writer.flush()?;
    writer.get_ref().sync_all()?;
    Ok(())
}

fn read_payload(
    reader: &mut impl Read,
    payload: &mut Vec<u8>,
    len: usize,
    expected_crc: u32,
) -> Result<()> {
    payload.clear();
    payload
        .try_reserve_exact(len)
        .map_err(|err| corrupt(err.to_string()))?;
    payload.resize(len, 0);
    reader.read_exact(payload)?;
    if crc32(payload) != expected_crc {
        return Err(corrupt("frame checksum mismatch"));
    }
    Ok(())
}

fn read_entry<'a>(cursor: &mut Cursor<&'a [u8]>) -> Result<(&'a [u8], &'a [u8])> {
    let key_len = read_u32(cursor)? as usize;
    let value_len = read_u32(cursor)? as usize;
    let start = cursor.position() as usize;
    let middle = start
        .checked_add(key_len)
        .ok_or_else(|| corrupt("entry key length overflow"))?;
    let end = middle
        .checked_add(value_len)
        .ok_or_else(|| corrupt("entry value length overflow"))?;
    let bytes = *cursor.get_ref();
    if end > bytes.len() {
        return Err(corrupt("truncated entry"));
    }
    cursor.set_position(end as u64);
    Ok((&bytes[start..middle], &bytes[middle..end]))
}

fn read_array<const N: usize>(reader: &mut impl Read) -> Result<[u8; N]> {
    let mut bytes = [0; N];
    reader.read_exact(&mut bytes)?;
    Ok(bytes)
}

fn read_u32(reader: &mut impl Read) -> Result<u32> {
    Ok(u32::from_le_bytes(read_array(reader)?))
}

fn read_u64(reader: &mut impl Read) -> Result<u64> {
    Ok(u64::from_le_bytes(read_array(reader)?))
}

pub(crate) fn corrupt(message: impl Into<String>) -> EngineError {
    EngineError::Corruption(message.into())
}

pub(crate) fn crc32(bytes: &[u8]) -> u32 {
    !crc_update(!0, bytes)
}

fn crc_update(mut crc: u32, bytes: &[u8]) -> u32 {
    for &byte in bytes {
        crc = (crc >> 8) ^ CRC_TABLE[((crc as u8) ^ byte) as usize];
    }
    crc
}

const fn crc_table() -> [u32; 256] {
    let mut table = [0; 256];
    let mut index = 0;
    while index < table.len() {
        let mut crc = index as u32;
        let mut bit = 0;
        while bit < 8 {
            crc = if crc & 1 == 0 {
                crc >> 1
            } else {
                (crc >> 1) ^ 0xedb8_8320
            };
            bit += 1;
        }
        table[index] = crc;
        index += 1;
    }
    table
}
