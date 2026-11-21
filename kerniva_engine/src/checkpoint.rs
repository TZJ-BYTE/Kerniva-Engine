use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufWriter, Cursor, Read, Write};
use std::path::Path;

use crate::error::{EngineError, Result};
use crate::record::{MAX_KEY_BYTES, MAX_VALUE_BYTES, ValueLocation, checksum_bytes};

const CHECKPOINT_MAGIC: [u8; 8] = *b"RDG2CKPT";
const CHECKPOINT_VERSION: u32 = 2;
const MAX_CHECKPOINT_BYTES: u64 = isize::MAX as u64;

#[derive(Debug, Clone)]
pub struct CheckpointState {
    pub active_segment_id: u64,
    pub active_offset: u64,
    pub index: BTreeMap<Vec<u8>, ValueLocation>,
}

pub fn load(path: &Path) -> Result<Option<CheckpointState>> {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err.into()),
    };
    let length = file.metadata()?.len();
    if !(36..=MAX_CHECKPOINT_BYTES).contains(&length) {
        return Err(EngineError::Corruption("invalid checkpoint length".into()));
    }
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(length as usize)
        .map_err(|err| EngineError::Corruption(err.to_string()))?;
    file.take(length + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 != length || bytes[..8] != CHECKPOINT_MAGIC {
        return Err(EngineError::Corruption("invalid checkpoint header".into()));
    }
    let version = u32::from_le_bytes(bytes[8..12].try_into().unwrap());
    match version {
        1 => {}
        CHECKPOINT_VERSION => {
            let checksum_offset = bytes.len() - 4;
            let expected = u32::from_le_bytes(bytes[checksum_offset..].try_into().unwrap());
            if checksum_bytes(&bytes[..checksum_offset]) != expected {
                return Err(EngineError::Corruption(
                    "checkpoint checksum mismatch".into(),
                ));
            }
            bytes.truncate(checksum_offset);
        }
        _ => {
            return Err(EngineError::Corruption(format!(
                "unsupported checkpoint version {version}"
            )));
        }
    }
    let mut cursor = Cursor::new(&bytes[12..]);
    let active_segment_id = read_u64(&mut cursor)?;
    let active_offset = read_u64(&mut cursor)?;
    let entry_count = read_u64(&mut cursor)?;
    if active_segment_id == 0 || entry_count > (bytes.len() as u64 - 36) / 32 {
        return Err(EngineError::Corruption(
            "invalid checkpoint entry count or segment".into(),
        ));
    }
    let mut index = BTreeMap::new();
    for _ in 0..entry_count {
        let key_len = read_u32(&mut cursor)? as usize;
        let remaining = cursor
            .get_ref()
            .len()
            .saturating_sub(cursor.position() as usize);
        if key_len > MAX_KEY_BYTES || remaining < key_len + 28 {
            return Err(EngineError::Corruption(
                "invalid checkpoint key length".into(),
            ));
        }
        let mut key = vec![0_u8; key_len];
        cursor.read_exact(&mut key)?;
        let location = ValueLocation {
            segment_id: read_u64(&mut cursor)?,
            record_offset: read_u64(&mut cursor)?,
            value_offset: read_u64(&mut cursor)?,
            value_len: read_u32(&mut cursor)?,
        };
        if location.segment_id == 0
            || location.value_len as usize > MAX_VALUE_BYTES
            || location.record_offset.checked_add(20 + key_len as u64)
                != Some(location.value_offset)
            || index.insert(key, location).is_some()
        {
            return Err(EngineError::Corruption(
                "invalid checkpoint index entry".into(),
            ));
        }
    }
    if cursor.position() != cursor.get_ref().len() as u64 {
        return Err(EngineError::Corruption(
            "unexpected bytes after checkpoint".into(),
        ));
    }
    Ok(Some(CheckpointState {
        active_segment_id,
        active_offset,
        index,
    }))
}

pub fn write_index(
    path: &Path,
    active_segment_id: u64,
    active_offset: u64,
    index: &BTreeMap<Vec<u8>, ValueLocation>,
) -> Result<()> {
    let tmp_path = path.with_extension("tmp");
    let file = File::create(&tmp_path)?;
    let mut writer = CheckpointWriter {
        file: BufWriter::new(file),
        checksum: 2_166_136_261,
    };
    writer.write(&CHECKPOINT_MAGIC)?;
    writer.write(&CHECKPOINT_VERSION.to_le_bytes())?;
    writer.write(&active_segment_id.to_le_bytes())?;
    writer.write(&active_offset.to_le_bytes())?;
    writer.write(&(index.len() as u64).to_le_bytes())?;
    for (key, location) in index {
        writer.write(&(key.len() as u32).to_le_bytes())?;
        writer.write(key)?;
        writer.write(&location.segment_id.to_le_bytes())?;
        writer.write(&location.record_offset.to_le_bytes())?;
        writer.write(&location.value_offset.to_le_bytes())?;
        writer.write(&location.value_len.to_le_bytes())?;
    }
    writer.file.write_all(&writer.checksum.to_le_bytes())?;
    writer.file.flush()?;
    writer.file.get_ref().sync_all()?;
    drop(writer);
    atomic_replace(&tmp_path, path)?;
    sync_directory(path.parent().expect("checkpoint has a parent"))?;
    Ok(())
}

struct CheckpointWriter {
    file: BufWriter<File>,
    checksum: u32,
}

impl CheckpointWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        self.file.write_all(bytes)?;
        for byte in bytes {
            self.checksum = self.checksum.wrapping_mul(16_777_619) ^ u32::from(*byte);
        }
        Ok(())
    }
}

#[cfg(windows)]
fn atomic_replace(source: &Path, destination: &Path) -> std::io::Result<()> {
    use std::os::windows::ffi::OsStrExt;
    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn MoveFileExW(source: *const u16, destination: *const u16, flags: u32) -> i32;
    }
    let source: Vec<u16> = source.as_os_str().encode_wide().chain(Some(0)).collect();
    let destination: Vec<u16> = destination
        .as_os_str()
        .encode_wide()
        .chain(Some(0))
        .collect();
    // Replace atomically and ask Windows to complete the move before returning.
    if unsafe { MoveFileExW(source.as_ptr(), destination.as_ptr(), 0x1 | 0x8) } == 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(not(windows))]
fn atomic_replace(source: &Path, destination: &Path) -> std::io::Result<()> {
    std::fs::rename(source, destination)
}

pub(crate) fn sync_directory(path: &Path) -> std::io::Result<()> {
    #[cfg(unix)]
    File::open(path)?.sync_all()?;
    #[cfg(windows)]
    let _ = path;
    Ok(())
}

fn read_u32(reader: &mut impl Read) -> Result<u32> {
    let mut buf = [0_u8; 4];
    reader.read_exact(&mut buf)?;
    Ok(u32::from_le_bytes(buf))
}

fn read_u64(reader: &mut impl Read) -> Result<u64> {
    let mut buf = [0_u8; 8];
    reader.read_exact(&mut buf)?;
    Ok(u64::from_le_bytes(buf))
}
