use std::fs::{File, OpenOptions, TryLockError};
use std::path::Path;
use std::sync::Arc;

use crate::error::{EngineError, Result};

#[derive(Debug)]
pub(crate) struct DirectoryLock {
    file: File,
}

impl Drop for DirectoryLock {
    fn drop(&mut self) {
        // Duplicated or inherited descriptors can outlive the last engine or snapshot owner.
        let _ = self.file.unlock();
    }
}

pub(crate) fn acquire(data_dir: &Path) -> Result<Arc<DirectoryLock>> {
    acquire_named(data_dir, "LOCK")
}

pub(crate) fn acquire_named(data_dir: &Path, name: &str) -> Result<Arc<DirectoryLock>> {
    let path = data_dir.join(name);
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path)?;
    match file.try_lock() {
        Ok(()) => Ok(Arc::new(DirectoryLock { file })),
        Err(TryLockError::WouldBlock) => Err(EngineError::Locked(data_dir.display().to_string())),
        Err(TryLockError::Error(err)) => Err(EngineError::Io(err)),
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::acquire;
    use crate::error::EngineError;

    #[test]
    fn releases_after_last_owner_with_duplicate_descriptor() {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let data_dir = std::env::temp_dir().join(format!(
            "kerniva_directory_lock_duplicate_{}_{nonce}",
            std::process::id()
        ));
        fs::create_dir(&data_dir).unwrap();

        let lock = acquire(&data_dir).unwrap();
        let duplicate = lock.file.try_clone().unwrap();
        let lease = Arc::clone(&lock);
        drop(lock);
        assert!(matches!(acquire(&data_dir), Err(EngineError::Locked(_))));
        drop(lease);

        let reopened = acquire(&data_dir);
        let error = reopened.as_ref().err().map(ToString::to_string);
        drop(duplicate);
        drop(reopened);
        fs::remove_dir_all(&data_dir).unwrap();

        assert!(
            error.is_none(),
            "directory lock outlived its owners: {error:?}"
        );
    }
}
