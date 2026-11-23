use std::mem::MaybeUninit;

use crate::{
    DurableMemoryEngine, DurableMemoryError, MemoryBatchOp, MemoryEngine, MemoryRead, MemoryStats,
    MemoryValue,
};

use super::{ApiResult, DURABILITY_UNKNOWN, REOPEN_REQUIRED, STORAGE_ERROR, UNSUPPORTED, status};

pub enum KernivaMemoryEngine {
    Volatile(MemoryEngine),
    Durable(DurableMemoryEngine),
}

impl KernivaMemoryEngine {
    pub(super) fn volatile(&self) -> ApiResult<&MemoryEngine> {
        match self {
            Self::Volatile(engine) => Ok(engine),
            Self::Durable(_) => Err(UNSUPPORTED),
        }
    }

    pub(super) fn get_into_uninit(
        &self,
        key: &[u8],
        dst: &mut [MaybeUninit<u8>],
    ) -> ApiResult<MemoryRead> {
        match self {
            Self::Volatile(engine) => engine.get_into_uninit(key, dst).map_err(status),
            Self::Durable(engine) => engine.get_into_uninit(key, dst).map_err(durable_status),
        }
    }

    pub(super) fn put(&self, key: &[u8], value: &[u8]) -> ApiResult<()> {
        match self {
            Self::Volatile(engine) => engine.put(key, value).map_err(status),
            Self::Durable(engine) => engine.put(key, value).map_err(durable_status),
        }
    }

    pub(super) fn delete(&self, key: &[u8]) -> ApiResult<bool> {
        match self {
            Self::Volatile(engine) => engine.delete(key).map_err(status),
            Self::Durable(engine) => engine.delete(key).map_err(durable_status),
        }
    }

    pub(super) fn apply_batch(&self, operations: &[MemoryBatchOp<'_>]) -> ApiResult<bool> {
        match self {
            Self::Volatile(engine) => engine.apply_batch_at(operations, 0).map_err(status),
            Self::Durable(engine) => engine.apply_batch_at(operations, 0).map_err(durable_status),
        }
    }

    pub(super) fn get_many(&self, keys: &[&[u8]]) -> ApiResult<Vec<Option<MemoryValue>>> {
        match self {
            Self::Volatile(engine) => engine.get_many(keys).map_err(status),
            Self::Durable(engine) => engine.get_many(keys).map_err(durable_status),
        }
    }

    pub(super) fn stats(&self) -> ApiResult<MemoryStats> {
        match self {
            Self::Volatile(engine) => engine.stats().map_err(status),
            Self::Durable(engine) => engine.stats().map_err(durable_status),
        }
    }
}

pub(super) fn durable_status(error: DurableMemoryError) -> i32 {
    match error {
        DurableMemoryError::Memory(error) => status(error),
        DurableMemoryError::Storage(_) => STORAGE_ERROR,
        DurableMemoryError::UncertainCommit { .. } => DURABILITY_UNKNOWN,
        DurableMemoryError::ReopenRequired => REOPEN_REQUIRED,
    }
}
