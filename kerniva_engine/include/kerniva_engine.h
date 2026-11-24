#ifndef KERNIVA_ENGINE_H
#define KERNIVA_ENGINE_H
#ifdef __cplusplus
extern "C" {
#endif
#include <stddef.h>
#include <stdint.h>
typedef struct { uint8_t* ptr; uintptr_t len; } KernivaBuf;
typedef struct {
    uint32_t kind; /* 1 = put, 2 = delete */
    const uint8_t* key_ptr;
    uintptr_t key_len;
    const uint8_t* value_ptr;
    uintptr_t value_len;
} KernivaWriteOp;
typedef enum { KERNIVA_SYNC_NONE=0, KERNIVA_SYNC_FLUSH=1, KERNIVA_SYNC_SYNC_DATA=2 } KernivaSyncPolicy;
typedef struct { uint64_t keys, active_segment_id, segment_count, cached_readers, pinned_segments, pending_reclaims; } KernivaStats;
void kerniva_buf_free(uint8_t* ptr, uintptr_t len);
void kerniva_cstring_free(char* ptr);
uintptr_t kerniva_engine_open(const char* data_dir, uint64_t seg_size, uint64_t ckpt_after, uint32_t sync_policy, KernivaBuf* err);
int32_t kerniva_engine_close(uintptr_t engine, KernivaBuf* err);
int32_t kerniva_engine_put(uintptr_t engine, const uint8_t* key, uintptr_t key_len, const uint8_t* value, uintptr_t value_len, KernivaBuf* err);
int32_t kerniva_engine_delete(uintptr_t engine, const uint8_t* key, uintptr_t key_len, KernivaBuf* err);
int32_t kerniva_engine_clear(uintptr_t engine, KernivaBuf* err);
int32_t kerniva_engine_write_batch(uintptr_t engine, const KernivaWriteOp* operations, uintptr_t operations_len, KernivaBuf* err);
int32_t kerniva_engine_sync(uintptr_t engine, KernivaBuf* err);
int32_t kerniva_engine_checkpoint(uintptr_t engine, KernivaBuf* err);
int32_t kerniva_engine_get(uintptr_t engine, const uint8_t* key, uintptr_t key_len, KernivaBuf* value_out, KernivaBuf* err);
uintptr_t kerniva_engine_iter_open(uintptr_t engine, KernivaBuf* err);
int32_t kerniva_engine_iter_next(uintptr_t iter, KernivaBuf* key_out, KernivaBuf* value_out, KernivaBuf* err);
void kerniva_engine_iter_close(uintptr_t iter);
int32_t kerniva_engine_compact_until_stable(uintptr_t engine, uintptr_t max_rounds, uint64_t* rewritten, KernivaBuf* err);
int32_t kerniva_engine_maybe_compact(uintptr_t engine, uint64_t* rewritten, KernivaBuf* err);
int32_t kerniva_engine_maybe_checkpoint(uintptr_t engine, KernivaBuf* err);
int32_t kerniva_engine_should_checkpoint(uintptr_t engine);
int32_t kerniva_engine_stats(uintptr_t engine, KernivaStats* out, KernivaBuf* err);
uintptr_t kerniva_golog_recover(const char* data_dir, uint64_t* last_sequence, uint64_t* active_end, KernivaBuf* err);
int32_t kerniva_golog_recovery_next(uintptr_t handle, KernivaBuf* key, KernivaBuf* value, KernivaBuf* err);
void kerniva_golog_recovery_close(uintptr_t handle);
typedef struct { uint64_t first, last; } KernivaGoLogRange;
/* Validates 1 to 128 contiguous sealed ranges after the published sequence.
   reset is 1 if the current view must be cleared before applying the changes. */
uintptr_t kerniva_golog_delta_open(const char* data_dir, uint64_t after, const KernivaGoLogRange* ranges, size_t range_count, uint8_t* reset, KernivaBuf* err);
/* Returns 1 for a block, 0 at the end, or -1 on error. Free each block with
   kerniva_buf_free. Records are kind (1 put, 2 delete), two little-endian
   uint32_t key and value lengths, then the key and value bytes. */
int32_t kerniva_golog_delta_next(uintptr_t handle, KernivaBuf* block, KernivaBuf* err);
void kerniva_golog_delta_close(uintptr_t handle);
const char* kerniva_engine_version(void);
#ifdef __cplusplus
}
#endif
#endif
