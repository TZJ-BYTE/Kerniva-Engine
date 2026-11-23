#ifndef KERNIVA_MEMORY_H
#define KERNIVA_MEMORY_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct KernivaMemoryEngine KernivaMemoryEngine;

typedef struct {
    const uint8_t *data;
    size_t len;
} KernivaMemorySlice;

typedef struct {
    KernivaMemorySlice key;
    KernivaMemorySlice value;
} KernivaMemoryPair;

typedef struct {
    uint64_t ttl_ms;
    uint32_t condition;
} KernivaMemoryPutOptions;

typedef struct {
    size_t offset;
    size_t len;
    size_t found;
} KernivaMemorySpan;

typedef struct {
    size_t keys;
    size_t payload_bytes;
    size_t shards;
} KernivaMemoryStats;

typedef struct {
    uint64_t log_bytes;
    uint64_t commits;
    uint64_t failed_commits;
    uint64_t sync_attempts;
    uint64_t sync_elapsed_ns;
    uint64_t requires_reopen;
} KernivaMemoryWalStats;

enum {
    KERNIVA_MEMORY_OK = 0,
    KERNIVA_MEMORY_MISSING = 1,
    KERNIVA_MEMORY_BUFFER_TOO_SMALL = 2,
    KERNIVA_MEMORY_PAYLOAD_LIMIT = 3,
    KERNIVA_MEMORY_INVALID_INTEGER = 4,
    KERNIVA_MEMORY_INVALID_EXPIRATION = 5,
    KERNIVA_MEMORY_INVALID_ARGUMENT = -1,
    KERNIVA_MEMORY_INTERNAL = -2,
    KERNIVA_MEMORY_STORAGE_ERROR = -3,
    KERNIVA_MEMORY_DURABILITY_UNKNOWN = -4,
    KERNIVA_MEMORY_REOPEN_REQUIRED = -5,
    KERNIVA_MEMORY_UNSUPPORTED = -6
};

enum {
    KERNIVA_MEMORY_SET_ANY = 0,
    KERNIVA_MEMORY_SET_NX = 1,
    KERNIVA_MEMORY_SET_XX = 2
};

/*
 * All handles must come from a successful open call. Calls can run concurrently
 * on the same handle. Quiesce users before closing the handle once.
 * Caller storage is borrowed only during the call. A zero length permits a
 * null array or byte buffer. Every output must be valid writable storage,
 * disjoint from other outputs and all inputs. Output storage may be uninitialized.
 */
uint32_t kerniva_memory_abi_version(void);

/* limited = 0 for no limit, 1 to apply the resident key/value byte limit. */
int32_t kerniva_memory_open(size_t shards, uint32_t limited, size_t limit,
                            KernivaMemoryEngine **out);
/*
 * Opens the sync-on-write store in a UTF-8 directory. read_during_sync is 0
 * to hold affected shards, 1 to allow reads of the previous committed state.
 * Sync handles support GET, PUT, DELETE, MGET, MSET, stats and WAL stats.
 */
int32_t kerniva_memory_open_sync(const uint8_t *path, size_t path_len,
                                 size_t shards, uint32_t limited, size_t limit,
                                 uint32_t read_during_sync, KernivaMemoryEngine **out);
int32_t kerniva_memory_close(KernivaMemoryEngine *handle);

/*
 * OK writes the value and its byte count. MISSING writes a zero byte count.
 * BUFFER_TOO_SMALL sets needed without touching dst. A retry can observe
 * a newer value, so callers should check its status and required length again.
 */
int32_t kerniva_memory_get_into(KernivaMemoryEngine *handle,
                                const uint8_t *key, size_t key_len,
                                uint8_t *dst, size_t dst_len, size_t *needed);
int32_t kerniva_memory_put(KernivaMemoryEngine *handle,
                           const uint8_t *key, size_t key_len,
                           const uint8_t *value, size_t value_len);
int32_t kerniva_memory_delete(KernivaMemoryEngine *handle,
                              const uint8_t *key, size_t key_len);

/* A zero SET TTL creates a persistent key. MISSING means the condition failed. */
int32_t kerniva_memory_put_options(KernivaMemoryEngine *handle,
                                   const uint8_t *key, size_t key_len,
                                   const uint8_t *value, size_t value_len,
                                   const KernivaMemoryPutOptions *options);

/* EXPIRE with a nonpositive TTL deletes a present key. */
int32_t kerniva_memory_expire(KernivaMemoryEngine *handle,
                              const uint8_t *key, size_t key_len,
                              const int64_t *ttl_ms);
/* The output is -2 for a missing key, -1 for a persistent key. */
int32_t kerniva_memory_pttl(KernivaMemoryEngine *handle,
                            const uint8_t *key, size_t key_len, int64_t *out);
int32_t kerniva_memory_increment(KernivaMemoryEngine *handle,
                                 const uint8_t *key, size_t key_len,
                                 const int64_t *delta, int64_t *out);

/* EXISTS counts repeated keys. DELETE removes the keys together and counts each once. */
int32_t kerniva_memory_count_existing(KernivaMemoryEngine *handle,
                                      const KernivaMemorySlice *keys, size_t count,
                                      size_t *out);
int32_t kerniva_memory_delete_many(KernivaMemoryEngine *handle,
                                   const KernivaMemorySlice *keys, size_t count,
                                   size_t *out);

/* An entire write batch commits or fails together. Duplicate keys are applied in order. */
int32_t kerniva_memory_mset(KernivaMemoryEngine *handle,
                            const KernivaMemoryPair *pairs, size_t count);

/*
 * One span per supplied key in input order. found is zero for missing keys
 * and one for a hit, including empty values. BUFFER_TOO_SMALL only writes
 * needed, leaving dst and spans untouched. Each retry reads a fresh snapshot.
 */
int32_t kerniva_memory_mget_into(KernivaMemoryEngine *handle,
                                 const KernivaMemorySlice *keys, size_t count,
                                 uint8_t *dst, size_t dst_len,
                                 KernivaMemorySpan *spans, size_t *needed);
int32_t kerniva_memory_stats(KernivaMemoryEngine *handle, KernivaMemoryStats *out);
int32_t kerniva_memory_wal_stats(KernivaMemoryEngine *handle, KernivaMemoryWalStats *out);

#ifdef __cplusplus
}
#endif

#endif
