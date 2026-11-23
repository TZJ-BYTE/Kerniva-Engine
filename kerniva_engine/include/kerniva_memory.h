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
    size_t offset;
    size_t len;
    size_t found;
} KernivaMemorySpan;

typedef struct {
    size_t keys;
    size_t payload_bytes;
    size_t shards;
} KernivaMemoryStats;

enum {
    KERNIVA_MEMORY_OK = 0,
    KERNIVA_MEMORY_MISSING = 1,
    KERNIVA_MEMORY_BUFFER_TOO_SMALL = 2,
    KERNIVA_MEMORY_PAYLOAD_LIMIT = 3,
    KERNIVA_MEMORY_INVALID_ARGUMENT = -1,
    KERNIVA_MEMORY_INTERNAL = -2
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

#ifdef __cplusplus
}
#endif

#endif
