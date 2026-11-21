#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "kerniva_engine.h"

int main(void) {
    KernivaBuf err = {0};

    /* Open engine */
    uintptr_t engine = kerniva_engine_open(
        "./demo_data",
        64 * 1024 * 1024,   /* 64 MB segments */
        10000,               /* checkpoint every 10k ops */
        0,                   /* sync policy: none */
        &err
    );
    if (!engine) {
        fprintf(stderr, "open failed: %.*s\n", (int)err.len, err.ptr);
        kerniva_buf_free(err.ptr, err.len);
        return 1;
    }

    /* Put some data */
    const char *keys[] = {"hello", "foo", "bar"};
    const char *vals[] = {"world", "baz", "qux"};
    for (int i = 0; i < 3; i++) {
        int rc = kerniva_engine_put(
            engine,
            (const uint8_t*)keys[i], strlen(keys[i]),
            (const uint8_t*)vals[i], strlen(vals[i]),
            &err
        );
        if (rc != 0) {
            fprintf(stderr, "put %s failed: %.*s\n", keys[i], (int)err.len, err.ptr);
            kerniva_buf_free(err.ptr, err.len);
        }
    }

    /* Get data */
    KernivaBuf value = {0};
    int rc = kerniva_engine_get(engine, (const uint8_t*)"hello", 5, &value, &err);
    if (rc == 1) {
        printf("get hello = %.*s\n", (int)value.len, value.ptr);
        kerniva_buf_free(value.ptr, value.len);
    } else if (rc == 0) {
        printf("hello: not found\n");
    } else {
        fprintf(stderr, "get failed: %.*s\n", (int)err.len, err.ptr);
        kerniva_buf_free(err.ptr, err.len);
    }

    /* Delete */
    kerniva_engine_delete(engine, (const uint8_t*)"hello", 5, &err);

    /* Stats */
    KernivaStats stats;
    kerniva_engine_stats(engine, &stats, &err);
    printf("keys=%llu segments=%llu\n",
           (unsigned long long)stats.keys,
           (unsigned long long)stats.segment_count);

    /* Checkpoint */
    kerniva_engine_maybe_checkpoint(engine, &err);

    /* Compact */
    uint64_t rewritten = 0;
    kerniva_engine_compact_until_stable(engine, 16, &rewritten, &err);
    printf("compact: %llu entries rewritten\n", (unsigned long long)rewritten);

    /* Close */
    kerniva_engine_close(engine, &err);

    printf("Done.\n");
    return 0;
}
