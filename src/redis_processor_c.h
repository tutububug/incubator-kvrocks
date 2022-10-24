#ifndef REDIS_PROCESSOR_C_H
#define REDIS_PROCESSOR_C_H

#include "rocksdb/c.h"
#ifdef __cplusplus
extern "C" {
#endif

typedef struct redis_processor redis_processor_t;

redis_processor_t* new_redis_processor(void* db);

void free_redis_processor(redis_processor_t* p);

typedef struct redis_processor_handle_result {
  char* err_msg;
  size_t err_len;
  char* resp_cstr;
  size_t resp_len;
  rocksdb_writebatch_t* batch;
} redis_processor_handle_result_t;

redis_processor_handle_result_t
redis_processor_handle(redis_processor_t* p, int64_t table_id, const char* req_cstr, size_t req_len);

void
free_redis_processor_handle_result(redis_processor_handle_result_t* res);

size_t
get_redis_key_prefix_length(const char* key_cstr, size_t key_len);

typedef struct {
  char* err_msg;
  size_t err_len;
  int expired;
} redis_key_is_expired_result_t;

redis_key_is_expired_result_t
redis_key_is_expired(redis_processor_t* p,
                     const char* key_cstr, size_t key_len,
                     const char* val_cstr, size_t val_len);

void
free_redis_key_is_expired_result(redis_key_is_expired_result_t res);

typedef struct {
  char* err_msg;
  size_t err_len;
  int expire_ts;
} redis_get_expire_ts_result_t;

redis_get_expire_ts_result_t
redis_get_expire_ts(redis_processor_t* p,
                    const char* key_cstr, size_t key_len,
                    const char* val_cstr, size_t val_len);

void
free_redis_get_expire_ts_result(redis_get_expire_ts_result_t res);

#ifdef __cplusplus
}
#endif

#endif // REDIS_PROCESSOR_C_H