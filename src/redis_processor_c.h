#ifndef REDIS_PROCESSOR_C_H
#define REDIS_PROCESSOR_C_H

#include "c.h" // rocksdb/c.h
#ifdef __cplusplus
extern "C" {
#endif

typedef struct redis_processor_handle_result {
  char* err_msg;
  size_t err_len;
  char* resp_cstr;
  size_t resp_len;
  rocksdb_writebatch_t* batch;
} redis_processor_handle_result_t;

redis_processor_handle_result_t
redis_processor_handle(rocksdb_t* db, int64_t table_id, const char* req_cstr, size_t req_len);

void
free_redis_processor_handle_result(redis_processor_handle_result_t* res);

#ifdef __cplusplus
}
#endif

#endif // REDIS_PROCESSOR_C_H