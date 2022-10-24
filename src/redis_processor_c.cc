#include "redis_processor_c.h"
#include "redis_processor.h"
#include "status.h"
#include "redis_metadata.h"

struct redis_processor { Redis::Processor* p; };
struct rocksdb_t { rocksdb::DB* rep; };

// new_redis_processor
// the input pointer type of 'void* db' is required as 'rocksdb_t*' which defined in 'rocksdb/c.h'
redis_processor_t* new_redis_processor(void* db) {
  auto p = new redis_processor();
  p->p = new Redis::Processor(new Redis::Storage(reinterpret_cast<struct rocksdb_t*>(db)->rep));
  return p;
}

void free_redis_processor(redis_processor* p) {
  if (p != nullptr) {
    delete p->p;
    delete p;
  }
}

void copy_string_to_char_array(char** out, size_t* out_len, const std::string& in);

redis_processor_handle_result_t
redis_processor_handle(redis_processor_t* p, int64_t table_id, const char* req_cstr, size_t req_len) {
  redis_processor_handle_result_t ret;
  memset(&ret, 0, sizeof(redis_processor_handle_result_t));

  std::string req_str(req_cstr, req_len);
  std::string resp_str;
  rocksdb::WriteBatch batch;
  auto s = p->p->Do(resp_str, &batch, table_id, req_str);
  if (!s.IsOK()) {
    copy_string_to_char_array(&ret.err_msg, &ret.err_len, s.Msg());
    return ret;
  }
  copy_string_to_char_array(&ret.resp_cstr, &ret.resp_len, resp_str);
  ret.batch = rocksdb_writebatch_create_from(batch.Data().data(), batch.GetDataSize());
  return ret;
}

void free_redis_processor_handle_result(redis_processor_handle_result_t* res) {
  if (res->err_msg) free(res->err_msg);
  if (res->resp_cstr) free(res->resp_cstr);
  if (res->batch) {
    rocksdb_writebatch_clear(res->batch);
    rocksdb_writebatch_destroy(res->batch);
  }
}

void copy_string_to_char_array(char** out, size_t* out_len, const std::string& in) {
  char* o = (char*)calloc(1, in.size());
  memcpy(o, in.data(), in.size());
  *out = o;
  *out_len = in.size();
}

size_t get_redis_key_prefix_length(const char* key_cstr, size_t key_len) {
  auto key = std::string(key_cstr, key_len);
  size_t off = 0;
  CalculateNamespaceKeyPrefixLength(key, off);
  return off;
}

redis_key_is_expired_result_t
redis_key_is_expired(redis_processor_t* p,
                     const char* key_cstr, size_t key_len,
                     const char* val_cstr, size_t val_len) {
  bool expired = false;
  auto s = p->p->Expired(expired,
                         rocksdb::Slice(key_cstr, key_len),
                         rocksdb::Slice(val_cstr, val_len));

  redis_key_is_expired_result_t res;
  if (!s.ok()) {
    copy_string_to_char_array(&res.err_msg, &res.err_len, s.ToString());
  }
  res.expired = expired;
  return res;
}

void free_redis_key_is_expired_result(redis_key_is_expired_result_t res) {
  if (res.err_msg) free(res.err_msg);
}

redis_get_expire_ts_result_t
redis_get_expire_ts(redis_processor_t* p,
                    const char* key_cstr, size_t key_len,
                    const char* val_cstr, size_t val_len) {
  int expire_ts = 0;
  auto s = p->p->GetExpireTs(expire_ts,
                             rocksdb::Slice(key_cstr, key_len),
                             rocksdb::Slice(val_cstr, val_len));

  redis_get_expire_ts_result_t res;
  if (!s.ok()) {
    copy_string_to_char_array(&res.err_msg, &res.err_len, s.ToString());
  }
  res.expire_ts = expire_ts;
  return res;
}

void
free_redis_get_expire_ts_result(redis_get_expire_ts_result_t res) {
  if (res.err_msg) free(res.err_msg);
}