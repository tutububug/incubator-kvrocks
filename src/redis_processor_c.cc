#include "redis_processor_c.h"
#include "redis_processor.h"
#include "status.h"

void copy_string_to_char_array(char** out, size_t* out_len, const std::string& in);

redis_processor_handle_result_t
redis_processor_handle(rocksdb_t* db, int64_t table_id, const char* req_cstr, size_t req_len) {
  redis_processor_handle_result_t ret;
  memset(&ret, 0, sizeof(redis_processor_handle_result_t));

  std::string req_str(req_cstr, req_len);
  std::string resp_str;
  auto batch = new rocksdb::WriteBatch();
  auto& p = Redis::Processor::New(reinterpret_cast<rocksdb::DB*>(db));
  auto s = p->Do(resp_str, batch, table_id, req_str);
  if (!s.IsOK()) {
    copy_string_to_char_array(&ret.err_msg, &ret.err_len, s.Msg());
    return ret;
  }
  copy_string_to_char_array(&ret.resp_cstr, &ret.resp_len, resp_str);
  ret.batch = rocksdb_writebatch_create_from(batch->Data().data(), batch->GetDataSize());
  return ret;
}

void free_redis_processor_handle_result(redis_processor_handle_result_t* res) {
  if (res->err_msg) free(res->err_msg);
  if (res->resp_cstr) free(res->resp_cstr);
  if (res->batch) free(res->batch);
}

void copy_string_to_char_array(char** out, size_t* out_len, const std::string& in) {
  char* o = (char*)calloc(1, in.size());
  memcpy(o, in.data(), in.size());
  *out = o;
  *out_len = in.size();
}