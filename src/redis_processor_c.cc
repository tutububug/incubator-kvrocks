#include "redis_processor_c.h"
#include "redis_processor.h"
#include "status.h"

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
    ret.err_msg = std::move(const_cast<char*>(s.Msg().data()));
    ret.err_len = s.Msg().size();
    return ret;
  }
  ret.resp_cstr = std::move(const_cast<char*>(resp_str.data()));
  ret.resp_len = resp_str.size();
  ret.batch = rocksdb_writebatch_create_from(batch->Data().data(), batch->GetDataSize());
  return ret;
}
