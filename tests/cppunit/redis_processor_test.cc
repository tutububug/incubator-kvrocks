#include <gtest/gtest.h>
#include <redis_processor_c.h>

#include "redis_processor.h"
#include "test_base.h"

class RedisProcessorTest : public TestBase {
protected:
  explicit RedisProcessorTest() : TestBase() {}
  ~RedisProcessorTest() = default;
  void SetUp() override {
  }
  void TearDown() override {
  }
};

TEST_F(RedisProcessorTest, String_Base) {
  Redis::Processor p(storage_);

  int64_t table_id = 1;
  rocksdb::WriteBatch batch;
  {
    std::string req_str = "*3\r\n$3\r\nset\r\n$1\r\na\r\n$1\r\n1\r\n*3\r\n$3\r\nset\r\n$1\r\nb\r\n$1\r\n2\r\n";
    std::string resp_str;
    auto s = p.Do(resp_str, &batch, table_id, req_str);
    assert(s.IsOK());
    assert(resp_str == "+OK\r\n");
  }
  {
    std::string req_str = "*2\r\n$3\r\nget\r\n$1\r\na\r\n*2\r\n$3\r\nget\r\n$1\r\nb\r\n";
    std::string resp_str;
    auto s = p.Do(resp_str, &batch, table_id, req_str);
    assert(s.IsOK());
    assert(resp_str == "$1\r\n2\r\n");
  }
  {
    std::string req_str = "*2\r\n$3\r\ndel\r\n$1\r\nb\r\n";
    std::string resp_str;
    auto s = p.Do(resp_str, &batch, table_id, req_str);
    assert(s.IsOK());
    assert(resp_str == ":1\r\n");
  }
}

TEST_F(RedisProcessorTest, C) {
  int64_t table_id = 1;
  const char* req_str = "*3\r\n$3\r\nset\r\n$1\r\na\r\n$4\r\n1234\r\n";
  size_t req_len = strlen(req_str);

  auto res = redis_processor_handle(reinterpret_cast<rocksdb_t*>(storage_->GetDB()), table_id, req_str, req_len);
  assert(res.err_msg == NULL);
  assert(res.err_len == 0);
  assert(strcmp(res.resp_cstr, "+OK\r\n") == 0);
  assert(strlen(res.resp_cstr) == strlen("+OK\r\n"));
  free_redis_processor_handle_result(&res);
}
