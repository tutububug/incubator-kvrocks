#pragma once

#include <thread>
#include <rocksdb/db.h>
#include <rocksdb/c.h>
#include "store.h"
#include "redis_cmd.h"
#include "redis_request.h"

namespace Redis {

class Processor {
public:
  explicit Processor(Storage* s);

  Processor() = delete;
  Processor(const Processor& p) = delete;
  Processor& operator=(const Processor& p) = delete;

public:
  Status Do(std::string& resp_str, rocksdb::WriteBatch* batch, int64_t table_id, const std::string& req_str);
  rocksdb::Status Expired(bool& filtered, const rocksdb::Slice &key, const rocksdb::Slice &value);
  rocksdb::Status GetExpireTs(int& expire_ts, const rocksdb::Slice& key, const rocksdb::Slice& value);
  Status GetCmdAttr(const std::string& cmd_name, std::unique_ptr<Redis::Commander>& cmd);

private:
  Status lookupAndCreateCommand(const std::string &cmd_name, std::unique_ptr<Redis::Commander> *cmd);
  Status executeCommands(std::string& resp_str, rocksdb::WriteBatch* batch, int64_t table_id, const std::vector<Redis::CommandTokens> &to_process_cmds);
  Status checkCommandArgs(const Redis::CommandTokens& cmd_tokens, const CommandAttributes* attributes);

private:
  Storage* storage_;
};

} // namespace Redis