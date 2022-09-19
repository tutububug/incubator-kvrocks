#include "redis_processor.h"

namespace Redis {

std::once_flag flag;

std::unique_ptr<Processor>& Processor::New(rocksdb::DB* db) {
  std::call_once(flag, [&]() {
    p_.reset(new Processor(new Storage(db)));
  });
  return p_;
}

Status Processor::Do(std::string& resp_str, rocksdb::WriteBatch* batch, int64_t table_id, const std::string& req_str) {
  Request req;
  auto s = req.Tokenize(req_str);
  if (!s.IsOK()) {
    std::ostringstream ss;
    ss << "tokenize the request failed, err=" << s.Msg();
    return Status(Status::NotOK, ss.str());
  }
  s = executeCommands(resp_str, batch, table_id, req.GetCommands());
  if (!s.IsOK()) {
    std::ostringstream ss;
    ss << "execute commands failed: err=" << s.Msg();
    return Status(Status::NotOK, ss.str());
  }
  return Status::OK();
}

Status Processor::executeCommands(std::string& resp_str, rocksdb::WriteBatch* batch, int64_t table_id, const std::vector<Redis::CommandTokens> &to_process_cmds) {
  if (to_process_cmds.empty()) {
    return Status(Status::RedisInvalidCmd, "the 'to process cmds' is empty");
  }

  std::unique_ptr<Redis::Commander> current_cmd = nullptr;
  for (auto &cmd_tokens : to_process_cmds) {
    auto s = lookupAndCreateCommand(cmd_tokens.front(), &current_cmd);
    if (!s.IsOK()) {
      std::ostringstream ss;
      ss << "lookup command failed: err=" << s.Msg();
      return Status(Status::RedisUnknownCmd, ss.str());
    }
    const auto attributes = current_cmd->GetAttributes();
    auto cmd_name = attributes->name;

    current_cmd->SetArgs(cmd_tokens);
    s = current_cmd->Parse(cmd_tokens);
    if (!s.IsOK()) {
      std::ostringstream ss;
      ss << "cmd parse failed: " << s.Msg();
      return Status(Status::RedisParseErr, ss.str());
    }

    s = current_cmd->Execute(table_id, &resp_str, batch, storage_);
    if (!s.IsOK()) {
      std::ostringstream ss;
      ss << "cmd exec failed: err=" << s.Msg();
      return Status(Status::NotOK, ss.str());
    }
  }
  return Status::OK();
}

Status Processor::lookupAndCreateCommand(const std::string &cmd_name, std::unique_ptr<Redis::Commander> *cmd) {
  auto commands = Redis::GetCommands();
  if (cmd_name.empty()) return Status(Status::RedisUnknownCmd);
  auto cmd_iter = commands->find(Util::ToLower(cmd_name));
  if (cmd_iter == commands->end()) {
    return Status(Status::RedisUnknownCmd);
  }
  auto redisCmd = cmd_iter->second;
  *cmd = redisCmd->factory();
  (*cmd)->SetAttributes(redisCmd);
  return Status::OK();
}

} // namespace redis