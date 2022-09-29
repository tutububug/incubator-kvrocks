#include <sstream>
#include "redis_processor.h"

namespace Redis {

std::once_flag once_flag;
Processor::Processor(Storage* s): storage_(s) {
  std::call_once(once_flag, []() {
    InitCommandsTable();
    PopulateCommands();
  });
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
    s = checkCommandArgs(cmd_tokens, current_cmd->GetAttributes());
    if (!s.IsOK()) {
      std::ostringstream ss;
      ss << "check command args failed: err=" << s.Msg();
      return Status(Status::RedisInvalidCmd, ss.str());
    }
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

Status Processor::checkCommandArgs(const Redis::CommandTokens& cmd_tokens, const CommandAttributes* attributes) {
  int arity = attributes->arity;
  int tokens = static_cast<int>(cmd_tokens.size());
  if ((arity > 0 && tokens != arity)
  || (arity < 0 && tokens < -arity)) {
    return Status(Status::RedisInvalidCmd, "ERR wrong number of arguments");
  }
  return Status::OK();
}

} // namespace redis