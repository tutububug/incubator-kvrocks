#include <sstream>
#include "redis_processor.h"
#include "redis_metadata.h"

namespace Redis {

using rocksdb::Slice;
//extern rocksdb::Status extractNamespaceKey(const std::string& ns_key, size_t& off,
//                                           int64_t& table_id, std::string *key,
//                                           bool slot_id_encoded, int64_t& slot_id,
//                                           int64_t& cf_code);

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

rocksdb::Status Processor::getCfCode(const std::string& key, int64_t& cf_code) {
  size_t off = 0;
  int64_t table_id = 0;
  std::string user_key;
  int64_t slot_id = 0;
  return extractNamespaceKey(key, off, table_id, &user_key, storage_->IsSlotIdEncoded(), slot_id, cf_code);
}

bool Processor::isMetaKey(const std::string& key) {
  int64_t cf_code = 0;
  auto s = getCfCode(key, cf_code);
  if (!s.ok()) {
    return false;
  }
  return cf_code == kColumnFamilyIDMetadata;
}

bool Processor::isSubKey(const std::string& key) {
  int64_t cf_code = 0;
  auto s = getCfCode(key, cf_code);
  if (!s.ok()) {
    return false;
  }
  return cf_code == kColumnFamilyIDData ||
  cf_code == kColumnFamilyIDZSetScore;
}

rocksdb::Status Processor::metadataFilter(bool& filtered, const Slice &key, const Slice &value) {
  filtered = false;

  std::string ns, user_key, bytes = value.ToString();
  Metadata metadata(kRedisNone, false);
  rocksdb::Status s = metadata.Decode(bytes);
  if (!s.ok()) {
    return s;
  }
  int64_t table_id = 0;
  s = ExtractNamespaceKey(key, table_id, &user_key, storage_->IsSlotIdEncoded());
  if (!s.ok()) {
    return s;
  }
  filtered = metadata.Expired();
  return rocksdb::Status::OK();
}

rocksdb::Status Processor::subKeyFilter(bool& filtered, const Slice &key, const Slice &value) {
  filtered = false;

  InternalKey ikey;
  auto s = ikey.Init(key, storage_->IsSlotIdEncoded());
  if (!s.ok()) {
    return s;
  }
  std::string metadata_key;
  int64_t cf_code = 0;
  auto db = storage_->GetDB();
  ComposeNamespaceKey(ikey.GetNamespace(), ikey.GetKey(), &metadata_key, storage_->IsSlotIdEncoded(), cf_code);
  std::string meta_value;
  s = db->Get(rocksdb::ReadOptions(), metadata_key, &meta_value);
  if (!s.ok()) {
    return s;
  }
  Metadata metadata(kRedisNone, false);
  s = metadata.Decode(meta_value);
  if (!s.ok()) {
    return s;
  }
  if (metadata.Type() == kRedisString  // metadata key was overwrite by set command
      || metadata.Expired()
      || ikey.GetVersion() != metadata.version) {
    filtered = true;
  }
  return rocksdb::Status::OK();
}

rocksdb::Status Processor::Expired(bool& filtered, const Slice &key, const Slice &value) {
  rocksdb::Status s;
  auto k = key.ToString();
  if (isMetaKey(k)) {
    s = metadataFilter(filtered, key, value);
  } else if (isSubKey(k)) {
    s = subKeyFilter(filtered, key, value);
  } else {
    s = rocksdb::Status::IOError("unknown cfcode");
  }
  return s;
}

} // namespace redis