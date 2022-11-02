#include "store.h"
#include "status.h"
#include "redis_metadata.h"

namespace Redis {

thread_local Storage::ExpireCache Storage::expire_cache;

Storage::Storage() {
  lock_mgr_ = new LockManager(16);
}

Storage::~Storage() {
  delete lock_mgr_;
}

Status Storage::Open(rocksdb::DB* db) {
  db_ = db;
  return Status::OK();
}

rocksdb::Status Storage::Compact(const rocksdb::Slice *begin, const rocksdb::Slice *end) {
  rocksdb::CompactRangeOptions compact_opts;
  compact_opts.change_level = true;
  return db_->CompactRange(compact_opts, begin, end);
}

LockManager* Storage::GetLockManager() {
  return lock_mgr_;
}

rocksdb::DB* Storage::GetDB() {
  return db_;
}

bool Storage::IsSlotIdEncoded() {
  return false;
}

rocksdb::Status Storage::Write(const rocksdb::WriteOptions& options, rocksdb::WriteBatch* updates) {
#ifdef REDIS_STORAGE_DO_WRITE
  return db_->Write(options, updates);
#else
  return rocksdb::Status::OK();
#endif
}

rocksdb::Status Storage::Expired(bool& filtered, const rocksdb::Slice &key, const rocksdb::Slice &value) {
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

rocksdb::Status Storage::getCfCode(const std::string& key, int64_t& cf_code) {
  size_t off = 0;
  int64_t table_id = 0;
  std::string user_key;
  int64_t slot_id = 0;
  return extractNamespaceKey(key, off, table_id, &user_key, IsSlotIdEncoded(), slot_id, cf_code);
}

bool Storage::isMetaKey(const std::string& key) {
  int64_t cf_code = 0;
  auto s = getCfCode(key, cf_code);
  if (!s.ok()) {
    return false;
  }
  return cf_code == kColumnFamilyIDMetadata;
}

bool Storage::isSubKey(const std::string& key) {
  int64_t cf_code = 0;
  auto s = getCfCode(key, cf_code);
  if (!s.ok()) {
    return false;
  }
  return cf_code == kColumnFamilyIDData ||
  cf_code == kColumnFamilyIDZSetScore;
}

rocksdb::Status Storage::metadataFilter(bool& filtered, const Slice &key, const Slice &value) {
  filtered = false;

  std::string ns, user_key, bytes = value.ToString();
  Metadata metadata(kRedisNone, false);
  rocksdb::Status s = metadata.Decode(bytes);
  if (!s.ok()) {
    return s;
  }
  int64_t table_id = 0;
  s = ExtractNamespaceKey(key, table_id, &user_key, IsSlotIdEncoded());
  if (!s.ok()) {
    return s;
  }
  filtered = metadata.Expired();
  Storage::expire_cache.key = key.ToString();
  Storage::expire_cache.data = bytes;
  return rocksdb::Status::OK();
}

rocksdb::Status Storage::subKeyFilter(bool& filtered, const Slice &key, const Slice &value) {
  filtered = false;

  InternalKey ikey;
  auto s = ikey.Init(key, IsSlotIdEncoded());
  if (!s.ok()) {
    return s;
  }
  std::string metadata_key;
  auto db = GetDB();
  ComposeNamespaceKey(ikey.GetNamespace(), ikey.GetKey(), &metadata_key, IsSlotIdEncoded(), kColumnFamilyIDMetadata);

  auto cached_key = &Storage::expire_cache.key;
  auto cached_metadata = &Storage::expire_cache.data;
  if (cached_key->empty() || metadata_key != *cached_key) {
    std::string meta_value;
    s = db->Get(rocksdb::ReadOptions(), metadata_key, &meta_value);
    *cached_key = std::move(metadata_key);
    if (s.ok()) {
      *cached_metadata = std::move(meta_value);
    } else if (s.IsNotFound()) {
      cached_metadata->clear();
    } else {
      cached_key->clear();
      cached_metadata->clear();
    }
  }
  // the metadata was not found
  if (cached_metadata->empty()) {
    filtered = true;
    return rocksdb::Status::OK();
  }

  Metadata metadata(kRedisNone, false);
  s = metadata.Decode(*cached_metadata);
  if (!s.ok()) {
    return s;
  }
  if (metadata.Type() == kRedisString  // metadata key was overwrite by set command
  || ikey.GetVersion() != metadata.version) {
    filtered = true;
  }
  return rocksdb::Status::OK();
}

}
