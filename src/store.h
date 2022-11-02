#pragma once

#include "rocksdb/db.h"
#include "rocksdb/compaction_filter.h"
#include "lock_manager.h"
#include "config.h"

namespace Redis{

class Storage {
public:
    explicit Storage();
    ~Storage();

    Storage(const Storage& s) = delete;
    Storage& operator=(const Storage& s) = delete;

public:
    Status Open(rocksdb::DB* db);
    rocksdb::Status Compact(const rocksdb::Slice *begin, const rocksdb::Slice *end);
    LockManager* GetLockManager();
    rocksdb::DB* GetDB();
    bool IsSlotIdEncoded();

public:
    rocksdb::Status Write(const rocksdb::WriteOptions& options, rocksdb::WriteBatch* updates);
    rocksdb::Status Expired(bool& filtered, const rocksdb::Slice &key, const rocksdb::Slice &value);

    rocksdb::Status getCfCode(const std::string& key, int64_t& cf_code);
    bool isMetaKey(const std::string& key);
    bool isSubKey(const std::string& key);
    rocksdb::Status metadataFilter(bool& filtered, const rocksdb::Slice &key, const rocksdb::Slice &value);
    rocksdb::Status subKeyFilter(bool& filtered, const rocksdb::Slice &key, const rocksdb::Slice &value);

public:
  struct ExpireCache {
    std::string key = {};
    std::string data ={};
  };
  static thread_local ExpireCache expire_cache;

private:
    rocksdb::DB* db_;
    LockManager* lock_mgr_;
};

class ExpireFilter: public rocksdb::CompactionFilter {
public:
  ExpireFilter(Storage* s): s_(s) {}

  bool Filter(int level, const rocksdb::Slice &key, const rocksdb::Slice &value,
              std::string *new_value, bool *modified) const override {
    bool filtered = false;
    s_->Expired(filtered, key, value);
    return filtered;
  }

  const char *Name() const override { return "redis_expire_filter"; }

private:
  Storage* s_;
};

class ExpireFilterFactory: public rocksdb::CompactionFilterFactory {
public:
  ExpireFilterFactory(Storage* s): s_(s) {}

  std::unique_ptr<rocksdb::CompactionFilter>
  CreateCompactionFilter(const rocksdb::CompactionFilter::Context& context) {
    return std::unique_ptr<rocksdb::CompactionFilter>(new ExpireFilter(s_));
  };

  const char* Name() const { return "redis_expire_compaction_filter_factory"; }

private:
  Storage* s_;
};

class ExpireCollector: public rocksdb::TablePropertiesCollector {
public:
  rocksdb::Status Finish(rocksdb::UserCollectedProperties* properties) {
    // TODO to impl
    return rocksdb::Status::OK();
  }

  rocksdb::UserCollectedProperties GetReadableProperties() const {
    // TODO to impl
    return rocksdb::UserCollectedProperties();
  }

  const char* Name() const { return "redis_expire_collector"; }
};

class ExpireCollectorFactory: public rocksdb::TablePropertiesCollectorFactory {
  rocksdb::TablePropertiesCollector*
  CreateTablePropertiesCollector(rocksdb::TablePropertiesCollectorFactory::Context context) {
    return new ExpireCollector;
  }

  const char* Name() const { return "redis_expire_properties_collector_factory"; }
};

}
