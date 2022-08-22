#pragma once

#include "rocksdb/db.h"
#include "lock_manager.h"
#include "config.h"

namespace rockdis {

class Storage {
public:
    explicit Storage(rocksdb::DB* db);
    ~Storage();

    Storage() = delete;
    Storage(const Storage& s) = delete;
    Storage& operator=(const Storage& s) = delete;

public:
    Status Open();
    rocksdb::Status Compact(const rocksdb::Slice *begin, const rocksdb::Slice *end);
    LockManager* GetLockManager();
    rocksdb::DB* GetDB();
    bool IsSlotIdEncoded();

public:
    rocksdb::Status Write(const rocksdb::WriteOptions& options, rocksdb::WriteBatch* updates);
    rocksdb::Status Delete(const rocksdb::WriteOptions& options, const rocksdb::Slice& key);
    rocksdb::Status DeleteRange(const rocksdb::Slice& first_key, const rocksdb::Slice& last_key);

private:
    rocksdb::DB* db_;
    LockManager* lock_mgr_;
};

}
