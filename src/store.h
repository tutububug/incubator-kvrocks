#pragma once

#include "rocksdb/db.h"
#include "lock_manager.h"
#include "config.h"

namespace Redis{

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
    rocksdb::Status Write(const rocksdb::WriteOptions& options, rocksdb::WriteBatch* updates, bool skip_write_db);

private:
    rocksdb::DB* db_;
    LockManager* lock_mgr_;
};

}
