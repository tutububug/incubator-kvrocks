#include "store.h"
#include "status.h"

namespace Redis {

Storage::Storage(rocksdb::DB* db):
    db_(db) {
  lock_mgr_ = new LockManager(16);
}

Storage::~Storage() {
  db_->Close();
  delete lock_mgr_;
}

Status Storage::Open() {
  return Status::OK();
}

rocksdb::Status Storage::Compact(const rocksdb::Slice *begin, const rocksdb::Slice *end) {
//    rocksdb::CompactRangeOptions compact_opts;
//    compact_opts.change_level = true;
//    for (const auto &cf_handle : cf_handles_) {
//        rocksdb::Status s = db_->CompactRange(compact_opts, cf_handle, begin, end);
//        if (!s.ok()) return s;
//    }
  return rocksdb::Status::OK();
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

rocksdb::Status Storage::Write(const rocksdb::WriteOptions& options, rocksdb::WriteBatch* updates, bool skip_write_db) {
  if (skip_write_db) {
    return rocksdb::Status::OK();
  } else {
    return db_->Write(options, updates);
  }
}

}
