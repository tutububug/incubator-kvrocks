#include "store.h"
#include "status.h"

namespace rockdis {

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

rocksdb::ColumnFamilyHandle* Storage::GetCFHandle(const std::string& str) {
    return nullptr;
}

rocksdb::Status Storage::Write(const rocksdb::WriteOptions& options, rocksdb::WriteBatch* updates) {
    return db_->Write(options, updates);
}

rocksdb::Status Storage::Delete(const rocksdb::WriteOptions& options, const rocksdb::Slice& key) {
    return db_->Delete(options, key);
}

rocksdb::Status Storage::DeleteRange(const rocksdb::Slice& first_key, const rocksdb::Slice& last_key) {
    rocksdb::WriteBatch batch;
    rocksdb::ColumnFamilyHandle *cf_handle = GetCFHandle("metadata");
    auto s = batch.DeleteRange(cf_handle, first_key, last_key);
    if (!s.ok()) {
        return s;
    }
    s = batch.Delete(cf_handle, last_key);
    if (!s.ok()) {
        return s;
    }
    return Write(rocksdb::WriteOptions(), &batch);
}

}
