/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

#pragma once

#include "bitfield_util.h"
#include "storage/redis_db.h"
#include "storage/redis_metadata.h"

namespace redis {

constexpr uint32_t kHyperLogLogRegisterCountMask = kHyperLogLogRegisterCount - 1; /* Mask to index register. */
constexpr uint32_t kHyperLogLogBits = 6;
constexpr uint32_t kHyperLogLogRegisterMax = ((1 << kHyperLogLogBits) - 1);
constexpr double kHyperLogLogAlphaInf = 0.721347520444481703680; /* constant for 0.5/ln(2) */
constexpr uint32_t kHyperLogLogSegmentCount = 16;
constexpr uint32_t kHyperLogLogRegisterCountPerSegment = kHyperLogLogRegisterCount / kHyperLogLogSegmentCount;
constexpr uint32_t kHyperLogLogRegisterBytesPerSegment =
    kHyperLogLogRegisterCountPerSegment * kHyperLogLogBits / sizeof(uint8_t);
constexpr uint32_t kHyperLogLogRegisterBytes = kHyperLogLogRegisterCount * kHyperLogLogBits / sizeof(uint8_t);

class Hyperloglog : public Database {
 public:
  class SegmentCacheStore;

  explicit Hyperloglog(engine::Storage *storage, const std::string &ns) : Database(storage, ns) {}
  rocksdb::Status Add(const Slice &user_key, const std::vector<Slice> &elements, int *ret);
  rocksdb::Status Count(const Slice &user_key, int *ret);
  rocksdb::Status Merge(const std::vector<Slice> &user_keys);

 private:
  uint64_t hllCount(const std::vector<uint8_t> &counts);
  void hllMerge(uint8_t *max, const std::vector<uint8_t> &counts);
  rocksdb::Status getRegisters(const Slice &user_key, std::vector<uint8_t> *registers);

  rocksdb::Status GetMetadata(const Slice &ns_key, HyperloglogMetadata *metadata);
  int hllPatLen(unsigned char *ele, size_t elesize, long *regp);
};

class Hyperloglog::SegmentCacheStore {
 public:
  SegmentCacheStore(engine::Storage *storage, rocksdb::ColumnFamilyHandle *metadata_cf_handle,
                    std::string namespace_key, const Metadata &bitmap_metadata)
      : storage_(storage), ns_key_(std::move(namespace_key)), metadata_(bitmap_metadata) {}
  SegmentCacheStore(const SegmentCacheStore &) = delete;
  SegmentCacheStore &operator=(const SegmentCacheStore &) = delete;
  ~SegmentCacheStore() = default;

rocksdb::Status Set(uint32_t segment_index, const std::vector<uint8_t> &registers) {
  auto bitfield = new ArrayBitfieldBitmap;
  auto s = bitfield->Set(0, registers.size(), registers.data());
  if (!s.IsOK()) return rocksdb::Status::IOError("ArrayBitfieldBitmap: " + s.Msg());
  cache_[segment_index].reset(bitfield);
  return rocksdb::Status::OK();
}

  rocksdb::Status Set(uint32_t register_index, uint8_t count) {
    uint32_t segment_index = register_index / kHyperLogLogRegisterCountPerSegment;
    uint32_t offset_in_segment = register_index % kHyperLogLogRegisterCountPerSegment;

    auto it = cache_.find(segment_index);
    if (it == cache_.end()) {
      std::string sub_key =
          InternalKey(ns_key_, std::to_string(segment_index), metadata_.version, storage_->IsSlotIdEncoded()).Encode();
      std::string value;
      auto s = storage_->Get(rocksdb::ReadOptions(), sub_key, &value);
      if (!s.ok() && !s.IsNotFound()) return s;

      std::vector<uint8_t> registers(kHyperLogLogRegisterBytesPerSegment);
      auto bitfield = std::unique_ptr<ArrayBitfieldBitmap>(new ArrayBitfieldBitmap);
      if (s.IsNotFound()) {
        auto s = bitfield->Set(0, registers.size(), registers.data());
        if (!s.IsOK()) return rocksdb::Status::IOError("ArrayBitfieldBitmap: " + s.Msg());
      } else {
        auto s = bitfield->Set(0, value.size(), reinterpret_cast<uint8_t *>(value.data()));
        if (!s.IsOK()) return rocksdb::Status::IOError("ArrayBitfieldBitmap: " + s.Msg());
      }
      cache_[segment_index] = std::move(bitfield);
    }

    auto &segment = cache_[segment_index];
    uint8_t old_count = segment->GetUnsignedBitfield(offset_in_segment, kHyperLogLogBits).GetValue();
    if (count > old_count) {
      auto s = segment->SetBitfield(offset_in_segment, kHyperLogLogBits, count);
      if (!s.IsOK()) return rocksdb::Status::IOError("ArrayBitfieldBitmap: " + s.Msg());
      dirty_++;
    }
    return rocksdb::Status::OK();
  }

  rocksdb::Status BatchForFlush(ObserverOrUniquePtr<rocksdb::WriteBatchBase> &batch) {
    for (auto &[index, content] : cache_) {
      std::string sub_key =
          InternalKey(ns_key_, std::to_string(index), metadata_.version, storage_->IsSlotIdEncoded()).Encode();
      std::vector<uint8_t> registers(kHyperLogLogRegisterBytesPerSegment);
      auto s = content->Get(0, registers.size(), registers.data());
      if (!s.IsOK()) return rocksdb::Status::IOError("ArrayBitfieldBitmap: " + s.Msg());
      // Skip empty segment
      if (std::all_of(registers.begin(), registers.end(), [](uint8_t val) { return val == 0; })) {
        continue;
      };
      batch->Put(sub_key, std::string(registers.begin(), registers.end()));
    }
    return rocksdb::Status::OK();
  }

  size_t GetDirtyCount() { return dirty_; }

 private:
  engine::Storage *storage_;
  std::string ns_key_;
  Metadata metadata_;
  std::unordered_map<uint32_t, std::unique_ptr<ArrayBitfieldBitmap>> cache_;
  size_t dirty_ = 0;
};

}  // namespace redis
