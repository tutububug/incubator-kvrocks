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

#include "redis_metadata.h"
#include "redis_slot.h"
#include <time.h>
#include <stdlib.h>
#include <sys/time.h>

#include <vector>
#include <atomic>
#include <rocksdb/env.h>
#include "redis_key_encoding.h"

// 52 bit for microseconds and 11 bit for counter
const int VersionCounterBits = 11;

static std::atomic<uint64_t> version_counter_ = {0};

const char* kErrMsgWrongType = "WRONGTYPE Operation against a key holding the wrong kind of value";
const char* kErrMsgKeyExpired = "the key was expired";

InternalKey::InternalKey(Slice input, bool slot_id_encoded)
        : slot_id_encoded_(slot_id_encoded) {
  auto ns_key = input.ToString();
  size_t off = 0;

  table_id_ = Redis::DecodeInt(ns_key, off);
  if (slot_id_encoded) {
      slotid_ = Redis::DecodeInt(ns_key, off); // decode slot
  }
  std::string key;
  Redis::DecodeBytes(ns_key, off, &key); // decode user key
  key_ = std::move(key);

  cf_code_ = Redis::DecodeInt(ns_key, off); // decode cf code
  version_ = static_cast<uint64_t>(Redis::DecodeInt(ns_key, off)); // decode sub key version
  std::string sub_key;
  Redis::DecodeBytes(ns_key, off, &sub_key); // decode sub key
  sub_key_ = std::move(sub_key);
}

InternalKey::InternalKey(Slice nsk, Slice sub_key, uint64_t version, bool slot_id_encoded, int64_t cf_code)
        : sub_key_(sub_key), version_(version), slot_id_encoded_(slot_id_encoded), cf_code_(cf_code) {
    slot_id_encoded_ = slot_id_encoded;

    auto ns_key = nsk.ToString();
    size_t off = 0;

    table_id_ = Redis::DecodeInt(ns_key, off);
    if (slot_id_encoded) {
        slotid_ = Redis::DecodeInt(ns_key, off); // decode slot
    }
    std::string key;
    Redis::DecodeBytes(ns_key, off, &key); // decode user key
    key_ = std::move(key);
}

InternalKey::~InternalKey() {
}

int64_t InternalKey::GetNamespace() const {
  return table_id_;
}

Slice InternalKey::GetKey() const {
  return key_;
}

int64_t InternalKey::GetCF() const {
    return cf_code_;
}

Slice InternalKey::GetSubKey() const {
  return sub_key_;
}

uint64_t InternalKey::GetVersion() const {
  return version_;
}

void InternalKey::Encode(std::string *out) {
    out->clear();

    Redis::EncodeInt(out, table_id_); // encode table id
    if (slot_id_encoded_) {
        auto slot_id = GetSlotNumFromKey(key_.ToString());
        Redis::EncodeInt(out, slot_id); // encode slot
    }
    Redis::EncodeBytes(out, key_.ToString()); // encode user key
    Redis::EncodeInt(out, cf_code_); // encode cf code
    Redis::EncodeInt(out, version_); // encode version
    Redis::EncodeBytes(out, sub_key_.ToString()); // encode sub key
}

bool InternalKey::operator==(const InternalKey &that) const {
  if (key_ != that.key_) return false;
  if (sub_key_ != that.sub_key_) return false;
  return version_ == that.version_;
}

void ExtractNamespaceKey(const Slice& nsk, int64_t& table_id, std::string *key, bool slot_id_encoded) {
    auto ns_key = nsk.ToString();
    size_t off = 0;

    try{
        table_id = Redis::DecodeInt(ns_key, off); // decode table id
        if (slot_id_encoded) {
            Redis::DecodeInt(ns_key, off); // decode slot
        }
        Redis::DecodeBytes(ns_key, off, key); // decode user key
        Redis::DecodeInt(ns_key, off); // decode cf code
    } catch (const std::exception& e) {
        throw e;
    }
}

void ComposeNamespaceKey(int64_t table_id, const Slice& key, std::string *ns_key, bool slot_id_encoded, int64_t cf_code) {
    ns_key->clear();

    Redis::EncodeInt(ns_key, table_id); // encode table id
    auto key_str = key.ToString();
    if (slot_id_encoded) {
        auto slot_id = GetSlotNumFromKey(key_str);
        Redis::EncodeInt(ns_key, slot_id); // encode slot
    }
    Redis::EncodeBytes(ns_key, key_str); // encode user key
    Redis::EncodeInt(ns_key, cf_code); // encode cf code
}

void ComposeSlotKeyPrefix(int64_t table_id, int slotid, std::string *output) {
    output->clear();

    Redis::EncodeInt(output, table_id);
    Redis::EncodeInt(output, slotid);
}

Metadata::Metadata(RedisType type, bool generate_version) {
  flags = (uint8_t)0x0f & type;
  expire = 0;
  version = 0;
  size = 0;
  if (generate_version) version = generateVersion();
}

rocksdb::Status Metadata::Decode(const std::string &bytes) {
  // flags(1byte) + expire (4byte)
  if (bytes.size() < 5) {
    return rocksdb::Status::InvalidArgument("the metadata was too short");
  }
  Slice input(bytes);
  GetFixed8(&input, &flags);
  GetFixed32(&input, reinterpret_cast<uint32_t *>(&expire));
  if (Type() != kRedisString) {
    if (input.size() < 12) rocksdb::Status::InvalidArgument("the metadata was too short");
    GetFixed64(&input, &version);
    GetFixed32(&input, &size);
  }
  return rocksdb::Status::OK();
}

void Metadata::Encode(std::string *dst) {
  PutFixed8(dst, flags);
  PutFixed32(dst, (uint32_t) expire);
  if (Type() != kRedisString) {
    PutFixed64(dst, version);
    PutFixed32(dst, size);
  }
}

void Metadata::InitVersionCounter() {
  struct timeval now;
  gettimeofday(&now, nullptr);
  // use random position for initial counter to avoid conflicts,
  // when the slave was promoted as master and the system clock may backoff
  srand(static_cast<unsigned>(now.tv_sec));
  version_counter_ = static_cast<uint64_t>(std::rand());
}

uint64_t Metadata::generateVersion() {
  struct timeval now;
  gettimeofday(&now, nullptr);
  uint64_t version = static_cast<uint64_t >(now.tv_sec)*1000000;
  version += static_cast<uint64_t>(now.tv_usec);
  uint64_t counter = version_counter_.fetch_add(1);
  return (version << VersionCounterBits) + (counter%(1 << VersionCounterBits));
}

bool Metadata::operator==(const Metadata &that) const {
  if (flags != that.flags) return false;
  if (expire != that.expire) return false;
  if (Type() != kRedisString) {
    if (size != that.size) return false;
    if (version != that.version) return false;
  }
  return true;
}

RedisType Metadata::Type() const {
  return static_cast<RedisType>(flags & (uint8_t)0x0f);
}

int32_t Metadata::TTL() const {
  int64_t now;
  if (expire <= 0) {
    return -1;
  }
  rocksdb::Env::Default()->GetCurrentTime(&now);
  if (expire < now) {
    return -2;
  }
  return int32_t (expire - now);
}

timeval Metadata::Time() const {
  auto t = version >> VersionCounterBits;
  struct timeval created_at{static_cast<uint32_t>(t / 1000000), static_cast<int32_t>(t % 1000000)};
  return created_at;
}

bool Metadata::Expired() const {
  if (Type() != kRedisString && size == 0) {
    return true;
  }
  if (expire <= 0) {
    return false;
  }
  int64_t now;
  rocksdb::Env::Default()->GetCurrentTime(&now);
  return expire < now;
}

ListMetadata::ListMetadata(bool generate_version) : Metadata(kRedisList, generate_version) {
  head = UINT64_MAX/2;
  tail = head;
}

void ListMetadata::Encode(std::string *dst) {
  Metadata::Encode(dst);
  PutFixed64(dst, head);
  PutFixed64(dst, tail);
}

rocksdb::Status ListMetadata::Decode(const std::string &bytes) {
  Slice input(bytes);
  GetFixed8(&input, &flags);
  GetFixed32(&input, reinterpret_cast<uint32_t *>(&expire));
  if (Type() != kRedisString) {
    if (input.size() < 12) rocksdb::Status::InvalidArgument("the metadata was too short");
    GetFixed64(&input, &version);
    GetFixed32(&input, &size);
  }
  if (Type() == kRedisList) {
    if (input.size() < 16) rocksdb::Status::InvalidArgument("the metadata was too short");
    GetFixed64(&input, &head);
    GetFixed64(&input, &tail);
  }
  return rocksdb::Status::OK();
}
