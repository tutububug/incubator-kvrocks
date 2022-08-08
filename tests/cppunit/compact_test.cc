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

#include <gtest/gtest.h>

#include "config.h"
#include "store.h"
#include "redis_metadata.h"
#include "redis_hash.h"
#include "redis_zset.h"
#include "util.h"

TEST(Compact, Filter) {
  rocksdb::Options options;
  options.IncreaseParallelism();
  options.OptimizeLevelStyleCompaction();
  options.create_if_missing = true;

  rocksdb::DB* db;
  rocksdb::DB::Open(options, "/tmp/compactdb", &db);

  auto storage_ = Util::MakeUnique<rockdis::Storage>(db);
  auto s = storage_->Open();
  assert(s.IsOK());

  int ret;
  std::string ns = "test_compact";
  auto hash = Util::MakeUnique<Redis::Hash>(storage_.get(), ns);
  std::string expired_hash_key = "expire_hash_key";
  std::string live_hash_key = "live_hash_key";
  hash->Set(expired_hash_key, "f1", "v1", &ret);
  hash->Set(expired_hash_key, "f2", "v2", &ret);
  hash->Expire(expired_hash_key, 1); // expired
  usleep(10000);
  hash->Set(live_hash_key, "f1", "v1", &ret);
  hash->Set(live_hash_key, "f2", "v2", &ret);
  auto status = storage_->Compact(nullptr, nullptr);
  assert(status.ok());

  rocksdb::ReadOptions read_options;
  read_options.snapshot = db->GetSnapshot();
  read_options.fill_cache = false;

  auto NewIterator = [db, read_options, &storage_](const std::string& name){
    return std::unique_ptr<rocksdb::Iterator>(db->NewIterator(read_options, storage_->GetCFHandle(name)));
  };

  auto iter = NewIterator("metadata");
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    std::string user_key, user_ns;
    ExtractNamespaceKey(iter->key(), &user_ns, &user_key, storage_->IsSlotIdEncoded());
    EXPECT_EQ(user_key, live_hash_key);
  }

  iter = NewIterator("subkey");
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    InternalKey ikey(iter->key(), storage_->IsSlotIdEncoded());
    EXPECT_EQ(ikey.GetKey().ToString(), live_hash_key);
  }

  auto zset = Util::MakeUnique<Redis::ZSet>(storage_.get(), ns);
  std::string expired_zset_key = "expire_zset_key";
  std::vector<MemberScore> member_scores =  {MemberScore{"z1", 1.1}, MemberScore{"z2", 0.4}};
  zset->Add(expired_zset_key, 0, &member_scores, &ret);
  zset->Expire(expired_zset_key, 1); // expired
  usleep(10000);

  status = storage_->Compact(nullptr, nullptr);
  assert(status.ok());

  iter = NewIterator("default");
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    InternalKey ikey(iter->key(), storage_->IsSlotIdEncoded());
    EXPECT_EQ(ikey.GetKey().ToString(), live_hash_key);
  }

  iter = NewIterator("zset_score");
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    EXPECT_TRUE(false);  // never reach here
  }

  db->ReleaseSnapshot(read_options.snapshot);
}
