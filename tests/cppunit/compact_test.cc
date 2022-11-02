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

struct KeyResult {
  KeyResult(bool isMetaKey, const std::string& key, const std::string& field)
      : isMetaKey(isMetaKey), isSubKey(!isMetaKey), key(key), field(field) {}
  KeyResult() = delete;

  bool operator==(const KeyResult& other);

  bool isMetaKey;
  bool isSubKey;
  std::string key;
  std::string field;
};

bool KeyResult::operator==(const KeyResult& other) {
  return this->isMetaKey == other.isMetaKey &&
         this->isSubKey == other.isSubKey &&
         this->key == other.key &&
         this->field == other.field;
}

class CompactionTester: public testing::Test {
public:
  void SetUp() {
    store = new Redis::Storage();
    rocksdb::Options options;
    options.IncreaseParallelism();
    options.OptimizeLevelStyleCompaction();
    options.create_if_missing = true;
    options.disable_auto_compactions = true;
    options.compaction_filter_factory = std::make_shared<Redis::ExpireFilterFactory>(store);
    options.table_properties_collector_factories.resize(1);
    options.table_properties_collector_factories[0] = std::make_shared<Redis::ExpireCollectorFactory>();
    rocksdb::DB::Open(options, "/tmp/compactdb_"+std::to_string(std::chrono::system_clock::now().time_since_epoch().count()), &db);
    store->Open(db);
  }
  void TearDown() {
    system("rm -rf /tmp/compactdb_*");
  }

  void CheckData(const std::vector<KeyResult>& expect) {
    auto NewIterator = [](rocksdb::DB* db){
      rocksdb::ReadOptions read_options;
      read_options.snapshot = db->GetSnapshot();
      return std::unique_ptr<rocksdb::Iterator>(db->NewIterator(read_options));
    };

    auto checkData = [NewIterator](const std::vector<KeyResult>& expect, Redis::Storage* store, int64_t table_id) {
      std::vector<KeyResult> actual;
      auto iter = NewIterator(store->GetDB());
      for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
        std::string user_key;
        rocksdb::Status sts;
        auto k = iter->key().ToString();
        if (store->isMetaKey(k)) {
          sts = ExtractNamespaceKey(k, table_id, &user_key, store->IsSlotIdEncoded());
          EXPECT_EQ(sts, rocksdb::Status::OK());
          actual.emplace_back(true, user_key, "");
        } else if (store->isSubKey(k)) {
          InternalKey ikey;
          sts = ikey.Init(k, store->IsSlotIdEncoded());
          EXPECT_EQ(sts, rocksdb::Status::OK());
          actual.emplace_back(false, ikey.GetKey().ToString(), ikey.GetSubKey().ToString());
        } else {
          EXPECT_TRUE(false);
        };
      }
      EXPECT_TRUE(expect.size() == actual.size());
      for (auto i = 0; i < expect.size(); i++) {
        EXPECT_TRUE(((KeyResult) expect[i]) == actual[i]);
      }
    };
    checkData(expect, store, table_id);
  }

protected:
  rocksdb::DB* db = nullptr;
  int64_t table_id = 1;
  Redis::Storage* store = nullptr;
};

TEST_F(CompactionTester, FilterHash) {
  int ret;
  rocksdb::WriteBatch batch;
  auto hash = Util::MakeUnique<Redis::Hash>(store, table_id, &batch);
  std::string expired_hash_key = "expire_hash_key";
  std::string live_hash_key = "live_hash_key";
  hash->Set(expired_hash_key, "f1", "v1", &ret);
  hash->Set(expired_hash_key, "f2", "v2", &ret);
  hash->Expire(expired_hash_key, 1); // expired
  usleep(10000);
  hash->Set(live_hash_key, "f1", "v1", &ret);
  hash->Set(live_hash_key, "f2", "v2", &ret);
  { // first time compaction, expire metakey: 'expire_hash_key'
    auto status = store->Compact(nullptr, nullptr);
    assert(status.ok());
    std::vector<KeyResult> expect;
    expect.emplace_back(false, expired_hash_key, "f1");
    expect.emplace_back(false, expired_hash_key, "f2");
    expect.emplace_back(true, live_hash_key, "");
    expect.emplace_back(false, live_hash_key, "f1");
    expect.emplace_back(false, live_hash_key, "f2");
    CheckData(expect);
  }
  { // second time compaction, expire subkey: 'expire_hash_key: f1, expire_hash_key: f2'
    auto status = store->Compact(nullptr, nullptr);
    assert(status.ok());
    std::vector<KeyResult> expect;
    expect.emplace_back(true, live_hash_key, "");
    expect.emplace_back(false, live_hash_key, "f1");
    expect.emplace_back(false, live_hash_key, "f2");
    CheckData(expect);
  }
}

TEST_F(CompactionTester, FilterZset) {
  int ret;
  rocksdb::WriteBatch batch;
  auto zset = Util::MakeUnique<Redis::ZSet>(store, table_id, &batch);
  std::string expired_zset_key = "expire_zset_key";
  std::string z1 = "z1";
  std::string z2 = "z2";
  double s1 = 1.1;
  double s2 = 0.4;
  std::vector<MemberScore> member_scores = {MemberScore{z1, s1}, MemberScore{z2, s2}};
  auto sts = zset->Add(expired_zset_key, 0, &member_scores, &ret);
  assert(sts.ok());
  sts = zset->Expire(expired_zset_key, 1); // expired
  assert(sts.ok());
  usleep(10000);
  { // expire metakey
    auto status = store->Compact(nullptr, nullptr);
    assert(status.ok());
    std::vector<KeyResult> expect;
    expect.emplace_back(false, expired_zset_key, z1);
    expect.emplace_back(false, expired_zset_key, z2);
    std::string score2;
    PutDouble(&score2, s2);
    expect.emplace_back(false, expired_zset_key, score2.append(z2));
    std::string score1;
    PutDouble(&score1, s1);
    expect.emplace_back(false, expired_zset_key, score1.append(z1));
    CheckData(expect);
  }
  { // expire subkey and scorekey
    auto status = store->Compact(nullptr, nullptr);
    assert(status.ok());
    std::vector<KeyResult> expect;
    CheckData(expect);
  }
}
