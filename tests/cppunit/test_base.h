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
#ifndef KVROCKS_TEST_BASE_H
#define KVROCKS_TEST_BASE_H

#include <gtest/gtest.h>
#include "redis_db.h"
#include "redis_hash.h"

class TestBase : public testing::Test {
protected:
  explicit TestBase() {
    rocksdb::Options options;
    options.IncreaseParallelism();
    options.OptimizeLevelStyleCompaction();
    options.create_if_missing = true;

    db_path_ = "/tmp/testsdb-" + std::to_string(std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch()).count());
    rocksdb::DB* db;
    auto ss = rocksdb::DB::Open(options, db_path_, &db);
    assert(ss.ok());

    storage_ = new Redis::Storage();
    Status s = storage_->Open(db);
    if (!s.IsOK()) {
      std::cout << "Failed to open the storage, encounter error: " << s.Msg() << std::endl;
      assert(s.IsOK());
    }
  }
  ~TestBase() override {
    system(("rm -rf " + db_path_).c_str());
    delete storage_;
    delete config_;
  }

protected:
  Redis::Storage *storage_;
  Config *config_ = nullptr;
  std::string key_;
  std::vector<Slice> fields_;
  std::vector<Slice> values_;
  rocksdb::WriteBatch batch_;
  std::string db_path_;
};
#endif //KVROCKS_TEST_BASE_H
