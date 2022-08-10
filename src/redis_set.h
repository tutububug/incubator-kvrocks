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

#include <string>
#include <vector>

#include "redis_db.h"
#include "redis_metadata.h"

namespace Redis {

class Set : public SubKeyScanner {
 public:
  explicit Set(rockdis::Storage* storage, int64_t table_id)
      : SubKeyScanner(storage, table_id) {}

  rocksdb::Status Card(const Slice &user_key, int *ret);
  rocksdb::Status IsMember(const Slice &user_key, const Slice &member, int *ret);
  rocksdb::Status Add(const Slice &user_key, const std::vector<Slice> &members, int *ret);
  rocksdb::Status Remove(const Slice &user_key, const std::vector<Slice> &members, int *ret);
  rocksdb::Status Members(const Slice &user_key, std::vector<std::string> *members);
  rocksdb::Status Move(const Slice &src, const Slice &dst, const Slice &member, int *ret);
  rocksdb::Status Take(const Slice &user_key, std::vector<std::string> *members, int count, bool pop);
  rocksdb::Status Diff(const std::vector<Slice> &keys, std::vector<std::string> *members);
  rocksdb::Status Union(const std::vector<Slice> &keys, std::vector<std::string> *members);
  rocksdb::Status Inter(const std::vector<Slice> &keys, std::vector<std::string> *members);
  rocksdb::Status Overwrite(Slice user_key, const std::vector<std::string> &members);
  rocksdb::Status DiffStore(const Slice &dst, const std::vector<Slice> &keys, int *ret);
  rocksdb::Status UnionStore(const Slice &dst, const std::vector<Slice> &keys, int *ret);
  rocksdb::Status InterStore(const Slice &dst, const std::vector<Slice> &keys, int *ret);
  rocksdb::Status Scan(const Slice &user_key,
                       const std::string &cursor,
                       uint64_t limit,
                       const std::string &member_prefix,
                       std::vector<std::string> *members);

 private:
  rocksdb::Status GetMetadata(const Slice &ns_key, SetMetadata *metadata);
};

}  // namespace Redis
