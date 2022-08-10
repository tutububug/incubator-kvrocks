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

#include <stdint.h>
#include <vector>
#include <string>

#include "redis_db.h"
#include "redis_metadata.h"
#include "encoding.h"

namespace Redis {
class List : public Database {
 public:
  explicit List(rockdis::Storage* storage, int64_t table_id) : Database(storage, table_id) {}
  rocksdb::Status Size(const Slice &user_key, uint32_t *ret);
  rocksdb::Status Trim(const Slice &user_key, int start, int stop);
  rocksdb::Status Set(const Slice &user_key, int index, Slice elem);
  rocksdb::Status Insert(const Slice &user_key, const Slice &pivot, const Slice &elem, bool before, int *ret);
  rocksdb::Status Pop(const Slice &user_key, bool left, std::string *elem);
  rocksdb::Status PopMulti(const Slice &user_key, bool left, uint32_t count, std::vector<std::string> *elems);
  rocksdb::Status Rem(const Slice &user_key, int count, const Slice &elem, int *ret);
  rocksdb::Status Index(const Slice &user_key, int index, std::string *elem);
  rocksdb::Status RPopLPush(const Slice &src, const Slice &dst, std::string *elem);
  rocksdb::Status LMove(const Slice &src, const Slice &dst, bool src_left, bool dst_left, std::string *elem);
  rocksdb::Status Push(const Slice &user_key, const std::vector<Slice> &elems, bool left, int *ret);
  rocksdb::Status PushX(const Slice &user_key, const std::vector<Slice> &elems, bool left, int *ret);
  rocksdb::Status Range(const Slice &user_key, int start, int stop, std::vector<std::string> *elems);

 private:
  rocksdb::Status GetMetadata(const Slice &ns_key, ListMetadata *metadata);
  rocksdb::Status push(const Slice &user_key, std::vector<Slice> elems, bool create_if_missing, bool left, int *ret);
  rocksdb::Status lmoveOnSingleList(const Slice &src, bool src_left, bool dst_left, std::string *elem);
  rocksdb::Status lmoveOnTwoLists(const Slice &src, const Slice &dst, bool src_left, bool dst_left, std::string *elem);
};
}  // namespace Redis
