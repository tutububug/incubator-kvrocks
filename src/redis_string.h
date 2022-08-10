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

#include <vector>
#include <string>

#include "redis_db.h"
#include "redis_metadata.h"

typedef struct {
  Slice key;
  Slice value;
} StringPair;

namespace Redis {

const int STRING_HDR_SIZE = 5;

class String : public Database {
 public:
  explicit String(rockdis::Storage* storage, int64_t table_id) : Database(storage, table_id) {}
  rocksdb::Status Append(const std::string &user_key, const std::string &value, int *ret);
  rocksdb::Status Get(const std::string &user_key, std::string *value);
  rocksdb::Status GetSet(const std::string &user_key, const std::string &new_value, std::string *old_value);
  rocksdb::Status Set(const std::string &user_key, const std::string &value);
  rocksdb::Status SetEX(const std::string &user_key, const std::string &value, int ttl);
  rocksdb::Status SetNX(const std::string &user_key, const std::string &value, int ttl, int *ret);
  rocksdb::Status SetXX(const std::string &user_key, const std::string &value, int ttl, int *ret);
  rocksdb::Status SetRange(const std::string &user_key, int offset, const std::string &value, int *ret);
  rocksdb::Status IncrBy(const std::string &user_key, int64_t increment, int64_t *ret);
  rocksdb::Status IncrByFloat(const std::string &user_key, double increment, double *ret);
  std::vector<rocksdb::Status> MGet(const std::vector<Slice> &keys, std::vector<std::string> *values);
  rocksdb::Status MSet(const std::vector<StringPair> &pairs, int ttl = 0);
  rocksdb::Status MSetNX(const std::vector<StringPair> &pairs, int ttl, int *ret);
  rocksdb::Status CAS(const std::string &user_key, const std::string &old_value,
                      const std::string &new_value, int ttl, int *ret);
  rocksdb::Status CAD(const std::string &user_key, const std::string &value, int *ret);

 private:
  rocksdb::Status getValue(const std::string &ns_key, std::string *value);
  std::vector<rocksdb::Status> getValues(const std::vector<Slice> &ns_keys, std::vector<std::string> *values);
  rocksdb::Status getRawValue(const std::string &ns_key, std::string *raw_value);
  std::vector<rocksdb::Status> getRawValues(const std::vector<Slice> &keys, std::vector<std::string> *raw_values);
  rocksdb::Status updateRawValue(const std::string &ns_key, const std::string &raw_value);
};

}  // namespace Redis
