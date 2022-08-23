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

#include <chrono>
#include <utility>
#include <memory>
#include <glog/logging.h>

#include "util.h"
#include "redis_request.h"
#include "redis_slot.h"

namespace Redis {
const size_t PROTO_INLINE_MAX_SIZE = 16 * 1024L;
const size_t PROTO_BULK_MAX_SIZE = 512 * 1024L * 1024L;
const size_t PROTO_MULTI_MAX_SIZE = 1024 * 1024L;

Status Request::Tokenize(const std::string& input) {
  size_t last_pos = 0;
  while (true) {
    switch (state_) {
      case ArrayLen: {
        auto pos = input.find("\r\n", last_pos);
        if (pos == std::string::npos) {
          if (pos != input.size()-1) {
            std::ostringstream ss;
            ss << "invalid input protocol: left=" << std::string(input, last_pos);
            return Status(Status::NotOK, ss.str());
          }
          return Status::OK();
        }
        auto line = std::string(std::begin(input)+last_pos, std::begin(input)+pos);
        last_pos = pos+2;

        if (line[0] == '*') {
          try {
            multi_bulk_len_ = std::stoll(std::string(line.c_str() + 1, line.size() - 1));
          } catch (std::exception &e) {
            return Status(Status::NotOK, "Protocol error: invalid multibulk length");
          }
          if (multi_bulk_len_ <= 0) {
            multi_bulk_len_ = 0;
            continue;
          }
          if (multi_bulk_len_ > (int64_t)PROTO_MULTI_MAX_SIZE) {
            return Status(Status::NotOK, "Protocol error: invalid multibulk length");
          }
          state_ = BulkLen;
        } else {
          if (line.size() > PROTO_INLINE_MAX_SIZE) {
            return Status(Status::NotOK, "Protocol error: invalid bulk length");
          }
          tokens_ = Util::Split(std::string(line.c_str(), line.size()), " \t");
          commands_.emplace_back(std::move(tokens_));
          state_ = ArrayLen;
        }
        break;
      }
      case BulkLen: {
        auto pos = input.find("\r\n", last_pos);
        if (pos == std::string::npos) {
          if (pos != input.size()-1) {
            std::ostringstream ss;
            ss << "invalid input protocol: left=" << std::string(input, last_pos);
            return Status(Status::NotOK, ss.str());
          }
          return Status::OK();
        }
        auto line = std::string(std::begin(input)+last_pos, std::begin(input)+pos);
        last_pos = pos+2;

        if (line.size() <= 0) return Status::OK();
        if (line[0] != '$') {
          return Status(Status::NotOK, "Protocol error: expected '$'");
        }
        try {
          bulk_len_ = std::stoull(std::string(line.c_str() + 1, line.size() - 1));
        } catch (std::exception &e) {
          return Status(Status::NotOK, "Protocol error: invalid bulk length");
        }
        if (bulk_len_ > PROTO_BULK_MAX_SIZE) {
          return Status(Status::NotOK, "Protocol error: invalid bulk length");
        }
        state_ = BulkData;
        break;
      }
      case BulkData:
        auto line = std::string(input, last_pos, bulk_len_);
        last_pos += bulk_len_+2;

        tokens_.emplace_back(line);
        --multi_bulk_len_;
        if (multi_bulk_len_ == 0) {
          state_ = ArrayLen;
          commands_.emplace_back(std::move(tokens_));
          tokens_.clear();
        } else {
          state_ = BulkLen;
        }
        break;
    }
  }
}

}  // namespace Redis
