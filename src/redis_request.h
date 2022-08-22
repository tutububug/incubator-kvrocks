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

#include <deque>
#include <vector>
#include <string>

#include "status.h"

class Server;

namespace Redis {

using CommandTokens = std::vector<std::string>;

class Request {
 public:
  Request() = default;
  // Not copyable
  Request(const Request &) = delete;
  Request &operator=(const Request &) = delete;

  // Parse the redis requests (bulk string array format)
  Status Tokenize(const std::string& input);

  std::vector<CommandTokens>& GetCommands() { return commands_; }

 private:
  // internal states related to parsing

  enum ParserState { ArrayLen, BulkLen, BulkData };
  ParserState state_ = ArrayLen;
  int64_t multi_bulk_len_ = 0;
  size_t bulk_len_ = 0;
  CommandTokens tokens_;
  std::vector<CommandTokens> commands_;
};

}  // namespace Redis
