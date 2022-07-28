# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

cmake_minimum_required(VERSION 3.13)
project(kvrocks-redisdb
        DESCRIPTION "NoSQL which based on rocksdb and compatible with the Redis protocol"
        LANGUAGES CXX)

# redisdb library
set(REDISDB_SRCS
        src/redis_zset.cc
        src/redis_set.cc
        src/redis_hash.cc
        src/redis_string.cc
        src/redis_db.cc
        src/redis_metadata.cc
        src/encoding.cc
        )
add_library(redisdb STATIC ${REDISDB_SRCS})
target_include_directories(redisdb PUBLIC
        ${PROJECT_SOURCE_DIR}/src
        ${rocksdb_SOURCE_DIR}/include
        ${libevent_SOURCE_DIR}/include ${libevent_BINARY_DIR}/include
        )
target_compile_features(redisdb PUBLIC cxx_std_11)
target_compile_options(redisdb PUBLIC ${WARNING_FLAGS} -fno-omit-frame-pointer)
