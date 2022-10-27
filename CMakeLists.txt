cmake_minimum_required(VERSION 3.13)
project(rockdis
        DESCRIPTION "storage engine of redis data structure on rocksdb"
        LANGUAGES CXX C)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_C_FLAGS_DEBUG "-D_DEBUG")

if(CMAKE_BUILD_TYPE STREQUAL "Debug")
        set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -O0 -g")
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O0 -g")
else()
        set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -O2 -g")
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O2 -g")
endif()

if (CMAKE_HOST_APPLE)
        set(DISABLE_JEMALLOC 1)
endif ()
if(NOT DISABLE_JEMALLOC)
        include(cmake/jemalloc.cmake)
        list(APPEND EXTERNAL_LIBS PRIVATE ${jemalloc_BINARY_DIR})
        list(APPEND EXTERNAL_INCS PRIVATE ${jemalloc_SOURCE_DIR}/include)
endif()
include(cmake/glog.cmake)
include(cmake/snappy.cmake)
include(cmake/lz4.cmake)
include(cmake/rocksdb.cmake)
include(cmake/gtest.cmake)

list(APPEND EXTERNAL_INCS ${gtest_SOURCE_DIR}/include)
list(APPEND EXTERNAL_LIBS ${rocksdb_BINARY_DIR})
list(APPEND EXTERNAL_INCS ${rocksdb_SOURCE_DIR}/include)
list(APPEND EXTERNAL_INCS ${rocksdb_SOURCE_DIR}/include/rocksdb)
list(APPEND EXTERNAL_LIBS ${snappy_BINARY_DIR})
list(APPEND EXTERNAL_INCS ${snappy_SOURCE_DIR})
list(APPEND EXTERNAL_LIBS ${lz4_SOURCE_DIR}/lib)
list(APPEND EXTERNAL_INCS ${lz4_SOURCE_DIR})
list(APPEND EXTERNAL_LIBS ${glog_BINARY_DIR})
list(APPEND EXTERNAL_INCS ${glog_SOURCE_DIR}/src ${glog_BINARY_DIR})

include_directories(
        ${PROJECT_SOURCE_DIR}/src
        ${EXTERNAL_INCS}
)

link_directories(
        ${PROJECT_BINARY_DIR}
        ${PROJECT_BINARY_DIR}/lib
        ${EXTERNAL_LIBS}
)

option(STORAGE_DO_WRITE "redisdb write to underlay directly" OFF)
if(STORAGE_DO_WRITE)
        add_definitions(-DREDIS_STORAGE_DO_WRITE)
endif()

# redisdb library
set(REDISDB_SRCS
        src/redis_zset.cc
        src/redis_set.cc
        src/redis_list.cc
        src/redis_hash.cc
        src/redis_string.cc
        src/redis_db.cc
        src/redis_metadata.cc
        src/encoding.cc
        src/store.cc
        src/util.cc
        src/lock_manager.cc
        src/redis_slot.cc
        src/redis_key_encoding.cc
        src/redis_processor.cc
        src/redis_processor_c.cc
        src/redis_request.cc
        src/redis_cmd.cc
        src/redis_reply.cc
        )
add_library(redisdb STATIC ${REDISDB_SRCS})

# unit test
set(TESTS_SRCS
        tests/cppunit/main.cc
#        tests/cppunit/compact_test.cc
#        tests/cppunit/rwlock_test.cc
#        tests/cppunit/t_encoding_test.cc
        tests/cppunit/t_hash_test.cc
        tests/cppunit/t_list_test.cc
#        tests/cppunit/t_metadata_test.cc
        tests/cppunit/t_set_test.cc
        tests/cppunit/t_string_test.cc
        tests/cppunit/t_zset_test.cc
        tests/cppunit/encoding_test.cc
        tests/cppunit/request_test.cc
        tests/cppunit/redis_processor_test.cc
        )
add_executable(unittest ${TESTS_SRCS})
add_dependencies(unittest gtest redisdb rocksdb snappy lz4 glog)
target_link_libraries(unittest PUBLIC -lgtestd -lredisdb -lrocksdb -lsnappy -llz4 -lglogd)
#target_link_libraries(unittest PUBLIC -lgtestd -lredisdb -lrocksdb -lsnappy -llz4 -lglog -lpthread) # on linux
