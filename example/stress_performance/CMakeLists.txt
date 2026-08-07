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

cmake_minimum_required(VERSION 2.8.10)
project(rdma_performance C CXX)

option(LINK_SO "Whether examples are linked dynamically" OFF)
option(WITH_RDMA "Build rdma_performance with RDMA transport support" ON)

set(BRPC_OUTPUT_PATH "" CACHE PATH
    "Path containing the output/include and output/lib directories of brpc")
if(BRPC_OUTPUT_PATH)
    set(OUTPUT_PATH ${BRPC_OUTPUT_PATH})
else()
    execute_process(
        COMMAND bash -c "find ${PROJECT_SOURCE_DIR}/../.. -type d -regex \".*output/include$\" | head -n1 | xargs -r dirname | tr -d '\n'"
        OUTPUT_VARIABLE OUTPUT_PATH
    )
endif()

set(CMAKE_PREFIX_PATH ${OUTPUT_PATH} ${CMAKE_PREFIX_PATH})

include_directories(/home/zfz/ins/protobuf/include)
include(FindThreads)
include(FindProtobuf)
protobuf_generate_cpp(PROTO_SRC PROTO_HEADER test.proto)
# include PROTO_HEADER
include_directories(${CMAKE_CURRENT_BINARY_DIR})

# Search for libthrift* by best effort. If it is not found and brpc is
# compiled with thrift protocol enabled, a link error would be reported.
find_library(THRIFT_LIB NAMES thrift)
if (NOT THRIFT_LIB)
    set(THRIFT_LIB "")
endif()

find_path(BRPC_INCLUDE_PATH NAMES brpc/server.h)
if(LINK_SO)
    find_library(BRPC_LIB NAMES brpc)
else()
    find_library(BRPC_LIB NAMES libbrpc.a brpc)
endif()
if((NOT BRPC_INCLUDE_PATH) OR (NOT BRPC_LIB))
    message(FATAL_ERROR "Fail to find brpc")
endif()
include_directories(${BRPC_INCLUDE_PATH})

find_path(GFLAGS_INCLUDE_PATH gflags/gflags.h)
find_library(GFLAGS_LIBRARY NAMES gflags libgflags)
if((NOT GFLAGS_INCLUDE_PATH) OR (NOT GFLAGS_LIBRARY))
    message(FATAL_ERROR "Fail to find gflags")
endif()
include_directories(${GFLAGS_INCLUDE_PATH})

if(CMAKE_SYSTEM_NAME STREQUAL "Darwin")
    include(CheckFunctionExists)
    CHECK_FUNCTION_EXISTS(clock_gettime HAVE_CLOCK_GETTIME)
    if(NOT HAVE_CLOCK_GETTIME)
        set(DEFINE_CLOCK_GETTIME "-DNO_CLOCK_GETTIME_IN_MAC")
    endif()
endif()

set(WITH_RDMA_VAL "0")
if(WITH_RDMA)
    set(WITH_RDMA_VAL "1")
endif()

set(CMAKE_CPP_FLAGS "${DEFINE_CLOCK_GETTIME} -DBRPC_WITH_RDMA=${WITH_RDMA_VAL}")
set(CMAKE_CXX_FLAGS "${CMAKE_CPP_FLAGS} -DNDEBUG -O2 -D__const__=__unused__ -pipe -W -Wall -Wno-unused-parameter -fPIC -fno-omit-frame-pointer")

if(CMAKE_VERSION VERSION_LESS "3.1.3")
    if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17")
    endif()
    if(CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17")
    endif()
else()
    set(CMAKE_CXX_STANDARD 17)
    set(CMAKE_CXX_STANDARD_REQUIRED ON)
endif()

find_path(LEVELDB_INCLUDE_PATH NAMES leveldb/db.h)
find_library(LEVELDB_LIB NAMES leveldb)
if ((NOT LEVELDB_INCLUDE_PATH) OR (NOT LEVELDB_LIB))
    message(FATAL_ERROR "Fail to find leveldb")
endif()
include_directories(${LEVELDB_INCLUDE_PATH})

if(CMAKE_SYSTEM_NAME STREQUAL "Darwin")
    set(OPENSSL_ROOT_DIR
        "/usr/local/opt/openssl"    # Homebrew installed OpenSSL
        )
endif()

find_package(OpenSSL)
include_directories(${OPENSSL_INCLUDE_DIR})

if(WITH_RDMA)
    find_path(RDMA_INCLUDE_PATH NAMES infiniband/verbs.h)
    find_library(RDMA_LIB NAMES ibverbs)
    if ((NOT RDMA_INCLUDE_PATH) OR (NOT RDMA_LIB))
        message(FATAL_ERROR "Fail to find ibverbs")
    endif()
    include_directories(${RDMA_INCLUDE_PATH})
endif()


set(DYNAMIC_LIB
    ${CMAKE_THREAD_LIBS_INIT}
    ${GFLAGS_LIBRARY}
    ${PROTOBUF_LIBRARIES}
    ${LEVELDB_LIB}
    ${OPENSSL_CRYPTO_LIBRARY}
    ${OPENSSL_SSL_LIBRARY}
    ${THRIFT_LIB}
    dl
    )

if(WITH_RDMA)
    set(DYNAMIC_LIB ${DYNAMIC_LIB} ${RDMA_LIB})
endif()

if(CMAKE_SYSTEM_NAME STREQUAL "Darwin")
    set(DYNAMIC_LIB ${DYNAMIC_LIB}
        pthread
        "-framework CoreFoundation"
        "-framework CoreGraphics"
        "-framework CoreData"
        "-framework CoreText"
        "-framework Security"
        "-framework Foundation"
        "-Wl,-U,_MallocExtension_ReleaseFreeMemory"
        "-Wl,-U,_ProfilerStart"
        "-Wl,-U,_ProfilerStop"
        "-Wl,-U,__Z13GetStackTracePPvii"
        "-Wl,-U,_mallctl"
        "-Wl,-U,_malloc_stats_print"
    )
endif()

add_executable(client client.cpp ${PROTO_SRC} ${PROTO_HEADER})
add_executable(server server.cpp ${PROTO_SRC} ${PROTO_HEADER})

target_link_libraries(client ${BRPC_LIB} ${DYNAMIC_LIB})
target_link_libraries(server ${BRPC_LIB} ${DYNAMIC_LIB})

file(GLOB ABSL_LIBS "/home/zfz/ins/protobuf/lib64/libabsl_*.so")

# 2. 定义强制链接标志
# -Wl,--no-as-needed 会强制链接器加载后续的所有库，不论是否看起来“没被直接使用”
set(FORCE_LINK_FLAGS "-Wl,--no-as-needed")

# 3. 更新链接逻辑
if(TARGET client)
    target_link_libraries(client PRIVATE ${FORCE_LINK_FLAGS} ${ABSL_LIBS})
endif()
if(TARGET server)
    target_link_libraries(server PRIVATE ${FORCE_LINK_FLAGS} ${ABSL_LIBS})
endif()
