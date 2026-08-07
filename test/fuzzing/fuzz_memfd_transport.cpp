// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <assert.h>
#include <stddef.h>
#include <stdint.h>

#include <algorithm>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <vector>

#include <fuzzer/FuzzedDataProvider.h>
#include <gflags/gflags.h>

#define private public
#include "brpc/memfd_transport.h"
#include "brpc/socket.h"
#undef private

#include "brpc/memfd/shm_zero_copy_stream.h"

namespace brpc {
DECLARE_int64(shm_block_size);
DECLARE_uint64(shm_buff_size);
DECLARE_uint64(shm_queue_size);
}  // namespace brpc

namespace {

struct ReassembleState {
    brpc::ShmBlockAllocator* allocator;
    std::string bytes;
};

void ReassembleElement(void* arg, const brpc::ShmQueueElement* elem) {
    ReassembleState* state = static_cast<ReassembleState*>(arg);
    assert(state->allocator->IsValidDataRange(elem->offset, elem->data_size));
    char* data = state->allocator->GetDataPtrFromOffset(elem->offset);
    state->bytes.append(data, elem->data_size);
    const uint32_t block_index =
        state->allocator->GetBlockIndexFromOffset(elem->offset);
    state->allocator->DecPendingCount(block_index);
}

bool AppendChunk(butil::IOBuf* buf, const std::vector<uint8_t>& chunk,
                 bool use_shm) {
    if (chunk.empty()) {
        return true;
    }
    return buf->append(chunk.data(), chunk.size(),
                       use_shm ? butil::IOBuf::BUFFER_TYPE_SHM
                               : butil::IOBuf::BUFFER_TYPE_NORMAL) == 0;
}

void BuildIOBuf(FuzzedDataProvider* provider, butil::IOBuf* buf,
                std::string* expected) {
    const size_t chunk_count =
        provider->ConsumeIntegralInRange<size_t>(0, 32);
    for (size_t i = 0; i < chunk_count && provider->remaining_bytes() > 0; ++i) {
        const bool use_shm = provider->ConsumeBool();
        const size_t chunk_size =
            provider->ConsumeIntegralInRange<size_t>(
                0, std::min<size_t>(8193, provider->remaining_bytes()));
        std::vector<uint8_t> chunk =
            provider->ConsumeBytes<uint8_t>(chunk_size);
        if (!AppendChunk(buf, chunk, use_shm)) {
            return;
        }
        expected->append(reinterpret_cast<const char*>(chunk.data()),
                         chunk.size());
    }
}

std::string RemainingBytes(const std::vector<std::unique_ptr<butil::IOBuf>>& bufs) {
    std::string remaining;
    for (const auto& buf : bufs) {
        remaining += buf->to_string();
    }
    return remaining;
}

std::string DrainQueue(brpc::ShmQueue* queue,
                       brpc::ShmBlockAllocator* allocator) {
    ReassembleState state;
    state.allocator = allocator;
    while (queue->Size() > 0) {
        const int n = queue->DequeueAndProcessBatch(
            ReassembleElement, &state, 64);
        if (n <= 0) {
            break;
        }
    }
    return state.bytes;
}

void FuzzOneTransport(FuzzedDataProvider* provider) {
    brpc::SocketOptions options;
    options.socket_mode = brpc::SOCKET_MODE_MEMFD;
    brpc::SocketId id;
    if (brpc::Socket::Create(options, &id) != 0) {
        return;
    }
    brpc::SocketUniquePtr socket;
    if (brpc::Socket::Address(id, &socket) != 0) {
        return;
    }
    brpc::MemfdTransport* transport =
        dynamic_cast<brpc::MemfdTransport*>(socket->_transport.get());
    assert(transport != nullptr);

    std::vector<std::unique_ptr<butil::IOBuf>> bufs;
    std::string expected;
    const bool use_list = provider->ConsumeBool();
    const size_t nbufs = use_list
        ? provider->ConsumeIntegralInRange<size_t>(1, 4)
        : 1;
    for (size_t i = 0; i < nbufs; ++i) {
        std::unique_ptr<butil::IOBuf> buf(new butil::IOBuf);
        BuildIOBuf(provider, buf.get(), &expected);
        bufs.push_back(std::move(buf));
    }

    ssize_t sent = 0;
    if (use_list) {
        std::vector<butil::IOBuf*> raw;
        for (auto& buf : bufs) {
            raw.push_back(buf.get());
        }
        sent = transport->CutFromIOBufList(raw.data(), raw.size());
    } else {
        sent = transport->CutFromIOBuf(bufs[0].get());
    }
    assert(sent >= 0);
    assert(static_cast<size_t>(sent) <= expected.size());

    brpc::ShmQueue* queue =
        transport->Session()->queue_manager()->send_queue();
    std::string actual = DrainQueue(queue, brpc::GetShmAllocator());
    assert(actual == expected.substr(0, static_cast<size_t>(sent)));
    assert(RemainingBytes(bufs) ==
           expected.substr(static_cast<size_t>(sent)));

    socket->SetFailed();
}

}  // namespace

extern "C" int LLVMFuzzerInitialize(int*, char***) {
    brpc::FLAGS_shm_block_size = 4096;
    brpc::FLAGS_shm_buff_size = 1024 * 1024;
    brpc::FLAGS_shm_queue_size = 64 * 1024;
    brpc::InitShmAllocator();
    return 0;
}

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    if (size > 64 * 1024) {
        return 0;
    }
    FuzzedDataProvider provider(data, size);
    FuzzOneTransport(&provider);
    return 0;
}
