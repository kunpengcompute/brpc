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

#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include <algorithm>
#include <atomic>
#include <vector>

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/syscall.h>
#include <unistd.h>

#include <fuzzer/FuzzedDataProvider.h>

#include "brpc/memfd/shm_block_allocator.h"
#include "brpc/memfd/shm_session.h"

#ifndef MFD_CLOEXEC
#define MFD_CLOEXEC 0x0001U
#endif

namespace {

int CreateMemfd(const char* name, size_t size) {
    int fd = static_cast<int>(
        syscall(SYS_memfd_create, name, MFD_CLOEXEC));
    if (fd < 0) {
        return -1;
    }
    if (ftruncate(fd, size) != 0) {
        close(fd);
        return -1;
    }
    return fd;
}

void CopyBytes(void* dst, size_t dst_size,
               const uint8_t* data, size_t size) {
    memset(dst, 0, dst_size);
    memcpy(dst, data, std::min(dst_size, size));
}

bool HasPrefix(const uint8_t* data, size_t size, const char* prefix) {
    const size_t prefix_size = strlen(prefix);
    return size >= prefix_size && memcmp(data, prefix, prefix_size) == 0;
}

void InstallValidAllocatorHeader(void* mem, size_t size) {
    brpc::ShmBufferHeader* header =
        reinterpret_cast<brpc::ShmBufferHeader*>(mem);
    const uint32_t block_size = 4096;
    const size_t per_block = sizeof(brpc::ShmBlockState) + block_size;
    const uint32_t block_count = static_cast<uint32_t>(
        std::max<size_t>(1, (size - sizeof(brpc::ShmBufferHeader)) /
                                per_block));
    header->magic = brpc::SHM_MAGIC;
    header->version = brpc::SHM_VERSION_V1;
    header->header_size = sizeof(brpc::ShmBufferHeader);
    header->block_state_size = sizeof(brpc::ShmBlockState);
    header->feature_flags = 0;
    header->block_size = block_size;
    header->block_count = block_count;
    header->free_head.store(0, std::memory_order_relaxed);
    header->free_tail.store(block_count - 1, std::memory_order_relaxed);
    header->free_count.store(block_count, std::memory_order_relaxed);
    header->free_notify.store(0, std::memory_order_relaxed);
    header->initialized.store(0, std::memory_order_relaxed);
    memset(header->reserved, 0, sizeof(header->reserved));
}

void InstallValidQueueHeader(void* mem, size_t size, bool full) {
    brpc::ShmQueueHeader* header =
        reinterpret_cast<brpc::ShmQueueHeader*>(mem);
    const uint32_t capacity = static_cast<uint32_t>(
        std::max<size_t>(1, (size - sizeof(brpc::ShmQueueHeader)) /
                                sizeof(brpc::ShmQueueElement)));
    header->magic = brpc::SHM_QUEUE_MAGIC;
    header->version = brpc::SHM_VERSION_V1;
    header->header_size = sizeof(brpc::ShmQueueHeader);
    header->elem_size = sizeof(brpc::ShmQueueElement);
    header->feature_flags = 0;
    header->queue_size = capacity;
    header->write_pos.store(full ? capacity : 0, std::memory_order_relaxed);
    header->read_pos.store(0, std::memory_order_relaxed);
    header->reading.store(0, std::memory_order_relaxed);
    header->reserved = 0;
}

void ProcessQueueElement(void* arg, const brpc::ShmQueueElement* elem) {
    std::vector<brpc::ShmQueueElement>* seen =
        static_cast<std::vector<brpc::ShmQueueElement>*>(arg);
    seen->push_back(*elem);
}

void FuzzAllocatorMetadata(const uint8_t* data, size_t size,
                           FuzzedDataProvider* provider) {
    const size_t mapping_size = HasPrefix(data, size, "VALID_SHM_ALLOC")
        ? 64 * 1024
        : provider->ConsumeIntegralInRange<size_t>(
              sizeof(brpc::ShmBufferHeader), 64 * 1024);
    int fd = CreateMemfd("brpc_fuzz_shm_metadata", mapping_size);
    if (fd < 0) {
        return;
    }
    void* mem = mmap(nullptr, mapping_size, PROT_READ | PROT_WRITE,
                     MAP_SHARED, fd, 0);
    if (mem == MAP_FAILED) {
        close(fd);
        return;
    }
    CopyBytes(mem, mapping_size, data, size);
    if (HasPrefix(data, size, "VALID_SHM_ALLOC")) {
        InstallValidAllocatorHeader(mem, mapping_size);
    }
    munmap(mem, mapping_size);

    brpc::ShmBlockAllocator allocator;
    if (allocator.OpenFromFd(fd, mapping_size) == 0) {
        fd = -1;  // allocator owns the fd after a successful OpenFromFd.
        butil::IOBuf::Block* block = allocator.TryAllocBlock();
        if (block) {
            const uint32_t block_index = allocator.GetBlockIndex(block);
            if (block_index != brpc::SHM_INVALID_BLOCK_INDEX) {
                allocator.FreeBlock(block_index);
            }
        }
    }
    if (fd >= 0) {
        close(fd);
    }
}

void FuzzQueueMetadata(const uint8_t* data, size_t size,
                       FuzzedDataProvider* provider) {
    const bool valid_empty = HasPrefix(data, size, "VALID_QUEUE_EMPTY");
    const bool valid_full = HasPrefix(data, size, "VALID_QUEUE_FULL");
    const size_t mapping_size = (valid_empty || valid_full)
        ? 64 * 1024
        : provider->ConsumeIntegralInRange<size_t>(
              sizeof(brpc::ShmQueueHeader), 64 * 1024);
    std::vector<uint8_t> mem(mapping_size);
    CopyBytes(mem.data(), mem.size(), data, size);
    if (valid_empty) {
        InstallValidQueueHeader(mem.data(), mem.size(), false);
    } else if (valid_full) {
        InstallValidQueueHeader(mem.data(), mem.size(), true);
    }

    brpc::ShmQueue queue;
    if (queue.Open(mem.data(), mem.size()) != 0) {
        return;
    }
    const size_t offset = provider->ConsumeIntegral<size_t>();
    const uint32_t data_size = provider->ConsumeIntegral<uint32_t>();
    queue.Enqueue(offset, data_size);
    std::vector<brpc::ShmQueueElement> seen;
    queue.DequeueAndProcessBatch(ProcessQueueElement, &seen,
                                 provider->ConsumeIntegralInRange<size_t>(
                                     0, 128));
}

}  // namespace

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    if (size == 0 || size > 4096) {
        return 0;
    }
    FuzzedDataProvider provider(data, size);
    FuzzAllocatorMetadata(data, size, &provider);
    FuzzQueueMetadata(data, size, &provider);
    return 0;
}
