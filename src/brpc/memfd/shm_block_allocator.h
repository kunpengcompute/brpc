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

#ifndef BRPC_MEMFD_SHM_BLOCK_ALLOCATOR_H
#define BRPC_MEMFD_SHM_BLOCK_ALLOCATOR_H

#include <stdint.h>
#include <stddef.h>
#include <string>
#include <atomic>
#include <functional>
#include "butil/iobuf.h"

namespace brpc {

static const uint32_t SHM_MAGIC = 0x4D53484D;
static const uint32_t SHM_QUEUE_MAGIC = 0x4D534D51;
static const uint16_t SHM_VERSION_V1 = 1;
static const uint16_t SHM_VERSION_CURRENT = SHM_VERSION_V1;

struct ShmBlockState {
    std::atomic<int32_t>  pending_count;
    std::atomic<uint8_t>  write_complete;
    uint8_t               _pad[3];
};

struct ShmBufferHeader {
    uint32_t              magic;
    uint16_t              version;
    uint16_t              header_size;
    uint16_t              block_state_size;
    uint16_t              feature_flags;
    uint32_t              block_size;
    uint32_t              block_count;
    std::atomic<uint32_t> free_head;
    std::atomic<uint32_t> free_tail;
    std::atomic<int32_t>  free_count;
    std::atomic<uint32_t> free_notify;
    std::atomic<uint8_t>  initialized;
    uint8_t               reserved[3];
};

static_assert(sizeof(ShmBlockState) == 8, "ShmBlockState must be 8 bytes");
static_assert(sizeof(ShmBufferHeader) == 40, "ShmBufferHeader must be 40 bytes");

static const uint32_t SHM_INVALID_BLOCK_INDEX = 0xFFFFFFFFU;

class ShmBlockAllocator {
public:
    ShmBlockAllocator();
    ~ShmBlockAllocator();

    int Init(const std::string& name, size_t total_size);
    int OpenFromFd(int fd, size_t size);

    butil::IOBuf::Block* TryAllocBlock();
    void FreeBlock(uint32_t block_index);

    void MarkWriteComplete(uint32_t block_index);
    inline int32_t IncPendingCount(uint32_t block_index) {
        return _block_states[block_index].pending_count.fetch_add(1, std::memory_order_acq_rel);
    }
    inline int32_t DecPendingCount(uint32_t block_index) {
        return _block_states[block_index].pending_count.fetch_sub(1, std::memory_order_acq_rel);
    }

    inline bool IsWriteComplete(uint32_t block_index) {
        return _block_states[block_index].write_complete.load(std::memory_order_acquire) != 0;
    }

    inline size_t GetDataOffset(const char* addr) const {
        return addr - _block_region;
    }

    inline uint32_t GetBlockIndexFromOffset(size_t offset) const {
        return offset / _header->block_size;
    }

    inline char* GetDataPtrFromOffset(size_t offset) const {
        return reinterpret_cast<char*>(_block_region) + offset;
    }

    inline uint32_t GetBlockIndex(size_t offset) const {
        return static_cast<uint32_t>(offset / _header->block_size);
    }

    inline uint32_t GetBlockIndex(const butil::IOBuf::Block* block) const {
        const char* addr = block->data;
        if (addr < _block_region) return SHM_INVALID_BLOCK_INDEX;
        size_t offset = addr - _block_region;
        return static_cast<uint32_t>(offset / _header->block_size);
    }

    inline bool IsWritable() const {
        return _header->free_count.load(std::memory_order_acquire) > 1;
    }

    int memfd() const { return _memfd; }
    int notify_fd() const { return _notify_fd; }
    void set_notify_fd(int fd) { _notify_fd = fd; }
    void NotifyWaiter();
    int WaitNotify(const timespec* abstime = nullptr);
    bool IsWaitNotify() { return (_header->free_notify.load(std::memory_order_acquire) > 0); }

    uint16_t peer_version() const { return _peer_version; }
    uint16_t peer_features() const { return _peer_features; }

private:
    void InitPointers();

    ShmBufferHeader* _header;
    ShmBlockState*   _block_states;
    char*            _block_region;
    size_t           _header_size;
    size_t           _size;
    int              _memfd;
    void*            _base;
    int              _notify_fd;
    uint16_t         _peer_version;
    uint16_t         _peer_features;
};

class ShmBlockRecycler {
public:
    ShmBlockRecycler(ShmBlockAllocator* allocator, uint32_t block_index)
        : _allocator(allocator), _block_index(block_index) {}

    void operator()(void* data);

private:
    ShmBlockAllocator* _allocator;
    uint32_t _block_index;
};

ShmBlockAllocator *GetShmAllocator();
int InitShmAllocator();

} // namespace brpc

#endif // BRPC_MEMFD_SHM_BLOCK_ALLOCATOR_H
