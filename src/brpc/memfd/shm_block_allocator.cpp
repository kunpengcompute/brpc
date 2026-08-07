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

#include "brpc/memfd/shm_block_allocator.h"
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/mman.h>
#include <sys/syscall.h>
#include <unistd.h>
#include <errno.h>
#include <cstring>
#include <limits>
#include <linux/futex.h>
#include "butil/logging.h"
#include "butil/errno.h"
#include "butil/iobuf_inl.h"
#include "bthread/unstable.h"
#include <gflags/gflags.h>

namespace brpc {

DEFINE_uint64(shm_buff_size, 1L * 1024 * 1024 * 1024, "Maximum size of shared memory buffer");
DEFINE_int64(shm_block_size, 8 * 1024,
             "Max unwritten bytes in each socket, if the limit is reached,"
             " Socket.Write fails with EOVERCROWDED");

static ShmBlockAllocator *g_shm_allocator = nullptr;
static pthread_mutex_t g_shm_init_mutex = PTHREAD_MUTEX_INITIALIZER;
static bool g_shm_initialized = false;

namespace {

bool CheckedMul(size_t a, size_t b, size_t* result) {
    if (a != 0 && b > std::numeric_limits<size_t>::max() / a) {
        return false;
    }
    *result = a * b;
    return true;
}

bool CheckedAdd(size_t a, size_t b, size_t* result) {
    if (b > std::numeric_limits<size_t>::max() - a) {
        return false;
    }
    *result = a + b;
    return true;
}

bool ValidateShmBufferHeader(const ShmBufferHeader* header, size_t size) {
    if (!header || header->magic != SHM_MAGIC ||
        header->version == 0 || header->version > SHM_VERSION_CURRENT ||
        header->header_size < sizeof(ShmBufferHeader) ||
        header->header_size > size ||
        header->block_state_size != sizeof(ShmBlockState) ||
        header->block_size <= sizeof(butil::IOBuf::Block) ||
        header->block_count == 0) {
        return false;
    }

    size_t states_size = 0;
    size_t header_size = 0;
    if (!CheckedMul(header->block_count, header->block_state_size,
                    &states_size) ||
        !CheckedAdd(header->header_size, states_size, &header_size) ||
        header_size > size) {
        return false;
    }

    size_t block_region_size = 0;
    if (!CheckedMul(header->block_count, header->block_size,
                    &block_region_size) ||
        block_region_size > size - header_size) {
        return false;
    }

    const uint32_t free_head =
        header->free_head.load(std::memory_order_acquire);
    const uint32_t free_tail =
        header->free_tail.load(std::memory_order_acquire);
    const int32_t free_count =
        header->free_count.load(std::memory_order_acquire);
    const uint8_t initialized =
        header->initialized.load(std::memory_order_acquire);
    return free_head < header->block_count &&
           free_tail < header->block_count &&
           free_count >= 0 &&
           static_cast<uint32_t>(free_count) <= header->block_count &&
           initialized <= 1;
}

}  // namespace

ShmBlockAllocator::ShmBlockAllocator()
    : _header(nullptr),
      _block_states(nullptr),
      _block_region(nullptr),
      _header_size(0),
      _size(0),
      _memfd(-1),
      _base(nullptr),
      _notify_fd(-1),
      _peer_version(0),
      _peer_features(0) {
}

ShmBlockAllocator::~ShmBlockAllocator() {
    if (_base && _base != MAP_FAILED) {
        munmap(_base, _size);
        _base = nullptr;
    }
    if (_memfd >= 0) {
        close(_memfd);
        _memfd = -1;
    }
    if (_notify_fd >= 0) {
        close(_notify_fd);
        _notify_fd = -1;
    }
}

ShmBlockAllocator *GetShmAllocator() {
    return g_shm_allocator;
}

int InitShmAllocator() {
    if (g_shm_initialized) {
        return 0;
    }
    pthread_mutex_lock(&g_shm_init_mutex);
    if (g_shm_initialized) {
        pthread_mutex_unlock(&g_shm_init_mutex);
        return 0;
    }

    g_shm_allocator = new (std::nothrow)ShmBlockAllocator();
    if (g_shm_allocator == nullptr) {
        LOG(ERROR) << "Failed to allocate ShmBlockAllocator";
        pthread_mutex_unlock(&g_shm_init_mutex);
        return -1;
    }

    std::string shm_name = "brpc_shm_send_" + std::to_string(getpid());
    int rc = g_shm_allocator->Init(shm_name, FLAGS_shm_buff_size);
    if (rc != 0) {
        delete g_shm_allocator;
        g_shm_allocator = nullptr;
        pthread_mutex_unlock(&g_shm_init_mutex);
        LOG(ERROR) << "Failed to init ShmBlockAllocator";
        return -1;
    }

    g_shm_initialized = true;
    pthread_mutex_unlock(&g_shm_init_mutex);
    return 0;
}

void ShmBlockAllocator::InitPointers() {
    _header = reinterpret_cast<ShmBufferHeader*>(_base);
    uint32_t hsz = _header->header_size;
    uint32_t bssz = _header->block_state_size;
    _block_states = reinterpret_cast<ShmBlockState*>(
        reinterpret_cast<char*>(_base) + hsz);
    _header_size = hsz + _header->block_count * bssz;
    _block_region = reinterpret_cast<char*>(_base) + _header_size;
}

int ShmBlockAllocator::Init(const std::string& name, size_t total_size) {
    if (FLAGS_shm_block_size <=
            static_cast<int64_t>(sizeof(butil::IOBuf::Block)) ||
        total_size <= sizeof(ShmBufferHeader)) {
        LOG(ERROR) << "ShmBlockAllocator: invalid block or buffer size";
        return -1;
    }
    uint32_t block_count = static_cast<uint32_t>(
        (total_size - sizeof(ShmBufferHeader)) /
        (sizeof(ShmBlockState) + FLAGS_shm_block_size));
    if (block_count == 0) {
        LOG(ERROR) << "ShmBlockAllocator: total_size too small";
        return -1;
    }

    int notify_fd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (notify_fd < 0) {
        LOG(ERROR) << "Failed to create notify eventfd for g_shm_allocator";
        return -1;
    }

    size_t actual_size = sizeof(ShmBufferHeader) +
        block_count * sizeof(ShmBlockState) +
        block_count * FLAGS_shm_block_size;

    _memfd = syscall(SYS_memfd_create, name.c_str(), MFD_CLOEXEC | MFD_ALLOW_SEALING);
    if (_memfd < 0) {
        LOG(ERROR) << "ShmBlockAllocator: memfd_create failed, name=" << name;
        close(notify_fd);
        return -1;
    }

    if (ftruncate(_memfd, actual_size) < 0) {
        LOG(ERROR) << "ShmBlockAllocator: ftruncate failed";
        close(_memfd);
        close(notify_fd);
        _memfd = -1;
        return -1;
    }

    _base = mmap(nullptr, actual_size, PROT_READ | PROT_WRITE, MAP_SHARED, _memfd, 0);
    if (_base == MAP_FAILED) {
        LOG(ERROR) << "ShmBlockAllocator: mmap failed";
        close(_memfd);
        _memfd = -1;
        _base = nullptr;
        close(notify_fd);
        return -1;
    }

    _size = actual_size;
    _notify_fd = notify_fd;

    _header = reinterpret_cast<ShmBufferHeader*>(_base);
    _header->magic = SHM_MAGIC;
    _header->version = SHM_VERSION_CURRENT;
    _header->header_size = static_cast<uint16_t>(sizeof(ShmBufferHeader));
    _header->block_state_size = static_cast<uint16_t>(sizeof(ShmBlockState));
    _header->feature_flags = 0;
    _header->block_size = FLAGS_shm_block_size;
    _header->block_count = block_count;
    _header->free_head.store(0, std::memory_order_relaxed);
    _header->free_tail.store(block_count - 1, std::memory_order_relaxed);
    _header->free_count.store(static_cast<int32_t>(block_count), std::memory_order_relaxed);
    _header->free_notify.store(0, std::memory_order_relaxed);
    _header->initialized.store(0, std::memory_order_relaxed);
    memset(_header->reserved, 0, sizeof(_header->reserved));

    _block_states = reinterpret_cast<ShmBlockState*>(
        reinterpret_cast<char*>(_base) + _header->header_size);
    _header_size = _header->header_size + block_count * _header->block_state_size;
    _block_region = reinterpret_cast<char*>(_base) + _header_size;

    for (uint32_t i = 0; i < block_count; ++i) {
        _block_states[i].pending_count.store(0, std::memory_order_relaxed);
        _block_states[i].write_complete.store(0, std::memory_order_relaxed);
        _block_states[i].recycle_claimed.store(0, std::memory_order_relaxed);
        char* block_data = _block_region + i * _header->block_size;
        *reinterpret_cast<uint32_t*>(block_data) =
            i + 1 < block_count ? i + 1 : SHM_INVALID_BLOCK_INDEX;
    }
    _header->initialized.store(1, std::memory_order_release);

    return 0;
}

int ShmBlockAllocator::OpenFromFd(int fd, size_t size) {
    if (fd < 0 || size < sizeof(ShmBufferHeader)) {
        return -1;
    }

    _base = mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (_base == MAP_FAILED) {
        LOG(ERROR) << "ShmBlockAllocator: OpenFromFd mmap failed";
        return -1;
    }

    _header = reinterpret_cast<ShmBufferHeader*>(_base);
    if (!ValidateShmBufferHeader(_header, size)) {
        LOG(ERROR) << "ShmBlockAllocator: invalid shared memory header";
        munmap(_base, size);
        _base = nullptr;
        _header = nullptr;
        return -1;
    }

    _peer_version = _header->version;
    _peer_features = _header->feature_flags;

    _memfd = fd;
    _size = size;
    InitPointers();
    return 0;
}

butil::IOBuf::Block* ShmBlockAllocator::TryAllocBlock() {
    int32_t old_free_count = _header->free_count.fetch_sub(1, std::memory_order_acquire);
    if (old_free_count <= 1) {
        _header->free_count.fetch_add(1, std::memory_order_relaxed);
        return nullptr;
    }

    while (true) {
        uint32_t old_head = _header->free_head.load(std::memory_order_acquire);
        if (old_head >= _header->block_count) {
            LOG_FIRST_N(WARNING, 1) << "Get BLOCK ERR:" << old_head;
            _header->free_count.fetch_add(1, std::memory_order_relaxed);
            return nullptr;
        }

        char* block_data = _block_region + old_head * _header->block_size;
        uint32_t new_head = *reinterpret_cast<uint32_t*>(block_data);
        if (new_head == SHM_INVALID_BLOCK_INDEX) {
            // FreeBlock publishes the new tail before linking the previous
            // tail to it. Retry until that link becomes visible.
            continue;
        }

        if (_header->free_head.compare_exchange_weak(old_head, new_head,
                std::memory_order_acq_rel, std::memory_order_acquire)) {
            _block_states[old_head].write_complete.store(0, std::memory_order_relaxed);
            _block_states[old_head].pending_count.store(0, std::memory_order_relaxed);
            _block_states[old_head].recycle_claimed.store(0, std::memory_order_relaxed);
            char* data_area = block_data + sizeof(butil::IOBuf::Block);
            uint32_t data_cap = _header->block_size - sizeof(butil::IOBuf::Block);

            butil::IOBuf::Block* b = new (block_data) butil::IOBuf::Block(
                data_area, data_cap, butil::IOBUF_BLOCK_FLAGS_SHM);
            return b;
        }
    }
}

bool ShmBlockAllocator::IsValidDataRange(size_t offset, size_t size) const {
    if (!_header || !_block_region) {
        return false;
    }
    size_t data_region_size = 0;
    if (!CheckedMul(_header->block_count, _header->block_size,
                    &data_region_size) ||
        offset > data_region_size ||
        size > data_region_size - offset) {
        return false;
    }
    if (size == 0) {
        return true;
    }
    const size_t start_block = offset / _header->block_size;
    const size_t end_block = (offset + size - 1) / _header->block_size;
    return start_block == end_block;
}

void ShmBlockAllocator::FreeBlock(uint32_t block_index) {
    if (!_header || block_index >= _header->block_count) return;

    char* block_data = _block_region + block_index * _header->block_size;
    *reinterpret_cast<uint32_t*>(block_data) = SHM_INVALID_BLOCK_INDEX;

    while (true) {
        uint32_t old_tail = _header->free_tail.load(std::memory_order_acquire);

        if (_header->free_tail.compare_exchange_weak(old_tail, block_index,
                std::memory_order_acq_rel, std::memory_order_acquire)) {
            char* tail_block_data = _block_region + old_tail * _header->block_size;
            *reinterpret_cast<uint32_t*>(tail_block_data) = block_index;
            _header->free_count.fetch_add(1, std::memory_order_release);
            break;
        }
    }

    if (_header->free_notify.load(std::memory_order_acquire) > 0) {
        NotifyWaiter();
    }
}

void ShmBlockAllocator::MarkWriteComplete(uint32_t block_index) {
    if (!_header || block_index >= _header->block_count) return;

    _block_states[block_index].write_complete.store(1, std::memory_order_release);
    TryRecycleBlock(block_index);
}

bool ShmBlockAllocator::TryRecycleBlock(uint32_t block_index) {
    if (!_header || block_index >= _header->block_count) {
        return false;
    }
    ShmBlockState& state = _block_states[block_index];
    if (state.pending_count.load(std::memory_order_acquire) != 0 ||
        state.write_complete.load(std::memory_order_acquire) == 0) {
        return false;
    }
    uint8_t expected = 0;
    if (state.recycle_claimed.compare_exchange_strong(
            expected, 1, std::memory_order_acq_rel,
            std::memory_order_acquire)) {
        FreeBlock(block_index);
        return true;
    }
    return false;
}

void ShmBlockAllocator::NotifyWaiter() {
    uint64_t notify = 1;
    ssize_t ret = write(_notify_fd, &notify, sizeof(notify));
    if (ret < 0 && errno != EAGAIN) {
        LOG(WARNING) << "Failed to notify waiter: " << berror();
    }
}

int ShmBlockAllocator::WaitNotify(const timespec* abstime) {
    uint64_t val;

    _header->free_notify.fetch_add(1, std::memory_order_release);
    if (_header->free_count.load(std::memory_order_acquire) > 1) {
        _header->free_notify.fetch_sub(1, std::memory_order_release);
        return 0;
    }

    int ret = bthread_fd_timedwait(_notify_fd, EPOLLIN, abstime);
    _header->free_notify.fetch_sub(1, std::memory_order_release);
    if (ret < 0) {
        return -1;
    }

    const ssize_t nread = read(_notify_fd, &val, sizeof(val));
    if (nread < 0 && errno != EAGAIN) {
        LOG(WARNING) << "Failed to consume allocator notification: " << berror();
        return -1;
    }

    return 0;
}

void ShmBlockRecycler::operator()(void* data) {
    int32_t remaining = _allocator->DecPendingCount(_block_index);
    if (remaining == 1) {
        _allocator->TryRecycleBlock(_block_index);
    }
}

} // namespace brpc
