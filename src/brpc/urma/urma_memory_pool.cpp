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

#include "brpc/urma/urma_memory_pool.h"
#include <gflags/gflags.h>

#if BRPC_WITH_URMA
#include <sys/mman.h>
#include <pthread.h>
#include <atomic>
#include <algorithm>
#include <cerrno>
#include <cstdlib>
#include <functional>
#include <limits>
#include <string>
#include <vector>
#include "butil/iobuf.h"
#include "butil/logging.h"
#include "butil/scoped_lock.h"
#include "butil/thread_local.h"
#include "butil/synchronization/lock.h"

namespace butil {
namespace iobuf {
extern void* (*blockmem_allocate)(size_t);
extern void (*blockmem_deallocate)(void*);
}
}

namespace brpc {
namespace urma {
DECLARE_int32(urma_large_block_threshold);
namespace {
const size_t kShards = 64;
class RegisteredPool;
// Registration/destruction and thread exit use this lock. Allocation hits only
// touch the owning pthread's Cache, without acquiring this lock.
struct PoolCacheState {
    butil::Mutex mutex;
    RegisteredPool* owner{nullptr};
    struct Cache {
        explicit Cache(std::shared_ptr<PoolCacheState> s) : state(std::move(s)) {}
        ~Cache();
        std::shared_ptr<PoolCacheState> state;
        std::vector<void*> blocks;
        size_t limit{0};
    };
    std::vector<Cache*> caches;
    size_t reserved_capacity{0};
};
struct ThreadPoolCaches {
    std::vector<std::unique_ptr<PoolCacheState::Cache>> entries;
};
static __thread ThreadPoolCaches* tls_pool_caches = nullptr;
static __thread bool tls_pool_caches_retired = false;
static void ReleaseThreadPoolCaches(void*) {
    // IOBuf/object-pool exit callbacks may still return memory after this one.
    // Disable caching first so those returns never touch a destroyed TLS list.
    tls_pool_caches_retired = true;
    auto* caches = tls_pool_caches;
    tls_pool_caches = nullptr;
    delete caches;
}
class RegisteredPool : public UrmaMemoryPool {
public:
    RegisteredPool(size_t size, size_t count)
        : _cache_state(new PoolCacheState), _block_size(size), _count(count) {
        _cache_state->owner = this;
        if (size == 64) { _cache_limit = 256; _cache_batch = 32; }
        else if (size == 4096) { _cache_limit = 64; _cache_batch = 16; }
        else if (size == 8192) { _cache_limit = 128; _cache_batch = 32; }
        else if (size >= 65536) { _cache_limit = 32; _cache_batch = 8; }
        // Tiny pools must leave enough blocks available to other pthreads.
        _cache_limit = std::min(_cache_limit, count / kShards);
        _cache_batch = std::min(_cache_batch, _cache_limit);
    }
    ~RegisteredPool() override {
        FlushThreadCaches();
        {
            BAIDU_SCOPED_LOCK(_cache_state->mutex);
            _cache_state->owner = nullptr;
        }
        // Never invalidate DMA or application-owned blocks on a failed cleanup.
        if (_outstanding.load() == 0 && Unregister() && _base) {
            munmap(_base, _block_size * _count);
        }
    }
    bool Init(urma_context_t* context) {
        if (!context || !_count || _block_size < sizeof(void*) ||
            _block_size % alignof(void*) ||
            _count > std::numeric_limits<size_t>::max() / _block_size) {
            errno = EINVAL;
            return false;
        }
        _base = mmap(nullptr, _block_size * _count, PROT_READ | PROT_WRITE,
                     MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
        if (_base == MAP_FAILED) {
            const int error = errno;
            LOG(ERROR) << "URMA registered pool mmap failed: block_size=" << _block_size
                       << " block_count=" << _count << " bytes=" << _block_size * _count
                       << " errno=" << error;
            _base = nullptr;
            errno = error;
            return false;
        }
        urma_seg_cfg_t cfg{};
        cfg.va = reinterpret_cast<uint64_t>(_base);
        cfg.len = _block_size * _count;
        cfg.flag.bs.token_policy = URMA_TOKEN_NONE;
        cfg.flag.bs.cacheable = URMA_NON_CACHEABLE;
        cfg.flag.bs.access = URMA_ACCESS_READ | URMA_ACCESS_WRITE |
                             URMA_ACCESS_ATOMIC;
        errno = 0;
        _seg = urma_register_seg(context, &cfg);
        if (!_seg) {
            const int error = errno ? errno : EIO;
            LOG(ERROR) << "URMA registered pool urma_register_seg failed: block_size="
                       << _block_size << " block_count=" << _count
                       << " bytes=" << cfg.len << " errno=" << error;
            errno = error;
            return false;
        }
        _used.assign(_count, 0);
        for (size_t i = 0; i < _count; ++i) {
            _free[i % kShards].push_back(static_cast<char*>(_base) +
                                          i * _block_size);
        }
        return true;
    }
    void* Allocate(size_t size) override {
        if (!_seg) { errno = ENODEV; return nullptr; }
        if (size > _block_size) {
            errno = EMSGSIZE;
            return nullptr;
        }
        auto* cache = LocalCache();
        if (cache && !cache->blocks.empty()) {
            void* p = cache->blocks.back();
            cache->blocks.pop_back();
            _used[Index(p)] = 1;
            _outstanding.fetch_add(1, std::memory_order_relaxed);
            return p;
        }
        const size_t start = std::hash<pthread_t>()(pthread_self()) % kShards;
        for (size_t i = 0; i < kShards; ++i) {
            const size_t shard = (start + i) % kShards;
            BAIDU_SCOPED_LOCK(_mutex[shard]);
            if (_free[shard].empty()) { continue; }
            void* p = _free[shard].back();
            _free[shard].pop_back();
            if (cache) {
                // Count the returned block as part of the refill batch.
                for (size_t n = 1; n < std::min(_cache_batch, cache->limit) && !_free[shard].empty(); ++n) {
                    cache->blocks.push_back(_free[shard].back());
                    _free[shard].pop_back();
                }
            }
            _used[Index(p)] = 1;
            _outstanding.fetch_add(1, std::memory_order_relaxed);
            return p;
        }
        errno = ENOMEM;
        return nullptr;
    }
    void Deallocate(void* p) override {
        if (!p) {
            return;
        }
        CHECK(Owns(p));
        CHECK_EQ((reinterpret_cast<uintptr_t>(p) -
                  reinterpret_cast<uintptr_t>(_base)) % _block_size, 0u);
        const size_t index = Index(p);
        CHECK(_used[index]);
        _used[index] = 0;
        // After Unregister, late IOBuf returns must bypass TLS.
        auto* cache = _seg ? LocalCache() : nullptr;
        if (cache) {
            if (cache->blocks.size() >= cache->limit) {
                ReturnCached(cache, std::min(_cache_batch, cache->limit));
            }
            cache->blocks.push_back(p);
        } else {
            BAIDU_SCOPED_LOCK(_mutex[index % kShards]);
            _free[index % kShards].push_back(p);
        }
        _outstanding.fetch_sub(1, std::memory_order_release);
    }
    void FlushThreadCaches() override {
        // The caller must quiesce allocation/free before pool teardown.
        // The registry lock serializes teardown with pthread exit only.
        BAIDU_SCOPED_LOCK(_cache_state->mutex);
        for (auto* cache : _cache_state->caches) {
            ReturnCached(cache, cache->blocks.size());
        }
    }
    void ReturnCached(PoolCacheState::Cache* cache, size_t count) {
        // Group the returned suffix by shard to acquire each mutex once.
        const size_t begin = cache->blocks.size() - count;
        auto first = cache->blocks.begin() + begin;
        std::sort(first, cache->blocks.end(), [this](void* a, void* b) {
            return Index(a) % kShards < Index(b) % kShards;
        });
        while (cache->blocks.size() > begin) {
            const size_t shard = Index(cache->blocks.back()) % kShards;
            BAIDU_SCOPED_LOCK(_mutex[shard]);
            do {
                _free[shard].push_back(cache->blocks.back());
                cache->blocks.pop_back();
            } while (cache->blocks.size() > begin &&
                     Index(cache->blocks.back()) % kShards == shard);
        }
    }
    bool Owns(const void* p) const override {
        const uintptr_t base = reinterpret_cast<uintptr_t>(_base);
        const uintptr_t addr = reinterpret_cast<uintptr_t>(p);
        return _base && addr >= base && addr - base < _block_size * _count;
    }
    urma_target_seg_t* SegmentFor(const void* p) const override {
        return !p || Owns(p) ? _seg : nullptr;
    }
    size_t outstanding() const override { return _outstanding.load(); }
    bool Unregister() override {
        FlushThreadCaches();
        if (_seg && urma_unregister_seg(_seg) != URMA_SUCCESS) {
            return false;
        }
        _seg = nullptr;
        return true;
    }
private:
    PoolCacheState::Cache* LocalCache() {
        if (!_cache_limit || tls_pool_caches_retired) { return nullptr; }
        if (!tls_pool_caches) {
            std::unique_ptr<ThreadPoolCaches> caches(new ThreadPoolCaches);
            if (butil::thread_atexit(ReleaseThreadPoolCaches, nullptr) != 0) {
                return nullptr;
            }
            tls_pool_caches = caches.release();
        }
        auto& local = tls_pool_caches->entries;
        for (const auto& cache : local) {
            if (cache->state.get() == _cache_state.get()) {
                return cache->limit ? cache.get() : nullptr;
            }
        }
        std::unique_ptr<PoolCacheState::Cache> cache(new PoolCacheState::Cache(_cache_state));
        auto* result = cache.get();
        {
            BAIDU_SCOPED_LOCK(_cache_state->mutex);
            // Bound aggregate idle-cache capacity to half the pool. Threads
            // without a cache quota keep using the shared pool, which retains
            // capacity even with hundreds of long-lived worker pthreads.
            const size_t quota = std::min(_cache_limit,
                _count / 2 - _cache_state->reserved_capacity);
            cache->blocks.reserve(quota);
            _cache_state->caches.push_back(result);
            cache->limit = quota;
            _cache_state->reserved_capacity += quota;
        }
        local.push_back(std::move(cache));
        return result->limit ? result : nullptr;
    }
    size_t _cache_limit{0};
    size_t _cache_batch{0};
    std::shared_ptr<PoolCacheState> _cache_state;
    size_t Index(const void* p) const {
        return (reinterpret_cast<uintptr_t>(p) -
                reinterpret_cast<uintptr_t>(_base)) / _block_size;
    }
    size_t _block_size;
    size_t _count;
    void* _base{nullptr};
    urma_target_seg_t* _seg{nullptr};
    butil::Mutex _mutex[kShards];
    std::vector<void*> _free[kShards];
    std::vector<char> _used;
    std::atomic<size_t> _outstanding{0};
};

PoolCacheState::Cache::~Cache() {
    BAIDU_SCOPED_LOCK(state->mutex);
    if (state->owner) { state->owner->ReturnCached(this, blocks.size()); }
    state->reserved_capacity -= limit;
    auto& entries = state->caches;
    entries.erase(std::remove(entries.begin(), entries.end(), this), entries.end());
}

// These allocators intentionally live until process exit: IOBuf has TLS block
// caches. On rollback unregister DMA but retain mappings and the deallocator
// while any application/TLS block still refers to them.
UrmaMemoryPoolFactory g_factory = CreateUrmaMemoryPool;
UrmaMemoryPool* g_data_pool = nullptr;
UrmaMemoryPool* g_header_pool = nullptr;
UrmaMemoryPool* g_ctp_pool = nullptr;
UrmaMemoryPool* g_large_pool = nullptr;
size_t g_data_block_size = 0;
size_t g_large_block_size = 0;
void* (*g_original_allocate)(size_t) = nullptr;
void (*g_original_deallocate)(void*) = nullptr;
size_t g_original_block_size = 0;
void* AllocateIOBuf(size_t size) {
    // Explicit CTP blocks must remain registered; never fall back to malloc.
    if (size == URMA_CTP_BLOCK_SIZE && g_ctp_pool) { return AllocCtpBuffer(); }
    if (g_large_pool && size == g_large_block_size) { return g_large_pool->Allocate(size); }
    void* p = g_data_pool->Allocate(size);
    return p ? p : g_original_allocate(size);
}
void DeallocateIOBuf(void* p) {
    if (g_large_pool && g_large_pool->Owns(p)) {
        g_large_pool->Deallocate(p);
    } else if (g_ctp_pool && g_ctp_pool->Owns(p)) {
        g_ctp_pool->Deallocate(p);
    } else if (g_data_pool->Owns(p)) {
        g_data_pool->Deallocate(p);
    } else {
        g_original_deallocate(p);
    }
}
}  // namespace

std::unique_ptr<UrmaMemoryPool> CreateUrmaMemoryPool(
        urma_context_t* context, size_t size, size_t count) {
    std::unique_ptr<RegisteredPool> pool(new RegisteredPool(size, count));
    if (!pool->Init(context)) {
        const int error = errno;
        pool.reset();
        errno = error;
        return nullptr;
    }
    return std::unique_ptr<UrmaMemoryPool>(pool.release());
}

bool SetUrmaMemoryPoolFactory(UrmaMemoryPoolFactory factory) {
    if (g_data_pool || g_header_pool || g_ctp_pool || g_large_pool) { errno = EBUSY; return false; }
    g_factory = factory ? factory : CreateUrmaMemoryPool;
    return true;
}
bool InitializeUrmaMemoryPools(urma_context_t* context, size_t size,
                               size_t count, size_t headers, size_t ctp_count,
                               size_t large_count, size_t large_size) {
    if (g_data_pool || g_header_pool || g_ctp_pool || g_large_pool) {
        errno = EALREADY;
        return false;
    }
    auto create = [&](const char* name, const char* count_flag,
                      size_t block_size, size_t block_count) {
        auto pool = g_factory(context, block_size, block_count);
        if (!pool) {
            const int error = errno;
            const std::string bytes = block_size &&
                block_count > std::numeric_limits<size_t>::max() / block_size
                ? "overflow" : std::to_string(block_size * block_count);
            LOG(ERROR) << "Failed to initialize URMA registered pool: pool=" << name
                       << " block_size=" << block_size << " " << count_flag << "="
                       << block_count << " bytes=" << bytes << " errno=" << error;
            errno = error;
        }
        return pool;
    };
    auto data = create("data", "urma_buffer_count", size, count);
    auto header = create("header", "urma_header_buf_count", 64, headers);
    auto ctp = create("ctp", "urma_ctp_buf_count", URMA_CTP_BLOCK_SIZE, ctp_count);
    auto large = large_count
        ? create("large", "urma_large_buffer_count", large_size, large_count) : nullptr;
    if (!data || !header || !ctp || (large_count && !large)) {
        return false;
    }
    return InstallUrmaMemoryPools(std::move(data), std::move(header),
                                  std::move(ctp), size, std::move(large), large_size);
}

bool InstallUrmaMemoryPools(std::unique_ptr<UrmaMemoryPool> data,
                            std::unique_ptr<UrmaMemoryPool> header,
                            std::unique_ptr<UrmaMemoryPool> ctp, size_t size,
                            std::unique_ptr<UrmaMemoryPool> large, size_t large_size) {
    if (g_data_pool || g_header_pool || g_ctp_pool || g_large_pool || !data || !header || !ctp ||
        size <= sizeof(butil::IOBuf::Block) || (large && large_size <= size)) {
        errno = EINVAL;
        return false;
    }
    g_data_pool = data.release();
    g_header_pool = header.release();
    g_ctp_pool = ctp.release();
    g_large_pool = large.release();
    g_data_block_size = size;
    g_large_block_size = large_size;
    g_original_allocate = butil::iobuf::blockmem_allocate;
    g_original_deallocate = butil::iobuf::blockmem_deallocate;
    g_original_block_size = butil::GetDefaultBlockSize();
    butil::iobuf::blockmem_allocate = AllocateIOBuf;
    butil::iobuf::blockmem_deallocate = DeallocateIOBuf;
    butil::SetDefaultBlockSize(size);
    return true;
}

bool ReleaseUrmaMemoryPools() {
    // All data-path users must already have stopped, as for Unregister.
    for (auto* pool : {g_ctp_pool, g_header_pool, g_data_pool, g_large_pool}) {
        if (pool) { pool->FlushThreadCaches(); }
    }
    if (g_large_pool) {
        if (g_large_pool->outstanding() != 0 || !g_large_pool->Unregister()) return false;
        delete g_large_pool;
        g_large_pool = nullptr;
    }
    // Delivered IOBufs may still own CTP blocks after queues have stopped.
    // Their deleters require both the pool object and its registration.
    if (g_ctp_pool) {
        if (g_ctp_pool->outstanding() != 0 || !g_ctp_pool->Unregister()) {
            return false;
        }
        delete g_ctp_pool;
        g_ctp_pool = nullptr;
    }
    if (g_header_pool) {
        if (g_header_pool->outstanding() != 0 || !g_header_pool->Unregister()) {
            return false;
        }
        delete g_header_pool;
        g_header_pool = nullptr;
    }
    if (g_data_pool) {
        if (!g_data_pool->Unregister()) { return false; }
        butil::SetDefaultBlockSize(g_original_block_size);
        if (g_data_pool->outstanding() == 0) {
            butil::iobuf::blockmem_allocate = g_original_allocate;
            butil::iobuf::blockmem_deallocate = g_original_deallocate;
            delete g_data_pool;
            g_data_pool = nullptr;
        }
    }
    return true;
}
void* AllocHeaderBuffer() {
    return g_header_pool ? g_header_pool->Allocate(64) : nullptr;
}
void FreeHeaderBuffer(void* p) {
    if (p) { g_header_pool->Deallocate(p); }
}
urma_target_seg_t* GetHeaderPoolSegFor(void* p) {
    return g_header_pool ? g_header_pool->SegmentFor(p) : nullptr;
}
void* AllocCtpBuffer() {
    if (!g_ctp_pool) { errno = ENODEV; return nullptr; }
    return g_ctp_pool->Allocate(URMA_CTP_BLOCK_SIZE);
}
void FreeCtpBuffer(void* p) {
    if (p) { g_ctp_pool->Deallocate(p); }
}
urma_target_seg_t* GetCtpPoolSegFor(void* p) {
    return g_ctp_pool ? g_ctp_pool->SegmentFor(p) : nullptr;
}
urma_target_seg_t* GetPooledMemorySegFor(void* p) {
    // nullptr keeps advertising the standard pool in the handshake. Address
    // lookup includes CTP buffers, so echoing received data stays zero-copy.
    if (p && g_large_pool && g_large_pool->Owns(p)) {
        return g_large_pool->SegmentFor(p);
    }
    if (p && g_ctp_pool && g_ctp_pool->Owns(p)) {
        return g_ctp_pool->SegmentFor(p);
    }
    return g_data_pool ? g_data_pool->SegmentFor(p) : nullptr;
}
size_t GetUrmaSerializationBlockSize(size_t body_size) {
    return HasUrmaLargeBlockPool() && body_size > static_cast<size_t>(FLAGS_urma_large_block_threshold)
        ? g_large_block_size : 0;
}
size_t GetUrmaBlockSizeForPayload(size_t payload_size) {
    const size_t header = sizeof(butil::IOBuf::Block);
    if (g_data_pool && g_data_pool->SegmentFor(nullptr) &&
        g_data_block_size > header && payload_size <= g_data_block_size - header) return g_data_block_size;
    if (HasUrmaLargeBlockPool() && g_large_block_size > header &&
        payload_size <= g_large_block_size - header) return g_large_block_size;
    return 0;
}
size_t GetUrmaLargeBlockSize() { return g_large_block_size; }
bool HasUrmaLargeBlockPool() {
    return g_large_pool && g_large_pool->SegmentFor(nullptr);
}
std::vector<urma_seg_t> GetUrmaPoolSegments() {
    std::vector<urma_seg_t> result;
    for (auto* pool : {g_data_pool, g_ctp_pool, g_large_pool}) {
        if (pool && pool->SegmentFor(nullptr)) result.push_back(pool->SegmentFor(nullptr)->seg);
    }
    return result;
}
}  // namespace urma
}  // namespace brpc
#endif
