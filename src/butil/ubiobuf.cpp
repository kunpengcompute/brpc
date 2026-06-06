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

#include <algorithm>
#include <cstring>
#include <stdexcept>                       // std::invalid_argument
#include "butil/logging.h"
#include "butil/ubiobuf.h"
#include "butil/fd_guard.h"                 // butil::fd_guard
#ifdef BRPC_WITH_URMA
#include "include/ubsocket_def.h"
#include "include/ubsocket.h"
#endif

namespace brpc {
    DECLARE_string(ubsocket_block_type);
} // namespace brpc

namespace butil {
using brpc::FLAGS_ubsocket_block_type;   

const UBIOBuf::Area UBIOBuf::INVALID_AREA;

UBIOBuf::UBIOBuf(const Movable& rhs) {
    reset_block_ref(_sv.refs[0]);
    reset_block_ref(_sv.refs[1]);
    append(rhs);
}

namespace ubiobuf {

struct TLSData {
    // Head of the UB TLS block chain.
    IOBuf::Block* block_head;

    // Number of UB TLS blocks.
    int num_blocks;

    // True if the UB TLS block chain is registered to the thread.
    bool registered;
};

static __thread TLSData g_ub_data = { NULL, 0, false };

void* ub_blockmem_allocate(size_t size) {
#if BRPC_WITH_URMA
    return ubsocket_iobuf_allocate(size);
#else
    (void)size;
    return NULL;
#endif
}

void ub_blockmem_deallocate(void* p) {
#if BRPC_WITH_URMA
    ubsocket_iobuf_deallocate(p);
#else
    (void)p;
#endif
}

void* (*blockmem_allocate)(size_t) = ub_blockmem_allocate;
void (*blockmem_deallocate)(void*) = ub_blockmem_deallocate;

static butil::static_atomic<size_t> g_ub_nblock = BUTIL_STATIC_ATOMIC_INIT(0);
static butil::static_atomic<size_t> g_ub_blockmem = BUTIL_STATIC_ATOMIC_INIT(0);
static butil::static_atomic<size_t> g_num_hit_ub_threshold = BUTIL_STATIC_ATOMIC_INIT(0);

void remove_tls_ub_block_chain();

TLSData* get_g_ub_data() { return &g_ub_data; }
IOBuf::Block* get_ub_block_head() { return g_ub_data.block_head; }
int get_ub_block_count() { return g_ub_data.num_blocks; }

void inc_g_nblock() {
    g_ub_nblock.fetch_add(1, butil::memory_order_relaxed);
}

void dec_g_nblock() {
    g_ub_nblock.fetch_sub(1, butil::memory_order_relaxed);
}

void inc_g_blockmem() {
    g_ub_blockmem.fetch_add(1, butil::memory_order_relaxed);
}

void dec_g_blockmem() {
    g_ub_blockmem.fetch_sub(1, butil::memory_order_relaxed);
}

void inc_g_num_hit_ub_threshold() {
    g_num_hit_ub_threshold.fetch_add(1, butil::memory_order_relaxed);
}

// Max number of blocks in each TLS. This is a soft limit namely
// release_tls_ub_block_chain() may exceed this limit sometimes.
const int MAX_BLOCKS_PER_THREAD = 8;

static inline int max_blocks_per_thread() {
    // If IOBufProfiler is enabled, do not cache ub blocks in TLS.
    return IsIOBufProfilerEnabled() ? 0 : MAX_BLOCKS_PER_THREAD;
}

size_t block_count() {
    return g_ub_nblock.load(butil::memory_order_relaxed);
}

size_t block_memory() {
    return g_ub_blockmem.load(butil::memory_order_relaxed);
}

size_t num_hit_ub_threshold() {
    return g_num_hit_ub_threshold.load(butil::memory_order_relaxed);
}

IOBuf::Block* create_ub_block(const size_t block_size) {
    if (block_size > 0xFFFFFFFFULL) {
        LOG(FATAL) << "block_size=" << block_size << " is too large";
        return NULL;
    }
    char* mem = (char*)blockmem_allocate(block_size);
    if (mem == NULL) {
        return NULL;
    }
    return new (mem) IOBuf::Block(mem + sizeof(IOBuf::Block),
                                  block_size - sizeof(IOBuf::Block),
                                  IOBUF_BLOCK_FLAGS_UB);
}

IOBuf::Block* create_ub_block() {
    return create_ub_block(UBIOBuf::get_block_size());
}

void release_tls_ub_block(IOBuf::Block* b) {
    if (!b) {
        return;
    }
    TLSData* ub_data = get_g_ub_data();
    if (b->full()) {
        b->dec_ref();
    } else if (ub_data->num_blocks >= max_blocks_per_thread()) {
        b->dec_ref();
        inc_g_num_hit_ub_threshold();
    } else {
        b->u.portal_next = ub_data->block_head;
        ub_data->block_head = b;
        ++ub_data->num_blocks;
        if (!ub_data->registered) {
            ub_data->registered = true;
            butil::thread_atexit(remove_tls_ub_block_chain);
        }
    }
}

void remove_tls_ub_block_chain() {
    TLSData& ub_data = g_ub_data;
    IOBuf::Block* b = ub_data.block_head;
    if (!b) {
        return;
    }
    ub_data.block_head = NULL;
    int n = 0;
    do {
        IOBuf::Block* const saved_next = b->u.portal_next;
        b->dec_ref();
        b = saved_next;
        ++n;
    } while (b);
    CHECK_EQ(n, ub_data.num_blocks);
    ub_data.num_blocks = 0;
}

IOBuf::Block* share_tls_ub_block() {
    TLSData& ub_data = g_ub_data;
    IOBuf::Block* const b = ub_data.block_head;
    if (b != NULL && !b->full()) {
        return b;
    }
    IOBuf::Block* new_block = NULL;
    if (b) {
        new_block = b;
        while (new_block && new_block->full()) {
            IOBuf::Block* const saved_next = new_block->u.portal_next;
            new_block->dec_ref();
            --ub_data.num_blocks;
            new_block = saved_next;
        }
    } else if (!ub_data.registered) {
        ub_data.registered = true;
        butil::thread_atexit(remove_tls_ub_block_chain);
    }
    if (!new_block) {
        new_block = create_ub_block();
        if (new_block) {
            ++ub_data.num_blocks;
        }
    }
    ub_data.block_head = new_block;
    return new_block;
}

void release_tls_ub_block_chain(IOBuf::Block* b) {
    TLSData& ub_data = g_ub_data;
    size_t n = 0;
    if (ub_data.num_blocks >= max_blocks_per_thread()) {
        do {
            ++n;
            IOBuf::Block* const saved_next = b->u.portal_next;
            b->dec_ref();
            b = saved_next;
        } while (b);
        inc_g_num_hit_ub_threshold();
        return;
    }
    IOBuf::Block* first_b = b;
    IOBuf::Block* last_b = NULL;
    do {
        ++n;
        CHECK(!b->full());
        CHECK(b->flags & IOBUF_BLOCK_FLAGS_UB);
        if (b->u.portal_next == NULL) {
            last_b = b;
            break;
        }
        b = b->u.portal_next;
    } while (true);
    last_b->u.portal_next = ub_data.block_head;
    ub_data.block_head = first_b;
    ub_data.num_blocks += n;
    if (!ub_data.registered) {
        ub_data.registered = true;
        butil::thread_atexit(remove_tls_ub_block_chain);
    }
}

IOBuf::Block* acquire_tls_ub_block() {
    TLSData& ub_data = g_ub_data;
    IOBuf::Block* b = ub_data.block_head;
    if (!b) {
        return create_ub_block();
    }
    while (b->full()) {
        IOBuf::Block* const saved_next = b->u.portal_next;
        b->dec_ref();
        ub_data.block_head = saved_next;
        --ub_data.num_blocks;
        b = saved_next;
        if (!b) {
            return create_ub_block();
        }
    }
    ub_data.block_head = b->u.portal_next;
    --ub_data.num_blocks;
    b->u.portal_next = NULL;
    return b;
}

static inline void* cp(void *__restrict dest, const void *__restrict src, size_t n) {
    // memcpy in gcc 4.8 seems to be faster enough.
    return memcpy(dest, src, n);
}

}  // namespace ubiobuf

namespace {

static const int REF_INDEX_BITS = 19;
static const int REF_OFFSET_BITS = 15;
static const int AREA_SIZE_BITS = 30;
static const uint32_t MAX_REF_INDEX = (((uint32_t)1) << REF_INDEX_BITS) - 1;
static const uint32_t MAX_REF_OFFSET = (((uint32_t)1) << REF_OFFSET_BITS) - 1;
static const uint32_t MAX_AREA_SIZE = (((uint32_t)1) << AREA_SIZE_BITS) - 1;

inline UBIOBuf::Area make_ub_area(uint32_t ref_index, uint32_t ref_offset,
                                  uint32_t size) {
    if (ref_index > MAX_REF_INDEX ||
        ref_offset > MAX_REF_OFFSET ||
        size > MAX_AREA_SIZE) {
        LOG(ERROR) << "Too big parameters!";
        return UBIOBuf::INVALID_AREA;
    }
    return (((uint64_t)ref_index) << (REF_OFFSET_BITS + AREA_SIZE_BITS))
        | (((uint64_t)ref_offset) << AREA_SIZE_BITS)
        | size;
}

}  // namespace

void UBIOBuf::operator=(const IOBuf& rhs) {
    if (this == &rhs) {
        return;
    }
    clear();
    append(rhs);
}

void UBIOBuf::operator=(const Movable& rhs) {
    clear();
    append(rhs);
}

void UBIOBuf::operator=(const char* s) {
    clear();
    append(s);
}

void UBIOBuf::operator=(const std::string& s) {
    clear();
    append(s);
}

void UBIOBuf::append(const IOBuf& other) {
    const size_t nref = other._ref_num();
    for (size_t i = 0; i < nref; ++i) {
        _push_back_ref(other._ref_at(i));
    }
}

void UBIOBuf::append(const Movable& movable_other) {
    if (empty()) {
        swap(movable_other.value());
    } else {
        IOBuf& other = movable_other.value();
        const size_t nref = other._ref_num();
        for (size_t i = 0; i < nref; ++i) {
            _move_back_ref(other._ref_at(i));
        }
        if (!other._small()) {
            delete[] other._bv.refs;
        }
        reset_block_ref(other._sv.refs[0]);
        reset_block_ref(other._sv.refs[1]);
    }
}

int UBIOBuf::push_back(char c) {
    Block* b = ubiobuf::share_tls_ub_block();
    if (BAIDU_UNLIKELY(!b)) {
        return -1;
    }
    b->data[b->size] = c;
    const BlockRef r = { b->size, 1, b };
    ++b->size;
    _push_back_ref(r);
    return 0;
}

int UBIOBuf::append(char const* s) {
    if (BAIDU_LIKELY(s != NULL)) {
        return append(s, strlen(s));
    }
    return -1;
}

int UBIOBuf::append(void const* data, size_t count) {
    if (BAIDU_UNLIKELY(!data)) {
        return -1;
    }
    if (count == 1) {
        return push_back(*(char const*)data);
    }
    size_t total_nc = 0;
    while (total_nc < count) {
        Block* b = ubiobuf::share_tls_ub_block();
        if (BAIDU_UNLIKELY(!b)) {
            return -1;
        }
        const size_t nc = std::min(count - total_nc, b->left_space());
        ubiobuf::cp(b->data + b->size, (char*)data + total_nc, nc);

        const BlockRef r = { (uint32_t)b->size, (uint32_t)nc, b };
        _push_back_ref(r);
        b->size += nc;
        total_nc += nc;
    }
    return 0;
}

int UBIOBuf::appendv(const const_iovec* vec, size_t n) {
    size_t offset = 0;
    for (size_t i = 0; i < n;) {
        Block* b = ubiobuf::share_tls_ub_block();
        if (BAIDU_UNLIKELY(!b)) {
            return -1;
        }
        uint32_t total_cp = 0;
        for (; i < n; ++i, offset = 0) {
            const const_iovec& vec_i = vec[i];
            const size_t nc = std::min(vec_i.iov_len - offset,
                                       b->left_space() - total_cp);
            ubiobuf::cp(b->data + b->size + total_cp,
                      (char*)vec_i.iov_base + offset, nc);
            total_cp += nc;
            offset += nc;
            if (offset != vec_i.iov_len) {
                break;
            }
        }

        const BlockRef r = { (uint32_t)b->size, total_cp, b };
        b->size += total_cp;
        _push_back_ref(r);
    }
    return 0;
}

int UBIOBuf::append(const std::string& s) {
    return append(s.data(), s.length());
}

int UBIOBuf::append_user_data_with_meta(void* data,
                                        size_t size,
                                        std::function<void(void*)> deleter,
                                        uint64_t meta) {
    if (size > 0xFFFFFFFFULL - 100) {
        LOG(FATAL) << "data_size=" << size << " is too large";
        return -1;
    }
    if (!deleter) {
        deleter = ::free;
    }
    if (!size) {
        deleter(data);
        return 0;
    }
    char* mem = (char*)malloc(sizeof(Block) + sizeof(UserDataExtension));
    if (mem == NULL) {
        return -1;
    }
    Block* b = new (mem) Block((char*)data, size, std::move(deleter));
    b->u.data_meta = meta;
    const BlockRef r = { 0, b->cap, b };
    _move_back_ref(r);
    return 0;
}

int UBIOBuf::resize(size_t n, char c) {
    const size_t saved_len = length();
    if (n < saved_len) {
        pop_back(saved_len - n);
        return 0;
    }
    const size_t count = n - saved_len;
    size_t total_nc = 0;
    while (total_nc < count) {
        Block* b = ubiobuf::share_tls_ub_block();
        if (BAIDU_UNLIKELY(!b)) {
            return -1;
        }
        const size_t nc = std::min(count - total_nc, b->left_space());
        memset(b->data + b->size, c, nc);

        const BlockRef r = { (uint32_t)b->size, (uint32_t)nc, b };
        _push_back_ref(r);
        b->size += nc;
        total_nc += nc;
    }
    return 0;
}

UBIOBuf::Area UBIOBuf::reserve(size_t count) {
    Area result = INVALID_AREA;
    size_t total_nc = 0;
    while (total_nc < count) {
        Block* b = ubiobuf::share_tls_ub_block();
        if (BAIDU_UNLIKELY(!b)) {
            return INVALID_AREA;
        }
        const size_t nc = std::min(count - total_nc, b->left_space());
        const BlockRef r = { (uint32_t)b->size, (uint32_t)nc, b };
        _push_back_ref(r);
        if (total_nc == 0) {
            result = make_ub_area(_ref_num() - 1, _back_ref().length - nc, count);
        }
        total_nc += nc;
        b->size += nc;
    }
    return result;
}

int UBIOBuf::normalize() {
    return normalize(this);
}

int UBIOBuf::normalize(IOBuf* buf) {
    if (buf == NULL || buf->empty()) {
        return 0;
    }

    int copied_blocks = 0;
    const size_t nref = buf->_ref_num();
    for (size_t i = 0; i < nref; ++i) {
        const BlockRef& r = buf->_ref_at(i);
        if (!(r.block->flags & IOBUF_BLOCK_FLAGS_UB)) {
            ++copied_blocks;
        }
    }
    if (copied_blocks == 0) {
        return 0;
    }

    UBIOBuf normalized;
    for (size_t i = 0; i < nref; ++i) {
        const BlockRef& r = buf->_ref_at(i);
        if (r.block->flags & IOBUF_BLOCK_FLAGS_UB) {
            normalized._push_back_ref(r);
        } else {
            if (normalized.append(r.block->data + r.offset, r.length) != 0) {
                return -1;
            }
        }
    }
    buf->swap(normalized);
    return copied_blocks;
}

size_t UBIOBuf::get_block_size() {
#ifdef BRPC_WITH_URMA
    if (brpc::FLAGS_ubsocket_block_type == "tiny") {
        return 4UL * 1024;
    } else if (brpc::FLAGS_ubsocket_block_type == "default") {
        return UBIOBuf::DEFAULT_BLOCK_SIZE;
    } else if (brpc::FLAGS_ubsocket_block_type == "small") {
        return 16UL * 1024;
    } else if (brpc::FLAGS_ubsocket_block_type == "medium") {
        return 32UL * 1024;
    } else if (brpc::FLAGS_ubsocket_block_type == "large") {
        return 64UL * 1024;
    } else {
        LOG(WARNING) << "Unknown ubsocket_block_type: " << brpc::FLAGS_ubsocket_block_type << ", use the default IOBuf BLOCK_SIZE";
        return UBIOBuf::DEFAULT_BLOCK_SIZE;
    }
#else
    return UBIOBuf::DEFAULT_BLOCK_SIZE;
#endif
}

UBIOBufAsZeroCopyOutputStream::UBIOBufAsZeroCopyOutputStream(UBIOBuf* buf)
    : _buf(buf)
    , _block_size(0)
    , _cur_block(NULL)
    , _byte_count(0) {
}

UBIOBufAsZeroCopyOutputStream::UBIOBufAsZeroCopyOutputStream(
    UBIOBuf* buf, uint32_t block_size)
    : _buf(buf)
    , _block_size(block_size)
    , _cur_block(NULL)
    , _byte_count(0) {
    if (_block_size <= offsetof(IOBuf::Block, data)) {
        throw std::invalid_argument("block_size is too small");
    }
}

UBIOBufAsZeroCopyOutputStream::~UBIOBufAsZeroCopyOutputStream() {
    _release_block();
}

bool UBIOBufAsZeroCopyOutputStream::Next(void** data, int* size) {
    if (_cur_block == NULL || _cur_block->full()) {
        _release_block();
        if (_block_size > 0) {
            _cur_block = ubiobuf::create_ub_block(_block_size);
        } else {
            _cur_block = ubiobuf::acquire_tls_ub_block();
        }
        if (_cur_block == NULL) {
            return false;
        }
    }
    const IOBuf::BlockRef r = { _cur_block->size,
                                (uint32_t)_cur_block->left_space(),
                                _cur_block };
    *data = _cur_block->data + r.offset;
    *size = r.length;
    _cur_block->size = _cur_block->cap;
    _buf->_push_back_ref(r);
    _byte_count += r.length;
    return true;
}

void UBIOBufAsZeroCopyOutputStream::BackUp(int count) {
    while (!_buf->empty()) {
        IOBuf::BlockRef& r = _buf->_back_ref();
        if (_cur_block) {
            if (r.block != _cur_block) {
                LOG(FATAL) << "r.block=" << r.block
                           << " does not match _cur_block=" << _cur_block;
                return;
            }
            if (r.offset + r.length != _cur_block->size) {
                LOG(FATAL) << "r.offset(" << r.offset << ") + r.length("
                           << r.length << ") != _cur_block->size("
                           << _cur_block->size << ")";
                return;
            }
        } else {
            if (r.block->ref_count() == 1) {
                if (r.offset + r.length != r.block->size) {
                    LOG(FATAL) << "r.offset(" << r.offset << ") + r.length("
                               << r.length << ") != r.block->size("
                               << r.block->size << ")";
                    return;
                }
            } else if (r.offset + r.length != r.block->size) {
                _byte_count -= _buf->pop_back(count);
                return;
            }
            _cur_block = r.block;
            _cur_block->inc_ref();
        }
        if (BAIDU_LIKELY(r.length > (uint32_t)count)) {
            r.length -= count;
            if (!_buf->_small()) {
                _buf->_bv.nbytes -= count;
            }
            _cur_block->size -= count;
            _byte_count -= count;
            if (_block_size == 0) {
                ubiobuf::release_tls_ub_block(_cur_block);
                _cur_block = NULL;
            }
            return;
        }
        _cur_block->size -= r.length;
        _byte_count -= r.length;
        count -= r.length;
        _buf->_pop_back_ref();
        _release_block();
        if (count == 0) {
            return;
        }
    }
    LOG_IF(FATAL, count != 0) << "BackUp an empty UBIOBuf";
}

int64_t UBIOBufAsZeroCopyOutputStream::ByteCount() const {
    return _byte_count;
}

void UBIOBufAsZeroCopyOutputStream::_release_block() {
    if (_block_size > 0) {
        if (_cur_block) {
            _cur_block->dec_ref();
        }
    } else {
        ubiobuf::release_tls_ub_block(_cur_block);
    }
    _cur_block = NULL;
}

UBIOBufAsSnappySink::UBIOBufAsSnappySink(butil::UBIOBuf& buf)
    : _cur_buf(NULL), _cur_len(0), _buf(&buf), _buf_stream(&buf) {
}

void UBIOBufAsSnappySink::Append(const char* bytes, size_t n) {
    if (_cur_len > 0) {
        CHECK(bytes == _cur_buf && static_cast<int>(n) <= _cur_len)
            << "bytes must be _cur_buf";
        _buf_stream.BackUp(_cur_len - n);
        _cur_len = 0;
    } else {
        _buf->append(bytes, n);
    }
}

char* UBIOBufAsSnappySink::GetAppendBuffer(size_t length, char* scratch) {
    if (length <= 8000) {
        if (_buf_stream.Next(reinterpret_cast<void**>(&_cur_buf), &_cur_len)) {
            if (_cur_len >= static_cast<int>(length)) {
                return _cur_buf;
            } else {
                _buf_stream.BackUp(_cur_len);
            }
        } else {
            LOG(FATAL) << "Fail to alloc buffer";
        }
    }
    _cur_buf = NULL;
    _cur_len = 0;
    return scratch;
}

ssize_t IOPortal::ub_append_from_file_descriptor(
    int fd, size_t max_count) {
    if (max_count == 0) {
        return 0;
    }
    if (_block == NULL) {
        _block = ubiobuf::acquire_tls_ub_block();
        if (BAIDU_UNLIKELY(!_block)) {
            errno = ENOMEM;
            return -1;
        }
    }

    iovec vec;
    vec.iov_base = _block->data + _block->size;
    vec.iov_len = std::min(_block->left_space(), max_count);

    ssize_t nr = ::ubsocket_wrapper_readv(fd, &vec, 1);
    if (nr <= 0) {  // -1 or 0
        if (empty()) {
            ubiobuf::release_tls_ub_block(_block);
            _block = NULL;
        }
        return nr;
    }

    size_t total_len = nr;
    while (total_len && _block) {
        const size_t left_space = _block->left_space();
        if (left_space == 0) {
            Block* const saved_next = _block->u.portal_next;
            _block->u.portal_next = NULL;
            _block->dec_ref();
            _block = saved_next;
            continue;
        }
        const size_t len = std::min(total_len, left_space);
        total_len -= len;
        const IOBuf::BlockRef r = { _block->size, (uint32_t)len, _block };
        _push_back_ref(r);
        _block->size += len;
        if (_block->full()) {
            Block* const saved_next = _block->u.portal_next;
            _block->u.portal_next = NULL;
            _block->dec_ref();  // _block may be deleted
            _block = saved_next;
        }
    }
    return nr;
}

}  // namespace butil
