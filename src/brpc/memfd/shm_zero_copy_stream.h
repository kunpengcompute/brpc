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

#ifndef BRPC_MEMFD_SHM_ZERO_COPY_STREAM_H
#define BRPC_MEMFD_SHM_ZERO_COPY_STREAM_H

#include <google/protobuf/io/zero_copy_stream.h>
#include "butil/iobuf.h"
#include "brpc/memfd/shm_block_allocator.h"

namespace brpc {

struct ShmTLSData {
    butil::IOBuf::Block* block_head;
    int num_blocks;
    bool registered;

    ShmTLSData() : block_head(nullptr), num_blocks(0), registered(false) {}
};

const int MAX_SHM_BLOCKS_PER_THREAD = 8;

butil::IOBuf::Block* share_shm_tls_block(ShmBlockAllocator* allocator);
butil::IOBuf::Block* acquire_shm_tls_block(ShmBlockAllocator* allocator);
void release_shm_tls_block(butil::IOBuf::Block* block);

class ShmZeroCopyOutputStream
    : public google::protobuf::io::ZeroCopyOutputStream {
public:
    ShmZeroCopyOutputStream(ShmBlockAllocator* allocator, butil::IOBuf* output)
        : _allocator(allocator)
        , _output(output)
        , _cur_block(nullptr)
        , _byte_count(0)
        , _fallback_stream(nullptr) {}

    ~ShmZeroCopyOutputStream() {
        Finish();
    }

    bool Next(void** data, int* size) override {
        if (_fallback_stream) {
            return FallbackNext(data, size);
        }

        if (_cur_block == nullptr || _cur_block->full()) {
            _release_block();
            if (_allocator->IsWaitNotify()) {
                return FallbackNext(data, size);
            }
            _cur_block = acquire_shm_tls_block(_allocator);
            if (_cur_block == nullptr) {
                return FallbackNext(data, size);
            }
        }

        uint32_t start = _cur_block->size;
        uint32_t length = _cur_block->left_space();
        *data = _cur_block->data + start;
        *size = length;
        _cur_block->size = _cur_block->cap;
        _output->append_user_block(_cur_block, start, length);
        _byte_count += length;
        return true;
    }

    void BackUp(int count) override {
        if (_fallback_stream) {
            FallbackBackUp(count);
            return;
        }

        while (!_output->empty() && count > 0) {
            butil::IOBuf::BlockRef& r = _output->_back_ref();
            if (_cur_block) {
                if (r.block != _cur_block) {
                    return;
                }
                if (r.offset + r.length != _cur_block->size) {
                    return;
                }
            } else {
                if (r.block->ref_count() == 1) {
                    if (r.offset + r.length != r.block->size) {
                        return;
                    }
                } else if (r.offset + r.length != r.block->size) {
                    _byte_count -= _output->pop_back(count);
                    return;
                }
                _cur_block = r.block;
                _cur_block->inc_ref();
            }
            if (BAIDU_LIKELY((int)r.length > count)) {
                r.length -= count;
                if (!_output->_small()) {
                    _output->_bv.nbytes -= count;
                }
                _cur_block->size -= count;
                _byte_count -= count;
                if (!_cur_block->full()) {
                    release_shm_tls_block(_cur_block);
                    _cur_block = nullptr;
                }
                return;
            }
            _cur_block->size -= r.length;
            _byte_count -= r.length;
            count -= r.length;
            _output->_pop_back_ref();
            _release_block();
            if (count == 0) {
                return;
            }
        }
    }

    int64_t ByteCount() const override {
        return _byte_count;
    }

    void Finish() {
        if (_cur_block) {
            _release_block();
        }
    }

private:
    bool FallbackNext(void** data, int* size) {
        LOG_FIRST_N(WARNING, 1) << "Get BLOCK malloc";
        if (!_fallback_stream) {
            _fallback_stream.reset(
                new butil::IOBufAsZeroCopyOutputStream(_output));
        }
        int64_t old_bytes = _fallback_stream->ByteCount();
        if (!_fallback_stream->Next(data, size)) {
            return false;
        }
        _byte_count += (_fallback_stream->ByteCount() - old_bytes);
        return true;
    }

    void FallbackBackUp(int count) {
        int64_t old_bytes = _fallback_stream->ByteCount();
        _fallback_stream->BackUp(count);
        _byte_count -= (old_bytes - _fallback_stream->ByteCount());
    }

    void _release_block() {
        if (_cur_block) {
            if (!_cur_block->full()) {
                release_shm_tls_block(_cur_block);
            } else {
                _cur_block->dec_ref();
            }
            _cur_block = nullptr;
        }
    }

    ShmBlockAllocator* _allocator;
    butil::IOBuf* _output;
    butil::IOBuf::Block* _cur_block;
    int64_t _byte_count;
    std::unique_ptr<butil::IOBufAsZeroCopyOutputStream> _fallback_stream;
};

} // namespace brpc

#endif // BRPC_MEMFD_SHM_ZERO_COPY_STREAM_H
