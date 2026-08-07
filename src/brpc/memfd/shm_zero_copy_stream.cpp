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

#include "brpc/memfd/shm_zero_copy_stream.h"
#include "butil/logging.h"
#include "butil/thread_local.h"

namespace brpc {

static __thread ShmTLSData* g_shm_tls_data = nullptr;

static void destroy_shm_tls_data(void* arg) {
    ShmTLSData* tls = static_cast<ShmTLSData*>(arg);
    if (!tls) return;
    g_shm_tls_data = nullptr;

    butil::IOBuf::Block* b = tls->block_head;
    while (b) {
        butil::IOBuf::Block* next = b->u.portal_next;
        b->dec_ref();
        b = next;
    }
    delete tls;
}

static ShmTLSData* get_or_create_shm_tls() {
    if (g_shm_tls_data) return g_shm_tls_data;

    ShmTLSData* tls = new ShmTLSData();
    g_shm_tls_data = tls;

    butil::thread_atexit(destroy_shm_tls_data, tls);
    tls->registered = true;
    return tls;
}

butil::IOBuf::Block* acquire_shm_tls_block(ShmBlockAllocator* allocator) {
    ShmTLSData* tls = get_or_create_shm_tls();
    butil::IOBuf::Block* b = tls->block_head;

    if (b) {
        while (b->full()) {
            tls->block_head = b->u.portal_next;
            tls->num_blocks--;
            b->dec_ref();
            b = tls->block_head;
            if (!b) {
                return allocator->TryAllocBlock();
            }
        }
        tls->block_head = b->u.portal_next;
        tls->num_blocks--;
        b->u.portal_next = NULL;
        return b;
    } else {
        return allocator->TryAllocBlock();
    }
}

butil::IOBuf::Block* share_shm_tls_block(ShmBlockAllocator* allocator) {
    ShmTLSData* tls = get_or_create_shm_tls();
    butil::IOBuf::Block* b = tls->block_head;

    if (b != NULL && !b->full()) {
        return b;
    }

    while (b && b->full()) {
        tls->block_head = b->u.portal_next;
        tls->num_blocks--;
        b->dec_ref();
        b = tls->block_head;
    }

    if (b != NULL) {
        return b;
    }

    b = allocator->TryAllocBlock();
    if (b) {
        b->u.portal_next = tls->block_head;
        tls->block_head = b;
        tls->num_blocks++;
    }
    return b;
}

void release_shm_tls_block(butil::IOBuf::Block* block) {
    if (!block) return;

    ShmTLSData* tls = get_or_create_shm_tls();

    if (tls->num_blocks >= MAX_SHM_BLOCKS_PER_THREAD) {
        block->dec_ref();
        return;
    }

    block->u.portal_next = tls->block_head;
    tls->block_head = block;
    tls->num_blocks++;
}

static void ShmBlockReleaseFunc(butil::IOBuf::Block* block) {
    uint32_t block_index = GetShmAllocator()->GetBlockIndex(block);

    block->~Block();

    if (block_index != SHM_INVALID_BLOCK_INDEX) {
        GetShmAllocator()->MarkWriteComplete(block_index);
    }
}

struct ShmBlockReleaseInitializer {
    ShmBlockReleaseInitializer() {
        butil::iobuf::shm_block_release = ShmBlockReleaseFunc;
    }
};

static ShmBlockReleaseInitializer g_shm_block_release_init;

static butil::IOBuf::Block* ShmBlockAcquireFunc() {
    return share_shm_tls_block(GetShmAllocator());
}

struct ShmBlockAcquireInitializer {
    ShmBlockAcquireInitializer() {
        butil::iobuf::shm_block_acquire = ShmBlockAcquireFunc;
    }
};

static ShmBlockAcquireInitializer g_shm_block_acquire_init;

} // namespace brpc
