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

#ifndef BRPC_EVENT_DISPATCHER_IOURING_IMPL_H
#define BRPC_EVENT_DISPATCHER_IOURING_IMPL_H

#include <brpc/event_dispatcher.h>

namespace brpc {
namespace iouring_backend {

struct IoUringFdInfo {
    int fd;
    uint32_t events;
    IOEventDataId id;
    bool use_multishot;
    bool is_listen;
    char* buffer;
    int32_t res;

    // For multishot recv with registered buffers: instead of memcpy'ing
    // data out to a heap buffer, we pass the registered buffer pointer
    // directly to IOBuf::append_user_data (zero-copy). The deleter
    // recycles the buffer back to the ring when IOBuf releases it.
    bool is_registered_buffer;
    uint32_t registered_buf_id;
    void* recycle_ctx;  // opaque pointer to IoUringContext

    IoUringFdInfo() : fd(-1), events(0), id(-1), use_multishot(false), is_listen(false),
                       buffer(NULL), res(0), is_registered_buffer(false),
                       registered_buf_id(0), recycle_ctx(nullptr) {}
    IoUringFdInfo(int f, uint32_t e, IOEventDataId id, bool multishot = false, bool listen = false)
        : fd(f), events(e), id(id), use_multishot(multishot), buffer(NULL), is_listen(listen),
          res(0), is_registered_buffer(false), registered_buf_id(0), recycle_ctx(nullptr) {}
};

// Recycle a registered buffer back to the buffer ring.
// Called by the IOBuf deleter (in Socket::iouring_callback) when the
// application is done with the buffer data, so the kernel can reuse it.
void RecycleRegisteredBuffer(void* ctx, uint32_t buf_id);

} // namespace iouring_backend
} // namespace brpc

#endif  // BRPC_EVENT_DISPATCHER_IOURING_IMPL_H
