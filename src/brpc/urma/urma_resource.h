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

#ifndef BRPC_URMA_RESOURCE_H
#define BRPC_URMA_RESOURCE_H

#include <array>
#include <cstddef>
#include <cstdint>
#include <ctime>
#include <functional>
#include <memory>
#include <ostream>
#include <sys/types.h>
#include "bthread/types.h"
#include "butil/atomicops.h"
#include "butil/iobuf.h"
#include "brpc/socket_id.h"

#if BRPC_WITH_URMA
namespace brpc {
namespace urma {

class UrmaConnectionResource;
class UrmaResourceProvider;
using UrmaConnectionHandle = std::shared_ptr<UrmaConnectionResource>;

enum class UrmaResourceMode : uint8_t { EXCLUSIVE = 0, POOLED = 1 };
enum class UrmaPeerResourceKind : uint8_t { JETTY = 0, JFR = 1 };
enum class UrmaReceiveCountKind : uint8_t {
    CONNECTION_CREDIT = 0, SHARED_JFR_DEPTH = 1
};
enum class UrmaConnectionRole : uint8_t { CLIENT = 0, SERVER = 1 };

struct UrmaConnectionOptions {
    SocketId socket_id = INVALID_SOCKET_ID;
    UrmaConnectionRole role = UrmaConnectionRole::CLIENT;
    bthread_tag_t tag = 0;
    bthread_keytable_pool_t* keytable_pool = nullptr;  // Borrowed.
};

struct UrmaSegmentDescription {
    std::array<uint8_t, 16> eid{};
    uint32_t uasid = 0;
    uint64_t va = 0;
    uint64_t length = 0;
    uint32_t token_id = 0;
};

// Host-order description, never serialized by copying this struct.
struct UrmaResourceDescription {
    UrmaResourceMode mode = UrmaResourceMode::EXCLUSIVE;
    UrmaPeerResourceKind resource_kind = UrmaPeerResourceKind::JETTY;
    UrmaReceiveCountKind receive_count_kind =
        UrmaReceiveCountKind::CONNECTION_CREDIT;
    uint32_t resource_id = 0;
    std::array<uint8_t, 16> eid{};
    uint32_t uasid = 0;
    uint8_t tp_type = 0;
    uint32_t recv_buffer_size = 0;
    uint32_t recv_buffer_count = 0;
    UrmaSegmentDescription segment;
    bool has_socket_id = false;
    SocketId socket_id = INVALID_SOCKET_ID;
};

struct UrmaResourceCallbacks {
    UrmaConnectionHandle (*lookup_connection)(SocketId id) = nullptr;
    void (*on_received)(SocketId id,
                        const UrmaConnectionHandle& source,
                        butil::IOBuf data) = nullptr;
    void (*on_error)(SocketId id,
                     const UrmaConnectionHandle& source,
                     int error) = nullptr;
};

class UrmaConnectionResource {
public:
    virtual ~UrmaConnectionResource() = default;
    virtual int GetLocalDescription(UrmaResourceDescription* out) const = 0;
    virtual int BindPeer(const UrmaResourceDescription& peer) = 0;
    // Consume exactly the accepted continuous prefix from the caller's IOBufs.
    // Retain accepted data until completion; leave the unaccepted suffix in
    // its original IOBufs, including when a later WR fails. Return a positive
    // prefix after partial success; only an empty input returns zero.
    // The caller serializes data sends and holds a resource handle throughout.
    virtual ssize_t Send(butil::IOBuf** data, size_t ndata) = 0;
    virtual bool IsWritable() const = 0;
    // After binding, wait on the owning Socket's epollout butex only. The
    // caller reads expected before checking IsWritable; no pointer is retained.
    virtual int WaitWritable(butil::atomic<int>* butex, int expected,
                             const timespec* deadline) = 0;
    // Reject new work and detach event sources without waiting for admitted
    // sends/callbacks. Their handles retain hardware and buffers until exit.
    virtual void Close() = 0;
    virtual void DebugInfo(std::ostream& out) const = 0;
};

class UrmaResourceProvider {
public:
    virtual ~UrmaResourceProvider() = default;
    virtual int Initialize(const UrmaResourceCallbacks& callbacks) = 0;
    virtual int CreateConnection(const UrmaConnectionOptions& options,
                                 UrmaConnectionHandle* out) = 0;
    virtual int InitializePollingMode(bthread_tag_t tag,
                                       std::function<void()> callback,
                                       std::function<void()> init_fn,
                                       std::function<void()> release_fn) = 0;
    virtual void ReleasePollingMode(bthread_tag_t tag) = 0;
    virtual bool Shutdown() = 0;
};

// Creates an uninitialized provider; unsupported modes return ENOTSUP.
int CreateUrmaResourceProvider(UrmaResourceMode mode,
                              std::unique_ptr<UrmaResourceProvider>* out);

// Process owner. Initialized after the common device and memory services.
int InitializeUrmaResourceProvider(const UrmaResourceCallbacks& callbacks);
UrmaResourceProvider* GetUrmaResourceProvider();
bool ShutdownUrmaResourceProvider();
}  // namespace urma
}  // namespace brpc
#endif  // BRPC_WITH_URMA
#endif  // BRPC_URMA_RESOURCE_H
