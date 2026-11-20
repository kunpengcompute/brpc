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

#ifndef BRPC_URMA_CONNECTION_H
#define BRPC_URMA_CONNECTION_H

#include <array>
#include <cstddef>
#include <cstdint>
#include <ctime>
#include <functional>
#include <memory>
#include <ostream>
#include <vector>
#include <sys/types.h>
#include "bthread/types.h"
#include "butil/atomicops.h"
#include "butil/iobuf.h"
#include "brpc/socket.h"

#if BRPC_WITH_URMA
namespace brpc {
namespace urma {

class UrmaConnection;
class UrmaConnectionProvider;
using UrmaConnectionHandle = std::shared_ptr<UrmaConnection>;

constexpr uint32_t URMA_BIG_CAP_READ = 2;

enum class UrmaConnectionMode : uint8_t { EXCLUSIVE = 0, POOLED = 1 };
enum class UrmaReceiveTargetKind : uint8_t { JETTY = 0, JFR = 1 };
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
struct UrmaConnectionDescription {
    UrmaConnectionMode mode = UrmaConnectionMode::EXCLUSIVE;
    UrmaReceiveTargetKind receive_target_kind = UrmaReceiveTargetKind::JETTY;
    UrmaReceiveCountKind receive_count_kind =
        UrmaReceiveCountKind::CONNECTION_CREDIT;
    uint32_t receive_target_id = 0;
    std::array<uint8_t, 16> eid{};
    uint32_t uasid = 0;
    uint8_t tp_type = 0;
    uint32_t recv_buffer_size = 0;
    uint32_t recv_buffer_count = 0;
    UrmaSegmentDescription segment;
    uint32_t big_capabilities = 0;
    uint32_t read_max_size = 0;
    std::vector<UrmaSegmentDescription> segments;
    bool has_socket_id = false;
    SocketId socket_id = INVALID_SOCKET_ID;
};

// Both backends deliver ordered bytes using an already pinned Socket.
struct UrmaConnectionCallbacks {
    bool (*on_bytes)(SocketUniquePtr&, const UrmaConnection*, butil::IOBuf) = nullptr;
    void (*on_input_ready)(SocketUniquePtr&, InputMessageClosure&) = nullptr;
    void (*on_rx_error)(SocketId, int) = nullptr;
    void (*on_error)(SocketId, const UrmaConnectionHandle&, int) = nullptr;
};

class UrmaConnection {
public:
    virtual ~UrmaConnection() = default;
    virtual int GetLocalDescription(UrmaConnectionDescription* out) const = 0;
    virtual int ImportPeer(const UrmaConnectionDescription& peer) = 0;
    // Consume exactly the accepted continuous prefix from the caller's IOBufs.
    // Retain accepted data until completion; leave the unaccepted suffix in
    // its original IOBufs, including when a later WR fails. Return a positive
    // prefix after partial success; only an empty input returns zero.
    // The caller serializes data sends and pins the owning Socket or connection.
    virtual ssize_t CutFromIOBufList(butil::IOBuf** data, size_t ndata) = 0;
    virtual bool IsWritable() const = 0;
    // After importing the peer, wait on the owning Socket's epollout butex only. The
    // caller reads expected before checking IsWritable; no pointer is retained.
    virtual int WaitWritable(butil::atomic<int>* butex, int expected,
                             const timespec* deadline) = 0;
    // Reject new work and detach event sources without waiting for admitted
    // sends/callbacks. Their handles retain hardware and buffers until exit.
    virtual void Close() = 0;
    virtual void DebugInfo(std::ostream& out) const = 0;

};

class UrmaConnectionProvider {
public:
    virtual ~UrmaConnectionProvider() = default;
    virtual int Initialize(const UrmaConnectionCallbacks& callbacks) = 0;
    virtual int CreateConnection(const UrmaConnectionOptions& options,
                                 UrmaConnectionHandle* out) = 0;
    virtual int InitializePollingMode(bthread_tag_t tag,
                                       std::function<void()> callback,
                                       std::function<void()> init_fn,
                                       std::function<void()> release_fn) = 0;
    virtual void ReleasePollingMode(bthread_tag_t tag) = 0;
    virtual bool Shutdown() = 0;
};

// Parse the startup-only mode before initializing device and memory services.
int ConfigureUrmaConnectionMode();
// Read the mode fixed by common initialization; later flag changes have no effect.
UrmaConnectionMode GetUrmaConnectionMode();

// Creates an uninitialized provider; unsupported modes return ENOTSUP.
int CreateUrmaConnectionProvider(UrmaConnectionMode mode,
                              std::unique_ptr<UrmaConnectionProvider>* out);

// Process owner. Initialized after the common device and memory services.
int InitializeUrmaConnectionProvider(const UrmaConnectionCallbacks& callbacks);
UrmaConnectionProvider* GetUrmaConnectionProvider();
bool ShutdownUrmaConnectionProvider();
}  // namespace urma
}  // namespace brpc
#endif  // BRPC_WITH_URMA
#endif  // BRPC_URMA_CONNECTION_H
