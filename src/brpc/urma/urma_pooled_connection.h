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

#ifndef BRPC_URMA_POOLED_CONNECTION_H
#define BRPC_URMA_POOLED_CONNECTION_H
#include <unordered_map>
#include <map>
#include "brpc/urma/urma_pooled_resource.h"
#include "brpc/urma/urma_helper.h"
#if BRPC_WITH_URMA
namespace brpc {
namespace urma {
constexpr int BRPC_URMA_MAX_CQE_PER_POLL = 32;
DECLARE_bool(urma_use_polling);
struct UrmaPooledProviderState {
    butil::Mutex mutex;
    butil::Mutex shutdown_mutex;
    std::atomic<bool> stopping{false};
    static constexpr uint32_t CALLBACKS_CLOSED = uint32_t(1) << 31;
    std::atomic<uint32_t> callback_gate{0};
    bool initialized{false};
    std::atomic<size_t> live_connections{0};
    std::unordered_map<UrmaConnection*, std::weak_ptr<UrmaConnection>> connections;
    UrmaConnectionCallbacks callbacks;
    UrmaPooledResource* pool{nullptr};
    ~UrmaPooledProviderState() {
        if (pool && pool->Shutdown()) {
            if (g_urma_pool == pool) g_urma_pool = nullptr;
            delete pool;
        }
        // On failed native deletion retain the pool, contexts and imports.
    }
};

class UrmaPooledConnection final : public UrmaConnection,
                                   public std::enable_shared_from_this<UrmaPooledConnection> {
public:
    UrmaPooledConnection(const UrmaConnectionOptions& options,
                         std::shared_ptr<UrmaPooledProviderState> provider_state)
        : _options(options), _provider_state(std::move(provider_state)),
          _shared_state(new UrmaPooledConnectionState) {
        _provider_state->live_connections.fetch_add(1);
        _shared_state->local_jfr_index = _provider_state->pool->PickJfr();
        _shared_state->imports = _provider_state->pool->imports;
        _preferred_jfs = UrmaPooledResource::PreferredJfs(
            _options.socket_id, _provider_state->pool->io_jfs.size());
    }
    ~UrmaPooledConnection() override {
        Close();
        _shared_state.reset();
        { BAIDU_SCOPED_LOCK(_provider_state->mutex);
          _provider_state->connections.erase(this); }
        _provider_state->live_connections.fetch_sub(1);
    }
    int GetLocalDescription(UrmaConnectionDescription* out) const override;
    int ImportPeer(const UrmaConnectionDescription& peer) override;
    ssize_t CutFromIOBufList(butil::IOBuf**, size_t) override;
    bool IsWritable() const override {
        return !_closed.load() && !_shared_state->failed.load() && _peer_imported.load() &&
            _provider_state->pool->IsWritable(_preferred_jfs);
    }
    int WaitWritable(butil::atomic<int>*, int, const timespec*) override;
    void Close() override {
        { BAIDU_SCOPED_LOCK(_lifecycle_mutex);
          if (_closed.exchange(true)) return;
          _shared_state->failed.store(true, std::memory_order_release); }
        SocketUniquePtr socket;
        if (Socket::Address(_options.socket_id, &socket) == 0) socket->WakeAsEpollOut();
    }
    void DebugInfo(std::ostream& out) const override {
        out << "resource_mode=pooled jfr_index=" << _shared_state->local_jfr_index
            << " preferred_jfs=" << _preferred_jfs;
    }
private:
    friend class UrmaPooledConnectionProvider;
    static UrmaConnection* FindConnection(SocketUniquePtr&);
    // One poll result, never a wait for additional completions. Socket references
    // keep endpoint storage alive until its accumulated bytes are dispatched.
    struct UrmaRxParseBatch {
        SocketUniquePtr sockets[BRPC_URMA_MAX_CQE_PER_POLL];
        size_t size{0};
        void Add(SocketUniquePtr&);
    };

    // Callback-owned, bounded to one message across all logical connections.
    // Declare before the CQ lock so destruction always happens after unlocking.
    struct UrmaRxDispatchContext {
        UrmaRxDispatchContext() = default;
        InputMessageBase* last_message{nullptr};
        bool event_released{false};
        UrmaRxParseBatch batch;
        static void RetainLastMessage(InputMessageClosure&, UrmaRxDispatchContext*);
        static void ProcessPendingInputs(UrmaRxParseBatch*, const UrmaConnectionCallbacks&,
                               UrmaRxDispatchContext*);
        ~UrmaRxDispatchContext() noexcept(false);
    private:
        DISALLOW_COPY_AND_ASSIGN(UrmaRxDispatchContext);
    };
    static void RouteReceivedFragment(SocketId, uint32_t, uint16_t, butil::IOBuf,
                           UrmaRxDispatchContext*, UrmaRxParseBatch*);
    static void ProcessReceivedFragment(SocketUniquePtr&, uint32_t, uint16_t, butil::IOBuf,
                               UrmaRxDispatchContext*, UrmaRxParseBatch*);
    static int AttachSlotSocket(UrmaIoSlot*);
    static void DetachSlotSocket(UrmaIoSlot*);
    static void PollJfc(Socket*);
    static void HandleTxCompletion(UrmaJfsSlot*, const urma_cr_t&);
    static void HandleRxCompletion(UrmaJfrSlot*, const urma_cr_t&,
                                   UrmaRxDispatchContext* = nullptr, UrmaRxParseBatch* = nullptr);

    // Caller holds _reorder_mutex through delivery to preserve stream order.
    int AssembleFragmentLocked(uint32_t queue, uint16_t seq, butil::IOBuf* data);
    bool ValidateReceiveQueue(uint32_t queue) const {
        return !_closed.load() && _shared_state->local_jfr_index == queue;
    }
    const UrmaConnectionOptions _options;
    std::shared_ptr<UrmaPooledProviderState> _provider_state;
    std::shared_ptr<UrmaPooledConnectionState> _shared_state;
    butil::Mutex _lifecycle_mutex;
    std::atomic<bool> _closed{false}, _peer_imported{false};
    uint32_t _preferred_jfs{UINT32_MAX};
    uint32_t _remote_recv_block_size{0};
    SocketId _peer_socket_id{INVALID_SOCKET_ID};
    uint16_t _next_send_seq{0};
    // Serialize fragment assembly through ordered delivery. Close only marks
    // the resource closed: callback-held references keep this state alive,
    // avoiding a dispatch -> Close -> reorder lock inversion.
    butil::Mutex _reorder_mutex;
    std::map<uint16_t, butil::IOBuf> _reorder_queue;
    uint16_t _deliver_base{0};
};

class UrmaPooledConnectionProvider final : public UrmaConnectionProvider {
public:
    UrmaPooledConnectionProvider() : _state(new UrmaPooledProviderState) {}
    int Initialize(const UrmaConnectionCallbacks&) override;
    int CreateConnection(const UrmaConnectionOptions&, UrmaConnectionHandle*) override;
    int InitializePollingMode(bthread_tag_t, std::function<void()>,
                              std::function<void()>, std::function<void()>) override {
        if (FLAGS_urma_use_polling) { errno = ENOTSUP; return -1; }
        return 0;
    }
    void ReleasePollingMode(bthread_tag_t) override {}
    bool Shutdown() override;
private:
    std::shared_ptr<UrmaPooledProviderState> _state;
};

int CreatePooledUrmaConnectionProvider(std::unique_ptr<UrmaConnectionProvider>* out);

}  // namespace urma
}  // namespace brpc
#endif
#endif
