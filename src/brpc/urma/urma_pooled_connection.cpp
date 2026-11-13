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

#include "brpc/urma/urma_pooled_connection.h"
#include "brpc/input_messenger.h"
#include "brpc/urma/urma_endpoint.h"
#if BRPC_WITH_URMA
#include <algorithm>
#include <cstring>
#include <unistd.h>
#include "brpc/urma/urma_pooled_resource.h"
#include "brpc/urma/urma_helper.h"
#include "butil/containers/stack_container.h"
#include "butil/sys_byteorder.h"
#include "bthread/butex.h"
#include "brpc/errno.pb.h"
#include "bthread/task_group.h"
#include "bthread/unstable.h"
#include "brpc/urma_transport.h"

namespace bthread {
EXTERN_BAIDU_VOLATILE_THREAD_LOCAL(TaskGroup*, tls_task_group);
// Same internal cleanup primitive used by TaskGroup::task_runner.
extern void return_keytable(bthread_keytable_pool_t*, KeyTable*);
}


namespace brpc {
namespace urma {
namespace {
// A shared CQ carrier has no per-server keytable pool. Give its one inline
// message the same fresh bthread-local context as QueueMessage would create.
// Keep the CQ context intact, including when authentication has used TLS.
class ScopedMessageLocalStorage {
public:
    explicit ScopedMessageLocalStorage(bthread_keytable_pool_t* pool)
        : _saved(*bthread::tls_bls_ptr()), _pool(pool) {
        auto* group = bthread::BAIDU_GET_VOLATILE_THREAD_LOCAL(tls_task_group);
        _task = group ? group->current_task() : nullptr;
        if (_task) {
            BAIDU_SCOPED_LOCK(_task->version_lock);
            _old_pool = _task->attr.keytable_pool;
            _task->attr.keytable_pool = pool;
            _task->local_storage = bthread::LOCAL_STORAGE_INIT;
        }
        *bthread::tls_bls_ptr() = bthread::LOCAL_STORAGE_INIT;
    }
    ~ScopedMessageLocalStorage() {
        // Destructors may yield: reload TLS pointers after each callback.
        auto* local = bthread::tls_bls_ptr();
        if (local->rpcz_parent_span && bthread::g_rpcz_parent_span_dtor) {
            bthread::g_rpcz_parent_span_dtor(local->rpcz_parent_span);
            bthread::tls_bls_ptr()->rpcz_parent_span = nullptr;
            if (_task) { _task->local_storage.rpcz_parent_span = nullptr; }
        }
        bthread::return_keytable(_pool, bthread::tls_bls_ptr()->keytable);
        *bthread::tls_bls_ptr() = _saved;
        if (_task) {
            BAIDU_SCOPED_LOCK(_task->version_lock);
            _task->attr.keytable_pool = _old_pool;
            _task->local_storage = _saved;
        }
    }
private:
    bthread::LocalStorage _saved;
    bthread::TaskMeta* _task;
    bthread_keytable_pool_t* _pool;
    bthread_keytable_pool_t* _old_pool{nullptr};
};
}  // namespace


UrmaPooledConnection::UrmaRxDispatchContext::~UrmaRxDispatchContext() noexcept(false) {
    if (!last_message) { return; }
    // Errors may terminate the CQ drain before it hands event ownership back.
    // Never run business code in that case; fail and destroy outside locks.
    if (!event_released || last_message->socket()->Failed()) {
        if (!event_released) {
            last_message->socket()->SetFailed(EIO, "URMA CQ stopped before message delivery");
        }
        last_message->Destroy();
        return;
    }
    if (!bthread_self() ||
        !bthread::BAIDU_GET_VOLATILE_THREAD_LOCAL(tls_task_group)) {
        // A raw pthread cannot borrow a bthread keytable pool. Preserve the
        // normal dispatch context for this uncommon direct-callback path.
        bthread_t thread;
        bthread_attr_t attr = BTHREAD_ATTR_NORMAL;
        attr.keytable_pool = last_message->socket()->keytable_pool();
        attr.tag = bthread_self_tag();
        if (bthread_start_background(&thread, &attr,
                                    Transport::ProcessInputMessage, last_message) != 0) {
            // Existing dispatch fallback, now outside the CQ locks.
            InputMessageClosure run;
            run.reset(last_message);
        }
        return;
    }
    ScopedMessageLocalStorage local(last_message->socket()->keytable_pool());
    InputMessageClosure run;
    run.reset(last_message);
}

DEFINE_bool(urma_rx_batch_parse, true,
            "Parse each connection once per CQ poll result (startup only)");

void UrmaPooledConnection::UrmaRxParseBatch::Add(SocketUniquePtr& socket) {
    for (size_t i = 0; i < size; ++i) {
        if (sockets[i].get() == socket.get()) { return; }
    }
    CHECK_LT(size, static_cast<size_t>(BRPC_URMA_MAX_CQE_PER_POLL));
    sockets[size++] = std::move(socket);
}

void UrmaPooledConnection::UrmaRxDispatchContext::RetainLastMessage(InputMessageClosure& last_msg,
                                      UrmaRxDispatchContext* dispatch_context) {
    if (!dispatch_context) return;
    InputMessageBase* current = last_msg.release();
    if (!current) return;
    InputMessageBase* displaced = dispatch_context->last_message;
    InputMessageClosure previous;
    previous.reset(displaced);
    dispatch_context->last_message = current;
    if (displaced) {
        int created = 0;
        displaced->socket()->_transport->QueueMessage(previous, &created, false);
        if (created) bthread_flush();
    }
}

void UrmaPooledConnection::UrmaRxDispatchContext::ProcessPendingInputs(UrmaRxParseBatch* batch,
                                        const UrmaConnectionCallbacks& callbacks,
                                        UrmaRxDispatchContext* context) {
    for (size_t i = 0; i < batch->size; ++i) {
        auto& socket = batch->sockets[i];
        InputMessageClosure last_msg;
        if (!socket->Failed()) callbacks.on_input_ready(socket, last_msg);
        RetainLastMessage(last_msg, context);
        socket.reset();
    }
    batch->size = 0;
}


DECLARE_bool(urma_use_polling);

DECLARE_bool(urma_recv_zerocopy);
DECLARE_int32(urma_zerocopy_min_size);
DECLARE_int32(urma_cqe_poll_once);

UrmaConnection* UrmaPooledConnection::FindConnection(SocketUniquePtr& socket) {
    if (socket->Failed() || !socket->_transport ||
        socket->socket_mode() != SOCKET_MODE_URMA) return nullptr;
    auto* ep = static_cast<UrmaTransport*>(socket->_transport.get())->_urma_ep;
    return ep ? ep->BorrowConnection() : nullptr;
}


int UrmaPooledConnection::GetLocalDescription(UrmaConnectionDescription* out) const {
    if (!out || _closed.load()) { errno = EINVAL; return -1; }
    *out = UrmaConnectionDescription{};
    out->mode = UrmaConnectionMode::POOLED;
    out->receive_target_kind = UrmaReceiveTargetKind::JFR;
    out->receive_count_kind = UrmaReceiveCountKind::SHARED_JFR_DEPTH;
    out->has_socket_id = true;
    out->socket_id = _options.socket_id;
    const auto* slot = _provider_state->pool->io_jfr[_shared_state->local_jfr_index];
    out->receive_target_id = slot->jfr_id;
    out->uasid = slot->jfr->jfr_id.uasid;
    const auto* eid = GetUrmaLocalEid();
    std::memcpy(out->eid.data(), eid ? eid->raw : slot->jfr->jfr_id.eid.raw, 16);
    out->tp_type = URMA_CTP;
    out->recv_buffer_size = GetUrmaRecvBlockSize();
    out->recv_buffer_count = slot->rx_depth;
    auto* pool = GetPoolSegFor(nullptr);
    if (!pool) { errno = ENODEV; return -1; }
    std::memcpy(out->segment.eid.data(), pool->seg.ubva.eid.raw, 16);
    out->segment.uasid = pool->seg.ubva.uasid;
    out->segment.va = pool->seg.ubva.va;
    out->segment.length = pool->seg.len;
    out->segment.token_id = pool->seg.token_id;
    return 0;
}

int UrmaPooledConnection::ImportPeer(const UrmaConnectionDescription& peer) {
    BAIDU_SCOPED_LOCK(_lifecycle_mutex);
    if (_closed.load()) { errno = EPIPE; return -1; }
    if (_peer_imported.load() || _shared_state->remote_seg) { errno = EALREADY; return -1; }
    if (peer.mode != UrmaConnectionMode::POOLED ||
        peer.receive_target_kind != UrmaReceiveTargetKind::JFR ||
        peer.receive_count_kind != UrmaReceiveCountKind::SHARED_JFR_DEPTH ||
        !peer.has_socket_id || peer.socket_id == INVALID_SOCKET_ID ||
        !peer.receive_target_id || peer.tp_type != URMA_CTP ||
        !peer.recv_buffer_size ||
        !peer.recv_buffer_count || peer.recv_buffer_count > 65535 ||
        !peer.segment.va || !peer.segment.length ||
        peer.segment.length > UINT64_MAX - peer.segment.va) {
        errno = EPROTO; return -1;
    }
    urma_context_t* context = GetUrmaContext();
    if (!context) { errno = ENODEV; return -1; }
    urma_seg_t segment{};
    std::memcpy(segment.ubva.eid.raw, peer.segment.eid.data(), 16);
    segment.ubva.uasid = peer.segment.uasid;
    segment.ubva.va = peer.segment.va;
    segment.len = peer.segment.length;
    segment.token_id = peer.segment.token_id;
    urma_token_t token{};
    urma_import_seg_flag_t flag{};
    flag.bs.cacheable = URMA_NON_CACHEABLE;
    flag.bs.access = URMA_ACCESS_READ | URMA_ACCESS_WRITE | URMA_ACCESS_ATOMIC;
    flag.bs.mapping = URMA_SEG_NOMAP;
    _shared_state->remote_seg = urma_import_seg(context, &segment, &token, 0, flag);
    if (!_shared_state->remote_seg) { _closed.store(true); return -1; }
    urma_rjfr_t remote{};
    std::memcpy(remote.jfr_id.eid.raw, peer.eid.data(), 16);
    remote.jfr_id.uasid = peer.uasid;
    remote.jfr_id.id = peer.receive_target_id;
    remote.trans_mode = URMA_TM_RM;
    remote.tp_type = URMA_CTP;
    remote.flag.bs.token_policy = URMA_TOKEN_NONE;
    _shared_state->remote_jfr = _shared_state->imports->Acquire(context, &remote, &token);
    if (!_shared_state->remote_jfr) { _closed.store(true); return -1; }
    _peer_socket_id = peer.socket_id;
    _remote_recv_block_size = peer.recv_buffer_size;
    _shared_state->owner_connection = shared_from_this();
    _peer_imported.store(true);
    return 0;
}

ssize_t UrmaPooledConnection::CutFromIOBufList(butil::IOBuf** from, size_t ndata) {
    if (ndata == 0) { return 0; }
    if (!from) { errno = EINVAL; return -1; }
    bool empty = true;
    for (size_t i = 0; i < ndata; ++i) {
        if (!from[i]) { errno = EINVAL; return -1; }
        empty = empty && from[i]->empty();
    }
    if (empty) { return 0; }
    if (_closed.load()) { errno = EPIPE; return -1; }
    if (!_peer_imported.load() || !_shared_state || !_shared_state->remote_jfr || !_provider_state->pool) {
        errno = ENOTCONN; return -1;
    }
    UrmaOperation operation(&_provider_state->pool->operations);
    if (!operation || _shared_state->failed.load(std::memory_order_acquire)) {
        errno = EIO; return -1;
    }
    const uint32_t start = FLAGS_urma_jfs_affinity ? _preferred_jfs
        : _provider_state->pool->jfs_rr_counter.fetch_add(1);
    butil::IOBuf remaining;
    for (size_t j = 0; j < ndata; ++j) { remaining.append(*from[j]); }
    if (remaining.empty()) { return 0; }
    const size_t max_wr_per_post = std::min<size_t>(
        std::max(1, FLAGS_urma_io_tx_batch_max), URMA_TX_BATCH_MAX);
    size_t total_bytes = 0;
    for (int route = 0; route < 2; ++route) {
        auto* slot = route == 0 ? _provider_state->pool->io_jfs[start % _provider_state->pool->io_jfs.size()]
                               : _provider_state->pool->PickBackup(start);
        if (!slot) { break; }
        const uint32_t used = slot->sq_inflight.load(std::memory_order_relaxed);
        if (slot->stopped.load() || used >= slot->sq_depth) { errno = EAGAIN; continue; }
        const size_t budget = std::min<size_t>(max_wr_per_post, slot->sq_depth - used);
        auto discard = [](TxBatch* node) { if (node) { node->DiscardPrepared(); } };
        TxBatch* pooled_batch = butil::get_object<TxBatch>();
        if (!pooled_batch) { errno = ENOMEM; break; }
        pooled_batch->ResetForUse();
        std::unique_ptr<TxBatch, decltype(discard)> batch(pooled_batch, discard);
        size_t bytes = 0;
        int prepare_error = 0;
        while (!remaining.empty() && batch->count < budget) {
            auto recycle_ctx = [](TxCompletionCtx* value) {
                if (value) { value->Release(); }
            };
            TxCompletionCtx* pooled_ctx = butil::get_object<TxCompletionCtx>();
            if (!pooled_ctx) { prepare_error = ENOMEM; break; }
            pooled_ctx->ResetForUse();
            std::unique_ptr<TxCompletionCtx, decltype(recycle_ctx)> ctx(
                pooled_ctx, recycle_ctx);
            ctx->self_socket_id = _options.socket_id;
            ctx->jfs_index = slot->index;
            ctx->connection_state = _shared_state;
            ctx->seq = static_cast<uint16_t>(_next_send_seq + batch->count);
            auto& sge = ctx->sge;
            size_t limit = std::min<size_t>(_remote_recv_block_size, GetUrmaRecvBlockSize());
            limit = std::min(limit, URMA_CTP_MAX_MESSAGE_SIZE - sizeof(UrmaMsgHeader));
            size_t len = 0;

            ctx->header_buf = static_cast<UrmaMsgHeader*>(AllocHeaderBuffer());
            if (!ctx->header_buf) { prepare_error = ENOMEM; break; }
            const uint32_t max_sge = slot->jfs->jfs_cfg.max_sge;
            if (max_sge < 2) { prepare_error = EINVAL; break; }
            sge->push_back(urma_sge_t{});
            sge[0].addr = reinterpret_cast<uint64_t>(ctx->header_buf);
            sge[0].len = sizeof(UrmaMsgHeader);
            sge[0].tseg = GetHeaderPoolSegFor(ctx->header_buf);
            for (size_t block = 0; block < remaining.backing_block_num() &&
                    len < limit && sge->size() < max_sge; ++block) {
                const auto piece = remaining.backing_block(block);
                urma_sge_t body{};
                body.addr = reinterpret_cast<uint64_t>(piece.data());
                body.len = std::min(piece.size(), limit - len);
                body.tseg = GetPoolSegFor(const_cast<char*>(piece.data()));
                if (!body.tseg) {
                    butil::IOBuf first_byte;
                    remaining.append_to(&first_byte, 1, len);
                    body.tseg = reinterpret_cast<urma_target_seg_t*>(
                        static_cast<uintptr_t>(first_byte.get_first_data_meta()));
                }
                sge->push_back(body);
                len += body.len;
            }
            remaining.append_to(&ctx->send_data, len);

            if (!len) { prepare_error = EMSGSIZE; break; }
            for (const auto& part : sge.container()) {
                if (!part.tseg) { prepare_error = ERDMAMEM; break; }
            }
            if (prepare_error) { break; }
            std::memset(ctx->header_buf, 0, sizeof(UrmaMsgHeader));
            ctx->header_buf->dst_socket_id = butil::HostToNet64(_peer_socket_id);
            ctx->header_buf->seq = butil::HostToNet16(ctx->seq);
            ctx->header_buf->type = MSG_SMALL;
            ctx->body_bytes = len;
            ctx->wr.opcode = URMA_OPC_SEND;
            ctx->wr.flag.bs.complete_enable = 1;
            ctx->wr.tjetty = _shared_state->remote_jfr;
            ctx->wr.send.src = urma_sg_t{sge->data(), static_cast<uint32_t>(sge->size())};
            ctx->wr.user_ctx = reinterpret_cast<uint64_t>(ctx.get());
            batch->Append(ctx.release());
            remaining.pop_front(len);
            bytes += len;
        }
        if (!batch->count) { errno = prepare_error ? prepare_error : EAGAIN; break; }
        const size_t accepted = batch->count;
        if (_provider_state->pool->Enqueue(slot, batch.get()) != 0) { break; }
        batch.release(); // Queue owns node; it may already have completed.
        total_bytes += bytes;
        _next_send_seq = static_cast<uint16_t>(_next_send_seq + accepted);
        // Bound preparation and keep a batch on the preferred JFS while it has room.
        if (remaining.empty() || accepted == max_wr_per_post || prepare_error) { break; }
    }
    if (!total_bytes) { return -1; }
    size_t left = total_bytes;
    for (size_t j = 0; j < ndata && left; ++j) { left -= from[j]->pop_front(left); }
    return total_bytes;
}

int UrmaPooledConnection::AssembleFragmentLocked(uint32_t queue, uint16_t seq,
                                                 butil::IOBuf* data) {
    if (!ValidateReceiveQueue(queue) || _shared_state->failed.load()) return 0;
    const uint16_t distance = static_cast<uint16_t>(seq - _deliver_base);
    if (distance >= 32768 || (distance && _reorder_queue.count(seq))) {
        _shared_state->failed.store(true);
        errno = EPROTO;
        return -1;
    }
    if (distance) {
        _reorder_queue.emplace(seq, data->movable());
        return 0;
    }
    ++_deliver_base;
    for (auto it = _reorder_queue.find(_deliver_base); it != _reorder_queue.end();
         it = _reorder_queue.find(_deliver_base)) {
        data->append(it->second.movable());
        _reorder_queue.erase(it);
        ++_deliver_base;
    }
    return 1;
}

int UrmaPooledConnection::WaitWritable(butil::atomic<int>* butex,
                                          int expected,
                                          const timespec* deadline) {
    if (!butex) {
        errno = EINVAL;
        return -1;
    }
    if (_closed.load()) {
        errno = EPIPE;
        return -1;
    }
    if (!_peer_imported.load()) {
        errno = ENOTCONN;
        return -1;
    }
    // A connection handle pins the Jetty, not its owning Socket. Pin the Socket
    // for this wait and accept only its fixed epollout butex.
    SocketUniquePtr socket;
    if (Socket::Address(_options.socket_id, &socket) != 0) {
        errno = EPIPE;
        return -1;
    }
    if (butex != socket->_epollout_butex) {
        errno = EINVAL;
        return -1;
    }
    if (IsWritable())
        return 0;
    const int rc = _provider_state->pool->WaitWritable(butex, expected, deadline, _preferred_jfs);
    if (_closed.load()) {
        errno = EPIPE;
        return -1;
    }
    return rc;
}

void UrmaPooledConnection::HandleTxCompletion(UrmaJfsSlot* slot, const urma_cr_t& cr) {
    slot->pool->CompleteSend(slot, cr);
}

void UrmaPooledConnection::HandleRxCompletion(
        UrmaJfrSlot* slot, const urma_cr_t& cr,
        UrmaRxDispatchContext* dispatch_context, UrmaRxParseBatch* batch) {
    auto* ctx = reinterpret_cast<RxRecvCtx*>(cr.user_ctx);
    if (!slot->Owns(ctx) || !ctx->posted) {
        LOG(ERROR) << "Unknown URMA RX completion context";
        slot->stopped.store(true);
        return;
    }
    ctx->posted = false;
    --slot->rx_posted;
    SocketId id = INVALID_SOCKET_ID;
    uint16_t seq = 0;
    butil::IOBuf data;
    if (cr.status == URMA_CR_SUCCESS && cr.opcode == URMA_CR_OPC_SEND &&
        cr.completion_len > sizeof(UrmaMsgHeader) &&
        cr.completion_len <= sizeof(UrmaMsgHeader) + GetUrmaRecvBlockSize() &&
        ctx->header_buf->type == MSG_SMALL) {
        id = butil::NetToHost64(ctx->header_buf->dst_socket_id);
        seq = butil::NetToHost16(ctx->header_buf->seq);
        const size_t len = cr.completion_len - sizeof(UrmaMsgHeader);
        if (FLAGS_urma_recv_zerocopy &&
            len >= static_cast<size_t>(FLAGS_urma_zerocopy_min_size)) {
            ctx->data_iobuf.cutn(&data, len);
            ctx->data_iobuf.clear();
            ctx->data_block = nullptr;
        } else {
            data.append(ctx->data_block, len);
        }
    }
    if (slot->pool->RepostRecv(*slot, ctx) != 0) {
        slot->stopped.store(true);
        LOG(ERROR) << "Failed to replenish shared URMA JFR " << slot->index;
        slot->callbacks.on_rx_error(id, EIO);
        return;
    }
    if (id != INVALID_SOCKET_ID) {
        RouteReceivedFragment(id, slot->index, seq, data.movable(), dispatch_context, batch);
    }
}

int UrmaPooledConnection::AttachSlotSocket(UrmaIoSlot* slot) {
    // First create a carrier without a user or fd. A failed Socket::Create
    // may invoke BeforeRecycle, so transfer slot ownership only after success.
    SocketOptions options;
    options.on_edge_triggered_events = PollJfc;
    if (Socket::Create(options, &slot->jfc_sid) != 0) { return -1; }
    SocketUniquePtr socket;
    if (Socket::Address(slot->jfc_sid, &socket) != 0) { return -1; }
    slot->AddRef();
    socket->_user = slot;
    // Keep liburma's fd ownership independent of the carrier's lifetime.
    const int fd = dup(slot->jfce->fd);
    if (fd < 0 || socket->ResetFileDescriptor(fd) != 0) {
        if (fd >= 0) { close(fd); }
        socket->SetFailed();
        slot->jfc_sid = INVALID_SOCKET_ID;
        return -1;
    }
    return 0;
}
void UrmaPooledConnection::DetachSlotSocket(UrmaIoSlot* slot) {
    if (slot->jfc_sid == INVALID_SOCKET_ID) { return; }
    SocketUniquePtr socket;
    if (Socket::Address(slot->jfc_sid, &socket) == 0) {
        if (socket->_fd >= 0) { socket->_io_event.RemoveConsumer(socket->_fd); }
        // Socket closes its duplicate; the provider owns the original fd.
        socket->SetFailed();
    }
    slot->jfc_sid = INVALID_SOCKET_ID;
}
void UrmaPooledConnection::PollJfc(Socket* carrier) {
    auto* slot = static_cast<UrmaIoSlot*>(carrier->user());
    auto owner = std::static_pointer_cast<UrmaPooledProviderState>(slot->callback_owner.lock());
    if (!owner) return;
    uint32_t gate = owner->callback_gate.load();
    do {
        if (gate & UrmaPooledProviderState::CALLBACKS_CLOSED) return;
    } while (!owner->callback_gate.compare_exchange_weak(gate, gate + 1));
    // Keep the owner/admission through unlock AND deferred business delivery.
    // Shutdown closes admission atomically before inspecting active callbacks.
    struct CallbackGuard {
        UrmaPooledProviderState* state;
        ~CallbackGuard() { state->callback_gate.fetch_sub(1); }
    } admitted{owner.get()};
    UrmaRxDispatchContext dispatch_context;
    UrmaRxDispatchContext* dispatch_context_ptr = &dispatch_context;
    UrmaOperation operation(&slot->operations);
    if (!operation || slot->stopped.load()) { return; }
    auto drain = [&]() -> bool {
        while (!slot->stopped.load()) {
            urma_cr_t cr[BRPC_URMA_MAX_CQE_PER_POLL];
            const int count = urma_poll_jfc(slot->jfc,
                std::max(1, std::min(BRPC_URMA_MAX_CQE_PER_POLL,
                                     FLAGS_urma_cqe_poll_once)), cr);
            if (count < 0) { return false; }
            if (!count) { return true; }
            if (slot->type == UrmaIoSlot::JFS_SLOT) {
                for (int i = 0; i < count; ++i) {
                    HandleTxCompletion(static_cast<UrmaJfsSlot*>(slot), cr[i]);
                }
            } else if (FLAGS_urma_rx_batch_parse) {
                UrmaRxParseBatch& batch = dispatch_context.batch;
                for (int i = 0; i < count; ++i) {
                    HandleRxCompletion(static_cast<UrmaJfrSlot*>(slot), cr[i],
                                       dispatch_context_ptr, &batch);
                }
                // Bound latency and storage to this poll result. Even a single
                // CQE is flushed immediately; never wait for another arrival.
                UrmaRxDispatchContext::ProcessPendingInputs(&batch, slot->callbacks, dispatch_context_ptr);
            } else {
                for (int i = 0; i < count; ++i) {
                    HandleRxCompletion(static_cast<UrmaJfrSlot*>(slot), cr[i], dispatch_context_ptr);
                }
            }
        }
        return false;
    };
    int progress = Socket::PROGRESS_INIT;
    while (true) {
        urma_jfc_t* event = nullptr;
        const int count = urma_wait_jfc(slot->jfce, 1, 0, &event);
        if (count < 0 && errno == EINTR) { continue; }
        if (count < 0 && errno != EAGAIN && errno != EWOULDBLOCK) {
            slot->stopped.store(true);
            return;
        }
        if (count > 0) {
            const bool ok = drain();
            uint32_t events = 1;
            urma_ack_jfc(&event, &events, 1);
            if (!ok || urma_rearm_jfc(slot->jfc, false) != URMA_SUCCESS || !drain()) {
                slot->stopped.store(true);
                return;
            }
            continue;
        }
        if (!carrier->MoreReadEvents(&progress)) {
            dispatch_context.event_released = true;
            return;
        }
    }
}

void UrmaPooledConnection::RouteReceivedFragment(SocketId id, uint32_t queue, uint16_t seq,
                                       butil::IOBuf data, UrmaRxDispatchContext* context,
                                       UrmaRxParseBatch* batch) {
    SocketUniquePtr socket;
    if (Socket::Address(id, &socket) != 0) return;
    ProcessReceivedFragment(socket, queue, seq, data.movable(), context, batch);
}

void UrmaPooledConnection::ProcessReceivedFragment(SocketUniquePtr& socket, uint32_t queue,
                                           uint16_t seq, butil::IOBuf data,
                                           UrmaRxDispatchContext* context,
                                           UrmaRxParseBatch* batch) {
    auto* connection = dynamic_cast<UrmaPooledConnection*>(FindConnection(socket));
    if (!connection) return;
    InputMessageClosure last_msg;
    {
        BAIDU_SCOPED_LOCK(connection->_reorder_mutex);
        const int result = connection->AssembleFragmentLocked(queue, seq, &data);
        if (result < 0) socket->SetFailed(EPROTO, "Invalid URMA receive sequence");
        else if (result > 0) {
            const auto& callbacks = connection->_provider_state->callbacks;
            if (callbacks.on_bytes(socket, connection, data.movable())) {
                if (batch) batch->Add(socket);
                else callbacks.on_input_ready(socket, last_msg);
            }
        }
    }
    UrmaRxDispatchContext::RetainLastMessage(last_msg, context);
}

int UrmaPooledConnectionProvider::Initialize(const UrmaConnectionCallbacks& callbacks) {
    BAIDU_SCOPED_LOCK(_state->mutex);
    if (_state->initialized || _state->stopping) { errno = EALREADY; return -1; }
    if (!callbacks.on_bytes || !callbacks.on_input_ready ||
        !callbacks.on_rx_error || !callbacks.on_error) { errno = EINVAL; return -1; }
    if (!GetUrmaContext() || FLAGS_urma_use_polling || IsUrmaBondingDevice()) {
        errno = ENOTSUP; return -1;
    }
    _state->callbacks = callbacks;
    _state->pool = CreateUrmaPooledResource();
    if (!_state->pool) { errno = ENOMEM; return -1; }
    UrmaPooledResource::EventAdapter events;
    auto* pool = _state->pool;
    std::weak_ptr<UrmaPooledProviderState> owner = _state;
    events.attach = [pool, callbacks, owner](UrmaIoSlot* slot) {
        slot->callback_owner = owner;
        slot->pool = pool;
        slot->callbacks = callbacks;
        return UrmaPooledConnection::AttachSlotSocket(slot);
    };
    events.detach = UrmaPooledConnection::DetachSlotSocket;
    if (_state->pool->Initialize(GetUrmaContext(), events) != 0) return -1;
    g_urma_pool = _state->pool;  // Non-owning diagnostics compatibility.
    _state->initialized = true;
    return 0;
}
int UrmaPooledConnectionProvider::CreateConnection(
        const UrmaConnectionOptions& options, UrmaConnectionHandle* out) {
    if (!out || options.socket_id == INVALID_SOCKET_ID) { errno = EINVAL; return -1; }
    UrmaConnectionHandle previous;
    {
        BAIDU_SCOPED_LOCK(_state->mutex);
        if (!_state->initialized || _state->stopping) { errno = ESHUTDOWN; return -1; }
        auto connection = std::make_shared<UrmaPooledConnection>(options, _state);
        _state->connections.emplace(connection.get(), connection);
        previous.swap(*out);
        *out = connection;
    }
    return 0;
}
bool UrmaPooledConnectionProvider::Shutdown() {
    _state->stopping.store(true);
    const uint32_t admitted = _state->callback_gate.fetch_or(UrmaPooledProviderState::CALLBACKS_CLOSED);

    std::vector<UrmaConnectionHandle> live;
    {
        BAIDU_SCOPED_LOCK(_state->mutex);
        _state->stopping = true;
        for (auto& entry : _state->connections) {
            if (auto connection = entry.second.lock()) live.push_back(connection);
        }
    }
    for (auto& connection : live) connection->Close();
    // Never wait for a CQ while called from that CQ, or hold teardown's mutex
    // while another admitted callback could reenter Shutdown.
    if ((admitted & ~UrmaPooledProviderState::CALLBACKS_CLOSED) ||
        !live.empty() || _state->live_connections.load() != 0) return false;
    BAIDU_SCOPED_LOCK(_state->shutdown_mutex);
    if (!_state->pool) return true;
    if (!_state->pool->Shutdown()) return false;
    if (g_urma_pool == _state->pool) g_urma_pool = nullptr;
    delete _state->pool;
    _state->pool = nullptr;
    return true;
}
int CreatePooledUrmaConnectionProvider(std::unique_ptr<UrmaConnectionProvider>* out) {
    out->reset(new UrmaPooledConnectionProvider);
    return 0;
}
}  // namespace urma
}  // namespace brpc
#endif
