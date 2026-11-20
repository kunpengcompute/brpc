// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file distributed
// with this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.
#include "brpc/urma/urma_pooled_connection.h"
#if BRPC_WITH_URMA
#include <algorithm>
#include "brpc/input_messenger.h"
#include <gflags/gflags.h>
#include "brpc/versioned_ref_with_id.h"
#include "brpc/urma/urma_big_message.h"
#include "brpc/urma/urma_memory_pool.h"
#include "butil/sys_byteorder.h"
namespace brpc {
namespace urma {
DEFINE_string(urma_large_transfer, "auto",
              "Pooled transfer: send, read (force), or auto (per-IOBuf size threshold)");
static bool ValidateLargeTransfer(const char*, const std::string& value) {
    return value == "send" || value == "read" || value == "auto";
}
DEFINE_validator(urma_large_transfer, &ValidateLargeTransfer);
DECLARE_int32(urma_read_items_per_control);
DECLARE_bool(urma_rx_batch_parse);
namespace {
std::atomic<uint64_t> controls{0}, posted{0}, completed{0}, sources{0};
struct SourceOptions {
    SocketId sid;
    uint16_t seq;
    butil::IOBuf* data;
    std::shared_ptr<UrmaPooledConnectionState> connection;
};
// The versioned registry owns a reference until the peer's matching FREE.
// Close cannot recycle these buffers: the peer may still be reading them.
class SourceContext : public VersionedRefWithId<SourceContext> {
public:
    explicit SourceContext(Forbidden f) : VersionedRefWithId(f) {}
    SocketId sid{INVALID_SOCKET_ID};
    uint16_t seq{0};
    butil::IOBuf data;
    std::shared_ptr<UrmaPooledConnectionState> connection;
private:
    friend class VersionedRefWithId<SourceContext>;
    bool armed{false};
    int OnCreated(const SourceOptions& o) {
        sid = o.sid; seq = o.seq; connection = o.connection;
        data.swap(*o.data); armed = true;
        sources.fetch_add(1, std::memory_order_relaxed);
        return 0;
    }
    void BeforeRecycled() {
        data.clear(); connection.reset();
        if (armed) sources.fetch_sub(1, std::memory_order_relaxed);
        armed = false;
    }
};
struct UnpublishedSource {
    VRefId id{0};
    bool armed{false};
    ~UnpublishedSource() { if (armed) SourceContext::SetFailedById(id); }
};
struct BatchDeleter {
    void operator()(TxBatch* b) const { if (b) b->DiscardPrepared(); }
};
struct ContextDeleter {
    void operator()(TxCompletionCtx* c) const { if (c) c->Release(); }
};
std::unique_ptr<TxBatch, BatchDeleter> NewBatch() {
    auto* b = butil::get_object<TxBatch>();
    if (b) b->ResetForUse();
    return std::unique_ptr<TxBatch, BatchDeleter>(b);
}
std::unique_ptr<TxCompletionCtx, ContextDeleter> NewContext() {
    auto* c = butil::get_object<TxCompletionCtx>();
    if (c) c->ResetForUse();
    return std::unique_ptr<TxCompletionCtx, ContextDeleter>(c);
}
}  // namespace
struct UrmaReadBatch {
    struct Entry { uint16_t seq; uint32_t length; butil::IOBuf data; };
    uint64_t source{0};
    uint32_t total_length{0};
    std::vector<Entry> entries;
    std::atomic<size_t> remaining{0};
    std::atomic<bool> failed{false};
    std::shared_ptr<UrmaPooledConnection> connection;
};
UrmaReadStats GetUrmaReadStats() {
    return {controls.load(), posted.load(), completed.load(), sources.load()};
}
int UrmaPooledConnection::SendControl(uint8_t type, const UrmaControl& control,
                                      UrmaJfsSlot* selected) {
    if (_closed.load() || !_peer_imported.load() || _shared_state->failed.load()) {
        errno = EPIPE; return -1;
    }
    butil::IOBuf payload;
    if (!control.Encode(&payload) || payload.size() > _remote_recv_block_size) {
        errno = EMSGSIZE; return -1;
    }
    auto batch = NewBatch(); auto ctx = NewContext();
    if (!batch || !ctx) { errno = ENOMEM; return -1; }
    auto* slot = selected ? selected : _provider_state->pool->io_jfs[_preferred_jfs];
    ctx->self_socket_id = _options.socket_id;
    ctx->connection_state = _shared_state; ctx->jfs_index = slot->index; ctx->type = type;
    ctx->header_buf = static_cast<UrmaMsgHeader*>(AllocHeaderBuffer());
    if (!ctx->header_buf) { errno = ENOMEM; return -1; }
    std::memset(ctx->header_buf, 0, sizeof(UrmaMsgHeader));
    ctx->header_buf->dst_socket_id = butil::HostToNet64(_peer_socket_id);
    ctx->header_buf->type = type;
    void* bytes = nullptr; int capacity = 0;
    {
        butil::IOBufAsZeroCopyOutputStream stream(&ctx->send_data, 8192);
        if (!stream.Next(&bytes, &capacity) || capacity < static_cast<int>(payload.size())) {
            errno = ENOMEM; return -1;
        }
        payload.copy_to(bytes, payload.size());
        stream.BackUp(capacity - payload.size());
    }
    ctx->sge->push_back(urma_sge_t{});
    ctx->sge[0].addr = reinterpret_cast<uint64_t>(ctx->header_buf);
    ctx->sge[0].len = sizeof(UrmaMsgHeader);
    ctx->sge[0].tseg = GetHeaderPoolSegFor(ctx->header_buf);
    ctx->sge->push_back(urma_sge_t{});
    ctx->sge[1].addr = reinterpret_cast<uint64_t>(bytes);
    ctx->sge[1].len = payload.size(); ctx->sge[1].tseg = GetPoolSegFor(bytes);
    if (!ctx->sge[0].tseg || !ctx->sge[1].tseg) { errno = ERDMAMEM; return -1; }
    ctx->wr.opcode = URMA_OPC_SEND; ctx->wr.flag.bs.complete_enable = 1;
    ctx->wr.tjetty = _shared_state->remote_jfr;
    ctx->wr.send.src = urma_sg_t{ctx->sge->data(), 2};
    ctx->wr.user_ctx = reinterpret_cast<uint64_t>(ctx.get());
    batch->Append(ctx.release());
    // Protocol work already accepted from a peer must progress even when all
    // hardware SQs are full. The owner limits actual device submissions.
    if (_provider_state->pool->Enqueue(slot, batch.get()) != 0) return -1;
    batch.release(); controls.fetch_add(1, std::memory_order_relaxed);
    return 0;
}
ssize_t UrmaPooledConnection::StartReadTransfer(const butil::IOBuf& input) {
    auto* pool = _provider_state->pool;
    auto* slot = pool->io_jfs[_preferred_jfs];
    if (slot->sq_inflight.load(std::memory_order_relaxed) >= slot->sq_depth) {
        slot = pool->PickBackup(_preferred_jfs);
        if (!slot || slot->sq_inflight.load(std::memory_order_relaxed) >= slot->sq_depth) {
            errno = EAGAIN; return -1;
        }
    }
    butil::IOBuf remaining = input;
    UrmaControl control; UnpublishedSource unpublished;
    size_t accepted = 0;
    const size_t payload_capacity = std::min<size_t>(_remote_recv_block_size,
        URMA_CTP_MAX_MESSAGE_SIZE - sizeof(UrmaMsgHeader));
    const size_t limit = std::min<size_t>(UrmaControl::kMaxItems,
        std::min<size_t>(std::max(1, FLAGS_urma_read_items_per_control),
            (payload_capacity - UrmaControl::kHeaderSize) / UrmaControl::kItemSize));
    while (!remaining.empty() && control.items_size() < limit && accepted < UINT32_MAX) {
        const auto part = remaining.backing_block(0);
        const size_t length = std::min<size_t>(std::min<size_t>(part.size(),
            _shared_state->read_max_size), UINT32_MAX - accepted);
        auto* seg = GetPoolSegFor(const_cast<char*>(part.data()));
        if (!seg || !length) { errno = ERDMAMEM; return -1; }
        remaining.pop_front(length);
        const uint16_t seq = static_cast<uint16_t>(_next_send_seq + control.items_size());
        auto* item = control.add_items();
        item->set_seq(seq); item->set_length(length);
        item->set_addr(reinterpret_cast<uint64_t>(part.data()));
        item->set_token_id(seg->seg.token_id);
        accepted += length;
    }
    if (!accepted) { errno = EMSGSIZE; return -1; }
    // One registry entry owns the complete accepted prefix until batch FREE.
    butil::IOBuf source_data = input;
    source_data.pop_back(input.size() - accepted);
    if (SourceContext::Create(&unpublished.id, SourceOptions{
            _options.socket_id, _next_send_seq, &source_data, _shared_state}) != 0) {
        errno = ENOMEM; return -1;
    }
    unpublished.armed = true;
    for (size_t i = 0; i < control.items_size(); ++i) {
        control.mutable_items_begin()[i].set_source_id(unpublished.id);
    }
    if (SendControl(MSG_PULL_MEM, control, slot) != 0) return -1;
    VLOG(1) << "URMA READ PULL items=" << control.items_size() << " bytes=" << accepted;
    unpublished.armed = false;
    _next_send_seq = static_cast<uint16_t>(_next_send_seq + control.items_size());
    return accepted;
}
int UrmaPooledConnection::HandleControl(uint8_t type, const butil::IOBuf& data) {
    UrmaControl control;
    if (!_peer_imported.load() || _closed.load() || !(_shared_state->big_capabilities & URMA_BIG_CAP_READ) ||
        !control.Decode(data)) { errno = EPROTO; return -1; }
    if (type == MSG_FREE) {
        // A single FREE retires the entire PULL batch, never an individual item.
        if (control.items_size() != 1) { errno = EPROTO; return -1; }
        const auto& item = *control.items().begin();
        VersionedRefWithIdUniquePtr<SourceContext> source;
        if (item.mask != 1 || SourceContext::Address(item.source_id(), &source) != 0 ||
            source->sid != _options.socket_id || source->connection != _shared_state ||
            source->seq != item.seq() || source->data.size() != item.length()) {
            errno = EPROTO; return -1;
        }
        source->SetFailed();
        VLOG(1) << "URMA READ FREE batches=1";
        return 0;
    }
    if (type != MSG_PULL_MEM) { errno = EPROTO; return -1; }
    const auto& first = *control.items().begin();
    size_t item_index = 0;
    uint64_t total_length = 0;
    for (const auto& item : control.items()) {
        total_length += item.length();
        if (item.mask != 13 || item.source_id() != first.source_id() ||
            item.seq() != static_cast<uint16_t>(first.seq() + item_index++) ||
            total_length > UINT32_MAX ||
            !GetUrmaBlockSizeForPayload(item.length()) ||
            !_shared_state->SegmentFor(item.addr(), item.length(), item.token_id())) {
            errno = EPROTO; return -1;
        }
    }
    auto reads = std::make_shared<UrmaReadBatch>();
    reads->source = first.source_id(); reads->total_length = total_length;
    reads->connection = shared_from_this(); reads->entries.resize(control.items_size());
    reads->remaining.store(control.items_size());
    // Prepare all buffers before posting so allocation failure cannot publish
    // a partial batch or release its source storage prematurely.
    std::vector<std::unique_ptr<TxBatch, BatchDeleter>> batches;
    size_t index = 0;
    auto* slot = _provider_state->pool->io_jfs[_preferred_jfs];
    const size_t max_wr = std::min<size_t>(std::max(1, FLAGS_urma_io_tx_batch_max), URMA_TX_BATCH_MAX);
    for (const auto& item : control.items()) {
        if (batches.empty() || batches.back()->count == max_wr) {
            batches.push_back(NewBatch());
            if (!batches.back()) { errno = ENOMEM; return -1; }
        }
        auto ctx = NewContext();
        if (!ctx) { errno = ENOMEM; return -1; }
        ctx->self_socket_id = _options.socket_id; ctx->connection_state = _shared_state;
        ctx->jfs_index = slot->index; ctx->type = MSG_READ_DATA;
        ctx->seq = item.seq(); ctx->read_batch = reads; ctx->read_index = index;
        ctx->on_completed = CompleteRead;
        auto& entry = reads->entries[index++];
        entry.seq = item.seq(); entry.length = item.length();
        void* bytes = nullptr; int capacity = 0;
        {
            butil::IOBufAsZeroCopyOutputStream stream(&ctx->send_data,
                GetUrmaBlockSizeForPayload(item.length()));
            if (!stream.Next(&bytes, &capacity) || capacity < static_cast<int>(item.length())) {
                errno = ENOMEM; return -1;
            }
            stream.BackUp(capacity - item.length());
        }
        ctx->sge->push_back(urma_sge_t{}); ctx->sge->push_back(urma_sge_t{});
        ctx->sge[0].addr = item.addr(); ctx->sge[0].len = item.length();
        ctx->sge[0].tseg = _shared_state->SegmentFor(item.addr(), item.length(), item.token_id());
        ctx->sge[1].addr = reinterpret_cast<uint64_t>(bytes); ctx->sge[1].len = item.length();
        ctx->sge[1].tseg = GetPoolSegFor(bytes);
        if (!ctx->sge[1].tseg) { errno = ERDMAMEM; return -1; }
        ctx->wr.opcode = URMA_OPC_READ; ctx->wr.flag.bs.complete_enable = 1;
        ctx->wr.tjetty = _shared_state->remote_jfr;
        ctx->wr.rw.src = urma_sg_t{&ctx->sge[0], 1};
        ctx->wr.rw.dst = urma_sg_t{&ctx->sge[1], 1};
        ctx->wr.user_ctx = reinterpret_cast<uint64_t>(ctx.get());
        batches.back()->Append(ctx.release());
    }
    for (auto& batch : batches) {
        const size_t count = batch->count;
        if (_provider_state->pool->Enqueue(slot, batch.get()) != 0) {
            reads->failed.store(true); return -1;
        }
        batch.release(); posted.fetch_add(count, std::memory_order_relaxed);
    }
    return 0;
}
void UrmaPooledConnection::CompleteRead(TxCompletionCtx* ctx, const urma_cr_t& cr, void* dispatch_context) {
    auto batch = ctx->read_batch;
    if (cr.status != URMA_CR_SUCCESS) batch->failed.store(true);
    else {
        batch->entries[ctx->read_index].data.swap(ctx->send_data);
        completed.fetch_add(1, std::memory_order_relaxed);
    }
    if (batch->remaining.fetch_sub(1, std::memory_order_acq_rel) != 1 || batch->failed.load()) return;
    auto connection = batch->connection;
    UrmaControl free;
    auto* item = free.add_items();
    item->set_seq(batch->entries.front().seq);
    item->set_length(batch->total_length); item->set_source_id(batch->source);
    if (connection->SendControl(MSG_FREE, free) != 0) {
        connection->_provider_state->callbacks.on_error(connection->_options.socket_id,
                                                   connection, errno);
        return;
    }
    SocketUniquePtr socket;
    if (Socket::Address(connection->_options.socket_id, &socket) != 0) return;
    if (FindConnection(socket) != connection.get()) return;
    auto* dispatch = static_cast<UrmaRxDispatchContext*>(dispatch_context);
    InputMessageClosure last_msg;
    {
        BAIDU_SCOPED_LOCK(connection->_reorder_mutex);
        butil::IOBuf contiguous;
        for (auto& entry : batch->entries) {
            const int result = connection->AssembleFragmentLocked(
                connection->_shared_state->local_jfr_index, entry.seq, &entry.data);
            if (result < 0) {
                socket->SetFailed(EPROTO, "Invalid READ receive sequence");
                return;
            }
            if (result > 0) contiguous.append(entry.data.movable());
        }
        if (!contiguous.empty()) {
            const auto& callbacks = connection->_provider_state->callbacks;
            if (callbacks.on_bytes(socket, connection.get(), contiguous.movable())) {
                if (dispatch && FLAGS_urma_rx_batch_parse) dispatch->batch.Add(socket);
                else callbacks.on_input_ready(socket, last_msg);
            }
        }
    }
    UrmaRxDispatchContext::RetainLastMessage(last_msg, dispatch);
}
}  // namespace urma
}  // namespace brpc
#endif
