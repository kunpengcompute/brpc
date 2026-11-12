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

#include "brpc/urma/urma_resource.h"

#if BRPC_WITH_URMA
#include <sys/resource.h>
#include <unistd.h>
#include <algorithm>
#include <cstdio>
#include <cstring>
#include <mutex>
#include <limits>
#include <unordered_set>
#include <vector>
#include <gflags/gflags.h>
#include "bthread/bthread.h"
#include "bthread/butex.h"
#include "bthread/mutex.h"
#include "butil/containers/mpsc_queue.h"
#include "butil/logging.h"
#include "butil/synchronization/lock.h"
#include "butil/time.h"
#include "brpc/socket.h"
#include "brpc/urma/urma_bonding.h"
#include "brpc/urma/urma_helper.h"
#include "urma_api.h"

DECLARE_int32(task_group_ntags);
namespace brpc {
namespace urma {
DECLARE_bool(urma_use_polling);
DECLARE_int32(urma_poller_num);
DECLARE_bool(urma_disable_bthread);
DECLARE_int32(urma_sq_size);
DECLARE_int32(urma_rq_size);
DECLARE_int32(urma_cqe_poll_once);
DECLARE_bool(urma_recv_zerocopy);
DECLARE_int32(urma_zerocopy_min_size);
DECLARE_int32(urma_prepared_jetty_cnt);
DECLARE_bool(urma_poller_yield);
static const size_t IOBUF_BLOCK_HEADER_LEN = sizeof(butil::IOBuf::Block);

// Buffers are retained along with the hardware if provider teardown fails.
struct UrmaResource {
    UrmaResource* next = nullptr;
    urma_jfc_t* jfc = nullptr;
    urma_jfce_t* jfce = nullptr;
    urma_jfr_t* jfr = nullptr;
    urma_jetty_t* jetty = nullptr;
    urma_target_jetty_t* remote_jetty = nullptr;
    urma_target_seg_t* remote_seg = nullptr;
    std::vector<butil::IOBuf> retained_tx, retained_rx;
    bool Release() {
#define RELEASE_URMA_OBJECT(member, operation) \
    if (member) {                              \
        if (operation(member) != URMA_SUCCESS) \
            return false;                      \
        member = nullptr;                      \
    }
        // Stop DMA before dropping imported targets and registered data.
        RELEASE_URMA_OBJECT(jetty, urma_delete_jetty);
        RELEASE_URMA_OBJECT(jfr, urma_delete_jfr);
        RELEASE_URMA_OBJECT(remote_jetty, urma_unimport_jetty);
        RELEASE_URMA_OBJECT(remote_seg, urma_unimport_seg);
        RELEASE_URMA_OBJECT(jfc, urma_delete_jfc);
        RELEASE_URMA_OBJECT(jfce, urma_delete_jfce);
#undef RELEASE_URMA_OBJECT
        return true;
    }
};

struct CqSidOp {
    enum OpType { ADD, REMOVE } type;
    SocketId sid;
};
struct BAIDU_CACHELINE_ALIGNMENT UrmaPoller {
    bthread_t tid = INVALID_BTHREAD;
    butil::atomic<bthread_t> active_tid{INVALID_BTHREAD};
    butil::MPSCQueue<CqSidOp, butil::ObjectPoolAllocator<CqSidOp>> op_queue;
    std::function<void()> callback, init_fn, release_fn;
};
struct BAIDU_CACHELINE_ALIGNMENT UrmaPollerGroup {
    UrmaPollerGroup() : pollers(FLAGS_urma_poller_num), running(false) {}
    std::vector<UrmaPoller> pollers;
    butil::atomic<bool> running;
};
struct ExclusiveProviderState {
    bthread::Mutex mutex;
    bthread::Mutex creation_mutex;
    bthread::Mutex shutdown_mutex;
    bthread::Mutex poller_mutex;
    // weak_ptr::expired() becomes true before the destructor finishes.
    // Shutdown must also wait for native teardown/retirement to finish.
    butil::atomic<size_t> live_connections{0};
    UrmaResourceCallbacks callbacks;
    butil::atomic<bool> initialized{false};
    butil::atomic<bool> stopping{false};
    UrmaResource* prepared = nullptr;
    int prepared_count = 0;
    std::vector<UrmaPollerGroup> poller_groups;
    std::vector<std::weak_ptr<UrmaConnectionResource>> connections;
    std::vector<UrmaResource*> retired;

    void Retire(UrmaResource* r) {
        if (!r)
            return;
        if (r->Release()) {
            delete r;
            return;
        }
        BAIDU_SCOPED_LOCK(mutex);
        retired.push_back(r);
    }
};

class UrmaExclusiveConnection final
    : public UrmaConnectionResource,
      public std::enable_shared_from_this<UrmaExclusiveConnection> {
public:
    UrmaExclusiveConnection(
        const UrmaConnectionOptions& options,
        const std::shared_ptr<ExclusiveProviderState>& state);
    ~UrmaExclusiveConnection() override;
    int Initialize();
    int GetLocalDescription(UrmaResourceDescription* out) const override;
    int BindPeer(const UrmaResourceDescription& peer) override;
    ssize_t Send(butil::IOBuf** data, size_t ndata) override;
    bool IsWritable() const override;
    int WaitWritable(butil::atomic<int>* butex, int expected,
                     const timespec* deadline) override;
    void Close() override;
    void DebugInfo(std::ostream& out) const override;
    static void PollCq(Socket* socket);

private:
    struct Carrier : SocketUser {
        std::weak_ptr<UrmaExclusiveConnection> connection;
        void BeforeRecycle(Socket*) override { delete this; }
    };
    void DrainCq(Socket* carrier);
    int AllocateResources();
    int ImportPeer(const UrmaResourceDescription& peer);
    int PostRecv(uint32_t count, bool zerocopy);
    int DoPostRecv(void* data, size_t size);
    int SendImm(uint32_t imm);
    int SendAck(int count);
    ssize_t HandleCompletion(const urma_cr_t& cr, butil::IOBuf* received);
    int WaitCqEvent(urma_jfc_t** event);
    int ReqNotifyCq();
    void PollerAddCqSid();
    void PollerRemoveCqSid();
    void NotifyWritable();
    void ReportError(int error);
    const UrmaConnectionOptions _options;
    std::shared_ptr<ExclusiveProviderState> _provider;
    UrmaResource* _resource = nullptr;
    SocketId _cq_sid = INVALID_SOCKET_ID;
    // Control path only: serialize initialization/binding with Close. Native
    // SEND/ACK posts use provider synchronization and never take this mutex.
    butil::Mutex _lifecycle_mutex;
    butil::atomic<bool> _closed{false};
    butil::atomic<bool> _bound{false};
    uint16_t _sq_size = 0, _rq_size = 0;
    std::vector<butil::IOBuf> _sbuf, _rbuf;
    std::vector<void*> _rbuf_data;
    uint32_t _remote_recv_block_size = 0;
    uint16_t _local_window_capacity = 0, _remote_window_capacity = 0;
    butil::atomic<uint16_t> _remote_rq_window_size{0}, _sq_window_size{0};
    butil::atomic<uint16_t> _new_rq_wrs{0};
    butil::atomic<uint16_t> _sq_imm_window_size{0};
    uint16_t _sq_current = 0, _sq_sent = 0, _rq_received = 0;
    static constexpr uint16_t RESERVED_WR_NUM = 3;
};

class UrmaExclusiveResourceProvider final : public UrmaResourceProvider {
public:
    UrmaExclusiveResourceProvider() : _state(new ExclusiveProviderState) {}
    int Initialize(const UrmaResourceCallbacks& callbacks) override;
    int CreateConnection(const UrmaConnectionOptions& options,
                         UrmaConnectionHandle* out) override;
    int InitializePollingMode(bthread_tag_t tag, std::function<void()> callback,
                              std::function<void()> init_fn,
                              std::function<void()> release_fn) override;
    void ReleasePollingMode(bthread_tag_t tag) override;
    bool Shutdown() override;

private:
    bool RunningOnPoller() const;
    int Prepare();
    std::shared_ptr<ExclusiveProviderState> _state;
};
static int PreparedJettyCount() {
    const int requested =
        std::max(0, std::min(FLAGS_urma_prepared_jetty_cnt, 1024));
    if (requested == 0) {
        return 0;
    }

    struct rlimit nofile;
    if (getrlimit(RLIMIT_NOFILE, &nofile) != 0 ||
        nofile.rlim_cur == RLIM_INFINITY) {
        return requested;
    }

    // In event mode each prepared JFCE consumes a file descriptor. Keep room
    // for one TCP fd per future URMA connection and for brpc/system internals.
    static const rlim_t kReservedFdCount = 64;
    const rlim_t max_prepared = nofile.rlim_cur > kReservedFdCount
                                    ? (nofile.rlim_cur - kReservedFdCount) / 2
                                    : 0;
    if (max_prepared >= static_cast<rlim_t>(requested)) {
        return requested;
    }

    LOG(WARNING) << "Cap URMA prepared jetty count from " << requested << " to "
                 << max_prepared << " due to RLIMIT_NOFILE=" << nofile.rlim_cur;
    return static_cast<int>(max_prepared);
}

UrmaExclusiveConnection::UrmaExclusiveConnection(
    const UrmaConnectionOptions& options,
    const std::shared_ptr<ExclusiveProviderState>& state)
    : _options(options), _provider(state) {
    _provider->live_connections.fetch_add(1, butil::memory_order_relaxed);
    _sq_size =
        static_cast<uint16_t>(std::max(16, std::min(4096, FLAGS_urma_sq_size)));
    _rq_size =
        static_cast<uint16_t>(std::max(16, std::min(4096, FLAGS_urma_rq_size)));
}

UrmaExclusiveConnection::~UrmaExclusiveConnection() {
    Close();
    if (_resource) {
        _resource->retained_tx.swap(_sbuf);
        _resource->retained_rx.swap(_rbuf);
        _provider->Retire(_resource);
    }
    _provider->live_connections.fetch_sub(1, butil::memory_order_release);
}

int UrmaExclusiveConnection::Initialize() {
    BAIDU_SCOPED_LOCK(_lifecycle_mutex);
    if (_closed.load(butil::memory_order_acquire)) {
        errno = EPIPE;
        return -1;
    }
    return AllocateResources();
}
int UrmaExclusiveConnection::AllocateResources() {
    if (_resource) {
        return 0;
    }
    urma_context_t* ctx = GetUrmaContext();
    if (!ctx) {
        errno = ENODEV;
        return -1;
    }

    _resource = new (std::nothrow) UrmaResource();
    if (!_resource) {
        return -1;
    }

    // Try the prepared pool first (sized sq/rq match).
    if (_sq_size <= static_cast<uint16_t>(FLAGS_urma_sq_size) &&
        _rq_size <= static_cast<uint16_t>(FLAGS_urma_rq_size)) {
        BAIDU_SCOPED_LOCK(_provider->mutex);
        if (_provider->prepared) {
            UrmaResource* next = _provider->prepared->next;
            delete _resource;
            _resource = _provider->prepared;
            _provider->prepared = next;
            _resource->next = nullptr;
            --_provider->prepared_count;
        }
    }

    if (!_resource->jfc) {
        // The SDK requires every JFC to reference a JFCE. Polling mode does
        // not arm or consume it, but still supplies the required object.
        _resource->jfce = urma_create_jfce(ctx);
        if (!_resource->jfce ||
            (!FLAGS_urma_use_polling && _resource->jfce->fd < 0)) {
            LOG(ERROR) << "Fail to create a usable URMA JFCE";
            errno = ENODEV;
            return -1;
        }

        urma_jfc_cfg_t jfc_cfg{};
        jfc_cfg.depth = static_cast<uint32_t>(_sq_size + _rq_size);
        jfc_cfg.jfce = _resource->jfce;
        _resource->jfc = urma_create_jfc(ctx, &jfc_cfg);
        if (!_resource->jfc) {
            PLOG(ERROR) << "urma_create_jfc";
            return -1;
        }

        urma_jfr_cfg_t jfr_cfg{};
        jfr_cfg.depth = static_cast<uint32_t>(_rq_size);
        jfr_cfg.trans_mode = URMA_TM_RM;
        jfr_cfg.max_sge = 1;
        jfr_cfg.min_rnr_timer = URMA_TYPICAL_MIN_RNR_TIMER;
        jfr_cfg.jfc = _resource->jfc;
        _resource->jfr = urma_create_jfr(ctx, &jfr_cfg);
        if (!_resource->jfr) {
            PLOG(ERROR) << "urma_create_jfr";
            return -1;
        }

        urma_jetty_cfg_t jetty_cfg{};
        jetty_cfg.flag.bs.share_jfr = 1;
        jetty_cfg.jfs_cfg.depth = static_cast<uint32_t>(_sq_size);
        jetty_cfg.jfs_cfg.trans_mode = URMA_TM_RM;
        jetty_cfg.jfs_cfg.priority = GetUrmaJettyPriority();
        jetty_cfg.jfs_cfg.max_sge = static_cast<uint8_t>(GetUrmaMaxSge());
        jetty_cfg.jfs_cfg.rnr_retry = URMA_TYPICAL_RNR_RETRY;
        jetty_cfg.jfs_cfg.err_timeout = URMA_TYPICAL_ERR_TIMEOUT;
        jetty_cfg.jfs_cfg.jfc = _resource->jfc;
        jetty_cfg.shared.jfr = _resource->jfr;
        jetty_cfg.shared.jfc = _resource->jfc;
        _resource->jetty = urma_create_jetty(ctx, &jetty_cfg);
        if (!_resource->jetty) {
            PLOG(ERROR) << "urma_create_jetty";
            return -1;
        }
    }

    _sbuf.resize(_sq_size - RESERVED_WR_NUM);
    _rbuf.resize(_rq_size);
    _rbuf_data.resize(_rq_size, nullptr);

    if ((_options.role == UrmaConnectionRole::CLIENT ||
         !IsUrmaBondingDevice()) &&
        PostRecv(_rq_size, FLAGS_urma_recv_zerocopy) != 0)
        return -1;

    // Wrap the JFCE fd in a brpc Socket so PollCq is driven by epoll.
    if (!FLAGS_urma_use_polling) {
        if (!_resource->jfce || _resource->jfce->fd < 0) {
            LOG(ERROR) << "Prepared URMA resource has no usable JFCE";
            errno = ENODEV;
            return -1;
        }
        if (ReqNotifyCq() != 0) {
            return -1;
        }
        SocketOptions options;
        std::unique_ptr<Carrier> carrier(new Carrier);
        carrier->connection = shared_from_this();
        options.user = carrier.get();
        options.keytable_pool = _options.keytable_pool;
        options.bthread_tag = _options.tag;
        options.fd = _resource->jfce->fd;
        options.on_edge_triggered_events = PollCq;
        if (Socket::Create(options, &_cq_sid) < 0) {
            PLOG(ERROR) << "Fail to create CQ socket";
            return -1;
        }
        carrier.release();
    } else {
        // Polling mode: synthetic carrier socket (no fd).
        SocketOptions options;
        std::unique_ptr<Carrier> carrier(new Carrier);
        carrier->connection = shared_from_this();
        options.user = carrier.get();
        options.keytable_pool = _options.keytable_pool;
        options.bthread_tag = _options.tag;
        options.on_edge_triggered_events = PollCq;
        if (Socket::Create(options, &_cq_sid) < 0) {
            PLOG(ERROR) << "Fail to create CQ socket (polling)";
            return -1;
        }
        carrier.release();
        PollerAddCqSid();
    }
    return 0;
}
int UrmaExclusiveConnection::GetLocalDescription(
    UrmaResourceDescription* out) const {
    if (!out) {
        errno = EINVAL;
        return -1;
    }
    if (_closed.load(butil::memory_order_acquire)) {
        errno = EPIPE;
        return -1;
    }
    *out = UrmaResourceDescription{};
    out->recv_buffer_size = static_cast<uint32_t>(GetUrmaRecvBlockSize());
    out->recv_buffer_count = _rq_size - 1;
    if (_resource && _resource->jetty) {
        out->resource_id = _resource->jetty->jetty_id.id;
        out->uasid = _resource->jetty->jetty_id.uasid;
        const urma_eid_t* local_eid = GetUrmaLocalEid();
        const uint8_t* advertised_eid =
            local_eid != nullptr ? local_eid->raw
                                 : _resource->jetty->jetty_id.eid.raw;
        std::memcpy(out->eid.data(), advertised_eid, 16);
    }
    out->tp_type = static_cast<uint8_t>(URMA_CTP);
    // Pool segment: flatten g_pool_seg's seg fields.
    urma_target_seg_t* pool = GetPoolSegFor(nullptr);
    if (pool) {
        std::memcpy(out->segment.eid.data(), pool->seg.ubva.eid.raw, 16);
        out->segment.uasid = pool->seg.ubva.uasid;
        out->segment.va = pool->seg.ubva.va;
        out->segment.length = pool->seg.len;
        out->segment.token_id = pool->seg.token_id;
    }
    return 0;
}
int UrmaExclusiveConnection::ImportPeer(const UrmaResourceDescription& peer) {
    urma_context_t* ctx = GetUrmaContext();
    if (!ctx) {
        errno = ENODEV;
        return -1;
    }

    // 1. urma_import_seg FIRST so the kernel establishes TP routing for the
    //    remote EID. Without this the first SEND is rejected by hardware with
    //    URMA_CR_RNR_RETRY_CNT_EXC_ERR.
    urma_seg_t peer_seg{};
    std::memcpy(peer_seg.ubva.eid.raw, peer.segment.eid.data(), 16);
    peer_seg.ubva.uasid = peer.segment.uasid;
    peer_seg.ubva.va = peer.segment.va;
    peer_seg.len = peer.segment.length;
    peer_seg.token_id = peer.segment.token_id;
    urma_token_t seg_token{};
    urma_import_seg_flag_t seg_flag{};
    seg_flag.bs.cacheable = URMA_NON_CACHEABLE;
    seg_flag.bs.access =
        URMA_ACCESS_READ | URMA_ACCESS_WRITE | URMA_ACCESS_ATOMIC;
    seg_flag.bs.mapping = URMA_SEG_NOMAP;
    _resource->remote_seg =
        urma_import_seg(ctx, &peer_seg, &seg_token, 0, seg_flag);
    if (!_resource->remote_seg) {
        PLOG(ERROR) << "urma_import_seg failed";
        return -1;
    }

    // 2. urma_import_jetty.
    urma_rjetty_t remote{};
    std::memcpy(remote.jetty_id.eid.raw, peer.eid.data(), 16);
    remote.jetty_id.uasid = peer.uasid;
    remote.jetty_id.id = peer.resource_id;
    remote.trans_mode = URMA_TM_RM;
    remote.type = URMA_JETTY;
    if (peer.tp_type > static_cast<uint8_t>(URMA_UTP)) {
        errno = EPROTO;
        return -1;
    }
    remote.tp_type = static_cast<urma_tp_type_t>(peer.tp_type);

    urma_token_t token{};
    const bool use_bonding_extension =
        IsUrmaBondingDevice() && remote.trans_mode == URMA_TM_RM;
    errno = 0;
    if (use_bonding_extension) {
#if BRPC_URMA_HAS_BONDING_EXT
        // The bonding provider needs the local jetty to associate its send
        // path with the imported target. A plain import may return success
        // without setting that association, leaving traffic one-way only.
        bondp_rjetty_t bonding_remote{};
        bonding_remote.base = remote;
        bonding_remote.base.flag.bs.has_drv_ext = 1;
        bonding_remote.jetty = _resource->jetty;
        _resource->remote_jetty =
            urma_import_jetty(ctx, &bonding_remote.base, &token);
#else
        LOG(ERROR) << "Bonding remote jetty import requires provider header "
                      "urma_ubagg.h";
        errno = ENOTSUP;
#endif
    } else {
        _resource->remote_jetty = urma_import_jetty(ctx, &remote, &token);
    }
    if (!_resource->remote_jetty) {
        if (errno == 0) {
            errno = EIO;
        }
        char remote_eid[URMA_EID_STR_LEN + 1] = {};
        std::snprintf(remote_eid, sizeof(remote_eid), EID_FMT,
                      EID_RAW_ARGS(peer.eid.data()));
        PLOG(ERROR) << "urma_import_jetty failed"
                    << " remote_eid=" << remote_eid
                    << " remote_uasid=" << peer.uasid
                    << " remote_jetty_id=" << peer.resource_id
                    << " trans_mode=" << remote.trans_mode
                    << " tp_type=" << remote.tp_type
                    << " bonding_extension=" << use_bonding_extension;
        return -1;
    }
    return 0;
}
int UrmaExclusiveConnection::BindPeer(const UrmaResourceDescription& remote) {
    std::unique_lock<butil::Mutex> lifecycle(_lifecycle_mutex);
    if (_closed.load(butil::memory_order_acquire)) {
        errno = EPIPE;
        return -1;
    }
    if (_bound.load(butil::memory_order_acquire)) {
        errno = EALREADY;
        return -1;
    }
    if (remote.mode != UrmaResourceMode::EXCLUSIVE ||
        remote.resource_kind != UrmaPeerResourceKind::JETTY ||
        remote.receive_count_kind != UrmaReceiveCountKind::CONNECTION_CREDIT ||
        remote.has_socket_id || !remote.resource_id ||
        remote.recv_buffer_size < 1024 || remote.recv_buffer_count < 3 ||
        remote.recv_buffer_count > 65535 ||
        remote.tp_type > static_cast<uint8_t>(URMA_UTP) ||
        !remote.segment.length || !remote.segment.va ||
        remote.segment.length >
            std::numeric_limits<uint64_t>::max() - remote.segment.va) {
        errno = EPROTO;
        return -1;
    }
    _remote_recv_block_size = remote.recv_buffer_size;
    const uint32_t peer_rq_size = remote.recv_buffer_count + 1;
    const uint32_t local_capacity = std::min<uint32_t>(_sq_size, peer_rq_size);
    _local_window_capacity = static_cast<uint16_t>(
        local_capacity > RESERVED_WR_NUM ? local_capacity - RESERVED_WR_NUM
                                         : 0);
    _remote_window_capacity =
        _rq_size > RESERVED_WR_NUM ? _rq_size - RESERVED_WR_NUM : 0;
    _sq_imm_window_size.store(RESERVED_WR_NUM, butil::memory_order_relaxed);
    _remote_rq_window_size.store(_local_window_capacity,
                                 butil::memory_order_relaxed);
    _sq_window_size.store(_local_window_capacity, butil::memory_order_relaxed);
    if (ImportPeer(remote) != 0 ||
        (_options.role == UrmaConnectionRole::SERVER && IsUrmaBondingDevice() &&
         PostRecv(_rq_size, FLAGS_urma_recv_zerocopy) != 0)) {
        const int saved_errno = errno ? errno : EIO;
        lifecycle.unlock();
        // Import/prepost may have partially succeeded. Never replace those
        // targets or reuse posted RX buffers in a second BindPeer attempt.
        Close();
        errno = saved_errno;
        return -1;
    }
    _bound.store(true, butil::memory_order_release);
    lifecycle.unlock();
    // Early receive completions may have exhausted peer credit while this
    // side was still importing. No future CQE is required to flush it.
    return SendAck(0);
}

// Build SGEs and transfer their references directly into the SQ slot. Whole
// block refs move without the retain-then-pop pass; partial refs stay shared.
class UrmaIOBuf : private butil::IOBuf {
public:
    ssize_t CutIntoSgList(urma_sge_t* sges, size_t* count, butil::IOBuf* to,
                          size_t max_sge, size_t max_len) {
        size_t length = 0;
        while (*count < max_sge && length < max_len && !empty()) {
            const BlockRef& ref = _ref_at(0);
            void* start = ref.block->data + ref.offset;
            urma_target_seg_t* seg = GetPoolSegFor(start);
            if (!seg &&
                (ref.block->flags & butil::IOBUF_BLOCK_FLAGS_USER_DATA)) {
                seg = reinterpret_cast<urma_target_seg_t*>(
                    ref.block->u.data_meta);
            }
            if (!seg) {
                errno = ERDMAMEM;
                return -1;
            }
            const size_t n = std::min<size_t>(ref.length, max_len - length);
            sges[*count] = urma_sge_t{reinterpret_cast<uint64_t>(start),
                                      static_cast<uint32_t>(n), seg, nullptr};
            cutn(to, n);
            length += n;
            ++*count;
        }
        return length;
    }
};
ssize_t UrmaExclusiveConnection::Send(butil::IOBuf** from, size_t ndata) {
    // Socket serializes data writers. The provider synchronizes native posts
    // against CQ-side ACKs; the caller's handle pins resources across Close.
    if (_closed.load(butil::memory_order_acquire)) {
        errno = EPIPE;
        return -1;
    }
    if (ndata == 0)
        return 0;
    if (!from) {
        errno = EINVAL;
        return -1;
    }
    bool has_data = false;
    for (size_t i = 0; i < ndata; ++i) {
        if (!from[i]) {
            errno = EINVAL;
            return -1;
        }
        has_data = has_data || !from[i]->empty();
    }
    if (!has_data)
        return 0;
    if (!_bound.load(butil::memory_order_acquire)) {
        errno = ENOTCONN;
        return -1;
    }
    if (!_resource || !_resource->jetty || !_resource->remote_jetty) {
        errno = ENOTCONN;
        return -1;
    }
    int max_sge = GetUrmaMaxSge();
    if (max_sge < 1) {
        max_sge = 1;
    }

    urma_sge_t* sglist =
        static_cast<urma_sge_t*>(alloca(sizeof(urma_sge_t) * max_sge));
    if (!sglist) {
        errno = ENOMEM;
        return -1;
    }

    // At most one input slice per SGE. This journal is used only to restore
    // the current, unsubmitted WR after a gather/close/provider failure.
    struct InputSlice {
        butil::IOBuf* input;
        size_t bytes;
    };
    auto* consumed = static_cast<InputSlice*>(
        alloca(sizeof(InputSlice) * max_sge));
    size_t nconsumed = 0;

    size_t current = 0;
    ssize_t total_len = 0;
    auto failed = [&](int error) -> ssize_t {
        // No CQE can reference an unsubmitted WR. Restore each prefix before
        // its original suffix without copying payload or losing user metadata.
        butil::IOBuf* staged = &_sbuf[_sq_current];
        for (size_t i = 0; i < nconsumed; ++i) {
            butil::IOBuf prefix;
            staged->cutn(&prefix, consumed[i].bytes);
            prefix.append(consumed[i].input->movable());
            consumed[i].input->swap(prefix);
        }
        staged->clear();
        ReportError(error);
        errno = error;
        return total_len > 0 ? total_len : -1;
    };
    while (current < ndata) {
        nconsumed = 0;
        if (_closed.load(butil::memory_order_acquire)) {
            errno = EPIPE;
            return total_len > 0 ? total_len : -1;
        }
        while (current < ndata && from[current]->empty())
            ++current;
        if (current == ndata)
            break;
        uint16_t remote_wnd =
            _remote_rq_window_size.load(butil::memory_order_relaxed);
        uint16_t sq_wnd = _sq_window_size.load(butil::memory_order_acquire);
        if (remote_wnd == 0 || sq_wnd == 0) {
            if (total_len > 0) {
                break;
            }
            errno = EAGAIN;
            return -1;
        }
        butil::IOBuf* to = &_sbuf[_sq_current];
        size_t sge_index = 0;
        size_t this_len = 0;
        size_t max_len = _remote_recv_block_size > 0 ? _remote_recv_block_size
                                                     : GetUrmaRecvBlockSize();
        while (sge_index < static_cast<size_t>(max_sge) && this_len < max_len &&
               current < ndata) {
            butil::IOBuf* input = from[current];
            if (input->empty()) {
                ++current;
                continue;
            }
            const size_t before = input->size();
            const ssize_t n = reinterpret_cast<UrmaIOBuf*>(input)->CutIntoSgList(
                sglist, &sge_index, to, max_sge, max_len - this_len);
            const size_t bytes = before - input->size();
            if (bytes) consumed[nconsumed++] = InputSlice{input, bytes};
            if (n < 0)
                return failed(errno);
            this_len += n;
        }
        if (sge_index == 0) {
            break;
        }

        urma_sg_t sg{sglist, static_cast<uint32_t>(sge_index)};
        urma_jfs_wr_t wr{};
        std::memset(&wr, 0, sizeof(wr));
        // Send payload with URMA_OPC_SEND. Receive credits are flushed
        // separately by SendImm() after SendAck() reaches its threshold.
        // Piggybacking credits turns every payload into SEND_IMM and can
        // produce asymmetric completions with the bonding provider.
        wr.opcode = URMA_OPC_SEND;
        wr.flag.bs.complete_enable = 1;
        wr.tjetty = _resource->remote_jetty;
        wr.send.src = sg;
        wr.user_ctx = 1;
        urma_jfs_wr_t* bad_wr = nullptr;
        const uint16_t sq_slot = _sq_current;
        const uint32_t local_jetty_id = _resource->jetty->jetty_id.id;
        const uint32_t remote_jetty_id = _resource->remote_jetty->id.id;

        // Reserve both credits before making the WR visible to the provider.
        // In polling mode a completion (and even the peer's receive-credit
        // ACK) can be processed by another thread before post_send returns.
        // Decrementing after post therefore creates a transient capacity + 1
        // window and makes the strict credit check tear down a healthy
        // connection.
        if (_closed.load(butil::memory_order_acquire)) {
            return failed(EPIPE);
        }
        _remote_rq_window_size.fetch_sub(1, butil::memory_order_relaxed);
        _sq_window_size.fetch_sub(1, butil::memory_order_relaxed);
        int rc = urma_post_jetty_send_wr(_resource->jetty, &wr, &bad_wr);
        if (rc != URMA_SUCCESS) {
            const int provider_errno = errno;
            _remote_rq_window_size.fetch_add(1, butil::memory_order_relaxed);
            _sq_window_size.fetch_add(1, butil::memory_order_relaxed);
            LOG(WARNING) << "urma_post_jetty_send_wr failed: " << rc
                         << ", provider_errno=" << provider_errno << " ("
                         << berror(provider_errno) << ')'
                         << ", bad_wr=" << static_cast<const void*>(bad_wr)
                         << ", bad_is_current=" << (bad_wr == &wr)
                         << ", sq_slot=" << sq_slot
                         << ", local_jetty_id=" << local_jetty_id
                         << ", remote_jetty_id=" << remote_jetty_id
                         << ", sq_window=" << sq_wnd
                         << ", remote_rq_window=" << remote_wnd
                         << ", num_sge=" << sge_index
                         << ", configured_max_sge=" << GetUrmaMaxSge()
                         << ", payload_size=" << this_len << " on "
                         << _options.socket_id;
            return failed(provider_errno ? provider_errno : EIO);
        }
        _sq_current = (_sq_current + 1) % (_sq_size - RESERVED_WR_NUM);
        total_len += static_cast<ssize_t>(this_len);
    }
    return total_len;
}

bool UrmaExclusiveConnection::IsWritable() const {
    return !_closed.load(butil::memory_order_acquire) &&
           _bound.load(butil::memory_order_acquire) &&
           _remote_rq_window_size.load(butil::memory_order_relaxed) > 0 &&
           _sq_window_size.load(butil::memory_order_acquire) > 0;
}

int UrmaExclusiveConnection::WaitWritable(butil::atomic<int>* butex,
                                          int expected,
                                          const timespec* deadline) {
    if (!butex) {
        errno = EINVAL;
        return -1;
    }
    if (_closed.load(butil::memory_order_acquire)) {
        errno = EPIPE;
        return -1;
    }
    if (!_bound.load(butil::memory_order_acquire)) {
        errno = ENOTCONN;
        return -1;
    }
    // A resource handle pins the Jetty, not its owning Socket. Pin the Socket
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
    const int rc = bthread::butex_wait(butex, expected, deadline);
    if (_closed.load(butil::memory_order_acquire)) {
        errno = EPIPE;
        return -1;
    }
    return rc;
}

void UrmaExclusiveConnection::NotifyWritable() {
    // Resolve the versioned id for each notification. Never retain a raw
    // Socket/butex pointer: an in-flight CQ may outlive the Endpoint owner.
    SocketUniquePtr socket;
    if (Socket::Address(_options.socket_id, &socket) == 0) {
        socket->WakeAsEpollOut();
    }
}

void UrmaExclusiveConnection::ReportError(int error) {
    if (!_closed.load(butil::memory_order_acquire)) {
        _provider->callbacks.on_error(_options.socket_id, shared_from_this(),
                                      error);
    }
}

void UrmaExclusiveConnection::Close() {
    {
        BAIDU_SCOPED_LOCK(_lifecycle_mutex);
        if (_closed.exchange(true, butil::memory_order_acq_rel))
            return;
    }
    NotifyWritable();
    if (FLAGS_urma_use_polling)
        PollerRemoveCqSid();
    if (_cq_sid != INVALID_SOCKET_ID) {
        SocketUniquePtr carrier;
        if (Socket::Address(_cq_sid, &carrier) == 0) {
            if (carrier->fd() >= 0) {
                carrier->_io_event.RemoveConsumer(carrier->_fd);
            }
            // The provider owns the fd; carrier owns only its weak user.
            carrier->_fd = -1;
            carrier->SetFailed();
        }
    }
    // A SEND/ACK that already observed the open state may finish posting after
    // this point. Actual teardown waits for its Send/CQ/callback handle to exit.
}
int UrmaExclusiveConnection::DoPostRecv(void* block, size_t block_size) {
    urma_target_seg_t* tseg = GetPoolSegFor(block);
    if (!tseg) {
        errno = ERDMAMEM;
        return -1;
    }
    urma_sge_t sge{reinterpret_cast<uint64_t>(block),
                   static_cast<uint32_t>(block_size), tseg, nullptr};
    urma_sg_t sg{&sge, 1};
    urma_jfr_wr_t wr{sg, 0, nullptr};
    urma_jfr_wr_t* bad = nullptr;
    // Use the shared-JFR path on every device, including bonding. The bonding
    // provider owns physical receive scheduling for the JFR; a local
    // jetty-to-target association is not part of the RM receive API.
    const urma_status_t status = urma_post_jfr_wr(_resource->jfr, &wr, &bad);
    if (status != URMA_SUCCESS) {
        LOG(WARNING) << "Failed to post URMA receive WR: status=" << status
                     << " bonding=" << IsUrmaBondingDevice()
                     << " bad_wr=" << static_cast<const void*>(bad)
                     << " bad_is_current=" << (bad == &wr)
                     << " local_jetty_id=" << _resource->jetty->jetty_id.id
                     << " provider_associated_remote="
                     << static_cast<const void*>(_resource->jetty->remote_jetty)
                     << " on " << _options.socket_id;
        errno = status;
        return -1;
    }
    return 0;
}
int UrmaExclusiveConnection::PostRecv(uint32_t num, bool zerocopy) {
    for (uint32_t i = 0; i < num; ++i) {
        size_t block_size = GetUrmaRecvBlockSize();
        if (zerocopy) {
            _rbuf[_rq_received].clear();
            butil::IOBufAsZeroCopyOutputStream zcis(
                &_rbuf[_rq_received], block_size + IOBUF_BLOCK_HEADER_LEN);
            void* data = nullptr;
            int size = 0;
            if (!zcis.Next(&data, &size) || !data ||
                size < static_cast<int>(block_size)) {
                errno = ENOMEM;
                return -1;
            }
            _rbuf_data[_rq_received] = data;
            if (DoPostRecv(data, block_size) < 0) {
                return -1;
            }
        } else {
            if (_rbuf_data[_rq_received] == nullptr) {
                _rbuf[_rq_received].clear();
                butil::IOBufAsZeroCopyOutputStream zcos(
                    &_rbuf[_rq_received], block_size + IOBUF_BLOCK_HEADER_LEN);
                void* data = nullptr;
                int size = 0;
                if (!zcos.Next(&data, &size) || !data ||
                    size < static_cast<int>(block_size)) {
                    errno = ENOMEM;
                    return -1;
                }
                _rbuf_data[_rq_received] = data;
            }
            if (DoPostRecv(_rbuf_data[_rq_received], block_size) < 0) {
                return -1;
            }
        }
        _rq_received = (_rq_received + 1) % _rq_size;
    }
    return 0;
}
int UrmaExclusiveConnection::SendImm(uint32_t imm) {
    if (_closed.load(butil::memory_order_acquire)) {
        errno = EPIPE;
        return -1;
    }
    if (imm == 0) {
        return 0;
    }
    if (!_resource || !_resource->jetty || !_resource->remote_jetty) {
        errno = ENOTCONN;
        return -1;
    }
    // BindPeer's initial credit flush can overlap the CQ's ACK producer.
    // Reserve a slot atomically; provider locking does not protect our budget.
    uint16_t available = _sq_imm_window_size.load(butil::memory_order_relaxed);
    while (true) {
        if (available == 0) {
            _new_rq_wrs.fetch_add(imm, butil::memory_order_relaxed);
            errno = EAGAIN;
            return -1;
        }
        if (_sq_imm_window_size.compare_exchange_weak(
                available, static_cast<uint16_t>(available - 1),
                butil::memory_order_relaxed)) {
            break;
        }
    }
    // Empty-payload SEND_IMM flushes peer-side receive credit. Connection
    // lifetime is owned by the TCP fd, so this is not an EOF marker.
    urma_jfs_wr_t wr{};
    std::memset(&wr, 0, sizeof(wr));
    wr.opcode = URMA_OPC_SEND_IMM;
    wr.flag.bs.complete_enable = 1;
    wr.flag.bs.solicited_enable = 1;
    wr.tjetty = _resource->remote_jetty;
    wr.send.imm_data = imm;
    wr.user_ctx = 0;  // 0 == pure ack (HandleCompletion reuses budget).
    urma_jfs_wr_t* bad = nullptr;
    // Reserve the ACK-only SQ slot before posting for the same reason as the
    // data windows in CutFromIOBufList: polling may observe its completion as
    // soon as the provider accepts the WR.
    const urma_status_t status =
        urma_post_jetty_send_wr(_resource->jetty, &wr, &bad);
    if (status != URMA_SUCCESS) {
        const int provider_errno = errno;
        _sq_imm_window_size.fetch_add(1, butil::memory_order_relaxed);
        _new_rq_wrs.fetch_add(imm, butil::memory_order_relaxed);
        LOG(WARNING) << "Failed to post URMA credit ACK: status=" << status
                     << " provider_errno=" << provider_errno << " ("
                     << berror(provider_errno) << ')'
                     << " bad_wr=" << static_cast<const void*>(bad)
                     << " bad_is_current=" << (bad == &wr) << " imm=" << imm
                     << " local_jetty_id=" << _resource->jetty->jetty_id.id
                     << " remote_jetty_id=" << _resource->remote_jetty->id.id
                     << " on " << _options.socket_id;
        errno = status;
        return -1;
    }
    return 0;
}
int UrmaExclusiveConnection::SendAck(int num) {
    const uint16_t old =
        _new_rq_wrs.fetch_add(num, butil::memory_order_relaxed);
    if (_bound.load(butil::memory_order_acquire) &&
        old + num > _remote_window_capacity / 2 &&
        _sq_imm_window_size.load(butil::memory_order_relaxed) > 0) {
        return SendImm(_new_rq_wrs.exchange(0, butil::memory_order_relaxed));
    }
    return 0;
}
ssize_t UrmaExclusiveConnection::HandleCompletion(const urma_cr_t& cr,
                                                  butil::IOBuf* received) {
    bool zerocopy = FLAGS_urma_recv_zerocopy;
    if (cr.status != URMA_CR_SUCCESS) {
        LOG(WARNING) << "URMA completion failed, status=" << cr.status;
        errno = EIO;
        return -1;
    }
    const bool bound = _bound.load(butil::memory_order_acquire);
    if ((!cr.flag.bs.s_r || cr.opcode == URMA_CR_OPC_SEND_WITH_IMM) && !bound) {
        errno = EPROTO;
        return -1;
    }
    if (cr.flag.bs.s_r == 0) {
        // Send completion: reclaim SQ window and wake the writer.
        if (cr.user_ctx == 0) {
            // Pure-ack WR: just replenish the imm budget.
            if (_sq_imm_window_size.load(butil::memory_order_relaxed) >=
                RESERVED_WR_NUM) {
                LOG(WARNING)
                    << "URMA credit-ACK completion exceeds reserved SQ "
                       "window: current="
                    << _sq_imm_window_size.load(butil::memory_order_relaxed)
                    << " capacity=" << RESERVED_WR_NUM << " on "
                    << _options.socket_id;
                errno = EPROTO;
                return -1;
            }
            _sq_imm_window_size.fetch_add(1, butil::memory_order_relaxed);
            SendAck(0);
            return 0;
        }
        const uint16_t old = _sq_window_size.load(butil::memory_order_relaxed);
        if (old >= _local_window_capacity) {
            errno = EPROTO;
            return -1;
        }
        _sbuf[_sq_sent].clear();
        _sq_sent = (_sq_sent + 1) % (_sq_size - RESERVED_WR_NUM);
        _sq_window_size.fetch_add(1, butil::memory_order_release);
        butil::subtle::MemoryBarrier();
        if (_remote_rq_window_size.load(butil::memory_order_relaxed) >=
            _local_window_capacity / 8) {
            NotifyWritable();
        }
        return 0;
    }
    // Recv completion.
    if (cr.opcode == URMA_CR_OPC_SEND_WITH_IMM && cr.imm_data > 0) {
        if (cr.imm_data > _local_window_capacity) {
            LOG(WARNING) << "Invalid URMA receive credit: " << cr.imm_data;
            errno = EPROTO;
            return -1;
        }
        const uint16_t acks = static_cast<uint16_t>(cr.imm_data);
        uint16_t old = _remote_rq_window_size.load(butil::memory_order_relaxed);
        while (true) {
            if (old > _local_window_capacity - acks) {
                LOG(WARNING)
                    << "URMA receive credit exceeds window: old=" << old
                    << " credit=" << acks
                    << " capacity=" << _local_window_capacity
                    << " imm=" << cr.imm_data
                    << " remote_window_capacity=" << _remote_window_capacity
                    << " on " << _options.socket_id;
                errno = EPROTO;
                return -1;
            }
            if (_remote_rq_window_size.compare_exchange_weak(
                    old, static_cast<uint16_t>(old + acks),
                    butil::memory_order_relaxed)) {
                break;
            }
        }
        if (_sq_window_size.load(butil::memory_order_relaxed) > 0) {
            NotifyWritable();
        }
    } else if (cr.completion_len == 0) {
        LOG(WARNING) << "Zero-length URMA receive without immediate credit";
        errno = EPROTO;
        return -1;
    }
    if (cr.completion_len > GetUrmaRecvBlockSize()) {
        LOG(WARNING) << "URMA completion exceeds receive buffer: "
                     << cr.completion_len;
        errno = EPROTO;
        return -1;
    }
    if (cr.completion_len <
        static_cast<uint32_t>(FLAGS_urma_zerocopy_min_size)) {
        zerocopy = false;
    }
    butil::IOBuf& read_buf = *received;
    if (zerocopy) {
        _rbuf[_rq_received].cutn(&read_buf, cr.completion_len);
    } else {
        read_buf.append(_rbuf_data[_rq_received], cr.completion_len);
    }
    if (PostRecv(1, zerocopy) < 0) {
        return -1;
    }
    if (cr.completion_len > 0) {
        SendAck(1);
    }
    return static_cast<ssize_t>(cr.completion_len);
}

void UrmaExclusiveConnection::PollCq(Socket* socket) {
    auto* carrier = static_cast<Carrier*>(socket->user());
    if (!carrier)
        return;
    auto connection = carrier->connection.lock();
    if (!connection || connection->_closed.load(butil::memory_order_acquire))
        return;
    connection->DrainCq(socket);
}

void UrmaExclusiveConnection::DrainCq(Socket* m) {
    const bool event_mode = !FLAGS_urma_use_polling;
    int progress = Socket::PROGRESS_INIT;
    while (true) {
        urma_jfc_t* event_jfc = nullptr;
        if (event_mode) {
            const int event_count = WaitCqEvent(&event_jfc);
            if (event_count < 0) {
                return;
            }
            if (event_count == 0) {
                if (!m->MoreReadEvents(&progress)) {
                    return;
                }
                continue;
            }
        }

        butil::IOBuf received;
        auto drain_cq = [&]() -> int {
            while (true) {
                const int n =
                    std::max(1, std::min<int>(FLAGS_urma_cqe_poll_once, 32));
                urma_cr_t crs[32];
                const int cnt = urma_poll_jfc(_resource->jfc, n, crs);
                if (cnt < 0) {
                    return EIO;
                }
                if (cnt == 0) {
                    return 0;
                }
                for (int i = 0; i < cnt; ++i) {
                    if (_closed.load(butil::memory_order_acquire)) {
                        return ECANCELED;
                    }
                    const ssize_t nr = HandleCompletion(crs[i], &received);
                    if (nr < 0) {
                        return errno ? errno : EIO;
                    }
                }
            }
        };

        int completion_error = drain_cq();
        if (event_mode) {
            // The bonding provider records which physical JFCs produced CRs
            // while bondp_poll_jfc drains the virtual JFC.
            // bondp_rearm_jfc consumes that mask, so rearming before the drain
            // leaves those physical JFCs unarmed.
            uint32_t nevents = 1;
            urma_ack_jfc(&event_jfc, &nevents, 1);
            if (completion_error == 0) {
                if (ReqNotifyCq() != 0) {
                    ReportError(errno ? errno : EIO);
                    return;
                }

                // Close the drain/rearm race. A completion that arrived while
                // the JFC was unarmed may not produce an edge on every
                // provider. The JFC is armed now, so a final nonblocking drain
                // is safe.
                completion_error = drain_cq();
            }
        }

        if (completion_error != 0) {
            if (!_closed.load(butil::memory_order_acquire)) {
                ReportError(completion_error);
            }
            return;
        }
        if (!received.empty()) {
            _provider->callbacks.on_received(_options.socket_id,
                                             shared_from_this(),
                                             received.movable());
        }
        if (_closed.load(butil::memory_order_acquire))
            return;

        if (!event_mode) {
            return;
        }
        // The bonding JFCE fd is itself an epoll fd aggregating physical
        // JFCEs, while brpc watches it with EPOLLET. urma_wait_jfc(..., 1, ...)
        // consumes only one aggregated event. Keep draining the inner JFCE
        // until it reports no event; otherwise another physical event can
        // leave the fd continuously readable and never create a new outer
        // edge. The event_count == 0 branch above resets _nevent only after
        // the inner queue is empty.
    }
}
int UrmaExclusiveConnection::WaitCqEvent(urma_jfc_t** event_jfc) {
    if (!_resource || !_resource->jfce || !_resource->jfc) {
        errno = ENODEV;
        return -1;
    }
    *event_jfc = nullptr;
    int count = urma_wait_jfc(_resource->jfce, 1, 0, event_jfc);
    if (count < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) {
            return 0;
        }
        const int saved_errno = errno;
        PLOG(ERROR) << "Fail to wait URMA JFC event from "
                    << _options.socket_id;
        ReportError(saved_errno);
        return -1;
    }
    if (count == 0) {
        return 0;
    }
    if (*event_jfc != _resource->jfc) {
        LOG(ERROR) << "Unexpected URMA JFC event on " << _options.socket_id;
        errno = EPROTO;
        ReportError(EPROTO);
        return -1;
    }
    return 1;
}
int UrmaExclusiveConnection::ReqNotifyCq() {
    if (!_resource || !_resource->jfc) {
        errno = ENODEV;
        return -1;
    }
    const int rc = urma_rearm_jfc(_resource->jfc, false);
    if (rc != URMA_SUCCESS) {
        errno = rc;
        PLOG(WARNING) << "Fail to rearm URMA JFC";
        return -1;
    }
    return 0;
}
void UrmaExclusiveConnection::PollerAddCqSid() {
    if (_cq_sid == INVALID_SOCKET_ID || _provider->poller_groups.empty()) {
        return;
    }
    if (_options.tag >= _provider->poller_groups.size()) {
        return;
    }
    auto& pollers = _provider->poller_groups[_options.tag].pollers;
    if (pollers.empty()) {
        return;
    }
    const size_t index = butil::fmix32(_cq_sid) % pollers.size();
    pollers[index].op_queue.Enqueue(CqSidOp{CqSidOp::ADD, _cq_sid});
}
void UrmaExclusiveConnection::PollerRemoveCqSid() {
    if (_cq_sid == INVALID_SOCKET_ID || _provider->poller_groups.empty() ||
        _options.tag >= _provider->poller_groups.size()) {
        return;
    }
    auto& pollers = _provider->poller_groups[_options.tag].pollers;
    if (pollers.empty()) {
        return;
    }
    const size_t index = butil::fmix32(_cq_sid) % pollers.size();
    pollers[index].op_queue.Enqueue(CqSidOp{CqSidOp::REMOVE, _cq_sid});
}
int UrmaExclusiveResourceProvider::InitializePollingMode(
    bthread_tag_t tag, std::function<void()> callback,
    std::function<void()> init_fn, std::function<void()> release_fn) {
    if (_state->stopping.load(butil::memory_order_acquire)) {
        errno = ESHUTDOWN;
        return -1;
    }
    if (RunningOnPoller()) {
        errno = EDEADLK;
        return -1;
    }
    BAIDU_SCOPED_LOCK(_state->poller_mutex);
    if (!_state->initialized || _state->stopping) {
        errno = ESHUTDOWN;
        return -1;
    }
    if (!FLAGS_urma_use_polling) {
        return 0;
    }
    if (tag >= _state->poller_groups.size() ||
        _state->poller_groups[tag].pollers.empty()) {
        errno = EINVAL;
        return -1;
    }
    auto& group = _state->poller_groups[tag];
    bool expected = false;
    if (!group.running.compare_exchange_strong(expected, true)) {
        return 0;
    }
    struct FnArgs {
        UrmaPoller* poller;
        butil::atomic<bool>* running;
    };
    auto fn = [](void* p) -> void* {
        std::unique_ptr<FnArgs> args(static_cast<FnArgs*>(p));
        UrmaPoller* poller = args->poller;
        butil::atomic<bool>* running = args->running;
        std::unordered_set<SocketId> cq_sids;
        CqSidOp op;

        poller->active_tid.store(bthread_self(), butil::memory_order_release);
        if (poller->init_fn) {
            poller->init_fn();
        }
        while (running->load(butil::memory_order_relaxed)) {
            while (poller->op_queue.Dequeue(op)) {
                if (op.type == CqSidOp::ADD) {
                    cq_sids.emplace(op.sid);
                } else {
                    cq_sids.erase(op.sid);
                }
            }
            for (SocketId sid : cq_sids) {
                SocketUniquePtr s;
                if (Socket::Address(sid, &s) == 0) {
                    UrmaExclusiveConnection::PollCq(s.get());
                }
            }
            if (poller->callback) {
                poller->callback();
            }
            if (FLAGS_urma_poller_yield || cq_sids.empty()) {
                bthread_yield();
            }
        }
        if (poller->release_fn) {
            poller->release_fn();
        }
        poller->active_tid.store(INVALID_BTHREAD, butil::memory_order_release);
        return nullptr;
    };

    auto& pollers = group.pollers;
    for (size_t i = 0; i < pollers.size(); ++i) {
        pollers[i].callback = callback;
        pollers[i].init_fn = init_fn;
        pollers[i].release_fn = release_fn;
        std::unique_ptr<FnArgs> args(new (std::nothrow)
                                         FnArgs{&pollers[i], &group.running});
        if (!args) {
            group.running.store(false, butil::memory_order_relaxed);
            for (size_t j = 0; j < i; ++j) {
                bthread_join(pollers[j].tid, nullptr);
                pollers[j].tid = INVALID_BTHREAD;
            }
            errno = ENOMEM;
            return -1;
        }
        bthread_attr_t attr = FLAGS_urma_disable_bthread ? BTHREAD_ATTR_PTHREAD
                                                         : BTHREAD_ATTR_NORMAL;
        attr.tag = tag;
        bthread_attr_set_name(&attr, "UrmaPolling");
        const int rc =
            bthread_start_background(&pollers[i].tid, &attr, fn, args.get());
        if (rc != 0) {
            group.running.store(false, butil::memory_order_relaxed);
            for (size_t j = 0; j < i; ++j) {
                bthread_join(pollers[j].tid, nullptr);
                pollers[j].tid = INVALID_BTHREAD;
            }
            errno = rc;
            return -1;
        }
        args.release();
    }
    return 0;
}
bool UrmaExclusiveResourceProvider::RunningOnPoller() const {
    const bthread_t self = bthread_self();
    if (self == INVALID_BTHREAD)
        return false;
    for (const auto& group : _state->poller_groups) {
        for (const auto& poller : group.pollers) {
            if (poller.active_tid.load(butil::memory_order_acquire) == self)
                return true;
        }
    }
    return false;
}

void UrmaExclusiveResourceProvider::ReleasePollingMode(bthread_tag_t tag) {
    if (RunningOnPoller()) {
        LOG(ERROR) << "ReleasePollingMode must run outside URMA pollers";
        return;
    }
    BAIDU_SCOPED_LOCK(_state->poller_mutex);
    if (!FLAGS_urma_use_polling || tag >= _state->poller_groups.size()) {
        return;
    }
    auto& group = _state->poller_groups[tag];
    group.running.store(false, butil::memory_order_relaxed);
    for (auto& poller : group.pollers) {
        if (poller.tid != INVALID_BTHREAD) {
            bthread_join(poller.tid, nullptr);
            poller.tid = INVALID_BTHREAD;
        }
    }
}
int UrmaExclusiveResourceProvider::Prepare() {
    // Pre-allocate the prepared jetty pool. Skipped if URMA init is skipped
    // (unit-test mode).
    if (FLAGS_urma_use_polling && _state->poller_groups.empty()) {
        if (FLAGS_urma_poller_num <= 0) {
            LOG(ERROR) << "urma_poller_num must be positive";
            errno = EINVAL;
            return -1;
        }
        size_t ntags = static_cast<size_t>(FLAGS_task_group_ntags);
        if (ntags == 0) {
            ntags = 1;
        }
        _state->poller_groups = std::vector<UrmaPollerGroup>(ntags);
    }
    if (_state->prepared_count > 0) {
        return 0;
    }
    urma_context_t* ctx = GetUrmaContext();
    if (!ctx) {
        return 0;
    }
    const int prepared_jetty_count = PreparedJettyCount();
    for (int i = 0; i < prepared_jetty_count; ++i) {
        auto* r = new (std::nothrow) UrmaResource();
        if (!r) {
            break;
        }
        r->jfce = urma_create_jfce(ctx);
        if (!r->jfce || (!FLAGS_urma_use_polling && r->jfce->fd < 0)) {
            _state->Retire(r);
            break;
        }
        urma_jfc_cfg_t jfc_cfg{};
        jfc_cfg.depth =
            static_cast<uint32_t>(FLAGS_urma_sq_size + FLAGS_urma_rq_size);
        jfc_cfg.jfce = r->jfce;
        r->jfc = urma_create_jfc(ctx, &jfc_cfg);
        if (!r->jfc) {
            _state->Retire(r);
            break;
        }
        urma_jfr_cfg_t jfr_cfg{};
        jfr_cfg.depth = static_cast<uint32_t>(FLAGS_urma_rq_size);
        jfr_cfg.trans_mode = URMA_TM_RM;
        jfr_cfg.max_sge = 1;
        jfr_cfg.min_rnr_timer = URMA_TYPICAL_MIN_RNR_TIMER;
        jfr_cfg.jfc = r->jfc;
        r->jfr = urma_create_jfr(ctx, &jfr_cfg);
        if (!r->jfr) {
            _state->Retire(r);
            break;
        }
        urma_jetty_cfg_t jetty_cfg{};
        jetty_cfg.flag.bs.share_jfr = 1;
        jetty_cfg.jfs_cfg.depth = static_cast<uint32_t>(FLAGS_urma_sq_size);
        jetty_cfg.jfs_cfg.trans_mode = URMA_TM_RM;
        jetty_cfg.jfs_cfg.priority = GetUrmaJettyPriority();
        jetty_cfg.jfs_cfg.max_sge = static_cast<uint8_t>(GetUrmaMaxSge());
        jetty_cfg.jfs_cfg.rnr_retry = URMA_TYPICAL_RNR_RETRY;
        jetty_cfg.jfs_cfg.err_timeout = URMA_TYPICAL_ERR_TIMEOUT;
        jetty_cfg.jfs_cfg.jfc = r->jfc;
        jetty_cfg.shared.jfr = r->jfr;
        jetty_cfg.shared.jfc = r->jfc;
        r->jetty = urma_create_jetty(ctx, &jetty_cfg);
        if (!r->jetty) {
            _state->Retire(r);
            break;
        }
        r->next = _state->prepared;
        _state->prepared = r;
        ++_state->prepared_count;
    }
    return 0;
}

void UrmaExclusiveConnection::DebugInfo(std::ostream& os) const {
    os << " resource=exclusive sq_size=" << _sq_size << " rq_size=" << _rq_size
       << " remote_recv_block_size=" << _remote_recv_block_size
       << " sq_window=" << _sq_window_size.load(butil::memory_order_relaxed)
       << " remote_rq_window="
       << _remote_rq_window_size.load(butil::memory_order_relaxed)
       << " closed=" << _closed.load(butil::memory_order_relaxed);
}

int UrmaExclusiveResourceProvider::Initialize(
    const UrmaResourceCallbacks& callbacks) {
    BAIDU_SCOPED_LOCK(_state->creation_mutex);
    if (_state->initialized || _state->stopping) {
        errno = EALREADY;
        return -1;
    }
    if (!callbacks.lookup_connection || !callbacks.on_received ||
        !callbacks.on_error) {
        errno = EINVAL;
        return -1;
    }
    _state->callbacks = callbacks;
    if (Prepare() != 0)
        return -1;
    _state->initialized = true;
    return 0;
}

int UrmaExclusiveResourceProvider::CreateConnection(
    const UrmaConnectionOptions& options, UrmaConnectionHandle* out) {
    BAIDU_SCOPED_LOCK(_state->creation_mutex);
    if (!out || options.socket_id == INVALID_SOCKET_ID) {
        errno = EINVAL;
        return -1;
    }
    {
        BAIDU_SCOPED_LOCK(_state->mutex);
        if (!_state->initialized || _state->stopping) {
            errno = ESHUTDOWN;
            return -1;
        }
    }
    UrmaConnectionHandle previous;
    auto connection =
        std::make_shared<UrmaExclusiveConnection>(options, _state);
    // Register before initialization so Shutdown also sees in-progress creates.
    {
        BAIDU_SCOPED_LOCK(_state->mutex);
        if (_state->stopping) {
            errno = ESHUTDOWN;
            return -1;
        }
        _state->connections.erase(
            std::remove_if(_state->connections.begin(),
                           _state->connections.end(),
                           [](const std::weak_ptr<UrmaConnectionResource>& c) {
                               return c.expired();
                           }),
            _state->connections.end());
        _state->connections.push_back(connection);
    }
    if (connection->Initialize() != 0)
        return -1;
    {
        BAIDU_SCOPED_LOCK(_state->mutex);
        if (_state->stopping) {
            errno = ESHUTDOWN;
            return -1;
        }
        previous.swap(*out);
        *out = connection;
    }
    return 0;
}

bool UrmaExclusiveResourceProvider::Shutdown() {
    if (RunningOnPoller()) {
        errno = EDEADLK;
        return false;
    }
    BAIDU_SCOPED_LOCK(_state->shutdown_mutex);
    std::vector<UrmaConnectionHandle> live;
    {
        // Stop new creation and finish any in-progress CreateConnection, but
        // release this lock before joining callbacks which may try to create.
        BAIDU_SCOPED_LOCK(_state->creation_mutex);
        BAIDU_SCOPED_LOCK(_state->mutex);
        _state->stopping = true;
        for (auto& weak : _state->connections) {
            if (auto connection = weak.lock())
                live.push_back(connection);
        }
    }
    for (auto& connection : live)
        connection->Close();
    for (size_t tag = 0; tag < _state->poller_groups.size(); ++tag) {
        ReleasePollingMode(static_cast<bthread_tag_t>(tag));
    }
    if (!live.empty() ||
        _state->live_connections.load(butil::memory_order_acquire) != 0)
        return false;
    // No connection destructor can mutate retirement after the acquire
    // load of live_connections reaches zero. Keep deleters outside mutex.
    while (_state->prepared) {
        UrmaResource* r = _state->prepared;
        if (!r->Release())
            return false;
        _state->prepared = r->next;
        delete r;
    }
    _state->prepared_count = 0;
    for (auto it = _state->retired.begin(); it != _state->retired.end();) {
        if (!(*it)->Release())
            return false;
        delete *it;
        it = _state->retired.erase(it);
    }
    return true;
}

int CreateUrmaResourceProvider(UrmaResourceMode mode,
                               std::unique_ptr<UrmaResourceProvider>* out) {
    if (!out) {
        errno = EINVAL;
        return -1;
    }
    if (mode != UrmaResourceMode::EXCLUSIVE) {
        errno = ENOTSUP;
        return -1;
    }
    out->reset(new UrmaExclusiveResourceProvider);
    return 0;
}

// Intentionally process-lived, like the common registered-memory service.
// No atexit cleanup: IOBuf TLS chains may outlive atexit handlers.
static auto* g_provider = new std::unique_ptr<UrmaResourceProvider>;
int InitializeUrmaResourceProvider(const UrmaResourceCallbacks& callbacks) {
    if (*g_provider) {
        errno = EALREADY;
        return -1;
    }
    if (CreateUrmaResourceProvider(UrmaResourceMode::EXCLUSIVE, g_provider) !=
        0)
        return -1;
    return (*g_provider)->Initialize(callbacks);
}
UrmaResourceProvider* GetUrmaResourceProvider() { return g_provider->get(); }
bool ShutdownUrmaResourceProvider() {
    if (!*g_provider)
        return true;
    if (!(*g_provider)->Shutdown())
        return false;
    g_provider->reset();
    return true;
}
}  // namespace urma
}  // namespace brpc
#endif  // BRPC_WITH_URMA
