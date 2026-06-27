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

#include <liburing.h>
#include <poll.h>
#include <sys/utsname.h>
#include <unordered_map>
#include <mutex>
#include <sched.h>

#include "brpc/socket.h"
#include "brpc/input_messenger.h"
#include "brpc/event_dispatcher_iouring_impl.h"
#include "butil/strings/string_split.h"

namespace brpc {
namespace iouring_backend {

DEFINE_int32(io_uring_entries, 2048,
              "Number of entries in io_uring ring");
DEFINE_int32(register_buf_count, 8192,
              "Number of buffers to register with io_uring");
DEFINE_int32(register_buf_size, 8192,
              "Size of buffers to register with io_uring");
DEFINE_bool(io_uring_multishot_recv, true,
              "Whether to use multishot recv with io_uring");
DEFINE_string(io_uring_sq_thread_cpu_list, "",
              "Comma-separated list of CPU cores for io_uring SQPOLL thread "
              "affinity, e.g. '44,45,46,47'. Each dispatcher picks the next "
              "CPU from this list in round-robin order. When empty, no CPU "
              "affinity is set (kernel decides). "
              "Only works with --io_uring_sqpoll=true");
DEFINE_bool(io_uring_sqpoll, false,
            "Enable io_uring SQPOLL mode. Kernel thread polls SQ to avoid "
            "submit syscalls. Requires kernel 5.11+ and root/CAP_SYS_NICE. "
            "Only works with --io_backend=io_uring");
DEFINE_int32(io_uring_sqpoll_idle, 1000,
             "Idle time in milliseconds before SQPOLL kernel thread goes "
             "to sleep. Only works with --io_uring_sqpoll=true");

static const uint64_t IOURING_INTERNAL_EVENT = 0xFFFFFFFFFFFFFFFEULL;

static struct io_uring_buf_ring* GetBufRing(struct IoUringContext* ctx);

struct IoUringContext {
    struct io_uring ring;
    bool initialized;
    bool sqpoll;
    std::unordered_map<IOEventDataId, IoUringFdInfo> fd_info_map;

    void* register_buf;
    void** buf_entries;
    void* buf_pool;
    __u32 buf_count;
    bool buffers_registered;

    butil::Mutex recycle_mutex;
    std::vector<uint32_t> pending_recycle_ids;

    IoUringContext() : initialized(false), sqpoll(false), buf_count(0),
            buffers_registered(false), register_buf(NULL), buf_entries(NULL),
            buf_pool(NULL) {
            memset(&ring, 0, sizeof(ring));
            pending_recycle_ids.reserve(256);
            }

    ~IoUringContext() {
        if (buffers_registered && register_buf) {
            io_uring_unregister_buf_ring(&ring, 0);
            free(register_buf);
        }

        if (buf_entries) {
            free(buf_entries);
            buf_entries = NULL;
        }
        if (buf_pool) {
            free(buf_pool);
            buf_pool = NULL;
        }

        register_buf = NULL;

        if (initialized) {
            io_uring_queue_exit(&ring);
            initialized = false;
        }
    }

    void QueueRecycle(uint32_t buf_id) {
        std::lock_guard<butil::Mutex> guard(recycle_mutex);
        pending_recycle_ids.push_back(buf_id);
    }

    void FlushRecycles() {
        std::vector<uint32_t> ids;
        {
            std::lock_guard<butil::Mutex> guard(recycle_mutex);
            if (pending_recycle_ids.empty()) {
                return;
            }
            ids.swap(pending_recycle_ids);
            pending_recycle_ids.reserve(256);
        }
        struct io_uring_buf_ring* br = GetBufRing(this);
        int mask = io_uring_buf_ring_mask(buf_count);
        for (uint32_t buf_id : ids) {
            io_uring_buf_ring_add(br, buf_entries[buf_id],
                                  FLAGS_register_buf_size, buf_id, mask, 0);
            io_uring_buf_ring_advance(br, 1);
        }
    }

    int Submit() {
        return io_uring_submit(&ring);
    }

    int SubmitAndWait(unsigned wait_nr) {
        return io_uring_submit_and_wait(&ring, wait_nr);
    }
};

static IoUringContext* GetCtx(EventDispatcher* disp) {
    return static_cast<IoUringContext*>(disp->_iouring_ctx);
}

static struct io_uring_buf_ring* GetBufRing(IoUringContext* ctx) {
    return static_cast<struct io_uring_buf_ring*>(ctx->register_buf);
}

static void RecycleBuffer(IoUringContext* ctx, uint32_t buf_id) {
    if (buf_id >= ctx->buf_count) {
        LOG(ERROR) << "Invalid buffer id: " << buf_id << ", max=" << ctx->buf_count;
        return;
    }
    struct io_uring_buf_ring* br = GetBufRing(ctx);
    io_uring_buf_ring_add(br, ctx->buf_entries[buf_id],
                          FLAGS_register_buf_size, buf_id,
                          io_uring_buf_ring_mask(ctx->buf_count), 0);
    io_uring_buf_ring_advance(br, 1);
}

void RecycleRegisteredBuffer(void* ctx, uint32_t buf_id) {
    static_cast<IoUringContext*>(ctx)->QueueRecycle(buf_id);
}

static void SubmitTask(Socket* s, IoUringFdInfo* src, bthread_t* tid,
                       const bthread_attr_t* attr) {
    auto* t = new IoUringFdInfo();
    t->fd = src->fd;
    t->id = src->id;
    t->buffer = src->buffer;
    t->res = src->res;
    t->is_registered_buffer = src->is_registered_buffer;
    t->registered_buf_id = src->registered_buf_id;
    t->recycle_ctx = src->recycle_ctx;
    src->buffer = nullptr;
    src->is_registered_buffer = false;
    s->add_task(t);

    if (!s->_is_working.exchange(true, butil::memory_order_acq_rel)) {
        SocketId sid = s->id();
        int rc = bthread_start_background(tid, attr, Socket::iouring_callback,
                                          reinterpret_cast<void*>(sid));
        if (rc != 0) {
            Socket::iouring_callback(reinterpret_cast<void*>(sid));
        }
    }
}

static void DeleteUserData(IoUringFdInfo*& u) {
    if (u->buffer && !u->is_registered_buffer) {
        delete[] u->buffer;
    }
    delete u;
    u = nullptr;
}

static int GetKernelVersion() {
    struct utsname buf;
    if (uname(&buf) != 0) {
        return 0;
    }
    int major = 0, minor = 0;
    sscanf(buf.release, "%d.%d", &major, &minor);
    return major * 1000 + minor;
}

static constexpr __u32 IOURING_MAX_BUF_RING = 32768;

static __u32 SanitizeBufRingEntries(__u32 requested) {
    if (requested == 0) {
        return IOURING_MAX_BUF_RING;
    }
    // Round up to next power of 2
    __u32 v = requested;
    v--;
    v |= v >> 1;
    v |= v >> 2;
    v |= v >> 4;
    v |= v >> 8;
    v |= v >> 16;
    v++;
    // Cap at kernel maximum
    if (v > IOURING_MAX_BUF_RING) {
        v = IOURING_MAX_BUF_RING;
    }
    return v;
}

static int GetNextSqCpu() {
    static std::vector<int> cpu_list;
    static int next_idx = 0;
    static bool parsed = false;

    if (!parsed) {
        parsed = true;
        if (!FLAGS_io_uring_sq_thread_cpu_list.empty()) {
            std::vector<butil::StringPiece> pieces;
            butil::SplitString(FLAGS_io_uring_sq_thread_cpu_list, ',', &pieces);
            for (const auto& p : pieces) {
                std::string trimmed(p.data(), p.size());
                size_t start = trimmed.find_first_not_of(" \t");
                size_t end = trimmed.find_last_not_of(" \t");
                if (start != std::string::npos && end != std::string::npos) {
                    int cpu = atoi(trimmed.substr(start, end - start + 1).c_str());
                    cpu_list.push_back(cpu);
                }
            }
            if (!cpu_list.empty()) {
                LOG(INFO) << "io_uring SQPOLL CPU list: ["
                          << FLAGS_io_uring_sq_thread_cpu_list
                          << "], " << cpu_list.size() << " CPUs";
            }
        }
    }

    if (cpu_list.empty()) {
        return -1;  // No list: let kernel decide
    }

    int cpu = cpu_list[next_idx % cpu_list.size()];
    ++next_idx;
    return cpu;
}

void Init(EventDispatcher* disp) {
    IoUringContext* ctx = new IoUringContext();
    disp->_iouring_ctx = ctx;

    struct io_uring_params params;
    memset(&params, 0, sizeof(params));

    params.flags |= IORING_SETUP_CQSIZE;
    params.cq_entries = FLAGS_io_uring_entries * 2;

    bool use_sqpoll = FLAGS_io_uring_sqpoll;
    int kernel_ver = GetKernelVersion();

    if (use_sqpoll && kernel_ver < 5011) {
        LOG(WARNING) << "io_uring SQPOLL requires kernel 5.11+, current kernel is "
                     << kernel_ver / 1000 << "." << kernel_ver % 1000
                     << ", falling back to default mode";
        use_sqpoll = false;
    }

    if (use_sqpoll) {
        params.flags |= IORING_SETUP_SQPOLL;
        params.sq_thread_idle = FLAGS_io_uring_sqpoll_idle;

        int sq_cpu = GetNextSqCpu();
        if (sq_cpu >= 0) {
            params.flags |= IORING_SETUP_SQ_AFF;
            params.sq_thread_cpu = sq_cpu;
        }
    }

    int ret = io_uring_queue_init_params(FLAGS_io_uring_entries, &ctx->ring, &params);
    if (ret < 0) {
        if (use_sqpoll && (ret == -EINVAL || ret == -EPERM)) {
            LOG(WARNING) << "io_uring SQPOLL not supported (ret=" << ret
                         << "), retrying without SQPOLL";
            use_sqpoll = false;
            memset(&params, 0, sizeof(params));
            params.flags |= IORING_SETUP_CQSIZE;
            params.cq_entries = FLAGS_io_uring_entries * 2;
            ret = io_uring_queue_init_params(FLAGS_io_uring_entries, &ctx->ring, &params);
        }
        if (ret < 0) {
            LOG(FATAL) << "Fail to create io_uring: " << strerror(-ret);
            delete ctx;
            disp->_iouring_ctx = NULL;
            return;
        }
    }

    // register buffers
    struct io_uring_buf_ring *br;
    __u32 nr_entries = SanitizeBufRingEntries(FLAGS_register_buf_count);
    if ((__u32)FLAGS_register_buf_count != nr_entries) {
        static bool warned = false;
        if (!warned) {
            warned = true;
            LOG(WARNING) << "--register_buf_count=" << FLAGS_register_buf_count
                         << " is not a valid power-of-2 or exceeds kernel limit"
                         << " (max=" << IOURING_MAX_BUF_RING << "), adjusted to "
                         << nr_entries;
        }
    }

    size_t ring_size = (nr_entries * sizeof(struct io_uring_buf) + 4095) & ~4095;
    void* ring_ptr;
    if (posix_memalign(&ring_ptr, 4096, ring_size) != 0) {
        PLOG(FATAL) << "Fail to allocate buffer ring";
        delete ctx;
        disp->_iouring_ctx = NULL;
        return;
    }
    br = (struct io_uring_buf_ring *)ring_ptr;

    void** bufs = static_cast<void**>(malloc(sizeof(void*) * nr_entries));
    if (!bufs) {
        PLOG(FATAL) << "Fail to allocate memory for buffer";
        free(ring_ptr);
        delete ctx;
        disp->_iouring_ctx = NULL;
        return;
    }

    struct io_uring_buf_reg buf_reg = {
        .ring_addr = (__u64)br,
        .ring_entries = nr_entries,
        .bgid = 0
    };
    ret = io_uring_register_buf_ring(&ctx->ring, &buf_reg, 0);
    if (ret < 0) {
        LOG(FATAL) << "Fail to register buffer ring: " << strerror(-ret);
        free(bufs);
        free(ring_ptr);
        delete ctx;
        disp->_iouring_ctx = NULL;
        return;
    }

    void* pool = nullptr;
    if (posix_memalign(&pool, 8192, nr_entries * FLAGS_register_buf_size) != 0) {
        PLOG(FATAL) << "Fail to allocate buffer pool";
        free(bufs);
        free(ring_ptr);
        delete ctx;
        disp->_iouring_ctx = NULL;
        return;
    }
    for (int i = 0; i < nr_entries; ++i) {
        bufs[i] = static_cast<char*>(pool) + i * FLAGS_register_buf_size;
        io_uring_buf_ring_add(br, bufs[i], FLAGS_register_buf_size, i, io_uring_buf_ring_mask(nr_entries), i);
    }
    io_uring_buf_ring_advance(br, nr_entries);

    ctx->register_buf = br;
    ctx->buf_entries = bufs;
    ctx->buf_pool = pool;
    ctx->buf_count = nr_entries;
    ctx->buffers_registered = true;

    LOG(INFO) << "Registering " << nr_entries << " buffers"
              << ", buffer_size=" << FLAGS_register_buf_size
              << ", buffer_ring_addr=" << (uint64_t)br
              << ", buffer_ring_entries=" << nr_entries;

    ctx->initialized = true;
    ctx->sqpoll = use_sqpoll;
    disp->_event_dispatcher_fd = ctx->ring.ring_fd;

    LOG(INFO) << "io_uring dispatcher [" << (void*)disp << "] created"
              << ": ring_fd=" << disp->_event_dispatcher_fd
              << ", sq_entries=" << params.sq_entries
              << ", cq_entries=" << params.cq_entries
              << ", sqpoll=" << (use_sqpoll ? "enabled" : "disabled")
              << ", sq_thread_cpu=" << (use_sqpoll ? (int)params.sq_thread_cpu : -1)
              << ", sq_thread_idle=" << (use_sqpoll ? (int)params.sq_thread_idle : 0) << "ms";

    disp->_wakeup_fds[0] = -1;
    disp->_wakeup_fds[1] = -1;
    if (pipe(disp->_wakeup_fds) != 0) {
        PLOG(FATAL) << "Fail to create pipe";
        return;
    }
    CHECK_EQ(0, butil::make_close_on_exec(disp->_wakeup_fds[0]));
    CHECK_EQ(0, butil::make_close_on_exec(disp->_wakeup_fds[1]));
}

void Destroy(EventDispatcher* disp) {
    IoUringContext* ctx = GetCtx(disp);
    if (ctx) {
        delete ctx;
        disp->_iouring_ctx = NULL;
    }
    if (disp->_wakeup_fds[0] > 0) {
        close(disp->_wakeup_fds[0]);
        close(disp->_wakeup_fds[1]);
        disp->_wakeup_fds[0] = -1;
        disp->_wakeup_fds[1] = -1;
    }
}

int Start(EventDispatcher* disp, const bthread_attr_t* thread_attr) {
    IoUringContext* ctx = GetCtx(disp);
    if (!ctx || !ctx->initialized) {
        LOG(ERROR) << "io_uring was not created";
        return -1;
    }

    if (disp->_tid != 0) {
        LOG(ERROR) << "Already started this dispatcher(" << disp
                   << ") in bthread=" << disp->_tid;
        return -1;
    }

    if (thread_attr) {
        disp->_thread_attr = *thread_attr;
    }

    bthread_attr_t io_uring_thread_attr =
        disp->_thread_attr | BTHREAD_NEVER_QUIT | BTHREAD_GLOBAL_PRIORITY;

    int rc = bthread_start_background(&disp->_tid, &io_uring_thread_attr, EventDispatcher::RunThis, disp);
    if (rc) {
        LOG(ERROR) << "Fail to create io_uring thread: " << berror(rc);
        return -1;
    }
    return 0;
}

void Stop(EventDispatcher* disp) {
    disp->_stop = true;

    if (disp->_event_dispatcher_fd >= 0 && disp->_wakeup_fds[1] >= 0) {
        IoUringContext* ctx = GetCtx(disp);
        if (ctx && ctx->initialized) {
            struct io_uring_sqe* sqe = io_uring_get_sqe(&ctx->ring);
            if (sqe) {
                io_uring_prep_poll_add(sqe, disp->_wakeup_fds[1], POLLOUT);
                io_uring_sqe_set_data64(sqe, IOURING_INTERNAL_EVENT);
                ctx->Submit();
            }
        }
    }
}

// Returns true if the SQE was successfully filled, false otherwise.
static bool FillRecvSqe(IoUringContext* ctx, IoUringFdInfo* info) {
    struct io_uring_sqe* sqe = io_uring_get_sqe(&ctx->ring);
    if (!sqe) {
        int submit_ret = ctx->Submit();
        if (submit_ret < 0) {
            LOG(ERROR) << "Failed to submit: " << strerror(-submit_ret);
            return false;
        }
        sqe = ::io_uring_get_sqe(&ctx->ring);
        if (!sqe) {
            LOG(ERROR) << "Failed to get SQE";
            return false;
        }
    }

    if (info->use_multishot) {
        io_uring_prep_recv_multishot(sqe, info->fd, NULL, 0, 0);
        sqe->buf_group = 0;
        sqe->flags |= IOSQE_BUFFER_SELECT;
    } else {
        char* recv_buffer = new char[FLAGS_register_buf_size];
        io_uring_prep_recv(sqe, info->fd, recv_buffer, FLAGS_register_buf_size, 0);
        info->buffer = recv_buffer;
    }
    io_uring_sqe_set_data(sqe, info);
    return true;
}

int AddConsumer(EventDispatcher* disp, IOEventDataId event_data_id, int fd) {
    IoUringContext* ctx = GetCtx(disp);
    if (!ctx || !ctx->initialized) {
        errno = EINVAL;
        return -1;
    }

    if (fd < 0) {
        return -1;
    }

    uint32_t events = POLLIN | EPOLLET;
    int is_listen = 0;
    socklen_t optlen = sizeof(is_listen);
    if (getsockopt(fd, SOL_SOCKET, SO_ACCEPTCONN, &is_listen, &optlen) == 0 && is_listen) {
        struct io_uring_sqe* sqe = io_uring_get_sqe(&ctx->ring);
        if (!sqe) {
            int submit_ret = ctx->Submit();
            if (submit_ret < 0) {
                LOG(ERROR) << "Failed to submit: " << strerror(-submit_ret);
                return -1;
            }
            sqe = io_uring_get_sqe(&ctx->ring);
            if (!sqe) {
                LOG(ERROR) << "Failed to get SQE for accept";
                return -1;
            }
        }

        IoUringFdInfo* user_data = new IoUringFdInfo(fd, events, event_data_id, FLAGS_io_uring_multishot_recv, true);
        io_uring_prep_poll_multishot(sqe, fd, POLLIN);  // this is listen sqe
        io_uring_sqe_set_data(sqe, user_data);

        int ret = ctx->Submit();
        if (ret < 0) {
            LOG(ERROR) << "Failed to submit poll multishot: " << strerror(-ret);
            delete user_data;
            user_data = nullptr;
            return -1;
        }
        return 0;
    }

    RemoveConsumer(disp, fd);
    auto user_data = new IoUringFdInfo(fd, events, event_data_id, FLAGS_io_uring_multishot_recv, false);
    FillRecvSqe(ctx, user_data);

    int ret = ctx->Submit();
    if (ret < 0) {
        LOG(ERROR) << "Failed to submit recv: " << strerror(-ret);
        if (user_data->buffer != NULL) {
            delete[] user_data->buffer;
        }
        delete user_data;
        user_data = nullptr;
        return -1;
    }

    return 0;
}

int RemoveConsumer(EventDispatcher* disp, int fd) {
    if (fd < 0) {
        return -1;
    }

    IoUringContext* ctx = GetCtx(disp);
    if (!ctx || !ctx->initialized) {
        return -1;
    }

    struct io_uring_sqe* sqe = io_uring_get_sqe(&ctx->ring);
    if (!sqe) {
        ctx->Submit();
        sqe = io_uring_get_sqe(&ctx->ring);
    }
    if (sqe) {
        io_uring_prep_cancel_fd(sqe, fd, IORING_ASYNC_CANCEL_ALL);
        io_uring_sqe_set_data64(sqe, IOURING_INTERNAL_EVENT);
        ctx->Submit();
    }

    return 0;
}

int RegisterEvent(EventDispatcher* disp, IOEventDataId event_data_id, int fd, bool pollin) {
    IoUringContext* ctx = GetCtx(disp);
    if (!ctx || !ctx->initialized) {
        errno = EINVAL;
        return -1;
    }

    if (fd < 0) {
        return -1;
    }

    uint32_t events = POLLOUT | EPOLLET;

    struct io_uring_sqe* sqe = io_uring_get_sqe(&ctx->ring);
    if (!sqe) {
        ctx->Submit();
        sqe = io_uring_get_sqe(&ctx->ring);
        if (!sqe) {
            LOG(ERROR) << "Failed to get SQE for register event";
            return -1;
        }
    }
    auto user_data = new IoUringFdInfo(fd, events, event_data_id, false, false);
    io_uring_prep_poll_add(sqe, fd, events);
    io_uring_sqe_set_data(sqe, user_data);
    int ret = ctx->Submit();
    if (ret < 0) {
        LOG(ERROR) << "Failed to submit register event: " << strerror(-ret);
        delete user_data;
        user_data = nullptr;
        return -1;
    }

    return 0;
}

int UnregisterEvent(EventDispatcher* disp, IOEventDataId event_data_id, int fd, bool pollin) {
    IoUringContext* ctx = GetCtx(disp);
    if (!ctx || !ctx->initialized) {
        errno = EINVAL;
        return -1;
    }

    if (fd < 0) {
        return -1;
    }

    if (pollin) {
        return 0;  // iouring的所有sqe除非multishot都是一次性的，不用删除，自动会取消。
    } else {
        return RemoveConsumer(disp, fd);
    }
}

void Run(EventDispatcher* disp) {
    IoUringContext* ctx = GetCtx(disp);
    if (!ctx || !ctx->initialized) {
        LOG(ERROR) << "io_uring context not initialized";
        return;
    }

    while (!disp->_stop) {
        ctx->FlushRecycles();

        int ret = ctx->SubmitAndWait(1);
        if (disp->_stop) {
            break;
        }
        if (ret < 0) {
            if (ret == -EINTR) {
                continue;
            }
            LOG(ERROR) << "io_uring wait failed: ret=" << ret
                        << " (" << strerror(-ret) << ")";
            break;
        }

        unsigned head;
        unsigned count = 0;
        struct io_uring_cqe* cqe;
        InputMessageClosure last_msg;
        io_uring_for_each_cqe(&ctx->ring, head, cqe) {
            count++;
            if (disp->_stop) {
                continue;
            }
            if (io_uring_cqe_get_data64(cqe) == IOURING_INTERNAL_EVENT) {
                continue;
            }
            int32_t res = cqe->res;
            IoUringFdInfo* user_data = static_cast<IoUringFdInfo*>(io_uring_cqe_get_data(cqe));
            if (!user_data) continue;
            if (user_data->fd < 0 || user_data->events == 0) {
                continue;
            }

            IOEventDataId event_data_id = user_data->id;
            uint32_t events = user_data->events;
            if (events & (POLLIN | POLLERR | POLLHUP)) {
                if (user_data->is_listen) {
                    int64_t start_ns = butil::cpuwide_time_ns();
                    EventDispatcher::CallInputEventCallback(event_data_id, events, disp->_thread_attr);
                    (*g_edisp_read_lantency) << (butil::cpuwide_time_ns() - start_ns);
                } else {
                    EventDataUniquePtr event_data;
                    if (IOEventData::Address(event_data_id, &event_data) != 0 || event_data == NULL) {
                        LOG(WARNING) << "IOEventData recycled: event_data_id=" << event_data_id
                                     << " (version=" << VersionOfVRefId(event_data_id)
                                     << ", slot=" << SlotOfVRefId<IOEventData>(event_data_id).value
                                     << "), res=" << res << ", fd=" << user_data->fd
                                     << ", use_multishot=" << user_data->use_multishot;
                        if (user_data->use_multishot) {
                            if (res > 0) {
                                RecycleBuffer(ctx, cqe->flags >> IORING_CQE_BUFFER_SHIFT);
                            }
                            RemoveConsumer(disp, user_data->fd);
                        }
                        DeleteUserData(user_data);
                        continue;
                    }

                    auto socket_id = reinterpret_cast<SocketId>(event_data->_options.user_data);
                    SocketUniquePtr s;
                    int addr_ret = Socket::Address(socket_id, &s);
                    if (addr_ret < 0 || s == NULL) {
                        LOG(ERROR) << "Socket Address failed: socket_id=" << socket_id;
                        if (user_data->use_multishot) {
                            if (res > 0) {
                                RecycleBuffer(ctx, cqe->flags >> IORING_CQE_BUFFER_SHIFT);
                            }
                            RemoveConsumer(disp, user_data->fd);
                        }
                        DeleteUserData(user_data);
                        continue;
                    }

                    bool read_eof = false;
                    if (res < 0) {
                        if (user_data->use_multishot && res == -ENOBUFS) {
                            LOG(ERROR) << "io_uring multishot buffer ring exhausted (ENOBUFS)"
                                       << " fd=" << user_data->fd
                                       << ", buf_count=" << ctx->buf_count
                                       << ", marking socket as failed."
                                       << " Consider increasing --register_buf_count";
                            RemoveConsumer(disp, user_data->fd);
                            s->SetFailed(ENOBUFS, "io_uring buffer ring exhausted");
                            delete user_data;
                            user_data = nullptr;
                            continue;
                        }
                        if (res == -EBADF || res == -ENOENT ||
                            res == -ECONNRESET || res == -ENETRESET ||
                            res == -EPIPE || res == -ENOTCONN) {
                                LOG(ERROR) << "Socket " << user_data->fd << " is closed: " << strerror(-res);
                            } else if (res != -ECANCELED) {
                                LOG(WARNING) << "io_uring event canceled: " << strerror(-res);
                            }
                        if (user_data->use_multishot) {
                            RemoveConsumer(disp, user_data->fd);
                        }
                        DeleteUserData(user_data);
                        continue;
                    } else if (res == 0) {
                        read_eof = true;
                    }

                    if (user_data->use_multishot && res > 0) {
                        uint32_t buf_id = cqe->flags >> IORING_CQE_BUFFER_SHIFT;
                        if (buf_id < ctx->buf_count) {
                            user_data->buffer = static_cast<char*>(ctx->buf_entries[buf_id]);
                            user_data->is_registered_buffer = true;
                            user_data->registered_buf_id = buf_id;
                            user_data->recycle_ctx = ctx;
                        } else {
                            LOG(ERROR) << "Invalid multishot buffer id: " << buf_id
                                       << " (fd=" << user_data->fd << ", res=" << res << ")";
                            // buf_id is corrupt, but the kernel may have consumed
                            // a buffer. We can't recycle without a valid id, but
                            // we log the error for diagnosis.
                            delete user_data;
                            user_data = nullptr;
                            continue;
                        }
                    }

                    bthread_t tid;
                    bthread_attr_t attr = BTHREAD_ATTR_NORMAL;
                    user_data->res = res;
                    if (read_eof) {
                        s->SetEOF();
                        DeleteUserData(user_data);
                    } else {
                        bool need_rearm = !(cqe->flags & IORING_CQE_F_MORE);

                        IoUringFdInfo* new_user_data = nullptr;

                        if (need_rearm && user_data->use_multishot) {
                            new_user_data = new IoUringFdInfo(user_data->fd, user_data->events,
                                                              user_data->id, user_data->use_multishot,
                                                              user_data->is_listen);
                        } else if (need_rearm) {
                            new_user_data = new IoUringFdInfo(user_data->fd, user_data->events,
                                                              user_data->id, user_data->use_multishot,
                                                              user_data->is_listen);
                        }

                        SubmitTask(s.get(), user_data, &tid, &attr);
                        if (need_rearm) {
                            SocketUniquePtr check_s;
                            if (Socket::Address(socket_id, &check_s) == 0 && check_s) {
                                if (FillRecvSqe(ctx, new_user_data)) {
                                    ctx->Submit();
                                } else {
                                    delete new_user_data;
                                }
                            } else {
                                delete new_user_data;
                            }
                        }
                    }
                }
            } else if (events & (POLLOUT | POLLERR | POLLHUP)) {
                int64_t start_ns = butil::cpuwide_time_ns();
                EventDispatcher::CallOutputEventCallback(event_data_id, events, disp->_thread_attr);
                (*g_edisp_write_lantency) << (butil::cpuwide_time_ns() - start_ns);

                DeleteUserData(user_data);
            }
        }

        if (count > 0) {
            io_uring_cq_advance(&ctx->ring, count);
        }
    }
}

} // namespace iouring_backend
} // namespace brpc
