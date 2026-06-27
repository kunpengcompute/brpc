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

#include "butil/logging.h"
#include "butil/fd_utility.h"
#include <liburing.h>
#include <sys/utsname.h>
#include <poll.h>
#include <atomic>
#include <vector>
#include <pthread.h>

namespace brpc {

extern bvar::LatencyRecorder* g_edisp_read_lantency;
extern bvar::LatencyRecorder* g_edisp_write_lantency;

static const uint64_t IOURING_INTERNAL_EVENT = 0xFFFFFFFFFFFFFFFEULL;

struct IoUringFdInfo {
    IOEventDataId event_data_id;
    int fd;
    uint32_t events;

    IoUringFdInfo() : event_data_id(0), fd(-1), events(0) {}
    IoUringFdInfo(IOEventDataId id, int f, uint32_t e)
        : event_data_id(id), fd(f), events(e) {}
};

struct IoUringContext {
    struct io_uring ring;
    bool initialized;
    pthread_mutex_t fd_map_mutex;
    std::vector<IoUringFdInfo> fd_info_vec;

    IoUringContext() : initialized(false) {
        memset(&ring, 0, sizeof(ring));
        pthread_mutex_init(&fd_map_mutex, NULL);
    }

    ~IoUringContext() {
        pthread_mutex_destroy(&fd_map_mutex);
    }
};

static IoUringContext* g_iouring_ctx = NULL;

static IoUringContext& GetIoUringContext() {
    if (!g_iouring_ctx) {
        g_iouring_ctx = new IoUringContext();
    }
    return *g_iouring_ctx;
}

static struct io_uring_sqe* GetSqeWithRetry(IoUringContext& ctx, int max_retry = 3) {
    struct io_uring_sqe* sqe = nullptr;
    for (int i = 0; i < max_retry; ++i) {
        sqe = io_uring_get_sqe(&ctx.ring);
        if (sqe) {
            return sqe;
        }
        if (i < max_retry - 1) {
            int submitted = io_uring_submit(&ctx.ring);
            if (submitted < 0) {
                LOG(WARNING) << "Failed to submit pending requests: " << strerror(-submitted);
                break;
            }
        }
    }
    return nullptr;
}

EventDispatcher::EventDispatcher()
    : _event_dispatcher_fd(-1)
    , _stop(false)
    , _tid(0)
    , _thread_attr(BTHREAD_ATTR_NORMAL) {

    IoUringContext& ctx = GetIoUringContext();

    struct io_uring_params params;
    memset(&params, 0, sizeof(params));

    params.flags |= IORING_SETUP_CQSIZE;
    params.cq_entries = 256;

    int ret = io_uring_queue_init_params(128, &ctx.ring, &params);
    if (ret < 0) {
        PLOG(FATAL) << "Fail to create io_uring: " << strerror(-ret);
        return;
    }

    ctx.initialized = true;
    _event_dispatcher_fd = ctx.ring.ring_fd;

    LOG(INFO) << "io_uring created: ring_fd=" << _event_dispatcher_fd
              << ", sq_entries=" << params.sq_entries
              << ", cq_entries=" << params.cq_entries;

    if (pipe(_wakeup_fds) != 0) {
        PLOG(FATAL) << "Fail to create pipe";
        return;
    }
    CHECK_EQ(0, butil::make_close_on_exec(_wakeup_fds[0]));
    CHECK_EQ(0, butil::make_close_on_exec(_wakeup_fds[1]));
}

EventDispatcher::~EventDispatcher() {
    Stop();
    Join();

    if (_wakeup_fds[0] > 0) {
        close(_wakeup_fds[0]);
        close(_wakeup_fds[1]);
        _wakeup_fds[0] = -1;
        _wakeup_fds[1] = -1;
    }
}

int EventDispatcher::Start(const bthread_attr_t* thread_attr) {
    IoUringContext& ctx = GetIoUringContext();
    if (!ctx.initialized) {
        LOG(ERROR) << "io_uring was not created";
        return -1;
    }

    if (_tid != 0) {
        LOG(ERROR) << "Already started this dispatcher(" << this
                   << ") in bthread=" << _tid;
        return -1;
    }

    if (thread_attr) {
        _thread_attr = *thread_attr;
    }

    bthread_attr_t io_uring_thread_attr =
        _thread_attr | BTHREAD_NEVER_QUIT | BTHREAD_GLOBAL_PRIORITY;

    int rc = bthread_start_background(&_tid, &io_uring_thread_attr, RunThis, this);
    if (rc) {
        LOG(ERROR) << "Fail to create io_uring thread: " << berror(rc);
        return -1;
    }
    return 0;
}

bool EventDispatcher::Running() const {
    return !_stop && _event_dispatcher_fd >= 0 && _tid != 0;
}

void EventDispatcher::Stop() {
    _stop = true;

    if (_event_dispatcher_fd >= 0 && _wakeup_fds[1] >= 0) {
        IoUringContext& ctx = GetIoUringContext();
        struct io_uring_sqe* sqe = GetSqeWithRetry(ctx);
        if (sqe) {
            io_uring_prep_poll_add(sqe, _wakeup_fds[1], POLLOUT);
            sqe->user_data = IOURING_INTERNAL_EVENT;
            io_uring_submit(&ctx.ring);
        } else {
            int ret = io_uring_submit(&ctx.ring);
            if (ret < 0) {
                PLOG(WARNING) << "Failed to submit wakeup request";
            }
        }
    }
}

void EventDispatcher::Join() {
    if (_tid) {
        bthread_join(_tid, NULL);
        _tid = 0;
    }
}

int EventDispatcher::AddConsumer(IOEventDataId event_data_id, int fd) {
    IoUringContext& ctx = GetIoUringContext();
    if (!ctx.initialized) {
        errno = EINVAL;
        return -1;
    }

    if (fd < 0) {
        return -1;
    }

    struct io_uring_sqe* sqe = GetSqeWithRetry(ctx);
    if (!sqe) {
        LOG(ERROR) << "Failed to get SQE after retry";
        return -1;
    }

    io_uring_prep_poll_add(sqe, fd, POLLIN | EPOLLET);
    sqe->user_data = event_data_id;

    pthread_mutex_lock(&ctx.fd_map_mutex);
    ctx.fd_info_vec.push_back(IoUringFdInfo(event_data_id, fd, POLLIN | EPOLLET));
    pthread_mutex_unlock(&ctx.fd_map_mutex);

    int ret = io_uring_submit(&ctx.ring);
    if (ret < 0) {
        LOG(ERROR) << "Failed to submit poll_add: " << strerror(-ret);
        return -1;
    }

    return 0;
}

int EventDispatcher::RemoveConsumer(int fd) {
    if (fd < 0) {
        return -1;
    }

    IoUringContext& ctx = GetIoUringContext();
    if (!ctx.initialized) {
        return -1;
    }

    IOEventDataId event_data_id_to_remove = 0;
    bool found = false;

    pthread_mutex_lock(&ctx.fd_map_mutex);
    for (size_t i = 0; i < ctx.fd_info_vec.size(); ++i) {
        if (ctx.fd_info_vec[i].fd == fd) {
            event_data_id_to_remove = ctx.fd_info_vec[i].event_data_id;
            ctx.fd_info_vec[i] = ctx.fd_info_vec.back();
            ctx.fd_info_vec.pop_back();
            found = true;
            break;
        }
    }
    pthread_mutex_unlock(&ctx.fd_map_mutex);

    if (!found) {
        return 0;
    }

    struct io_uring_sqe* sqe = GetSqeWithRetry(ctx);
    if (!sqe) {
        LOG(WARNING) << "Failed to get SQE for poll remove after retry";
        return -1;
    }

    io_uring_prep_poll_remove(sqe, (unsigned long long)event_data_id_to_remove);
    sqe->user_data = IOURING_INTERNAL_EVENT;

    int ret = io_uring_submit(&ctx.ring);
    if (ret < 0) {
        LOG(WARNING) << "Failed to submit poll_remove: " << strerror(-ret);
        return -1;
    }

    return 0;
}

int EventDispatcher::RegisterEvent(IOEventDataId event_data_id,
                                   int fd, bool pollin) {
    IoUringContext& ctx = GetIoUringContext();
    if (!ctx.initialized) {
        errno = EINVAL;
        return -1;
    }

    if (fd < 0) {
        return -1;
    }

    uint32_t events = POLLOUT | EPOLLET;
    if (pollin) {
        events |= POLLIN;
    }

    struct io_uring_sqe* sqe = GetSqeWithRetry(ctx);
    if (!sqe) {
        LOG(ERROR) << "Failed to get SQE for register event after retry";
        return -1;
    }

    io_uring_prep_poll_add(sqe, fd, events);
    sqe->user_data = event_data_id;

    pthread_mutex_lock(&ctx.fd_map_mutex);
    ctx.fd_info_vec.push_back(IoUringFdInfo(event_data_id, fd, events));
    pthread_mutex_unlock(&ctx.fd_map_mutex);

    int ret = io_uring_submit(&ctx.ring);
    if (ret < 0) {
        LOG(ERROR) << "Failed to submit register event: " << strerror(-ret);
        return -1;
    }

    return 0;
}

int EventDispatcher::UnregisterEvent(IOEventDataId event_data_id,
                                      int fd, bool pollin) {
    IoUringContext& ctx = GetIoUringContext();
    if (!ctx.initialized) {
        errno = EINVAL;
        return -1;
    }

    if (fd < 0) {
        return -1;
    }

    if (pollin) {
        pthread_mutex_lock(&ctx.fd_map_mutex);
        for (size_t i = 0; i < ctx.fd_info_vec.size(); ++i) {
            if (ctx.fd_info_vec[i].fd == fd) {
                ctx.fd_info_vec[i].events = POLLIN | EPOLLET;
                break;
            }
        }
        pthread_mutex_unlock(&ctx.fd_map_mutex);
        return 0;
    } else {
        return RemoveConsumer(fd);
    }
}

static int RearmFd(IoUringContext& ctx, int fd, IOEventDataId event_data_id, uint32_t events) {
    struct io_uring_sqe* sqe = GetSqeWithRetry(ctx);
    if (!sqe) {
        LOG(WARNING) << "Failed to get SQE for rearm after retry";
        return -1;
    }

    io_uring_prep_poll_add(sqe, fd, events);
    sqe->user_data = event_data_id;

    return io_uring_submit(&ctx.ring);
}

void* EventDispatcher::RunThis(void* arg) {
    ((EventDispatcher*)arg)->Run();
    return NULL;
}

void EventDispatcher::Run() {
    IoUringContext& ctx = GetIoUringContext();
    if (!ctx.initialized) {
        LOG(ERROR) << "io_uring context not initialized";
        return;
    }

    while (!_stop) {
        struct io_uring_cqe* cqe = NULL;
        int wait_ret = io_uring_wait_cqe(&ctx.ring, &cqe);

        if (_stop) {
            if (cqe) {
                io_uring_cqe_seen(&ctx.ring, cqe);
            }
            break;
        }

        if (wait_ret < 0) {
            if (wait_ret == -EINTR) {
                continue;
            }
            PLOG(ERROR) << "io_uring_wait_cqe failed";
            break;
        }

        if (!cqe) {
            continue;
        }

        IOEventDataId event_data_id = cqe->user_data;
        int32_t res = cqe->res;
        io_uring_cqe_seen(&ctx.ring, cqe);

        if (event_data_id == IOURING_INTERNAL_EVENT) {
            continue;
        }

        if (res < 0) {
            if (res == -EBADF || res == -ENOENT) {
                pthread_mutex_lock(&ctx.fd_map_mutex);
                for (size_t i = 0; i < ctx.fd_info_vec.size(); ++i) {
                    if (ctx.fd_info_vec[i].event_data_id == event_data_id) {
                        ctx.fd_info_vec[i] = ctx.fd_info_vec.back();
                        ctx.fd_info_vec.pop_back();
                        break;
                    }
                }
                pthread_mutex_unlock(&ctx.fd_map_mutex);
            } else if (res != -ECANCELED) {
                LOG(WARNING) << "io_uring poll event failed: " << strerror(-res);
            }
            continue;
        }

        uint32_t events = static_cast<uint32_t>(res);

        pthread_mutex_lock(&ctx.fd_map_mutex);
        int fd_to_rearm = -1;
        uint32_t events_to_rearm = 0;
        bool found = false;

        for (size_t i = 0; i < ctx.fd_info_vec.size(); ++i) {
            if (ctx.fd_info_vec[i].event_data_id == event_data_id) {
                fd_to_rearm = ctx.fd_info_vec[i].fd;
                events_to_rearm = ctx.fd_info_vec[i].events;
                found = true;
                break;
            }
        }
        pthread_mutex_unlock(&ctx.fd_map_mutex);

        if (events & (POLLIN | POLLERR | POLLHUP)) {
            int64_t start_ns = butil::cpuwide_time_ns();
            CallInputEventCallback(event_data_id, events, _thread_attr);
            (*g_edisp_read_lantency) << (butil::cpuwide_time_ns() - start_ns);
        }

        if (events & (POLLOUT | POLLERR | POLLHUP)) {
            int64_t start_ns = butil::cpuwide_time_ns();
            CallOutputEventCallback(event_data_id, events, _thread_attr);
            (*g_edisp_write_lantency) << (butil::cpuwide_time_ns() - start_ns);
        }

        if (found) {
            RearmFd(ctx, fd_to_rearm, event_data_id, events_to_rearm);
        }
    }
}

} // namespace brpc
