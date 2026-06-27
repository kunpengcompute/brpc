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

#ifdef BRPC_SOCKET_HAS_EOF
#include "brpc/details/has_epollrdhup.h"
#endif

namespace brpc {
namespace epoll_backend {

void Init(EventDispatcher* disp) {
    disp->_event_dispatcher_fd = epoll_create(1024 * 1024);
    if (disp->_event_dispatcher_fd < 0) {
        PLOG(FATAL) << "Fail to create epoll";
        return;
    }
    CHECK_EQ(0, butil::make_close_on_exec(disp->_event_dispatcher_fd));

    disp->_wakeup_fds[0] = -1;
    disp->_wakeup_fds[1] = -1;
    if (pipe(disp->_wakeup_fds) != 0) {
        PLOG(FATAL) << "Fail to create pipe";
        return;
    }
}

void Destroy(EventDispatcher* disp) {
    if (disp->_event_dispatcher_fd >= 0) {
        close(disp->_event_dispatcher_fd);
        disp->_event_dispatcher_fd = -1;
    }
    if (disp->_wakeup_fds[0] > 0) {
        close(disp->_wakeup_fds[0]);
        close(disp->_wakeup_fds[1]);
    }
}

int Start(EventDispatcher* disp, const bthread_attr_t* thread_attr) {
    if (disp->_event_dispatcher_fd < 0) {
        LOG(FATAL) << "epoll was not created";
        return -1;
    }

    if (disp->_tid != 0) {
        LOG(FATAL) << "Already started this dispatcher(" << disp
                   << ") in bthread=" << disp->_tid;
        return -1;
    }

    if (thread_attr) {
        disp->_thread_attr = *thread_attr;
    }

    bthread_attr_t epoll_thread_attr =
        disp->_thread_attr | BTHREAD_NEVER_QUIT | BTHREAD_GLOBAL_PRIORITY;

    int rc = bthread_start_background(&disp->_tid, &epoll_thread_attr, EventDispatcher::RunThis, disp);
    if (rc) {
        LOG(FATAL) << "Fail to create epoll thread: " << berror(rc);
        return -1;
    }
    return 0;
}

void Stop(EventDispatcher* disp) {
    disp->_stop = true;
    if (disp->_event_dispatcher_fd >= 0) {
        epoll_event evt = { EPOLLOUT,  { NULL } };
        epoll_ctl(disp->_event_dispatcher_fd, EPOLL_CTL_ADD, disp->_wakeup_fds[1], &evt);
    }
}

int AddConsumer(EventDispatcher* disp, IOEventDataId event_data_id, int fd) {
    if (disp->_event_dispatcher_fd < 0) {
        errno = EINVAL;
        return -1;
    }
    epoll_event evt;
    evt.data.u64 = event_data_id;
    evt.events = EPOLLIN | EPOLLET;
#ifdef BRPC_SOCKET_HAS_EOF
    evt.events |= has_epollrdhup;
#endif
    return epoll_ctl(disp->_event_dispatcher_fd, EPOLL_CTL_ADD, fd, &evt);
}

int RemoveConsumer(EventDispatcher* disp, int fd) {
    if (fd < 0) {
        return -1;
    }
    if (epoll_ctl(disp->_event_dispatcher_fd, EPOLL_CTL_DEL, fd, NULL) < 0) {
        PLOG(WARNING) << "Fail to remove fd=" << fd << " from epfd=" << disp->_event_dispatcher_fd;
        return -1;
    }
    return 0;
}

int RegisterEvent(EventDispatcher* disp, IOEventDataId event_data_id, int fd, bool pollin) {
    if (disp->_event_dispatcher_fd < 0) {
        errno = EINVAL;
        return -1;
    }

    epoll_event evt;
    evt.data.u64 = event_data_id;
    evt.events = EPOLLOUT | EPOLLET;
#ifdef BRPC_SOCKET_HAS_EOF
    evt.events |= has_epollrdhup;
#endif
    if (pollin) {
        evt.events |= EPOLLIN;
        if (epoll_ctl(disp->_event_dispatcher_fd, EPOLL_CTL_MOD, fd, &evt) < 0) {
            return -1;
        }
    } else {
        if (epoll_ctl(disp->_event_dispatcher_fd, EPOLL_CTL_ADD, fd, &evt) < 0) {
            return -1;
        }
    }
    return 0;
}

int UnregisterEvent(EventDispatcher* disp, IOEventDataId event_data_id, int fd, bool pollin) {
    if (pollin) {
        epoll_event evt;
        evt.data.u64 = event_data_id;
        evt.events = EPOLLIN | EPOLLET;
#ifdef BRPC_SOCKET_HAS_EOF
        evt.events |= has_epollrdhup;
#endif
        return epoll_ctl(disp->_event_dispatcher_fd, EPOLL_CTL_MOD, fd, &evt);
    } else {
        return epoll_ctl(disp->_event_dispatcher_fd, EPOLL_CTL_DEL, fd, NULL);
    }
    return -1;
}

void Run(EventDispatcher* disp) {
    while (!disp->_stop) {
        epoll_event e[32];
#ifdef BRPC_ADDITIONAL_EPOLL
        int n = epoll_wait(disp->_event_dispatcher_fd, e, ARRAY_SIZE(e), 0);
        if (n == 0) {
            n = epoll_wait(disp->_event_dispatcher_fd, e, ARRAY_SIZE(e), -1);
        }
#else
        const int n = epoll_wait(disp->_event_dispatcher_fd, e, ARRAY_SIZE(e), -1);
#endif
        if (disp->_stop) {
            break;
        }
        if (n < 0) {
            if (EINTR == errno) {
                continue;
            }
            PLOG(FATAL) << "Fail to epoll_wait epfd=" << disp->_event_dispatcher_fd;
            break;
        }
        for (int i = 0; i < n; ++i) {
            if (e[i].events & (EPOLLIN | EPOLLERR | EPOLLHUP)
#ifdef BRPC_SOCKET_HAS_EOF
                || (e[i].events & has_epollrdhup)
#endif
                ) {
                int64_t start_ns = butil::cpuwide_time_ns();
                EventDispatcher::CallInputEventCallback(e[i].data.u64, e[i].events, disp->_thread_attr);
                (*g_edisp_read_lantency) << (butil::cpuwide_time_ns() - start_ns);
            }
        }
        for (int i = 0; i < n; ++i) {
            if (e[i].events & (EPOLLOUT | EPOLLERR | EPOLLHUP)) {
                int64_t start_ns = butil::cpuwide_time_ns();
                EventDispatcher::CallOutputEventCallback(e[i].data.u64, e[i].events, disp->_thread_attr);
                (*g_edisp_write_lantency) << (butil::cpuwide_time_ns() - start_ns);
            }
        }
    }
}

} // namespace epoll_backend
} // namespace brpc
