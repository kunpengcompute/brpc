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


#include <gflags/gflags.h>
#include "butil/compat.h"
#include "butil/fd_utility.h"
#include "butil/logging.h"
#include "butil/third_party/murmurhash3/murmurhash3.h"
#include "bvar/latency_recorder.h"
#include "bthread/bthread.h"
#include "brpc/event_dispatcher.h"

#if BRPC_WITH_IO_URING
#include <liburing.h>
#endif

DECLARE_int32(task_group_ntags);

namespace brpc {

DEFINE_int32(event_dispatcher_num, 1, "Number of event dispatcher");

DEFINE_bool(usercode_in_pthread, false,
            "Call user's callback in pthreads, use bthreads otherwise");
DEFINE_bool(usercode_in_coroutine, false,
            "User's callback are run in coroutine, no bthread or pthread blocking call");

DEFINE_string(io_backend, "epoll",
              "I/O backend: auto, epoll, io_uring. Only works when BRPC_WITH_IO_URING is enabled");

static const int IO_BACKEND_EPOLL = 0;
static const int IO_BACKEND_IOURING = 1;

static EventDispatcher* g_edisp = NULL;
static bvar::LatencyRecorder* g_edisp_read_lantency = NULL;
static bvar::LatencyRecorder* g_edisp_write_lantency = NULL;
static pthread_once_t g_edisp_once = PTHREAD_ONCE_INIT;

static int ResolveIoBackend() {
    const std::string& backend = FLAGS_io_backend;
    if (backend == "epoll") {
        return IO_BACKEND_EPOLL;
    }
#if BRPC_WITH_IO_URING
    if (backend == "io_uring") {
        return IO_BACKEND_IOURING;
    }
    if (backend == "auto") {
        return IO_BACKEND_IOURING;
    }
#else
    if (backend == "io_uring") {
        LOG(WARNING) << "io_uring is not compiled in, falling back to epoll. "
                     << "Recompile with -DWITH_IO_URING=ON to enable io_uring.";
    }
#endif
    return IO_BACKEND_EPOLL;
}

static void StopAndJoinGlobalDispatchers() {
    for (int i = 0; i < FLAGS_task_group_ntags; ++i) {
        for (int j = 0; j < FLAGS_event_dispatcher_num; ++j) {
            g_edisp[i * FLAGS_event_dispatcher_num + j].Stop();
            g_edisp[i * FLAGS_event_dispatcher_num + j].Join();
        }
    }
    delete g_edisp_read_lantency;
    delete g_edisp_write_lantency;
}

void InitializeGlobalDispatchers() {
    g_edisp_read_lantency = new bvar::LatencyRecorder("event_dispatcher_read_latency");
    g_edisp_write_lantency = new bvar::LatencyRecorder("event_dispatcher_write_latency");

    g_edisp = new EventDispatcher[FLAGS_task_group_ntags * FLAGS_event_dispatcher_num];
    for (int i = 0; i < FLAGS_task_group_ntags; ++i) {
        for (int j = 0; j < FLAGS_event_dispatcher_num; ++j) {
            bthread_attr_t attr =
                FLAGS_usercode_in_pthread ? BTHREAD_ATTR_PTHREAD : BTHREAD_ATTR_NORMAL;
            attr.tag = (BTHREAD_TAG_DEFAULT + i) % FLAGS_task_group_ntags;
            CHECK_EQ(0, g_edisp[i * FLAGS_event_dispatcher_num + j].Start(&attr));
        }
    }
    CHECK_EQ(0, atexit(StopAndJoinGlobalDispatchers));
}

EventDispatcher& GetGlobalEventDispatcher(int fd, bthread_tag_t tag) {
    pthread_once(&g_edisp_once, InitializeGlobalDispatchers);
    if (FLAGS_task_group_ntags == 1 && FLAGS_event_dispatcher_num == 1) {
        return g_edisp[0];
    }
    int index = butil::fmix32(fd) % FLAGS_event_dispatcher_num;
    return g_edisp[tag * FLAGS_event_dispatcher_num + index];
}

int IOEventData::OnCreated(const IOEventDataOptions& options) {
    if (!options.input_cb) {
        LOG(ERROR) << "Invalid input_cb=NULL";
        return -1;
    }
    if (!options.output_cb) {
        LOG(ERROR) << "Invalid output_cb=NULL";
        return -1;
    }

    _options = options;
    return 0;
}

void IOEventData::BeforeRecycled() {
    _options = { NULL, NULL, NULL };
}

} // namespace brpc

#if defined(OS_LINUX)

#include "brpc/event_dispatcher_epoll_impl.cpp"
#if BRPC_WITH_IO_URING
#include "brpc/event_dispatcher_iouring_impl.cpp"
#endif

namespace brpc {

EventDispatcher::EventDispatcher()
    : _event_dispatcher_fd(-1)
    , _stop(false)
    , _tid(0)
    , _thread_attr(BTHREAD_ATTR_NORMAL)
    , _wakeup_fds{-1, -1}
    , _backend_type(ResolveIoBackend())
    , _iouring_ctx(NULL) {
    if (_backend_type == IO_BACKEND_IOURING) {
#if BRPC_WITH_IO_URING
        iouring_backend::Init(this);
#endif
    } else {
        epoll_backend::Init(this);
    }
}

EventDispatcher::~EventDispatcher() {
    Stop();
    Join();
    if (_backend_type == IO_BACKEND_IOURING) {
#if BRPC_WITH_IO_URING
        iouring_backend::Destroy(this);
#endif
    } else {
        epoll_backend::Destroy(this);
    }
}

int EventDispatcher::Start(const bthread_attr_t* thread_attr) {
    if (_backend_type == IO_BACKEND_IOURING) {
#if BRPC_WITH_IO_URING
        return iouring_backend::Start(this, thread_attr);
#endif
    }
    return epoll_backend::Start(this, thread_attr);
}

bool EventDispatcher::Running() const {
    return !_stop && _event_dispatcher_fd >= 0 && _tid != 0;
}

void EventDispatcher::Stop() {
    if (_backend_type == IO_BACKEND_IOURING) {
#if BRPC_WITH_IO_URING
        iouring_backend::Stop(this);
#endif
    } else {
        epoll_backend::Stop(this);
    }
}

void EventDispatcher::Join() {
    if (_tid) {
        bthread_join(_tid, NULL);
        _tid = 0;
    }
}

int EventDispatcher::AddConsumer(IOEventDataId event_data_id, int fd) {
    if (_backend_type == IO_BACKEND_IOURING) {
#if BRPC_WITH_IO_URING
        return iouring_backend::AddConsumer(this, event_data_id, fd);
#endif
    }
    return epoll_backend::AddConsumer(this, event_data_id, fd);
}

int EventDispatcher::RemoveConsumer(int fd) {
    if (_backend_type == IO_BACKEND_IOURING) {
#if BRPC_WITH_IO_URING
        return iouring_backend::RemoveConsumer(this, fd);
#endif
    }
    return epoll_backend::RemoveConsumer(this, fd);
}

int EventDispatcher::RegisterEvent(IOEventDataId event_data_id, int fd, bool pollin) {
    if (_backend_type == IO_BACKEND_IOURING) {
#if BRPC_WITH_IO_URING
        return iouring_backend::RegisterEvent(this, event_data_id, fd, pollin);
#endif
    }
    return epoll_backend::RegisterEvent(this, event_data_id, fd, pollin);
}

int EventDispatcher::UnregisterEvent(IOEventDataId event_data_id, int fd, bool pollin) {
    if (_backend_type == IO_BACKEND_IOURING) {
#if BRPC_WITH_IO_URING
        return iouring_backend::UnregisterEvent(this, event_data_id, fd, pollin);
#endif
    }
    return epoll_backend::UnregisterEvent(this, event_data_id, fd, pollin);
}

void* EventDispatcher::RunThis(void* arg) {
    ((EventDispatcher*)arg)->Run();
    return NULL;
}

void EventDispatcher::Run() {
    if (_backend_type == IO_BACKEND_IOURING) {
#if BRPC_WITH_IO_URING
        iouring_backend::Run(this);
#endif
    } else {
        epoll_backend::Run(this);
    }
}

} // namespace brpc

#elif defined(OS_MACOSX)
    #include "brpc/event_dispatcher_kqueue.cpp"
#else
    #error Not implemented
#endif
