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

#ifndef BRPC_EVENT_DISPATCHER_IOURING_H
#define BRPC_EVENT_DISPATCHER_IOURING_H

#include "brpc/event_dispatcher.h"

#ifdef BRPC_WITH_IO_URING

#include <liburing.h>
#include <unordered_map>
#include <mutex>

namespace brpc {

struct IoUringConfig {
    uint32_t sq_entries;
    uint32_t cq_entries;
    bool sqpoll_enabled;
    bool polling_enabled;
    int sqpoll_cpu;
    uint32_t sqpoll_idle;

    IoUringConfig()
        : sq_entries(256)
        , cq_entries(512)
        , sqpoll_enabled(false)
        , polling_enabled(false)
        , sqpoll_cpu(-1)
        , sqpoll_idle(2000) {}
};

class IoUringEventDispatcher : public EventDispatcher {
public:
    IoUringEventDispatcher();
    virtual ~IoUringEventDispatcher();

    virtual int Start(const bthread_attr_t* thread_attr) override;

    int AddConsumer(IOEventDataId event_data_id, int fd) override;
    int RemoveConsumer(int fd) override;
    int RegisterEvent(IOEventDataId event_data_id, int fd, bool pollin) override;
    int UnregisterEvent(IOEventDataId event_data_id, int fd, bool pollin) override;

    void SetConfig(const IoUringConfig& config) { _config = config; }
    const IoUringConfig& GetConfig() const { return _config; }

private:
    void Run() override;

    int InitIoUring();
    void DestroyIoUring();

    int SubmitRequests();
    int ProcessCompletions();

    int PreparePollRequest(IOEventDataId event_data_id, int fd,
                          uint32_t events, bool add);

    struct io_uring _ring;
    IoUringConfig _config;

    bool _initialized;

    std::mutex _fd_map_mutex;
    std::unordered_map<int, IOEventDataId> _fd_to_event_data;

    DISALLOW_COPY_AND_ASSIGN(IoUringEventDispatcher);
};

bool CheckIoUringSupport();

} // namespace brpc

#endif // BRPC_WITH_IO_URING

#endif // BRPC_EVENT_DISPATCHER_IOURING_H
