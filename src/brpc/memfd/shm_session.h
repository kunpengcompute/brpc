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

#ifndef BRPC_MEMFD_SHM_SESSION_H
#define BRPC_MEMFD_SHM_SESSION_H

#include <stdint.h>
#include <string>
#include <memory>
#include <atomic>
#include <map>
#include <utility>
#include <vector>
#include <sys/types.h>
#include "brpc/memfd/shm_block_allocator.h"
#include "butil/atomicops.h"
#include "butil/scoped_lock.h"

namespace brpc {

class Socket;

struct ShmQueueHeader {
    uint32_t                magic;
    uint16_t                version;
    uint16_t                header_size;
    uint16_t                elem_size;
    uint16_t                feature_flags;
    uint32_t                queue_size;
    butil::atomic<uint64_t> write_pos;
    butil::atomic<uint64_t> read_pos;
    butil::atomic<uint32_t> reading;
    uint32_t                reserved;
};

static_assert(sizeof(ShmQueueHeader) == 40, "ShmQueueHeader must be 40 bytes");

struct ShmQueueElement {
    size_t   offset;
    uint32_t data_size;
};

class ShmQueue {
public:
    ShmQueue();
    ~ShmQueue();

    int Init(void* mem, size_t size);
    int Open(void* mem, size_t size);
    void Reset();

    int Enqueue(size_t offset, uint32_t data_size);

    inline size_t Size() const {
        if (!_header) {
            return 0;
        }
        return static_cast<size_t>(
            _header->write_pos.load(std::memory_order_acquire) -
            _header->read_pos.load(std::memory_order_acquire));
    }

    inline bool IsFree() {
        if (!_header || _capacity == 0) {
            return false;
        }
        return (_header->write_pos.load(std::memory_order_relaxed) -
            _header->read_pos.load(std::memory_order_relaxed)) < _capacity;
    }

    typedef void (*ProcessZeroCopyCallback)(void* arg, const ShmQueueElement* elem);
    int DequeueAndProcessBatch(ProcessZeroCopyCallback callback, void* arg, size_t max_count);

    inline int notify_fd() const { return _notify_fd; }
    inline void set_notify_fd(int fd) { _notify_fd = fd; }

    int WaitNotify(const timespec* abstime);
    void NotifyWaiter();

    inline void SetReading(bool reading) {
        if (!_header) {
            return;
        }
        _header->reading.store(reading ? 1 : 0, std::memory_order_release);
    }

    inline bool IsNofify() const {
        if (!_header) {
            return false;
        }
        bool send = _header->reading.load(std::memory_order_acquire) == 0;
        if (send) {
            _header->reading.fetch_add(1, std::memory_order_release);
        }
        return send;
    }

    uint16_t peer_version() const { return _peer_version; }
    uint16_t peer_features() const { return _peer_features; }

private:
    ShmQueueHeader* _header;
    char* _data;
    size_t _capacity;
    int _notify_fd;
    std::atomic<uint32_t> _free_notify;
    uint16_t _peer_version;
    uint16_t _peer_features;
};

class ShmQueueManager {
public:
    ShmQueueManager();
    ~ShmQueueManager();

    int CreateSendQueue(const std::string& name);
    int MapRecvQueueFromFd(int fd, int notify_fd);
    void Close();

    inline ShmQueue* send_queue() { return _send_queue.get(); }
    inline ShmQueue* recv_queue() { return _recv_queue.get(); }

    inline int send_queue_fd() const { return _send_queue_fd; }
    inline int recv_queue_fd() const { return _recv_queue_fd; }

private:
    std::unique_ptr<ShmQueue> _send_queue;
    std::unique_ptr<ShmQueue> _recv_queue;

    int _send_queue_fd;
    int _recv_queue_fd;
    void* _send_queue_mem;
    void* _recv_queue_mem;
    size_t _send_queue_size;
    size_t _recv_queue_size;
};

struct PeerShmEntry {
    ShmBlockAllocator* allocator;
    int ref_count;
    int notify_fd;
    dev_t st_dev;
    ino_t st_ino;
};

class PeerShmRegistry {
public:
    static PeerShmRegistry* Instance();

    ShmBlockAllocator* GetOrCreate(int fd, size_t size, int notify_fd);
    void Release(ShmBlockAllocator* allocator);

private:
    PeerShmRegistry() = default;
    ~PeerShmRegistry();

    std::map<std::pair<dev_t, ino_t>, PeerShmEntry> _registry;
    butil::Mutex _mutex;

    static PeerShmRegistry* _instance;
};

class ShmSession {
public:
    ShmSession();
    ~ShmSession();

    int Init(Socket* socket, bool is_server);
    void Close();

    int SendFdsToPeer(int control_fd);
    int RecvFdsFromPeer(int control_fd);

    inline ShmBlockAllocator* recv_allocator() { return _recv_allocator; }
    inline ShmQueueManager* queue_manager() { return _queue_manager; }

private:

    ShmBlockAllocator* _recv_allocator;
    ShmQueueManager* _queue_manager;
};

} // namespace brpc

#endif // BRPC_MEMFD_SHM_SESSION_H
