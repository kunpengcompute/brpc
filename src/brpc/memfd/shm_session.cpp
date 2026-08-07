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

#include "brpc/memfd/shm_session.h"
#include <gflags/gflags.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/mman.h>
#include <sys/syscall.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <cstring>
#include <algorithm>
#include <limits>
#include <map>
#include "butil/logging.h"
#include "butil/scoped_lock.h"
#include "brpc/socket.h"
#include "brpc/memfd_transport.h"
#include "brpc/memfd/shm_zero_copy_stream.h"
#include "bthread/bthread.h"
#include "bthread/unstable.h"

namespace brpc {

DEFINE_uint64(shm_queue_size, 1L * 1024 * 1024,
              "Maximum size of shared memory queue");

PeerShmRegistry* PeerShmRegistry::_instance = nullptr;
static butil::Mutex _shm_registry_mutex;

namespace {

bool CheckedMul(size_t a, size_t b, size_t* result) {
    if (a != 0 && b > std::numeric_limits<size_t>::max() / a) {
        return false;
    }
    *result = a * b;
    return true;
}

bool CheckedAdd(size_t a, size_t b, size_t* result) {
    if (b > std::numeric_limits<size_t>::max() - a) {
        return false;
    }
    *result = a + b;
    return true;
}

bool ValidateShmQueueHeader(const ShmQueueHeader* header, size_t size) {
    if (!header || header->magic != SHM_QUEUE_MAGIC ||
        header->version == 0 || header->version > SHM_VERSION_CURRENT ||
        header->header_size < sizeof(ShmQueueHeader) ||
        header->header_size > size ||
        header->elem_size < sizeof(ShmQueueElement) ||
        header->queue_size == 0) {
        return false;
    }

    size_t elements_size = 0;
    size_t required_size = 0;
    if (!CheckedMul(header->queue_size, header->elem_size, &elements_size) ||
        !CheckedAdd(header->header_size, elements_size, &required_size) ||
        required_size > size) {
        return false;
    }

    const uint64_t write_pos =
        header->write_pos.load(std::memory_order_acquire);
    const uint64_t read_pos =
        header->read_pos.load(std::memory_order_acquire);
    const uint32_t reading =
        header->reading.load(std::memory_order_acquire);
    return write_pos >= read_pos &&
           write_pos - read_pos <= header->queue_size &&
           reading <= 1;
}

}  // namespace

ShmQueue::ShmQueue()
    : _header(nullptr),
      _data(nullptr),
      _capacity(0),
      _free_notify(0),
      _peer_version(0),
      _notify_fd(-1),
      _peer_features(0) {
}

ShmQueue::~ShmQueue() {
    Reset();
}

int ShmQueue::Init(void* mem, size_t size) {
    if (!mem ||
        size < sizeof(ShmQueueHeader) + sizeof(ShmQueueElement)) {
        return -1;
    }

    int notify_fd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (notify_fd < 0) {
        LOG(ERROR) << "Failed to create notify eventfd for shm queue";
        return -1;
    }

    _header = reinterpret_cast<ShmQueueHeader*>(mem);
    _header->magic = SHM_QUEUE_MAGIC;
    _header->version = SHM_VERSION_CURRENT;
    _header->header_size = static_cast<uint16_t>(sizeof(ShmQueueHeader));
    _header->elem_size = static_cast<uint16_t>(sizeof(ShmQueueElement));
    _header->feature_flags = 0;
    _header->queue_size = 0;
    _header->write_pos.store(0, std::memory_order_relaxed);
    _header->read_pos.store(0, std::memory_order_relaxed);
    _header->reading.store(0, std::memory_order_relaxed);
    _header->reserved = 0;

    _data = reinterpret_cast<char*>(mem) + _header->header_size;
    _capacity = (size - _header->header_size) / _header->elem_size;
    _header->queue_size = static_cast<uint32_t>(_capacity);
    _notify_fd = notify_fd;

    return 0;
}

int ShmQueue::Open(void* mem, size_t size) {
    if (!mem || size < sizeof(ShmQueueHeader)) {
        return -1;
    }

    _header = reinterpret_cast<ShmQueueHeader*>(mem);
    if (!ValidateShmQueueHeader(_header, size)) {
        LOG(ERROR) << "ShmQueue: invalid queue header";
        _header = nullptr;
        return -1;
    }

    _peer_version = _header->version;
    _peer_features = _header->feature_flags;

    uint32_t hsz = _header->header_size;
    _data = reinterpret_cast<char*>(mem) + hsz;
    _capacity = _header->queue_size;

    return 0;
}

void ShmQueue::Reset() {
    _header = nullptr;
    _data = nullptr;
    _capacity = 0;
    close(_notify_fd);
    _notify_fd = -1;
}

int ShmQueue::Enqueue(size_t offset, uint32_t data_size) {
    if (!_header || !_data || _capacity == 0) {
        return -1;
    }
    uint64_t write_pos = _header->write_pos.load(std::memory_order_relaxed);
    uint64_t read_pos = _header->read_pos.load(std::memory_order_relaxed);

    if (write_pos - read_pos >= _capacity) {
        return -1;
    }

    size_t pos = write_pos % _capacity;
    ShmQueueElement* elem = reinterpret_cast<ShmQueueElement*>(
        _data + pos * _header->elem_size);
    elem->offset = offset;
    elem->data_size = data_size;
    _header->write_pos.store(write_pos + 1, std::memory_order_release);
    return 0;
}

void ShmQueue::NotifyWaiter() {
    uint64_t notify = 1;
    ssize_t ret = write(_notify_fd, &notify, sizeof(notify));
    if (ret < 0 && errno != EAGAIN) {
        LOG(WARNING) << "Failed to notify waiter: " << berror();
    }
}

int ShmQueue::WaitNotify(const timespec* abstime) {
    uint64_t val;

    _free_notify.fetch_add(1, std::memory_order_release);

    if (IsFree()) {
        _free_notify.fetch_sub(1, std::memory_order_release);
        return 0;
    }

    int ret = bthread_fd_wait(_notify_fd, EPOLLIN);
    _free_notify.fetch_sub(1, std::memory_order_release);
    if (ret < 0) {
        return -1;
    }

    const ssize_t nread = read(_notify_fd, &val, sizeof(val));
    if (nread < 0 && errno != EAGAIN) {
        LOG(WARNING) << "Failed to consume queue notification: " << berror();
        return -1;
    }

    return 0;
}

int ShmQueue::DequeueAndProcessBatch(ProcessZeroCopyCallback callback, void* arg, size_t max_count) {
    if (!_header || !_data || _capacity == 0 || !callback || max_count == 0) {
        return 0;
    }
    uint64_t read_pos = _header->read_pos.load(std::memory_order_relaxed);
    uint64_t write_pos = _header->write_pos.load(std::memory_order_acquire);

    size_t available = static_cast<size_t>(write_pos - read_pos);
    size_t count = std::min(available, max_count);

    for (size_t i = 0; i < count; i++) {
        size_t pos = (read_pos + i) % _capacity;
        ShmQueueElement* e = reinterpret_cast<ShmQueueElement*>(
            _data + pos * _header->elem_size);
        callback(arg, e);
    }
    _header->read_pos.store(read_pos + count, std::memory_order_release);
    if (_free_notify.load(std::memory_order_acquire) > 0) {
        NotifyWaiter();
    }

    return static_cast<int>(count);
}

ShmQueueManager::ShmQueueManager()
    : _send_queue_fd(-1),
      _recv_queue_fd(-1),
      _send_queue_mem(nullptr),
      _recv_queue_mem(nullptr),
      _send_queue_size(0),
      _recv_queue_size(0) {
    _send_queue.reset(new ShmQueue());
    _recv_queue.reset(new ShmQueue());
}

ShmQueueManager::~ShmQueueManager() {
    Close();
}

int ShmQueueManager::CreateSendQueue(const std::string& name) {
    _send_queue_size = FLAGS_shm_queue_size;

    _send_queue_fd = syscall(SYS_memfd_create, name.c_str(), MFD_CLOEXEC | MFD_ALLOW_SEALING);
    if (_send_queue_fd < 0) {
        LOG(ERROR) << "Failed to create send queue memfd";
        return -1;
    }

    if (ftruncate(_send_queue_fd, _send_queue_size) < 0) {
        LOG(ERROR) << "Failed to ftruncate send queue";
        close(_send_queue_fd);
        _send_queue_fd = -1;
        return -1;
    }

    _send_queue_mem = mmap(nullptr, _send_queue_size, PROT_READ | PROT_WRITE, MAP_SHARED, _send_queue_fd, 0);
    if (_send_queue_mem == MAP_FAILED) {
        LOG(ERROR) << "Failed to mmap send queue";
        close(_send_queue_fd);
        _send_queue_fd = -1;
        _send_queue_mem = nullptr;
        return -1;
    }

    if (_send_queue->Init(_send_queue_mem, _send_queue_size) != 0) {
        LOG(ERROR) << "Failed to init send queue";
        Close();
        return -1;
    }

    return 0;
}

int ShmQueueManager::MapRecvQueueFromFd(int fd, int notify_fd) {
    struct stat sb;
    if (fstat(fd, &sb) < 0) {
        LOG(ERROR) << "Failed to stat recv queue fd";
        return -1;
    }

    _recv_queue_mem = mmap(nullptr, sb.st_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (_recv_queue_mem == MAP_FAILED) {
        LOG(ERROR) << "Failed to mmap recv queue from fd";
        return -1;
    }

    if (_recv_queue->Open(_recv_queue_mem, sb.st_size) != 0) {
        munmap(_recv_queue_mem, sb.st_size);
        _recv_queue_mem = nullptr;
        LOG(ERROR) << "Failed to open recv queue from fd";
        return -1;
    }
    _recv_queue->set_notify_fd(notify_fd);
    _recv_queue_fd = fd;
    _recv_queue_size = sb.st_size;

    return 0;
}

void ShmQueueManager::Close() {
    _send_queue.reset(new ShmQueue());
    _recv_queue.reset(new ShmQueue());

    if (_send_queue_mem && _send_queue_mem != MAP_FAILED) {
        munmap(_send_queue_mem, _send_queue_size);
        _send_queue_mem = nullptr;
    }
    if (_recv_queue_mem && _recv_queue_mem != MAP_FAILED) {
        munmap(_recv_queue_mem, _recv_queue_size);
        _recv_queue_mem = nullptr;
    }

    if (_send_queue_fd >= 0) {
        close(_send_queue_fd);
        _send_queue_fd = -1;
    }
    if (_recv_queue_fd >= 0) {
        close(_recv_queue_fd);
        _recv_queue_fd = -1;
    }

    _send_queue_size = 0;
    _recv_queue_size = 0;
}

ShmSession::ShmSession()
    : _recv_allocator(nullptr),
      _queue_manager(nullptr) {
}

ShmSession::~ShmSession() {
    Close();
}

int ShmSession::Init(Socket* socket, bool is_server) {
    _queue_manager = new ShmQueueManager();
    if (_queue_manager == nullptr) {
        LOG(ERROR) << "Failed to create queue manager";
        return -1;
    }
    static std::atomic<uint32_t> queue_index{0};
    uint32_t queue_idx = queue_index.fetch_add(1, std::memory_order_relaxed);
    char queue_name[64];
    if (is_server) {
        snprintf(queue_name, sizeof(queue_name), "brpc_zc_reply_q_%u", queue_idx);
    } else {
        snprintf(queue_name, sizeof(queue_name), "brpc_zc_request_q_%u", queue_idx);
    }

    if (_queue_manager->CreateSendQueue(queue_name) != 0) {
        LOG(ERROR) << "Failed to create reply queue";
        delete _queue_manager;
        _queue_manager = nullptr;
        return -1;
    }

    return 0;
}

void ShmSession::Close() {
    if (_queue_manager) {
        _queue_manager->Close();
        delete _queue_manager;
        _queue_manager = nullptr;
    }

    if (_recv_allocator) {
        PeerShmRegistry::Instance()->Release(_recv_allocator);
        _recv_allocator = nullptr;
    }
}

int ShmSession::SendFdsToPeer(int control_fd) {
    struct msghdr msg = {};
    struct iovec iov;
    char buf[1] = {0};
    iov.iov_base = buf;
    iov.iov_len = 1;
    msg.msg_iov = &iov;
    msg.msg_iovlen = 1;

    char cmsg_buf[CMSG_SPACE(4 * sizeof(int))];
    msg.msg_control = cmsg_buf;
    msg.msg_controllen = sizeof(cmsg_buf);

    struct cmsghdr* cmsg = CMSG_FIRSTHDR(&msg);
    cmsg->cmsg_level = SOL_SOCKET;
    cmsg->cmsg_type = SCM_RIGHTS;
    cmsg->cmsg_len = CMSG_LEN(4 * sizeof(int));

    int* fds = reinterpret_cast<int*>(CMSG_DATA(cmsg));
    fds[0] = GetShmAllocator()->memfd();
    fds[1] = _queue_manager->send_queue_fd();
    fds[2] = GetShmAllocator()->notify_fd();
    fds[3] = _queue_manager->send_queue()->notify_fd();

    if (sendmsg(control_fd, &msg, 0) < 0) {
        LOG(ERROR) << "Failed to send fds to peer: " << strerror(errno);
        return -1;
    }
    return 0;
}

int ShmSession::RecvFdsFromPeer(int control_fd) {
    struct msghdr msg = {};
    struct iovec iov;
    char buf[1];
    iov.iov_base = buf;
    iov.iov_len = 1;
    msg.msg_iov = &iov;
    msg.msg_iovlen = 1;

    char cmsg_buf[CMSG_SPACE(4 * sizeof(int))];
    msg.msg_control = cmsg_buf;
    msg.msg_controllen = sizeof(cmsg_buf);

    ssize_t ret = recvmsg(control_fd, &msg, 0);
    if (ret < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            return 1;
        }
        LOG(ERROR) << "Failed to recv fds from peer: " << strerror(errno);
        return -1;
    }

    struct cmsghdr* cmsg = CMSG_FIRSTHDR(&msg);
    if (!cmsg || cmsg->cmsg_level != SOL_SOCKET || cmsg->cmsg_type != SCM_RIGHTS) {
        LOG(ERROR) << "Invalid control message for fds";
        return -1;
    }

    int num_fds = (cmsg->cmsg_len - CMSG_LEN(0)) / sizeof(int);
    if (num_fds < 4) {
        LOG(ERROR) << "Expected 4 fds, got " << num_fds;
        int* received_fds = reinterpret_cast<int*>(CMSG_DATA(cmsg));
        for (int i = 0; i < num_fds; ++i) {
            close(received_fds[i]);
        }
        return -1;
    }

    int* fds = reinterpret_cast<int*>(CMSG_DATA(cmsg));
    int peer_send_shm_fd = fds[0];
    int peer_send_queue_fd = fds[1];
    int peer_notify_fd = fds[2];
    int peer_notify_queue_fd = fds[3];

    struct stat sb;
    if (fstat(peer_send_shm_fd, &sb) < 0) {
        LOG(ERROR) << "Failed to stat peer send shm fd";
        close(peer_send_shm_fd);
        close(peer_send_queue_fd);
        close(peer_notify_fd);
        close(peer_notify_queue_fd);
        return -1;
    }

    _recv_allocator = PeerShmRegistry::Instance()->GetOrCreate(
        peer_send_shm_fd, sb.st_size, peer_notify_fd);
    if (!_recv_allocator) {
        LOG(ERROR) << "Failed to get or create recv allocator from fd";
        close(peer_send_shm_fd);
        close(peer_send_queue_fd);
        close(peer_notify_fd);
        close(peer_notify_queue_fd);
        return -1;
    }

    if (_queue_manager->MapRecvQueueFromFd(peer_send_queue_fd, peer_notify_queue_fd) != 0) {
        LOG(ERROR) << "Failed to map recv queue from fd";
        PeerShmRegistry::Instance()->Release(_recv_allocator);
        _recv_allocator = nullptr;
        close(peer_send_queue_fd);
        close(peer_notify_queue_fd);
        return -1;
    }

    return 0;
}

PeerShmRegistry* PeerShmRegistry::Instance() {
    std::lock_guard<butil::Mutex> lock(_shm_registry_mutex);
    if (!_instance) {
        _instance = new PeerShmRegistry();
    }
    return _instance;
}

PeerShmRegistry::~PeerShmRegistry() {
    std::lock_guard<butil::Mutex> lock(_mutex);
    for (auto& pair : _registry) {
        delete pair.second.allocator;
    }
    _registry.clear();
}

ShmBlockAllocator* PeerShmRegistry::GetOrCreate(int fd, size_t size, int notify_fd) {
    struct stat sb;
    if (fstat(fd, &sb) < 0) {
        return nullptr;
    }

    std::lock_guard<butil::Mutex> lock(_mutex);
    auto key = std::make_pair(sb.st_dev, sb.st_ino);
    auto it = _registry.find(key);
    if (it != _registry.end()) {
        if (fd != it->second.allocator->memfd()) {
            close(fd);
        }
        if (notify_fd != it->second.notify_fd) {
            close(notify_fd);
        }
        it->second.ref_count++;
        return it->second.allocator;
    }

    auto* allocator = new ShmBlockAllocator();
    if (allocator->OpenFromFd(fd, size) != 0) {
        LOG(ERROR) << "PeerShmRegistry: OpenFromFd failed";
        delete allocator;
        return nullptr;
    }

    allocator->set_notify_fd(notify_fd);

    PeerShmEntry entry;
    entry.allocator = allocator;
    entry.ref_count = 1;
    entry.notify_fd = notify_fd;
    entry.st_dev = sb.st_dev;
    entry.st_ino = sb.st_ino;
    _registry[key] = entry;

    return allocator;
}

void PeerShmRegistry::Release(ShmBlockAllocator* allocator) {
    if (!allocator) return;

    std::lock_guard<butil::Mutex> lock(_mutex);
    for (auto it = _registry.begin(); it != _registry.end(); ++it) {
        if (it->second.allocator == allocator) {
            if (--it->second.ref_count == 0) {
                delete allocator;
                _registry.erase(it);
            }
            return;
        }
    }
}

} // namespace brpc
