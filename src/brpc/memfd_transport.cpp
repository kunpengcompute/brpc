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

#include "brpc/memfd_transport.h"
#include <sys/eventfd.h>
#include <unistd.h>
#include <cstring>
#include <pthread.h>
#include "brpc/input_messenger.h"
#include "butil/logging.h"
#include "butil/iobuf_inl.h"
#include "bthread/bthread.h"
#include "brpc/memfd/shm_zero_copy_stream.h"

namespace brpc {

DECLARE_bool(usercode_in_coroutine);
DECLARE_bool(usercode_in_pthread);
DECLARE_bool(log_connection_close);

const size_t BATCH_SIZE_ONCE = 64;
const size_t MEMFD_COALESCE_THRESHOLD = 4 * 1024;

int MemfdTransport::ContextInitOrDie(bool serverOrNot, const void* _options) {
    return InitShmAllocator();
}

MemfdTransport::MemfdTransport()
    : _shm_session(nullptr),
      _control_fd(-1),
      _is_server(false),
      _initialized(false),
      _handshake_done(false),
      _handshake_done_cb(nullptr),
      _handshake_cb_data(nullptr) {
}

void MemfdTransport::Init(Socket* socket, const SocketOptions& options) {
    _shm_session = new ShmSession();
    if (_shm_session == nullptr) {
        LOG(ERROR) << "MemfdTransport: New session failed";
        return;
    }

    _socket = socket;
    _is_server = options.is_server;
    if (!_is_server) {
        _shm_session->Init(socket, false);
    }
    _on_edge_trigger = MemfdTransport::OnNewDataFromShm;
    _default_connect = std::make_shared<MemfdAppConnect>(this);
    _initialized = true;
}

void MemfdTransport::DoHandshake() {
    if (_is_server) {
        if (_shm_session->RecvFdsFromPeer(_control_fd) != 0) {
            LOG(ERROR) << "Server failed to receive fds from client";
            const int saved_errno = errno;
            _socket->SetFailed(saved_errno, "Fail to receive fds from client %s: %s",
                _socket->description().c_str(), berror(saved_errno));
            return;
        }
        _handshake_done = true;
    } else {
        ssize_t ret = _shm_session->RecvFdsFromPeer(_control_fd);
        if (ret != 0) {
            CompleteHandshake(-1);
            LOG(ERROR) << "Client failed to receive fds from server, ret:" << ret;
            return;
        }

        if (_shm_session->SendFdsToPeer(_control_fd) != 0) {
            CompleteHandshake(-1);
            LOG(ERROR) << "Client failed to send fds to server";
            return;
        }
        int progress = Socket::PROGRESS_INIT;
        if (_socket->MoreReadEvents(&progress)) {
            CompleteHandshake(-1);
            LOG(ERROR) << "Client failed to receive memfd from server, ret:" << ret;
            return;
        }
        _handshake_done = true;
        CompleteHandshake(0);
    }
}

void MemfdTransport::CompleteHandshake(int err) {
    if (_handshake_done_cb) {
        void (*cb)(int, void*) = _handshake_done_cb;
        void* data = _handshake_cb_data;
        _handshake_done_cb = nullptr;
        _handshake_cb_data = nullptr;
        cb(err, data);
    }
}

void MemfdTransport::Release() {
    if (_shm_session) {
        delete _shm_session;
        _shm_session = nullptr;
    }
    _control_fd = -1;
    _initialized = false;
    _handshake_done = false;
    _handshake_done_cb = nullptr;
    _handshake_cb_data = nullptr;
}

int MemfdTransport::Reset(int32_t expected_nref) {
    Release();
    return 0;
}

std::shared_ptr<AppConnect> MemfdTransport::Connect() {
    return _default_connect;
}

int MemfdTransport::CutFromIOBufWithoutNotify(butil::IOBuf* buf) {
    ShmQueue* send_queue = _shm_session->queue_manager()->send_queue();
    ShmBlockAllocator* shm_allocator = GetShmAllocator();
    int send = buf->length();

    // A small baidu_std message normally consists of separate header, body
    // and attachment refs. Publishing every ref independently costs one
    // queue operation and one cross-process block reference each. Coalesce
    // the complete small message into the current SHM block so the receiver
    // observes one element while large messages remain zero-copy.
    if (send > 0 && static_cast<size_t>(send) <= MEMFD_COALESCE_THRESHOLD) {
        butil::IOBuf::Block* shm_block =
            acquire_shm_tls_block(shm_allocator);
        if (shm_block &&
            static_cast<size_t>(send) > shm_block->left_space()) {
            // Retire the partially used block instead of falling back to
            // publishing every source ref. The next block preserves the
            // single-element fast path for all small messages.
            shm_block->dec_ref();
            shm_block = shm_allocator->TryAllocBlock();
        }
        if (shm_block && static_cast<size_t>(send) <= shm_block->left_space()) {
            char* dst = shm_block->data + shm_block->size;
            if (buf->copy_to(dst, send) == static_cast<size_t>(send)) {
                size_t offset = shm_allocator->GetDataOffset(dst);
                uint32_t block_index =
                    shm_allocator->GetBlockIndex(offset);
                shm_allocator->IncPendingCount(block_index);
                if (send_queue->Enqueue(
                        offset, static_cast<uint32_t>(send)) == 0) {
                    shm_block->size += send;
                    buf->pop_front(send);
                    if (shm_block->full()) {
                        shm_block->dec_ref();
                    } else {
                        release_shm_tls_block(shm_block);
                    }
                    return send;
                }
                shm_allocator->DecPendingCount(block_index);
            }
        }
        if (shm_block) {
            release_shm_tls_block(shm_block);
        }
    }

    while (!buf->empty()) {
        const butil::IOBuf::BlockRef& ref = buf->backing_block_ref(0);
        if (ref.block->is_shm()) {
            size_t offset = shm_allocator->GetDataOffset(ref.block->data + ref.offset);
            uint32_t block_index = shm_allocator->GetBlockIndex(offset);
            shm_allocator->IncPendingCount(block_index);
            if (send_queue->Enqueue(offset, static_cast<uint32_t>(ref.length)) != 0) {
                LOG_FIRST_N(WARNING, 1) << "Failed to enqueue zero copy element";
                shm_allocator->DecPendingCount(block_index);
                return send - buf->length();
            }
        } else {
            const char* src = ref.block->data + ref.offset;
            size_t remaining = ref.length;
            while (remaining > 0) {
                butil::IOBuf::Block* shm_block = share_shm_tls_block(shm_allocator);
                if (!shm_block) {
                    buf->pop_front(ref.length - remaining);
                    LOG_FIRST_N(WARNING, 1) << "Failed to allocate SHM block for non-SHM data";
                    return send - buf->length();
                }
                size_t cap = shm_block->cap - shm_block->size;
                size_t to_copy = std::min(remaining, cap);
                memcpy(shm_block->data + shm_block->size, src, to_copy);

                size_t offset = shm_allocator->GetDataOffset(shm_block->data + shm_block->size);
                uint32_t block_index = shm_allocator->GetBlockIndex(offset);
                shm_allocator->IncPendingCount(block_index);
                if (send_queue->Enqueue(offset, static_cast<uint32_t>(to_copy)) != 0) {
                    LOG_FIRST_N(WARNING, 1) << "Failed to enqueue copy element";
                    shm_allocator->DecPendingCount(block_index);
                    buf->pop_front(ref.length - remaining);
                    return send - buf->length();
                }
                remaining -= to_copy;
                src += to_copy;
                shm_block->size += to_copy;
            }
        }
        buf->pop_front(ref.length);
    }
    return send;
}

void MemfdTransport::NotifyPeer() {
    ShmQueue* send_queue = _shm_session->queue_manager()->send_queue();
    if ((_control_fd >= 0) && send_queue->IsNofify()) {
        uint64_t notify = 1;
        const ssize_t nw = write(_control_fd, &notify, sizeof(notify));
        if (nw < 0 && errno != EAGAIN) {
            PLOG(WARNING) << "Failed to notify MEMFD receiver";
        }
    }
}

int MemfdTransport::CutFromIOBuf(butil::IOBuf* buf) {
    const int cut = CutFromIOBufWithoutNotify(buf);
    if (cut > 0) {
        NotifyPeer();
    }
    return cut;
}

ssize_t MemfdTransport::CutFromIOBufList(butil::IOBuf** buf, size_t ndata) {
    ssize_t total_cut = 0;
    for (size_t i = 0; i < ndata; ++i) {
        const size_t send = buf[i]->length();
        const int curr_send = CutFromIOBufWithoutNotify(buf[i]);
        total_cut += curr_send;
        if (curr_send < 0 || static_cast<size_t>(curr_send) < send) {
            break;
        }
    }
    if (total_cut > 0) {
        NotifyPeer();
    }

    return total_cut;
}

int MemfdTransport::WaitEpollOut(butil::atomic<int>* _epollout_butex, bool pollin, timespec duetime) {
    ShmQueue* send_queue = _shm_session->queue_manager()->send_queue();
    ShmBlockAllocator* shm_allocator = GetShmAllocator();
    while (true) {
        bool block_ok = shm_allocator->IsWritable();
        bool queue_ok = send_queue->IsFree();
        if (block_ok && queue_ok) {
            return 0;
        }

        if (!block_ok) {
            if (shm_allocator->WaitNotify(&duetime) < 0) {
                if (errno == ETIMEDOUT) {
                    errno = EAGAIN;
                    return -1;
                }
            }
        }

        if (!queue_ok) {
            if (send_queue->WaitNotify(&duetime) < 0) {
                if (errno == ETIMEDOUT) {
                    errno = EAGAIN;
                    return -1;
                }
            }
        }
    }

    return 0;
}

void MemfdTransport::ProcessEvent(bthread_attr_t attr) {
    if (!_initialized) {
        return;
    }

    bthread_t tid;
    if (FLAGS_usercode_in_coroutine) {
        OnEdge(_socket);
    } else if (bthread_start_urgent(&tid, &attr, OnEdge, _socket) != 0) {
        LOG(FATAL) << "Fail to start ProcessEvent";
        OnEdge(_socket);
    }
}

void MemfdTransport::OnNewDataFromShm(Socket* m) {
    int progress = Socket::PROGRESS_INIT;
    auto* memfd_transport = static_cast<MemfdTransport*>(m->_transport.get());
    uint64_t notify;
    int fd = m->fd();
    int ret;

    if (!memfd_transport->IsHandshake()) {
        memfd_transport->DoHandshake();
        if (!memfd_transport->IsServer() && !m->MoreReadEvents(&progress)) {
            return;
        }
    }

    while (true) {
        do {
            ret = read(fd, &notify, sizeof(notify));
        } while (ret > 0);
        if (ret == 0) {
            LOG_IF(WARNING, FLAGS_log_connection_close) << *m << " was closed by remote side";
            m->SetEOF();
            return;
        } else if (errno != EAGAIN) {
            if (errno == EINTR) {
                continue;
            }
            const int saved_errno = errno;
            LOG(ERROR) << "Fail to read from " << *m << ", " << berror(saved_errno);
            m->SetFailed(saved_errno, "Fail to read from %s: %s",
                m->description().c_str(), berror(saved_errno));
            return;
        }
        memfd_transport->ProcessShmData();
        if (!m->MoreReadEvents(&progress)) {
            return;
        } else {
            continue;
        }
    }
}

void MemfdTransport::ProcessShmBuffer(void* arg, const ShmQueueElement* elem) {
    MemfdTransport* transport = static_cast<MemfdTransport*>(arg);
    ShmBlockAllocator* recv_allocator = transport->_shm_session->recv_allocator();
    if (!recv_allocator ||
        !recv_allocator->IsValidDataRange(elem->offset, elem->data_size)) {
        LOG(WARNING) << "Drop invalid MEMFD queue element, offset="
                     << elem->offset << " size=" << elem->data_size;
        return;
    }
    char* data_ptr = recv_allocator->GetDataPtrFromOffset(elem->offset);
    uint32_t block_index = recv_allocator->GetBlockIndexFromOffset(elem->offset);
    ShmBlockRecycler deleter(recv_allocator, block_index);
    transport->_socket->_read_buf.append_user_data(data_ptr, elem->data_size, std::move(deleter));
}

void MemfdTransport::ProcessShmData() {
    ShmQueue* recv_queue = _shm_session->queue_manager()->recv_queue();
    InputMessageClosure last_msg;

    while (true) {
        recv_queue->SetReading(true);
        while (true) {
            const int64_t received_us = butil::cpuwide_time_us();
            const int64_t base_realtime = butil::gettimeofday_us() - received_us;
            int count = recv_queue->DequeueAndProcessBatch(ProcessShmBuffer, this, BATCH_SIZE_ONCE);
            if (count <= 0) {
                break;
            }
            InputMessenger* messenger = static_cast<InputMessenger*>(_socket->user());
            if (messenger->ProcessNewMessage(_socket, _socket->_read_buf.size(),
                false, received_us, base_realtime, last_msg) < 0) {
                break;
            }
        }
        recv_queue->SetReading(false);
        if (recv_queue->Size() == 0) {
            break;
        }
    }

    return;
}

void MemfdTransport::QueueMessage(InputMessageClosure& input_msg, int* num_bthread_created, bool last_msg) {
    InputMessageBase* to_run_msg = input_msg.release();
    if (!to_run_msg) {
        return;
    }
    bthread_t th;
    bthread_attr_t tmp =
        (FLAGS_usercode_in_pthread ? BTHREAD_ATTR_PTHREAD : BTHREAD_ATTR_NORMAL) |
        BTHREAD_NOSIGNAL;
    tmp.keytable_pool = _socket->keytable_pool();
    tmp.tag = bthread_self_tag();
    if (!FLAGS_usercode_in_coroutine && bthread_start_background(
            &th, &tmp, ProcessInputMessage, to_run_msg) == 0) {
        ++*num_bthread_created;
    } else {
        ProcessInputMessage(to_run_msg);
    }
}

void MemfdTransport::QueueMessages(InputMessageBatch* input_msgs,
                                   int* num_bthread_created, bool) {
    if (!input_msgs || input_msgs->empty()) {
        delete input_msgs;
        return;
    }
    bthread_t th;
    bthread_attr_t tmp =
        (FLAGS_usercode_in_pthread ? BTHREAD_ATTR_PTHREAD : BTHREAD_ATTR_NORMAL) |
        BTHREAD_NOSIGNAL;
    tmp.keytable_pool = _socket->keytable_pool();
    tmp.tag = bthread_self_tag();
    if (!FLAGS_usercode_in_coroutine && bthread_start_background(
            &th, &tmp, ProcessInputMessageBatch, input_msgs) == 0) {
        ++*num_bthread_created;
    } else {
        input_msgs->Run();
        delete input_msgs;
    }
}

void MemfdTransport::Debug(std::ostream &os) {
    os << "MemfdTransport: ";
    os << "initialized=" << _initialized
       << ", handshake_done=" << _handshake_done;
}

void MemfdTransport::OnFdSet() {
    _control_fd = _socket->fd();

    if (!_initialized || _handshake_done || !_is_server) {
        return;
    }

    bthread_t th;
    bthread_attr_t tmp =
        (FLAGS_usercode_in_pthread ? BTHREAD_ATTR_PTHREAD : BTHREAD_ATTR_NORMAL);
    tmp.keytable_pool = _socket->keytable_pool();
    tmp.tag = bthread_self_tag();
    if (!FLAGS_usercode_in_coroutine && bthread_start_urgent(
            &th, &tmp, ServerMemfdSend, this) == 0) {
        return;
    } else {
        ServerMemfdSend(this);
    }
}

void* MemfdTransport::ServerMemfdSend(void* void_arg) {
    MemfdTransport* memfd_transport = static_cast<MemfdTransport*>(void_arg);
    memfd_transport->ServerMemfdCreateSend();
    return NULL;
}

void MemfdTransport::ServerMemfdCreateSend() {
    if (_shm_session->Init(_socket, true) != 0) {
        LOG(ERROR) << "Server failed to init session";
        _socket->SetFailed(ENOMEM, "Server failed to init session");
        return;
    }
    if (_shm_session->SendFdsToPeer(_socket->fd()) != 0) {
        LOG(ERROR) << "Server failed to send fds to client";
        _socket->SetFailed(errno, "Server failed to send fds to client");
        return;
    }
}

void MemfdAppConnect::StartConnect(const Socket* socket, void (*done)(int err, void* data), void* data) {
    if (!_transport) {
        done(-1, data);
        return;
    }
    _transport->_handshake_done_cb = done;
    _transport->_handshake_cb_data = data;
}

} // namespace brpc
