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

#include "brpc/urma/urma_endpoint.h"

#if BRPC_WITH_URMA

#include <unistd.h>

#include <cstring>
#include <memory>
#include <mutex>
#include <string>
#include <utility>

#include <gflags/gflags.h>

#include "butil/atomicops.h"
#include "butil/iobuf.h"
#include "butil/logging.h"
#include "butil/macros.h"
#include "butil/sys_byteorder.h"
#include "butil/time.h"
#include "bthread/bthread.h"
#include "bthread/butex.h"


#include "bthread/task_group.h"
#include "brpc/input_messenger.h"
#include "brpc/socket.h"
#include "brpc/urma/urma_handshake.h"
#include "brpc/urma/urma_handshake.pb.h"
#include "brpc/urma/urma_helper.h"

#include "brpc/urma_transport.h"

DECLARE_int32(task_group_ntags);

namespace bthread {
EXTERN_BAIDU_VOLATILE_THREAD_LOCAL(TaskGroup*, tls_task_group);
// Same internal cleanup primitive used by TaskGroup::task_runner.
extern void return_keytable(bthread_keytable_pool_t*, KeyTable*);
}


namespace brpc {
namespace urma {
// ---- Constants shared with the handshake module ----
static const int WAIT_TIMEOUT_MS = 50;
static const size_t HELLO_ACK_LEN = 4;
static const uint32_t HELLO_ACK_URMA_OK = 0x1;

// ============================================================================
// Constructor / destructor / Reset.
// ============================================================================

UrmaEndpoint::UrmaEndpoint(Socket* s)
    : _socket(s),
      _state(UNINIT),
      _handshake_version(0) {
    _read_butex = bthread::butex_create_checked<butil::atomic<int>>();
    _read_butex->store(0, butil::memory_order_relaxed);
    _input_processor.Init(s, InputMessengerProcessor::STREAM_URMA_JETTY);
}

UrmaEndpoint::~UrmaEndpoint() {
    DeallocateResources();
    if (_read_butex) {
        bthread::butex_destroy(_read_butex);
        _read_butex = nullptr;
    }
}

void UrmaEndpoint::Reset() {
    DeallocateResources();
    // Destroy hardware/resources outside the parsing lock.
    UrmaConnectionHandle retired_connection;
    BAIDU_SCOPED_LOCK(_dispatch_mutex);
    // Socket::Revive waits for old Socket users before Transport::Reset.
    // Never invoke Reset concurrently with a borrowed connection user.
    _published_connection.store(nullptr, std::memory_order_release);
    retired_connection.swap(_connection);
    _connection_closed = false;
    _state = UNINIT;
    _handshake_version = 0;
    _pending_received_bytes.store(0, butil::memory_order_relaxed);
    _input_processor.Reset();
    _read_butex->store(0, butil::memory_order_relaxed);
}

// ============================================================================
// Handshake IO helpers (ReadFromFd / WriteToFd / PushBackToReadBuf).
// Modeled on RdmaEndpoint::ReadFromFdLoop / WriteToFdLoop.
// ============================================================================

int UrmaEndpoint::ReadFromFd(void* data, size_t len) {
    char* p = static_cast<char*>(data);
    size_t received = 0;
    while (received < len) {
        const int expected_val = _read_butex->load(butil::memory_order_acquire);
        const timespec duetime = butil::milliseconds_from_now(WAIT_TIMEOUT_MS);
        const int fd = _socket->fd();
        const ssize_t nr = read(fd, p + received, len - received);
        if (nr < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                int rc = bthread::butex_wait(_read_butex, expected_val, &duetime);
                if (rc < 0 && errno != EWOULDBLOCK && errno != ETIMEDOUT) {
                    return -1;
                }
                continue;
            }
            return -1;
        }
        if (nr == 0) {
            errno = EEOF;
            return -1;
        }
        received += nr;
    }
    return 0;
}

void UrmaEndpoint::PushBackToReadBuf(const void* data, size_t len) {
    _socket->fd_input_processor().read_buf().append(data, len);
}

int UrmaEndpoint::WriteToFd(void* data, size_t len) {
    char* p = static_cast<char*>(data);
    size_t written = 0;
    while (written < len) {
        const timespec duetime = butil::milliseconds_from_now(WAIT_TIMEOUT_MS);
        const int fd = _socket->fd();
        const ssize_t nw = write(fd, p + written, len - written);
        if (nw >= 0) {
            written += nw;
            continue;
        }
        if (errno != EAGAIN && errno != EWOULDBLOCK) {
            return -1;
        }
        if (_socket->WaitEpollOut(fd, true, &duetime) != 0 && errno != ETIMEDOUT) {
            return -1;
        }
    }
    return 0;
}

// ============================================================================
// Hello builders / parsers.
// ============================================================================

void UrmaEndpoint::MakeLocalParsedHello(ParsedHello* out) const {
    *out = ParsedHello{};
    auto* connection = BorrowConnection();
    UrmaConnectionDescription local_description;
    if (!connection || connection->GetLocalDescription(&local_description) != 0) return;
    out->buffer_size = local_description.recv_buffer_size;
    out->recv_buffer_cnt = local_description.recv_buffer_count;
    out->jetty_id = local_description.receive_target_id;
    out->pooled = local_description.mode == UrmaConnectionMode::POOLED;
    out->socket_id = local_description.socket_id;
    out->segments = local_description.segments;
    out->big_capabilities = local_description.big_capabilities;
    out->read_max_size = local_description.read_max_size;
    std::memcpy(out->eid, local_description.eid.data(), 16);
    out->uasid = local_description.uasid;
    out->tp_type = local_description.tp_type;
    std::memcpy(out->seg_eid, local_description.segment.eid.data(), 16);
    out->seg_uasid = local_description.segment.uasid;
    out->seg_va = local_description.segment.va;
    out->seg_len = local_description.segment.length;
    out->seg_token_id = local_description.segment.token_id;
}

void UrmaEndpoint::FillLocalHelloV2(v2_wire::HelloMessage* out) const {
    *out = v2_wire::HelloMessage{};  // value-initialize
    out->msg_len = (GetUrmaConnectionMode() == UrmaConnectionMode::POOLED) ? v2_wire::POOLED_PACKET_LEN :
                                               v2_wire::HELLO_PACKET_LEN;
    out->hello_ver = v2_wire::HELLO_V2_VERSION;
    out->impl_ver = (GetUrmaConnectionMode() == UrmaConnectionMode::POOLED) ? v2_wire::IMPL_POOLED_VERSION :
                                                v2_wire::IMPL_V2_VERSION;
    ParsedHello p;
    MakeLocalParsedHello(&p);
    out->buffer_size = p.buffer_size;
    out->recv_buffer_cnt = p.recv_buffer_cnt;
    out->jetty_id = p.jetty_id;
    out->socket_id = p.socket_id;
    out->segments = p.segments;
    out->big_capabilities = p.big_capabilities;
    out->read_max_size = p.read_max_size;
    std::memcpy(out->eid, p.eid, 16);
    out->uasid = p.uasid;
    out->tp_type = p.tp_type;
    std::memcpy(out->seg_eid, p.seg_eid, 16);
    out->seg_uasid = p.seg_uasid;
    out->seg_va = p.seg_va;
    out->seg_len = p.seg_len;
    out->seg_token_id = p.seg_token_id;
}

void UrmaEndpoint::FillLocalHelloV3(UrmaHello* out) const {
    ParsedHello p;
    MakeLocalParsedHello(&p);
    out->set_buffer_size(p.buffer_size);
    out->set_recv_buffer_cnt(p.recv_buffer_cnt);
    out->set_jetty_id(p.jetty_id);
    if (p.pooled) {
        out->set_socket_id(p.socket_id);
        out->set_resource_mode(v2_wire::IMPL_POOLED_VERSION);
        out->set_big_capabilities(p.big_capabilities);
        out->set_read_max_size(p.read_max_size);
        for (const auto& seg : p.segments) FillUrmaMemorySegment(seg, out->add_segments());
    }
    out->set_eid(p.eid, 16);
    out->set_uasid(p.uasid);
    out->set_tp_type(p.tp_type);
    out->set_seg_eid(p.seg_eid, 16);
    out->set_seg_uasid(p.seg_uasid);
    out->set_seg_va(p.seg_va);
    out->set_seg_len(p.seg_len);
    out->set_seg_token_id(p.seg_token_id);
}

int UrmaEndpoint::WriteHelloV3(const UrmaHello& msg) {
    butil::IOBuf packet;
    packet.append("URM3", 4);
    std::string body;
    if (!msg.SerializeToString(&body)) {
        LOG(ERROR) << "Fail to serialize UrmaHello";
        return -1;
    }
    uint32_t pb_size_be = butil::HostToNet32(static_cast<uint32_t>(body.size()));
    packet.append(&pb_size_be, sizeof(pb_size_be));
    packet.append(body);
    return WriteToFd(packet);
}

int UrmaEndpoint::WriteToFd(butil::IOBuf& data) {
    // Write out the IOBuf in a single WriteToFd-style loop.
    while (!data.empty()) {
        const timespec duetime = butil::milliseconds_from_now(WAIT_TIMEOUT_MS);
        const int fd = _socket->fd();
        const ssize_t nw = data.cut_into_file_descriptor(fd);
        if (nw >= 0) {
            continue;
        }
        if (errno != EAGAIN && errno != EWOULDBLOCK) {
            return -1;
        }
        if (_socket->WaitEpollOut(fd, true, &duetime) != 0 && errno != ETIMEDOUT) {
            return -1;
        }
    }
    return 0;
}

int UrmaEndpoint::ReadAndParseHelloV3(ParsedHello* out, bool* negotiated) {
    *negotiated = false;
    uint32_t pb_size_be = 0;
    if (ReadFromFd(&pb_size_be, sizeof(pb_size_be)) < 0) {
        return -1;
    }
    const uint32_t pb_size = butil::NetToHost32(pb_size_be);
    if (pb_size == 0 || pb_size > 4096) {
        return 0;
    }
    std::string body(pb_size, '\0');
    if (ReadFromFd(&body[0], pb_size) < 0) {
        return -1;
    }
    UrmaHello msg;
    if (!msg.ParseFromArray(body.data(), static_cast<int>(body.size()))) {
        return 0;
    }
    if (msg.eid().size() != 16 || msg.seg_eid().size() != 16) {
        return 0;
    }
    const uint32_t mode = msg.resource_mode();
    const bool pooled = mode == 2;
    if ((mode != 1 && mode != 2) ||
        (pooled && !msg.has_socket_id()) || (!pooled && msg.has_socket_id())) {
        errno = EPROTO; return -1;
    }
    out->big_capabilities = pooled ? msg.big_capabilities() : 0;
    out->read_max_size = pooled ? msg.read_max_size() : 0;
    if (msg.segments_size() > 16) { errno = EPROTO; return -1; }
    out->segments.clear();
    if (pooled) {
        for (const auto& item : msg.segments()) {
            UrmaSegmentDescription seg;
            if (!ParseUrmaMemorySegment(item, &seg)) { errno = EPROTO; return -1; }
            out->segments.push_back(seg);
        }
    }
    out->pooled = pooled;
    out->socket_id = pooled ? msg.socket_id() : INVALID_SOCKET_ID;
    out->buffer_size = msg.buffer_size();
    out->recv_buffer_cnt = msg.recv_buffer_cnt();
    out->jetty_id = msg.jetty_id();
    std::memcpy(out->eid, msg.eid().data(), 16);
    out->uasid = msg.uasid();
    out->tp_type = static_cast<uint8_t>(msg.tp_type());
    std::memcpy(out->seg_eid, msg.seg_eid().data(), 16);
    out->seg_uasid = msg.seg_uasid();
    out->seg_va = msg.seg_va();
    out->seg_len = msg.seg_len();
    out->seg_token_id = msg.seg_token_id();
    if (!ValidHello(*out)) {
        return 0;
    }
    *negotiated = true;
    return 0;
}

// ============================================================================
// Allocate / deallocate per-connection resources.
// ============================================================================

int UrmaEndpoint::AllocateResources(UrmaConnectionRole role) {
    UrmaConnectionOptions options;
    options.socket_id = _socket->id();
    options.role = role;
    options.tag = bthread_self_tag();
    options.keytable_pool = _socket->keytable_pool();
    UrmaConnectionProvider* provider = GetUrmaConnectionProvider();
    if (!provider) { errno = ENODEV; return -1; }
    UrmaConnectionHandle connection;
    if (provider->CreateConnection(options, &connection) != 0) return -1;
    BAIDU_SCOPED_LOCK(_dispatch_mutex);
    if (_connection_closed) { errno = ECANCELED; return -1; }
    if (_connection) { errno = EALREADY; return -1; }
    _connection.swap(connection);
    _published_connection.store(_connection.get(), std::memory_order_release);
    return 0;
}

void UrmaEndpoint::DeallocateResources() {
    UrmaConnection* connection;
    {
        BAIDU_SCOPED_LOCK(_dispatch_mutex);
        _connection_closed = true;
        connection = BorrowConnection();
    }
    // Logical close only. Keeping the binding lets Socket-pinned callbacks
    // safely borrow it even when they race failure or TCP fallback.
    if (connection) connection->Close();
}


int UrmaEndpoint::ImportPeer(const ParsedHello& peer) {
    UrmaConnectionDescription peer_description;
    peer_description.recv_buffer_size = peer.buffer_size;
    peer_description.recv_buffer_count = peer.recv_buffer_cnt;
    peer_description.receive_target_id = peer.jetty_id;
    peer_description.segments = peer.segments;
    peer_description.big_capabilities = peer.big_capabilities;
    peer_description.read_max_size = peer.read_max_size;
    if (peer.pooled) {
        peer_description.mode = UrmaConnectionMode::POOLED;
        peer_description.receive_target_kind = UrmaReceiveTargetKind::JFR;
        peer_description.receive_count_kind = UrmaReceiveCountKind::SHARED_JFR_DEPTH;
        peer_description.has_socket_id = true;
        peer_description.socket_id = peer.socket_id;
    }
    std::memcpy(peer_description.eid.data(), peer.eid, 16);
    peer_description.uasid = peer.uasid;
    peer_description.tp_type = peer.tp_type;
    std::memcpy(peer_description.segment.eid.data(), peer.seg_eid, 16);
    peer_description.segment.uasid = peer.seg_uasid;
    peer_description.segment.va = peer.seg_va;
    peer_description.segment.length = peer.seg_len;
    peer_description.segment.token_id = peer.seg_token_id;
    auto* connection = BorrowConnection();
    if (!connection) { errno = ENOTCONN; return -1; }
    return connection->ImportPeer(peer_description);
}


ssize_t UrmaEndpoint::CutFromIOBufList(butil::IOBuf** from, size_t ndata) {
    auto* connection = BorrowConnection();
    if (!connection) { errno = ENOTCONN; return -1; }
    return connection->CutFromIOBufList(from, ndata);
}

bool UrmaEndpoint::IsWritable() const {
    auto* connection = BorrowConnection();
    return connection && connection->IsWritable();
}

int UrmaEndpoint::WaitWritable(butil::atomic<int>* butex, int expected,
                              const timespec* deadline) {
    auto* connection = BorrowConnection();
    if (!connection) { errno = ENOTCONN; return -1; }
    return connection->WaitWritable(butex, expected, deadline);
}

UrmaConnectionHandle UrmaEndpoint::PinConnection() const {
    // An acquire load makes the immutable owner visible. Normal RX/TX uses
    // BorrowConnection to avoid touching the shared reference count.
    return BorrowConnection() ? _connection : UrmaConnectionHandle();
}

bool UrmaEndpoint::AppendReceivedBytes(SocketUniquePtr& socket,
                                        const UrmaConnection* expected_connection,
                                        butil::IOBuf data) {
    BAIDU_SCOPED_LOCK(_dispatch_mutex);
    const State state = _state.load(butil::memory_order_acquire);
    if (!expected_connection || _connection_closed || state == FAILED || state == FALLBACK_TCP ||
        socket->Failed() || expected_connection != BorrowConnection()) return false;
    const size_t bytes = data.size();
    _input_processor.read_buf().append(data.movable());
    _pending_received_bytes.fetch_add(bytes);
    return true;
}

void UrmaEndpoint::ProcessReceivedBytes(SocketUniquePtr& socket,
                                        InputMessageClosure& last_msg) {
    BAIDU_SCOPED_LOCK(_dispatch_mutex);
    if (_connection_closed ||
        _state.load(butil::memory_order_acquire) != ESTABLISHED || socket->Failed()) return;
    const int64_t pending = _pending_received_bytes.exchange(0);
    if (pending <= 0 || !socket->user()) return;
    const int64_t now = butil::cpuwide_time_us();
    _input_processor.ProcessNewMessage(pending, false, now,
        butil::gettimeofday_us() - now, last_msg);
}

bool UrmaEndpoint::OnReceivedBytes(SocketUniquePtr& socket,
                                    const UrmaConnection* expected_connection, butil::IOBuf data) {
    if (!socket->_transport || socket->socket_mode() != SOCKET_MODE_URMA) return false;
    auto* ep = static_cast<UrmaTransport*>(socket->_transport.get())->_urma_ep;
    return ep && ep->AppendReceivedBytes(socket, expected_connection, data.movable());
}

void UrmaEndpoint::OnInputReady(SocketUniquePtr& socket, InputMessageClosure& last_msg) {
    if (!socket->_transport || socket->socket_mode() != SOCKET_MODE_URMA) return;
    auto* ep = static_cast<UrmaTransport*>(socket->_transport.get())->_urma_ep;
    if (ep) ep->ProcessReceivedBytes(socket, last_msg);
}

void UrmaEndpoint::OnRxError(SocketId id, int error) {
    SocketUniquePtr socket;
    if (Socket::Address(id, &socket) == 0 && !socket->Failed() &&
        socket->socket_mode() == SOCKET_MODE_URMA)
        socket->SetFailed(error, "URMA receive queue failure");
}

void UrmaEndpoint::OnConnectionError(SocketId id, const UrmaConnectionHandle& expected_connection,
                                      int error) {
    SocketUniquePtr socket;
    if (Socket::Address(id, &socket) != 0 || socket->Failed() ||
        !socket->_transport || socket->socket_mode() != SOCKET_MODE_URMA) return;
    auto* ep = static_cast<UrmaTransport*>(socket->_transport.get())->_urma_ep;
    if (ep && expected_connection && expected_connection.get() == ep->BorrowConnection())
        socket->SetFailed(error, "URMA connection failure");
}


static void TryReadOnTcpDuringUrmaEst(Socket* socket);

void UrmaEndpoint::OnNewDataFromTcp(Socket* m) {
    auto* tp = static_cast<UrmaTransport*>(m->_transport.get());
    if (!tp) {
        return;
    }
    // Access _urma_ep directly (OnNewDataFromTcp is a friend of UrmaTransport);
    // GetUrmaEp() CHECKs non-null which would crash on TCP-fallback sockets.
    UrmaEndpoint* ep = tp->_urma_ep;
    if (!ep) {
        // No URMA endpoint: pure TCP path.
        InputMessenger::OnNewMessages(m);
        return;
    }
    int progress = Socket::PROGRESS_INIT;
    while (true) {
        const State state =
            ep->_state.load(butil::memory_order_acquire);
        if (state == UNINIT) {
            if (!m->CreatedByConnect()) {
                // Server side: kick off the handshake bthread.
                if (!IsUrmaAvailable()) {
                    ep->_state = FALLBACK_TCP;
                    tp->_urma_state = UrmaTransport::URMA_OFF;
                    InputMessenger::OnNewMessages(m);
                    return;
                }
                SocketUniquePtr s;
                m->ReAddress(&s);
                ep->_state = S_HELLO_WAIT;
                bthread_t tid;
                bthread_attr_t attr = BTHREAD_ATTR_NORMAL;
                bthread_attr_set_name(&attr, "UrmaServerHandshake");
                if (bthread_start_background(&tid, &attr,
                        ProcessHandshakeAtServer, ep) != 0) {
                    ep->_state = UNINIT;
                    LOG(FATAL) << "Fail to start UrmaServerHandshake bthread";
                } else {
                    s.release();
                }
            }
            // Client side: handled by ProcessHandshakeAtClient.
        } else if (state < ESTABLISHED) {
            // During handshake: wake the handshake bthread parked in ReadFromFd.
            ep->_read_butex->fetch_add(1, butil::memory_order_release);
            bthread::butex_wake(ep->_read_butex);
        } else if (state == FALLBACK_TCP) {
            InputMessenger::OnNewMessages(m);
            return;
        } else if (state == ESTABLISHED) {
            TryReadOnTcpDuringUrmaEst(m);
            return;
        }
        if (!m->MoreReadEvents(&progress)) {
            break;
        }
    }
}

inline void UrmaEndpoint::TryReadOnTcp() {
    if ((GetUrmaConnectionMode() == UrmaConnectionMode::POOLED)) {
        // Recheck an EOF consumed as a handshake notification through the
        // Socket event owner, never start a concurrent TCP reader.
        Socket::OnInputEvent(reinterpret_cast<void*>(_socket->id()), 0,
                             BTHREAD_ATTR_NORMAL);
        return;
    }
    if (_state.load(butil::memory_order_acquire) == FALLBACK_TCP) {
        InputMessenger::OnNewMessages(_socket);
    }
}

static void TryReadOnTcpDuringUrmaEst(Socket* socket) {
    int progress = Socket::PROGRESS_INIT;
    while (true) {
        uint8_t byte = 0;
        const ssize_t nr = read(socket->fd(), &byte, 1);
        if (nr < 0) {
            if (errno != EAGAIN) {
                const int saved_errno = errno;
                socket->SetFailed(saved_errno, "Fail to read URMA TCP fd: %s",
                                  berror(saved_errno));
                return;
            }
            if (!socket->MoreReadEvents(&progress)) {
                return;
            }
        } else if (nr == 0) {
            socket->SetEOF();
            return;
        } else {
            socket->SetFailed(
                EPROTO, "Unexpected TCP data after URMA was established");
            return;
        }
    }
}

int UrmaEndpoint::SendFallbackHello(int version) {
    // A complete, zero-resource Hello rejects URMA without leaving protocol
    // bytes in the TCP stream. No ACK follows this rejection response.
    if (version == 2) {
        v2_wire::HelloMessage hello{};
        hello.msg_len = v2_wire::HELLO_PACKET_LEN;
        hello.hello_ver = v2_wire::HELLO_V2_VERSION;
        hello.impl_ver = v2_wire::IMPL_V2_VERSION;
        uint8_t packet[v2_wire::HELLO_PACKET_LEN]{};
        std::memcpy(packet, "URMA", 4);
        hello.Serialize(packet + 4);
        return WriteToFd(packet, sizeof(packet));
    }
    UrmaHello hello;
    FillLocalHelloV3(&hello);
    hello.set_jetty_id(0);
    hello.clear_socket_id();
    hello.clear_resource_mode();
    return WriteHelloV3(hello);
}

void UrmaEndpoint::FallbackToTcp(UrmaTransport* transport, bool process_tcp) {
    transport->_urma_state = UrmaTransport::URMA_OFF;
    _state.store(FALLBACK_TCP, butil::memory_order_release);
    DeallocateResources();
    if (process_tcp) {
        TryReadOnTcp();
    }
}

void UrmaEndpoint::FailHandshake(UrmaTransport* transport, int error,
                                 const char* reason) {
    LOG(ERROR) << "URMA handshake failed in state=" << GetStateStr()
               << " on " << _socket->description()
               << ": " << reason << ", error=" << error
               << " (" << berror(error) << ')';
    transport->_urma_state = UrmaTransport::URMA_OFF;
    _state.store(FAILED, butil::memory_order_release);
    DeallocateResources();
    auto* connect =
        static_cast<UrmaConnect*>(_socket->_app_connect.get());
    if (connect) {
        connect->_error = error;
    }
    _socket->SetFailed(error, "URMA handshake failed: %s", reason);
}

// ============================================================================
// Handshake state machines (client / server). Run in a background bthread.
// ============================================================================

void* UrmaEndpoint::ProcessHandshakeAtClient(void* arg) {
    auto* ep = static_cast<UrmaEndpoint*>(arg);
    SocketUniquePtr s(ep->_socket);
    auto* tp = static_cast<UrmaTransport*>(s->_transport.get());
    UrmaConnect::RunGuard guard(static_cast<UrmaConnect*>(s->_app_connect.get()));
    if (!IsUrmaAvailable()) {
        ep->FallbackToTcp(tp, true);
        return nullptr;
    }
    ep->_state = C_ALLOC_RES;
    if (ep->AllocateResources(UrmaConnectionRole::CLIENT) < 0) {
        ep->FallbackToTcp(tp, true);
        return nullptr;
    }
    ep->_state = C_HELLO_SEND;
    std::unique_ptr<UrmaHandshake> hs(CreateClientHandshake(ep));
    ep->_handshake_version = hs->ProtocolVersion();
    if (hs->SendLocalHello() < 0) {
        const int saved_errno = errno ? errno : EIO;
        ep->FailHandshake(tp, saved_errno, "send client hello");
        return nullptr;
    }
    ep->_state = C_HELLO_WAIT;
    ParsedHello remote;
    bool negotiated = false;
    if (hs->ReceiveAndParseRemoteHello(&remote, &negotiated) < 0) {
        const int saved_errno = errno ? errno : EIO;
        ep->FailHandshake(tp, saved_errno, "read server hello");
        return nullptr;
    }
    if (!negotiated) {
        ep->FallbackToTcp(tp, true);
        return nullptr;
    }
    if (remote.pooled != (GetUrmaConnectionMode() == UrmaConnectionMode::POOLED)) {
        // A peer that offered valid resources is waiting for an ACK.
        uint32_t rejected = 0;
        if (ep->WriteToFd(&rejected, HELLO_ACK_LEN) < 0) {
            ep->FailHandshake(tp, errno ? errno : EIO, "send fallback ack");
            return nullptr;
        }
        LOG(INFO) << "URMA resource mode mismatch: client falling back to TCP";
        ep->FallbackToTcp(tp, true);
        return nullptr;
    }
    ep->_state = C_IMPORT_PEER;
    if (ep->ImportPeer(remote) < 0) {
        const int saved_errno = errno ? errno : EIO;
        ep->FailHandshake(tp, saved_errno, "import server resources");
        return nullptr;
    }
    ep->_state = C_ACK_SEND;
    uint32_t flags = HELLO_ACK_URMA_OK;
    uint32_t flags_be = butil::HostToNet32(flags);
    if (ep->WriteToFd(&flags_be, HELLO_ACK_LEN) < 0) {
        const int saved_errno = errno ? errno : EIO;
        ep->FailHandshake(tp, saved_errno, "send client ack");
        return nullptr;
    }
    tp->_urma_state = UrmaTransport::URMA_ON;
    ep->_state = ESTABLISHED;
    {
        InputMessageClosure last_msg;
        ep->ProcessReceivedBytes(s, last_msg);
    }
    if ((GetUrmaConnectionMode() == UrmaConnectionMode::POOLED)) ep->TryReadOnTcp();
    return nullptr;
}

void* UrmaEndpoint::ProcessHandshakeAtServer(void* arg) {
    auto* ep = static_cast<UrmaEndpoint*>(arg);
    SocketUniquePtr s(ep->_socket);
    auto* tp = static_cast<UrmaTransport*>(s->_transport.get());
    UrmaConnect::RunGuard guard(static_cast<UrmaConnect*>(s->_app_connect.get()));
    ep->_state = S_HELLO_WAIT;
    uint8_t magic[v2_wire::MAGIC_STR_LEN];
    if (ep->ReadFromFd(magic, v2_wire::MAGIC_STR_LEN) < 0) {
        const int saved_errno = errno ? errno : EIO;
        ep->FailHandshake(tp, saved_errno, "read client magic");
        return nullptr;
    }
    std::unique_ptr<UrmaHandshake> hs(CreateServerHandshakeByMagic(ep, magic));
    if (!hs) {
        // Not an URMA peer: push the magic back and fall back to TCP.
        ep->PushBackToReadBuf(magic, v2_wire::MAGIC_STR_LEN);
        ep->FallbackToTcp(tp, true);
        return nullptr;
    }
    ep->_handshake_version = hs->ProtocolVersion();
    ParsedHello remote;
    bool negotiated = false;
    if (hs->ReceiveAndParseRemoteHello(&remote, &negotiated) < 0) {
        const int saved_errno = errno ? errno : EIO;
        ep->FailHandshake(tp, saved_errno, "read client hello");
        return nullptr;
    }
    if (!negotiated) {
        ep->FailHandshake(tp, EPROTO, "invalid client hello");
        return nullptr;
    }
    if (remote.pooled != (GetUrmaConnectionMode() == UrmaConnectionMode::POOLED)) {
        LOG(INFO) << "URMA resource mode mismatch: server falling back to TCP";
        if (ep->SendFallbackHello(hs->ProtocolVersion()) < 0) {
            ep->FailHandshake(tp, errno ? errno : EIO, "send fallback hello");
            return nullptr;
        }
        ep->FallbackToTcp(tp, true);
        return nullptr;
    }
    ep->_state = S_ALLOC_RES;
    if (ep->AllocateResources(UrmaConnectionRole::SERVER) < 0) {
        const int saved_errno = errno ? errno : EIO;
        ep->FailHandshake(tp, saved_errno, "allocate server resources");
        return nullptr;
    }
    ep->_state = S_IMPORT_PEER;
    if (ep->ImportPeer(remote) < 0) {
        const int saved_errno = errno ? errno : EIO;
        ep->FailHandshake(tp, saved_errno, "import client resources");
        return nullptr;
    }
    ep->_state = S_HELLO_SEND;
    if (hs->SendLocalHello() < 0) {
        const int saved_errno = errno ? errno : EIO;
        ep->FailHandshake(tp, saved_errno, "send server hello");
        return nullptr;
    }
    ep->_state = S_ACK_WAIT;
    uint32_t flags_be = 0;
    if (ep->ReadFromFd(&flags_be, HELLO_ACK_LEN) < 0) {
        const int saved_errno = errno ? errno : EIO;
        ep->FailHandshake(tp, saved_errno, "read client ack");
        return nullptr;
    }
    uint32_t flags = butil::NetToHost32(flags_be);
    bool client_ack_ok = (flags & HELLO_ACK_URMA_OK) != 0;
    if (client_ack_ok) {
        if (tp->_urma_state.load(butil::memory_order_acquire) ==
            UrmaTransport::URMA_OFF) {
            // Protocol breakdown: client wants URMA but we already fell back.
            ep->FailHandshake(tp, EPROTO, "client ack mismatch");
            return nullptr;
        }
        tp->_urma_state = UrmaTransport::URMA_ON;
        ep->_state = ESTABLISHED;
        {
            InputMessageClosure last_msg;
            ep->ProcessReceivedBytes(s, last_msg);
        }
        if ((GetUrmaConnectionMode() == UrmaConnectionMode::POOLED)) ep->TryReadOnTcp();
    } else {
        ep->FallbackToTcp(tp, true);
    }
    return nullptr;
}

// ============================================================================
// UrmaConnect: drives the client handshake bthread.
// ============================================================================

void UrmaConnect::StartConnect(const Socket* socket,
                               void (*done)(int, void*), void* data) {
    SocketUniquePtr s;
    if (Socket::Address(socket->id(), &s) != 0) {
        return;
    }
    _done = done;
    _data = data;
    _error = 0;
    auto* tp = static_cast<UrmaTransport*>(socket->_transport.get());
    if (!tp) {
        Run();
        return;
    }
    if (!tp->_urma_ep || !IsUrmaAvailable()) {
        if ((GetUrmaConnectionMode() == UrmaConnectionMode::POOLED)) {
            _error = ENODEV;
            Run();
            return;
        }
        // Fall back to TCP immediately.
        if (tp->_urma_ep) {
            tp->_urma_ep->_state = UrmaEndpoint::FALLBACK_TCP;
        }
        tp->_urma_state = UrmaTransport::URMA_OFF;
        Run();
        return;
    }
    bthread_t tid;
    bthread_attr_t attr = BTHREAD_ATTR_NORMAL;
    bthread_attr_set_name(&attr, "UrmaClientHandshake");
    if (bthread_start_background(&tid, &attr,
            UrmaEndpoint::ProcessHandshakeAtClient,
            tp->_urma_ep) != 0) {
        tp->_urma_ep->_state = UrmaEndpoint::FALLBACK_TCP;
        tp->_urma_state = UrmaTransport::URMA_OFF;
        Run();
    } else {
        // ProcessHandshakeAtClient adopts this reference in its
        // SocketUniquePtr constructor.
        s.release();
    }
}

void UrmaConnect::StopConnect(Socket*) {}

void UrmaConnect::Run() {
    if (_done) {
        auto cb = _done;
        _done = nullptr;
        cb(_error, _data);
    }
}

// ============================================================================
// Debug / polling-mode stubs.
// ============================================================================

std::string UrmaEndpoint::GetStateStr() const {
    switch (_state.load(butil::memory_order_acquire)) {
    case UNINIT:        return "UNINIT";
    case C_ALLOC_RES:   return "C_ALLOC_RES";
    case C_HELLO_SEND:  return "C_HELLO_SEND";
    case C_HELLO_WAIT:  return "C_HELLO_WAIT";
    case C_IMPORT_PEER: return "C_IMPORT_PEER";
    case C_ACK_SEND:    return "C_ACK_SEND";
    case S_HELLO_WAIT:  return "S_HELLO_WAIT";
    case S_ALLOC_RES:   return "S_ALLOC_RES";
    case S_IMPORT_PEER: return "S_IMPORT_PEER";
    case S_HELLO_SEND:  return "S_HELLO_SEND";
    case S_ACK_WAIT:    return "S_ACK_WAIT";
    case ESTABLISHED:   return "ESTABLISHED";
    case FALLBACK_TCP:  return "FALLBACK_TCP";
    case FAILED:        return "FAILED";
    }
    return "UNKNOWN";
}

void UrmaEndpoint::DebugInfo(std::ostream& os, butil::StringPiece) const {
    os << "state=" << GetStateStr() << " handshake_version=" << _handshake_version
       << " read_buf=" << _input_processor.read_buf().size();
    auto* connection = BorrowConnection();
    if (connection) connection->DebugInfo(os);
}

int UrmaEndpoint::PollingModeInitialize(
        bthread_tag_t tag, std::function<void()> callback,
        std::function<void()> init_fn, std::function<void()> release_fn) {
    if (!FLAGS_urma_use_polling) return 0;
    auto* provider = GetUrmaConnectionProvider();
    if (!provider) { errno = ENODEV; return -1; }
    return provider->InitializePollingMode(tag, callback, init_fn, release_fn);
}

void UrmaEndpoint::PollingModeRelease(bthread_tag_t tag) {
    auto* provider = GetUrmaConnectionProvider();
    if (provider) provider->ReleasePollingMode(tag);
}

int UrmaEndpoint::GlobalInitialize() {
    UrmaConnectionCallbacks callbacks;
    callbacks.on_bytes = OnReceivedBytes;
    callbacks.on_input_ready = OnInputReady;
    callbacks.on_rx_error = OnRxError;
    callbacks.on_error = OnConnectionError;
    return InitializeUrmaConnectionProvider(callbacks);
}

bool UrmaEndpoint::GlobalRelease() {
    return ShutdownUrmaConnectionProvider();
}

}  // namespace urma
}  // namespace brpc

#endif  // BRPC_WITH_URMA
