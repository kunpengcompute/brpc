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

#ifndef BRPC_URMA_ENDPOINT_H
#define BRPC_URMA_ENDPOINT_H

#include <atomic>
#include <cstdint>
#include <functional>
#include <ostream>
#include <vector>

#include "butil/atomicops.h"
#include "butil/iobuf.h"
#include "butil/macros.h"
#include "butil/synchronization/lock.h"
#include "bthread/types.h"

#include "brpc/socket.h"

#if BRPC_WITH_URMA

#include "brpc/urma/urma_connection.h"
#include "brpc/urma/urma_handshake.h"
#include "brpc/urma/urma_handshake.pb.h"

namespace brpc {

class UrmaTransport;

namespace urma {

class UrmaHandshake;
class UrmaConnection;
struct ParsedHello;

DECLARE_bool(urma_use_polling);
DECLARE_int32(urma_poller_num);
DECLARE_bool(urma_disable_bthread);

// Per-connection application-level connect object. Returned by
// UrmaTransport::Connect(); its StartConnect spawns the client-side handshake
// bthread that drives the URMA negotiation over the already-connected TCP fd.
class UrmaConnect : public AppConnect {
    friend class UrmaEndpoint;
public:
    void StartConnect(const Socket* socket,
                      void (*done)(int err, void* data), void* data) override;
    void StopConnect(Socket*) override;

    struct RunGuard {
        explicit RunGuard(UrmaConnect* rc) : this_rc(rc) {}
        ~RunGuard() { if (this_rc) this_rc->Run(); }
        UrmaConnect* this_rc;
    };

private:
    void Run();
    void (*_done)(int, void*){nullptr};
    void* _data{nullptr};
    int _error{0};
};

// Per-Socket negotiation and brpc stream integration. Native resources live
// behind the connection interface.
class BAIDU_CACHELINE_ALIGNMENT UrmaEndpoint {
    friend class UrmaConnect;
    friend class UrmaPooledConnection;
    friend class Socket;
    friend class ::brpc::UrmaTransport;
    friend class UrmaHandshakeClientV2;
    friend class UrmaHandshakeServerV2;
    friend class UrmaHandshakeClientV3;
    friend class UrmaHandshakeServerV3;
    friend int DrainBytes(UrmaEndpoint*, size_t);
    friend int ReadBodyAndNegotiate(UrmaEndpoint*, ParsedHello*, bool*);

public:
    explicit UrmaEndpoint(Socket* s);
    ~UrmaEndpoint();

    // ---- Global initialization / release ----
    // Pre-allocate the prepared Jetty pool. Called once at GlobalUrmaInitializeOrDie.
    static int GlobalInitialize();
    static bool GlobalRelease();

    // ---- Per-connection lifecycle ----
    // Reset the endpoint back to UNINIT for reuse.
    void Reset();

    // ---- Data path (called by UrmaTransport) ----
    // Cut data from the IOBuf list and post it via URMA SEND.
    // Returns bytes sent, or -1 (errno set; EAGAIN when windows are full).
    ssize_t CutFromIOBufList(butil::IOBuf** data, size_t ndata);

    // Whether the endpoint can post more sends (both windows non-empty).
    bool IsWritable() const;

    int WaitWritable(butil::atomic<int>* butex, int expected,
                     const timespec* deadline);
    // For debug: dump endpoint state to @os.
    void DebugInfo(std::ostream& os,
                   butil::StringPiece connector = "\n") const;

    // Edge-triggered callback installed on the TCP socket when the transport
    // is URMA. Dispatches on _state (see the .cpp for the full state machine):
    //   UNINIT (server)  -> start the server handshake bthread
    //   < ESTABLISHED    -> wake the handshake bthread parked in ReadFromFd
    //   FALLBACK_TCP     -> hand off to InputMessenger::OnNewMessages
    //   ESTABLISHED      -> drain stray TCP bytes
    static void OnNewDataFromTcp(Socket* m);

    // Initialize the per-tag polling infrastructure (polling mode).
    static int PollingModeInitialize(bthread_tag_t tag,
                                     std::function<void()> callback,
                                     std::function<void()> init_fn,
                                     std::function<void()> release_fn);
    static void PollingModeRelease(bthread_tag_t tag);

    // ---- Handshake IO helpers (also used by urma_handshake.cpp) ----
    // Read at most @len bytes from the TCP fd into @data; waits on _read_butex
    // on EAGAIN. Returns 0 on success, -1 on IO error (errno set).
    int ReadFromFd(void* data, size_t len);
    // Push @len bytes back into the TCP input processor so it can re-parse
    // them (used on fallback when the magic is not "URMA").
    void PushBackToReadBuf(const void* data, size_t len);
    // Write at most @len bytes from @data to the TCP fd; waits on
    // _epollout_butex on EAGAIN. Returns 0 on success, -1 on IO error.
    int WriteToFd(void* data, size_t len);
    // Write an IOBuf to the TCP fd (used by v3 protobuf handshake).
    int WriteToFd(butil::IOBuf& data);

    // ---- Hello builders/parsers (used by urma_handshake.cpp) ----
    // Fill the v2 binary HelloMessage with this endpoint's local params.
    void FillLocalHelloV2(v2_wire::HelloMessage* out) const;
    // Fill the v3 protobuf UrmaHello with this endpoint's local params.
    void FillLocalHelloV3(UrmaHello* out) const;
    // Write "URM3" + 4B big-endian pb_size + protobuf bytes.
    int WriteHelloV3(const UrmaHello& msg);
    // Read pb_size (4B) + protobuf bytes; parse; validate. Sets *negotiated
    // to false (returns 0) if the message is invalid.
    int ReadAndParseHelloV3(ParsedHello* out, bool* negotiated);

private:
    enum State {
        UNINIT = 0x0,
        C_ALLOC_RES   = 0x1,  // client: allocate Jetty/JFC/JFR
        C_HELLO_SEND  = 0x2,
        C_HELLO_WAIT  = 0x3,
        C_IMPORT_PEER = 0x4,  // urma_import_seg then urma_import_jetty
        C_ACK_SEND    = 0x5,
        S_HELLO_WAIT  = 0x11,
        S_ALLOC_RES   = 0x12,
        S_IMPORT_PEER = 0x13,
        S_HELLO_SEND  = 0x14,
        S_ACK_WAIT    = 0x15,
        ESTABLISHED   = 0x100,
        FALLBACK_TCP  = 0x200,
        FAILED        = 0x300
    };

    // Process handshake at the client / server (run in a background bthread).
    static void* ProcessHandshakeAtClient(void* arg);
    static void* ProcessHandshakeAtServer(void* arg);

    // Publish per-connection resources once. DeallocateResources logically
    // closes without detaching; destruction/quiescent Reset releases ownership.
    // AllocateResources returns 0 on success.
    int AllocateResources(UrmaConnectionRole role);
    void DeallocateResources();

    // Import the peer's jetty and buffer-pool segment. CRITICAL:
    // urma_import_seg is called BEFORE urma_import_jetty, otherwise the kernel
    // does not establish the transport-path routing for the remote EID and
    // the first SEND is rejected with URMA_CR_RNR_RETRY_CNT_EXC_ERR.
    // Returns 0 on success.
    int ImportPeer(const ParsedHello& peer);

    // Queue received bytes for InputMessenger. CQ receive completions can
    // arrive while the server is still waiting for the final TCP handshake
    // ACK. Keep those bytes in the URMA input processor and dispatch them only
    // after ESTABLISHED, matching the connection-ready boundary seen by user
    // code.
    // Accept bytes only from the current, open URMA connection and append them
    // to the input stream. Return false when the delivery is rejected.
    bool AppendReceivedBytes(SocketUniquePtr&, const UrmaConnection*, butil::IOBuf);
    // Parse accumulated input and return the last message for caller dispatch.
    void ProcessReceivedBytes(SocketUniquePtr&, InputMessageClosure&);
    // Forward on_bytes to the Endpoint using the already pinned Socket.
    static bool OnReceivedBytes(SocketUniquePtr&, const UrmaConnection*, butil::IOBuf);
    // Forward input readiness and return the last parsed message to the caller.
    static void OnInputReady(SocketUniquePtr&, InputMessageClosure&);
    // Resolve a receive error by SocketId; ignore sockets no longer using URMA.
    static void OnRxError(SocketId, int);
    // Report an asynchronous error only if its source is the current connection.
    static void OnConnectionError(SocketId, const UrmaConnectionHandle&, int);
    // Caller must keep the Socket alive. Reset requires quiescent old users.
    UrmaConnection* BorrowConnection() const {
        return _published_connection.load(std::memory_order_acquire);
    }
    UrmaConnectionHandle PinConnection() const;

    inline void TryReadOnTcp();
    int SendFallbackHello(int version);
    void FallbackToTcp(UrmaTransport* transport, bool process_tcp);
    void FailHandshake(UrmaTransport* transport, int error,
                       const char* reason);

    // Construct a ParsedHello from local state (used by SendLocalHello).
    void MakeLocalParsedHello(ParsedHello* out) const;

    std::string GetStateStr() const;

    // Not owned.
    Socket* _socket;

    // Handshake state.
    butil::atomic<State> _state{UNINIT};
    int _handshake_version{0};  // 0 = unnegotiated; 2 = v2; 3 = v3
    butil::atomic<int64_t> _pending_received_bytes{0};
    butil::Mutex _dispatch_mutex;

    // Write once before release-publication; never detach on failure/fallback.
    // Socket references pin this owner. Only destruction or quiescent Reset
    // may release it. TX contexts retain their independent DMA/import owners.
    UrmaConnectionHandle _connection;
    std::atomic<UrmaConnection*> _published_connection{nullptr};
    // Protected by _dispatch_mutex; prevents publication after logical close.
    bool _connection_closed{false};
    InputMessengerProcessor _input_processor;

    // Butex for waking the handshake bthread parked in ReadFromFd.
    butil::atomic<int>* _read_butex{nullptr};

    DISALLOW_COPY_AND_ASSIGN(UrmaEndpoint);
};

}  // namespace urma
}  // namespace brpc

#else  // BRPC_WITH_URMA

namespace brpc {
namespace urma {
class UrmaEndpoint {};
}  // namespace urma
}  // namespace brpc

#endif  // BRPC_WITH_URMA

#endif  // BRPC_URMA_ENDPOINT_H
