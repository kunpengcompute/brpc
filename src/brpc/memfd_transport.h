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

#ifndef BRPC_MEMFD_TRANSPORT_H
#define BRPC_MEMFD_TRANSPORT_H

#include "brpc/transport.h"
#include "brpc/socket.h"
#include "brpc/channel.h"
#include "brpc/server.h"
#include "brpc/memfd/shm_session.h"

namespace brpc {

class InputMessenger;

class MemfdTransport : public Transport {
friend class InputMessenger;
friend class MemfdAppConnect;
public:
    MemfdTransport();
    void Init(Socket* socket, const SocketOptions& options) override;
    void Release() override;
    int Reset(int32_t expected_nref) override;
    std::shared_ptr<AppConnect> Connect() override;
    int CutFromIOBuf(butil::IOBuf* buf) override;
    ssize_t CutFromIOBufList(butil::IOBuf** buf, size_t ndata) override;
    int WaitEpollOut(butil::atomic<int>* _epollout_butex, bool pollin, timespec duetime) override;
    void ProcessEvent(bthread_attr_t attr) override;
    void QueueMessage(InputMessageClosure& input_msg, int* num_bthread_created, bool last_msg) override;
    void Debug(std::ostream &os) override;
    void OnFdSet() override;
    bool IsHandshake() const { return _handshake_done; }
    void DoHandshake();
    static void* ServerMemfdSend(void* void_arg);
    void ServerMemfdCreateSend();
    bool IsServer() const { return _is_server; }
    ShmSession* Session() const { return _shm_session; }
    int ControlFd() {return _control_fd;}

    static int ContextInitOrDie(bool serverOrNot, const void* _options);

private:
    int CutFromIOBufWithoutNotify(butil::IOBuf* buf);
    void NotifyPeer();
    void ProcessShmData();
    void CompleteHandshake(int err);
    static void OnNewDataFromShm(Socket* m);
    static void ProcessShmBuffer(void* arg, const ShmQueueElement* elem);

    ShmSession* _shm_session;
    int _control_fd;
    bool _is_server;
    bool _initialized;
    bool _handshake_done;
    void (*_handshake_done_cb)(int err, void* data);
    void* _handshake_cb_data;
};

class MemfdAppConnect : public AppConnect {
public:
    MemfdAppConnect(MemfdTransport* transport) : _transport(transport) {}

    void StartConnect(const Socket* socket, void (*done)(int err, void* data), void* data) override;

    void StopConnect(Socket*) override {}

private:
    MemfdTransport* _transport;
};

} // namespace brpc

#endif // BRPC_MEMFD_TRANSPORT_H
