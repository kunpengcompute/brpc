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

#include <sys/eventfd.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <unistd.h>

#include <ifaddrs.h>
#include <netinet/in.h>

#include <atomic>
#include <chrono>
#include <limits>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include <gflags/gflags.h>
#include <gtest/gtest.h>

#include "brpc/channel.h"
#include "brpc/closure_guard.h"
#include "brpc/controller.h"
#include "brpc/memfd/memfd_endpoint.h"
#include "brpc/memfd_transport.h"
#include "brpc/server.h"
#include "brpc/socket.h"
#include "brpc/tcp_transport.h"
#include "brpc/transport_factory.h"
#include "echo.pb.h"

namespace brpc {

DECLARE_uint64(shm_buff_size);
DECLARE_int64(shm_block_size);
DECLARE_uint64(shm_queue_size);
DECLARE_bool(usercode_in_coroutine);

namespace {

class EchoService : public test::EchoService {
public:
    void Echo(google::protobuf::RpcController* controller,
              const test::EchoRequest* request,
              test::EchoResponse* response,
              google::protobuf::Closure* done) override {
        ClosureGuard done_guard(done);
        Controller* cntl = static_cast<Controller*>(controller);
        response->set_message(request->message());
        cntl->response_attachment() = cntl->request_attachment();
        last_socket_mode.store(cntl->socket_mode(), std::memory_order_relaxed);
        calls.fetch_add(1, std::memory_order_relaxed);
    }

    void ComboEcho(google::protobuf::RpcController*,
                   const test::ComboRequest* request,
                   test::ComboResponse* response,
                   google::protobuf::Closure* done) override {
        ClosureGuard done_guard(done);
        for (int i = 0; i < request->requests_size(); ++i) {
            response->add_responses()->set_message(
                request->requests(i).message());
        }
    }

    void BytesEcho1(google::protobuf::RpcController*,
                    const test::BytesRequest* request,
                    test::BytesResponse* response,
                    google::protobuf::Closure* done) override {
        ClosureGuard done_guard(done);
        response->set_databytes(request->databytes());
    }

    void BytesEcho2(google::protobuf::RpcController* controller,
                    const test::BytesRequest* request,
                    test::BytesResponse* response,
                    google::protobuf::Closure* done) override {
        BytesEcho1(controller, request, response, done);
    }

    std::atomic<int> calls{0};
    std::atomic<int> last_socket_mode{SOCKET_MODE_TCP};
};

struct ConnectResult {
    int calls = 0;
    int error = 0;
};

void SaveConnectResult(int error, void* arg) {
    ConnectResult* result = static_cast<ConnectResult*>(arg);
    ++result->calls;
    result->error = error;
}

class MemfdTransportTest : public testing::Test {
protected:
    static void SetUpTestSuite() {
        FLAGS_shm_block_size = 4096;
        FLAGS_shm_buff_size = 1024 * 1024;
        FLAGS_shm_queue_size = 64 * 1024;
        ASSERT_EQ(0, InitShmAllocator());
    }
};

TEST_F(MemfdTransportTest, FactoryCreatesSupportedTransports) {
    std::unique_ptr<Transport> tcp =
        TransportFactory::CreateTransport(SOCKET_MODE_TCP);
    ASSERT_NE(nullptr, tcp);
    EXPECT_NE(nullptr, dynamic_cast<TcpTransport*>(tcp.get()));

    std::unique_ptr<Transport> memfd =
        TransportFactory::CreateTransport(SOCKET_MODE_MEMFD);
    ASSERT_NE(nullptr, memfd);
    EXPECT_NE(nullptr, dynamic_cast<MemfdTransport*>(memfd.get()));

    EXPECT_EQ(nullptr, TransportFactory::CreateTransport(
                           static_cast<SocketMode>(99)));
    EXPECT_EQ(0, TransportFactory::ContextInitOrDie(
                     SOCKET_MODE_TCP, false, nullptr));
    EXPECT_EQ(0, TransportFactory::ContextInitOrDie(
                     SOCKET_MODE_MEMFD, false, nullptr));
    EXPECT_NE(0, TransportFactory::ContextInitOrDie(
                     static_cast<SocketMode>(99), false, nullptr));

#if !BRPC_WITH_RDMA
    EXPECT_EQ(nullptr, TransportFactory::CreateTransport(SOCKET_MODE_RDMA));
    EXPECT_NE(0, TransportFactory::ContextInitOrDie(
                     SOCKET_MODE_RDMA, false, nullptr));
    SocketOptions options;
    options.socket_mode = SOCKET_MODE_RDMA;
    SocketId id = INVALID_SOCKET_ID;
    EXPECT_NE(0, Socket::Create(options, &id));
#endif
}

TEST_F(MemfdTransportTest, LocalIpMapsToAbstractUnixEndpoint) {
    butil::ip_t loopback;
    ASSERT_EQ(0, butil::str2ip("127.0.0.1", &loopback));

    butil::EndPoint from_loopback;
    butil::EndPoint from_any;
    ASSERT_EQ(0, MakeMemfdEndpoint(
                     butil::EndPoint(loopback, 8123), &from_loopback));
    ASSERT_EQ(0, MakeMemfdEndpoint(
                     butil::EndPoint(butil::IP_ANY, 8123), &from_any));
    EXPECT_EQ(from_loopback, from_any);
    EXPECT_EQ(AF_UNIX, butil::get_endpoint_type(from_loopback));
    EXPECT_STREQ("unix:@brpc_memfd_8123",
                 butil::endpoint2str(from_loopback).c_str());

    butil::EndPoint invalid;
    EXPECT_NE(0, MakeMemfdEndpoint(
                     butil::EndPoint(loopback, 0), &invalid));

    butil::ip_t remote;
    ASSERT_EQ(0, butil::str2ip("192.0.2.1", &remote));
    EXPECT_NE(0, MakeMemfdEndpoint(
                     butil::EndPoint(remote, 8123), &invalid));

    // Force getifaddrs() to fail deterministically without a production hook:
    // its netlink socket cannot be opened while the soft fd limit is zero.
    rlimit original_limit;
    ASSERT_EQ(0, getrlimit(RLIMIT_NOFILE, &original_limit));
    rlimit no_fds = original_limit;
    no_fds.rlim_cur = 0;
    ASSERT_EQ(0, setrlimit(RLIMIT_NOFILE, &no_fds));
    const int fallback_result = MakeMemfdEndpoint(
        butil::EndPoint(remote, 8126), &invalid);
    const int fallback_errno = errno;
    ASSERT_EQ(0, setrlimit(RLIMIT_NOFILE, &original_limit));
    EXPECT_EQ(-1, fallback_result);
    EXPECT_EQ(ENETUNREACH, fallback_errno);

    Channel remote_channel;
    ChannelOptions options;
    options.socket_mode = SOCKET_MODE_MEMFD;
    EXPECT_EQ(-1, remote_channel.Init("192.0.2.1:8123", &options));

    EXPECT_EQ(-1, MakeMemfdEndpoint(
                      butil::EndPoint(loopback, 8123), nullptr));
    butil::EndPoint unchanged;
    EXPECT_EQ(0, MakeMemfdEndpoint(from_loopback, &unchanged));
    EXPECT_EQ(from_loopback, unchanged);

    butil::EndPoint local_interface;
    EXPECT_EQ(0, MakeMemfdEndpoint(
                     butil::EndPoint(butil::my_ip(), 8124),
                     &local_interface));
    EXPECT_EQ(AF_UNIX, butil::get_endpoint_type(local_interface));

    // Exercise interface enumeration as well as the loopback fast path. Some
    // minimal containers only expose loopback, in which case this part is not
    // an environment-independent requirement of the test.
    ifaddrs* addresses = nullptr;
    ASSERT_EQ(0, getifaddrs(&addresses));
    bool found_non_loopback = false;
    for (const ifaddrs* current = addresses;
         current != nullptr; current = current->ifa_next) {
        if (current->ifa_addr == nullptr ||
            current->ifa_addr->sa_family != AF_INET) {
            continue;
        }
        const sockaddr_in* ipv4 =
            reinterpret_cast<const sockaddr_in*>(current->ifa_addr);
        butil::ip_t interface_ip;
        interface_ip.s_addr = ipv4->sin_addr.s_addr;
        if ((ntohl(butil::ip2int(interface_ip)) & 0xff000000u) ==
            0x7f000000u) {
            continue;
        }
        butil::EndPoint mapped;
        EXPECT_EQ(0, MakeMemfdEndpoint(
                         butil::EndPoint(interface_ip, 8125), &mapped));
        EXPECT_EQ(AF_UNIX, butil::get_endpoint_type(mapped));
        found_non_loopback = true;
        break;
    }
    freeifaddrs(addresses);
    if (!found_non_loopback) {
        GTEST_LOG_(INFO) << "No non-loopback IPv4 interface is available";
    }
}

TEST_F(MemfdTransportTest, MemfdConnectCallbackAndLifecycle) {
    MemfdAppConnect invalid(nullptr);
    ConnectResult invalid_result;
    invalid.StartConnect(nullptr, SaveConnectResult, &invalid_result);
    EXPECT_EQ(1, invalid_result.calls);
    EXPECT_EQ(-1, invalid_result.error);

    MemfdTransport transport;
    SocketOptions options;
    options.socket_mode = SOCKET_MODE_MEMFD;
    transport.Init(nullptr, options);
    EXPECT_FALSE(transport.IsServer());
    EXPECT_FALSE(transport.IsHandshake());
    EXPECT_NE(nullptr, transport.Session());
    EXPECT_NE(nullptr, transport.Connect());

    ConnectResult result;
    transport.Connect()->StartConnect(nullptr, SaveConnectResult, &result);
    EXPECT_EQ(0, result.calls);
    transport.CompleteHandshake(0);
    EXPECT_EQ(1, result.calls);
    EXPECT_EQ(0, result.error);
    transport.CompleteHandshake(-1);
    EXPECT_EQ(1, result.calls);

    std::ostringstream os;
    transport.Debug(os);
    EXPECT_NE(std::string::npos, os.str().find("initialized=1"));
    EXPECT_EQ(0, transport.Reset(0));
    EXPECT_EQ(nullptr, transport.Session());
    transport.Release();
}

TEST_F(MemfdTransportTest, HandshakeAndServerSetupFailuresAreReported) {
    const bool saved_coroutine = FLAGS_usercode_in_coroutine;
    FLAGS_usercode_in_coroutine = true;

    {
        SocketOptions options;
        options.socket_mode = SOCKET_MODE_MEMFD;
        SocketId id;
        ASSERT_EQ(0, Socket::Create(options, &id));
        SocketUniquePtr socket;
        ASSERT_EQ(0, Socket::Address(id, &socket));
        MemfdTransport* transport =
            static_cast<MemfdTransport*>(socket->_transport.get());
        ConnectResult result;
        transport->Connect()->StartConnect(
            socket.get(), SaveConnectResult, &result);
        transport->_control_fd = -1;
        transport->DoHandshake();
        EXPECT_EQ(1, result.calls);
        EXPECT_EQ(-1, result.error);
        EXPECT_FALSE(transport->IsHandshake());
        transport->OnFdSet();
        socket->SetFailed();
    }

    {
        SocketOptions options;
        options.socket_mode = SOCKET_MODE_MEMFD;
        options.is_server = true;
        SocketId id;
        ASSERT_EQ(0, Socket::Create(options, &id));
        SocketUniquePtr socket;
        ASSERT_EQ(0, Socket::Address(id, &socket));
        MemfdTransport* transport =
            static_cast<MemfdTransport*>(socket->_transport.get());
        transport->_control_fd = -1;
        transport->DoHandshake();
        EXPECT_TRUE(socket->Failed());
    }

    {
        const uint64_t saved_queue_size = FLAGS_shm_queue_size;
        FLAGS_shm_queue_size =
            sizeof(ShmQueueHeader) + sizeof(ShmQueueElement) - 1;
        SocketOptions options;
        options.socket_mode = SOCKET_MODE_MEMFD;
        options.is_server = true;
        SocketId id;
        ASSERT_EQ(0, Socket::Create(options, &id));
        SocketUniquePtr socket;
        ASSERT_EQ(0, Socket::Address(id, &socket));
        MemfdTransport* transport =
            static_cast<MemfdTransport*>(socket->_transport.get());
        transport->ServerMemfdCreateSend();
        EXPECT_TRUE(socket->Failed());
        FLAGS_shm_queue_size = saved_queue_size;
    }

    {
        SocketOptions options;
        options.socket_mode = SOCKET_MODE_MEMFD;
        options.is_server = true;
        SocketId id;
        ASSERT_EQ(0, Socket::Create(options, &id));
        SocketUniquePtr socket;
        ASSERT_EQ(0, Socket::Address(id, &socket));
        MemfdTransport* transport =
            static_cast<MemfdTransport*>(socket->_transport.get());
        EXPECT_EQ(nullptr, MemfdTransport::ServerMemfdSend(transport));
        EXPECT_TRUE(socket->Failed());
    }

    FLAGS_usercode_in_coroutine = saved_coroutine;
}

TEST_F(MemfdTransportTest, TcpTransportCutsSingleAndMultipleBuffers) {
    int fds[2];
    ASSERT_EQ(0, socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, fds));

    SocketOptions options;
    options.fd = fds[0];
    options.socket_mode = SOCKET_MODE_TCP;
    SocketId id;
    ASSERT_EQ(0, Socket::Create(options, &id));

    SocketUniquePtr socket;
    ASSERT_EQ(0, Socket::Address(id, &socket));
    ASSERT_NE(nullptr, dynamic_cast<TcpTransport*>(socket->_transport.get()));

    butil::IOBuf first;
    first.append("first", 5);
    EXPECT_EQ(5, socket->_transport->CutFromIOBuf(&first));
    EXPECT_TRUE(first.empty());

    char received[32] = {};
    ASSERT_EQ(5, read(fds[1], received, sizeof(received)));
    EXPECT_EQ("first", std::string(received, 5));

    butil::IOBuf second;
    butil::IOBuf third;
    second.append("second", 6);
    third.append("third", 5);
    butil::IOBuf* buffers[] = {&second, &third};
    EXPECT_EQ(11, socket->_transport->CutFromIOBufList(buffers, 2));
    ASSERT_EQ(11, read(fds[1], received, sizeof(received)));
    EXPECT_EQ("secondthird", std::string(received, 11));
    EXPECT_EQ(0, socket->_transport->Reset(0));

    socket->SetFailed();
    socket.reset();
    close(fds[1]);
}

TEST_F(MemfdTransportTest, MemfdTransportQueuesNormalAndShmBuffers) {
    SocketOptions options;
    options.socket_mode = SOCKET_MODE_MEMFD;
    SocketId id;
    ASSERT_EQ(0, Socket::Create(options, &id));

    SocketUniquePtr socket;
    ASSERT_EQ(0, Socket::Address(id, &socket));
    MemfdTransport* transport =
        dynamic_cast<MemfdTransport*>(socket->_transport.get());
    ASSERT_NE(nullptr, transport);

    butil::IOBuf normal;
    normal.append("normal", 6);
    EXPECT_EQ(6, transport->CutFromIOBuf(&normal));
    EXPECT_TRUE(normal.empty());
    ShmQueue* send_queue =
        transport->Session()->queue_manager()->send_queue();
    EXPECT_EQ(1u, send_queue->Size());

    butil::IOBuf shared;
    ASSERT_EQ(0, shared.append("shared", 6, butil::IOBuf::BUFFER_TYPE_SHM));
    EXPECT_EQ(6, transport->CutFromIOBuf(&shared));
    EXPECT_TRUE(shared.empty());
    EXPECT_EQ(2u, send_queue->Size());

    butil::IOBuf a;
    butil::IOBuf b;
    a.append("a", 1);
    b.append("bc", 2);
    butil::IOBuf* buffers[] = {&a, &b};
    EXPECT_EQ(3, transport->CutFromIOBufList(buffers, 2));
    EXPECT_EQ(4u, send_queue->Size());

    // Messages above the coalescing threshold retain the original block-wise
    // zero-copy/copy path.
    butil::IOBuf large;
    large.append(std::string(8 * 1024, 'L'));
    EXPECT_EQ(8 * 1024, transport->CutFromIOBuf(&large));
    EXPECT_TRUE(large.empty());
    EXPECT_GT(send_queue->Size(), 4u);

    std::ostringstream os;
    transport->Debug(os);
    EXPECT_NE(std::string::npos, os.str().find("handshake_done=0"));
    socket->SetFailed();
}

TEST_F(MemfdTransportTest, MemfdTransportHandlesFullQueueAndBadNotifyFd) {
    SocketOptions options;
    options.socket_mode = SOCKET_MODE_MEMFD;
    SocketId id;
    ASSERT_EQ(0, Socket::Create(options, &id));
    SocketUniquePtr socket;
    ASSERT_EQ(0, Socket::Address(id, &socket));
    MemfdTransport* transport =
        static_cast<MemfdTransport*>(socket->_transport.get());
    ASSERT_NE(nullptr, transport);
    ShmQueue* queue =
        transport->Session()->queue_manager()->send_queue();
    while (queue->Enqueue(0, 0) == 0) {
    }
    ASSERT_FALSE(queue->IsFree());

    butil::IOBuf small;
    small.append("small", 5);
    EXPECT_EQ(0, transport->CutFromIOBuf(&small));
    EXPECT_EQ(5u, small.size());

    butil::IOBuf shared;
    const std::string shared_data(8 * 1024, 's');
    ASSERT_EQ(0, shared.append(
        shared_data.data(), shared_data.size(),
        butil::IOBuf::BUFFER_TYPE_SHM));
    EXPECT_EQ(0, transport->CutFromIOBuf(&shared));
    EXPECT_EQ(8u * 1024, shared.size());

    int dead_notify_fd = eventfd(0, EFD_CLOEXEC);
    ASSERT_GE(dead_notify_fd, 0);
    close(dead_notify_fd);
    transport->_control_fd = dead_notify_fd;
    queue->SetReading(false);
    transport->NotifyPeer();

    ShmQueueElement invalid = {
        std::numeric_limits<size_t>::max(), 1,
    };
    MemfdTransport::ProcessShmBuffer(transport, &invalid);

    socket->SetFailed();
}

TEST_F(MemfdTransportTest, MemfdWaitEpollOutHandlesQueueAndAllocatorPressure) {
    SocketOptions options;
    options.socket_mode = SOCKET_MODE_MEMFD;
    SocketId id;
    ASSERT_EQ(0, Socket::Create(options, &id));
    SocketUniquePtr socket;
    ASSERT_EQ(0, Socket::Address(id, &socket));
    MemfdTransport* transport =
        static_cast<MemfdTransport*>(socket->_transport.get());
    ASSERT_NE(nullptr, transport);

    timespec deadline;
    clock_gettime(CLOCK_REALTIME, &deadline);
    ++deadline.tv_sec;
    EXPECT_EQ(0, transport->WaitEpollOut(
                     nullptr, false, deadline));

    ShmQueue* queue =
        transport->Session()->queue_manager()->send_queue();
    while (queue->Enqueue(0, 0) == 0) {
    }
    std::atomic<int> queue_wait_result(99);
    std::thread queue_waiter([&] {
        queue_wait_result.store(
            transport->WaitEpollOut(nullptr, false, deadline));
    });
    for (int i = 0;
         i < 1000 && queue->_free_notify.load() == 0; ++i) {
        std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
    ASSERT_GT(queue->_free_notify.load(), 0u);
    EXPECT_EQ(1, queue->DequeueAndProcessBatch(
                     [](void*, const ShmQueueElement*) {}, nullptr, 1));
    queue_waiter.join();
    EXPECT_EQ(0, queue_wait_result.load());

    ShmBlockAllocator* allocator = GetShmAllocator();
    std::vector<butil::IOBuf::Block*> blocks;
    while (butil::IOBuf::Block* block = allocator->TryAllocBlock()) {
        blocks.push_back(block);
    }
    ASSERT_FALSE(blocks.empty());
    std::atomic<int> allocator_wait_result(99);
    std::thread allocator_waiter([&] {
        allocator_wait_result.store(
            transport->WaitEpollOut(nullptr, false, deadline));
    });
    for (int i = 0; i < 1000 && !allocator->IsWaitNotify(); ++i) {
        std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
    ASSERT_TRUE(allocator->IsWaitNotify());
    butil::IOBuf::Block* released = blocks.back();
    blocks.pop_back();
    const uint32_t released_index =
        allocator->GetBlockIndex(released);
    released->~Block();
    allocator->FreeBlock(released_index);
    allocator_waiter.join();
    EXPECT_EQ(0, allocator_wait_result.load());

    butil::IOBuf::Block* replacement = allocator->TryAllocBlock();
    ASSERT_NE(nullptr, replacement);
    clock_gettime(CLOCK_REALTIME, &deadline);
    deadline.tv_nsec += 5 * 1000 * 1000;
    if (deadline.tv_nsec >= 1000 * 1000 * 1000) {
        ++deadline.tv_sec;
        deadline.tv_nsec -= 1000 * 1000 * 1000;
    }
    EXPECT_EQ(-1, transport->WaitEpollOut(
                      nullptr, false, deadline));
    EXPECT_EQ(EAGAIN, errno);

    const uint32_t replacement_index =
        allocator->GetBlockIndex(replacement);
    replacement->~Block();
    allocator->FreeBlock(replacement_index);
    for (butil::IOBuf::Block* block : blocks) {
        const uint32_t index = allocator->GetBlockIndex(block);
        block->~Block();
        allocator->FreeBlock(index);
    }
    socket->SetFailed();
}

TEST_F(MemfdTransportTest, MemfdReadCallbackHandlesEofAndBadDescriptor) {
    {
        int fds[2];
        ASSERT_EQ(0, socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC,
                                0, fds));
        SocketOptions options;
        options.fd = fds[0];
        options.socket_mode = SOCKET_MODE_MEMFD;
        SocketId id;
        ASSERT_EQ(0, Socket::Create(options, &id));
        SocketUniquePtr socket;
        ASSERT_EQ(0, Socket::Address(id, &socket));
        MemfdTransport* transport =
            static_cast<MemfdTransport*>(socket->_transport.get());
        transport->_handshake_done = true;
        close(fds[1]);
        MemfdTransport::OnNewDataFromShm(socket.get());
        EXPECT_TRUE(socket->Failed());
        socket->SetFailed();
    }

    {
        SocketOptions options;
        options.socket_mode = SOCKET_MODE_MEMFD;
        SocketId id;
        ASSERT_EQ(0, Socket::Create(options, &id));
        SocketUniquePtr socket;
        ASSERT_EQ(0, Socket::Address(id, &socket));
        MemfdTransport* transport =
            static_cast<MemfdTransport*>(socket->_transport.get());
        transport->_handshake_done = true;
        MemfdTransport::OnNewDataFromShm(socket.get());
        EXPECT_TRUE(socket->Failed());
    }
}

void AssertEcho(test::EchoService_Stub* stub,
                const std::string& message,
                const std::string& attachment,
                SocketMode expected_socket_mode) {
    Controller cntl;
    test::EchoRequest request;
    test::EchoResponse response;
    request.set_message(message);
    cntl.request_attachment().append(attachment);
    stub->Echo(&cntl, &request, &response, nullptr);
    ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
    EXPECT_EQ(expected_socket_mode, cntl.socket_mode());
    EXPECT_EQ(message, response.message());
    EXPECT_EQ(attachment, cntl.response_attachment().to_string());
}

TEST_F(MemfdTransportTest, MemfdRpcHandlesPayloadsAttachmentsAndConcurrency) {
    const int port = 20000 + getpid() % 30000;
    EchoService service;
    Server server;
    ASSERT_EQ(0, server.AddService(&service, SERVER_DOESNT_OWN_SERVICE));

    ServerOptions server_options;
    server_options.socket_mode = SOCKET_MODE_MEMFD;
    server_options.has_builtin_services = false;
    ASSERT_EQ(0, server.Start(port, &server_options));
    EXPECT_EQ(AF_UNIX, butil::get_endpoint_type(server.listen_address()));
    EXPECT_EQ("unix:@brpc_memfd_" + std::to_string(port),
              std::string(butil::endpoint2str(
                  server.listen_address()).c_str()));

    Channel channel;
    ChannelOptions channel_options;
    channel_options.socket_mode = SOCKET_MODE_MEMFD;
    channel_options.connection_type = CONNECTION_TYPE_SINGLE;
    ASSERT_EQ(0, channel.Init("127.0.0.1", port, &channel_options));
    test::EchoService_Stub stub(&channel);

    AssertEcho(&stub, "", "", SOCKET_MODE_MEMFD);
    AssertEcho(&stub, "small", "attachment", SOCKET_MODE_MEMFD);
    const std::string large(32 * 1024, 'L');
    AssertEcho(&stub, large, std::string(12 * 1024, 'A'),
               SOCKET_MODE_MEMFD);

    std::atomic<int> failures(0);
    std::vector<std::thread> threads;
    for (int i = 0; i < 4; ++i) {
        threads.emplace_back([&, i] {
            for (int j = 0; j < 5; ++j) {
                Controller cntl;
                test::EchoRequest request;
                test::EchoResponse response;
                const std::string value =
                    "concurrent-" + std::to_string(i) + "-" +
                    std::to_string(j) + std::string(2048, 'x');
                request.set_message(value);
                stub.Echo(&cntl, &request, &response, nullptr);
                if (cntl.Failed() || response.message() != value) {
                    failures.fetch_add(1);
                }
            }
        });
    }
    for (std::thread& thread : threads) {
        thread.join();
    }
    EXPECT_EQ(0, failures.load());
    EXPECT_EQ(SOCKET_MODE_MEMFD, service.last_socket_mode.load());
    EXPECT_EQ(23, service.calls.load());

    ASSERT_EQ(0, server.Stop(0));
    ASSERT_EQ(0, server.Join());
}

TEST_F(MemfdTransportTest, TcpRpcRegressesConnectionTypes) {
    EchoService service;
    Server server;
    ASSERT_EQ(0, server.AddService(&service, SERVER_DOESNT_OWN_SERVICE));
    ServerOptions server_options;
    server_options.has_builtin_services = false;
    ASSERT_EQ(0, server.Start("127.0.0.1:0", &server_options));

    const ConnectionType connection_types[] = {
        CONNECTION_TYPE_SINGLE,
        CONNECTION_TYPE_POOLED,
        CONNECTION_TYPE_SHORT,
    };
    for (ConnectionType type : connection_types) {
        Channel channel;
        ChannelOptions options;
        options.socket_mode = SOCKET_MODE_TCP;
        options.connection_type = type;
        ASSERT_EQ(0, channel.Init("127.0.0.1",
                                  server.listen_address().port, &options));
        test::EchoService_Stub stub(&channel);
        AssertEcho(&stub, "tcp", "", SOCKET_MODE_TCP);
    }
    EXPECT_EQ(SOCKET_MODE_TCP, service.last_socket_mode.load());
    ASSERT_EQ(0, server.Stop(0));
    ASSERT_EQ(0, server.Join());
}

TEST_F(MemfdTransportTest, InvalidSocketModeIsRejected) {
    Channel channel;
    ChannelOptions options;
    options.socket_mode = static_cast<SocketMode>(99);
    EXPECT_EQ(-1, channel.Init("127.0.0.1:1", &options));

    Server server;
    ServerOptions server_options;
    server_options.socket_mode = static_cast<SocketMode>(99);
    EXPECT_EQ(-1, server.Start("127.0.0.1:0", &server_options));
}

}  // namespace
}  // namespace brpc
