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


#include <cctype>
#include <string>
#include <gflags/gflags.h>
#include "butil/atomicops.h"
#include "butil/logging.h"
#include "butil/time.h"
#if BRPC_WITH_RDMA
#include "brpc/rdma/rdma_helper.h"
#endif
#include "brpc/server.h"
#include "bvar/variable.h"
#include "test.pb.h"

namespace bthread {
DECLARE_int32(bthread_concurrency);
}

DEFINE_int32(port, 8002, "TCP Port of this server");
DEFINE_string(transport, "rdma",
              "Transport mode: tcp or rdma. When explicitly set, "
              "this overrides --use_rdma.");
DEFINE_bool(use_rdma, true,
            "Compatibility flag. Used only when --transport is not explicitly "
            "set: true means rdma, false means tcp.");
DEFINE_int32(server_num_threads, -1,
             "Number of brpc server worker threads. -1 keeps brpc default, "
             "0 lets bthread_concurrency control the worker count, >0 sets "
             "ServerOptions.num_threads explicitly");

butil::atomic<uint64_t> g_last_time(0);
bool g_transport_explicit = false;

bool IsFlagExplicitlySet(const char* flag_name) {
    GFLAGS_NAMESPACE::CommandLineFlagInfo info;
    return GFLAGS_NAMESPACE::GetCommandLineFlagInfo(flag_name, &info) &&
           !info.is_default;
}

struct TransportChoice {
    bool use_rdma;
    const char* name;
};

bool ResolveTransportMode(TransportChoice* choice) {
    std::string transport = FLAGS_transport;
    for (size_t i = 0; i < transport.size(); ++i) {
        transport[i] = static_cast<char>(
                std::tolower(static_cast<unsigned char>(transport[i])));
    }
    if (!g_transport_explicit) {
        transport = FLAGS_use_rdma ? "rdma" : "tcp";
    }

    if (transport == "tcp") {
        choice->use_rdma = false;
        choice->name = "tcp";
        return true;
    }
    if (transport == "rdma") {
#if BRPC_WITH_RDMA
        choice->use_rdma = true;
        choice->name = "rdma";
        return true;
#else
        LOG(ERROR) << "transport=rdma requires BRPC_WITH_RDMA=1. "
                   << "Rebuild brpc/example with RDMA support or use "
                   << "--transport=tcp.";
        return false;
#endif
    }
    LOG(ERROR) << "Invalid transport=" << FLAGS_transport
               << ", valid values are tcp and rdma";
    return false;
}

bool InitializeTransportRuntime(const TransportChoice& choice) {
    if (choice.use_rdma) {
#if BRPC_WITH_RDMA
        brpc::rdma::GlobalRdmaInitializeOrDie();
        return true;
#else
        return false;
#endif
    }
    return true;
}

namespace test {
class PerfTestServiceImpl : public PerfTestService {
public:
    PerfTestServiceImpl() {}
    ~PerfTestServiceImpl() {}

    void Test(google::protobuf::RpcController* cntl_base,
              const PerfTestRequest* request,
              PerfTestResponse* response,
              google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);
        uint64_t last = g_last_time.load(butil::memory_order_relaxed);
        uint64_t now = butil::monotonic_time_us();
        std::string cpu_usage;
        if (now > last && now - last > 100000) {
            if (g_last_time.exchange(now, butil::memory_order_relaxed) == last) {
                cpu_usage = bvar::Variable::describe_exposed("process_cpu_usage");
            } else {
                cpu_usage.clear();
            }
        } else {
            cpu_usage.clear();
        }
        response->set_cpu_usage(cpu_usage);
        brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);
        if (request->echo_attachment()) {
            cntl->response_attachment().append(cntl->request_attachment());
        }
    }
};
}

int main(int argc, char* argv[]) {
    GFLAGS_NAMESPACE::ParseCommandLineFlags(&argc, &argv, true);
    g_transport_explicit = IsFlagExplicitlySet("transport");
    TransportChoice transport;
    if (!ResolveTransportMode(&transport)) {
        return -1;
    }
    if (!InitializeTransportRuntime(transport)) {
        return -1;
    }

    brpc::Server server;
    test::PerfTestServiceImpl perf_test_service_impl;

    brpc::ServiceOptions service_options;
    service_options.ownership = brpc::SERVER_DOESNT_OWN_SERVICE;
    if (server.AddService(&perf_test_service_impl, service_options) != 0) {
        LOG(ERROR) << "Fail to add service";
        return -1;
    }
    g_last_time.store(0, butil::memory_order_relaxed);

    brpc::ServerOptions options;
    options.use_rdma = transport.use_rdma;
    if (FLAGS_server_num_threads >= 0) {
        options.num_threads = FLAGS_server_num_threads;
    }
    LOG(INFO) << "Starting stress_performance_server"
              << " port=" << FLAGS_port
              << " transport=" << transport.name
              << " server_num_threads=" << FLAGS_server_num_threads
              << " bthread_concurrency=" << bthread::FLAGS_bthread_concurrency;
    if (server.Start(FLAGS_port, &options) != 0) {
        LOG(ERROR) << "Fail to start stress_performance_server";
        return -1;
    }

    server.RunUntilAskedToQuit();
    return 0;
}
