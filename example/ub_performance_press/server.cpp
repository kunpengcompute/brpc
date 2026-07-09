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
#include "butil/atomicops.h"
#include "butil/logging.h"
#include "butil/time.h"
#include "brpc/server.h"
#include "brpc/rpc_pb_message_factory.h"
#include "bvar/variable.h"
#include "press.pb.h"

DEFINE_int32(port, 8002, "TCP Port of this server");
DEFINE_bool(use_rdma, false, "Use RDMA or not");
DEFINE_bool(use_ub, false, "Use UB or not");

DEFINE_bool(server_ignore_oc, false, "Server ignore eovercrowded, false by default");
DEFINE_int32(num_threads, 5, "The max number of threads are used");
DEFINE_int32(max_concurrency, 128, "max concurrency");
DEFINE_int32(server_bthread_concurrency, 5, "server bthread concurrency");
DEFINE_int32(stats_timeout_seconds, 20, "Timeout in seconds before collecting statistics. (default 20)");
DEFINE_bool(echo_attachment, false, "Echo request attachment to response. Set on server side directly.");

butil::atomic<uint64_t> g_total_cnt(0);

static void* StatsPrinter(void* arg) {
    std::cout << "timing for " << FLAGS_stats_timeout_seconds << " seconds" << std::endl;
    bthread_usleep(FLAGS_stats_timeout_seconds * 1000000UL);
    std::cout << "total requests: " << g_total_cnt.load(butil::memory_order_relaxed) << std::endl;
    return nullptr;
}

namespace press {
class PerfTestServiceImpl : public PerfTestService {
public:
    PerfTestServiceImpl() {}

    ~PerfTestServiceImpl() {}

    void Test(google::protobuf::RpcController* cntl_base,
              const Request* request,
              Request* response,
              google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);
        response->Swap(const_cast<Request*>(request));
        if (FLAGS_echo_attachment) {
            brpc::Controller* cntl =
                static_cast<brpc::Controller*>(cntl_base);
            cntl->response_attachment().append(cntl->request_attachment());
        }
        if (g_total_cnt.fetch_add(1, butil::memory_order_relaxed) == 0) {
            bthread_t tid;
            bthread_start_background(&tid, &BTHREAD_ATTR_NORMAL, StatsPrinter, nullptr);
        }
    }
};
}

namespace bthread {
    DECLARE_int32(bthread_concurrency);
}

int main(int argc, char* argv[]) {
    GFLAGS_NAMESPACE::ParseCommandLineFlags(&argc, &argv, true);
    bthread::FLAGS_bthread_concurrency = FLAGS_server_bthread_concurrency;

    brpc::Server server;
    press::PerfTestServiceImpl perf_test_service_impl;

    g_total_cnt.store(0, butil::memory_order_relaxed);

    if (server.AddService(&perf_test_service_impl,
                          brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        LOG(ERROR) << "Fail to add service";
        return -1;
    }

    brpc::ServerOptions options;
    options.use_rdma = FLAGS_use_rdma;
    options.use_ub = FLAGS_use_ub;
    options.max_concurrency = FLAGS_max_concurrency;
    options.num_threads = FLAGS_num_threads;
    options.ignore_eovercrowded = FLAGS_server_ignore_oc;
    options.rpc_pb_message_factory = brpc::GetArenaRpcPBMessageFactory();
    if (server.Start(FLAGS_port, &options) != 0) {
        LOG(ERROR) << "Fail to start EchoServer";
        return -1;
    }

    server.RunUntilAskedToQuit();
    return 0;
}
