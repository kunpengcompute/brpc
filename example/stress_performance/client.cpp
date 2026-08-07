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

#include <stdlib.h>
#include <unistd.h>

#include <errno.h>

#include <algorithm>
#include <cctype>
#include <iostream>
#include <memory>
#include <iomanip>
#include <limits>
#include <sstream>
#include <string>
#include <vector>

#include <gflags/gflags.h>

#include "butil/atomicops.h"
#include "butil/fast_rand.h"
#include "butil/logging.h"
#include "brpc/channel.h"
#if BRPC_WITH_RDMA
#include "brpc/rdma/rdma_helper.h"
#endif
#include "brpc/server.h"
#include "bthread/bthread.h"
#include "bvar/latency_recorder.h"
#include "bvar/variable.h"
#include "test.pb.h"

DEFINE_int32(thread_num, 0, "How many worker threads are used");
DEFINE_int32(queue_depth, 1, "How many requests are pending per worker in closed_loop mode");
DEFINE_int32(expected_qps, 0, "The expected QPS");
DEFINE_int32(max_thread_num, 16, "The max number of threads are used");
DEFINE_int32(attachment_size, -1, "Attachment size is used (in Bytes)");
DEFINE_bool(echo_attachment, false, "Select whether attachment should be echo");
DEFINE_string(connection_type, "single", "Connection type of the channel");
DEFINE_string(protocol, "baidu_std", "Protocol type.");
DEFINE_string(servers, "0.0.0.0:8002+0.0.0.0:8002", "IP Address of servers");
DEFINE_string(transport, "rdma",
              "Transport mode: tcp or rdma. When explicitly set, "
              "this overrides --use_rdma.");
DEFINE_bool(use_rdma, true,
            "Compatibility flag. Used only when --transport is not explicitly "
            "set: true means rdma, false means tcp.");
DEFINE_int32(rpc_timeout_ms, 2000, "RPC call timeout");
DEFINE_int32(connect_timeout_ms, -1,
             "Connection establishment timeout. -1 means use rpc_timeout_ms");
DEFINE_int32(test_seconds, 20, "Test running time");
DEFINE_int32(stop_grace_ms, 5000,
             "Max time to wait for in-flight RPCs after test_seconds is reached");
DEFINE_bool(log_rpc_error, false,
            "Print each failed RPC. Disabled by default to avoid log flooding under overload");
DEFINE_int32(dummy_port, 8001,
             "Dummy server port number. Set to -1 to disable the dummy server");
DEFINE_string(load_mode, "closed_loop", "Load mode of the client: closed_loop or open_loop");
DEFINE_int32(connection_num, 0, "How many Channel instances should be created");
DEFINE_int32(max_inflight, 0, "Global inflight limit in open_loop mode");
DEFINE_bool(unique_connection_group, false,
            "Create a unique connection_group per Channel to prevent SocketMap reuse");
DEFINE_bool(record_latency, true,
            "Record latency percentiles. Disable for max-throughput tests to reduce client-side stats overhead");

std::unique_ptr<bvar::LatencyRecorder> g_latency_recorder;
bvar::LatencyRecorder g_server_cpu_recorder("server_cpu");
bvar::LatencyRecorder g_client_cpu_recorder("client_cpu");
butil::atomic<uint64_t> g_last_time(0);
butil::atomic<uint64_t> g_total_bytes;
butil::atomic<uint64_t> g_total_cnt;
butil::atomic<uint64_t> g_failed_cnt;
butil::atomic<uint64_t> g_timeout_cnt;
butil::atomic<int64_t> g_inflight(0);
butil::atomic<int64_t> g_peak_inflight(0);
butil::atomic<uint32_t> g_rr_index(0);
butil::atomic<int64_t> g_token(10000);
butil::atomic<int64_t> g_open_loop_inflight_limit(0);
std::vector<std::string> g_servers;
volatile bool g_stop = false;
bool g_transport_explicit = false;

namespace {

const char* kClosedLoop = "closed_loop";
const char* kOpenLoop = "open_loop";
bool g_use_rdma = false;
std::string g_transport_name = "tcp";

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
    std::transform(transport.begin(), transport.end(), transport.begin(),
                   [](unsigned char c) { return std::tolower(c); });
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

struct ConnectionSlot {
    ConnectionSlot()
        : channel(NULL)
    {}

    ~ConnectionSlot() {
        delete channel;
        channel = NULL;
    }

    std::string server;
    std::string connection_group;
    brpc::Channel* channel;
};

struct Worker;

struct RespClosure {
    brpc::Controller* cntl;
    test::PerfTestResponse* resp;
    Worker* worker;
};

struct Worker {
    Worker(int worker_index_in, int attachment_size, bool echo_attachment)
        : addr(NULL)
        , worker_index(worker_index_in)
        , start_time_us(0)
        , stop(false)
        , echo_attachment_flag(echo_attachment)
    {
        if (attachment_size > 0) {
            addr = malloc(attachment_size);
            butil::fast_rand_bytes(addr, attachment_size);
            attachment.append(addr, attachment_size);
        }
    }

    ~Worker() {
        if (addr) {
            free(addr);
            addr = NULL;
        }
    }

    void* addr;
    int worker_index;
    uint64_t start_time_us;
    volatile bool stop;
    butil::IOBuf attachment;
    bool echo_attachment_flag;
};

std::vector<ConnectionSlot*> g_connection_slots;

static bool IsOpenLoop() {
    return FLAGS_load_mode == kOpenLoop;
}

static bool IsClosedLoop() {
    return FLAGS_load_mode == kClosedLoop;
}

static int LatencyWindowSeconds() {
    return FLAGS_test_seconds > 0 ? FLAGS_test_seconds : 10;
}

static uint64_t NowUs() {
    return butil::gettimeofday_us();
}

static bool ShouldStopByTime(uint64_t start_time_us) {
    return FLAGS_test_seconds > 0 &&
           NowUs() - start_time_us >= (uint64_t)FLAGS_test_seconds * 1000000UL;
}

static void UpdatePeakInflight(int64_t inflight) {
    int64_t peak = g_peak_inflight.load(butil::memory_order_relaxed);
    while (inflight > peak &&
           !g_peak_inflight.compare_exchange_weak(
                   peak, inflight, butil::memory_order_relaxed)) {
    }
}

static ConnectionSlot* PickConnectionSlot() {
    uint32_t index = g_rr_index.fetch_add(1, butil::memory_order_relaxed);
    return g_connection_slots[index % g_connection_slots.size()];
}

static bool AcquireQpsToken() {
    if (FLAGS_expected_qps <= 0) {
        return true;
    }
    while (!g_stop) {
        int64_t token = g_token.load(butil::memory_order_relaxed);
        if (token > 0 &&
            g_token.compare_exchange_weak(
                    token, token - 1, butil::memory_order_relaxed)) {
            return true;
        }
        bthread_usleep(10);
    }
    return false;
}

static bool TryAcquireRequestPermit(uint64_t start_time_us) {
    if (g_stop || ShouldStopByTime(start_time_us)) {
        return false;
    }
    if (!AcquireQpsToken()) {
        return false;
    }
    return true;
}

static void ReleaseInflightPermit() {
    g_inflight.fetch_sub(1, butil::memory_order_relaxed);
}

static std::string FormatQps(uint64_t completed_count, uint64_t elapsed_us) {
    if (elapsed_us == 0) {
        return "0.000";
    }
    const double qps = completed_count * 1000000.0 / elapsed_us;
    std::ostringstream oss;
    oss << std::fixed << std::setprecision(3);
    if (qps >= 1000.0) {
        oss << qps / 1000.0 << "k";
    } else {
        oss << qps;
    }
    return oss.str();
}

static void UpdateClientCpuSample() {
    uint64_t last = g_last_time.load(butil::memory_order_relaxed);
    uint64_t now = NowUs();
    if (now > last && now - last > 100000) {
        if (g_last_time.exchange(now, butil::memory_order_relaxed) == last) {
            g_client_cpu_recorder <<
                    atof(bvar::Variable::describe_exposed("process_cpu_usage").c_str()) * 100;
        }
    }
}

static void* GenerateToken(void* arg) {
    const int64_t start_time = butil::monotonic_time_ns();
    int64_t accumulative_token = g_token.load(butil::memory_order_relaxed);
    while (!g_stop) {
        bthread_usleep(100000);
        const int64_t now = butil::monotonic_time_ns();
        const long double expected_token_value =
                static_cast<long double>(FLAGS_expected_qps) *
                static_cast<long double>(now - start_time) / 1000000000.0L;
        const int64_t expected_token =
                expected_token_value >=
                        static_cast<long double>(std::numeric_limits<int64_t>::max())
                ? std::numeric_limits<int64_t>::max()
                : static_cast<int64_t>(expected_token_value);
        if (expected_token > accumulative_token) {
            const int64_t delta = expected_token - accumulative_token;
            g_token.fetch_add(delta, butil::memory_order_relaxed);
            accumulative_token = expected_token;
        }
    }
    return NULL;
}

static bool SendRequest(Worker* worker);

static bool ShouldStopWorkerAfterResponse(Worker* worker) {
    return ShouldStopByTime(worker->start_time_us);
}

static void ContinueClosedLoopIfNeeded(Worker* worker) {
    if (ShouldStopWorkerAfterResponse(worker)) {
        worker->stop = true;
        return;
    }
    if (IsClosedLoop() && !g_stop && !worker->stop) {
        SendRequest(worker);
    }
}

static void HandleResponse(RespClosure* closure) {
    std::unique_ptr<brpc::Controller> cntl_guard(closure->cntl);
    std::unique_ptr<test::PerfTestResponse> response_guard(closure->resp);
    std::unique_ptr<RespClosure> closure_guard(closure);
    Worker* worker = closure->worker;

    ReleaseInflightPermit();

    if (closure->cntl->Failed()) {
        g_failed_cnt.fetch_add(1, butil::memory_order_relaxed);
        if (closure->cntl->ErrorCode() == brpc::ERPCTIMEDOUT ||
            closure->cntl->ErrorCode() == ETIMEDOUT) {
            g_timeout_cnt.fetch_add(1, butil::memory_order_relaxed);
        }
        if (FLAGS_log_rpc_error && !g_stop) {
            LOG(ERROR) << "RPC call failed: " << closure->cntl->ErrorText();
        }
        ContinueClosedLoopIfNeeded(worker);
        return;
    }

    if (FLAGS_record_latency) {
        if (g_latency_recorder) {
            *g_latency_recorder << closure->cntl->latency_us();
        }
    }
    if (closure->resp != NULL && !closure->resp->cpu_usage().empty()) {
        g_server_cpu_recorder << atof(closure->resp->cpu_usage().c_str()) * 100;
    }
    const size_t payload_bytes = closure->cntl->request_attachment().size();
    g_total_bytes.fetch_add(payload_bytes, butil::memory_order_relaxed);
    g_total_cnt.fetch_add(1, butil::memory_order_relaxed);
    UpdateClientCpuSample();

    ContinueClosedLoopIfNeeded(worker);
}

static bool SendRequest(Worker* worker) {
    if (!TryAcquireRequestPermit(worker->start_time_us)) {
        worker->stop = true;
        return false;
    }

    ConnectionSlot* slot = PickConnectionSlot();
    RespClosure* closure = new RespClosure;
    closure->worker = worker;
    closure->cntl = new brpc::Controller();
    closure->resp = new test::PerfTestResponse();
    google::protobuf::Closure* done = brpc::NewCallback(&HandleResponse, closure);
    int64_t inflight = g_inflight.fetch_add(1, butil::memory_order_relaxed) + 1;
    UpdatePeakInflight(inflight);
    test::PerfTestRequest request;
    request.set_echo_attachment(worker->echo_attachment_flag);
    closure->cntl->request_attachment().append(worker->attachment);
    test::PerfTestService_Stub stub(slot->channel);
    stub.Test(closure->cntl, &request, closure->resp, done);
    return true;
}

static void* RunClosedLoopWorker(void* arg) {
    Worker* worker = static_cast<Worker*>(arg);
    worker->start_time_us = NowUs();
    for (int i = 0; i < FLAGS_queue_depth; ++i) {
        if (g_stop || worker->stop) {
            break;
        }
        SendRequest(worker);
    }
    return NULL;
}

static void* RunOpenLoopWorker(void* arg) {
    Worker* worker = static_cast<Worker*>(arg);
    worker->start_time_us = NowUs();
    const int64_t inflight_limit =
            g_open_loop_inflight_limit.load(butil::memory_order_relaxed);
    while (!g_stop && !worker->stop) {
        if (ShouldStopByTime(worker->start_time_us)) {
            worker->stop = true;
            break;
        }
        if (g_inflight.load(butil::memory_order_relaxed) >= inflight_limit) {
            bthread_usleep(50);
            continue;
        }
        if (!SendRequest(worker) && !worker->stop && !g_stop) {
            bthread_usleep(50);
        }
    }
    return NULL;
}

static int InitConnectionSlots(int connection_num, bool echo_attachment) {
    for (size_t i = 0; i < g_connection_slots.size(); ++i) {
        delete g_connection_slots[i];
    }
    g_connection_slots.clear();
    g_connection_slots.reserve(connection_num);

    for (int i = 0; i < connection_num; ++i) {
        ConnectionSlot* slot = new ConnectionSlot();
        slot->server = g_servers[i % g_servers.size()];
        if (FLAGS_unique_connection_group) {
            slot->connection_group = "rdma_perf_" + g_transport_name +
                                     "_conn_" + std::to_string(i);
        }
        brpc::ChannelOptions options;
        options.use_rdma = g_use_rdma;
        options.protocol = FLAGS_protocol;
        options.connection_type = FLAGS_connection_type;
        options.connect_timeout_ms =
                FLAGS_connect_timeout_ms >= 0 ? FLAGS_connect_timeout_ms : FLAGS_rpc_timeout_ms;
        options.timeout_ms = FLAGS_rpc_timeout_ms;
        options.max_retry = 0;
        options.connection_group = slot->connection_group;
        slot->channel = new brpc::Channel();
        if (slot->channel->Init(slot->server.c_str(), &options) != 0) {
            LOG(ERROR) << "Fail to initialize channel index=" << i;
            delete slot;
            return -1;
        }

        brpc::Controller cntl;
        test::PerfTestResponse response;
        test::PerfTestRequest request;
        request.set_echo_attachment(echo_attachment);
        test::PerfTestService_Stub stub(slot->channel);
        stub.Test(&cntl, &request, &response, NULL);
        if (cntl.Failed()) {
            LOG(ERROR) << "Warmup RPC failed on connection " << i << ": "
                       << cntl.ErrorText();
            delete slot;
            return -1;
        }
        g_connection_slots.push_back(slot);
    }
    return 0;
}

static void DestroyConnectionSlots() {
    for (size_t i = 0; i < g_connection_slots.size(); ++i) {
        delete g_connection_slots[i];
    }
    g_connection_slots.clear();
}

static void Test(int thread_num, int attachment_size) {
    const int actual_connection_num =
            FLAGS_connection_num > 0 ? FLAGS_connection_num : thread_num;
    const int64_t open_loop_inflight_limit =
            FLAGS_max_inflight > 0 ? FLAGS_max_inflight :
            (int64_t)thread_num * FLAGS_queue_depth;
    const int connect_timeout_ms =
            FLAGS_connect_timeout_ms >= 0 ? FLAGS_connect_timeout_ms : FLAGS_rpc_timeout_ms;
    std::cout << "[Threads: " << thread_num
              << ", Depth: " << FLAGS_queue_depth
              << ", Attachment: " << attachment_size << "B"
              << ", Transport: " << g_transport_name
              << ", Echo: " << (FLAGS_echo_attachment ? "yes" : "no")
              << ", RecordLatency: " << (FLAGS_record_latency ? "true" : "false")
              << ", LoadMode: " << FLAGS_load_mode
              << ", ConnectionType: " << FLAGS_connection_type
              << ", ConnectionNum: " << actual_connection_num
              << ", RpcTimeoutMs: " << FLAGS_rpc_timeout_ms
              << ", ConnectTimeoutMs: " << connect_timeout_ms
              << ", StopGraceMs: " << FLAGS_stop_grace_ms
              << ", UniqueConnectionGroup: "
              << (FLAGS_unique_connection_group ? "true" : "false")
              << ", ModelConcurrency: "
              << (IsClosedLoop() ? (int64_t)thread_num * FLAGS_queue_depth :
                                   open_loop_inflight_limit)
              << "]" << std::endl;
    if (IsClosedLoop()) {
        std::cout << "Closed-loop keeps roughly thread_num * queue_depth requests in flight."
                  << std::endl;
    } else {
        std::cout << "Open-loop keeps sending until max_inflight is hit, decoupled from worker count."
                  << std::endl;
    }

    const size_t request_bytes = std::max(attachment_size, 0);
    if (request_bytes >= 1024000 && FLAGS_echo_attachment && FLAGS_rpc_timeout_ms <= 2000) {
        LOG(WARNING) << "Large echo payload with rpc_timeout_ms=" << FLAGS_rpc_timeout_ms
                     << " may hit normal RPC timeout under pooled/open_loop pressure. "
                     << "Consider --rpc_timeout_ms=10000 or higher for large-payload tests.";
    }

    g_stop = false;
    g_last_time.store(0, butil::memory_order_relaxed);
    g_total_bytes.store(0, butil::memory_order_relaxed);
    g_total_cnt.store(0, butil::memory_order_relaxed);
    g_failed_cnt.store(0, butil::memory_order_relaxed);
    g_timeout_cnt.store(0, butil::memory_order_relaxed);
    g_inflight.store(0, butil::memory_order_relaxed);
    g_peak_inflight.store(0, butil::memory_order_relaxed);
    g_rr_index.store(0, butil::memory_order_relaxed);
    g_open_loop_inflight_limit.store(
            open_loop_inflight_limit, butil::memory_order_relaxed);
    if (FLAGS_record_latency) {
        g_latency_recorder.reset(
                new bvar::LatencyRecorder("client", LatencyWindowSeconds()));
    } else {
        g_latency_recorder.reset();
    }

    if (InitConnectionSlots(actual_connection_num, FLAGS_echo_attachment) < 0) {
        DestroyConnectionSlots();
        exit(1);
    }

    std::vector<Worker*> workers;
    workers.reserve(thread_num);
    for (int k = 0; k < thread_num; ++k) {
        workers.push_back(new Worker(k, attachment_size, FLAGS_echo_attachment));
    }

    uint64_t start_time = NowUs();
    std::vector<bthread_t> tids(thread_num);
    if (FLAGS_expected_qps > 0) {
        bthread_t tid;
        bthread_start_background(&tid, &BTHREAD_ATTR_NORMAL, GenerateToken, NULL);
    }
    for (int k = 0; k < thread_num; ++k) {
        void* (*fn)(void*) = IsClosedLoop() ? RunClosedLoopWorker : RunOpenLoopWorker;
        bthread_start_background(&tids[k], &BTHREAD_ATTR_NORMAL, fn, workers[k]);
    }

    bool stop_grace_expired = false;
    uint64_t stop_time_us = 0;
    while (true) {
        bool all_workers_stopped = true;
        for (int k = 0; k < thread_num; ++k) {
            if (!workers[k]->stop) {
                all_workers_stopped = false;
                break;
            }
        }
        bool no_more_inflight = g_inflight.load(butil::memory_order_relaxed) == 0;
        if ((all_workers_stopped && no_more_inflight) ||
            (FLAGS_test_seconds > 0 && NowUs() - start_time >=
                    (uint64_t)(FLAGS_test_seconds + 1) * 1000000UL &&
             no_more_inflight)) {
            break;
        }
        if (FLAGS_test_seconds > 0 && ShouldStopByTime(start_time)) {
            g_stop = true;
            if (stop_time_us == 0) {
                stop_time_us = NowUs();
            }
            if (!no_more_inflight && FLAGS_stop_grace_ms >= 0 &&
                NowUs() - stop_time_us >= (uint64_t)FLAGS_stop_grace_ms * 1000UL) {
                stop_grace_expired = true;
                LOG(ERROR) << "Stop grace expired with inflight="
                           << g_inflight.load(butil::memory_order_relaxed)
                           << ". Printing summary and exiting.";
                break;
            }
        }
        bthread_usleep(10000);
    }

    uint64_t end_time = NowUs();
    double throughput = 0;
    if (end_time > start_time) {
        throughput = g_total_bytes.load(butil::memory_order_relaxed) /
                     1.048576 / (end_time - start_time);
    }
    if (FLAGS_record_latency && g_latency_recorder) {
        std::cout << "Avg-Latency: " << g_latency_recorder->latency()
                  << ", 90th-Latency: " << g_latency_recorder->latency_percentile(0.9)
                  << ", 99th-Latency: " << g_latency_recorder->latency_percentile(0.99)
                  << ", 99.9th-Latency: " << g_latency_recorder->latency_percentile(0.999);
    } else {
        std::cout << "Avg-Latency: N/A"
                  << ", 90th-Latency: N/A"
                  << ", 99th-Latency: N/A"
                  << ", 99.9th-Latency: N/A";
    }
    std::cout << ", Throughput: " << throughput << "MB/s"
              << ", QPS: " << FormatQps(
                      g_total_cnt.load(butil::memory_order_relaxed),
                      end_time - start_time)
              << ", Completed: " << g_total_cnt.load(butil::memory_order_relaxed)
              << ", Failed: " << g_failed_cnt.load(butil::memory_order_relaxed)
              << ", Timeout: " << g_timeout_cnt.load(butil::memory_order_relaxed)
              << ", PeakInflight: " << g_peak_inflight.load(butil::memory_order_relaxed)
              << ", Server CPU-utilization: "
              << g_server_cpu_recorder.latency(10) << "%"
              << ", Client CPU-utilization: "
              << g_client_cpu_recorder.latency(10) << "%"
              << std::endl;

    if (stop_grace_expired) {
        fflush(NULL);
        _exit(0);
    }

    g_stop = true;
    for (int k = 0; k < thread_num; ++k) {
        delete workers[k];
    }
    DestroyConnectionSlots();
}

}  // namespace

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
    g_use_rdma = transport.use_rdma;
    g_transport_name = transport.name;

    if (!IsClosedLoop() && !IsOpenLoop()) {
        LOG(ERROR) << "Invalid load_mode=" << FLAGS_load_mode
                   << ", valid values are closed_loop and open_loop";
        return -1;
    }
    if (FLAGS_test_seconds <= 0) {
        LOG(ERROR) << "test_seconds must be > 0";
        return -1;
    }
    if (FLAGS_connect_timeout_ms < -1) {
        LOG(ERROR) << "connect_timeout_ms must be >= -1";
        return -1;
    }
    if (FLAGS_stop_grace_ms < -1) {
        LOG(ERROR) << "stop_grace_ms must be >= -1";
        return -1;
    }
    if (FLAGS_dummy_port < -1 || FLAGS_dummy_port >= 65536) {
        LOG(ERROR) << "dummy_port must be -1 or in [0, 65535]";
        return -1;
    }

    std::string server_list = FLAGS_servers;
    std::string::size_type pos1 = 0;
    std::string::size_type pos2 = server_list.find('+');
    while (pos2 != std::string::npos) {
        g_servers.push_back(server_list.substr(pos1, pos2 - pos1));
        pos1 = pos2 + 1;
        pos2 = server_list.find('+', pos1);
    }
    g_servers.push_back(server_list.substr(pos1));

    if (FLAGS_dummy_port >= 0 &&
        brpc::StartDummyServerAt(FLAGS_dummy_port) != 0) {
        LOG(WARNING) << "Failed to start the optional dummy server at port="
                     << FLAGS_dummy_port
                     << "; use --dummy_port=-1 to disable it or select a free port";
    }

    if (FLAGS_thread_num > 0 && FLAGS_attachment_size >= 0) {
        Test(FLAGS_thread_num, FLAGS_attachment_size);
    } else if (FLAGS_thread_num <= 0 && FLAGS_attachment_size >= 0) {
        for (int i = 1; i <= FLAGS_max_thread_num; i *= 2) {
            Test(i, FLAGS_attachment_size);
        }
    } else if (FLAGS_thread_num > 0 && FLAGS_attachment_size < 0) {
        for (int i = 1; i <= 1024; i *= 4) {
            Test(FLAGS_thread_num, i);
        }
    } else {
        for (int j = 1; j <= 1024; j *= 4) {
            for (int i = 1; i <= FLAGS_max_thread_num; i *= 2) {
                Test(i, j);
            }
        }
    }

    return 0;
}
