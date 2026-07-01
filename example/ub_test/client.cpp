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
#include <vector>
#include <climits>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <functional>
#include <future>
#include <fstream>
#include <algorithm>
#include <iomanip>
#include <gflags/gflags.h>
#include <random>
#include <thread>
#include <chrono>
#include "butil/atomicops.h"
#include "butil/fast_rand.h"
#include "butil/logging.h"
#ifdef WITH_RDMA
#include "brpc/rdma/rdma_helper.h"
#endif
#include "brpc/server.h"
#include "brpc/channel.h"
#include "bthread/bthread.h"
#include "bvar/latency_recorder.h"
#include "bvar/variable.h"
#include "test.pb.h"

// ==================== 命令行参数定义 ====================
// 来自ys thread_num 语义变更为总连接数
DEFINE_int32(thread_num, 1, "Total number of connections (replaces connections_per_server and max_connections_per_server)");
DEFINE_int32(queue_depth, 1, "How many requests can be pending in the queue");
DEFINE_int32(expected_qps, 0, "The expected QPS");
DEFINE_int64(initial_tokens, 10000, "The initial number of tokens");
DEFINE_int32(max_thread_num, 16, "The max number of threads are used");

// 来自ydl
DEFINE_int32(batch_size, 0, "Batch size for parallel connection (0 means no limit)");
DEFINE_int32(batch_interval_ms, 0, "Interval between batches in milliseconds");
DEFINE_int32(thread_pool_size, 8, "Thread pool size for parallel execution");
DEFINE_bool(only_first_rpc, false, "Only test first RPC connection, skip performance test");
DEFINE_int32(test_connections_per_round, 0, "Number of connections to select per round (0 means all)");
DEFINE_int32(round_period_ms, 0, "Target period per round in milliseconds (0 means no wait between rounds)");
DEFINE_bool(debug, false, "Enable debug mode to print latency details");

// 共用参数
DEFINE_int32(attachment_size, 0, "Attachment size is used (in Bytes)");
DEFINE_bool(echo_attachment, false, "Select whether attachment should be echo");
DEFINE_string(connection_type, "single", "Connection type of the channel");
DEFINE_string(protocol, "baidu_std", "Protocol type.");
DEFINE_string(servers, "0.0.0.0:8002+0.0.0.0:8002", "IP Address of servers (separated by '+')");
DEFINE_string(servers_file, "", "File path containing server list (one per line)");
DEFINE_bool(use_rdma, false, "Use RDMA or not");
DEFINE_bool(use_ub, false, "Use UB or not");
DEFINE_int32(rpc_timeout_ms, 2000, "RPC call timeout");
DEFINE_int32(test_seconds, 20, "Test running time");
DEFINE_int32(test_iterations, 0, "Test iterations");
DEFINE_int32(dummy_port, 8001, "Dummy server port number");
DEFINE_int32(connect_timeout_ms, 2000, "connect timeout");
DEFINE_int64(req_size, 0, "request size");
DEFINE_bool(client_ignore_oc, false, "Client ignore eovercrowded, false by default");
DEFINE_int32(max_retry, 3, "max retry times (0-1000)");
DEFINE_int32(connect_retry_interval, 200, "connect retry interval(ms)");
DEFINE_bool(use_connection_group, false, "Set connection_group for each channel or not");
DEFINE_bool(test_keep_alive, false, "Keep connections alive for 10 seconds after establishment");

// ==================== 全局变量 ====================
// 性能统计记录器 (bvar 内部线程安全)
bvar::LatencyRecorder g_latency_recorder("client");
bvar::LatencyRecorder g_server_cpu_recorder("server_cpu");
bvar::LatencyRecorder g_client_cpu_recorder("client_cpu");
bvar::LatencyRecorder g_client_memory_recorder("client_memory");

// 令牌桶
butil::atomic<int64_t> g_token(10000);
volatile bool g_stop = false;

// 首包延迟存储 (需要 mutex 保护)
std::vector<int64_t> g_connect_latencies_us;
std::vector<int64_t> g_first_rpc_latencies_us;
std::vector<double> g_first_rpc_memory_mbs;
pthread_mutex_t g_latency_mutex = PTHREAD_MUTEX_INITIALIZER;
butil::atomic<int64_t> g_channel_id(0);

// 原子计数器 (多线程安全)
butil::atomic<uint64_t> g_total_bytes;
butil::atomic<uint64_t> g_total_cnt;
butil::atomic<uint64_t> g_total_error_cnt(0);
butil::atomic<uint64_t> g_last_time(0);

// 服务器列表 (只读，无需保护)
std::vector<std::string> g_servers;

// 轮询索引 (原子操作保证线程安全)
butil::atomic<int> rr_index(0);

// 请求名称字符串
std::string g_name;

// ==================== 线程池实现 ====================
// 用于建链阶段的并行任务执行
class ThreadPool {
public:
    explicit ThreadPool(size_t num_threads) : stop_(false), active_tasks_(0) {
        for (size_t i = 0; i < num_threads; ++i) {
            workers_.emplace_back([this] {
                while (true) {
                    std::function<void()> task;
                    {
                        std::unique_lock<std::mutex> lock(queue_mutex_);
                        condition_.wait(lock, [this] { return stop_ || !tasks_.empty(); });
                        if (stop_ && tasks_.empty()) {
                            return;
                        }
                        task = std::move(tasks_.front());
                        tasks_.pop();
                    }
                    task();
                    {
                        std::unique_lock<std::mutex> lock(queue_mutex_);
                        --active_tasks_;
                    }
                    completed_.notify_all();
                }
            });
        }
    }

    ~ThreadPool() {
        {
            std::unique_lock<std::mutex> lock(queue_mutex_);
            stop_ = true;
        }
        condition_.notify_all();
        for (std::thread& worker : workers_) {
            worker.join();
        }
    }

    template<class F, class... Args>
    auto enqueue(F&& f, Args&&... args) -> std::future<typename std::result_of<F(Args...)>::type> {
        using return_type = typename std::result_of<F(Args...)>::type;
        auto task = std::make_shared<std::packaged_task<return_type()>>(
            std::bind(std::forward<F>(f), std::forward<Args>(args)...));
        std::future<return_type> result = task->get_future();
        {
            std::unique_lock<std::mutex> lock(queue_mutex_);
            if (stop_) {
                throw std::runtime_error("enqueue on stopped ThreadPool");
            }
            ++active_tasks_;
            tasks_.emplace([task]() { (*task)(); });
        }
        condition_.notify_one();
        return result;
    }

    void wait_all() {
        std::unique_lock<std::mutex> lock(queue_mutex_);
        completed_.wait(lock, [this] { return tasks_.empty() && active_tasks_ == 0; });
    }

private:
    std::vector<std::thread> workers_;
    std::queue<std::function<void()>> tasks_;
    std::mutex queue_mutex_;
    std::condition_variable condition_;
    std::condition_variable completed_;
    bool stop_;
    size_t active_tasks_;
};

// ==================== 性能测试类 ====================
// 首包延迟记录功能
class PerformanceTest {
public:
    PerformanceTest(int attachment_size, bool echo_attachment)
        : _addr(NULL)
        , _channel(NULL)
        , _connect_latency_us(0)
        , _first_rpc_latency_us(0)
        , _first_rpc_memory_mb(0)
        , _init_result(-1)
        , _start_time(0)
        , _iterations(0)
        , _stop(false)
    {
        if (attachment_size > 0) {
            _addr = malloc(attachment_size);
            butil::fast_rand_bytes(_addr, attachment_size);
            _attachment.append(_addr, attachment_size);
        }
        _echo_attachment = echo_attachment;
    }

    ~PerformanceTest() {
        if (_addr) {
            free(_addr);
        }
        delete _channel;
    }

    inline int64_t connect_latency_us() const { return _connect_latency_us; }
    inline int64_t first_rpc_latency_us() const { return _first_rpc_latency_us; }
    inline double first_rpc_memory_mb() const { return _first_rpc_memory_mb; }
    inline int init_result() const { return _init_result; }
    inline bool IsStop() { return _stop; }

    // 初始化 RPC 通道并执行首次 RPC (真正建立 TCP 连接)
    // 首包延迟记录
    int Init() {
        if (FLAGS_max_retry < 0 || FLAGS_max_retry > 1000) {
            LOG(WARNING) << "max_retry should be in [0, 1000], clamp to 3.";
            FLAGS_max_retry = 3;
        }

        brpc::ChannelOptions options;
        options.use_rdma = FLAGS_use_rdma;
        options.use_ub = FLAGS_use_ub;
        options.protocol = FLAGS_protocol;
        options.connection_type = FLAGS_connection_type;
        options.timeout_ms = FLAGS_rpc_timeout_ms;
        options.connect_timeout_ms = FLAGS_connect_timeout_ms;
        if (FLAGS_use_connection_group) {
            options.connection_group = "pt_" + std::to_string(g_channel_id.fetch_add(1, butil::memory_order_relaxed));
        }

        std::string server = g_servers[rr_index.fetch_add(1, butil::memory_order_relaxed) % g_servers.size()];
        _channel = new brpc::Channel();

        int64_t start_ns = butil::cpuwide_time_ns();
        int ret = _channel->Init(server.c_str(), &options);
        int64_t end_ns = butil::cpuwide_time_ns();
        _connect_latency_us = (end_ns - start_ns) / 1000;

        if (ret != 0) {
            LOG(ERROR) << "Fail to initialize channel";
            _init_result = -1;
            return -1;
        }

        test::PerfTestRequest request;
        request.set_echo_attachment(_echo_attachment);
        request.set_name(g_name);
        test::PerfTestService_Stub stub(_channel);

        int connect_retry_times = 0;
        while (connect_retry_times < FLAGS_max_retry) {
            brpc::Controller cntl;
            test::PerfTestResponse response;
            int64_t rpc_start_ns = butil::cpuwide_time_ns();
            stub.Test(&cntl, &request, &response, NULL);
            int64_t rpc_end_ns = butil::cpuwide_time_ns();

            if (cntl.Failed()) {
                LOG(WARNING) << connect_retry_times << "th, [First] RPC call failed: " << cntl.ErrorText() << ", retrying";
                std::random_device rd;
                std::mt19937 gen(rd());
                std::uniform_int_distribution<> distrib(FLAGS_connect_retry_interval, 2 * FLAGS_connect_retry_interval);
                int random_ms = distrib(gen);
                LOG(WARNING) << "waiting for " << random_ms << " ms";
                std::this_thread::sleep_for(std::chrono::milliseconds(random_ms));
            } else {
                _first_rpc_latency_us = (rpc_end_ns - rpc_start_ns) / 1000;
                _first_rpc_memory_mb = atof(bvar::Variable::describe_exposed("process_memory_resident").c_str()) / 1024 / 1024;
                _init_result = 0;
                break;
            }
            ++connect_retry_times;
        }

        if (connect_retry_times == FLAGS_max_retry) {
            LOG(ERROR) << "[First] RPC call failed, exiting...";
            _init_result = -1;
            return -1;
        }
        return 0;
    }

    // 发送请求
    void SendRequest() {
        if (FLAGS_expected_qps > 0) {
            while (g_token.load(butil::memory_order_relaxed) <= 0) {
                bthread_usleep(10);
            }
            g_token.fetch_sub(1, butil::memory_order_relaxed);
        }
        RespClosure* closure = new RespClosure;
        test::PerfTestRequest request;
        closure->resp = new test::PerfTestResponse();
        closure->cntl = new brpc::Controller();
        if (FLAGS_client_ignore_oc) {
            closure->cntl->ignore_eovercrowded();
        }
        request.set_echo_attachment(_echo_attachment);
        request.set_name(g_name);
        closure->cntl->request_attachment().append(_attachment);
        closure->test = this;
        google::protobuf::Closure* done = brpc::NewCallback(&HandleResponse, closure);
        test::PerfTestService_Stub stub(_channel);
        stub.Test(closure->cntl, &request, closure->resp, done);
    }

    // 异步 RPC 响应闭包
    struct RespClosure {
        brpc::Controller* cntl;
        test::PerfTestResponse* resp;
        PerformanceTest* test;
    };

    // 静态回调函数
    static void HandleResponse(RespClosure* closure) {
        std::unique_ptr<brpc::Controller> cntl_guard(closure->cntl);
        std::unique_ptr<test::PerfTestResponse> response_guard(closure->resp);
        if (closure->cntl->Failed()) {
            LOG(ERROR) << "[Performance] RPC call failed: " << closure->cntl->ErrorText();
            g_total_error_cnt.fetch_add(1, butil::memory_order_relaxed);
        } else {
            g_latency_recorder << closure->cntl->latency_us();
            if (closure->resp->cpu_usage().size() > 0) {
                g_server_cpu_recorder << atof(closure->resp->cpu_usage().c_str()) * 100;
            }

            if (FLAGS_req_size != 0) {
                g_total_bytes.fetch_add(closure->resp->name().size(), butil::memory_order_relaxed);
            } else {
                g_total_bytes.fetch_add(closure->cntl->request_attachment().size(), butil::memory_order_relaxed);
            }

            g_total_cnt.fetch_add(1, butil::memory_order_relaxed);
        }

        cntl_guard.reset(NULL);
        response_guard.reset(NULL);

        if (closure->test->_iterations == 0 && FLAGS_test_iterations > 0) {
            closure->test->_stop = true;
            return;
        }
        --closure->test->_iterations;
        uint64_t last = g_last_time.load(butil::memory_order_relaxed);
        uint64_t now = butil::gettimeofday_us();
        if (now > last && now - last > 100000) {
            if (g_last_time.exchange(now, butil::memory_order_relaxed) == last) {
                g_client_cpu_recorder <<
                    atof(bvar::Variable::describe_exposed("process_cpu_usage").c_str()) * 100;
                g_client_memory_recorder <<
                    atof(bvar::Variable::describe_exposed("process_memory_resident").c_str()) / 1024 / 1024;
            }
        }
        if (now - closure->test->_start_time > FLAGS_test_seconds * 1000000u) {
            closure->test->_stop = true;
            return;
        }
        closure->test->SendRequest();
    }

    static void* RunTest(void* arg) {
        PerformanceTest* test = (PerformanceTest*)arg;
        test->_start_time = butil::gettimeofday_us();
        test->_iterations = FLAGS_test_iterations;

        for (int i = 0; i < FLAGS_queue_depth; ++i) {
            test->SendRequest();
        }

        return NULL;
    }

private:
    void* _addr;
    brpc::Channel* _channel;
    int64_t _connect_latency_us;
    int64_t _first_rpc_latency_us;
    double _first_rpc_memory_mb;
    int _init_result;
    uint64_t _start_time;
    uint32_t _iterations;
    volatile bool _stop;
    butil::IOBuf _attachment;
    bool _echo_attachment;
};

// ==================== 令牌桶生成器 ====================
static void* GenerateToken(void* arg) {
    int64_t start_time = butil::monotonic_time_ns();
    int64_t accumulative_token = g_token.load(butil::memory_order_relaxed);
    while (!g_stop) {
        bthread_usleep(100000);
        int64_t now = butil::monotonic_time_ns();
        if (accumulative_token * 1000000000 / (now - start_time) < FLAGS_expected_qps) {
            int64_t delta = FLAGS_expected_qps * (now - start_time) / 1000000000 - accumulative_token;
            g_token.fetch_add(delta, butil::memory_order_relaxed);
            accumulative_token += delta;
        }
    }
    return NULL;
}

// ==================== 辅助函数 ====================
// 计算百分位延迟
static int64_t CalcPercentile(std::vector<int64_t>& latencies, double percentile) {
    if (latencies.empty()) return 0;
    std::sort(latencies.begin(), latencies.end());
    size_t idx = static_cast<size_t>(latencies.size() * percentile);
    if (idx >= latencies.size()) idx = latencies.size() - 1;
    return latencies[idx];
}

// 延迟统计结构
struct LatencyStats {
    int64_t avg = 0;
    int64_t min = INT64_MAX;
    int64_t max = 0;
    int64_t p99 = 0;
    int64_t p999 = 0;
    int64_t p9999 = 0;

    void Calculate(std::vector<int64_t>& latencies) {
        if (latencies.empty()) {
            min = 0;
            return;
        }
        for (auto lat : latencies) {
            avg += lat;
            if (lat > max) max = lat;
            if (lat < min) min = lat;
        }
        avg /= latencies.size();
        p99 = CalcPercentile(latencies, 0.99);
        p999 = CalcPercentile(latencies, 0.999);
        p9999 = CalcPercentile(latencies, 0.9999);
    }

    void Print(const std::string& name) const {
        std::cout << name << "(avg/min/max/p99/p999/p9999): ";
        if (min != 0 || max != 0) {
            std::cout << avg << "/" << min << "/" << max << "/" << p99 << "/" << p999 << "/" << p9999 << "us";
        } else {
            std::cout << "N/A";
        }
    }
};

// ==================== 分批建链函数 ====================
static int BatchEstablishConnections(
    int total_connections,
    int attachment_size,
    ThreadPool& pool,
    std::vector<PerformanceTest*>& tests,
    std::vector<std::future<int>>& futures) {

    int batch = (FLAGS_batch_size > 0) ? FLAGS_batch_size : total_connections;
    std::cout << "Creating thread pool with " << FLAGS_thread_pool_size << " threads..." << std::endl;
    std::cout << "Initializing connections in batches of " << batch << "..." << std::endl;

    uint64_t start_time = butil::gettimeofday_us();

    for (int batch_start = 0; batch_start < total_connections; batch_start += batch) {
        int batch_end = std::min(batch_start + batch, total_connections);
        int current_batch_size = batch_end - batch_start;
        int batch_index = batch_start / batch + 1;

        for (int k = 0; k < current_batch_size; ++k) {
            int idx = batch_start + k;
            tests[idx] = new PerformanceTest(attachment_size, FLAGS_echo_attachment);
            PerformanceTest* test = tests[idx];
            futures.push_back(pool.enqueue([test]() {
                return test->Init();
            }));
        }
        std::cout << "Batch " << batch_index << ": " << current_batch_size << " connections established, sleep " << FLAGS_batch_interval_ms << "ms" << std::endl;

        if (FLAGS_batch_interval_ms > 0 && batch_end < total_connections) {
            std::this_thread::sleep_for(std::chrono::milliseconds(FLAGS_batch_interval_ms));
        }
    }

    // 等待并收集结果
    int total_success = 0;
    for (int k = 0; k < total_connections; ++k) {
        if (futures[k].get() == 0) {
            total_success++;
            pthread_mutex_lock(&g_latency_mutex);
            g_connect_latencies_us.push_back(tests[k]->connect_latency_us());
            g_first_rpc_latencies_us.push_back(tests[k]->first_rpc_latency_us());
            g_first_rpc_memory_mbs.push_back(tests[k]->first_rpc_memory_mb());
            pthread_mutex_unlock(&g_latency_mutex);
        }
    }

    uint64_t end_time = butil::gettimeofday_us();
    uint64_t total_duration_us = end_time - start_time;
    std::cout << "Total: " << total_success << "/" << total_connections << " connections established in " 
              << total_duration_us << "us" << std::endl;
    return total_success;
}

// ==================== 首包统计输出 ====================
static void PrintLatencyStats() {
    LatencyStats connect_stats, first_rpc_stats;
    connect_stats.Calculate(g_connect_latencies_us);
    first_rpc_stats.Calculate(g_first_rpc_latencies_us);

    connect_stats.Print("Connect-Latency");
    std::cout << ", ";
    first_rpc_stats.Print("First-RPC-Latency");

    if (!g_first_rpc_memory_mbs.empty()) {
        double avg_mem = 0, min_mem = g_first_rpc_memory_mbs[0], max_mem = g_first_rpc_memory_mbs[0];
        for (auto m : g_first_rpc_memory_mbs) {
            avg_mem += m;
            if (m > max_mem) max_mem = m;
            if (m < min_mem) min_mem = m;
        }
        avg_mem /= g_first_rpc_memory_mbs.size();
        std::cout << ", First-RPC-Memory(avg/min/max): " << avg_mem << "/" << min_mem << "/" << max_mem << "MB";
    }

    std::cout << std::endl;
}

// ==================== 性能测试函数 ====================
static int RunPerformanceTest(std::vector<PerformanceTest*>& success_tests) {
    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(success_tests.begin(), success_tests.end(), g);

    int conn_per_round = (FLAGS_test_connections_per_round > 0)
        ? std::min(FLAGS_test_connections_per_round, (int)success_tests.size())
        : success_tests.size();

    std::cout << "Starting performance test: " << success_tests.size() << " connections total, "
              << conn_per_round << " per round, period " << FLAGS_round_period_ms << "ms, "
              << "duration " << FLAGS_test_seconds << "s" << std::endl;

    uint64_t start_time = butil::gettimeofday_us();
    int round = 0;
    size_t used_count = 0;

    while (used_count < success_tests.size()) {
        ++round;
        uint64_t round_start = butil::gettimeofday_us();

        std::vector<bthread_t> round_bthreads;
        int batch_size = std::min(conn_per_round, (int)(success_tests.size() - used_count));

        for (int i = 0; i < batch_size; ++i) {
            PerformanceTest* test = success_tests[used_count];
            ++used_count;

            bthread_t tid;
            bthread_start_background(&tid, &BTHREAD_ATTR_NORMAL, PerformanceTest::RunTest, test);
            round_bthreads.push_back(tid);
        }

        for (auto& tid : round_bthreads) {
            bthread_join(tid, NULL);
        }

        if (FLAGS_round_period_ms > 0) {
            uint64_t round_end = butil::gettimeofday_us();
            double elapsed_ms = (round_end - round_start) / 1000.0;
            double remaining_ms = FLAGS_round_period_ms - elapsed_ms;
            std::cout << "Round " << round << ": elapsed=" << std::fixed << std::setprecision(2) << elapsed_ms << "ms";
            if (remaining_ms > 0) {
                std::cout << ", sleep=" << remaining_ms << "ms" << std::endl;
                std::this_thread::sleep_for(std::chrono::milliseconds(static_cast<int64_t>(remaining_ms)));
            } else {
                std::cout << ", no sleep (overdue)" << std::endl;
            }
        }
    }

    for (size_t i = 0; i < success_tests.size(); ++i) {
        while (!success_tests[i]->IsStop()) {
            bthread_usleep(10000);
        }
    }

    uint64_t end_time = butil::gettimeofday_us();
    double throughput = g_total_bytes / 1.048576 / (end_time - start_time);

    std::cout << "Avg-Latency: " << g_latency_recorder.latency(10)
        << ", 50th-Latency: " << g_latency_recorder.latency_percentile(0.5)
        << ", 90th-Latency: " << g_latency_recorder.latency_percentile(0.9)
        << ", 99th-Latency: " << g_latency_recorder.latency_percentile(0.99)
        << ", 99.9th-Latency: " << g_latency_recorder.latency_percentile(0.999)
        << ", 99.99th-Latency: " << g_latency_recorder.latency_percentile(0.9999)
        << ", Max-Latency: " << g_latency_recorder.max_latency()
        << ", Throughput: " << throughput << "MB/s"
        << ", QPS: " << (g_total_cnt.load(butil::memory_order_relaxed) * 1000 * 1000 / (end_time - start_time))
        << ", Server CPU(avg/max): " << g_server_cpu_recorder.latency(10) << "/" << g_server_cpu_recorder.max_latency() << "\%"
        << ", Client CPU(avg/max): " << g_client_cpu_recorder.latency(10) << "/" << g_client_cpu_recorder.max_latency() << "\%"
        << ", Client Memory(avg/max): " << g_client_memory_recorder.latency(10) << "/" << g_client_memory_recorder.max_latency() << "MB"
 	    << ", Error rate " << (g_total_error_cnt.load(butil::memory_order_relaxed) * 1.0 / g_total_cnt.load(butil::memory_order_relaxed) * 100) << "%";
    std::cout << std::endl;

    std::cout << "Total rounds: " << round << std::endl;
    return round;
}

// ==================== 测试入口函数 ====================
// thread_num 直接作为总连接数使用
void Test(int thread_num, int attachment_size) {
    // 输出测试配置
    std::cout << "[Connections: " << thread_num
        << " (servers: " << g_servers.size()
        << ", per_server: " << (g_servers.empty() ? 0 : thread_num / g_servers.size()) << ")"
        << ", Attachment: " << attachment_size << "B"
        << ", string size: " << g_name.size() << "B"
        << ", RDMA: " << (FLAGS_use_rdma ? "yes" : "no")
        << ", Echo: " << (FLAGS_echo_attachment ? "yes" : "no")
        << ", Batch: " << (FLAGS_batch_size > 0 ? std::to_string(FLAGS_batch_size) : "unlimited")
        << ", BatchInterval: " << FLAGS_batch_interval_ms << "ms"
        << ", ThreadPool: " << FLAGS_thread_pool_size
        << ", QueueDepth: " << FLAGS_queue_depth
        << "]" << std::endl;

    // 重置全局计数器
    g_total_bytes.store(0, butil::memory_order_relaxed);
    g_total_cnt.store(0, butil::memory_order_relaxed);
    g_connect_latencies_us.clear();
    g_first_rpc_latencies_us.clear();
    g_first_rpc_memory_mbs.clear();

    // 阶段1: 分批建立连接（收集首包延迟统计）
    // 总连接数 = thread_num
    std::vector<PerformanceTest*> tests(thread_num);
    std::vector<std::future<int>> futures;
    futures.reserve(thread_num);
    ThreadPool pool(FLAGS_thread_pool_size);

    int total_success = BatchEstablishConnections(thread_num, attachment_size, pool, tests, futures);

    if (total_success == 0) {
        LOG(ERROR) << "No connections established, exiting...";
        for (auto t : tests) delete t;
        return;
    }

    // 阶段2: 统计首包延迟
    if (FLAGS_debug) {
        std::cout << "[Debug] Connect-Latencies (" << g_connect_latencies_us.size() << "): ";
        for (auto lat : g_connect_latencies_us) {
            std::cout << lat << " ";
        }
        std::cout << std::endl;
        std::cout << "[Debug] First-RPC-Latencies (" << g_first_rpc_latencies_us.size() << "): ";
        for (auto lat : g_first_rpc_latencies_us) {
            std::cout << lat << " ";
        }
        std::cout << std::endl;
    }

    PrintLatencyStats();

    // 只测首包模式
    if (FLAGS_only_first_rpc) {
         if (FLAGS_test_seconds > 0) {
            std::cout << "Keeping " << total_success << " connections alive for "
                      << FLAGS_test_seconds << " seconds..." << std::endl;
            std::this_thread::sleep_for(std::chrono::seconds(FLAGS_test_seconds));
            std::cout << "Keep duration elapsed, exiting..." << std::endl;
        }
        return;
    }

    // 阶段3: 性能测试 - 使用 thread_num 创建独立的 PerformanceTest 对象
    // 保留成功建立的连接用于性能测试
    std::vector<PerformanceTest*> success_tests;
    for (int k = 0; k < thread_num; ++k) {
        if (tests[k] && tests[k]->init_result() == 0) {
            success_tests.push_back(tests[k]);
        }
    }

    if (success_tests.empty()) {
        LOG(ERROR) << "No connections available for performance test";
        return;
    }

    // 启动令牌桶生成线程（如果需要限流）
    g_stop = false;
    if (FLAGS_expected_qps > 0) {
        bthread_t tid;
        bthread_start_background(&tid, &BTHREAD_ATTR_NORMAL, GenerateToken, NULL);
    }

    // 直接使用 RunPerformanceTest 进行性能测试
    RunPerformanceTest(success_tests);

    if (FLAGS_test_keep_alive) {
        std::cout << "Keeping connections alive for 10 seconds..." << std::endl;
        std::this_thread::sleep_for(std::chrono::seconds(10));
    }

    g_stop = true;
}

// ==================== 主函数 ====================
int main(int argc, char* argv[]) {
    GFLAGS_NAMESPACE::ParseCommandLineFlags(&argc, &argv, true);
    g_name.resize(FLAGS_req_size, 'r');

    g_token.store(FLAGS_initial_tokens);

#ifdef WITH_RDMA
    if (FLAGS_use_rdma) {
        brpc::rdma::GlobalRdmaInitializeOrDie();
    }
#endif

    brpc::StartDummyServerAt(FLAGS_dummy_port);

    // 解析服务器列表
    if (!FLAGS_servers_file.empty()) {
        std::ifstream file(FLAGS_servers_file);
        if (!file.is_open()) {
            LOG(ERROR) << "Failed to open servers file: " << FLAGS_servers_file;
            return -1;
        }
        std::string line;
        while (std::getline(file, line)) {
            if (!line.empty() && line[0] != '#') {
                g_servers.push_back(line);
            }
        }
        file.close();
        std::cout << "Loaded " << g_servers.size() << " servers from file: " << FLAGS_servers_file << std::endl;
    } else {
        std::string::size_type pos1 = 0;
        std::string::size_type pos2 = FLAGS_servers.find('+');
        while (pos2 != std::string::npos) {
            g_servers.push_back(FLAGS_servers.substr(pos1, pos2 - pos1));
            pos1 = pos2 + 1;
            pos2 = FLAGS_servers.find('+', pos1);
        }
        g_servers.push_back(FLAGS_servers.substr(pos1));
    }

    if (g_servers.empty()) {
        LOG(ERROR) << "No servers specified";
        return -1;
    }

    // thread_num 直接作为总连接数
    std::cout << "Total connections: " << FLAGS_thread_num
              << " (servers: " << g_servers.size() << ")" << std::endl;

    if (FLAGS_thread_num > 0 && FLAGS_attachment_size >= 0) {
        Test(FLAGS_thread_num, FLAGS_attachment_size);
    }
    else if (FLAGS_thread_num <= 0 && FLAGS_attachment_size >= 0) {
        for (int i = 1; i <= FLAGS_max_thread_num; i *= 2) {
            Test(i, FLAGS_attachment_size);
        }
    }
    else if (FLAGS_thread_num > 0 && FLAGS_attachment_size < 0) {
        for (int i = 1; i <= 1024; i *= 4) {
            Test(FLAGS_thread_num, i);
        }
    }
    else {
        for (int j = 1; j <= 1024; j *= 4) {
            for (int i = 1; i <= FLAGS_max_thread_num; i *= 2) {
                Test(i, j);
            }
        }
    }

    return 0;
}
