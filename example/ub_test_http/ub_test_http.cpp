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

// A combined HTTP server + RPC pressure test tool.
// Exposes REST endpoints to start/manage tests and runs the same
// performance-test infrastructure as example/ub_test/client.cpp.

#include <cstring>
#include <mutex>
#include <stdlib.h>
#include <unistd.h>
#include <cmath>
#include <vector>
#include <memory>
#include <climits>
#include <algorithm>
#include <iomanip>
#include <gflags/gflags.h>
#include <random>
#include <thread>
#include <chrono>
#include <pthread.h>
#include "butil/atomicops.h"
#include "butil/fast_rand.h"
#include "butil/logging.h"
#if BRPC_WITH_URMA
#include "brpc/urma/urma_helper.h"
#endif
#include "bvar/latency_recorder.h"
#include "bvar/variable.h"
#include "brpc/server.h"
#include "brpc/channel.h"
#include "brpc/retry_policy.h"
#include "brpc/restful.h"
#include "brpc/http_method.h"
#include "bthread/bthread.h"
#include "test.pb.h"
#include "ub_test_http.pb.h"

// ==================== HTTP server flags ====================
DEFINE_int32(port, 8888, "TCP Port of this server (RPC + HTTP management API). "
                         "See -srv_extra_ports for additional RPC-only ports.");
// 一个进程充当 N 个同构 ServerC: brpc 硬约束是"1 个 Server 对象只监听 1 个端口"
// (src/brpc/server.cpp 的状态机拒绝二次 Start, docs/cn/server.md:230-232 亦明文),
// 所以多端口 = 多个 brpc::Server 对象。所有端口共享本进程的全部 srv_* 业务参数。
DEFINE_string(srv_extra_ports, "",
              "Additional RPC-only listening ports, comma-separated (e.g. '8010,8011'). "
              "Each is served by its own brpc::Server sharing this process's PerfTestService "
              "and all srv_* business flags, i.e. one process acts as N identical ServerC "
              "backends. These ports do NOT expose the HTTP management API (/ub_test/* stays "
              "on -port only); brpc builtins (/vars /status /connections) exist on every port. "
              "Duplicates, a value equal to -port, and port 0 (kernel-assigned) are rejected at "
              "startup. Empty (default) = single-port behavior, unchanged.");

// ==================== Test parameter flags (set from HTTP request) ====================
DEFINE_int32(thread_num, 1, "Total number of connections (replaces connections_per_server and max_connections_per_server)");
DEFINE_int32(queue_depth, 1, "Max in-flight requests across all connections (process-wide)");
DEFINE_int32(expected_qps, 0, "The expected QPS");
DEFINE_int64(initial_tokens, 10, "The initial number of tokens (smaller value = smoother start, less initial burst)");
DEFINE_int32(max_thread_num, 16, "The max number of threads are used");

DEFINE_int32(batch_size, 0, "Batch size for parallel connection (0 means no limit)");
DEFINE_int32(batch_interval_ms, 0, "Interval between batches in milliseconds");
DEFINE_int32(thread_pool_size, 8, "Thread pool size for parallel execution");
DEFINE_bool(only_first_rpc, false, "Only test first RPC connection, skip performance test");
// 压测阶段已改为全部连接一次性激活 (中央发送泵模型), 以下两个 flag 仅保留 CLI 兼容, 不再生效。
// 建链阶段的分批由 batch_size/batch_interval_ms 控制。
DEFINE_int32(test_connections_per_round, 0, "[deprecated] Number of connections to select per round");
DEFINE_int32(round_period_ms, 0, "[deprecated] Target period per round in milliseconds");

DEFINE_int32(attachment_size, 0, "Attachment size is used (in Bytes)");
DEFINE_bool(echo_attachment, false, "Select whether attachment should be echo");
DEFINE_string(connection_type, "single", "Connection type of the channel");
DEFINE_bool(use_urma, false, "Use the real URMA provider for this process");

static brpc::SocketMode ProcessSocketMode() {
    return FLAGS_use_urma ? brpc::SOCKET_MODE_URMA : brpc::SOCKET_MODE_TCP;
}
// 定义在 src/butil/endpoint.cpp。开启后同 ip:port 可被多个 listen socket 同时绑定
// (内核分发连接), 会让多端口校验形同虚设 —— 见 ValidateListenPorts。
DECLARE_bool(reuse_port);
DEFINE_int32(rpc_timeout_ms, 2000, "RPC call timeout");
DEFINE_int32(test_seconds, 20, "Test running time in seconds (0 means no duration limit, for pure iterations mode)");
DEFINE_int32(test_iterations, 0, "Total requests to complete in performance phase, process-wide (0 means disabled)");
DEFINE_int32(sender_thread_num, 0, "Number of sender pump bthreads (0 means auto: min(queue_depth, 16))");
DEFINE_int32(connect_timeout_ms, 2000, "connect timeout");
DEFINE_string(req_size, "", "Request name sizes in bytes, comma-separated for multiple sizes "
                             "(e.g. '128,1024,4096'). Requests pick sizes round-robin by global "
                             "send index. Empty means no name payload (bandwidth then counts attachment).");
DEFINE_bool(client_ignore_oc, false, "Client ignore eovercrowded, false by default");
DEFINE_int32(max_retry, 3, "Max retry times per RPC, mapped to ChannelOptions::max_retry. "
                             "Default: 3 (up to 3 retries after initial attempt, 4 total). ");
DEFINE_int32(connect_retry_interval, 200, "Retry backoff interval for first RPC (ms). "
                                          "Actual delay is randomized in [interval, 2*interval].");
DEFINE_int32(token_tick_us, 1000, "Token generation tick interval in microseconds. Smaller tick = smoother "
                                 "token distribution, avoiding bursty sends and tail-latency degradation.");
DEFINE_int32(token_interval_period, 10, "Token generation tick multiplier (default 10, sleep = token_tick_us * token_interval_period)");
DEFINE_int32(error_log_interval_ms, 1000, "Min interval between error logs (ms). Errors within a window "
                                          "are counted and reported with the next log to avoid log flooding "
                                          "when the server is abnormal. 0 = log every error");
DEFINE_bool(use_connection_group, false, "Set connection_group for each channel or not");
DEFINE_bool(test_keep_alive, false, "Keep connections alive for 10 seconds after establishment");
DEFINE_bool(debug_latency, false, "Print latency of each successful request");
DEFINE_int32(max_random_delay_us, 5000, "Max random delay in microseconds before sending request. "
                                        "Only used in rate-limited mode (expected_qps > 0 or ramp enabled); "
                                        "set 0 for accurate ramp tracking.");
DEFINE_bool(warmup, false, "Enable warmup connections before benchmark test to eliminate cold-start effects");
DEFINE_int32(warmup_connections, 100, "Number of connections to warm up (0 means same as thread_num)");
DEFINE_bool(allow_stop_burst, false,
            "Legacy end-of-test behavior: on stop, batch-post tokens and keep releasing inflight so "
            "pumps may send a final burst. Default false = drain in-flight only, no new send burst.");

// ==================== Server-side flags (from server.cpp) ====================
DEFINE_int32(srv_num_threads, 5, "The max number of server threads");
DEFINE_int32(srv_max_concurrency, 128, "max concurrency on server side");
DEFINE_int32(srv_bthread_concurrency, 5, "server bthread concurrency");
DEFINE_int64(srv_rsp_size, 0, "server response size");
DEFINE_bool(srv_ignore_oc, false, "Server ignore eovercrowded, false by default");
DEFINE_int32(srv_idle_timeout_seconds, 5, "Idle timeout in seconds: server prints final report when no new request arrives for this duration");
DEFINE_int32(srv_busy_us, 0, "Simulate business logic: per-request busy CPU computation time "
                              "in microseconds. Real CPU burn (no sleep). 0 = disabled.");
DEFINE_int32(srv_busy_yield_us, 0, "Periodic bthread_yield() during the srv_busy_us busy phase: yield the "
                                   "worker every N microseconds so other requests sharing the same worker "
                                   "can run. Models business that releases CPU periodically (real code "
                                   "releases the worker at downstream/IO suspends). 0 = never yield = pure "
                                   "CPU-bound handler holding its worker. Values below the ~10us clock-check "
                                   "granularity behave as 'yield at every check'.");
// ---- 长尾/重尾请求模拟 (少数请求天生很慢: 缓存击穿/下游抖动/GC/大 key) ----
// 服务端按到达序号确定性地等间隔命中一部分请求并注入额外延迟, 命中请求在响应里置 is_tail
// 标记, 客户端据此把它们从主时延统计中剥离、单独统计 (详见 README "长尾模拟说明")。
DEFINE_int32(srv_tail_permille, 0, "Simulate long-tail requests: permille (0-1000) of inbound requests "
             "selected by arrival sequence. Selection is deterministic and EVENLY SPACED (Bresenham): "
             "exactly floor(N*p/1000) of the first N requests hit, gap between hits is 1000/p (+-1). "
             "NOT `seq%1000<p`, which would cluster p hits together. 0 = disabled (zero hot-path cost).");
DEFINE_int32(srv_tail_delay_us, 0, "Long-tail extra delay in microseconds. Fixed value, or the lower bound "
             "when srv_tail_delay_max_us > this. 0 = hit but do not delay (validates selection/plumbing "
             "without perturbing latency). Keep it well below the client rpc_timeout_ms (see README).");
DEFINE_int32(srv_tail_delay_max_us, 0, "Upper bound of the long-tail delay (us). 0 = use the fixed value "
             "srv_tail_delay_us; when > srv_tail_delay_us the delay is drawn uniformly from "
             "[srv_tail_delay_us, srv_tail_delay_max_us].");
DEFINE_bool(srv_tail_cpu, false, "Long-tail delay type: false = bthread_usleep (suspends the bthread and "
             "RELEASES the worker, models downstream/IO wait); true = busy CPU spin (OCCUPIES the worker, "
             "models heavy computation; honours srv_busy_yield_us for periodic yields).");
// ---- Embedding 查询模拟 (AI 推理 embedding 检索场景) ----
DEFINE_bool(srv_embedding_enabled, false, "Simulate embedding lookup business logic: per-request "
                                          "random memory read/write on a large in-memory table "
                                          "(cache-unfriendly, memory-bandwidth sensitive). "
                                          "false = disabled, no memory allocated.");
DEFINE_int64(srv_embedding_rows, 100000, "Rows of the simulated embedding table. "
                                         "Default 100000 rows x 256 dims x 4B = 100MB.");
DEFINE_int32(srv_embedding_dims, 256, "Dimension (floats) of each embedding row.");
DEFINE_int32(srv_embedding_lookups, 8, "Random row lookups (dot-product reads) per request. 0 = read disabled.");
DEFINE_bool(srv_embedding_write, true, "Write back one random row after lookups per request "
                                       "(simulates table/cache update writes).");
// ---- 进程级背景 CPU 负载 (整体背景负载模拟) ----
DEFINE_int32(srv_bg_cpu_threads, 0, "Background CPU load threads, each burning one core at "
                                    "srv_bg_cpu_percent duty cycle. 0 = disabled. Simulates "
                                    "whole-process background load (co-located jobs), "
                                    "independent of request traffic. Total load ~= "
                                    "threads x percent%.");
DEFINE_int32(srv_bg_cpu_percent, 100, "Duty cycle percent per background thread (1-100). "
                                      "Each thread burns CPU for percent% of a 10ms period.");
DEFINE_bool(srv_bg_cpu_on_worker, false, "Run background CPU load on brpc worker pool (bthreads) "
                                        "instead of dedicated std::threads. true = the load occupies "
                                        "brpc workers during its busy phase, competing directly with "
                                        "request bthreads (simulates in-process background jobs eating "
                                        "worker capacity; raise srv_bthread_concurrency accordingly). "
                                        "false = dedicated OS threads (simulates co-located load).");
DEFINE_int32(srv_forward_times, 1, "Fan-out multiplier of proxy forwarding: each inbound request "
                                  "is forwarded this many times to consecutive round-robin "
                                  "backends (different backends when <= backend count, wraps "
                                  "otherwise). 1 = forward once (original behavior). "
                                  "Valid range [1, 100].");
DEFINE_bool(srv_forward_async, false, "Async fan-out: fire all srv_forward_times forwards "
                                     "concurrently (brpc::DoNothing + Join) instead of serial "
                                     "synchronous calls. false = serial (A-observed latency ~= "
                                     "sum of per-forward latencies); true = parallel "
                                     "(A-observed latency ~= max of per-forward latencies). "
                                     "No difference when srv_forward_times = 1.");

// ==================== Runtime test configuration ====================
// Holds per-test parameters that can be overridden via HTTP request body.
// Populated in ExecuteTest() from FLAGS defaults + JSON overrides.
// Functions read from this instead of mutating FLAGS_* directly.
struct TestConfig {
    int thread_num;
    int link_num;
    int queue_depth;
    int expected_qps;
    int64_t initial_tokens;
    int max_thread_num;
    int batch_size;
    int batch_interval_ms;
    int thread_pool_size;
    int test_connections_per_round;
    int round_period_ms;
    int attachment_size;
    bool echo_attachment;
    std::string connection_type;
    int rpc_timeout_ms;
    int test_seconds;
    int test_iterations;
    int sender_thread_num;
    int connect_timeout_ms;
    std::string req_size;
    bool client_ignore_oc;
    int max_retry;
    int connect_retry_interval;
    int token_tick_us;
    int token_interval_period;
    int error_log_interval_ms;
    bool use_connection_group;
    int max_random_delay_us;
    bool only_first_rpc;
    bool debug_latency;
    bool test_keep_alive;
    int latency_window_seconds;
    bool warmup;
    int warmup_connections;
    bool allow_stop_burst;
    // ---- QPS 阶梯爬坡参数 (组装为 RampPlan, 见 BuildRampPlan) ----
    // 目标 QPS 按周期逐级抬升, 取代固定 expected_qps; 到顶后保持该 QPS 直到
    // test_seconds 结束 (不新增停止条件), 此时 expected_qps 被忽略 (打 WARNING)。
    // 两种描述形式: qps_ramp_list 非空时优先 (显式列出每级, 如 "100,200,400"),
    // 否则用算术式 qps_ramp_start/step/max (start, start+step, ... 直至 max)。
    // 无命令行 flag, 只能经 POST /ub_test/start 下发 (同 link_num); 默认值见 ResetFromFlags()。
    std::string qps_ramp_list;
    int qps_ramp_start;
    int qps_ramp_step;
    int qps_ramp_step_seconds;
    int qps_ramp_max;

    void ResetFromFlags() {
        thread_num = FLAGS_thread_num;
        link_num = -1;
        queue_depth = FLAGS_queue_depth;
        expected_qps = FLAGS_expected_qps;
        initial_tokens = FLAGS_initial_tokens;
        max_thread_num = FLAGS_max_thread_num;
        batch_size = FLAGS_batch_size;
        batch_interval_ms = FLAGS_batch_interval_ms;
        thread_pool_size = FLAGS_thread_pool_size;
        test_connections_per_round = FLAGS_test_connections_per_round;
        round_period_ms = FLAGS_round_period_ms;
        attachment_size = FLAGS_attachment_size;
        echo_attachment = FLAGS_echo_attachment;
        connection_type = FLAGS_connection_type;
        rpc_timeout_ms = FLAGS_rpc_timeout_ms;
        test_seconds = FLAGS_test_seconds;
        test_iterations = FLAGS_test_iterations;
        sender_thread_num = FLAGS_sender_thread_num;
        connect_timeout_ms = FLAGS_connect_timeout_ms;
        req_size = FLAGS_req_size;
        client_ignore_oc = FLAGS_client_ignore_oc;
        max_retry = FLAGS_max_retry;
        connect_retry_interval = FLAGS_connect_retry_interval;
        token_tick_us = FLAGS_token_tick_us;
        token_interval_period = FLAGS_token_interval_period;
        error_log_interval_ms = FLAGS_error_log_interval_ms;
        use_connection_group = FLAGS_use_connection_group;
        max_random_delay_us = FLAGS_max_random_delay_us;
        only_first_rpc = FLAGS_only_first_rpc;
        debug_latency = FLAGS_debug_latency;
        test_keep_alive = FLAGS_test_keep_alive;
        latency_window_seconds = 0;
        warmup = FLAGS_warmup;
        warmup_connections = FLAGS_warmup_connections;
        allow_stop_burst = FLAGS_allow_stop_burst;
        // 阶梯爬坡参数无命令行 flag, 只能经 /ub_test/start 下发; 这里给硬编码默认值,
        // 保证请求未携带时字段不会残留上一轮的值。
        qps_ramp_list.clear();
        qps_ramp_start = 0;
        qps_ramp_step = 0;
        qps_ramp_step_seconds = 10;
        qps_ramp_max = 0;
    }
};

TestConfig g_test_config;

inline int GetConnectionCount() {
    return g_test_config.link_num >= 0 ? g_test_config.link_num : g_test_config.thread_num;
}

// ==================== QPS 阶梯爬坡 (staircase ramp) ====================
// 目标 QPS 按周期逐级抬升 (阶梯模型), 取代固定 expected_qps。设计要点:
// 1) 积分式发牌: 令牌线程按 "到时刻 t 应累计发出的令牌数" due(t) = Σ 已结束级 qps_i×secs_i
//    + qps_cur×(t − 当前级起点) 补差额, 而不是按增量补。due(t) 在级边界连续 (只有斜率
//    跳变), 所以级切换瞬间不会产生令牌突发。
// 2) 每级独立统计: 请求按 "发送时刻所属级" 归属, 因此 Σ steps[].requests == total_requests
//    (每个发出的请求恰好产生一次 HandleResponse, 含失败/超时)。
// 3) 到顶 (最后一级) 后保持该 QPS 直到停止条件满足, 不新增停止条件。
// 阶梯启用时 expected_qps 被忽略 (见 BuildRampPlan)。

static const int kMaxRampSteps = 256;      // 级数上限 (内存 ~2.1KB/级, 见 RampStepStats)
static const int64_t kMaxRampQps = 100000000;  // 单级 QPS 上限 (防误配, 令牌桶本身也不适用更高)
// 单 tick 补发令牌的兜底上限: 正常配置 (1e8 QPS × 10ms = 1e6) 永远打不到,
// 只在进程被挂起/暂停后 due(t) 跳变时触发, 防 bthread_sem 的 unsigned 计数回绕。
static const int64_t kMaxTokensPerTick = 10000000;

// ---- 目标 QPS 计划 (start() 一次性构建, 压测期只读) ----
struct RampPlan {
    bool enabled;
    bool from_list;                     // true = qps_ramp_list, false = 算术式
    std::vector<int64_t> qps;           // 每级目标 QPS
    std::vector<int64_t> step_end_ns;   // 每级计划结束时刻 (相对令牌线程起点); 末级 = INT64_MAX
    std::vector<int64_t> prefix_tokens; // prefix_tokens[i] = 第 i 级起点前的累计应发令牌, size = n+1
    int64_t secs_per_step;              // 每级计划时长 (秒)
    int64_t max_target_qps;             // 计划中的最高级 (日志/JSON 展示)
    int64_t reach_max_s;                // 爬到最高级所需秒数 = (n-1) × secs_per_step

    RampPlan() { Clear(); }
    void Clear() {
        enabled = false;
        from_list = false;
        qps.clear();
        step_end_ns.clear();
        prefix_tokens.clear();
        secs_per_step = 0;
        max_target_qps = 0;
        reach_max_s = 0;
    }
    int num_steps() const { return static_cast<int>(qps.size()); }
};

RampPlan g_ramp;
// 当前级索引: 令牌线程写 (release), 发送/响应线程读 (acquire)
butil::atomic<int> g_cur_step(0);

// 是否处于"限速发送"模式: 固定 expected_qps 或阶梯 ramp 任一启用。
// 单点判定, 供令牌线程/发送泵/停止逻辑统一使用 (避免散落的 expected_qps > 0 判定漏改)。
inline bool IsQpsLimited() {
    return g_test_config.expected_qps > 0 || g_ramp.enabled;
}

// ---- 每级延迟直方图 (无锁对数分桶) ----
// 每级 256 个桶, 桶 i 上界 = ceil(2^((i+1)/8)) us, 覆盖 [1, 2^32] us ≈ 71 分钟。
// 每桶相对宽度 2^(1/8)-1 ≈ 9.05%, 分位取桶上界 (恒 ≥ 真值, 相对误差 ≤ +9.05%);
// avg/min/max 精确。内存 ~2.1KB/级, 与 QPS/级时长/连接数无关。
// 不用 bvar::LatencyRecorder 的原因: 其分位只读已闭合的整秒快照 (短级次失真), 且内存随
// 级数×级时长线性增长、TLS agent 随记录线程数放大。
static const int kRampLatBuckets = 256;

struct RampLatBuckets {
    int64_t upper[kRampLatBuckets];
    RampLatBuckets() {
        for (int i = 0; i < kRampLatBuckets; ++i) {
            upper[i] = static_cast<int64_t>(std::ceil(std::pow(2.0, (i + 1) / 8.0)));
        }
        upper[0] = 1;   // <=1us 归入 0 号桶
    }
};
static const RampLatBuckets g_ramp_lat_buckets;

struct RampStepStats {
    int64_t target_qps;                 // 该级目标 QPS (计划值)
    int64_t planned_seconds;            // 该级计划时长
    bool hold;                          // 末级: 到顶后保持
    uint64_t start_us;                  // 该级实际起点 (gettimeofday_us)
    butil::atomic<uint64_t> end_us{0};  // 该级终点; 0 = 尚未收尾
    butil::atomic<bool> closed{false};  // 收尾幂等标记
    butil::atomic<uint64_t> requests{0};
    butil::atomic<uint64_t> errors{0};
    butil::atomic<uint64_t> tail_requests{0};   // 长尾请求 (不计入本级延迟直方图)
    butil::atomic<uint64_t> lat_count{0};
    butil::atomic<int64_t> lat_sum{0};
    butil::atomic<int64_t> lat_min{INT64_MAX};
    butil::atomic<int64_t> lat_max{0};
    butil::atomic<uint64_t> buckets[kRampLatBuckets];

    RampStepStats()
        : target_qps(0), planned_seconds(0), hold(false), start_us(0) {
        for (int i = 0; i < kRampLatBuckets; ++i) {
            buckets[i].store(0, butil::memory_order_relaxed);
        }
    }
};

// 预分配的全部级统计 (启动线程前一次性分配, join 之后释放; 运行期只读集合本身)
std::vector<RampStepStats*> g_step_stats;

static void FreeRampStepStatsIfAny() {
    for (size_t i = 0; i < g_step_stats.size(); ++i) {
        delete g_step_stats[i];
    }
    g_step_stats.clear();
}

static void AllocRampStepStats() {
    FreeRampStepStatsIfAny();   // 防御: 上一轮异常残留
    const int n = g_ramp.num_steps();
    g_step_stats.reserve(n);
    for (int i = 0; i < n; ++i) {
        RampStepStats* s = new RampStepStats;
        s->target_qps = g_ramp.qps[i];
        s->planned_seconds = g_ramp.secs_per_step;
        s->hold = (i == n - 1);
        g_step_stats.push_back(s);
    }
}

static inline int LatBucketOf(int64_t v) {
    if (v <= 0) return 0;
    int lo = 0, hi = kRampLatBuckets - 1;
    while (lo < hi) {
        const int mid = (lo + hi) >> 1;
        if (g_ramp_lat_buckets.upper[mid] >= v) hi = mid; else lo = mid + 1;
    }
    return lo;
}

// 热路径打点 (brpc 响应线程): 全部为 relaxed 原子操作, 无锁无分配
static inline void RampRecordLatency(RampStepStats* s, int64_t lat_us) {
    if (s == NULL) return;
    s->lat_count.fetch_add(1, butil::memory_order_relaxed);
    s->lat_sum.fetch_add(lat_us, butil::memory_order_relaxed);
    int64_t cur = s->lat_min.load(butil::memory_order_relaxed);
    while (lat_us < cur && !s->lat_min.compare_exchange_weak(cur, lat_us)) {}
    cur = s->lat_max.load(butil::memory_order_relaxed);
    while (lat_us > cur && !s->lat_max.compare_exchange_weak(cur, lat_us)) {}
    s->buckets[LatBucketOf(lat_us)].fetch_add(1, butil::memory_order_relaxed);
}

// 分位: 取第一个累计计数超过 rank 的桶的上界
static int64_t HistPercentile(const RampStepStats& st, double ratio) {
    const uint64_t n = st.lat_count.load(butil::memory_order_relaxed);
    if (n == 0) return 0;
    uint64_t rank = static_cast<uint64_t>(ratio * n);
    if (rank >= n) rank = n - 1;
    uint64_t cum = 0;
    for (int i = 0; i < kRampLatBuckets; ++i) {
        const uint64_t c = st.buckets[i].load(butil::memory_order_relaxed);
        if (c == 0) continue;
        if (cum + c > rank) return g_ramp_lat_buckets.upper[i];
        cum += c;
    }
    return g_ramp_lat_buckets.upper[kRampLatBuckets - 1];
}

// ---- 每级统计结果 (实时行与最终 JSON 同源, 避免两套口径) ----
struct RampStepSummary {
    int index;
    int64_t target_qps;
    int64_t planned_seconds;
    bool hold;
    double duration_s;
    double actual_qps;
    uint64_t requests;
    uint64_t errors;
    uint64_t tail_requests;   // 其中长尾请求数 (不进本级延迟直方图)
    double error_rate;
    int64_t lat_avg, lat_min, lat_max;
    int64_t lat_p50, lat_p90, lat_p99, lat_p999, lat_p9999;

    RampStepSummary()
        : index(0), target_qps(0), planned_seconds(0), hold(false), duration_s(0),
          actual_qps(0), requests(0), errors(0), tail_requests(0), error_rate(0), lat_avg(0),
          lat_min(0), lat_max(0), lat_p50(0), lat_p90(0), lat_p99(0), lat_p999(0),
          lat_p9999(0) {}
};

// now_us: 该级尚未收尾时用 (此刻 − 起点) 作为时长
static RampStepSummary ComputeStepSummary(const RampStepStats& st, int index, uint64_t now_us) {
    RampStepSummary s;
    s.index = index;
    s.target_qps = st.target_qps;
    s.planned_seconds = st.planned_seconds;
    s.hold = st.hold;
    const uint64_t end_us = st.end_us.load(butil::memory_order_relaxed);
    const uint64_t stop_us = (end_us != 0) ? end_us : now_us;
    s.duration_s = (stop_us > st.start_us) ? (stop_us - st.start_us) / 1000000.0 : 0.0;
    s.requests = st.requests.load(butil::memory_order_relaxed);
    s.errors = st.errors.load(butil::memory_order_relaxed);
    s.tail_requests = st.tail_requests.load(butil::memory_order_relaxed);
    s.error_rate = (s.requests > 0) ? s.errors * 100.0 / s.requests : 0.0;
    s.actual_qps = (s.duration_s > 0) ? s.requests / s.duration_s : 0.0;
    const uint64_t lat_cnt = st.lat_count.load(butil::memory_order_relaxed);
    if (lat_cnt > 0) {
        s.lat_avg = st.lat_sum.load(butil::memory_order_relaxed)
                    / static_cast<int64_t>(lat_cnt);
        s.lat_min = st.lat_min.load(butil::memory_order_relaxed);
        if (s.lat_min == INT64_MAX) s.lat_min = 0;
        s.lat_max = st.lat_max.load(butil::memory_order_relaxed);
        s.lat_p50 = HistPercentile(st, 0.5);
        s.lat_p90 = HistPercentile(st, 0.9);
        s.lat_p99 = HistPercentile(st, 0.99);
        s.lat_p999 = HistPercentile(st, 0.999);
        s.lat_p9999 = HistPercentile(st, 0.9999);
        // 分位取桶上界 (≤ 真值 +9%), 而 min/max 是精确值 —— 用 max 封顶更准 (真分位恒 <= max),
        // 同时保证 min <= p50 <= ... <= p9999 <= max 的自洽性, 便于与其它工具对比。
        s.lat_p50 = std::min(std::max(s.lat_p50, s.lat_min), s.lat_max);
        s.lat_p90 = std::min(std::max(s.lat_p90, s.lat_min), s.lat_max);
        s.lat_p99 = std::min(std::max(s.lat_p99, s.lat_min), s.lat_max);
        s.lat_p999 = std::min(std::max(s.lat_p999, s.lat_min), s.lat_max);
        s.lat_p9999 = std::min(std::max(s.lat_p9999, s.lat_min), s.lat_max);
    }
    return s;
}

static void PrintRampStepLine(const RampStepSummary& s, int total_steps) {
    std::cout << "[Ramp] step " << (s.index + 1) << "/" << total_steps
              << " done | target=" << s.target_qps << " qps"
              << " | actual=" << s.actual_qps << " qps"
              << " | duration=" << s.duration_s << "s (planned " << s.planned_seconds << "s)"
              << (s.hold ? " (hold)" : "")
              << " | requests=" << s.requests << " errors=" << s.errors
              << " (" << s.error_rate << "%)"
              << " | latency(avg/p50/p90/p99/p999/p9999)="
              << s.lat_avg << "/" << s.lat_p50 << "/" << s.lat_p90 << "/" << s.lat_p99
              << "/" << s.lat_p999 << "/" << s.lat_p9999 << "us"
              << std::endl;
}

// 收尾一级: 记终点 + 打印实时行 (幂等)。这里只是即时快照 —— 边界之后仍可能有归属于
// 该级的在途响应回来, 最终 JSON 由收尾后的重算给出。
static void CloseRampStep(int step, uint64_t now_us) {
    if (step < 0 || step >= static_cast<int>(g_step_stats.size())) return;
    RampStepStats* st = g_step_stats[step];
    bool expected = false;
    if (!st->closed.compare_exchange_strong(expected, true, butil::memory_order_acq_rel)) {
        return;   // 已收尾
    }
    st->end_us.store(now_us, butil::memory_order_release);
    PrintRampStepLine(ComputeStepSummary(*st, step, now_us), g_ramp.num_steps());
}

// 级推进 (令牌线程唯一调用方): 收尾所有已过期的级, 跨多级时逐级收尾不漏行。
// 顺序: 先收尾旧级 -> 标新级起点 -> 发布新级索引, 保证发布之后发出的请求归属新级。
static int AdvanceRampStep(int cur_step, int64_t elapsed_ns) {
    const int n = g_ramp.num_steps();
    while (cur_step + 1 < n && elapsed_ns >= g_ramp.step_end_ns[cur_step]) {
        CloseRampStep(cur_step, butil::gettimeofday_us());
        ++cur_step;
        if (cur_step < static_cast<int>(g_step_stats.size())) {
            g_step_stats[cur_step]->start_us = butil::gettimeofday_us();
        }
        g_cur_step.store(cur_step, butil::memory_order_release);
    }
    return cur_step;
}

// 阶梯模式下 "到时刻 elapsed_ns 应累计发出的令牌数" (积分式, 级边界连续)。
// 用 double 做乘法: qps(<=1e8) × elapsed(可达 1e12+ ns) 会溢出 int64。
static inline int64_t RampDueTokens(int step, int64_t elapsed_ns) {
    const int64_t begin_ns = (step == 0) ? 0 : g_ramp.step_end_ns[step - 1];
    const double secs = static_cast<double>(elapsed_ns - begin_ns) / 1000000000.0;
    return g_ramp.prefix_tokens[step] + static_cast<int64_t>(g_ramp.qps[step] * secs);
}

// ---- 每级统计的最终汇总 (TaskState 持有; 不要放进 TaskResult, 其 Reset() 是 memset) ----
struct RampReport {
    bool enabled;
    bool from_list;
    int total_steps;        // 计划级数
    int reported_steps;     // 实际跑到的级数 (truncated 时 < total_steps)
    bool truncated;         // 未跑完计划的所有级
    int64_t max_target_qps;
    int64_t secs_per_step;
    std::vector<RampStepSummary> steps;

    RampReport() { Clear(); }
    void Clear() {
        enabled = false;
        from_list = false;
        total_steps = 0;
        reported_steps = 0;
        truncated = false;
        max_target_qps = 0;
        secs_per_step = 0;
        steps.clear();
    }
};

// 解析逗号分隔的阶梯 QPS 列表 (分词/去空白/报错风格同 ParseReqSizes)。
// 返回 0 成功, -1 失败 (原因写入 err)。
static int ParseRampList(const std::string& text, std::vector<int64_t>* out, std::string* err) {
    out->clear();
    std::string::size_type pos1 = 0;
    while (true) {
        std::string::size_type pos2 = text.find(',', pos1);
        std::string token = text.substr(
            pos1, pos2 == std::string::npos ? std::string::npos : pos2 - pos1);
        token.erase(0, token.find_first_not_of(" \t"));
        token.erase(token.find_last_not_of(" \t") + 1);
        if (token.empty()) {
            *err = "empty entry in qps_ramp_list: " + text;
            return -1;
        }
        char* end = NULL;
        const long v = strtol(token.c_str(), &end, 10);
        if (end == token.c_str() || *end != '\0' || v <= 0) {
            *err = "invalid QPS in qps_ramp_list: " + token + " (must be a positive integer)";
            return -1;
        }
        if (v > kMaxRampQps) {
            *err = "QPS in qps_ramp_list exceeds limit " + std::to_string(kMaxRampQps) + ": " + token;
            return -1;
        }
        out->push_back(static_cast<int64_t>(v));
        if (pos2 == std::string::npos) break;
        pos1 = pos2 + 1;
    }
    if (out->empty()) {
        *err = "empty qps_ramp_list";
        return -1;
    }
    return 0;
}

// 组装阶梯计划 (start() 调用, 失败返回 -1 并写 err -> HTTP 400)。
// 规则: list 非空 -> list 模式 (start>0 时 WARNING list 优先); 否则 start>0 -> 算术模式;
// 两者皆空 -> ramp 不启用。step_seconds 钳制 [1,3600]; step<=0 或 0<max<=start 退化为单级;
// max=0 时级数由 test_seconds 推导; 级数 > kMaxRampSteps 直接失败 (不静默截断)。
static int BuildRampPlan(std::string* err) {
    g_ramp.Clear();
    const std::string& list = g_test_config.qps_ramp_list;
    if (list.empty() && g_test_config.qps_ramp_start <= 0) {
        if (g_test_config.qps_ramp_step > 0 || g_test_config.qps_ramp_max > 0) {
            LOG(WARNING) << "qps_ramp_step/qps_ramp_max set without qps_ramp_start or "
                            "qps_ramp_list, ramp disabled";
        }
        return 0;
    }
    if (g_test_config.only_first_rpc) {
        LOG(WARNING) << "ramp has no effect in only_first_rpc mode, ramp disabled";
        g_ramp.Clear();
        return 0;
    }

    int64_t secs = g_test_config.qps_ramp_step_seconds;
    if (secs < 1 || secs > 3600) {
        LOG(WARNING) << "qps_ramp_step_seconds=" << secs << " out of [1, 3600], clamped to "
                     << std::min(std::max(secs, static_cast<int64_t>(1)), static_cast<int64_t>(3600));
        secs = std::min(std::max(secs, static_cast<int64_t>(1)), static_cast<int64_t>(3600));
    }
    g_ramp.secs_per_step = secs;

    if (!list.empty()) {
        g_ramp.from_list = true;
        if (g_test_config.qps_ramp_start > 0) {
            LOG(WARNING) << "qps_ramp_list set, takes precedence over qps_ramp_start="
                         << g_test_config.qps_ramp_start;
        }
        std::string perr;
        if (ParseRampList(list, &g_ramp.qps, &perr) != 0) {
            *err = perr;
            return -1;
        }
        if (g_ramp.num_steps() > kMaxRampSteps) {
            *err = "qps_ramp_list has " + std::to_string(g_ramp.num_steps())
                   + " steps, exceeds limit " + std::to_string(kMaxRampSteps);
            return -1;
        }
    } else {
        const int64_t start = g_test_config.qps_ramp_start;
        const int64_t step = g_test_config.qps_ramp_step;
        const int64_t max_qps = g_test_config.qps_ramp_max;
        if (start > kMaxRampQps) {
            *err = "qps_ramp_start exceeds limit " + std::to_string(kMaxRampQps);
            return -1;
        }
        if (step <= 0 || (max_qps > 0 && max_qps <= start)) {
            LOG(WARNING) << "qps_ramp_step=" << step << " qps_ramp_max=" << max_qps
                         << " -> degenerates to a single step (" << start
                         << " QPS), equivalent to constant rate limiting";
            g_ramp.qps.push_back(start);
        } else if (max_qps > 0) {
            for (int64_t q = start; q <= max_qps; q += step) {
                if (g_ramp.num_steps() >= kMaxRampSteps) {
                    *err = "ramp step count exceeds limit " + std::to_string(kMaxRampSteps)
                           + " (increase qps_ramp_step or decrease qps_ramp_max)";
                    return -1;
                }
                g_ramp.qps.push_back(q);
            }
        } else {
            // 无上限: 级数由 test_seconds 推导 (每 secs 秒一级, 末级保持)
            if (g_test_config.test_seconds <= 0) {
                *err = "qps_ramp_max=0 requires test_seconds > 0 to derive the step count";
                return -1;
            }
            const int64_t n = (g_test_config.test_seconds + secs - 1) / secs;
            if (n > kMaxRampSteps) {
                *err = "derived step count " + std::to_string(n) + " exceeds limit "
                       + std::to_string(kMaxRampSteps) + " (increase qps_ramp_step_seconds)";
                return -1;
            }
            for (int64_t i = 0; i < n; ++i) {
                const int64_t q = start + i * step;
                if (q > kMaxRampQps) {
                    *err = "derived QPS " + std::to_string(q) + " exceeds limit "
                           + std::to_string(kMaxRampQps);
                    return -1;
                }
                g_ramp.qps.push_back(q);
            }
        }
    }

    const int n = g_ramp.num_steps();
    g_ramp.step_end_ns.resize(n);
    g_ramp.prefix_tokens.resize(n + 1);
    g_ramp.prefix_tokens[0] = 0;
    for (int i = 0; i < n; ++i) {
        g_ramp.max_target_qps = std::max(g_ramp.max_target_qps, g_ramp.qps[i]);
        // 末级不设结束时刻 = 到顶后保持, 直到停止条件满足
        g_ramp.step_end_ns[i] = (i == n - 1)
            ? INT64_MAX
            : static_cast<int64_t>(i + 1) * secs * 1000000000LL;
        g_ramp.prefix_tokens[i + 1] = g_ramp.prefix_tokens[i] + g_ramp.qps[i] * secs;
    }
    g_ramp.reach_max_s = static_cast<int64_t>(n - 1) * secs;
    g_ramp.enabled = true;

    if (g_test_config.expected_qps > 0) {
        LOG(WARNING) << "ramp enabled, expected_qps=" << g_test_config.expected_qps
                     << " is ignored (ramp defines the rate curve)";
        g_test_config.expected_qps = 0;   // 物理关闭固定限速路径, 防遗漏点误用
    }
    if (g_test_config.test_seconds > 0 && g_test_config.test_seconds < g_ramp.reach_max_s) {
        LOG(WARNING) << "ramp needs " << g_ramp.reach_max_s << "s to reach "
                     << g_ramp.max_target_qps << " QPS, test_seconds="
                     << g_test_config.test_seconds << " will truncate it at step "
                     << (g_test_config.test_seconds / secs + 1) << "/" << n;
    }
    if (g_test_config.max_random_delay_us > 0) {
        // 每个泵拿到令牌后还会随机睡 [0, max_random_delay_us], 期间不发送 ——
        // 高阶梯的 actual_qps 会被这个延迟压住, 看起来像"服务端饱和", 实则被客户端节流。
        LOG(WARNING) << "ramp enabled with max_random_delay_us="
                     << g_test_config.max_random_delay_us
                     << "us: per-request random delay can cap the achievable QPS and flatten "
                        "high ramp steps; set max_random_delay_us=0 for accurate ramp tracking";
    }
    LOG(INFO) << "Ramp plan built: mode=" << (g_ramp.from_list ? "list" : "arithmetic")
              << " steps=" << n << " step_seconds=" << secs
              << " max_target_qps=" << g_ramp.max_target_qps
              << " reach_max_s=" << g_ramp.reach_max_s;
    return 0;
}

// ==================== Task state ====================
enum TaskStatus {
    TASK_IDLE = 0,
    TASK_RUNNING = 1,
    TASK_COMPLETED = 2,
    TASK_FAILED = 3
};

struct TaskResult {
    // Timing
    double duration_s;
    // Request counts
    uint64_t total_requests;
    uint64_t total_errors;
    uint64_t total_sent;
    double error_rate;
    double qps;
    double throughput_mb_s;
    // Latency (us)
    int64_t latency_avg;
    int64_t latency_min;
    int64_t latency_max;
    int64_t latency_p50;
    int64_t latency_p90;
    int64_t latency_p99;
    int64_t latency_p999;
    int64_t latency_p9999;
    // Server CPU
    double server_cpu_avg;
    double server_cpu_max;
    // Client CPU
    double client_cpu_avg;
    double client_cpu_max;
    // Client memory (MB)
    double client_memory_avg_mb;
    double client_memory_max_mb;
    // Connect latency
    int64_t connect_avg;
    int64_t connect_min;
    int64_t connect_max;
    int64_t connect_p50;
    int64_t connect_p90;
    int64_t connect_p99;
    int64_t connect_p999;
    int64_t connect_p9999;
    // First RPC latency
    int64_t first_rpc_avg;
    int64_t first_rpc_min;
    int64_t first_rpc_max;
    int64_t first_rpc_p50;
    int64_t first_rpc_p90;
    int64_t first_rpc_p99;
    int64_t first_rpc_p999;
    int64_t first_rpc_p9999;
    // First RPC memory
    double first_rpc_memory_avg_mb;
    double first_rpc_memory_min_mb;
    double first_rpc_memory_max_mb;
    // Rounds
    int total_rounds;
    // Connection establishment
    int expect_conn_count;
    int established_conn_count;
    int64_t establish_conn_time;
    // 长尾请求 (服务端 srv_tail_* 注入): 与上面的 latency_* 完全独立的口径。
    // 注意: 本结构体的 Reset() 是 memset(this,0,sizeof) —— 只能加 POD 成员!
    uint64_t tail_requests;
    int64_t tail_latency_avg;
    int64_t tail_latency_min;
    int64_t tail_latency_max;
    int64_t tail_latency_p50;
    int64_t tail_latency_p90;
    int64_t tail_latency_p99;
    int64_t tail_latency_p999;
    int64_t tail_latency_p9999;

    void Reset() {
        memset(this, 0, sizeof(*this));
        latency_min = INT64_MAX;
        connect_min = INT64_MAX;
        first_rpc_min = INT64_MAX;
        tail_latency_min = INT64_MAX;
    }
};

struct TaskState {
    std::string task_id;
    TaskStatus status;
    std::string error_message;
    TaskResult result;
    // 阶梯统计放在 TaskState 而非 TaskResult: TaskResult::Reset() 是 memset(this,0,sizeof),
    // 放 STL 成员会直接堆损坏。
    RampReport ramp;
    uint64_t started_at_us;
    // Temporary storage for request parameters (JSON body)
    std::string request_body;

    void Reset() {
        task_id.clear();
        status = TASK_IDLE;
        error_message.clear();
        result.Reset();
        ramp.Clear();
        started_at_us = 0;
        request_body.clear();
    }
};

pthread_mutex_t g_task_mutex = PTHREAD_MUTEX_INITIALIZER;
TaskState g_task;
int g_task_seq = 0;

// ==================== JSON helpers ====================

static void JsonEscape(std::string* out, const std::string& str) {
    for (size_t i = 0; i < str.size(); ++i) {
        char c = str[i];
        switch (c) {
            case '"':  out->append("\\\""); break;
            case '\\': out->append("\\\\"); break;
            case '\n': out->append("\\n"); break;
            case '\r': out->append("\\r"); break;
            case '\t': out->append("\\t"); break;
            default:   out->push_back(c); break;
        }
    }
}

static void AppendJsonKV(std::string* out, const char* key, const std::string& val, bool comma) {
    out->push_back('"'); out->append(key); out->append("\":\"");
    JsonEscape(out, val);
    out->push_back('"');
    if (comma) out->push_back(',');
}

static void AppendJsonKV(std::string* out, const char* key, int64_t val, bool comma) {
    out->push_back('"'); out->append(key); out->append("\":");
    out->append(std::to_string(val));
    if (comma) out->push_back(',');
}

static void AppendJsonKV(std::string* out, const char* key, double val, bool comma) {
    char buf[64];
    snprintf(buf, sizeof(buf), "%.6f", val);
    out->push_back('"'); out->append(key); out->append("\":");
    out->append(buf);
    if (comma) out->push_back(',');
}

static void AppendJsonKV(std::string* out, const char* key, bool val, bool comma) {
    out->push_back('"'); out->append(key); out->append("\":");
    out->append(val ? "true" : "false");
    if (comma) out->push_back(',');
}

static void BuildLatencyJson(std::string* out, const char* prefix,
                              int64_t avg, int64_t min, int64_t max,
                              int64_t p50, int64_t p90, int64_t p99,
                              int64_t p999, int64_t p9999) {
    out->push_back('{');
    AppendJsonKV(out, "avg", avg, true);
    if (min != INT64_MAX) {
        AppendJsonKV(out, "min", min, true);
    } else {
        AppendJsonKV(out, "min", (int64_t)0, true);
    }
    AppendJsonKV(out, "max", max, true);
    AppendJsonKV(out, "p50", p50, true);
    AppendJsonKV(out, "p90", p90, true);
    AppendJsonKV(out, "p99", p99, true);
    AppendJsonKV(out, "p999", p999, true);
    AppendJsonKV(out, "p9999", p9999, false);
    out->push_back('}');
}

// 单级 JSON (字段与实时行同源于 RampStepSummary)
// with_tail: 仅当整轮观测到长尾时才输出 tail_requests, 保证未启用长尾时 ramp JSON 逐字节不变。
// 不变量: lat_count == requests - errors - tail_requests (长尾不进本级延迟直方图)。
static void BuildRampStepJson(std::string* out, const RampStepSummary& s, bool with_tail) {
    out->push_back('{');
    AppendJsonKV(out, "index", (int64_t)s.index, true);
    AppendJsonKV(out, "target_qps", s.target_qps, true);
    AppendJsonKV(out, "planned_seconds", s.planned_seconds, true);
    AppendJsonKV(out, "duration_s", s.duration_s, true);
    AppendJsonKV(out, "actual_qps", s.actual_qps, true);
    AppendJsonKV(out, "requests", (int64_t)s.requests, true);
    AppendJsonKV(out, "errors", (int64_t)s.errors, true);
    if (with_tail) {
        AppendJsonKV(out, "tail_requests", (int64_t)s.tail_requests, true);
    }
    AppendJsonKV(out, "error_rate", s.error_rate, true);
    AppendJsonKV(out, "hold", s.hold, true);
    out->append("\"latency\":");
    BuildLatencyJson(out, "", s.lat_avg, s.lat_min, s.lat_max,
                     s.lat_p50, s.lat_p90, s.lat_p99, s.lat_p999, s.lat_p9999);
    out->push_back('}');
}

// ramp != NULL 且启用时追加 "ramp" 键; 未启用则完全不输出 (非 ramp 场景输出保持不变)
static std::string BuildResultJson(const TaskResult& r, const RampReport* ramp) {
    std::string out;
    out.reserve(4096);
    out.push_back('{');
    AppendJsonKV(&out, "duration_s", r.duration_s, true);
    AppendJsonKV(&out, "total_requests", (int64_t)r.total_requests, true);
    AppendJsonKV(&out, "total_errors", (int64_t)r.total_errors, true);
    AppendJsonKV(&out, "total_sent", (int64_t)r.total_sent, true);
    AppendJsonKV(&out, "error_rate", r.error_rate, true);
    AppendJsonKV(&out, "qps", r.qps, true);
    AppendJsonKV(&out, "throughput_mb_s", r.throughput_mb_s, true);

    // Latency
    out.append("\"latency\":");
    BuildLatencyJson(&out, "", r.latency_avg, r.latency_min, r.latency_max,
                     r.latency_p50, r.latency_p90, r.latency_p99,
                     r.latency_p999, r.latency_p9999);
    out.push_back(',');

    // Server CPU
    AppendJsonKV(&out, "server_cpu_avg", r.server_cpu_avg, true);
    AppendJsonKV(&out, "server_cpu_max", r.server_cpu_max, true);

    // Client CPU
    AppendJsonKV(&out, "client_cpu_avg", r.client_cpu_avg, true);
    AppendJsonKV(&out, "client_cpu_max", r.client_cpu_max, true);

    // Client memory
    AppendJsonKV(&out, "client_memory_avg_mb", r.client_memory_avg_mb, true);
    AppendJsonKV(&out, "client_memory_max_mb", r.client_memory_max_mb, true);

    // Connect latency
    out.append("\"channel_latency\":");
    BuildLatencyJson(&out, "", r.connect_avg, r.connect_min, r.connect_max,
                     r.connect_p50, r.connect_p90, r.connect_p99,
                     r.connect_p999, r.connect_p9999);
    out.push_back(',');

    // First RPC latency
    out.append("\"first_rpc_latency\":");
    BuildLatencyJson(&out, "", r.first_rpc_avg, r.first_rpc_min, r.first_rpc_max,
                     r.first_rpc_p50, r.first_rpc_p90, r.first_rpc_p99,
                     r.first_rpc_p999, r.first_rpc_p9999);
    out.push_back(',');

    // First RPC memory
    AppendJsonKV(&out, "first_rpc_memory_avg_mb", r.first_rpc_memory_avg_mb, true);
    AppendJsonKV(&out, "first_rpc_memory_min_mb", r.first_rpc_memory_min_mb, true);
    AppendJsonKV(&out, "first_rpc_memory_max_mb", r.first_rpc_memory_max_mb, true);

    // Connection establishment
    AppendJsonKV(&out, "expect_conn_count", (int64_t)r.expect_conn_count, true);
    AppendJsonKV(&out, "established_conn_count", (int64_t)r.established_conn_count, true);
    AppendJsonKV(&out, "establish_conn_time", r.establish_conn_time, true);

    // Rounds (末尾字段, 不带逗号: 逗号由下方可选块统一管理)
    AppendJsonKV(&out, "total_rounds", (int64_t)r.total_rounds, false);

    // 尾部可选块 (ramp / tail): 各自可能不出现, 逗号在这里统一拼接 ——
    // 每加一个可选块都要改前面字段的逗号是易错写法, 且两者都为空时会产出尾逗号(非法 JSON)。
    std::string blocks;
    // QPS 阶梯爬坡 (每级 target vs actual + 分位延迟)
    if (ramp != NULL && ramp->enabled) {
        if (!blocks.empty()) blocks.push_back(',');
        blocks.append("\"ramp\":{");
        AppendJsonKV(&blocks, "enabled", true, true);
        // 显式构造 std::string: 否则 const char* 会被重载决议到 bool 版本
        AppendJsonKV(&blocks, "mode", std::string(ramp->from_list ? "list" : "arithmetic"), true);
        AppendJsonKV(&blocks, "total_steps", (int64_t)ramp->total_steps, true);
        AppendJsonKV(&blocks, "reported_steps", (int64_t)ramp->reported_steps, true);
        AppendJsonKV(&blocks, "truncated", ramp->truncated, true);
        AppendJsonKV(&blocks, "max_target_qps", ramp->max_target_qps, true);
        AppendJsonKV(&blocks, "step_seconds", ramp->secs_per_step, true);
        blocks.append("\"steps\":[");
        for (size_t i = 0; i < ramp->steps.size(); ++i) {
            if (i) blocks.push_back(',');
            BuildRampStepJson(&blocks, ramp->steps[i], r.tail_requests > 0);
        }
        blocks.append("]}");
    }
    // 长尾 (仅当观测到长尾响应时才出现: 未启用长尾的场景输出逐字节不变)
    if (r.tail_requests > 0) {
        if (!blocks.empty()) blocks.push_back(',');
        AppendJsonKV(&blocks, "tail_requests", (int64_t)r.tail_requests, true);
        // 分母用成功请求数: 延迟只统计成功响应, 用总数会与 tail_latency 的口径不自洽
        const uint64_t ok = r.total_requests - r.total_errors;
        AppendJsonKV(&blocks, "tail_ratio",
                     ok > 0 ? r.tail_requests * 100.0 / ok : 0.0, true);
        blocks.append("\"tail_latency\":");
        BuildLatencyJson(&blocks, "", r.tail_latency_avg, r.tail_latency_min,
                         r.tail_latency_max, r.tail_latency_p50, r.tail_latency_p90,
                         r.tail_latency_p99, r.tail_latency_p999, r.tail_latency_p9999);
    }
    if (!blocks.empty()) {
        out.push_back(',');
        out += blocks;
    }

    out.push_back('}');
    return out;
}

// ==================== Global test stats (bvar::LatencyRecorder-based) ====================
// Use heap-allocated LatencyRecorder for bvar framework capabilities
// (reservoir-sampled percentiles, save_percentile_samples, /vars monitoring).
// Recreated per test run via ResetGlobalStats() with versioned exposure names.
bvar::LatencyRecorder* g_latency_recorder = nullptr;
bvar::LatencyRecorder* g_server_cpu_recorder = nullptr;
bvar::LatencyRecorder* g_client_cpu_recorder = nullptr;
bvar::LatencyRecorder* g_client_memory_recorder = nullptr;
butil::atomic<int64_t> g_latency_min{INT64_MAX};
butil::atomic<int64_t> g_latency_max{0};
// 长尾请求 (服务端 srv_tail_* 注入, 响应带 is_tail 标记) 的独立统计:
// 主 recorder / min / max 一律不含长尾, 保证"系统自身延迟"与"人为注入的长尾"互不污染。
bvar::LatencyRecorder* g_client_tail_recorder = nullptr;
butil::atomic<uint64_t> g_tail_req_cnt(0);
butil::atomic<int64_t> g_tail_lat_min{INT64_MAX};
butil::atomic<int64_t> g_tail_lat_max{0};
int g_run_index = 0;  // for versioned bvar exposure names

// Token bucket semaphore
bthread_sem_t g_token_sem;
// 进程级在途并发控制 (中央发送泵模型):
// queue_depth 是全局在途请求上限, K 个发送泵共享 g_inflight_sem 许可,
// 轮询所有活跃连接发送请求 (取代原"每连接 queue_depth 个在途"模型)。
class PerformanceTest;  // 前置声明, 供 g_active_tests 指针使用
bthread_sem_t g_inflight_sem;                      // 全局在途许可, 数量 = queue_depth
butil::atomic<bool> g_stop(false);
butil::atomic<uint64_t> g_pending_req_count(0);    // 全局在途请求数
butil::atomic<int> g_send_index(0);                // 压测阶段连接轮询索引
butil::atomic<int> g_pump_num(0);                  // 发送泵数量 (供停止唤醒使用)
std::vector<PerformanceTest*>* g_active_tests = NULL;  // 压测阶段活跃连接 (只读)
uint64_t g_perf_start_time = 0;                    // 压测阶段全局开始时间

// First-packet latencies
std::vector<int64_t> g_connect_latencies_us;
std::vector<int64_t> g_first_rpc_latencies_us;
std::vector<double> g_first_rpc_memory_mbs;
pthread_mutex_t g_latency_mutex = PTHREAD_MUTEX_INITIALIZER;

// Atomic counters (multi-thread safe)
butil::atomic<uint64_t> g_total_bytes(0);
butil::atomic<uint64_t> g_all_cnt(0);
butil::atomic<uint64_t> g_total_cnt(0);
butil::atomic<uint64_t> g_total_error_cnt(0);
butil::atomic<uint64_t> g_last_time(0);
butil::atomic<int64_t> g_channel_id(0);

// 错误日志节流：按时间窗口抽样打印（默认 1s 一条），窗口内其余错误仅计数，
// 随下一条日志输出，避免服务端异常时海量失败 RPC 打爆日志磁盘
butil::atomic<int64_t> g_last_error_log_time_us(0);
butil::atomic<uint64_t> g_suppressed_error_logs(0);

// Server list
std::vector<std::string> g_servers;

// Round-robin index
butil::atomic<int> rr_index(0);

// 请求 name 负载: 支持多个 size (req_size 逗号分隔), 请求按全局发送索引轮询平均分配
std::vector<int64_t> g_req_sizes;   // 解析后的 size 列表 (空 = 未设置 req_size)
std::vector<std::string> g_names;   // 与 g_req_sizes 一一对应的预生成负载

// ==================== Server-side globals ====================
pthread_mutex_t g_srv_metrics_lock = PTHREAD_MUTEX_INITIALIZER;
std::string g_srv_cpu_usage_str;                    // cached describe_exposed("process_cpu_usage") value
butil::atomic<bool> g_srv_cpu_usage_ready(false);   // claim token: at most one response per 100ms cycle
butil::atomic<uint64_t> g_srv_total_cnt(0);
butil::atomic<uint64_t> g_srv_total_bytes(0);
butil::atomic<uint64_t> g_srv_start_time(0);
butil::atomic<uint64_t> g_srv_last_request_time(0);
butil::atomic<uint64_t> g_srv_end_time(0);
bvar::LatencyRecorder g_srv_latency_recorder("server_latency");
bvar::LatencyRecorder g_srv_cpu_recorder("server_cpu");
bvar::LatencyRecorder g_srv_memory_recorder("server_memory");
// 长尾 (服务端注入) 的**进程汇总**口径; 每端口另有 SrvPort::tail_cnt / tail_latency。
// 命中计数与 g_srv_total_cnt 同生命周期 (在 StatsPrinter 同一处清零, 避免与并发首请求竞态);
// 长尾本身的注入逻辑见下方 "长尾/重尾请求模拟" 段。
butil::atomic<uint64_t> g_srv_tail_cnt(0);
bvar::LatencyRecorder g_srv_tail_latency_recorder("server_tail_latency");
std::string g_srv_rsp_name;
butil::atomic<bool> g_srv_stop(false);

// ---- 多端口: 每端口一个统计槽位 ----
// brpc 硬约束: 1 个 Server 对象只 Start 一次、只监听 1 个端口
// (src/brpc/server.cpp 的状态机; docs/cn/server.md:230-232 明文"需要监听N个端口
//  就起N个Server")。因此本工具用 N 个 brpc::Server 对象, 并配 N 个槽位:
// ports[0] = 主端口 (-port, RPC + HTTP 管理), ports[1..] = 额外 RPC 端口
// (-srv_extra_ports)。业务参数全部是进程级 gflags, 不做 per-port 差异。
//
// 口径分两层, 两层都写 (双写常数开销, 与 N 无关):
// - 进程汇总 = 既有的 g_srv_* 全局量, 名字与语义**保持不变** (向后兼容);
// - 每端口 = 本结构的计数 + server_<port>_latency / server_<port>_tail_latency。
//   LatencyRecorder 的分位来自各实例内部的蓄水池采样快照, 运行期无法把 N 个
//   per-port recorder 合并成汇总, 所以汇总必须独立记一份, 不能事后合并。
struct SrvPort {
    int port = 0;                    // 实际监听端口 (Start 成功后由 listen_address() 回填)
    brpc::Server* server = nullptr;  // 非拥有 (main 的 servers 向量拥有)
    // ---- 每端口独立统计 (口径与 g_srv_* 逐项对应) ----
    butil::atomic<uint64_t> total_cnt{0};
    butil::atomic<uint64_t> total_bytes{0};
    // 长尾到达序号**必须每端口独立**: 客户端的连接分配 (Init 的 rr_index % servers)
    // 与发送轮询 (g_send_index % n) 会和 TailHit 的等间隔"梳齿"锁步 —— thread_num 取
    // 端口数 S 的倍数时端口号 = 连接序号 % S, 而 p=100 的命中序列是 seq ≡ 9 (mod 10)
    // (全奇数), S=2 会让端口 1 独占 100% 长尾、端口 0 为 0%。每端口序号后, 不变量
    // 从"全进程误差 ≤1"变为"每端口误差 ≤1", 既正确又是最强的验证抓手。
    butil::atomic<uint64_t> tail_seq{0};
    butil::atomic<uint64_t> tail_cnt{0};
    bvar::LatencyRecorder* latency = nullptr;       // "server_<port>_latency"
    bvar::LatencyRecorder* tail_latency = nullptr;  // "server_<port>_tail_latency"
    // 不设 per-port cpu/memory: 数据源 process_cpu_usage / process_memory_resident
    // 本身就是进程级, 复制 N 份只会得到 N 条相同曲线, 是误导而不是信息。

    SrvPort() = default;
    // 含 butil::atomic 成员 -> 不可拷贝/移动。显式禁用, 防止误按值放进容器
    // (按值放 vector 既无法编译, 扩容还会让 service 持有的裸指针悬空)。
    SrvPort(const SrvPort&) = delete;
    SrvPort& operator=(const SrvPort&) = delete;
};

// 启动期一次建好, 之后只改元素内部的 atomic/recorder, 不做增删
// (运行期只读集合本身, 与 g_step_stats 同一套约定)。
std::vector<std::unique_ptr<SrvPort>> g_srv_ports;
// 单端口时关掉 per-port recorder 的创建与写入: 保证单端口下 /vars 集合与热路径开销
// 与改造前一致 (压测工具的基线不能因为一次重构而平移)。
bool g_srv_multi_port = false;

// 建 per-port recorder 并自检名字真的暴露成功。
// bvar 撞名是**静默失败** —— Variable::expose_impl 只打一行 ERROR 然后 _name.clear(),
// 此后该变量永远读出空值 (src/bvar/variable.cpp), 只有 FLAGS_bvar_abort_on_same_name
// (默认 false) 才会中止。统计恒为 0 且无任何异常, 极难排查, 所以这里主动把静默失败
// 变成响亮的启动失败。名字由端口唯一决定, 正常永远不触发, 属兜底。
static int CreatePortRecorders(SrvPort* p) {
    char buf[64];
    snprintf(buf, sizeof(buf), "server_%d_latency", p->port);
    p->latency = new bvar::LatencyRecorder(buf);
    if (bvar::Variable::describe_exposed(buf).empty()) {
        LOG(ERROR) << "bvar name conflict: " << buf
                   << " is not exposed (per-port statistics would silently read 0)";
        return -1;
    }
    snprintf(buf, sizeof(buf), "server_%d_tail_latency", p->port);
    p->tail_latency = new bvar::LatencyRecorder(buf);
    if (bvar::Variable::describe_exposed(buf).empty()) {
        LOG(ERROR) << "bvar name conflict: " << buf
                   << " is not exposed (per-port tail statistics would silently read 0)";
        return -1;
    }
    return 0;
}

// 读各端口当前连接数 (来自各 Server 自己的 acceptor)。这是唯一真正 per-port 且免费的
// 连接指标, 也是"客户端是否把连接均匀分到各端口"的直接证据。*total 为各端口之和。
// 放在 SrvPort 定义之后、消费者之前 —— 报告与控制台统计都用它, 避免两处口径分叉。
static void CollectPortConnections(std::vector<size_t>* per_port, size_t* total) {
    per_port->assign(g_srv_ports.size(), 0);
    *total = 0;
    for (size_t i = 0; i < g_srv_ports.size(); ++i) {
        if (g_srv_ports[i]->server == NULL) continue;
        brpc::ServerStatistics st;
        g_srv_ports[i]->server->GetStat(&st);
        (*per_port)[i] = st.connection_count;
        *total += st.connection_count;
    }
}

// ==================== Proxy 转发 (A→B→C) ====================
// 本进程可同时充当 ServerB (代理): 收到 A 的 PerfTestService::Test 请求后,
// 在服务 bthread 内同步转发到配置的后端 ServerC (round-robin: 全局转发计数 % N),
// 透传 name/attachment; A 观测延迟自然包含 B→C RTT (同 baidu_proxy_and_generic_call)。
// 后端列表是常驻配置 (非 per-test), 经 HTTP POST /ub_test/proxy_config 设置;
// 列表为空 = 不转发, 行为与未配置完全一致。旧快照由 shared_ptr 引用计数释放。

struct ProxySnapshot {  // 不可变快照, 经 std::atomic_load/store 热切换
    std::vector<std::string> servers;                     // 配置的后端 (查询展示用)
    std::vector<std::shared_ptr<brpc::Channel>> channels; // Init 成功的后端 (轮询集合)
};

// 前置声明 (定义在下方 "模拟业务处理" 段): embedding 查询模拟在转发前执行
static inline void SimulateEmbeddingLookup();

struct ProxyStats {    // 常驻进程级统计; reset=1 整体换新 (versioned bvar 名)
    butil::atomic<uint64_t> total_cnt{0};   // 转发尝试总数, 兼作 RR 选择计数
    butil::atomic<uint64_t> fanin_cnt{0};   // 入站请求数 (fan-out 时 total = fanin x times)
    butil::atomic<uint64_t> error_cnt{0};
    butil::atomic<uint64_t> byte_cnt{0};    // B→C 请求附件 + C→B 响应附件字节
    butil::atomic<int64_t> min_us{INT64_MAX};
    butil::atomic<int64_t> max_us{0};
    butil::atomic<int64_t> last_error_log_us{0};  // 转发错误日志 1s 窗口节流
    butil::atomic<uint64_t> suppressed_logs{0};
    bvar::LatencyRecorder* latency;         // B→C 转发延迟 (us), 窗口 3600s
    bvar::LatencyRecorder* backend_cpu;     // C 的 cpu_usage, atof*100 入录
    uint64_t start_time_us;

    explicit ProxyStats(int version)
        : latency(NULL), backend_cpu(NULL) {
        char name_buf[64];
        snprintf(name_buf, sizeof(name_buf), "proxy%d_latency", version);
        latency = new bvar::LatencyRecorder(name_buf, 3600);
        snprintf(name_buf, sizeof(name_buf), "proxy%d_backend_cpu", version);
        backend_cpu = new bvar::LatencyRecorder(name_buf, 3600);
        start_time_us = butil::gettimeofday_us();
    }
    ~ProxyStats() {
        delete latency;      // dtor 自动 hide() 从 /vars 摘除
        delete backend_cpu;
    }
};

pthread_mutex_t g_proxy_mutex = PTHREAD_MUTEX_INITIALIZER;  // 仅串行化 reset 自身
std::shared_ptr<const ProxySnapshot> g_proxy_snapshot;      // 只经 std::atomic_store/load
std::shared_ptr<ProxyStats> g_proxy_stats;                  // 同上
// 每个入站请求的转发次数 = 启动参数 srv_forward_times (main 中已校验到 [1, 100],
// 运行期只读 FLAGS_srv_forward_times)。
const int kMaxForwardTimes = 100;   // 防止误配造成的放大失控
int g_proxy_stats_version = 0;                              // bvar 版本命名

// B 自身 CPU/内存常驻采样 (proxy_stats 的 self.* 数据源)。
// 不用 StatsPrinter 的 g_srv_cpu_recorder/g_srv_memory_recorder 的原因是:
// 1) 那两处的 bvar::LatencyRecorder 默认窗口 = FLAGS_bvar_dump_interval (10s),
//    样本 10 秒后过期;
// 2) StatsPrinter 只在压测轮次内运行, 轮次结束打印完报告即退出, 不再采样,
//    压测结束后查询 proxy_stats 就会读到过期的空窗口 (avg/max 全 0)。
// 这里用独立常驻采样线程 + 3600s 窗口 recorder, 与轮次生命周期彻底解耦。
bvar::LatencyRecorder g_proxy_self_cpu_recorder("proxy_self_cpu", 3600);
bvar::LatencyRecorder g_proxy_self_memory_recorder("proxy_self_memory", 3600);

static void* ProxySelfSampler(void* /*arg*/) {
    while (!g_srv_stop.load(butil::memory_order_relaxed)) {
        bthread_usleep(100000);  // 100ms 采样间隔 (与 StatsPrinter 一致)
        const double cpu = atof(
            bvar::Variable::describe_exposed("process_cpu_usage").c_str()) * 100;
        const double mem = atof(
            bvar::Variable::describe_exposed("process_memory_resident").c_str()) / 1024 / 1024;
        g_proxy_self_cpu_recorder << static_cast<int64_t>(cpu);
        g_proxy_self_memory_recorder << static_cast<int64_t>(mem);
    }
    return NULL;
}

// 构造后端快照: 逐个建 Channel (Init 对 ip:port 无 I/O 阻塞), 选项取自进程 flags
// (与 PerformanceTest::Init 同源), 在配置时刻生效。Init 失败的后端不进入轮询集合。
// 返回 0=全部成功, -1=部分/全部失败 (err 汇总, 仅 ready 后端生效)。
static int BuildProxySnapshot(const std::vector<std::string>& servers,
                              std::shared_ptr<const ProxySnapshot>* out,
                              std::string* err) {
    std::shared_ptr<ProxySnapshot> snap(new ProxySnapshot);
    snap->servers.reserve(servers.size());
    snap->channels.reserve(servers.size());
    for (size_t i = 0; i < servers.size(); ++i) {
        brpc::ChannelOptions options;
        options.socket_mode = ProcessSocketMode();
        options.protocol = brpc::PROTOCOL_BAIDU_STD;
        options.connection_type = FLAGS_connection_type;    // 默认 "single"
        options.timeout_ms = FLAGS_rpc_timeout_ms;
        options.connect_timeout_ms = FLAGS_connect_timeout_ms;
        options.max_retry = FLAGS_max_retry;
        std::shared_ptr<brpc::Channel> ch(new brpc::Channel);
        if (ch->Init(servers[i].c_str(), &options) != 0) {
            if (!err->empty()) *err += "; ";
            *err += "channel init failed: " + servers[i];
            continue;
        }
        snap->servers.push_back(servers[i]);
        snap->channels.push_back(ch);
    }
    *out = snap;
    return snap->channels.size() == servers.size() ? 0 : -1;
}

// 换新统计组: 计数/recorder 全部清零。在途请求持有旧组 shared_ptr, 尾增量自然
// 丢失 (reset 从调用时刻即时生效)。
static void ResetProxyStats() {
    pthread_mutex_lock(&g_proxy_mutex);   // 串行化 reset
    ++g_proxy_stats_version;
    std::shared_ptr<ProxyStats> fresh(new ProxyStats(g_proxy_stats_version));
    std::atomic_store(&g_proxy_stats, fresh);
    pthread_mutex_unlock(&g_proxy_mutex);
    LOG(INFO) << "Proxy stats reset (version=" << g_proxy_stats_version << ")";
}

// 转发单个入站请求 (fan-out 模型, 次数由启动参数 srv_forward_times 控制, 默认 1)。
// 发送方式由启动参数 srv_forward_async 控制:
// - false (默认): 串行同步, 每次转发阻塞等待返回后再发下一个,
//   A 观测延迟 ≈ K 次转发延迟之和;
// - true: 全部异步并发 fire (brpc::DoNothing) 后逐一 brpc::Join 等待,
//   A 观测延迟 ≈ K 次转发延迟的最大值 (K=1 时两者无差异)。
// 返回 true = 已转发且全部成功 (response->name 已置为第一个 C 的 name);
// false = 未配置(不转发) 或 任一次转发失败。失败时已 cntl->SetFailed 把 A 的请求
// 置错 (取首个失败后端), response 的 required 字段 cpu_usage 保持 B 自身值、
// name 由调用方兜底填充。
// 转发次数 K > 1 时: 用 K 个连续 round-robin 索引选择 backend (K <= 后端数时互不
// 相同, K > 后端数时环形回绕, 部分后端会收到多次)。每个后端的失败独立计数/记日志,
// 任一失败即整体失败。
static bool ProxyForwardOnce(brpc::Controller* cntl,
                             const test::PerfTestRequest* request,
                             test::PerfTestResponse* response) {
    std::shared_ptr<const ProxySnapshot> snap = std::atomic_load(&g_proxy_snapshot);
    std::shared_ptr<ProxyStats> st = std::atomic_load(&g_proxy_stats);
    const size_t n = (snap != NULL) ? snap->channels.size() : 0;
    if (n == 0) {
        // 列表为空/未配置: 老路径, 由调用方回退 B 自身 name。
        return false;
    }

    st->fanin_cnt.fetch_add(1, butil::memory_order_relaxed);
    const int times = FLAGS_srv_forward_times;   // 启动参数, main 已校验 [1, 100]
    const bool async = FLAGS_srv_forward_async;

    // 每个转发目标的独立承接对象 (异步时 K 个同时在途, 必须各自独立存续到等待完成;
    // brpc::Controller 不可拷贝/移动, 用 unique_ptr 承接)
    struct PerForward {
        test::PerfTestRequest request;
        test::PerfTestResponse response;
        brpc::Controller cntl;
        size_t backend_idx;
    };
    std::vector<std::unique_ptr<PerForward>> fwds;
    fwds.reserve(times);

    // ---- Phase 1: 选择后端 (连续 RR 索引) + embedding 查询 (先查询再转发) + 组装 ----
    for (int k = 0; k < times; ++k) {
        fwds.push_back(std::unique_ptr<PerForward>(new PerForward));
        PerForward& f = *fwds.back();
        // round-robin: 全局转发计数 % N (成功/失败都计数, 保证各后端近均匀;
        // fan-out 时 K 次转发取 K 个连续索引, 命中不同后端)
        f.backend_idx = st->total_cnt.fetch_add(1, butil::memory_order_relaxed) % n;
        f.request = *request;   // name/echo_attachment 原样透传
        f.cntl.request_attachment().append(cntl->request_attachment());  // A 的附件 → C
    }

    // ---- Phase 2: 发送 (串行 = 每次同步阻塞; 异步 = 全部 fire 后统一等待) ----
    for (int k = 0; k < times; ++k) {
        PerForward& f = *fwds[k];
        test::PerfTestService_Stub stub(snap->channels[f.backend_idx].get());
        stub.Test(&f.cntl, &f.request, &f.response,
                  async ? brpc::DoNothing() : NULL);
    }

    // ---- Phase 3: 等待 (异步) + 统计/错误处理 ----
    bool all_ok = true;
    bool first_failure_logged = false;
    int fail_code = 0;
    std::string fail_text;
    for (int k = 0; k < times; ++k) {
        PerForward& f = *fwds[k];
        if (async) {
            brpc::Join(f.cntl.call_id());
        }

        // 字节统计: 请求侧 = name + 附件 (无论成败都计入, 请求已实际发出);
        // 响应侧 = name + 附件 (仅成功计入)。
        st->byte_cnt.fetch_add(static_cast<uint64_t>(f.request.name().size())
                               + f.cntl.request_attachment().size(),
                               butil::memory_order_relaxed);
        if (f.cntl.Failed()) {
            st->error_cnt.fetch_add(1, butil::memory_order_relaxed);
            // 错误日志按时间窗口抽样打印 (模式同 HandleResponse 的节流):
            // 后端异常时海量失败转发逐条打日志会占满磁盘, 窗口内其余错误只计数。
            int64_t now = butil::monotonic_time_us();
            int64_t last = st->last_error_log_us.load(butil::memory_order_relaxed);
            if (FLAGS_error_log_interval_ms <= 0
                || (now - last >= (int64_t)FLAGS_error_log_interval_ms * 1000
                    && st->last_error_log_us.exchange(now, butil::memory_order_relaxed) == last)) {
                uint64_t suppressed = st->suppressed_logs.exchange(0, butil::memory_order_relaxed);
                LOG(ERROR) << "[Proxy] forward #"
                           << (st->total_cnt.load(butil::memory_order_relaxed) - 1)
                           << " to backend[" << f.backend_idx << "] failed: "
                           << f.cntl.ErrorText()
                           << (suppressed > 0 ? " (suppressed " + std::to_string(suppressed)
                                                  + " errors since last log)" : "");
            } else {
                st->suppressed_logs.fetch_add(1, butil::memory_order_relaxed);
            }
            if (!first_failure_logged) {
                first_failure_logged = true;
                all_ok = false;
                fail_code = f.cntl.ErrorCode();
                fail_text = f.cntl.ErrorText();
            }
            continue;   // 继续统计剩余后端, 记录每个失败
        }

        int64_t lat = f.cntl.latency_us();
        *st->latency << lat;
        int64_t cur = st->min_us.load(butil::memory_order_relaxed);
        while (lat < cur && !st->min_us.compare_exchange_weak(cur, lat)) {}
        cur = st->max_us.load(butil::memory_order_relaxed);
        while (lat > cur && !st->max_us.compare_exchange_weak(cur, lat)) {}
        st->byte_cnt.fetch_add(static_cast<uint64_t>(f.response.name().size())
                               + f.cntl.response_attachment().size(),
                               butil::memory_order_relaxed);
        if (!f.response.cpu_usage().empty()) {      // C 的 cpu_usage 仅供代理统计
            *st->backend_cpu << static_cast<int64_t>(atof(f.response.cpu_usage().c_str()) * 100);
        }
        if (k == 0) {
            response->set_name(f.response.name());  // 透传第一个 C 的 name (required)
        }
    }

    if (!all_ok) {
        cntl->SetFailed(fail_code, "%s", fail_text.c_str());
        return false;   // name 兜底在调用方完成 (required)
    }
    return true;
}

// {code, message, enabled, backends:[...], ready, forward_times} — 后端配置查询/设置共用
static std::string BuildProxyBackendsJson(const char* msg, int code) {
    std::shared_ptr<const ProxySnapshot> snap = std::atomic_load(&g_proxy_snapshot);
    const size_t n = (snap != NULL) ? snap->channels.size() : 0;
    std::string out;
    out.reserve(1024);
    out.push_back('{');
    AppendJsonKV(&out, "code", (int64_t)code, true);
    // 显式构造 std::string: 否则 const char* 会被重载决议到 bool 版本 (指针→bool)
    AppendJsonKV(&out, "message", std::string(msg ? msg : "success"), true);
    AppendJsonKV(&out, "enabled", (bool)(n > 0), true);
    out.append("\"backends\":[");
    if (snap != NULL) {
        for (size_t i = 0; i < snap->servers.size(); ++i) {
            if (i) out.push_back(',');
            out.push_back('"');
            JsonEscape(&out, snap->servers[i]);
            out.push_back('"');
        }
    }
    out.append("],");
    AppendJsonKV(&out, "ready", (int64_t)n, true);
    AppendJsonKV(&out, "forward_times", (int64_t)FLAGS_srv_forward_times, false);
    out.push_back('}');
    return out;
}

// {code, message, enabled, backends, ready,
//  forward:{total, errors, error_rate, bytes,
//           latency:{avg/min/max/p50/p90/p99/p999/p9999},
//           backend_cpu_percent:{avg,max}},
//  self:{cpu_percent:{avg,max}, memory_mb:{avg,max}}}
// self.* 读常驻采样 recorder g_proxy_self_* (ProxySelfSampler 每 100ms 推原始
// percent 和 MB, 不做缩放, 3600s 窗口, 压测结束后查询仍有数据);
// reset 不影响它们 (与轮次生命周期解耦)。
static std::string BuildProxyStatsJson() {
    std::shared_ptr<const ProxySnapshot> snap = std::atomic_load(&g_proxy_snapshot);
    std::shared_ptr<ProxyStats> st = std::atomic_load(&g_proxy_stats);
    const size_t n = (snap != NULL) ? snap->channels.size() : 0;
    const uint64_t total = st->total_cnt.load(butil::memory_order_relaxed);
    const uint64_t errors = st->error_cnt.load(butil::memory_order_relaxed);
    int64_t min_us = st->min_us.load(butil::memory_order_relaxed);
    if (min_us == INT64_MAX) min_us = 0;

    std::string out;
    out.reserve(4096);
    out.push_back('{');
    AppendJsonKV(&out, "code", (int64_t)0, true);
    AppendJsonKV(&out, "message", std::string("success"), true);
    AppendJsonKV(&out, "enabled", (bool)(n > 0), true);
    out.append("\"backends\":[");
    if (snap != NULL) {
        for (size_t i = 0; i < snap->servers.size(); ++i) {
            if (i) out.push_back(',');
            out.push_back('"');
            JsonEscape(&out, snap->servers[i]);
            out.push_back('"');
        }
    }
    out.append("],");
    AppendJsonKV(&out, "ready", (int64_t)n, true);
    AppendJsonKV(&out, "forward_times", (int64_t)FLAGS_srv_forward_times, true);

    out.append("\"forward\":{");
    AppendJsonKV(&out, "requests", (int64_t)st->fanin_cnt.load(butil::memory_order_relaxed), true);
    AppendJsonKV(&out, "total", (int64_t)total, true);
    AppendJsonKV(&out, "errors", (int64_t)errors, true);
    AppendJsonKV(&out, "error_rate", total > 0 ? errors * 100.0 / total : 0.0, true);
    AppendJsonKV(&out, "bytes", (int64_t)st->byte_cnt.load(butil::memory_order_relaxed), true);
    out.append("\"latency\":");
    BuildLatencyJson(&out, "", st->latency->latency(), min_us,
                     st->max_us.load(butil::memory_order_relaxed),
                     st->latency->latency_percentile(0.5),
                     st->latency->latency_percentile(0.9),
                     st->latency->latency_percentile(0.99),
                     st->latency->latency_percentile(0.999),
                     st->latency->latency_percentile(0.9999));
    out.append(",\"backend_cpu_percent\":{");
    if (st->backend_cpu->count() > 0) {
        AppendJsonKV(&out, "avg", st->backend_cpu->latency() / 100.0, true);
        AppendJsonKV(&out, "max", st->backend_cpu->max_latency() / 100.0, false);
    } else {
        AppendJsonKV(&out, "avg", 0.0, true);
        AppendJsonKV(&out, "max", 0.0, false);
    }
    out.append("}},\"self\":{\"cpu_percent\":{");
    AppendJsonKV(&out, "avg", (int64_t)g_proxy_self_cpu_recorder.latency(), true);
    AppendJsonKV(&out, "max", (int64_t)g_proxy_self_cpu_recorder.max_latency(), false);
    out.append("},\"memory_mb\":{");
    AppendJsonKV(&out, "avg", (int64_t)g_proxy_self_memory_recorder.latency(), true);
    AppendJsonKV(&out, "max", (int64_t)g_proxy_self_memory_recorder.max_latency(), false);
    out.append("}}}");
    return out;
}

// ==================== Server 端统计 (per-port + 汇总) ====================
// GET /ub_test/server_stats —— 服务端视角的统计快照, 与控制台 "Server Final Report"
// 同源 (同一批原子计数与同一组 recorder), 避免两套口径。
//
// 两个口径必须分开看 (与最终报告一致):
// - requests/bytes/qps/share/connection_count: 本轮计数, 由 StatsPrinter 在轮次结束时
//   清零 -> 查询要趁轮次结束前或结束后立刻做, 否则读到的是新一轮的数;
// - latency/tail.latency: recorder 的**滑动窗口** (默认 10s), 与既有的 server_latency
//   行为一致, 不是本次引入。
// 不加 ?reset=: 那会引入第二个清零点, 正是 StatsPrinter 注释警告的竞态来源。
//
// 单端口时 per_port 仍输出 1 条 (等于 total), 接口形状不随端口数变化。
static std::string BuildServerStatsJson() {
    std::vector<size_t> per_port_conns;
    size_t total_conns = 0;
    CollectPortConnections(&per_port_conns, &total_conns);

    const uint64_t total_cnt = g_srv_total_cnt.load(butil::memory_order_relaxed);
    const uint64_t total_bytes = g_srv_total_bytes.load(butil::memory_order_relaxed);
    const uint64_t total_tail = g_srv_tail_cnt.load(butil::memory_order_relaxed);
    // 时长用进程级的"首请求 -> 末请求" (与最终报告的 Duration 同源)。所有端口共用它,
    // 保证 Σ per-port QPS == 汇总 QPS 精确成立。
    const uint64_t start_time = g_srv_start_time.load(butil::memory_order_relaxed);
    const uint64_t last_req = g_srv_last_request_time.load(butil::memory_order_relaxed);
    const uint64_t end_time = g_srv_end_time.load(butil::memory_order_relaxed);
    uint64_t stop_us = (end_time != 0) ? end_time : last_req;
    double duration_s = (start_time > 0 && stop_us > start_time)
                        ? (stop_us - start_time) / 1000000.0 : 0.0;
    const bool tail_on = (FLAGS_srv_tail_permille > 0);
    const uint64_t total_ok = (total_cnt >= total_tail) ? total_cnt - total_tail : 0;

    std::string out;
    out.reserve(4096);
    out.push_back('{');
    AppendJsonKV(&out, "code", (int64_t)0, true);
    AppendJsonKV(&out, "message", std::string("success"), true);
    AppendJsonKV(&out, "ports", (int64_t)g_srv_ports.size(), true);
    AppendJsonKV(&out, "duration_s", duration_s, true);
    AppendJsonKV(&out, "tail_simulation_enabled", tail_on, true);

    // ---- 汇总 (既有 server_latency / server_tail_latency, 名字与语义不变) ----
    out.append("\"total\":{");
    AppendJsonKV(&out, "requests", (int64_t)total_cnt, true);
    AppendJsonKV(&out, "bytes", (int64_t)total_bytes, true);
    AppendJsonKV(&out, "qps", duration_s > 0 ? total_cnt / duration_s : 0.0, true);
    AppendJsonKV(&out, "bandwidth_mb_s",
                 duration_s > 0 ? total_bytes / 1048576.0 / duration_s : 0.0, true);
    AppendJsonKV(&out, "connection_count", (int64_t)total_conns, true);
    out.append("\"latency\":");
    // 服务端 recorder 不跟踪 min (bvar::LatencyRecorder 只有 Maxer, 没有 Miner, 与既有
    // 汇总口径一致), 传 INT64_MAX -> BuildLatencyJson 统一输出 0 表示"未跟踪"。
    BuildLatencyJson(&out, "",
                     g_srv_latency_recorder.latency(10),
                     INT64_MAX,
                     g_srv_latency_recorder.max_latency(),
                     g_srv_latency_recorder.latency_percentile(0.5),
                     g_srv_latency_recorder.latency_percentile(0.9),
                     g_srv_latency_recorder.latency_percentile(0.99),
                     g_srv_latency_recorder.latency_percentile(0.999),
                     g_srv_latency_recorder.latency_percentile(0.9999));
    if (tail_on) {
        out.append(",\"tail\":{");
        AppendJsonKV(&out, "requests", (int64_t)total_tail, true);
        // 分母用成功(非长尾)请求数: 与 tail_latency 的口径自洽 (长尾延迟只统计长尾)
        AppendJsonKV(&out, "ratio", total_ok > 0 ? total_tail * 100.0 / total_ok : 0.0, true);
        out.append("\"latency\":");
        BuildLatencyJson(&out, "",
                         g_srv_tail_latency_recorder.latency(10),
                         INT64_MAX,
                         g_srv_tail_latency_recorder.max_latency(),
                         g_srv_tail_latency_recorder.latency_percentile(0.5),
                         g_srv_tail_latency_recorder.latency_percentile(0.9),
                         g_srv_tail_latency_recorder.latency_percentile(0.99),
                         g_srv_tail_latency_recorder.latency_percentile(0.999),
                         g_srv_tail_latency_recorder.latency_percentile(0.9999));
        out.push_back('}');
    }
    out.push_back('}');

    // ---- 每端口 ----
    // 单端口时 per-port 视图 == 汇总视图, 但延迟仍取自汇总 recorder (此时不创建
    // per-port recorder, 见 g_srv_multi_port), 所以直接复用上面的汇总数值。
    out.append(",\"per_port\":[");
    for (size_t i = 0; i < g_srv_ports.size(); ++i) {
        if (i) out.push_back(',');
        const SrvPort* p = g_srv_ports[i].get();
        const uint64_t cnt = p->total_cnt.load(butil::memory_order_relaxed);
        const uint64_t bytes = p->total_bytes.load(butil::memory_order_relaxed);
        const uint64_t tail = tail_on ? p->tail_cnt.load(butil::memory_order_relaxed) : 0;
        const bvar::LatencyRecorder* lr = g_srv_multi_port ? p->latency : &g_srv_latency_recorder;
        const bvar::LatencyRecorder* tr = g_srv_multi_port ? p->tail_latency
                                                           : &g_srv_tail_latency_recorder;
        const uint64_t ok = (cnt >= tail) ? cnt - tail : 0;
        out.push_back('{');
        AppendJsonKV(&out, "port", (int64_t)p->port, true);
        AppendJsonKV(&out, "requests", (int64_t)cnt, true);
        AppendJsonKV(&out, "share", total_cnt > 0 ? cnt * 100.0 / total_cnt : 0.0, true);
        AppendJsonKV(&out, "bytes", (int64_t)bytes, true);
        AppendJsonKV(&out, "qps", duration_s > 0 ? cnt / duration_s : 0.0, true);
        AppendJsonKV(&out, "bandwidth_mb_s",
                     duration_s > 0 ? bytes / 1048576.0 / duration_s : 0.0, true);
        AppendJsonKV(&out, "connection_count", (int64_t)per_port_conns[i], true);
        out.append("\"latency\":");
        BuildLatencyJson(&out, "", lr->latency(10), INT64_MAX, lr->max_latency(),
                         lr->latency_percentile(0.5), lr->latency_percentile(0.9),
                         lr->latency_percentile(0.99), lr->latency_percentile(0.999),
                         lr->latency_percentile(0.9999));
        if (tail_on) {
            out.append(",\"tail\":{");
            AppendJsonKV(&out, "requests", (int64_t)tail, true);
            AppendJsonKV(&out, "ratio", ok > 0 ? tail * 100.0 / ok : 0.0, true);
            out.append("\"latency\":");
            BuildLatencyJson(&out, "", tr->latency(10), INT64_MAX, tr->max_latency(),
                             tr->latency_percentile(0.5), tr->latency_percentile(0.9),
                             tr->latency_percentile(0.99), tr->latency_percentile(0.999),
                             tr->latency_percentile(0.9999));
            out.push_back('}');
        }
        out.push_back('}');
    }
    out.append("]}");
    return out;
}

// ==================== 模拟业务处理: 3ms CPU 忙计算 ====================
// 结果下沉用: 使计算链可观察, 编译器无法将循环当死代码消除
volatile double g_srv_busy_sink = 0.0;

// 单个业务工作单元: 整数散列混合 + 浮点乘加, 每次迭代约 4~8ns。
// 与请求负载无关, 保证每请求 CPU 时间恒定。
static inline double BusyWorkOnce(double x) {
    x = x * 1.000000119 + 0.000000001;             // 浮点乘加链
    uint64_t h = static_cast<uint64_t>(x * 1e6);   // 整数混合散列
    h ^= h >> 17;
    h *= 0x9E3779B97F4A7C15ULL;
    h ^= h >> 31;
    x += static_cast<double>(h & 0x3FF) * 1e-9;
    return x;
}

// 时间盒忙等循环: 持续 CPU 计算 busy_us 微秒。
// - 不是 sleep: bthread 不挂起、worker 线程持续满载, 模拟真实 CPU 密集业务;
// - 每 1024 次迭代检查一次时间 (~8.6us), vDSO 开销摊薄 <1%;
// - 基于真实时间, 对 CPU 睿频/降频不敏感, 无需校准;
// - srv_busy_yield_us > 0 时每 N us 主动 bthread_yield() 一次: 把当前 bthread 放回 runqueue
//   尾部, 让同 worker 上的其他请求得以运行 —— 模拟"业务周期释放 CPU"的形态 (真实业务靠
//   下游调用/IO 挂起来释放 worker, 这里用显式 yield 表达同一效果)。
//   注意: yield 不减少总 CPU 工作量, 只改善同 worker 上其他请求的等待时间。
static inline void SimulateBusinessLogic(int64_t busy_us) {
    const int64_t start_ns = butil::cpuwide_time_ns();
    const int64_t end_ns = start_ns + busy_us * 1000;
    const int64_t yield_interval_ns = static_cast<int64_t>(FLAGS_srv_busy_yield_us) * 1000;
    int64_t last_yield_ns = start_ns;   // 从起点计时: 满一个 interval 才让出一次
    double x = 1.0;
    int i = 0;
    while (true) {
        x = BusyWorkOnce(x);
        if (++i == 1024) {
            i = 0;
            const int64_t now_ns = butil::cpuwide_time_ns();
            if (now_ns >= end_ns) {
                break;
            }
            if (yield_interval_ns > 0 && now_ns - last_yield_ns >= yield_interval_ns) {
                last_yield_ns = now_ns;
                bthread_yield();
            }
        }
    }
    g_srv_busy_sink = x;  // volatile 下沉, 防优化消除
}

// ==================== 模拟业务处理: Embedding 查询 (内存随机读写) ====================
// 模拟 AI 推理中的 embedding 检索 (如向量库/embedding 服务查询):
// - 读: 每请求 K 次随机行读取, 对整行做点积累加 (256 float = 1KB/行),
//   随机访问大内存表, 缓存不友好, 产生真实 DRAM 读流量;
// - 写: 读完后随机挑一行写回一个小增量, 产生真实 DRAM 写流量
//   (模拟结果缓存/表更新; 并发写竞争为无害数据竞争, 仅影响数值不影响稳定性);
// 表在首个请求到达时惰性初始化 (pthread_once), 开关关闭时零内存开销。

// 表数据与配置 (init 后只读, 无锁)
float* g_embedding_table = NULL;
size_t g_embedding_rows = 0;
int g_embedding_dims = 0;
int g_embedding_lookups = 0;
volatile float g_embedding_sink = 0.0f;   // 结果下沉: 使读取计算链可观察, 防优化消除
pthread_once_t g_embedding_init_once = PTHREAD_ONCE_INIT;

// 惰性初始化: 分配并填充 rows x dims 的浮点表 (填充即真实触页, RSS 为真实占用)。
// 配置非法 (rows/dims <= 0) 或分配失败时置空表并告警, 后续请求直接跳过模拟。
static void InitEmbeddingTable() {
    const int64_t rows = FLAGS_srv_embedding_rows;
    const int dims = FLAGS_srv_embedding_dims;
    if (rows <= 0 || dims <= 0) {
        LOG(WARNING) << "Invalid embedding config (rows=" << rows
                     << ", dims=" << dims << "), embedding simulation disabled";
        return;
    }
    const size_t n = static_cast<size_t>(rows) * dims;
    g_embedding_table = static_cast<float*>(malloc(n * sizeof(float)));
    if (g_embedding_table == NULL) {
        LOG(ERROR) << "Fail to allocate embedding table: " << n * sizeof(float) / 1024 / 1024
                   << "MB, embedding simulation disabled";
        return;
    }
    for (size_t i = 0; i < n; ++i) {
        g_embedding_table[i] = static_cast<float>(butil::fast_rand() % 1000) / 1000.0f;
    }
    g_embedding_rows = static_cast<size_t>(rows);
    g_embedding_dims = dims;
    g_embedding_lookups = FLAGS_srv_embedding_lookups;
    LOG(INFO) << "Embedding table initialized: rows=" << g_embedding_rows
              << " dims=" << g_embedding_dims
              << " size=" << n * sizeof(float) / 1024 / 1024 << "MB"
              << " lookups=" << g_embedding_lookups
              << " write=" << FLAGS_srv_embedding_write;
}

// 每请求模拟: K 次随机行点积读 + 1 次随机行写回。
// 与请求负载无关, 每次请求的读写次数恒定, 模拟纯内存带宽/随机访问业务。
static inline void SimulateEmbeddingLookup() {
    if (!FLAGS_srv_embedding_enabled) {
        return;   // 开关关闭: pthread_once 前直接返回, 表不分配, 零开销
    }
    pthread_once(&g_embedding_init_once, InitEmbeddingTable);
    if (g_embedding_table == NULL) {
        return;   // 配置非法/分配失败
    }
    float sum = 0.0f;
    for (int k = 0; k < g_embedding_lookups; ++k) {
        const size_t row = butil::fast_rand_less_than(g_embedding_rows);
        const float* v = g_embedding_table + row * g_embedding_dims;
        // 自点积: 读完整行并聚合, 计算链依赖数据, 编译器无法消除
        float acc = 0.0f;
        for (int d = 0; d < g_embedding_dims; ++d) {
            acc += v[d] * v[d];
        }
        sum += acc;
    }
    if (FLAGS_srv_embedding_write) {
        // 随机行写回小增量: 真实 DRAM 写流量 (与读行不强制同址, 覆盖全表)
        const size_t row = butil::fast_rand_less_than(g_embedding_rows);
        const size_t col = butil::fast_rand_less_than(
            static_cast<size_t>(g_embedding_dims));
        g_embedding_table[row * g_embedding_dims + col] += sum * 1e-9f;
    }
    g_embedding_sink = sum;   // volatile 下沉, 防优化消除
}

// ==================== 长尾/重尾请求模拟 (srv_tail_*) ====================
// 服务端按到达序号确定性地等间隔命中一部分请求, 注入额外延迟, 并在响应里置 is_tail 标记;
// 客户端据此把长尾请求从主时延统计中剥离、单独统计 (口径细节见 README "长尾模拟说明")。

// 到达序号与命中计数都是**每端口**的 (见 SrvPort::tail_seq / tail_cnt): 用全局序号会
// 与客户端"连接按端口 round-robin"锁步, 使部分端口拿到 0%/100% 的长尾。
// 进程汇总量 (g_srv_tail_cnt / g_srv_tail_latency_recorder) 及生命周期约定
// 见上方 "Server-side globals" 段。

// 等间隔命中判定 (Bresenham / 误差扩散)。
// 返回 true ⟺ "前 seq+1 个请求的期望命中数比前 seq 个多 1", 即
//   floor((seq+1)*p/1000) != floor(seq*p/1000)
// p ∈ [0,1000] 时该差值恒 ∈ {0,1}, 故前 N 个到达中命中数恒为 floor(N*p/1000)
// (比例精确到 1 个请求), 且命中间隔恒为 1000/p (±1) —— 在时间轴上均匀散布。
// 不用 `seq % 1000 < p`: 那会把每 1000 个请求里的 p 个命中聚成一簇, 制造周期性尖峰,
// 长尾失去"孤立事件"的含义 (也可能与客户端的 req_size 轮询锁步)。
// 溢出安全: 令 q = seq/1000, r = seq%1000, 有 floor(seq*p/1000) = q*p + floor(r*p/1000);
// q*p 项在差分中抵消, 只需比较 r 上的两式, 而 r*p <= 999*1000 < 2^63, 永不上溢。
static inline bool TailHit(uint64_t seq) {
    const uint64_t p = static_cast<uint64_t>(FLAGS_srv_tail_permille);
    if (p == 0) {
        return false;
    }
    if (p >= 1000) {
        return true;
    }
    const uint64_t r = seq % 1000;
    return (r * p) / 1000 != ((r + 1) * p) / 1000;
}

// 命中请求的延迟: 固定值, 或 [srv_tail_delay_us, srv_tail_delay_max_us] 均匀分布
static inline int64_t PickTailDelayUs() {
    const int64_t lo = FLAGS_srv_tail_delay_us;
    const int64_t hi = FLAGS_srv_tail_delay_max_us;
    if (hi <= lo) {
        return lo > 0 ? lo : 0;
    }
    return butil::fast_rand_in<int64_t>(lo, hi);
}

// 注入长尾延迟。cpu=true 复用 SimulateBusinessLogic (内部遵从 srv_busy_yield_us 周期让出);
// 否则 bthread_usleep 单次睡满 —— 本 fork 的 TaskGroup::usleep 把 timeout 原样交给全局
// TimerThread, 没有 1000us 分段上限, 且到期唤醒会 signal worker, 不会出现"睡完没人跑"。
// 分段睡眠反而会放大抖动, 不要做。
static inline void InjectTailDelay(int64_t delay_us) {
    if (delay_us <= 0) {
        return;
    }
    if (FLAGS_srv_tail_cpu) {
        SimulateBusinessLogic(delay_us);
    } else {
        bthread_usleep(static_cast<uint64_t>(delay_us));   // 服务器退出时返回 -1, 忽略
    }
}

// ==================== 进程级背景 CPU 负载 ====================
// 与 SimulateBusinessLogic (单请求业务负载, 请求内同步执行) 的区别:
// 本负载常驻运行, 与请求流量完全无关, 模拟进程承载的背景负载对 CPU 的持续占用。
// 支持两种运行载体 (srv_bg_cpu_on_worker 切换), 模拟场景不同:
// - false (默认): 独立 std::thread。不占 brpc worker 池, 模拟同机混部/其他
//   进程抢 CPU —— 只在 OS 调度层面竞争, 核数富余时对请求几乎无感;
// - true: bthread 跑在 brpc worker 池上。忙等阶段 (真实 CPU 计算, 不 yield)
//   独占其所在 worker pthread, 与请求 bthread 直接竞争 worker, 模拟进程内部
//   后台任务侵蚀服务处理能力 —— 即使核数富余也会显著降速, 需相应调大
//   srv_bthread_concurrency 避免饿死请求。
// 两种模式共用 duty cycle 循环 (周期 10ms: busy percent% + 休眠其余),
// 总背景 CPU 占用 ≈ threads x percent% (threads 上限 64);
// 忙等工作复用 BusyWorkOnce, 结果按线程号下沉到 volatile 数组防优化消除;
// 背景负载会反映到 process_cpu_usage 采样 (self.cpu_percent / 服务端 CPU 统计)。

#define MAX_BG_CPU_THREADS 64
volatile double g_bg_cpu_sinks[MAX_BG_CPU_THREADS];  // 每线程结果下沉

// 常驻背景负载核心循环: 忙等 phase + 休眠 phase, 直到 g_srv_stop 置位。
// 忙等内每 1024 次迭代检查一次停止标志, 保证退出及时 (最坏 ~10us)。
// on_worker 仅决定休眠机制: std::thread 用 OS 休眠; bthread 用 bthread_usleep
// (让出 worker 给其他 bthread)。busy phase 两种模式行为一致: 真 CPU 忙等。
static void BackgroundCpuLoopCore(int thread_id, bool on_worker) {
    const int percent = std::min(std::max(FLAGS_srv_bg_cpu_percent, 0), 100);
    const int64_t period_ns = 10000000;  // 10ms 调度周期
    const int64_t busy_ns = period_ns * percent / 100;
    const int64_t idle_ns = period_ns - busy_ns;
    // worker 模式: 每 ~100us yield 一次, 使忙等可被同组其他 bthread 打断,
    // 更接近真实服务的 bthread 负载形态 (极少有毫秒级连续不 yield 的忙等)。
    // 注意: 本 fork 的 bthread 调度模型为每 worker 一个独立 task group,
    // start_background 自非 worker 上下文入队时硬编码 NOSIGNAL (不唤醒 worker),
    // 运行中的 bthread 也不可被 work-stealing 偷走。因此 worker 模式的负载
    // 水平不可精确控制: 实测 10 个 100% busy bthread 在 10 个 worker 上仅
    // ~4 核 (只有被初始 signal/请求事件唤醒过的 worker 才会捡到 bg bthread,
    // 沉睡 worker 无 signal 永不参与)。需要精确 CPU 占用量时请用默认的
    // std::thread 模式。
    const int64_t yield_interval_ns = 100000;  // 100us
    int64_t last_yield_ns = 0;
    while (!g_srv_stop.load(butil::memory_order_relaxed)) {
        if (busy_ns > 0) {
            const int64_t end_ns = butil::cpuwide_time_ns() + busy_ns;
            double x = 1.0;
            int i = 0;
            while (true) {
                x = BusyWorkOnce(x);
                if (++i == 1024) {   // ~5-8us 检查一次
                    i = 0;
                    if (g_srv_stop.load(butil::memory_order_relaxed)) {
                        break;
                    }
                    const int64_t now_ns = butil::cpuwide_time_ns();
                    if (now_ns >= end_ns) {
                        break;
                    }
                    if (on_worker && now_ns - last_yield_ns >= yield_interval_ns) {
                        last_yield_ns = now_ns;
                        bthread_yield();   // 回本 group rq 尾部, 可被空闲 worker steal
                    }
                }
            }
            g_bg_cpu_sinks[thread_id] = x;  // volatile 下沉, 防优化消除
        }
        if (idle_ns > 0) {
            if (on_worker) {
                bthread_usleep(idle_ns / 1000);   // 让出 worker 给其他 bthread
            } else {
                std::this_thread::sleep_for(std::chrono::nanoseconds(idle_ns));
            }
        }
    }
}

// std::thread 入口 (独立 OS 线程模式)
static void BackgroundCpuLoopStd(int thread_id) {
    BackgroundCpuLoopCore(thread_id, false);
}

// bthread 入口 (brpc worker 池模式)
static void* BackgroundCpuLoopBthread(void* arg) {
    BackgroundCpuLoopCore(static_cast<int>(reinterpret_cast<intptr_t>(arg)), true);
    return NULL;
}

// ==================== 客户端指标采样与全局停止判定 ====================
// 每 100ms 采样一次客户端 CPU/内存 (仅在成功响应路径上触发, 避免污染热路径)
static void SampleClientMetrics() {
    uint64_t last = g_last_time.load(butil::memory_order_relaxed);
    uint64_t now = butil::gettimeofday_us();
    if (now > last && now - last > 100000) {
        if (g_last_time.exchange(now, butil::memory_order_relaxed) == last) {
            double cpu = atof(bvar::Variable::describe_exposed("process_cpu_usage").c_str()) * 100;
            double mem = atof(bvar::Variable::describe_exposed("process_memory_resident").c_str()) / 1024 / 1024;
            *g_client_cpu_recorder << static_cast<int64_t>(cpu);
            *g_client_memory_recorder << static_cast<int64_t>(mem * 1024);
        }
    }
}

// 全局停止条件: 完成请求数达到 test_iterations, 或压测时长达到 test_seconds。
//
// 默认 (allow_stop_burst=false) 收尾兼顾:
// 1) 客户端不能在服务端仍在回包时拆连接/退出 (否则易 connection timeout) —
//    靠 RunPerformanceTest 等到 pending==0, 且此处唤醒卡死的泵以便 join;
// 2) 停测瞬间不能再批量补 token 制造尾部齐射 —
//    只 post inflight 叫醒“卡在许可等待”的泵, 泵见 g_stop 后退出且不再
//    SendRequest; 等 token 的泵靠 100ms timedwait 超时归还许可后见 g_stop 退出。
// allow_stop_burst=true 时恢复旧行为: 额外 post_n(token), 允许结束瞬间再发一波。
static void CheckGlobalStop() {
    bool should_stop = false;
    if (g_test_config.test_iterations > 0 &&
        g_total_cnt.load(butil::memory_order_relaxed) >= (uint64_t)g_test_config.test_iterations) {
        should_stop = true;
    }
    if (g_test_config.test_seconds > 0 && g_perf_start_time > 0 &&
        butil::gettimeofday_us() - g_perf_start_time >= (uint64_t)g_test_config.test_seconds * 1000000) {
        should_stop = true;
    }
    if (!should_stop) {
        return;
    }
    bool expected = false;
    if (g_stop.compare_exchange_strong(expected, true, butil::memory_order_relaxed)) {
        int pump_num = g_pump_num.load(butil::memory_order_relaxed);
        if (pump_num > 0) {
            bthread_sem_post_n(&g_inflight_sem, pump_num);
            if (g_test_config.allow_stop_burst && IsQpsLimited()) {
                bthread_sem_post_n(&g_token_sem, pump_num);
            }
        }
    }
}

// ==================== req_size 解析 ====================
// 解析逗号分隔的多 size (如 "128,1024,4096"), 生成 g_req_sizes/g_names。
// 未设置时 g_names 退化为单个空字符串 (与旧 req_size=0 行为一致)。
// 返回 0 成功, -1 失败 (错误信息写入 err)。
static int ParseReqSizes(const std::string& req_size, std::string* err) {
    g_req_sizes.clear();
    g_names.clear();
    if (req_size.empty()) {
        g_names.push_back(std::string());
        return 0;
    }
    std::string::size_type pos1 = 0;
    while (true) {
        std::string::size_type pos2 = req_size.find(',', pos1);
        std::string token = req_size.substr(
            pos1, pos2 == std::string::npos ? std::string::npos : pos2 - pos1);
        // 去掉首尾空白
        token.erase(0, token.find_first_not_of(" \t"));
        token.erase(token.find_last_not_of(" \t") + 1);
        if (token.empty()) {
            *err = "empty size in req_size: " + req_size;
            return -1;
        }
        char* end = NULL;
        long v = strtol(token.c_str(), &end, 10);
        if (end == token.c_str() || *end != '\0' || v < 0) {
            *err = "invalid size in req_size: " + token;
            return -1;
        }
        g_req_sizes.push_back((int64_t)v);
        g_names.push_back(std::string((size_t)v, 'r'));
        if (pos2 == std::string::npos) break;
        pos1 = pos2 + 1;
    }
    return 0;
}

// ==================== 多端口监听: 端口解析与启动期校验 ====================
// 解析逗号分隔的端口列表 (分词/去空白/报错风格同 ParseRampList/ParseReqSizes)。
// 范围 [1,65535]; 0 在此被拒 —— 内核动态分配端口后, 进程内没有把真实端口告知客户端
// 的通道 (客户端 servers 列表由 POST /ub_test/start 人工下发), 动态端口在多后端场景
// 不可用。列表内部重复在此查; 与主端口冲突及 -reuse_port 互斥见 ValidateListenPorts。
// 返回 0 成功, -1 失败 (原因写入 err)。
static int ParsePortList(const std::string& text, std::vector<int>* out, std::string* err) {
    out->clear();
    std::string::size_type pos1 = 0;
    while (true) {
        std::string::size_type pos2 = text.find(',', pos1);
        std::string token = text.substr(
            pos1, pos2 == std::string::npos ? std::string::npos : pos2 - pos1);
        token.erase(0, token.find_first_not_of(" \t"));
        token.erase(token.find_last_not_of(" \t") + 1);
        if (token.empty()) {
            *err = "empty entry in srv_extra_ports: " + text;
            return -1;
        }
        char* end = NULL;
        const long v = strtol(token.c_str(), &end, 10);
        if (end == token.c_str() || *end != '\0') {
            *err = "invalid port in srv_extra_ports: " + token;
            return -1;
        }
        if (v < 1 || v > 65535) {
            *err = "port out of range [1,65535] in srv_extra_ports: " + token;
            if (v == 0) {
                *err += " (port 0 = kernel-assigned is not supported here: the client must "
                        "know every backend port up front)";
            }
            return -1;
        }
        for (size_t i = 0; i < out->size(); ++i) {
            if ((*out)[i] == static_cast<int>(v)) {
                *err = "duplicate port in srv_extra_ports: " + token;
                return -1;
            }
        }
        out->push_back(static_cast<int>(v));
        if (pos2 == std::string::npos) break;
        pos1 = pos2 + 1;
    }
    if (out->empty()) {
        *err = "empty srv_extra_ports";
        return -1;
    }
    return 0;
}

// 启动期校验 (只看配置, 不碰 socket)。任一不满足即拒绝启动 —— 端口配错若被放行, 症状是
// "少起一个后端 -> 客户端连不上", 报错点离病因很远; 这是启动参数, 与 HTTP 下发参数的
// "WARNING + 回退默认"策略刻意不同 (不能为一条坏请求杀掉在跑的服务)。
static int ValidateListenPorts(int main_port, const std::vector<int>& extras,
                               std::string* err) {
    if (extras.empty()) {
        return 0;
    }
    if (FLAGS_reuse_port) {
        // SO_REUSEPORT 开启时两个 Server 可成功双绑同一端口且不报任何错
        // (src/butil/endpoint.cpp 的 setsockopt 分支), 后果是每端口统计失去意义
        // (内核在多个 listen socket 间分发连接), 且 brpc 自身的 per-port bvar 撞名后
        // 被静默 hide。这与 "额外端口" 的语义根本冲突, 直接拒绝而不是让用户踩。
        *err = "-reuse_port must not be combined with -srv_extra_ports: SO_REUSEPORT would "
               "silently bind the same port twice, invalidating per-port statistics";
        return -1;
    }
    if (main_port == 0) {
        *err = "-port=0 (kernel-assigned) must not be combined with -srv_extra_ports: the "
               "main port would be unpredictable and cannot be checked for conflicts";
        return -1;
    }
    for (size_t i = 0; i < extras.size(); ++i) {
        if (extras[i] == main_port) {
            *err = "port " + std::to_string(main_port)
                   + " appears in srv_extra_ports but is already the main -port";
            return -1;
        }
    }
    return 0;
}

// ==================== PerformanceTest (from client.cpp) ====================
class PerformanceTest {
public:
    PerformanceTest(int attachment_size, bool echo_attachment)
        : _addr(NULL)
        , _channel(NULL)
        , _connect_latency_us(0)
        , _first_rpc_latency_us(0)
        , _first_rpc_memory_mb(0)
        , _init_result(-1)
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

    // Initialize RPC channel and execute first RPC (establish real TCP connection).
    // Records connect latency and first-RPC latency.
    int Init() {
        // FLAGS_max_retry is mapped to ChannelOptions::max_retry for all RPCs
        // on this Channel. For the first RPC below, we explicitly set
        // cntl.set_max_retry(0) to disable brpc's internal retry and instead
        // use a manual retry loop for accurate single-RPC latency measurement.
        // Retry semantics (error filtering, retry count) are aligned with
        // brpc's DefaultRetryPolicy. Jittered backoff uses connect_retry_interval.

        brpc::ChannelOptions options;
        options.socket_mode = ProcessSocketMode();
        options.protocol = brpc::PROTOCOL_BAIDU_STD;
        options.connection_type = g_test_config.connection_type;
        options.timeout_ms = g_test_config.rpc_timeout_ms;
        options.connect_timeout_ms = g_test_config.connect_timeout_ms;
        options.max_retry = g_test_config.max_retry;
        if (g_test_config.use_connection_group) {
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
        // 首包 RPC 负载固定 1024B (不随 req_size 变化, 保证建链/首包延迟统计口径一致)
        static const std::string k_first_rpc_name(1024, 'r');
        request.set_name(k_first_rpc_name);
        // 标记为建连探针: 服务端保证长尾注入不作用于探针 (也不占用其到达序号),
        // 使建连质量与人工注入的长尾互不干扰。
        request.set_is_probe(true);
        test::PerfTestService_Stub stub(_channel);

        // First RPC with manual retry: disable brpc's internal retry per-call
        // so we measure the latency of a single successful RPC, not the
        // cumulative time of all retries.
        int retry_count = 0;
        while (true) {
            brpc::Controller cntl;
            cntl.set_max_retry(0);  // disable brpc internal retry for accurate latency
            test::PerfTestResponse response;
            int64_t rpc_start_ns = butil::cpuwide_time_ns();
            stub.Test(&cntl, &request, &response, NULL);
            int64_t rpc_end_ns = butil::cpuwide_time_ns();

            if (!cntl.Failed()) {
                _first_rpc_latency_us = (rpc_end_ns - rpc_start_ns) / 1000;
                _first_rpc_memory_mb = atof(bvar::Variable::describe_exposed("process_memory_resident").c_str()) / 1024 / 1024;
                _init_result = 0;
                return 0;
            }

            // Only retry on errors that brpc's DefaultRetryPolicy considers retryable
            // (EFAULTEDSOCKET, ECONNREFUSED, ETIMEDOUT, etc. — 13 error codes total).
            if (!brpc::DefaultRetryPolicy()->DoRetry(&cntl)) {
                LOG(ERROR) << "[First] RPC call failed with non-retryable error: " << cntl.ErrorText();
                _init_result = -1;
                return -1;
            }

            if (retry_count >= g_test_config.max_retry) {
                LOG(ERROR) << "[First] RPC call failed after " << g_test_config.max_retry
                           << " retries: " << cntl.ErrorText();
                _init_result = -1;
                return -1;
            }

            std::random_device rd;
            std::mt19937 gen(rd());
            std::uniform_int_distribution<> distrib(
                g_test_config.connect_retry_interval, 2 * g_test_config.connect_retry_interval);
            int random_ms = distrib(gen);
            LOG(WARNING) << "Retry " << (retry_count + 1) << "/"
                         << g_test_config.max_retry << " for first RPC after "
                         << random_ms << "ms: " << cntl.ErrorText();
            bthread_usleep(random_ms * 1000);
            ++retry_count;
        }
    }

    // 发送请求 (纯组装 + 异步发送; 并发/限流控制由中央发送泵 SendPump 完成)
    void SendRequest() {
        g_pending_req_count.fetch_add(1, butil::memory_order_relaxed);
        RespClosure* closure = new RespClosure;
        test::PerfTestRequest request;
        closure->resp = new test::PerfTestResponse();
        closure->cntl = new brpc::Controller();
        // 阶梯模式下"发送时盖章": 请求归属于发送时刻所在的级 (延迟是在该级流量条件下测得的),
        // 由此得到 Σ steps[].requests == total_requests 的划分不变量。
        closure->step_stats = NULL;
        if (g_ramp.enabled && !g_step_stats.empty()) {
            int idx = g_cur_step.load(butil::memory_order_acquire);
            if (idx < 0) idx = 0;
            if (idx >= static_cast<int>(g_step_stats.size())) {
                idx = static_cast<int>(g_step_stats.size()) - 1;
            }
            closure->step_stats = g_step_stats[idx];
        }
        if (g_test_config.client_ignore_oc) {
            closure->cntl->ignore_eovercrowded();
        }
        request.set_echo_attachment(_echo_attachment);
        // 按全局发送索引轮询分配各 size, 保证各 size 请求数平均
        uint64_t send_index = g_all_cnt.fetch_add(1, butil::memory_order_relaxed);
        request.set_name(g_names[send_index % g_names.size()]);
        closure->cntl->request_attachment().append(_attachment);
        google::protobuf::Closure* done = brpc::NewCallback(&HandleResponse, closure);
        test::PerfTestService_Stub stub(_channel);
        stub.Test(closure->cntl, &request, closure->resp, done);
    }

    // 异步 RPC 响应闭包
    struct RespClosure {
        brpc::Controller* cntl;
        test::PerfTestResponse* resp;
        RampStepStats* step_stats;   // 阶梯模式下发送时盖章的级统计 (非 ramp 为 NULL)
    };

    // 静态回调函数 (运行在 brpc 响应线程)
    // 注意：RespClosure 由 SendRequest 中 new 分配，这里用 unique_ptr 接管所有权，
    // 保证所有返回路径都能正确释放（修复原版闭包从未 delete 的内存泄漏）。
    static void HandleResponse(RespClosure* raw_closure) {
        std::unique_ptr<RespClosure> closure(raw_closure);
        std::unique_ptr<brpc::Controller> cntl_guard(closure->cntl);
        std::unique_ptr<test::PerfTestResponse> response_guard(closure->resp);

        g_total_cnt.fetch_add(1, butil::memory_order_relaxed);
        if (closure->cntl->Failed()) {
            // 错误日志按时间窗口抽样打印（默认 1s 一条）：
            // 服务端异常时会有海量失败 RPC，逐条打日志会占满磁盘；
            // 窗口内其余错误只累计计数，随下一条日志一并输出。
            int64_t now = butil::monotonic_time_us();
            int64_t last = g_last_error_log_time_us.load(butil::memory_order_relaxed);
            if (g_test_config.error_log_interval_ms <= 0
                || (now - last >= (int64_t)g_test_config.error_log_interval_ms * 1000
                    && g_last_error_log_time_us.exchange(now, butil::memory_order_relaxed) == last)) {
                uint64_t suppressed = g_suppressed_error_logs.exchange(0, butil::memory_order_relaxed);
                LOG(ERROR) << "[Performance] RPC call failed: " << closure->cntl->ErrorText()
                           << (suppressed > 0 ? " (suppressed " + std::to_string(suppressed)
                                                  + " errors since last log)" : "");
            } else {
                g_suppressed_error_logs.fetch_add(1, butil::memory_order_relaxed);
            }
            g_total_error_cnt.fetch_add(1, butil::memory_order_relaxed);
            if (closure->step_stats != NULL) {
                closure->step_stats->errors.fetch_add(1, butil::memory_order_relaxed);
            }
            bthread_usleep(1000);
        } else {
            int64_t lat = closure->cntl->latency_us();
            const bool is_tail = closure->resp->is_tail();
            if (is_tail) {
                // 长尾请求 (服务端注入): 不进主 recorder / 主 min,max / 阶梯直方图,
                // 只进长尾组与本级的 tail_requests 计数。
                *g_client_tail_recorder << lat;
                g_tail_req_cnt.fetch_add(1, butil::memory_order_relaxed);
                int64_t cur = g_tail_lat_min.load(butil::memory_order_relaxed);
                while (lat < cur && !g_tail_lat_min.compare_exchange_weak(cur, lat)) {}
                cur = g_tail_lat_max.load(butil::memory_order_relaxed);
                while (lat > cur && !g_tail_lat_max.compare_exchange_weak(cur, lat)) {}
                if (closure->step_stats != NULL) {
                    closure->step_stats->tail_requests.fetch_add(1, butil::memory_order_relaxed);
                }
            } else {
                *g_latency_recorder << lat;
                RampRecordLatency(closure->step_stats, lat);   // 阶梯模式: 计入发送时所属级
                // Track min/max (best-effort CAS, for per-run statistics)
                int64_t cur = g_latency_min.load(butil::memory_order_relaxed);
                while (lat < cur && !g_latency_min.compare_exchange_weak(cur, lat)) {}
                cur = g_latency_max.load(butil::memory_order_relaxed);
                while (lat > cur && !g_latency_max.compare_exchange_weak(cur, lat)) {}
            }
            if (g_test_config.debug_latency) {
                // 长尾照打但打标: 排除掉反而会让这个功能在长尾场景下看不见东西
                printf("[latency]%s %ldus\n", is_tail ? "[tail]" : "", (long)lat);
            }
            if (closure->resp->cpu_usage().size() > 0) {
                int cpu_val = static_cast<int>(atof(closure->resp->cpu_usage().c_str()) * 100);
                *g_server_cpu_recorder << cpu_val;
            }

            if (!g_test_config.req_size.empty()) {
                g_total_bytes.fetch_add(closure->resp->name().size(), butil::memory_order_relaxed);
            } else {
                g_total_bytes.fetch_add(closure->cntl->request_attachment().size(), butil::memory_order_relaxed);
            }

            SampleClientMetrics();
        }

        // 成败都计数 (每个发出的请求恰好一次), 保证 Σ steps[].requests == total_requests
        if (closure->step_stats != NULL) {
            closure->step_stats->requests.fetch_add(1, butil::memory_order_relaxed);
        }

        cntl_guard.reset(NULL);
        response_guard.reset(NULL);

        g_pending_req_count.fetch_sub(1, butil::memory_order_relaxed);
        if (g_test_config.allow_stop_burst) {
            // 旧行为: 先补下一发许可, 再判停 (停测瞬间仍可能续发)。
            bthread_sem_post(&g_inflight_sem);
            CheckGlobalStop();
        } else {
            // 新行为: 先判停, 停后不补发, 避免与唤醒 post 叠加齐射。
            CheckGlobalStop();
            if (!g_stop.load(butil::memory_order_relaxed)) {
                bthread_sem_post(&g_inflight_sem);
            }
        }
    }

private:
    void* _addr;
    brpc::Channel* _channel;
    int64_t _connect_latency_us;
    int64_t _first_rpc_latency_us;
    double _first_rpc_memory_mb;
    int _init_result;
    butil::IOBuf _attachment;
    bool _echo_attachment;
};

// ==================== Connection-pool worker for batch establishment ====================
// A fixed number of worker bthreads (= thread_pool_size) loop over an atomic
// work counter, replacing the one-bthread-per-connection model to avoid
// allocating a bthread stack per connection. The worker count naturally
// limits concurrency. (Ported from example/ub_test/client.cpp.)

struct PoolWorkArg {
    int batch_end;
    int attachment_size;
    std::vector<PerformanceTest*>* tests;
    butil::atomic<int>* work_index;
};

static void* PoolWorkerFunc(void* arg) {
    PoolWorkArg* a = static_cast<PoolWorkArg*>(arg);
    while (true) {
        int idx = a->work_index->fetch_add(1, butil::memory_order_relaxed);
        if (idx >= a->batch_end) break;

        (*a->tests)[idx] = new PerformanceTest(a->attachment_size, g_test_config.echo_attachment);
        (*a->tests)[idx]->Init();
    }
    return nullptr;
}

// ==================== Token bucket generator (from client.cpp) ====================
// 细粒度 tick 平滑发放: 每 token_tick_us × token_interval_period 补发一次差额 (默认 10ms)。
// 若按粗粒度 (如 100ms) 批量 post, 会同时唤醒大量等待泵造成发送突发,
// 服务端排队导致 99.9th+ 尾延迟劣化。
// 目标速率两种模式: 固定 expected_qps (常量), 或阶梯 ramp (积分式, 见 RampDueTokens 说明)。
static void* GenerateToken(void* /*arg*/) {
    const int64_t start_time = butil::monotonic_time_ns();
    const int64_t tick_us =
        static_cast<int64_t>(g_test_config.token_tick_us) * g_test_config.token_interval_period;
    int64_t accumulative_token = g_test_config.initial_tokens;
    int step = 0;

    while (!g_stop.load(butil::memory_order_relaxed)) {
        bthread_usleep(tick_us);
        const int64_t elapsed = butil::monotonic_time_ns() - start_time;

        // 阶梯模式: 先推进级 (收尾旧级 + 发布新级索引), 再按新级发牌 —— 保证索引发布
        // 之后发出的请求归属新级。跨多级时逐级收尾, 实时行不漏。
        if (g_ramp.enabled) {
            step = AdvanceRampStep(step, elapsed);
        }

        // 到此刻应累计发出的令牌数 (积分式)。两种模式都用 double 避免 qps × elapsed
        // 溢出 int64 (常量模式与原整数式在非溢出区间内结果一致, 差 < 1 个令牌)。
        const int64_t due = g_ramp.enabled
            ? RampDueTokens(step, elapsed)
            : static_cast<int64_t>(g_test_config.expected_qps
                                   * (static_cast<double>(elapsed) / 1000000000.0));
        const int64_t delta = due - accumulative_token;
        if (delta <= 0) {
            // 额度未到 (降阶梯、或初始令牌预支时也会出现): 不发牌, 也绝不回退记账
            continue;
        }
        int64_t post = delta;
        if (post > kMaxTokensPerTick) {
            // 只可能由进程长时间挂起/暂停触发; 按未截断 delta 记账 = 放弃追赶,
            // 避免把挂起期间攒的额度一次性砸向服务端 (bthread_sem 计数为 unsigned)。
            LOG(WARNING) << "Ramp token catch-up clamped: " << post << " -> "
                         << kMaxTokensPerTick << " (process suspended?)";
            post = kMaxTokensPerTick;
        }
        bthread_sem_post_n(&g_token_sem, static_cast<size_t>(post));
        accumulative_token += delta;   // 注意: 按未截断的 delta 记账
    }

    // 中途停止 (test_seconds 先到): 收尾尚未跑满的当前级, 保证它的统计进得了 JSON
    if (g_ramp.enabled) {
        CloseRampStep(step, butil::gettimeofday_us());
    }
    return NULL;
}

// ==================== 中央发送泵 ====================
// 全局并发模型: queue_depth 为进程级在途请求上限。K 个发送泵共享 g_inflight_sem
// 许可, 按轮询 (RR) 从所有活跃连接中选取连接发送请求; 响应线程在 HandleResponse
// 中释放许可, 泵据此补发, 取代原"每连接响应驱动自续"模型。
// 获取顺序为 许可 → 令牌: 许可由响应释放、按响应节奏逐个到达, 令牌消费因此被
// 响应节奏摊平, 避免令牌批量到账时多个泵同时抢令牌造成发送突发 (尾延迟劣化)。
// 两处等待均为 stop-aware 的 100ms timedwait; 持有许可等令牌超时需归还许可,
// 防止许可泄漏 (占着并发名额不发送)。
static void* SendPump(void* /*arg*/) {
    while (!g_stop.load(butil::memory_order_relaxed)) {
        // 并发: 先等待全局在途许可 (响应驱动)
        struct timespec abstime;
        struct timeval tv;
        gettimeofday(&tv, NULL);
        abstime.tv_sec = tv.tv_sec;
        abstime.tv_nsec = tv.tv_usec * 1000 + 100000000L;
        if (abstime.tv_nsec >= 1000000000L) {
            abstime.tv_sec += 1;
            abstime.tv_nsec -= 1000000000L;
        }
        int rc = bthread_sem_timedwait(&g_inflight_sem, &abstime);
        if (rc != 0) {
            if (g_stop.load(butil::memory_order_relaxed)) break;
            continue;
        }
        // 拿到许可后才发现停止: 直接退出 (许可无需归还, 信号量在下次 Test() 时重建)
        if (g_stop.load(butil::memory_order_relaxed)) {
            break;
        }

        // 限流: 再等待令牌桶令牌 (可选; 固定限速或阶梯 ramp)
        if (IsQpsLimited()) {
            struct timespec abstime2;
            struct timeval tv2;
            gettimeofday(&tv2, NULL);
            abstime2.tv_sec = tv2.tv_sec;
            abstime2.tv_nsec = tv2.tv_usec * 1000 + 100000000L;
            if (abstime2.tv_nsec >= 1000000000L) {
                abstime2.tv_sec += 1;
                abstime2.tv_nsec -= 1000000000L;
            }
            rc = bthread_sem_timedwait(&g_token_sem, &abstime2);
            if (rc != 0) {
                // 超时未拿到令牌: 归还持有的许可后重试, 避免占着并发名额不发送
                bthread_sem_post(&g_inflight_sem);
                if (g_stop.load(butil::memory_order_relaxed)) break;
                continue;
            }
            // 默认丢弃停测后的本发; allow_stop_burst 时保留旧行为继续发。
            if (!g_test_config.allow_stop_burst &&
                g_stop.load(butil::memory_order_relaxed)) {
                break;
            }
            bthread_usleep(butil::fast_rand_less_than(g_test_config.max_random_delay_us));
        }

        // random_delay 期间可能已停测: 默认发送前再查一次; 旧行为跳过此检查。
        if (!g_test_config.allow_stop_burst &&
            g_stop.load(butil::memory_order_relaxed)) {
            break;
        }

        if (g_active_tests == NULL || g_active_tests->empty()) {
            bthread_sem_post(&g_inflight_sem);
            if (IsQpsLimited()) {
                bthread_sem_post(&g_token_sem);
            }
            continue;
        }
        size_t n = g_active_tests->size();
        PerformanceTest* test =
            (*g_active_tests)[(size_t)g_send_index.fetch_add(1, butil::memory_order_relaxed) % n];
        test->SendRequest();
    }
    return NULL;
}

// bthread_concurrency forward declaration (used in main)
namespace bthread {
    DECLARE_int32(bthread_concurrency);
}

// ==================== Server-side StatsPrinter ====================
// Background bthread that periodically prints active connection count
// and prints a final statistics report after the client stops sending
// requests (idle timeout based, no dependency on test_seconds).
static void* StatsPrinter(void* /*arg*/) {
    uint64_t idle_timeout_us = (uint64_t)FLAGS_srv_idle_timeout_seconds * 1000000;

    while (!g_srv_stop.load(butil::memory_order_relaxed)) {
        bthread_usleep(100000);  // 100ms interval

        // Sample process CPU/memory every 100ms, off the RPC hot path.
        const std::string cpu_usage_str = bvar::Variable::describe_exposed("process_cpu_usage");
        const double cpu_percent = atof(cpu_usage_str.c_str()) * 100;
        const double memory_mb = atof(
            bvar::Variable::describe_exposed("process_memory_resident").c_str()) / 1024 / 1024;

        pthread_mutex_lock(&g_srv_metrics_lock);
        g_srv_cpu_usage_str = cpu_usage_str;
        g_srv_cpu_usage_ready.store(true, butil::memory_order_release);
        pthread_mutex_unlock(&g_srv_metrics_lock);

        g_srv_cpu_recorder << cpu_percent;
        g_srv_memory_recorder << memory_mb;

        // ---- Per-100ms: print active connection count ----
        // Use printf for compact per-100ms output (LOG(INFO) would be too verbose)
        // 多端口时在同一行追加各端口明细 —— 不要每端口一行, 那会把 100ms 一条变成 N 条。
        {
            std::vector<size_t> per_port;
            size_t total_conns = 0;
            CollectPortConnections(&per_port, &total_conns);
            if (g_srv_multi_port) {
                std::string detail;
                for (size_t i = 0; i < g_srv_ports.size(); ++i) {
                    if (i) detail.push_back(',');
                    detail += std::to_string(g_srv_ports[i]->port);
                    detail.push_back(':');
                    detail += std::to_string(per_port[i]);
                }
                printf("[srv-conn] active_connections=%zu ports=%s\n",
                       total_conns, detail.c_str());
            } else {
                printf("[srv-conn] active_connections=%zu\n", total_conns);
            }
        }
        fflush(stdout);  // Required: stdout is fully-buffered when not a TTY

        uint64_t now = butil::monotonic_time_us();
        uint64_t last_req = g_srv_last_request_time.load(butil::memory_order_relaxed);
        uint64_t end = g_srv_end_time.load(butil::memory_order_relaxed);
        uint64_t start_time = g_srv_start_time.load(butil::memory_order_relaxed);

        // Report when: (1) we've received at least one request,
        // (2) no report has been printed yet, and
        // (3) no new request arrived for idle_timeout_us
        if (end == 0 && start_time > 0 && last_req > 0 &&
            now - last_req >= idle_timeout_us) {
            uint64_t end_expected = 0;
            if (g_srv_end_time.compare_exchange_strong(end_expected, last_req)) {
                double duration_s = (last_req - start_time) / 1000000.0;
                uint64_t total_cnt = g_srv_total_cnt.load(butil::memory_order_relaxed);
                uint64_t total_bytes = g_srv_total_bytes.load(butil::memory_order_relaxed);
                // Clear per-round counters right after capture so the next round
                // starts from zero. Clearing them in Test() instead would race
                // with concurrent first requests of the new round and could
                // clobber their increments.
                g_srv_total_cnt.store(0, butil::memory_order_relaxed);
                g_srv_total_bytes.store(0, butil::memory_order_relaxed);
                // 长尾计数必须与 total_cnt 在同一处清零: 换到别处会与并发首请求竞态
                const bool tail_on = (FLAGS_srv_tail_permille > 0);
                uint64_t tail_cnt = 0;
                if (tail_on) {
                    tail_cnt = g_srv_tail_cnt.load(butil::memory_order_relaxed);
                    g_srv_tail_cnt.store(0, butil::memory_order_relaxed);
                }
                // 每端口计数同样"捕获即清零", 且必须与上面的汇总清零在同一处 (同一理由:
                // 并发首请求会与清零竞态)。tail_seq 不清零 —— 它是"到达序号"而非"每轮计数",
                // 跨轮重置相位反而会让每轮首批请求都命中长尾。
                std::vector<uint64_t> port_cnt(g_srv_ports.size(), 0);
                std::vector<uint64_t> port_bytes(g_srv_ports.size(), 0);
                std::vector<uint64_t> port_tail(g_srv_ports.size(), 0);
                for (size_t i = 0; i < g_srv_ports.size(); ++i) {
                    port_cnt[i] = g_srv_ports[i]->total_cnt.load(butil::memory_order_relaxed);
                    port_bytes[i] = g_srv_ports[i]->total_bytes.load(butil::memory_order_relaxed);
                    g_srv_ports[i]->total_cnt.store(0, butil::memory_order_relaxed);
                    g_srv_ports[i]->total_bytes.store(0, butil::memory_order_relaxed);
                    if (tail_on) {
                        port_tail[i] = g_srv_ports[i]->tail_cnt.load(butil::memory_order_relaxed);
                        g_srv_ports[i]->tail_cnt.store(0, butil::memory_order_relaxed);
                    }
                }

                LOG(INFO) << "========== Server Final Report ==========";
                LOG(INFO) << "Idle for " << FLAGS_srv_idle_timeout_seconds
                          << "s, test round considered complete.";
                LOG(INFO) << "Duration: " << duration_s << "s";
                LOG(INFO) << "Total Requests: " << total_cnt;
                if (duration_s > 0) {
                    LOG(INFO) << "QPS: " << total_cnt / duration_s;
                    LOG(INFO) << "Bandwidth: " << total_bytes / 1024.0 / 1024.0 / duration_s << "MB/s";
                }
                LOG(INFO) << "Latency(avg/p99/p999/p9999): "
                          << g_srv_latency_recorder.latency(10) << "/"
                          << g_srv_latency_recorder.latency_percentile(0.99) << "/"
                          << g_srv_latency_recorder.latency_percentile(0.999) << "/"
                          << g_srv_latency_recorder.latency_percentile(0.9999) << "us"
                          << (tail_on ? " (non-tail)" : "");
                if (tail_on) {
                    LOG(INFO) << "Tail Requests: " << tail_cnt << "/" << total_cnt
                              << " (" << (total_cnt > 0 ? tail_cnt * 100.0 / total_cnt : 0.0)
                              << "%)  [delay=" << FLAGS_srv_tail_delay_us << "us"
                              << (FLAGS_srv_tail_delay_max_us > FLAGS_srv_tail_delay_us
                                      ? "-" + std::to_string(FLAGS_srv_tail_delay_max_us) + "us"
                                      : "")
                              << ", mode=" << (FLAGS_srv_tail_cpu ? "cpu" : "sleep") << "]";
                    LOG(INFO) << "Tail Latency(avg/p99/p999/p9999): "
                              << g_srv_tail_latency_recorder.latency(10) << "/"
                              << g_srv_tail_latency_recorder.latency_percentile(0.99) << "/"
                              << g_srv_tail_latency_recorder.latency_percentile(0.999) << "/"
                              << g_srv_tail_latency_recorder.latency_percentile(0.9999) << "us";
                }
                LOG(INFO) << "CPU(avg/max): "
                          << g_srv_cpu_recorder.latency(10) << "/"
                          << g_srv_cpu_recorder.max_latency() << "%";
                LOG(INFO) << "Memory(avg/max): "
                          << g_srv_memory_recorder.latency(10) << "/"
                          << g_srv_memory_recorder.max_latency() << "MB";
                // 分端口展开 (仅在多端口时输出: 单端口下端口视图就等于上面的汇总视图,
                // 重复打印只会让人怀疑两者为什么一样)。
                if (g_srv_multi_port) {
                    LOG(INFO) << "---------- Per-port (" << g_srv_ports.size() << ") ----------";
                    for (size_t i = 0; i < g_srv_ports.size(); ++i) {
                        const bvar::LatencyRecorder* lr = g_srv_ports[i]->latency;
                        // 所有端口共用同一个 duration_s (进程级): 保证
                        // Σ per-port QPS == 汇总 QPS、Σ share == 100% 精确成立。
                        // 各端口自己的"首请求到末请求"时长口径不同, 会破坏可加性。
                        // 数值直接 << 到日志流 (与汇总段同一套格式化), 不要先 to_string
                        // 再拼字符串 —— 那会把 double 打印成 6 位小数的定长形式。
                        const double qps = (duration_s > 0) ? port_cnt[i] / duration_s : 0.0;
                        const double bw = (duration_s > 0)
                            ? port_bytes[i] / 1048576.0 / duration_s : 0.0;
                        const double share = (total_cnt > 0)
                            ? port_cnt[i] * 100.0 / total_cnt : 0.0;
                        LOG(INFO) << "[" << g_srv_ports[i]->port << "] Requests: " << port_cnt[i]
                                  << " (" << share << "%)"
                                  << "  QPS: " << qps
                                  << "  Bandwidth: " << bw << "MB/s"
                                  << "  Latency(avg/p99/p999/p9999): "
                                  << lr->latency(10) << "/"
                                  << lr->latency_percentile(0.99) << "/"
                                  << lr->latency_percentile(0.999) << "/"
                                  << lr->latency_percentile(0.9999) << "us"
                                  << (tail_on ? " (non-tail)" : "");
                    }
                    if (tail_on) {
                        LOG(INFO) << "---------- Per-port tail ----------";
                        for (size_t i = 0; i < g_srv_ports.size(); ++i) {
                            const bvar::LatencyRecorder* tr = g_srv_ports[i]->tail_latency;
                            LOG(INFO) << "[" << g_srv_ports[i]->port << "] Tail: "
                                      << port_tail[i] << "/" << port_cnt[i]
                                      << " (" << (port_cnt[i] > 0
                                                  ? port_tail[i] * 100.0 / port_cnt[i] : 0.0)
                                      << "%)  TailLatency(avg/p99/p999/p9999): "
                                      << tr->latency(10) << "/"
                                      << tr->latency_percentile(0.99) << "/"
                                      << tr->latency_percentile(0.999) << "/"
                                      << tr->latency_percentile(0.9999) << "us";
                        }
                    }
                }
                LOG(INFO) << "==========================================";
                return nullptr;
            }
        }
    }
    return nullptr;
}

// ==================== PerfTestServiceImpl (from server.cpp) ====================
// Handles incoming RPC Test requests from client.  This is the server-side
// counterpart that executes inside the same process when srv_enable=true.
namespace test {
class PerfTestServiceImpl : public PerfTestService {
public:
    // stats: 本端口专属的统计槽位, 由 main 在构造期绑定, 之后只读。
    // 每端口一个 service 实例 (而非共享一个 + 运行期查表): 端口↔统计的绑定成为构造期
    // 不变量, 热路径零查表/零 NULL 判断, 且不依赖"某协议是否设置了 Controller::server()"
    // 这一实现细节。代价只是 N 个 16 字节对象。
    explicit PerfTestServiceImpl(SrvPort* stats) : _stats(stats) {}
    ~PerfTestServiceImpl() {}

    SrvPort* port_stats() const { return _stats; }

    void Test(google::protobuf::RpcController* cntl_base,
              const PerfTestRequest* request,
              PerfTestResponse* response,
              google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);
        uint64_t now = butil::monotonic_time_us();

        // ---- 模拟业务逻辑: 每请求忙等 CPU 计算 (srv_busy_us 指定时长, 0=零开销) ----
        if (FLAGS_srv_busy_us > 0) {
            SimulateBusinessLogic(FLAGS_srv_busy_us);
        }

        // ---- 模拟业务逻辑: Embedding 查询 (内存随机读写) ----
        // srv_embedding_enabled 开启时, 每请求对大内存表做随机读+写,
        // 模拟 AI 推理中的 embedding 检索 (见 SimulateEmbeddingLookup 注释)。
        SimulateEmbeddingLookup();

        // ---- 模拟长尾/重尾请求: 按到达序号确定性等间隔命中, 注入额外延迟 ----
        // 位置与 srv_busy_us / embedding 一致 (属 B 自身业务处理耗时, 在 proxy 转发之前
        // 完成): 转发场景下 A 观测的端到端延迟自然包含它, 而 B→C 转发延迟统计不含它;
        // 同理 fan-out 只放大转发次数, 长尾每入站请求最多注入一次。
        // 标记必须在这里 set: ProxyForwardOnce 只 set_name(), 不会覆盖 response, 标记能
        // 存活到序列化; 若改到转发之后注入并依赖转发响应覆盖, 反而会丢标记。
        // 建连探针 (客户端首包) 永不命中长尾, 且**不占用到达序号** (短路求值保证不 fetch_add)
        // —— 长尾比例因此在"非探针请求"上仍是精确等间隔, 建连阶段也完全不受人工长尾干扰。
        // 到达序号取本端口槽位: 全局序号会与客户端"连接按端口轮询"锁步, 让部分端口
        // 恒 0%/恒 100% 命中 (详见 SrvPort::tail_seq 注释)。
        const bool is_tail = (FLAGS_srv_tail_permille > 0) && !request->is_probe() &&
            TailHit(_stats->tail_seq.fetch_add(1, butil::memory_order_relaxed));
        if (is_tail) {
            InjectTailDelay(PickTailDelayUs());
            _stats->tail_cnt.fetch_add(1, butil::memory_order_relaxed);   // 本端口
            g_srv_tail_cnt.fetch_add(1, butil::memory_order_relaxed);     // 进程汇总
            // 仅命中时 set: proto2 下显式 set(false) 也会上线 1 字节; response 每请求新建,
            // 不会残留上一请求的字段。
            response->set_is_tail(true);
        }

        // New round: the previous round was finalized (StatsPrinter has exited
        // after printing its final report). Reset the round-marker state so the
        // CAS below can relaunch StatsPrinter for this round. Per-round counters
        // (g_srv_total_cnt/g_srv_total_bytes) are cleared by StatsPrinter right
        // after the final report — clearing them here would race with concurrent
        // first requests and could lose increments.
        if (g_srv_end_time.load(butil::memory_order_relaxed) != 0) {
            g_srv_end_time.store(0, butil::memory_order_relaxed);
            g_srv_start_time.store(0, butil::memory_order_relaxed);
            g_srv_last_request_time.store(0, butil::memory_order_relaxed);
        }

        // Record start time on first request of a round and launch stats printer
        // (arg 未被 StatsPrinter 使用, 它遍历全局 g_srv_ports; 多端口只起一个统计打印器,
        //  与"一个进程 = 一个被测目标 = 一份轮次报告"的模型一致)
        uint64_t expected = 0;
        if (g_srv_start_time.compare_exchange_strong(expected, now)) {
            bthread_t tid;
            bthread_start_background(&tid, &BTHREAD_ATTR_NORMAL, StatsPrinter, NULL);
        }
        g_srv_last_request_time.store(now, butil::memory_order_relaxed);
        if (is_tail) {
            // now 是在注入延迟之前取的时间戳, 上面的 store 会把长尾自身的时长算进"空闲"
            // 窗口 —— 长尾延迟接近 srv_idle_timeout_seconds 时, StatsPrinter 可能在长尾
            // 请求仍在处理时就宣告本轮结束。这里用当前时间覆盖, 把长尾时长排除出空闲判定。
            g_srv_last_request_time.store(butil::monotonic_time_us(),
                                          butil::memory_order_relaxed);
        }

        if (g_srv_cpu_usage_ready.exchange(false, butil::memory_order_acq_rel)) {
            pthread_mutex_lock(&g_srv_metrics_lock);
            response->set_cpu_usage(g_srv_cpu_usage_str);
            pthread_mutex_unlock(&g_srv_metrics_lock);
        } else {
            response->set_cpu_usage("");
        }

        brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);

        // ---- Proxy 转发模式 (A→B→C) ----
        // 配置了后端列表时, 在服务 bthread 内同步转发到 ServerC (round-robin),
        // 透传 name/attachment; A 观测延迟自然包含 B→C RTT。
        // B 自身 cpu_usage 已在上方填入并原样返回 A; C 的 cpu_usage 仅供代理统计;
        // A→B 侧各项统计 (下方 tail) 不变。
        const bool fwd_ok = ProxyForwardOnce(cntl, request, response);
        if (!fwd_ok) {
            // srv_rsp_size=0 时回显请求 name ,否则返回预生成的填充 name
            if (FLAGS_srv_rsp_size == 0) {
                response->set_name(request->name());
            } else {
                response->set_name(g_srv_rsp_name);
            }
        }

        // Echo back the request attachment if requested (转发模式下与 C 回显内容一致)
        // 字节数双写: 本端口槽位 + 进程汇总 (既有 g_srv_total_bytes 语义不变)。
        if (request->echo_attachment()) {
            cntl->response_attachment().append(cntl->request_attachment());
            const uint64_t n = cntl->response_attachment().size();
            _stats->total_bytes.fetch_add(n, butil::memory_order_relaxed);
            g_srv_total_bytes.fetch_add(n, butil::memory_order_relaxed);
        } else {
            const uint64_t n = response->name().size();
            _stats->total_bytes.fetch_add(n, butil::memory_order_relaxed);
            g_srv_total_bytes.fetch_add(n, butil::memory_order_relaxed);
        }

        _stats->total_cnt.fetch_add(1, butil::memory_order_relaxed);
        g_srv_total_cnt.fetch_add(1, butil::memory_order_relaxed);
        // 长尾请求计入独立 recorder: 主 recorder 只反映非长尾 (即系统自身) 的延迟,
        // 注入的长尾不会污染它。注意 cntl->latency_us() 在本函数内恒为"收到报文至今"
        // (服务端 _end_time_us 仍为 UNSET), 因此注入的延迟一定被计入。
        // 每端口 recorder 只在多端口时写 (单端口下不创建, 见 g_srv_multi_port):
        // 单端口时汇总视图就是该端口的视图, 不必也不能再多写一份。
        const int64_t lat = cntl->latency_us();
        if (is_tail) {
            g_srv_tail_latency_recorder << lat;                       // 汇总 (名字不变)
            if (g_srv_multi_port) { *_stats->tail_latency << lat; }   // 本端口
        } else {
            g_srv_latency_recorder << lat;                            // 汇总 (名字不变)
            if (g_srv_multi_port) { *_stats->latency << lat; }        // 本端口
        }
    }

private:
    SrvPort* const _stats;   // 本端口统计槽位, 构造期绑定, 之后只读
};
}  // namespace test

// ==================== Stats helpers ====================
static int64_t CalcPercentile(const std::vector<int64_t>& latencies, double percentile) {
    if (latencies.empty()) return 0;
    std::vector<int64_t> sorted = latencies;
    std::sort(sorted.begin(), sorted.end());
    size_t idx = static_cast<size_t>(sorted.size() * percentile);
    if (idx >= sorted.size()) idx = sorted.size() - 1;
    return sorted[idx];
}

struct LatencyStats {
    int64_t avg = 0;
    int64_t min_val = INT64_MAX;
    int64_t max_val = 0;
    int64_t p50 = 0;
    int64_t p90 = 0;
    int64_t p99 = 0;
    int64_t p999 = 0;
    int64_t p9999 = 0;

    void Calculate(const std::vector<int64_t>& latencies) {
        if (latencies.empty()) { min_val = 0; return; }
        for (auto lat : latencies) {
            avg += lat;
            if (lat > max_val) max_val = lat;
            if (lat < min_val) min_val = lat;
        }
        avg /= latencies.size();
        p50 = CalcPercentile(latencies, 0.5);
        p90 = CalcPercentile(latencies, 0.9);
        p99 = CalcPercentile(latencies, 0.99);
        p999 = CalcPercentile(latencies, 0.999);
        p9999 = CalcPercentile(latencies, 0.9999);
    }

    void Print(const std::string& name) const {
        std::cout << name << "(avg/min/max/p99/p999/p9999): ";
        if (min_val != 0 || max_val != 0) {
            std::cout << avg << "/" << min_val << "/" << max_val << "/" << p99 << "/" << p999 << "/" << p9999 << "us";
        } else {
            std::cout << "N/A";
        }
    }
};

// ==================== Print latency stats (from client.cpp) ====================
static void PrintLatencyStats() {
    LatencyStats connect_stats, first_rpc_stats;
    connect_stats.Calculate(g_connect_latencies_us);
    first_rpc_stats.Calculate(g_first_rpc_latencies_us);

    connect_stats.Print("Channel-Init-Latency");
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

// ==================== Batch connection establishment (bthread-pool based) ====================
// A fixed number of worker bthreads (= thread_pool_size) process connection
// establishment per batch; each worker pulls work via an atomic counter.
// record_latency: false skips latency recording (used during warmup phase).
static int BatchEstablishConnections(
    int total_connections,
    int attachment_size,
    std::vector<PerformanceTest*>& tests,
    bool record_latency = true,
    uint64_t* out_duration_us = nullptr) {

    int batch = (g_test_config.batch_size > 0) ? g_test_config.batch_size : total_connections;
    int pool_size = g_test_config.thread_pool_size;
    std::cout << "Initializing connections with bthread pool (pool_size="
              << pool_size << "), batch size=" << batch << "..." << std::endl;

    uint64_t start_time = butil::gettimeofday_us();
    butil::atomic<int> work_index(0);

    for (int batch_start = 0; batch_start < total_connections; batch_start += batch) {
        int batch_end = std::min(batch_start + batch, total_connections);
        int current_batch_size = batch_end - batch_start;
        int batch_index = batch_start / batch + 1;
        int num_workers = std::min(pool_size, current_batch_size);

        std::vector<bthread_t> tids(num_workers);
        work_index.store(batch_start, butil::memory_order_relaxed);

        PoolWorkArg pool_arg = {batch_end, attachment_size, &tests, &work_index};

        for (int k = 0; k < num_workers; ++k) {
            bthread_start_background(&tids[k], &BTHREAD_ATTR_NORMAL, PoolWorkerFunc, &pool_arg);
        }

        std::cout << "Batch " << batch_index << ": " << current_batch_size
                  << " connections, " << num_workers << " workers"
                  << (g_test_config.batch_interval_ms > 0 && batch_end < total_connections
                      ? ", sleep " + std::to_string(g_test_config.batch_interval_ms) + "ms" : "")
                  << std::endl;

        for (int k = 0; k < num_workers; ++k) {
            bthread_join(tids[k], NULL);
        }

        if (g_test_config.batch_interval_ms > 0 && batch_end < total_connections) {
            std::this_thread::sleep_for(std::chrono::milliseconds(g_test_config.batch_interval_ms));
        }
    }

    // Collect results
    int total_success = 0;
    for (int k = 0; k < total_connections; ++k) {
        if (tests[k]->init_result() == 0) {
            total_success++;
            if (record_latency) {
                pthread_mutex_lock(&g_latency_mutex);
                g_connect_latencies_us.push_back(tests[k]->connect_latency_us());
                g_first_rpc_latencies_us.push_back(tests[k]->first_rpc_latency_us());
                g_first_rpc_memory_mbs.push_back(tests[k]->first_rpc_memory_mb());
                pthread_mutex_unlock(&g_latency_mutex);
            }
        }
    }

    uint64_t end_time = butil::gettimeofday_us();
    uint64_t total_duration_us = end_time - start_time;
    if (out_duration_us) {
        *out_duration_us = total_duration_us;
    }
    std::cout << "Total: " << total_success << "/" << total_connections << " connections established in "
              << total_duration_us << "us" << std::endl;
    return total_success;
}

// ==================== Reset all global stats ====================
static void ResetGlobalStats() {
    // Destroy old LatencyRecorders (destructor calls hide() to remove from /vars)
    delete g_latency_recorder;
    delete g_server_cpu_recorder;
    delete g_client_cpu_recorder;
    delete g_client_memory_recorder;
    delete g_client_tail_recorder;

    // Create fresh LatencyRecorders with versioned exposure names.
    // Window size: 显式指定 latency_window_seconds 时使用该值,
    // 否则自适应 test_seconds (各快照槽位预分配约 33KB, 匹配实际时长避免浪费)。
    // Ceiling: 3600s (MAX_SECONDS_LIMIT); test_seconds=0 → max 3600s.
    time_t window_size;
    if (g_test_config.latency_window_seconds > 0) {
        window_size = std::min((time_t)g_test_config.latency_window_seconds, (time_t)3600);
    } else {
        window_size = g_test_config.test_seconds > 0
            ? std::min((time_t)g_test_config.test_seconds, (time_t)3600)
            : 3600;
    }
    LOG(INFO) << "LatencyRecorder window_size=" << window_size << "s"
              << " (latency_window_seconds=" << g_test_config.latency_window_seconds
              << ", test_seconds=" << g_test_config.test_seconds << ")";
    g_run_index++;
    char name_buf[64];
    snprintf(name_buf, sizeof(name_buf), "run%d_client", g_run_index);
    g_latency_recorder = new bvar::LatencyRecorder(name_buf, window_size);
    snprintf(name_buf, sizeof(name_buf), "run%d_server_cpu", g_run_index);
    g_server_cpu_recorder = new bvar::LatencyRecorder(name_buf, window_size);
    snprintf(name_buf, sizeof(name_buf), "run%d_client_cpu", g_run_index);
    g_client_cpu_recorder = new bvar::LatencyRecorder(name_buf, window_size);
    snprintf(name_buf, sizeof(name_buf), "run%d_client_memory", g_run_index);
    g_client_memory_recorder = new bvar::LatencyRecorder(name_buf, window_size);
    snprintf(name_buf, sizeof(name_buf), "run%d_client_tail", g_run_index);
    g_client_tail_recorder = new bvar::LatencyRecorder(name_buf, window_size);

    // Reset latency min/max trackers
    g_latency_min.store(INT64_MAX, butil::memory_order_relaxed);
    g_latency_max.store(0, butil::memory_order_relaxed);
    g_tail_req_cnt.store(0, butil::memory_order_relaxed);
    g_tail_lat_min.store(INT64_MAX, butil::memory_order_relaxed);
    g_tail_lat_max.store(0, butil::memory_order_relaxed);

    pthread_mutex_lock(&g_latency_mutex);
    g_connect_latencies_us.clear();
    g_first_rpc_latencies_us.clear();
    g_first_rpc_memory_mbs.clear();
    pthread_mutex_unlock(&g_latency_mutex);

    g_total_bytes.store(0, butil::memory_order_relaxed);
    g_all_cnt.store(0, butil::memory_order_relaxed);
    g_total_cnt.store(0, butil::memory_order_relaxed);
    g_total_error_cnt.store(0, butil::memory_order_relaxed);
    g_last_time.store(0, butil::memory_order_relaxed);
    // 错误日志节流计数器跨轮清零, 防止上一轮 suppressed 计数泄漏到本轮日志
    g_last_error_log_time_us.store(0, butil::memory_order_relaxed);
    g_suppressed_error_logs.store(0, butil::memory_order_relaxed);
    rr_index.store(0, butil::memory_order_relaxed);
    g_stop.store(false, butil::memory_order_relaxed);

    // 阶梯状态: 当前级归零 + 防御性释放上一轮残留的统计块 (正常路径已在
    // RunPerformanceTest 的 join 之后释放)。注意这里不新建任何 bvar。
    g_cur_step.store(0, butil::memory_order_relaxed);
    FreeRampStepStatsIfAny();
}

// ==================== Compute results from global stats ====================
static void ComputeResults(TaskResult* result, uint64_t start_time_us, uint64_t end_time_us,
                            int total_rounds,
                            int expect_conn_count, int established_conn_count,
                            int64_t establish_conn_time) {
    result->duration_s = (end_time_us - start_time_us) / 1000000.0;
    result->total_requests = g_total_cnt.load(butil::memory_order_relaxed);
    result->total_errors = g_total_error_cnt.load(butil::memory_order_relaxed);
    result->total_sent = g_all_cnt.load(butil::memory_order_relaxed);

    if (result->total_requests > 0) {
        result->error_rate = result->total_errors * 100.0 / result->total_requests;
    }
    if (result->duration_s > 0) {
        result->qps = result->total_requests / result->duration_s;
        result->throughput_mb_s = g_total_bytes.load(butil::memory_order_relaxed)
                                  / 1048576.0 / result->duration_s;
    }

    // Latency — read from bvar::LatencyRecorder (reservoir-sampled percentiles)
    // Note: latency_percentile() combines all Percentile snapshots across the full
    // window history, providing statistically robust percentiles for the entire run.
    result->latency_avg = g_latency_recorder->latency();
    result->latency_min = g_latency_min.load(butil::memory_order_relaxed);
    if (result->latency_min == INT64_MAX) result->latency_min = 0;
    result->latency_max = g_latency_max.load(butil::memory_order_relaxed);
    result->latency_p50 = g_latency_recorder->latency_percentile(0.5);
    result->latency_p90 = g_latency_recorder->latency_percentile(0.9);
    result->latency_p99 = g_latency_recorder->latency_percentile(0.99);
    result->latency_p999 = g_latency_recorder->latency_percentile(0.999);
    result->latency_p9999 = g_latency_recorder->latency_percentile(0.9999);

    // Long-tail — 完全独立的口径 (服务端注入的长尾请求, 不进上面的 latency_*)
    result->tail_requests = g_tail_req_cnt.load(butil::memory_order_relaxed);
    result->tail_latency_avg = g_client_tail_recorder->latency();
    result->tail_latency_min = g_tail_lat_min.load(butil::memory_order_relaxed);
    if (result->tail_latency_min == INT64_MAX) result->tail_latency_min = 0;
    result->tail_latency_max = g_tail_lat_max.load(butil::memory_order_relaxed);
    result->tail_latency_p50 = g_client_tail_recorder->latency_percentile(0.5);
    result->tail_latency_p90 = g_client_tail_recorder->latency_percentile(0.9);
    result->tail_latency_p99 = g_client_tail_recorder->latency_percentile(0.99);
    result->tail_latency_p999 = g_client_tail_recorder->latency_percentile(0.999);
    result->tail_latency_p9999 = g_client_tail_recorder->latency_percentile(0.9999);

    // Server CPU — sampled values stored as percent * 100
    if (g_server_cpu_recorder->count() > 0) {
        result->server_cpu_avg = g_server_cpu_recorder->latency() / 100.0;
        result->server_cpu_max = g_server_cpu_recorder->max_latency() / 100.0;
    }

    // Client CPU
    if (g_client_cpu_recorder->count() > 0) {
        result->client_cpu_avg = g_client_cpu_recorder->latency() / 100.0;
        result->client_cpu_max = g_client_cpu_recorder->max_latency() / 100.0;
    }

    // Client memory — stored in KB, convert to MB for output
    if (g_client_memory_recorder->count() > 0) {
        result->client_memory_avg_mb = g_client_memory_recorder->latency() / 1024.0;
        result->client_memory_max_mb = g_client_memory_recorder->max_latency() / 1024.0;
    }

    // Connect latency
    pthread_mutex_lock(&g_latency_mutex);
    std::vector<int64_t> connect_lats = g_connect_latencies_us;
    std::vector<int64_t> first_rpc_lats = g_first_rpc_latencies_us;
    std::vector<double> first_rpc_mems = g_first_rpc_memory_mbs;
    pthread_mutex_unlock(&g_latency_mutex);

    LatencyStats cls;
    cls.Calculate(connect_lats);
    result->connect_avg = cls.avg;
    result->connect_min = cls.min_val;
    result->connect_max = cls.max_val;
    result->connect_p50 = cls.p50;
    result->connect_p90 = cls.p90;
    result->connect_p99 = cls.p99;
    result->connect_p999 = cls.p999;
    result->connect_p9999 = cls.p9999;

    LatencyStats frs;
    frs.Calculate(first_rpc_lats);
    result->first_rpc_avg = frs.avg;
    result->first_rpc_min = frs.min_val;
    result->first_rpc_max = frs.max_val;
    result->first_rpc_p50 = frs.p50;
    result->first_rpc_p90 = frs.p90;
    result->first_rpc_p99 = frs.p99;
    result->first_rpc_p999 = frs.p999;
    result->first_rpc_p9999 = frs.p9999;

    // First RPC memory
    if (!first_rpc_mems.empty()) {
        double sum = 0, min_val = first_rpc_mems[0], max_val = first_rpc_mems[0];
        for (auto m : first_rpc_mems) {
            sum += m;
            if (m > max_val) max_val = m;
            if (m < min_val) min_val = m;
        }
        result->first_rpc_memory_avg_mb = sum / first_rpc_mems.size();
        result->first_rpc_memory_min_mb = min_val;
        result->first_rpc_memory_max_mb = max_val;
    }

    result->total_rounds = total_rounds;

    result->expect_conn_count = expect_conn_count;
    result->established_conn_count = established_conn_count;
    result->establish_conn_time = establish_conn_time;
}

// ==================== Run performance test (central send-pump model) ====================
// 全部连接一次性激活, queue_depth 为进程级在途并发上限,
// 停止条件为全局 test_iterations (总完成请求数) / test_seconds (压测时长)。
// 清理顺序 (防齐射 + 防“服务端还在回、客户端已退”):
// g_stop 置位且不批量补 token → 泵见 stop 后不再新发 →
// 已在途请求全部 HandleResponse (pending 归零, Channel 仍存活) →
// join 泵/令牌线程 → 调用方再 delete 连接对象。
static void RunPerformanceTest(std::vector<PerformanceTest*>& success_tests,
                               RampReport* ramp_out) {
    std::cout << "Starting performance test: " << success_tests.size() << " connections, "
              << "global queue_depth=" << g_test_config.queue_depth
              << ", duration " << g_test_config.test_seconds << "s"
              << (g_test_config.test_iterations > 0
                  ? ", iterations " + std::to_string(g_test_config.test_iterations) : "")
              << (g_ramp.enabled
                  ? ", ramp=" + std::to_string(g_ramp.num_steps()) + " steps x "
                        + std::to_string(g_ramp.secs_per_step) + "s, max "
                        + std::to_string(g_ramp.max_target_qps) + " qps"
                  : "")
              << std::endl;

    g_active_tests = &success_tests;
    g_perf_start_time = butil::gettimeofday_us();
    g_pending_req_count.store(0, butil::memory_order_relaxed);
    g_send_index.store(0, butil::memory_order_relaxed);
    g_stop.store(false, butil::memory_order_relaxed);

    // 阶梯统计块: 在启动任何发送/令牌线程之前一次性分配 (bthread 启动自带 happens-before),
    // 运行期只改内容不改集合; join 之后才释放 (响应线程持有裸指针)。
    g_cur_step.store(0, butil::memory_order_relaxed);
    if (g_ramp.enabled) {
        AllocRampStepStats();
        if (!g_step_stats.empty()) {
            g_step_stats[0]->start_us = g_perf_start_time;
        }
    }

    // 启动令牌桶生成线程（如果需要限流）
    bthread_t token_tid = INVALID_BTHREAD;
    if (IsQpsLimited()) {
        bthread_start_background(&token_tid, &BTHREAD_ATTR_NORMAL, GenerateToken, NULL);
    }

    // 启动中央发送泵 (默认个数 = min(queue_depth, 16), 可用 sender_thread_num 覆盖)
    int pump_num = g_test_config.sender_thread_num > 0
        ? std::min(g_test_config.sender_thread_num, g_test_config.queue_depth)
        : std::min(g_test_config.queue_depth, 16);
    g_pump_num.store(pump_num, butil::memory_order_relaxed);
    std::vector<bthread_t> pump_tids(pump_num);
    for (int i = 0; i < pump_num; ++i) {
        bthread_start_background(&pump_tids[i], &BTHREAD_ATTR_NORMAL, SendPump, NULL);
    }

    // 必须等 pending 归零再往下拆泵/连接: 服务端可能仍在回最后一批包,
    // 若此处提前退出会导致对端写已关闭连接 → connection timeout。
    while (!(g_stop.load(butil::memory_order_relaxed) &&
             g_pending_req_count.load(butil::memory_order_relaxed) == 0)) {
        bthread_usleep(10000);
    }
    uint64_t end_time = butil::gettimeofday_us();
    uint64_t start_time = g_perf_start_time;

    // 回收发送泵与令牌线程 (泵已退出, join 保证释放连接前不再有发送路径)
    for (int i = 0; i < pump_num; ++i) {
        bthread_join(pump_tids[i], NULL);
    }
    if (IsQpsLimited() && token_tid != INVALID_BTHREAD) {
        bthread_join(token_tid, NULL);
    }
    g_active_tests = NULL;

    // 阶梯收尾: 此刻既无发送方也无响应回调在跑 (pending 已归零), 由同一份原子计数
    // 重算所有已启动级的最终 summary (实时行只是边界处的即时快照), 再释放统计块。
    if (ramp_out != NULL) {
        ramp_out->Clear();
        ramp_out->enabled = g_ramp.enabled;
        ramp_out->from_list = g_ramp.from_list;
        ramp_out->total_steps = g_ramp.num_steps();
        ramp_out->max_target_qps = g_ramp.max_target_qps;
        ramp_out->secs_per_step = g_ramp.secs_per_step;
        const uint64_t summary_us = butil::gettimeofday_us();
        for (int i = 0; i < static_cast<int>(g_step_stats.size()); ++i) {
            if (g_step_stats[i]->start_us == 0) break;   // 该级从未启动
            ramp_out->steps.push_back(ComputeStepSummary(*g_step_stats[i], i, summary_us));
        }
        ramp_out->reported_steps = static_cast<int>(ramp_out->steps.size());
        ramp_out->truncated = ramp_out->reported_steps < ramp_out->total_steps;
    }
    FreeRampStepStatsIfAny();

    double throughput = g_total_bytes / 1.048576 / (end_time - start_time);

    std::cout << "Avg-Latency: " << g_latency_recorder->latency()
        << ", 50th-Latency: " << g_latency_recorder->latency_percentile(0.5)
        << ", 90th-Latency: " << g_latency_recorder->latency_percentile(0.9)
        << ", 99th-Latency: " << g_latency_recorder->latency_percentile(0.99)
        << ", 99.9th-Latency: " << g_latency_recorder->latency_percentile(0.999)
        << ", 99.99th-Latency: " << g_latency_recorder->latency_percentile(0.9999)
        << ", Max-Latency: " << g_latency_max.load(butil::memory_order_relaxed)
        << ", Throughput: " << throughput << "MB/s"
        << ", QPS: " << (g_total_cnt.load(butil::memory_order_relaxed) * 1000 * 1000 / (end_time - start_time))
        << ", Total Sent: " << g_all_cnt.load(butil::memory_order_relaxed)
        << ", Total Errors: " << g_total_error_cnt.load(butil::memory_order_relaxed)
        << ", Total Requests: " << g_total_cnt.load(butil::memory_order_relaxed)
        << ", Error rate: "
        << ((g_total_cnt.load(butil::memory_order_relaxed) > 0)
            ? (g_total_error_cnt.load(butil::memory_order_relaxed) * 100.0 / g_total_cnt.load(butil::memory_order_relaxed))
            : 0.0) << "%";
    std::cout << std::endl;

}

// ==================== Main test execution (runs in bthread) ====================
static void* ExecuteTest(void* /*arg*/) {
    // g_test_config and g_servers are already populated by start() handler

    uint64_t test_start_us = butil::gettimeofday_us();

    // Reset global stats for this run
    ResetGlobalStats();

    if (g_servers.empty()) {
        pthread_mutex_lock(&g_task_mutex);
        g_task.status = TASK_FAILED;
        g_task.error_message = "No servers provided";
        pthread_mutex_unlock(&g_task_mutex);
        return NULL;
    }

    int thread_num = GetConnectionCount();
    int attachment_size = g_test_config.attachment_size;

    // 解析多 size 请求负载 (req_size 逗号分隔), 生成 g_req_sizes/g_names
    std::string parse_err;
    if (ParseReqSizes(g_test_config.req_size, &parse_err) != 0) {
        pthread_mutex_lock(&g_task_mutex);
        g_task.status = TASK_FAILED;
        g_task.error_message = "Invalid req_size: " + parse_err;
        pthread_mutex_unlock(&g_task_mutex);
        return NULL;
    }

    // Output test configuration
    std::cout << "[Connections: " << thread_num
        << " (link_num=" << g_test_config.link_num
        << ", thread_num=" << g_test_config.thread_num
        << ", servers: " << g_servers.size()
        << ", per_server: " << (g_servers.empty() ? 0 : thread_num / g_servers.size()) << ")"
        << ", Attachment: " << attachment_size << "B"
        << ", string sizes: [" << (g_test_config.req_size.empty() ? "0" : g_test_config.req_size) << "]B"
        << ", URMA: " << (FLAGS_use_urma ? "yes" : "no")
        << ", Echo: " << (g_test_config.echo_attachment ? "yes" : "no")
        << ", Batch: " << (g_test_config.batch_size > 0 ? std::to_string(g_test_config.batch_size) : "unlimited")
        << ", BatchInterval: " << g_test_config.batch_interval_ms << "ms"
        << ", MaxConcurrency: " << g_test_config.thread_pool_size
        << ", QueueDepth: " << g_test_config.queue_depth
        << ", Warmup: " << (g_test_config.warmup ? "yes" : "no")
        << (g_test_config.warmup ? ("(" + std::to_string(g_test_config.warmup_connections > 0 ? g_test_config.warmup_connections : thread_num) + ")") : "")
        << ", Ramp: " << (g_ramp.enabled
              ? (std::string(g_ramp.from_list ? "list" : "arithmetic") + " "
                 + std::to_string(g_ramp.num_steps()) + " steps x "
                 + std::to_string(g_ramp.secs_per_step) + "s, max "
                 + std::to_string(g_ramp.max_target_qps) + " qps")
              : std::string("no"))
        << "]" << std::endl;

    // 校验停止条件: 至少一个 > 0, 否则压测永不停止
    if (g_test_config.test_iterations <= 0 && g_test_config.test_seconds <= 0) {
        pthread_mutex_lock(&g_task_mutex);
        g_task.status = TASK_FAILED;
        g_task.error_message = "At least one of test_iterations/test_seconds must be > 0";
        pthread_mutex_unlock(&g_task_mutex);
        return NULL;
    }

    // Reset token semaphore if QPS-limited
    if (IsQpsLimited()) {
        bthread_sem_destroy(&g_token_sem);
        bthread_sem_init(&g_token_sem, static_cast<unsigned>(g_test_config.initial_tokens));
    }

    // ---- Phase 0: Warmup (optional, latencies NOT counted) ----
    // Pre-establish connections to eliminate server-side cold-start effects
    if (g_test_config.warmup) {
        int warmup_count = g_test_config.warmup_connections > 0 ? g_test_config.warmup_connections : thread_num;
        std::vector<PerformanceTest*> warmup_tests(warmup_count);
        int warmup_success = BatchEstablishConnections(warmup_count, attachment_size, warmup_tests, false);
        // Release warmup connections immediately
        for (auto t : warmup_tests) {
            delete t;
        }
    }

    // ---- Phase 1: Batch establish connections (collect first-packet latencies) ----
    std::vector<PerformanceTest*> tests(thread_num);

    uint64_t conn_duration_us = 0;
    int total_success = BatchEstablishConnections(thread_num, attachment_size, tests,
                                                  true, &conn_duration_us);
    int64_t establish_conn_time = static_cast<int64_t>(conn_duration_us);

    if (total_success == 0) {
        for (auto t : tests) delete t;
        pthread_mutex_lock(&g_task_mutex);
        g_task.status = TASK_FAILED;
        g_task.error_message = "No connections established";
        pthread_mutex_unlock(&g_task_mutex);
        return NULL;
    }

    // ---- Phase 2: Print first-packet latency stats ----
    if (g_test_config.debug_latency) {
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

    // ---- only_first_rpc: skip performance test, keep connections alive ----
    if (g_test_config.only_first_rpc) {
        if (g_test_config.test_seconds > 0) {
            std::cout << "Keeping " << total_success << " connections alive for "
                      << g_test_config.test_seconds << " seconds..." << std::endl;
            std::this_thread::sleep_for(std::chrono::seconds(g_test_config.test_seconds));
            std::cout << "Keep duration elapsed, exiting..." << std::endl;
        }
        // Compute results from connection-phase stats only
        uint64_t end_us = butil::gettimeofday_us();
        TaskResult result;
        result.Reset();
        ComputeResults(&result, test_start_us, end_us, 0,
                       thread_num, total_success, establish_conn_time);
        pthread_mutex_lock(&g_task_mutex);
        g_task.status = TASK_COMPLETED;
        g_task.result = result;
        pthread_mutex_unlock(&g_task_mutex);
        LOG(INFO) << "Test completed: task_id=" << g_task.task_id
                  << " expect_conn=" << result.expect_conn_count
                  << " established_conn=" << result.established_conn_count
                  << " establish_conn_time=" << result.establish_conn_time << "us"
                  << " connect_avg=" << result.connect_avg << "us"
                  << " first_rpc_avg=" << result.first_rpc_avg << "us";
        for (auto t : tests) delete t;
        return NULL;
    }

    // ---- Phase 3: Collect successful tests ----
    std::vector<PerformanceTest*> success_tests;
    for (int k = 0; k < thread_num; ++k) {
        if (tests[k] && tests[k]->init_result() == 0) {
            success_tests.push_back(tests[k]);
        }
    }

    if (success_tests.empty()) {
        LOG(ERROR) << "No connections available for performance test";
        for (auto t : tests) delete t;
        return NULL;
    }

    // ---- Phase 4: Performance test ----
    // 重建全局在途许可信号量 (支持多轮测试;
    // 上一轮 CheckGlobalStop 的唤醒补偿许可会抬升信号量计数, 必须清零)
    bthread_sem_destroy(&g_inflight_sem);
    bthread_sem_init(&g_inflight_sem, static_cast<unsigned>(g_test_config.queue_depth));

    uint64_t perf_start_us = butil::gettimeofday_us();
    RampReport ramp;
    RunPerformanceTest(success_tests, &ramp);
    uint64_t perf_end_us = butil::gettimeofday_us();

    if (g_test_config.test_keep_alive) {
        std::cout << "Keeping connections alive for 10 seconds..." << std::endl;
        std::this_thread::sleep_for(std::chrono::seconds(10));
    }

    // ---- Phase 5: Collect results ----
    TaskResult result;
    result.Reset();
    ComputeResults(&result, perf_start_us, perf_end_us, 0 /* pump model has no rounds */,
                   thread_num, total_success, establish_conn_time);

    // Store results
    pthread_mutex_lock(&g_task_mutex);
    g_task.status = TASK_COMPLETED;
    g_task.result = result;
    g_task.ramp = ramp;
    pthread_mutex_unlock(&g_task_mutex);

    LOG(INFO) << "Test completed: task_id=" << g_task.task_id
              << " expect_conn=" << result.expect_conn_count
              << " established_conn=" << result.established_conn_count
              << " establish_conn_time=" << result.establish_conn_time << "us"
              << " qps=" << result.qps
              << " latency_avg=" << result.latency_avg << "us"
              << " errors=" << result.total_errors
              << (ramp.enabled
                  ? " ramp_steps=" + std::to_string(ramp.reported_steps) + "/"
                        + std::to_string(ramp.total_steps)
                        + (ramp.truncated ? " (truncated)" : "")
                  : "");

    // Cleanup
    for (auto t : tests) delete t;
    return NULL;
}

// ==================== HTTP service implementation ====================
namespace ub_test_http {

class UbTestHttpServiceImpl : public UbTestHttpService {
public:
    UbTestHttpServiceImpl() {}
    virtual ~UbTestHttpServiceImpl() {}

    void start(google::protobuf::RpcController* cntl_base,
               const StartTestRequest* request,
               HttpResponse*,
               google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);
        brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);

        cntl->http_response().set_content_type("application/json");

        if ((request->has_protocol() && request->protocol() != "baidu_std") ||
            request->has_use_rdma() || request->has_ubsocket_use_ub() ||
            request->has_export_percentile_file()) {
            cntl->http_response().set_status_code(400);
            cntl->response_attachment().append(
                "{\"code\":-1,\"message\":\"unsupported transport or percentile export option\"}");
            return;
        }
        // Serialize starts through publication of TASK_RUNNING.
        static std::mutex start_mutex;
        std::lock_guard<std::mutex> start_lock(start_mutex);
        pthread_mutex_lock(&g_task_mutex);
        if (g_task.status == TASK_RUNNING) {
            pthread_mutex_unlock(&g_task_mutex);
            cntl->http_response().set_status_code(409);
            cntl->response_attachment().append(
                "{\"code\":-1,\"message\":\"A test is already running\",\"task_id\":\"\"}");
            return;
        }
        pthread_mutex_unlock(&g_task_mutex);

        // Populate test config from protobuf request (fallback to gflags defaults)
        g_test_config.ResetFromFlags();

        if (request->has_thread_num())              g_test_config.thread_num              = request->thread_num();
        if (request->has_link_num())                g_test_config.link_num                = request->link_num();
        if (request->has_queue_depth())             g_test_config.queue_depth             = request->queue_depth();
        if (request->has_expected_qps())            g_test_config.expected_qps            = request->expected_qps();
        if (request->has_test_seconds())            g_test_config.test_seconds            = request->test_seconds();
        if (request->has_test_iterations())         g_test_config.test_iterations         = request->test_iterations();
        if (request->has_connection_type())         g_test_config.connection_type         = request->connection_type();
        if (request->has_attachment_size())         g_test_config.attachment_size         = request->attachment_size();
        if (request->has_echo_attachment())         g_test_config.echo_attachment         = request->echo_attachment();
        if (request->has_rpc_timeout_ms())          g_test_config.rpc_timeout_ms          = request->rpc_timeout_ms();
        if (request->has_connect_timeout_ms())      g_test_config.connect_timeout_ms      = request->connect_timeout_ms();
        if (request->has_batch_size())              g_test_config.batch_size              = request->batch_size();
        if (request->has_batch_interval_ms())       g_test_config.batch_interval_ms       = request->batch_interval_ms();
        if (request->has_thread_pool_size())        g_test_config.thread_pool_size        = request->thread_pool_size();
        if (request->has_test_connections_per_round()) g_test_config.test_connections_per_round = request->test_connections_per_round();
        if (request->has_round_period_ms())         g_test_config.round_period_ms         = request->round_period_ms();
        if (request->has_max_retry())               g_test_config.max_retry               = request->max_retry();
        if (request->has_connect_retry_interval())  g_test_config.connect_retry_interval  = request->connect_retry_interval();
        if (request->has_req_size())                g_test_config.req_size                = request->req_size();
        if (request->has_client_ignore_oc())        g_test_config.client_ignore_oc        = request->client_ignore_oc();
        if (request->has_use_connection_group())    g_test_config.use_connection_group    = request->use_connection_group();
        if (request->has_max_random_delay_us())     g_test_config.max_random_delay_us     = request->max_random_delay_us();
        if (request->has_token_interval_period())   g_test_config.token_interval_period   = request->token_interval_period();
        if (request->has_initial_tokens())          g_test_config.initial_tokens          = request->initial_tokens();
        if (request->has_only_first_rpc())          g_test_config.only_first_rpc          = request->only_first_rpc();
        if (request->has_debug_latency())           g_test_config.debug_latency           = request->debug_latency();
        if (request->has_test_keep_alive())         g_test_config.test_keep_alive         = request->test_keep_alive();
        if (request->has_warmup())                  g_test_config.warmup                  = request->warmup();
        if (request->has_warmup_connections())      g_test_config.warmup_connections      = request->warmup_connections();
        if (request->has_max_thread_num())          g_test_config.max_thread_num          = request->max_thread_num();
        if (request->has_sender_thread_num())       g_test_config.sender_thread_num       = request->sender_thread_num();
        if (request->has_token_tick_us())           g_test_config.token_tick_us           = request->token_tick_us();
        if (request->has_error_log_interval_ms())   g_test_config.error_log_interval_ms   = request->error_log_interval_ms();
        if (request->has_latency_window_seconds())  g_test_config.latency_window_seconds  = request->latency_window_seconds();
        if (request->has_allow_stop_burst())        g_test_config.allow_stop_burst        = request->allow_stop_burst();
        if (request->has_qps_ramp_list())           g_test_config.qps_ramp_list           = request->qps_ramp_list();
        if (request->has_qps_ramp_start())          g_test_config.qps_ramp_start          = request->qps_ramp_start();
        if (request->has_qps_ramp_step())           g_test_config.qps_ramp_step           = request->qps_ramp_step();
        if (request->has_qps_ramp_step_seconds())   g_test_config.qps_ramp_step_seconds   = request->qps_ramp_step_seconds();
        if (request->has_qps_ramp_max())            g_test_config.qps_ramp_max            = request->qps_ramp_max();

        // 校验 queue_depth: 必须 >= 1, 否则发送泵数量为 0, 压测会永久挂起;
        // 同时设置上限, 避免负数经 std::vector 构造放大为巨大无符号数导致进程异常退出。
        // 非法值时打印一行说明并回退到默认值 1。
        const int kMaxQueueDepth = 1000000;
        if (g_test_config.queue_depth < 1 || g_test_config.queue_depth > kMaxQueueDepth) {
            LOG(WARNING) << "Invalid queue_depth=" << g_test_config.queue_depth
                         << ", must be in [1, " << kMaxQueueDepth << "], fallback to 1";
            g_test_config.queue_depth = 1;
        }

        // 组装 QPS 阶梯计划 (必须放在所有字段覆盖之后: 依赖 test_seconds/expected_qps/
        // only_first_rpc)。非法配置 -> HTTP 400 早返回, 不产生一个必然失败的任务 ——
        // 错误的流量模型比直接失败更糟, 因此不做静默回退。
        std::string ramp_err;
        if (BuildRampPlan(&ramp_err) != 0) {
            LOG(WARNING) << "Invalid ramp config: " << ramp_err;
            cntl->http_response().set_status_code(400);
            std::string resp = "{\"code\":-1,\"message\":\"";
            JsonEscape(&resp, ramp_err);
            resp += "\",\"task_id\":\"\"}";
            cntl->response_attachment().append(resp);
            return;
        }

        // Populate server list
        g_servers.clear();
        if (request->servers_size() > 0) {
            for (int i = 0; i < request->servers_size(); ++i) {
                g_servers.push_back(request->servers(i));
            }
        } else {
            g_servers.push_back("0.0.0.0:8002");
        }

        // Generate task ID
        ++g_task_seq;
        time_t now = time(NULL);
        char task_id_buf[64];
        struct tm tm_result;
        localtime_r(&now, &tm_result);
        snprintf(task_id_buf, sizeof(task_id_buf),
                 "task_%04d%02d%02d_%02d%02d%02d_%04d",
                 tm_result.tm_year + 1900, tm_result.tm_mon + 1, tm_result.tm_mday,
                 tm_result.tm_hour, tm_result.tm_min, tm_result.tm_sec,
                 g_task_seq);

        // Initialize task state
        pthread_mutex_lock(&g_task_mutex);
        g_task.Reset();
        g_task.task_id = task_id_buf;
        g_task.status = TASK_RUNNING;
        g_task.started_at_us = butil::gettimeofday_us();
        pthread_mutex_unlock(&g_task_mutex);

        // Spawn test execution bthread (no JSON body needed anymore)
        bthread_t tid;
        int ret = bthread_start_background(&tid, &BTHREAD_ATTR_NORMAL,
                                           ExecuteTest, NULL);
        if (ret != 0) {
            pthread_mutex_lock(&g_task_mutex);
            g_task.status = TASK_FAILED;
            g_task.error_message = "Failed to start test execution bthread";
            pthread_mutex_unlock(&g_task_mutex);
            cntl->http_response().set_status_code(500);
            cntl->response_attachment().append(
                "{\"code\":-1,\"message\":\"Failed to start test execution\",\"task_id\":\"\"}");
            return;
        }

        // Return success
        std::string resp = "{\"code\":0,\"message\":\"success\",\"task_id\":\"";
        resp += task_id_buf;
        resp += "\"}";
        cntl->response_attachment().append(resp);
    }

    void result(google::protobuf::RpcController* cntl_base,
                const HttpRequest*,
                HttpResponse*,
                google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);
        brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);

        cntl->http_response().set_content_type("application/json");

        // Parse task_id from query string
        std::string task_id;
        const std::string* tid_param = cntl->http_request().uri().GetQuery("task_id");
        if (tid_param) {
            task_id = *tid_param;
        }

        pthread_mutex_lock(&g_task_mutex);

        if (task_id.empty() || g_task.task_id.empty()) {
            pthread_mutex_unlock(&g_task_mutex);
            cntl->http_response().set_status_code(400);
            cntl->response_attachment().append(
                "{\"code\":-1,\"message\":\"Missing or invalid task_id\",\"result\":null}");
            return;
        }

        if (task_id != g_task.task_id) {
            pthread_mutex_unlock(&g_task_mutex);
            cntl->http_response().set_status_code(404);
            std::string resp = "{\"code\":-1,\"message\":\"Task not found: ";
            JsonEscape(&resp, task_id);
            resp += "\",\"result\":null}";
            cntl->response_attachment().append(resp);
            return;
        }

        std::string status_str;
        switch (g_task.status) {
            case TASK_RUNNING:   status_str = "running"; break;
            case TASK_COMPLETED: status_str = "completed"; break;
            case TASK_FAILED:    status_str = "failed"; break;
            default:             status_str = "idle"; break;
        }

        std::string resp = "{\"code\":0,\"task_id\":\"";
        resp += g_task.task_id;
        resp += "\",\"status\":\"";
        resp += status_str;
        resp += "\"";

        if (g_task.status == TASK_COMPLETED) {
            resp += ",\"result\":";
            resp += BuildResultJson(g_task.result, &g_task.ramp);
        } else if (g_task.status == TASK_FAILED) {
            resp += ",\"error_message\":\"";
            JsonEscape(&resp, g_task.error_message);
            resp += "\"";
        } else if (g_task.status == TASK_RUNNING) {
            uint64_t elapsed = butil::gettimeofday_us() - g_task.started_at_us;
            resp += ",\"elapsed_s\":";
            char buf[32];
            snprintf(buf, sizeof(buf), "%.2f", elapsed / 1000000.0);
            resp += buf;
        }

        resp += "}";
        pthread_mutex_unlock(&g_task_mutex);

        cntl->response_attachment().append(resp);
    }

    void status(google::protobuf::RpcController* cntl_base,
                const HttpRequest*,
                HttpResponse*,
                google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);
        brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);

        cntl->http_response().set_content_type("application/json");

        pthread_mutex_lock(&g_task_mutex);

        std::string resp = "{\"code\":0,";

        if (g_task.status == TASK_RUNNING) {
            resp += "\"running\":true,\"current_task_id\":\"";
            resp += g_task.task_id;
            resp += "\"";
            uint64_t elapsed = butil::gettimeofday_us() - g_task.started_at_us;
            char buf[32];
            snprintf(buf, sizeof(buf), "%.2f", elapsed / 1000000.0);
            resp += ",\"elapsed_s\":";
            resp += buf;
        } else {
            resp += "\"running\":false,\"current_task_id\":\"";
            resp += g_task.task_id;
            resp += "\"";
            if (g_task.status == TASK_COMPLETED) {
                resp += ",\"last_status\":\"completed\"";
            } else if (g_task.status == TASK_FAILED) {
                resp += ",\"last_status\":\"failed\"";
                resp += ",\"error_message\":\"";
                JsonEscape(&resp, g_task.error_message);
                resp += "\"";
            }
        }

        resp += "}";
        pthread_mutex_unlock(&g_task_mutex);

        cntl->response_attachment().append(resp);
    }

    // ---- Proxy 后端配置 (ServerC 列表) ----
    // GET  = 查询当前列表 (不改变状态); POST/PUT = 设置列表 (空列表 = 禁用转发)。
    // 热切换: 旧快照由在途请求的 shared_ptr 引用计数释放, 与并发转发无竞态。
    void proxy_config(google::protobuf::RpcController* cntl_base,
                      const ProxyConfigRequest* request,
                      HttpResponse*,
                      google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);
        brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);

        cntl->http_response().set_content_type("application/json");

        const brpc::HttpMethod method = cntl->http_request().method();
        if (method == brpc::HTTP_METHOD_GET) {
            cntl->response_attachment().append(BuildProxyBackendsJson("success", 0));
            return;
        }
        if (method != brpc::HTTP_METHOD_POST && method != brpc::HTTP_METHOD_PUT) {
            cntl->http_response().set_status_code(405);
            cntl->response_attachment().append(
                "{\"code\":-1,\"message\":\"use GET to query or POST to set\","
                "\"enabled\":false,\"backends\":[],\"ready\":0}");
            return;
        }

        // 收集后端列表 (去首尾空白; 空列表合法 = 禁用转发)
        std::vector<std::string> servers;
        std::string bad;
        for (int i = 0; i < request->servers_size(); ++i) {
            std::string s = request->servers(i);
            s.erase(0, s.find_first_not_of(" \t"));
            s.erase(s.find_last_not_of(" \t") + 1);
            if (s.empty()) {
                bad = "empty server entry at index " + std::to_string(i);
                break;
            }
            servers.push_back(s);
        }
        if (!bad.empty()) {
            cntl->http_response().set_status_code(400);
            std::string resp = "{\"code\":-1,\"message\":\"";
            JsonEscape(&resp, bad);
            resp += "\",\"enabled\":false,\"backends\":[],\"ready\":0}";
            cntl->response_attachment().append(resp);
            return;
        }

        std::string err;
        std::shared_ptr<const ProxySnapshot> snap;
        int rc = BuildProxySnapshot(servers, &snap, &err);
        std::atomic_store(&g_proxy_snapshot, snap);
        LOG(INFO) << "Proxy backend config updated: configured=" << servers.size()
                  << " ready=" << snap->channels.size()
                  << " forward_times=" << FLAGS_srv_forward_times
                  << (rc != 0 ? " (" + err + ")" : "");
        cntl->response_attachment().append(
            BuildProxyBackendsJson(rc == 0 ? "success" : err.c_str(), rc == 0 ? 0 : -1));
    }

    // ---- Proxy 转发统计 (B→C) ----
    // ?reset=1 清零转发计数/recorder (即时生效; self.* 为进程生命周期统计不随 reset)。
    void proxy_stats(google::protobuf::RpcController* cntl_base,
                     const HttpRequest*,
                     HttpResponse*,
                     google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);
        brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);

        cntl->http_response().set_content_type("application/json");

        const std::string* reset = cntl->http_request().uri().GetQuery("reset");
        if (reset != NULL && *reset == "1") {
            ResetProxyStats();
        }
        cntl->response_attachment().append(BuildProxyStatsJson());
    }

    // ---- Server 端 per-port + 汇总统计 (只读快照) ----
    // 没有 ?reset=: 计数只在轮次结束时由 StatsPrinter 统一清零 (多处清零会与并发首请求竞态)。
    void server_stats(google::protobuf::RpcController* cntl_base,
                      const HttpRequest*,
                      HttpResponse*,
                      google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);
        brpc::Controller* cntl = static_cast<brpc::Controller*>(cntl_base);

        cntl->http_response().set_content_type("application/json");
        cntl->response_attachment().append(BuildServerStatsJson());
    }
};

}  // namespace ub_test_http

// ==================== main ====================
int main(int argc, char* argv[]) {
    GFLAGS_NAMESPACE::ParseCommandLineFlags(&argc, &argv, true);

    // 校验 fan-out 放大倍数: 非法值回退默认 1 (避免放大失控/零转发挂起)
    if (FLAGS_srv_forward_times < 1 || FLAGS_srv_forward_times > kMaxForwardTimes) {
        LOG(WARNING) << "Invalid srv_forward_times=" << FLAGS_srv_forward_times
                     << ", must be in [1, " << kMaxForwardTimes << "], fallback to 1";
        FLAGS_srv_forward_times = 1;
    }

    // 校验长尾参数: 负数 permille 转成 uint64 后会变成"全命中", 必须先归零
    if (FLAGS_srv_tail_permille < 0) {
        LOG(WARNING) << "Invalid srv_tail_permille=" << FLAGS_srv_tail_permille
                     << ", must be in [0, 1000], fallback to 0 (disabled)";
        FLAGS_srv_tail_permille = 0;
    } else if (FLAGS_srv_tail_permille > 1000) {
        LOG(WARNING) << "srv_tail_permille=" << FLAGS_srv_tail_permille
                     << " > 1000, clamped to 1000 (every request is a tail)";
        FLAGS_srv_tail_permille = 1000;
    }
    if (FLAGS_srv_tail_delay_us < 0) {
        LOG(WARNING) << "Invalid srv_tail_delay_us=" << FLAGS_srv_tail_delay_us
                     << ", fallback to 0";
        FLAGS_srv_tail_delay_us = 0;
    }
    if (FLAGS_srv_tail_delay_max_us < 0) {
        LOG(WARNING) << "Invalid srv_tail_delay_max_us=" << FLAGS_srv_tail_delay_max_us
                     << ", fallback to 0";
        FLAGS_srv_tail_delay_max_us = 0;
    }
    if (FLAGS_srv_tail_permille > 0) {
        LOG(INFO) << "Long-tail simulation: permille=" << FLAGS_srv_tail_permille
                  << ", delay=" << FLAGS_srv_tail_delay_us << "us"
                  << (FLAGS_srv_tail_delay_max_us > FLAGS_srv_tail_delay_us
                          ? "-" + std::to_string(FLAGS_srv_tail_delay_max_us) + "us"
                          : "")
                  << ", mode=" << (FLAGS_srv_tail_cpu ? "cpu busy" : "sleep")
                  << ". Make sure the client rpc_timeout_ms (default "
                  << FLAGS_rpc_timeout_ms << "ms) is well above the worst-case delay.";
    }

    // ---- 解析并校验多端口监听配置 (启动参数, 非法即拒绝启动) ----
    // 与 HTTP 下发参数的 WARNING+回退策略刻意不同: 端口配错若被放行, 症状是"少起一个
    // 后端 -> 客户端连不上", 报错点离病因很远, 所以这里 fail-fast。
    std::vector<int> want_ports;
    want_ports.push_back(FLAGS_port);   // ports[0] = 主端口 (RPC + HTTP 管理)
    if (!FLAGS_srv_extra_ports.empty()) {
        std::vector<int> extra_ports;
        std::string perr;
        if (ParsePortList(FLAGS_srv_extra_ports, &extra_ports, &perr) != 0 ||
            ValidateListenPorts(FLAGS_port, extra_ports, &perr) != 0) {
            LOG(ERROR) << "Invalid -srv_extra_ports: " << perr;
            return -1;
        }
        want_ports.insert(want_ports.end(), extra_ports.begin(), extra_ports.end());
    }

    // Server::Start() creates workers via bthread_setconcurrency_by_tag(options.num_threads).
    // Assigning FLAGS_bthread_concurrency alone does NOT create workers, so wire the
    // intended knob (srv_bthread_concurrency) into num_threads. Keep srv_num_threads
    // as a backward-compatible alias: whichever is larger wins when they differ.
    int worker_concurrency = FLAGS_srv_bthread_concurrency;
    if (FLAGS_srv_num_threads > worker_concurrency) {
        worker_concurrency = FLAGS_srv_num_threads;
    }
    if (bthread_setconcurrency(worker_concurrency) != 0) {
        LOG(ERROR) << "Fail to set bthread concurrency to " << worker_concurrency
                   << " (srv_bthread_concurrency=" << FLAGS_srv_bthread_concurrency
                   << ", srv_num_threads=" << FLAGS_srv_num_threads << ")";
        return -1;
    }
    LOG(INFO) << "bthread worker concurrency=" << bthread_getconcurrency()
              << " (srv_bthread_concurrency=" << FLAGS_srv_bthread_concurrency
              << ", srv_num_threads=" << FLAGS_srv_num_threads << ")";

    // Initialize server-side response name
    if (FLAGS_use_urma) {
#if BRPC_WITH_URMA
        std::string resource_mode;
        if (GFLAGS_NAMESPACE::GetCommandLineOption("urma_resource_mode", &resource_mode) &&
            resource_mode == "exclusive") {
            GFLAGS_NAMESPACE::SetCommandLineOptionWithMode(
                "urma_buffer_size", "4096", GFLAGS_NAMESPACE::SET_FLAGS_DEFAULT);
        }
        brpc::urma::GlobalUrmaInitializeOrDie();
        auto* context = brpc::urma::GetUrmaContext();
        if (!brpc::urma::IsUrmaAvailable() || !context || !context->dev ||
            !context->dev->name[0] || std::strcmp(context->dev->name, "mock_urma_device") == 0) {
            LOG(ERROR) << "--use_urma requires an initialized real URMA device";
            return 1;
        }
#else
        LOG(ERROR) << "--use_urma requires a WITH_URMA build";
        return 1;
#endif
    }
    g_srv_rsp_name.resize(FLAGS_srv_rsp_size, 'r');
    LOG(INFO) << "server rsp/name len is " << g_srv_rsp_name.size() << "B";

    bthread_sem_init(&g_token_sem, 0);
    // 占位初始化, 实际数量在每次 ExecuteTest() 阶段4前重建为 queue_depth
    bthread_sem_init(&g_inflight_sem, 0);

    // Initialize per-run LatencyRecorders (creates "run1_*" bvar exposure)
    ResetGlobalStats();
    // 创建常驻 proxy%d_* 统计组 (version 从 1 起)
    ResetProxyStats();

    // Initialize task state
    g_task.status = TASK_IDLE;

    // ---- 多端口: {主端口} + {额外 RPC 端口}, 每个端口一个 brpc::Server 对象 ----
    // brpc 一个 Server 对象只能 Start 一次、只监听一个端口 (状态机见 src/brpc/server.cpp,
    // docs/cn/server.md:230-232 明文"需要监听N个端口就起N个Server")。
    // 声明顺序 = 析构顺序的逆序: **services 必须比 servers 活得久** —— 服务以
    // SERVER_DOESNT_OWN_SERVICE 注册 (Server 只存指针, 不 delete), 若 servers 后析构,
    // 其 Stop/Join 期间 service 已失效。单端口时代靠 ~Server 先 Stop+Join 侥幸成立,
    // 多端口下必须显式按此顺序声明。
    std::vector<std::unique_ptr<test::PerfTestServiceImpl>> per_port_svcs;
    ub_test_http::UbTestHttpServiceImpl http_svc;   // 仅注册到主端口

    brpc::ServerOptions options;
    options.socket_mode = ProcessSocketMode();
    options.enabled_protocols = "baidu_std";
    // num_threads 是**进程级**的 (bthread_setconcurrency_by_tag, 且只增不减), 所有端口
    // 必须是同一个值, 故共用同一份 options。max_concurrency 相反是 per-Server 的:
    // 每个端口各自一份并发预算, 这正是"N 个独立后端"应有的语义。
    options.num_threads = worker_concurrency;
    options.max_concurrency = FLAGS_srv_max_concurrency;
    options.ignore_eovercrowded = FLAGS_srv_ignore_oc;

    // 多端口模式下所有端口号在 Start **之前**就已最终确定 (-srv_extra_ports 拒绝端口 0,
    // 且与 -port=0 互斥), 所以统计槽位与 per-port recorder 可以提前建好 —— 从第一个
    // 请求起每端口统计就是完整的, 不存在"已开始服务但 recorder 还没建"的窗口。
    // (单端口时 g_srv_multi_port 为 false, 不建 per-port bvar; -port=0 走内核分配,
    //  真实端口在 Start 之后回填, 此时没有依赖端口号的 recorder。)
    g_srv_multi_port = (want_ports.size() > 1);

    std::vector<std::unique_ptr<brpc::Server>> servers;
    servers.reserve(want_ports.size());
    g_srv_ports.reserve(want_ports.size());
    for (size_t i = 0; i < want_ports.size(); ++i) {
        g_srv_ports.emplace_back(new SrvPort);
        g_srv_ports[i]->port = want_ports[i];
        if (g_srv_multi_port && CreatePortRecorders(g_srv_ports[i].get()) != 0) {
            return -1;
        }
        per_port_svcs.emplace_back(new test::PerfTestServiceImpl(g_srv_ports[i].get()));
        std::unique_ptr<brpc::Server> srv(new brpc::Server);
        if (srv->AddService(per_port_svcs[i].get(),
                            brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
            LOG(ERROR) << "Fail to add PerfTestService for port=" << want_ports[i];
            return -1;
        }
        // HTTP 管理 API 只挂主端口: 它的全部状态 (g_task/g_servers/g_proxy_snapshot/
        // g_test_config) 都是进程级单例, 挂到 N 个端口只会得到 N 个完全相同的视图,
        // 反而增加"误对纯 RPC 端口 POST /ub_test/start" (在被测进程里再起一个压测端)
        // 的风险。brpc builtin (/vars /status /connections) 每个端口都有。
        if (i == 0 &&
            srv->AddService(&http_svc, brpc::SERVER_DOESNT_OWN_SERVICE,
                            "/ub_test/start  => start,"
                            "/ub_test/result => result,"
                            "/ub_test/status => status,"
                            "/ub_test/proxy_config => proxy_config,"
                            "/ub_test/proxy_stats => proxy_stats,"
                            "/ub_test/server_stats => server_stats") != 0) {
            LOG(ERROR) << "Fail to add UbTestHttpService";
            return -1;
        }
        if (srv->Start(want_ports[i], &options) != 0) {
            LOG(ERROR) << "Fail to start server on port=" << want_ports[i];
            // 已 Start 的 Server 由 unique_ptr 析构 (~Server 会 Stop+Join) 收尾
            return -1;
        }
        // -port=0 (仅单端口模式允许) 时回填内核分配的真实端口; 多端口模式下端口号
        // 启动前后一致, 这里是幂等的。
        g_srv_ports[i]->port = srv->listen_address().port;
        g_srv_ports[i]->server = srv.get();
        servers.push_back(std::move(srv));
    }

    // 启动 proxy 自身 CPU/内存常驻采样线程 (生命周期跟随进程, 压测结束后仍持续采样)
    bthread_t sampler_tid;
    if (bthread_start_background(&sampler_tid, &BTHREAD_ATTR_NORMAL,
                                 ProxySelfSampler, NULL) != 0) {
        LOG(ERROR) << "Fail to start proxy self-metrics sampler";
    }

    // 启动进程级背景 CPU 负载 (0 = 禁用; 与请求流量无关的常驻负载)
    // srv_bg_cpu_on_worker=false: 独立 std::thread (模拟同机混部);
    // true: bthread 跑在 brpc worker 池上 (模拟进程内后台任务占 worker,
    //       需调大 srv_bthread_concurrency 避免饿死请求)。
    std::vector<std::thread> bg_cpu_threads;
    std::vector<bthread_t> bg_cpu_bthreads;
    const int bg_threads = std::min(std::max(FLAGS_srv_bg_cpu_threads, 0),
                                    MAX_BG_CPU_THREADS);
    if (bg_threads > 0 && FLAGS_srv_bg_cpu_percent > 0) {
        if (FLAGS_srv_bg_cpu_on_worker) {
            bg_cpu_bthreads.reserve(bg_threads);
            for (int i = 0; i < bg_threads; ++i) {
                bthread_t tid;
                if (bthread_start_background(
                        &tid, &BTHREAD_ATTR_NORMAL, BackgroundCpuLoopBthread,
                        reinterpret_cast<void*>(static_cast<intptr_t>(i))) == 0) {
                    bg_cpu_bthreads.push_back(tid);
                }
            }
            LOG(INFO) << "Background CPU load started on brpc workers: bthreads="
                      << bg_cpu_bthreads.size()
                      << " percent=" << FLAGS_srv_bg_cpu_percent
                      << "% (bthread_concurrency=" << FLAGS_srv_bthread_concurrency << ")";
        } else {
            bg_cpu_threads.reserve(bg_threads);
            for (int i = 0; i < bg_threads; ++i) {
                bg_cpu_threads.emplace_back(BackgroundCpuLoopStd, i);
            }
            LOG(INFO) << "Background CPU load started: threads=" << bg_threads
                      << " percent=" << FLAGS_srv_bg_cpu_percent << "%";
        }
    }

    // 启动横幅: 单端口时**逐字节保持旧输出** (外部日志解析/对比脚本不受影响),
    // 只有多端口时才展开端口列表与新端点说明。
    std::string listening_ports;
    for (size_t i = 0; i < g_srv_ports.size(); ++i) {
        if (i) listening_ports += ",";
        listening_ports += std::to_string(g_srv_ports[i]->port);
    }
    if (g_srv_multi_port) {
        LOG(INFO) << "UbTestHttpServer started on ports " << listening_ports
                  << " (main=" << g_srv_ports[0]->port << ", "
                  << g_srv_ports.size() << " listening port(s))";
    } else {
        LOG(INFO) << "UbTestHttpServer started on port " << g_srv_ports[0]->port;
    }
    LOG(INFO) << "Services:";
    LOG(INFO) << "  PerfTestService (RPC)       - handles pressure test requests"
              << (g_srv_multi_port ? " (on every port)" : "");
    LOG(INFO) << "  UbTestHttpService (HTTP)    - management API"
              << (g_srv_multi_port ? " (main port only)" : "");
    if (g_srv_multi_port) {
        LOG(INFO) << "Multi-port mode: this process acts as " << g_srv_ports.size()
                  << " identical ServerC backends sharing all srv_* business flags. "
                     "The client 'servers' list must cover e.g. 127.0.0.1:"
                  << listening_ports << " . /ub_test/* exists on port "
                  << g_srv_ports[0]->port << " only; brpc builtins (/vars /status "
                     "/connections) exist on every port.";
    }
    if (g_srv_multi_port) {
        LOG(INFO) << "HTTP Endpoints (port " << g_srv_ports[0]->port << "):";
    } else {
        LOG(INFO) << "HTTP Endpoints:";
    }
    LOG(INFO) << "  POST /ub_test/start         - Start a performance test";
    LOG(INFO) << "  GET  /ub_test/result?task_id=xxx - Query results";
    LOG(INFO) << "  GET  /ub_test/status        - Check server status";
    LOG(INFO) << "  POST/GET /ub_test/proxy_config - Set/query proxy backends (ServerC list)";
    LOG(INFO) << "  GET  /ub_test/proxy_stats?reset=1 - Proxy forwarding stats (B→C)";
    // server_stats 只在多端口时列进横幅: 单端口下它的内容与最终报告完全重合, 列出来
    // 反而会改变既有的启动输出 (外部日志对比脚本会看到差异)。
    if (g_srv_multi_port) {
        LOG(INFO) << "  GET  /ub_test/server_stats  - Server-side per-port + total stats";
    }

    // RunUntilAskedToQuit 内部只 Stop+Join **它自己**那一个 Server (等信号 + Stop(0)+Join),
    // 额外端口的 Server 不在它的管辖范围内, 必须显式收尾。
    servers[0]->RunUntilAskedToQuit();

    g_stop.store(true, butil::memory_order_relaxed);
    g_srv_stop.store(true, butil::memory_order_relaxed);
    // 收尾顺序与主 Server 的待遇一致: 先置停止标志 (StatsPrinter/采样/背景负载据此退出),
    // 再逐个 Stop+Join。Stop(0) -> StopAccept 会 SetFailed 所有连接, Join() -> 等连接清空;
    // 在途长尾/忙等会让它多等一个请求时长, 与主 Server 今天的行为相同, 非新增风险。
    // 已 Join 过的再调返回 -1, 无害 (幂等)。
    for (size_t i = 1; i < servers.size(); ++i) {
        servers[i]->Stop(0);
        servers[i]->Join();
    }
    for (auto& t : bg_cpu_threads) {   // 停止标志已置位, 背景负载线程最坏 ~10us 内退出
        t.join();
    }
    for (auto& tid : bg_cpu_bthreads) {   // worker 模式: 休眠相位最长 9ms 后退出
        bthread_join(tid, NULL);
    }
    bthread_sem_destroy(&g_token_sem);
    return 0;
}
