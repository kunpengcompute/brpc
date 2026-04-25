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


#ifndef USE_MESALINK
#include <openssl/ssl.h>
#include <openssl/conf.h>
#else
#include <mesalink/openssl/ssl.h>
#endif

#include <gflags/gflags.h>
#include <fcntl.h>                               // O_RDONLY
#include <signal.h>

#include "butil/build_config.h"                  // OS_LINUX
// Naming services
#ifdef BAIDU_INTERNAL
#include "brpc/policy/baidu_naming_service.h"
#endif
#include "brpc/policy/file_naming_service.h"
#include "brpc/policy/list_naming_service.h"
#include "brpc/policy/domain_naming_service.h"
#include "brpc/policy/remote_file_naming_service.h"
#include "brpc/policy/consul_naming_service.h"
#include "brpc/policy/discovery_naming_service.h"
#include "brpc/policy/nacos_naming_service.h"

// Load Balancers
#include "brpc/policy/round_robin_load_balancer.h"
#include "brpc/policy/weighted_round_robin_load_balancer.h"
#include "brpc/policy/randomized_load_balancer.h"
#include "brpc/policy/weighted_randomized_load_balancer.h"
#include "brpc/policy/locality_aware_load_balancer.h"
#include "brpc/policy/consistent_hashing_load_balancer.h"
#include "brpc/policy/hasher.h"
#include "brpc/policy/dynpart_load_balancer.h"


// Span
#include "brpc/span.h"
#include "bthread/unstable.h"

// Compress handlers
#include "brpc/compress.h"
#include "brpc/policy/gzip_compress.h"
#include "brpc/policy/snappy_compress.h"

// Checksum handlers
#include "brpc/checksum.h"
#include "brpc/policy/crc32c_checksum.h"

// Protocols
#include "brpc/protocol.h"
#include "brpc/policy/baidu_rpc_protocol.h"
#include "brpc/policy/http_rpc_protocol.h"
#include "brpc/policy/http2_rpc_protocol.h"
#include "brpc/policy/hulu_pbrpc_protocol.h"
#include "brpc/policy/nova_pbrpc_protocol.h"
#include "brpc/policy/public_pbrpc_protocol.h"
#include "brpc/policy/ubrpc2pb_protocol.h"
#include "brpc/policy/sofa_pbrpc_protocol.h"
#include "brpc/policy/memcache_binary_protocol.h"
#include "brpc/policy/streaming_rpc_protocol.h"
#include "brpc/policy/mongo_protocol.h"
#include "brpc/policy/redis_protocol.h"
#include "brpc/policy/nshead_mcpack_protocol.h"
#include "brpc/policy/rtmp_protocol.h"
#include "brpc/policy/esp_protocol.h"
#ifdef ENABLE_THRIFT_FRAMED_PROTOCOL
# include "brpc/policy/thrift_protocol.h"
#endif

// Concurrency Limiters
#include "brpc/concurrency_limiter.h"
#include "brpc/policy/auto_concurrency_limiter.h"
#include "brpc/policy/constant_concurrency_limiter.h"
#include "brpc/policy/timeout_concurrency_limiter.h"

#include "brpc/input_messenger.h"     // get_or_new_client_side_messenger
#include "brpc/socket_map.h"          // SocketMapList
#include "brpc/server.h"
#include "brpc/trackme.h"             // TrackMe
#include "brpc/details/usercode_backup_pool.h"
#if defined(OS_LINUX)
#include <malloc.h>                   // malloc_trim
#endif
#include "butil/fd_guard.h"
#include "butil/files/file_watcher.h"
#if BRPC_WITH_URMA
#include "brpc_context.h"
#include "brpc_thread_pool.h"
#include "ub_lock_ops.h"
#endif

#include "bthread/rwlock.h"
#include "bthread/bthread.h"

extern "C" {
// defined in gperftools/malloc_extension_c.h
void BAIDU_WEAK MallocExtension_ReleaseFreeMemory(void);
}

DECLARE_bool(ubsocket_enable);

namespace brpc {

DECLARE_bool(usercode_in_pthread);

DEFINE_int32(free_memory_to_system_interval, 0,
             "Try to return free memory to system every so many seconds, "
             "values <= 0 disables this feature");
BRPC_VALIDATE_GFLAG(free_memory_to_system_interval, PassValidate);

DEFINE_string(ubsocket_trans_mode, "ub", "Transport mode for ubsocket (e.g., 'ub', 'ib')");
DEFINE_string(ubsocket_dev_name, "", "Device name for ubsocket (e.g., 'udma2', 'bonding_dev_0')");
DEFINE_string(ubsocket_dev_ip, "", "Device ip for ubsocket");
DEFINE_string(ubsocket_eid_idx, "", "Normal device eid idx for ubsocket, necessary while using ub. Obtained by querying with the 'urma_admin show' command");
DEFINE_string(ubsocket_src_eid, "", "Bonding device eid idx for ubsocket, necessary while using ub. Obtained by querying with the 'urma_admin show' command");
DEFINE_string(ubsocket_log_level, "info", "Log level for ubsocket (e.g., 'error', 'warn', 'notice', 'info', 'debug')");
DEFINE_string(ubsocket_log_use_printf, "true", "Whether to print the logs to the foreground for ubsocket (e.g., 'false', 'true')");
DEFINE_string(ubsocket_tx_depth, "1024", "Send queue depth, the minimum value is 2. The upper limit of the setting is determined by the actual machine environment, based on the value of 'max_jfc_depth' in the command 'urma_admin show --whole'.");
DEFINE_string(ubsocket_rx_depth, "1024", "Receive queue depth, the minimum value is 2. The upper limit of the setting is determined by the actual machine environment, based on the value of 'max_jfc_depth' in the command 'urma_admin show --whole'.");
DEFINE_string(ubsocket_block_type, "default", "Minimum fragment of the memory pool for ubsocket (e.g., 'default'(8k), 'small'(16k), 'medium'(32k), 'large'(64k))");
DEFINE_string(ubsocket_pool_initial_size, "1024", "Total size of IO memory for ubsocket, in MB");
DEFINE_string(ubsocket_pool_max_size, "2048", "Max size of ubsocket pool, in MB");
DEFINE_string(ubsocket_buf_pool_depth, "12000", "Depth of ubsocket buffer pool");
DEFINE_string(ubsocket_schedule_policy, "affinity_priority", "Set the multi-plane load balancing policy (e.g., 'affinity_priority', 'affinity', 'rr')");
DEFINE_string(ubsocket_readv_unlimited, "true", "Whether to enable the readv reporting limit for ubsocket (e.g., 'false', 'true')");
DEFINE_string(ubsocket_use_polling, "false", "Whether to enable message processing polling for ubsocket (e.g., 'false', 'true')");
DEFINE_string(ubsocket_brpc_alloc_sym, "", "The global pointer symbol information of butil::iobuf::blockmem_allocate in the brpc component");
DEFINE_string(ubsocket_brpc_dealloc_sym, "", "The global pointer symbol information of butil::iobuf::blockmem_deallocate in the brpc component");
DEFINE_string(ubsocket_adpt_stats, "false", "Count statistics for ubsocket (e.g., 'false', 'true')");
DEFINE_string(ubsocket_auto_fallback_tcp, "true", "Whether to automatically downgrade TCP when the protocols do not match (e.g., 'false', 'true')");
DEFINE_string(ubsocket_enable_share_jfr, "true", "Whether to enable share jfr (e.g., 'false', 'true')");
DEFINE_string(ubsocket_share_jfr_rx_queue_depth, "1024", "Share jfr receive queue depth, the minimum value is 64. The upper limit of the setting is determined by the actual machine environment.");
DEFINE_string(ubsocket_trace_enable, "true", "Enable ubsocket trace statistics (e.g., 'false', 'true')");
DEFINE_string(ubsocket_trace_time, "10", "Set monitoring ubsocket data output interval, the minimum value is 1, the maximum value is 300");
DEFINE_string(ubsocket_trace_file_path, "/tmp/ubsocket/log", "Set monitoring ubsocket data output path (e.g., '/tmp/ubsocket/log')");
DEFINE_string(ubsocket_trace_file_size, "10", "Set monitoring ubsocket data file size, the minimum value is 1, the maximum value is 300");
DEFINE_string(ubsocket_stats_cli, "true", "Enable ubsocket cli service (e.g., 'false', 'true')");
DEFINE_string(ubsocket_ub_trans_mode, "RM_TP", "Protocol mode for ubsocket (e.g., 'RC_TP', 'RM_TP', 'RM_CTP', 'RC_CTP')");
DEFINE_string(ubsocket_min_reserved_credit, "64", "Minimum reserved credit, if the held credit <= min_reserved_credit, the credit will not be returned.");
DEFINE_string(ubsocket_link_priority, "-1", "Set urma flow service level priority, range from 0 to 15.");
DEFINE_string(ubsocket_degrade, "true", "Allow degradation to TCP when UB fails; default: true");
DEFINE_string(ubsocket_async_accept, "false", "Allow do accept async; default: false");
DEFINE_string(ubsocket_thread_pool_size, "1", "the number of threads in ubsocket thread pool; default: 0");
DEFINE_string(ubsocket_probe_enable, "false", "Enable ubsocket probe (e.g., 'false', 'true')");
DEFINE_string(ubsocket_probe_time_ms, "1000", "ubsocket probe interval time, the minimum value is 1, the maximum value is 360000");
DEFINE_string(ubsocket_probe_batch, "10", "ubsocket number of sock to probe per batch, the minimum value is 1, the maximum value is 500");
DEFINE_string(ubsocket_ub_epoll_enable, "false", "Whether to enable ub epoll; default: false (optional: false, true)");

namespace policy {
// Defined in http_rpc_protocol.cpp
void InitCommonStrings();
}

using namespace policy;

const char* const DUMMY_SERVER_PORT_FILE = "dummy_server.port";

struct GlobalExtensions {
    GlobalExtensions()
        : dns(80)
        , dns_with_ssl(443)
        , ch_mh_lb(CONS_HASH_LB_MURMUR3)
        , ch_md5_lb(CONS_HASH_LB_MD5)
        , ch_ketama_lb(CONS_HASH_LB_KETAMA)
        , constant_cl(0) {
    }
    
#ifdef BAIDU_INTERNAL
    BaiduNamingService bns;
#endif
    FileNamingService fns;
    ListNamingService lns;
    DomainListNamingService dlns;
    DomainNamingService dns;
    DomainNamingService dns_with_ssl;
    RemoteFileNamingService rfns;
    ConsulNamingService cns;
    DiscoveryNamingService dcns;
    NacosNamingService nns;

    RoundRobinLoadBalancer rr_lb;
    WeightedRoundRobinLoadBalancer wrr_lb;
    RandomizedLoadBalancer randomized_lb;
    WeightedRandomizedLoadBalancer wr_lb;
    LocalityAwareLoadBalancer la_lb;
    ConsistentHashingLoadBalancer ch_mh_lb;
    ConsistentHashingLoadBalancer ch_md5_lb;
    ConsistentHashingLoadBalancer ch_ketama_lb;
    DynPartLoadBalancer dynpart_lb;

    AutoConcurrencyLimiter auto_cl;
    ConstantConcurrencyLimiter constant_cl;
    TimeoutConcurrencyLimiter timeout_cl;
};

static pthread_once_t register_extensions_once = PTHREAD_ONCE_INIT;
static GlobalExtensions* g_ext = NULL;

static long ReadPortOfDummyServer(const char* filename) {
    butil::fd_guard fd(open(filename, O_RDONLY));
    if (fd < 0) {
        LOG(ERROR) << "Fail to open `" << DUMMY_SERVER_PORT_FILE << "'";
        return -1;
    }
    char port_str[32];
    const ssize_t nr = read(fd, port_str, sizeof(port_str));
    if (nr <= 0) {
        LOG(ERROR) << "Fail to read `" << DUMMY_SERVER_PORT_FILE << "': "
                   << (nr == 0 ? "nothing to read" : berror());
        return -1;
    }
    port_str[std::min((size_t)nr, sizeof(port_str)-1)] = '\0';
    const char* p = port_str;
    for (; isspace(*p); ++p) {}
    char* endptr = NULL;
    const long port = strtol(p, &endptr, 10);
    for (; isspace(*endptr); ++endptr) {}
    if (*endptr != '\0') {
        LOG(ERROR) << "Invalid port=`" << port_str << "'";
        return -1;
    }
    return port;
}

// Expose counters of butil::IOBuf
static int64_t GetIOBufBlockCount(void*) {
    return butil::IOBuf::block_count();
}
static int64_t GetIOBufBlockCountHitTLSThreshold(void*) {
    return butil::IOBuf::block_count_hit_tls_threshold();
}
static int64_t GetIOBufNewBigViewCount(void*) {
    return butil::IOBuf::new_bigview_count();
}
static int64_t GetIOBufBlockMemory(void*) {
    return butil::IOBuf::block_memory();
}

// Defined in server.cpp
extern butil::static_atomic<int> g_running_server_count;
static int GetRunningServerCount(void*) {
    return g_running_server_count.load(butil::memory_order_relaxed);
}

// Update global stuff periodically.
static void* GlobalUpdate(void*) {
    // Expose variables.
    bvar::PassiveStatus<int64_t> var_iobuf_block_count(
        "iobuf_block_count", GetIOBufBlockCount, NULL);
    bvar::PassiveStatus<int64_t> var_iobuf_block_count_hit_tls_threshold(
        "iobuf_block_count_hit_tls_threshold",
        GetIOBufBlockCountHitTLSThreshold, NULL);
    bvar::PassiveStatus<int64_t> var_iobuf_new_bigview_count(
        GetIOBufNewBigViewCount, NULL);
    bvar::PerSecond<bvar::PassiveStatus<int64_t> > var_iobuf_new_bigview_second(
        "iobuf_newbigview_second", &var_iobuf_new_bigview_count);
    bvar::PassiveStatus<int64_t> var_iobuf_block_memory(
        "iobuf_block_memory", GetIOBufBlockMemory, NULL);
    bvar::PassiveStatus<int> var_running_server_count(
        "rpc_server_count", GetRunningServerCount, NULL);

    butil::FileWatcher fw;
    if (fw.init_from_not_exist(DUMMY_SERVER_PORT_FILE) < 0) {
        LOG(FATAL) << "Fail to init FileWatcher on `" << DUMMY_SERVER_PORT_FILE << "'";
        return NULL;
    }

    std::vector<SocketId> conns;
    const int64_t start_time_us = butil::gettimeofday_us();
    const int WARN_NOSLEEP_THRESHOLD = 2;
    int64_t last_time_us = start_time_us;
    int consecutive_nosleep = 0;
    int64_t last_return_free_memory_time = start_time_us;
    while (1) {
        const int64_t sleep_us = 1000000L + last_time_us - butil::gettimeofday_us();
        if (sleep_us > 0) {
            if (bthread_usleep(sleep_us) < 0) {
                PLOG_IF(FATAL, errno != ESTOP) << "Fail to sleep";
                break;
            }
            consecutive_nosleep = 0;
        } else {
            if (++consecutive_nosleep >= WARN_NOSLEEP_THRESHOLD) {
                consecutive_nosleep = 0;
                LOG(WARNING) << __FUNCTION__ << " is too busy!";
            }
        }
        last_time_us = butil::gettimeofday_us();

        TrackMe();

        if (!IsDummyServerRunning()
            && g_running_server_count.load(butil::memory_order_relaxed) == 0
            && fw.check_and_consume() > 0) {
            long port = ReadPortOfDummyServer(DUMMY_SERVER_PORT_FILE);
            if (port >= 0) {
                StartDummyServerAt(port);
            }
        }

        SocketMapList(&conns);
        const int64_t now_ms = butil::cpuwide_time_ms();
        for (size_t i = 0; i < conns.size(); ++i) {
            SocketUniquePtr ptr;
            if (Socket::Address(conns[i], &ptr) == 0) {
                ptr->UpdateStatsEverySecond(now_ms);
            }
        }

        const int return_mem_interval =
            FLAGS_free_memory_to_system_interval/*reloadable*/;
        if (return_mem_interval > 0 &&
            last_time_us >= last_return_free_memory_time +
            return_mem_interval * 1000000L) {
            last_return_free_memory_time = last_time_us;
            // TODO: Calling MallocExtension::instance()->ReleaseFreeMemory may
            // crash the program in later calls to malloc, verified on tcmalloc
            // 1.7 and 2.5, which means making the static member function weak
            // in details/tcmalloc_extension.cpp is probably not correct, however
            // it does work for heap profilers.
            if (MallocExtension_ReleaseFreeMemory != NULL) {
                MallocExtension_ReleaseFreeMemory();
            } else {
#if defined(OS_LINUX)
                // GNU specific.
                malloc_trim(10 * 1024 * 1024/*leave 10M pad*/);
#endif
            }
        }
    }
    return NULL;
}

#if GOOGLE_PROTOBUF_VERSION < 3022000
static void BaiduStreamingLogHandler(google::protobuf::LogLevel level,
                                     const char* filename, int line,
                                     const std::string& message) {
    switch (level) {
    case google::protobuf::LOGLEVEL_INFO:
        LOG(INFO) << filename << ':' << line << ' ' << message;
        return;
    case google::protobuf::LOGLEVEL_WARNING:
        LOG(WARNING) << filename << ':' << line << ' ' << message;
        return;
    case google::protobuf::LOGLEVEL_ERROR:
        LOG(ERROR) << filename << ':' << line << ' ' << message;
        return;
    case google::protobuf::LOGLEVEL_FATAL:
        LOG(FATAL) << filename << ':' << line << ' ' << message;
        return;
    }
    CHECK(false) << filename << ':' << line << ' ' << message;
}
#endif

#if BRPC_WITH_URMA
static u_external_mutex_t* brpc_external_lock_create(u_external_mutex_type type)
{
    if (type == LT_RECURSIVE) {
        LOG(ERROR) << "Error to execute external_lock_create for LT_RECURSIVE is not supported in brpc";
        return nullptr;
    }
    auto* mutex = new(std::nothrow) bthread::Mutex();
    if (mutex == nullptr) {
        LOG(ERROR) << "Error when create mutex";
        return nullptr;
    }
    return reinterpret_cast<u_external_mutex_t*>(mutex);
}

static int brpc_external_lock_destroy(u_external_mutex_t *m)
{
    if (m == nullptr) {
        LOG(ERROR) << "Error to execute external_lock_destroy for the pointer is nullptr";
        return -1;
    }
    delete reinterpret_cast<bthread::Mutex*>(m);
    return 0;
}

static int brpc_external_lock_lock(u_external_mutex_t *m)
{
    if (m == nullptr) {
        LOG(ERROR) << "Error to execute external_lock_lock for the pointer is nullptr";
        return -1;
    }
    (reinterpret_cast<bthread::Mutex*>(m))->lock();
    return 0;
}

static int brpc_external_lock_unlock(u_external_mutex_t *m)
{
    if (m == nullptr) {
        LOG(ERROR) << "Error to execute external_lock_unlock for the pointer is nullptr";
        return -1;
    }
    (reinterpret_cast<bthread::Mutex*>(m))->unlock();
    return 0;
}

static int brpc_external_lock_try_lock(u_external_mutex_t *m)
{
    if (m == nullptr) {
        LOG(ERROR) << "Error to execute external_lock_try_lock for the pointer is nullptr";
        return -1;
    }
    return (reinterpret_cast<bthread::Mutex*>(m))->try_lock() ? 0 : -1;
}

static u_rw_lock_t* brpc_rw_lock_create()
{
    auto* rwlock = new(std::nothrow) bthread::RWLock();
    if (rwlock == nullptr) {
        LOG(ERROR) << "Error when create rwlock";
        return nullptr;
    }
    return reinterpret_cast<u_rw_lock_t*>(rwlock);
}

static int brpc_rw_lock_destroy(u_rw_lock_t *m)
{
    if (m == nullptr) {
        LOG(ERROR) << "Error to execute rw_lock_destroy for the pointer is nullptr";
        return -1;
    }
    delete reinterpret_cast<bthread::RWLock*>(m);
    return 0;
}

static int brpc_rw_lock_lock_read(u_rw_lock_t *m)
{
    if (m == nullptr) {
        LOG(ERROR) << "Error to execute rw_lock_lock_read for the pointer is nullptr";
        return -1;
    }
    (reinterpret_cast<bthread::RWLock*>(m))->rdlock();
    return 0;
}

static int brpc_rw_lock_lock_write(u_rw_lock_t *m)
{
    if (m == nullptr) {
        LOG(ERROR) << "Error to execute rw_lock_lock_write for the pointer is nullptr";
        return -1;
    }
    (reinterpret_cast<bthread::RWLock*>(m))->wrlock();
    return 0;
}

static int brpc_rw_lock_unlock_rw(u_rw_lock_t *m)
{
    if (m == nullptr) {
        LOG(ERROR) << "Error to execute rw_lock_unlock_rw for the pointer is nullptr";
        return -1;
    }
    (reinterpret_cast<bthread::RWLock*>(m))->unlock();
    return 0;
}

static int brpc_rw_lock_try_lock_read(u_rw_lock_t *m)
{
    if (m == nullptr) {
        LOG(ERROR) << "Error to execute rw_lock_try_lock_read for the pointer is nullptr";
        return -1;
    }
    return (reinterpret_cast<bthread::RWLock*>(m))->try_rdlock() ? 0 : -1;
}

static int brpc_rw_lock_try_lock_write(u_rw_lock_t *m)
{
    if (m == nullptr) {
        LOG(ERROR) << "Error to execute rw_lock_try_lock_write for the pointer is nullptr";
        return -1;
    }
    return (reinterpret_cast<bthread::RWLock*>(m))->try_wrlock() ? 0 : -1;
}

static u_semaphore_t* brpc_semaphore_create()
{
    auto* sem = new(std::nothrow) bthread_sem_t();
    if (sem == nullptr) {
        LOG(ERROR) << "Error when create sem";
        return nullptr;
    }
    return reinterpret_cast<u_semaphore_t*>(sem);
}

static int brpc_semaphore_destroy(u_semaphore_t *s)
{
    if (s == nullptr) {
        LOG(ERROR) << "Error to execute semaphore_destroy for the pointer is nullptr";
        return -1;
    }
    if (int ret = bthread_sem_destroy(reinterpret_cast<bthread_sem_t*>(s)) != 0) {
        LOG(ERROR) << "Error to execute bthread_sem_destroy, ret: " << ret;
        return ret;
    }
    delete reinterpret_cast<bthread_sem_t*>(s);
    return 0;
}

static int brpc_semaphore_init(u_semaphore_t *s, int shared, unsigned int value)
{
    if (s == nullptr) {
        LOG(ERROR) << "Error to execute semaphore_init for the pointer is nullptr";
        return -1;
    }
    return bthread_sem_init(reinterpret_cast<bthread_sem_t*>(s), value);
}

static int brpc_semaphore_wait(u_semaphore_t *s)
{
    if (s == nullptr) {
        LOG(ERROR) << "Error to execute semaphore_wait for the pointer is nullptr";
        return -1;
    }
    return bthread_sem_wait(reinterpret_cast<bthread_sem_t*>(s));
}

static int brpc_semaphore_post(u_semaphore_t *s)
{
    if (s == nullptr) {
        LOG(ERROR) << "Error to execute semaphore_post for the pointer is nullptr";
        return -1;
    }
    return bthread_sem_post(reinterpret_cast<bthread_sem_t*>(s));
}

u_external_lock_ops_t brpc_external_lock_ops = {
    .create = brpc_external_lock_create,
    .destroy = brpc_external_lock_destroy,
    .lock = brpc_external_lock_lock,
    .unlock = brpc_external_lock_unlock,
    .try_lock = brpc_external_lock_try_lock
};

u_rw_lock_ops_t brpc_rw_lock_ops = {
    .create = brpc_rw_lock_create,
    .destroy = brpc_rw_lock_destroy,
    .lock_read = brpc_rw_lock_lock_read,
    .lock_write = brpc_rw_lock_lock_write,
    .unlock_rw = brpc_rw_lock_unlock_rw,
    .try_lock_read = brpc_rw_lock_try_lock_read,
    .try_lock_write = brpc_rw_lock_try_lock_write
};

u_semaphore_ops_t brpc_semaphore_ops = {
    .create = brpc_semaphore_create,
    .destroy = brpc_semaphore_destroy,
    .init = brpc_semaphore_init,
    .wait = brpc_semaphore_wait,
    .post = brpc_semaphore_post
};

static void SetUbSocketEnv() {
    if (getenv("LD_PRELOAD") != nullptr) {
        return;
    }

    if (!FLAGS_ubsocket_trans_mode.empty()) {
        ::setenv("UBSOCKET_TRANS_MODE", FLAGS_ubsocket_trans_mode.c_str(), 1);
    }
    if (!FLAGS_ubsocket_dev_name.empty()) {
        ::setenv("UBSOCKET_DEV_NAME", FLAGS_ubsocket_dev_name.c_str(), 1);
    }
    if (!FLAGS_ubsocket_dev_ip.empty()) {
        ::setenv("UBSOCKET_DEV_IP", FLAGS_ubsocket_dev_ip.c_str(), 1);
    }
    if (!FLAGS_ubsocket_eid_idx.empty()) {
        ::setenv("UBSOCKET_EID_IDX", FLAGS_ubsocket_eid_idx.c_str(), 1);
    }
    if (!FLAGS_ubsocket_src_eid.empty()) {
        ::setenv("UBSOCKET_SRC_EID", FLAGS_ubsocket_src_eid.c_str(), 1);
    }
    if (!FLAGS_ubsocket_log_level.empty()) {
        ::setenv("UBSOCKET_LOG_LEVEL", FLAGS_ubsocket_log_level.c_str(), 1);
    }
    if (!FLAGS_ubsocket_log_use_printf.empty()) {
        ::setenv("UBSOCKET_LOG_USE_PRINTF", FLAGS_ubsocket_log_use_printf.c_str(), 1);
    }
    if (!FLAGS_ubsocket_tx_depth.empty()) {
        ::setenv("UBSOCKET_TX_DEPTH", FLAGS_ubsocket_tx_depth.c_str(), 1);
    }
    if (!FLAGS_ubsocket_rx_depth.empty()) {
        ::setenv("UBSOCKET_RX_DEPTH", FLAGS_ubsocket_rx_depth.c_str(), 1);
    }
    if (!FLAGS_ubsocket_block_type.empty()) {
        ::setenv("UBSOCKET_BLOCK_TYPE", FLAGS_ubsocket_block_type.c_str(), 1);
    }
    if (!FLAGS_ubsocket_pool_initial_size.empty()) {
        ::setenv("UBSOCKET_POOL_INITIAL_SIZE", FLAGS_ubsocket_pool_initial_size.c_str(), 1);
    }
    if (!FLAGS_ubsocket_pool_max_size.empty()) {
        ::setenv("UBSOCKET_POOL_MAX_SIZE", FLAGS_ubsocket_pool_max_size.c_str(), 1);
    }
    if (!FLAGS_ubsocket_buf_pool_depth.empty()) {
        ::setenv("UBSOCKET_BUF_POOL_DEPTH", FLAGS_ubsocket_buf_pool_depth.c_str(), 1);
    }
    if (!FLAGS_ubsocket_schedule_policy.empty()) {
        ::setenv("UBSOCKET_SCHEDULE_POLICY", FLAGS_ubsocket_schedule_policy.c_str(), 1);
    }
    if (!FLAGS_ubsocket_readv_unlimited.empty()) {
        ::setenv("UBSOCKET_READV_UNLIMITED", FLAGS_ubsocket_readv_unlimited.c_str(), 1);
    }
    if (!FLAGS_ubsocket_use_polling.empty()) {
        ::setenv("UBSOCKET_USE_POLLING", FLAGS_ubsocket_use_polling.c_str(), 1);
    }
    if (!FLAGS_ubsocket_brpc_alloc_sym.empty()) {
        ::setenv("UBSOCKET_BRPC_ALLOC_SYM", FLAGS_ubsocket_brpc_alloc_sym.c_str(), 1);
    }
    if (!FLAGS_ubsocket_brpc_dealloc_sym.empty()) {
        ::setenv("UBSOCKET_BRPC_DEALLOC_SYM", FLAGS_ubsocket_brpc_dealloc_sym.c_str(), 1);
    }
    if (!FLAGS_ubsocket_adpt_stats.empty()) {
        ::setenv("UBSOCKET_STATS_CLI", FLAGS_ubsocket_adpt_stats.c_str(), 1);
    }

    if (!FLAGS_ubsocket_auto_fallback_tcp.empty()) {
        ::setenv("UBSOCKET_AUTO_FALLBACK_TCP", FLAGS_ubsocket_auto_fallback_tcp.c_str(), 1);
    }

    if (!FLAGS_ubsocket_enable_share_jfr.empty()) {
        ::setenv("UBSOCKET_ENABLE_SHARE_JFR", FLAGS_ubsocket_enable_share_jfr.c_str(), 1);
    }

    if (!FLAGS_ubsocket_share_jfr_rx_queue_depth.empty()) {
        ::setenv("UBSOCKET_SHARE_JFR_RX_QUEUE_DEPTH", FLAGS_ubsocket_share_jfr_rx_queue_depth.c_str(), 1);
    }
    if (!FLAGS_ubsocket_trace_enable.empty()) {
        ::setenv("UBSOCKET_TRACE_ENABLE", FLAGS_ubsocket_trace_enable.c_str(), 1);
    }
    if (!FLAGS_ubsocket_trace_time.empty()) {
        ::setenv("UBSOCKET_TRACE_TIME", FLAGS_ubsocket_trace_time.c_str(), 1);
    }
    if (!FLAGS_ubsocket_trace_file_path.empty()) {
        ::setenv("UBSOCKET_TRACE_FILE_PATH", FLAGS_ubsocket_trace_file_path.c_str(), 1);
    }
    if (!FLAGS_ubsocket_trace_file_size.empty()) {
        ::setenv("UBSOCKET_TRACE_FILE_SIZE", FLAGS_ubsocket_trace_file_size.c_str(), 1);
    }
    if (!FLAGS_ubsocket_stats_cli.empty()) {
        ::setenv("UBSOCKET_STATS_CLI", FLAGS_ubsocket_stats_cli.c_str(), 1);
    }
    if (!FLAGS_ubsocket_ub_trans_mode.empty()) {
        ::setenv("UBSOCKET_UB_TRANS_MODE", FLAGS_ubsocket_ub_trans_mode.c_str(), 1);
    }
    if (!FLAGS_ubsocket_min_reserved_credit.empty()) {
        ::setenv("UBSOCKET_MIN_RESERVED_CREDIT", FLAGS_ubsocket_min_reserved_credit.c_str(), 1);
    }
    if (!FLAGS_ubsocket_link_priority.empty()) {
        ::setenv("UBSOCKET_LINK_PRIORITY", FLAGS_ubsocket_link_priority.c_str(), 1);
    }
    if (!FLAGS_ubsocket_degrade.empty()) {
        ::setenv("UBSOCKET_DEGRADE", FLAGS_ubsocket_degrade.c_str(), 1);
    }
    if (!FLAGS_ubsocket_async_accept.empty()) {
        ::setenv("UBSOCKET_ASYNC_ACCEPT", FLAGS_ubsocket_async_accept.c_str(), 1);
    }
    if (!FLAGS_ubsocket_thread_pool_size.empty()) {
        ::setenv("UBSOCKET_THREAD_POOL_SIZE", FLAGS_ubsocket_thread_pool_size.c_str(), 1);
    }
    if (!FLAGS_ubsocket_probe_enable.empty()) {
        ::setenv("UBSOCKET_PROBE_ENABLE", FLAGS_ubsocket_probe_enable.c_str(), 1);
    }
    if (!FLAGS_ubsocket_probe_time_ms.empty()) {
        ::setenv("UBSOCKET_PROBE_TIME_MS", FLAGS_ubsocket_probe_time_ms.c_str(), 1);
    }
    if (!FLAGS_ubsocket_probe_batch.empty()) {
        ::setenv("UBSOCKET_PROBE_BATCH", FLAGS_ubsocket_probe_batch.c_str(), 1);
    }
	if (!FLAGS_ubsocket_ub_epoll_enable.empty()) {
		::setenv("UBSOCKET_UB_EPOLL_ENABLE", FLAGS_ubsocket_ub_epoll_enable.c_str(), 1);
	}
    ::setenv("UBSOCKET_USE_UB_FORCE", "false", 1);
    u_register_external_lock_ops(&brpc_external_lock_ops);
    u_register_rw_lock_ops(&brpc_rw_lock_ops);
    u_register_semaphore_ops(&brpc_semaphore_ops);
    if (Brpc::Context::GetContext() != nullptr) {
        Brpc::Context::SetUbEnable();
    }
    Brpc::ExecutorService::GetExecutorService()->Start();
}
#endif

static void GlobalInitializeOrDieImpl() {
    //////////////////////////////////////////////////////////////////
    // Be careful about usages of gflags inside this function which //
    // may be called before main() only seeing gflags with default  //
    // values even if the gflags will be set after main().          //
    //////////////////////////////////////////////////////////////////

#if BRPC_WITH_URMA
    if (FLAGS_ubsocket_enable) {
        SetUbSocketEnv();
    }
#endif

    // Ignore SIGPIPE.
    struct sigaction oldact;
    if (sigaction(SIGPIPE, NULL, &oldact) != 0 ||
            (oldact.sa_handler == NULL && oldact.sa_sigaction == NULL)) {
        CHECK(SIG_ERR != signal(SIGPIPE, SIG_IGN));
    }

#if GOOGLE_PROTOBUF_VERSION < 3022000
    // Make GOOGLE_LOG print to comlog device
    SetLogHandler(&BaiduStreamingLogHandler);
#endif

    // Set bthread create span function
    bthread_set_create_span_func(CreateBthreadSpan);

    // Setting the variable here does not work, the profiler probably check
    // the variable before main() for only once.
    // setenv("TCMALLOC_SAMPLE_PARAMETER", "524288", 0);

    // Initialize openssl library
    SSL_library_init();
    // RPC doesn't require openssl.cnf, users can load it by themselves if needed
    SSL_load_error_strings();
    if (SSLThreadInit() != 0 || SSLDHInit() != 0) {
        exit(1);
    }

    // Defined in http_rpc_protocol.cpp
    InitCommonStrings();

    // Leave memory of these extensions to process's clean up.
    g_ext = new(std::nothrow) GlobalExtensions();
    if (NULL == g_ext) {
        exit(1);
    }
    // Naming Services
#ifdef BAIDU_INTERNAL
    NamingServiceExtension()->RegisterOrDie("bns", &g_ext->bns);
#endif
    NamingServiceExtension()->RegisterOrDie("file", &g_ext->fns);
    NamingServiceExtension()->RegisterOrDie("list", &g_ext->lns);
    NamingServiceExtension()->RegisterOrDie("dlist", &g_ext->dlns);
    NamingServiceExtension()->RegisterOrDie("http", &g_ext->dns);
    NamingServiceExtension()->RegisterOrDie("https", &g_ext->dns_with_ssl);
    NamingServiceExtension()->RegisterOrDie("redis", &g_ext->dns);
    NamingServiceExtension()->RegisterOrDie("remotefile", &g_ext->rfns);
    NamingServiceExtension()->RegisterOrDie("consul", &g_ext->cns);
    NamingServiceExtension()->RegisterOrDie("discovery", &g_ext->dcns);
    NamingServiceExtension()->RegisterOrDie("nacos", &g_ext->nns);

    // Load Balancers
    LoadBalancerExtension()->RegisterOrDie("rr", &g_ext->rr_lb);
    LoadBalancerExtension()->RegisterOrDie("wrr", &g_ext->wrr_lb);
    LoadBalancerExtension()->RegisterOrDie("random", &g_ext->randomized_lb);
    LoadBalancerExtension()->RegisterOrDie("wr", &g_ext->wr_lb);
    LoadBalancerExtension()->RegisterOrDie("la", &g_ext->la_lb);
    LoadBalancerExtension()->RegisterOrDie("c_murmurhash", &g_ext->ch_mh_lb);
    LoadBalancerExtension()->RegisterOrDie("c_md5", &g_ext->ch_md5_lb);
    LoadBalancerExtension()->RegisterOrDie("c_ketama", &g_ext->ch_ketama_lb);
    LoadBalancerExtension()->RegisterOrDie("_dynpart", &g_ext->dynpart_lb);

    // Compress Handlers
    CompressHandler gzip_compress = { GzipCompress, GzipDecompress, "gzip" };
    if (RegisterCompressHandler(COMPRESS_TYPE_GZIP, gzip_compress) != 0) {
        exit(1);
    }
    CompressHandler zlib_compress = { ZlibCompress, ZlibDecompress, "zlib" };
    if (RegisterCompressHandler(COMPRESS_TYPE_ZLIB, zlib_compress) != 0) {
        exit(1);
    }
    CompressHandler snappy_compress = { SnappyCompress, SnappyDecompress, "snappy" };
    if (RegisterCompressHandler(COMPRESS_TYPE_SNAPPY, snappy_compress) != 0) {
        exit(1);
    }

    // Checksum Handlers
    const ChecksumHandler crc32c_checksum = {Crc32cCompute, Crc32cVerify,
                                             "crc32c"};
    if (RegisterChecksumHandler(CHECKSUM_TYPE_CRC32C, crc32c_checksum) != 0) {
        exit(1);
    }

    // Protocols
    Protocol baidu_protocol = { ParseRpcMessage,
                                SerializeRpcRequest, PackRpcRequest,
                                ProcessRpcRequest, ProcessRpcResponse,
                                VerifyRpcRequest, NULL, NULL,
                                CONNECTION_TYPE_ALL, "baidu_std" };
    if (RegisterProtocol(PROTOCOL_BAIDU_STD, baidu_protocol) != 0) {
        exit(1);
    }

    Protocol streaming_protocol = { ParseStreamingMessage,
                                    NULL, NULL, ProcessStreamingMessage,
                                    ProcessStreamingMessage,
                                    NULL, NULL, NULL,
                                    CONNECTION_TYPE_SINGLE, "streaming_rpc" };

    if (RegisterProtocol(PROTOCOL_STREAMING_RPC, streaming_protocol) != 0) {
        exit(1);
    }

    Protocol http_protocol = { ParseHttpMessage,
                               SerializeHttpRequest, PackHttpRequest,
                               ProcessHttpRequest, ProcessHttpResponse,
                               VerifyHttpRequest, ParseHttpServerAddress,
                               GetHttpMethodName,
                               CONNECTION_TYPE_POOLED_AND_SHORT,
                               "http" };
    if (RegisterProtocol(PROTOCOL_HTTP, http_protocol) != 0) {
        exit(1);
    }

    Protocol http2_protocol = { ParseH2Message,
                                SerializeHttpRequest, PackH2Request,
                                ProcessHttpRequest, ProcessHttpResponse,
                                VerifyHttpRequest, ParseHttpServerAddress,
                                GetHttpMethodName,
                                CONNECTION_TYPE_SINGLE,
                                "h2" };
    if (RegisterProtocol(PROTOCOL_H2, http2_protocol) != 0) {
        exit(1);
    }

    Protocol hulu_protocol = { ParseHuluMessage,
                               SerializeRequestDefault, PackHuluRequest,
                               ProcessHuluRequest, ProcessHuluResponse,
                               VerifyHuluRequest, NULL, NULL,
                               CONNECTION_TYPE_ALL, "hulu_pbrpc" };
    if (RegisterProtocol(PROTOCOL_HULU_PBRPC, hulu_protocol) != 0) {
        exit(1);
    }

    // Only valid at client side
    Protocol nova_protocol = { ParseNsheadMessage,
                               SerializeNovaRequest, PackNovaRequest,
                               NULL, ProcessNovaResponse,
                               NULL, NULL, NULL,
                               CONNECTION_TYPE_POOLED_AND_SHORT,  "nova_pbrpc" };
    if (RegisterProtocol(PROTOCOL_NOVA_PBRPC, nova_protocol) != 0) {
        exit(1);
    }

    // Only valid at client side
    Protocol public_pbrpc_protocol = { ParseNsheadMessage,
                                       SerializePublicPbrpcRequest,
                                       PackPublicPbrpcRequest,
                                       NULL, ProcessPublicPbrpcResponse,
                                       NULL, NULL, NULL,
                                       // public_pbrpc server implementation
                                       // doesn't support full duplex
                                       CONNECTION_TYPE_POOLED_AND_SHORT,
                                       "public_pbrpc" };
    if (RegisterProtocol(PROTOCOL_PUBLIC_PBRPC, public_pbrpc_protocol) != 0) {
        exit(1);
    }

    Protocol sofa_protocol = { ParseSofaMessage,
                               SerializeRequestDefault, PackSofaRequest,
                               ProcessSofaRequest, ProcessSofaResponse,
                               VerifySofaRequest, NULL, NULL,
                               CONNECTION_TYPE_ALL, "sofa_pbrpc" };
    if (RegisterProtocol(PROTOCOL_SOFA_PBRPC, sofa_protocol) != 0) {
        exit(1);
    }

    // Only valid at server side. We generalize all the protocols that
    // prefixes with nshead as `nshead_protocol' and specify the content
    // parsing after nshead by ServerOptions.nshead_service.
    Protocol nshead_protocol = { ParseNsheadMessage,
                                 SerializeNsheadRequest, PackNsheadRequest,
                                 ProcessNsheadRequest, ProcessNsheadResponse,
                                 VerifyNsheadRequest, NULL, NULL,
                                 CONNECTION_TYPE_POOLED_AND_SHORT, "nshead" };
    if (RegisterProtocol(PROTOCOL_NSHEAD, nshead_protocol) != 0) {
        exit(1);
    }

    Protocol mc_binary_protocol = { ParseMemcacheMessage,
                                    SerializeMemcacheRequest,
                                    PackMemcacheRequest,
                                    NULL, ProcessMemcacheResponse,
                                    NULL, NULL, GetMemcacheMethodName,
                                    CONNECTION_TYPE_ALL, "memcache" };
    if (RegisterProtocol(PROTOCOL_MEMCACHE, mc_binary_protocol) != 0) {
        exit(1);
    }

    Protocol redis_protocol = { ParseRedisMessage,
                                SerializeRedisRequest,
                                PackRedisRequest,
                                ProcessRedisRequest, ProcessRedisResponse,
                                NULL, NULL, GetRedisMethodName,
                                CONNECTION_TYPE_ALL, "redis" };
    if (RegisterProtocol(PROTOCOL_REDIS, redis_protocol) != 0) {
        exit(1);
    }

    Protocol mongo_protocol = { ParseMongoMessage,
                                NULL, NULL,
                                ProcessMongoRequest, NULL,
                                NULL, NULL, NULL,
                                CONNECTION_TYPE_POOLED, "mongo" };
    if (RegisterProtocol(PROTOCOL_MONGO, mongo_protocol) != 0) {
        exit(1);
    }

// Use Macro is more straight forward than weak link technology(becasue of static link issue)
#ifdef ENABLE_THRIFT_FRAMED_PROTOCOL
    Protocol thrift_binary_protocol = {
        policy::ParseThriftMessage,
        policy::SerializeThriftRequest, policy::PackThriftRequest,
        policy::ProcessThriftRequest, policy::ProcessThriftResponse,
        policy::VerifyThriftRequest, NULL, NULL,
        CONNECTION_TYPE_POOLED_AND_SHORT, "thrift" };
    if (RegisterProtocol(PROTOCOL_THRIFT, thrift_binary_protocol) != 0) {
        exit(1);
    }
#endif

    // Only valid at client side
    Protocol ubrpc_compack_protocol = {
        ParseNsheadMessage,
        SerializeUbrpcCompackRequest, PackUbrpcRequest,
        NULL, ProcessUbrpcResponse,
        NULL, NULL, NULL,
        CONNECTION_TYPE_POOLED_AND_SHORT,  "ubrpc_compack" };
    if (RegisterProtocol(PROTOCOL_UBRPC_COMPACK, ubrpc_compack_protocol) != 0) {
        exit(1);
    }
    Protocol ubrpc_mcpack2_protocol = {
        ParseNsheadMessage,
        SerializeUbrpcMcpack2Request, PackUbrpcRequest,
        NULL, ProcessUbrpcResponse,
        NULL, NULL, NULL,
        CONNECTION_TYPE_POOLED_AND_SHORT,  "ubrpc_mcpack2" };
    if (RegisterProtocol(PROTOCOL_UBRPC_MCPACK2, ubrpc_mcpack2_protocol) != 0) {
        exit(1);
    }

    // Only valid at client side
    Protocol nshead_mcpack_protocol = {
        ParseNsheadMessage,
        SerializeNsheadMcpackRequest, PackNsheadMcpackRequest,
        NULL, ProcessNsheadMcpackResponse,
        NULL, NULL, NULL,
        CONNECTION_TYPE_POOLED_AND_SHORT,  "nshead_mcpack" };
    if (RegisterProtocol(PROTOCOL_NSHEAD_MCPACK, nshead_mcpack_protocol) != 0) {
        exit(1);
    }

    Protocol rtmp_protocol = {
        ParseRtmpMessage,
        SerializeRtmpRequest, PackRtmpRequest,
        ProcessRtmpMessage, ProcessRtmpMessage,
        NULL, NULL, NULL,
        (ConnectionType)(CONNECTION_TYPE_SINGLE|CONNECTION_TYPE_SHORT),
        "rtmp" };
    if (RegisterProtocol(PROTOCOL_RTMP, rtmp_protocol) != 0) {
        exit(1);
    }

    Protocol esp_protocol = {
        ParseEspMessage,
        SerializeEspRequest, PackEspRequest,
        NULL, ProcessEspResponse,
        NULL, NULL, NULL,
        CONNECTION_TYPE_POOLED_AND_SHORT, "esp"};
    if (RegisterProtocol(PROTOCOL_ESP, esp_protocol) != 0) {
        exit(1);
    }

    std::vector<Protocol> protocols;
    ListProtocols(&protocols);
    for (size_t i = 0; i < protocols.size(); ++i) {
        if (protocols[i].process_response) {
            InputMessageHandler handler;
            // `process_response' is required at client side
            handler.parse = protocols[i].parse;
            handler.process = protocols[i].process_response;
            // No need to verify at client side
            handler.verify = NULL;
            handler.arg = NULL;
            handler.name = protocols[i].name;
            if (get_or_new_client_side_messenger()->AddHandler(handler) != 0) {
                exit(1);
            }
        }
    }

    // Concurrency Limiters
    ConcurrencyLimiterExtension()->RegisterOrDie("auto", &g_ext->auto_cl);
    ConcurrencyLimiterExtension()->RegisterOrDie("constant", &g_ext->constant_cl);
    ConcurrencyLimiterExtension()->RegisterOrDie("timeout", &g_ext->timeout_cl);

    if (FLAGS_usercode_in_pthread) {
        // Optional. If channel/server are initialized before main(), this
        // flag may be false at here even if it will be set to true after
        // main(). In which case, the usercode pool will not be initialized
        // until the pool is used.
        InitUserCodeBackupPoolOnceOrDie();
    }

    // We never join GlobalUpdate, let it quit with the process.
    bthread_t th;
    CHECK(bthread_start_background(&th, NULL, GlobalUpdate, NULL) == 0)
        << "Fail to start GlobalUpdate";
}

void GlobalInitializeOrDie() {
    if (pthread_once(&register_extensions_once,
                     GlobalInitializeOrDieImpl) != 0) {
        LOG(FATAL) << "Fail to pthread_once";
        exit(1);
    }
}

} // namespace brpc
