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

#if BRPC_WITH_URMA
#include "brpc/log.h"
#include "bthread/rwlock.h"
#include "bthread/bthread.h"
#include "ubsocket.h"

DECLARE_bool(ubsocket_enable);

namespace brpc {

///////////////////////////////////////////////////////////////////////////////
// Parses gflags parameters and applies the configuration to UBSocket.
///////////////////////////////////////////////////////////////////////////////
DEFINE_string(ubsocket_trans_mode, "ub", "Transport mode for ubsocket (e.g., 'ub', 'ib')");
DEFINE_string(ubsocket_dev_name, "", "Device name for ubsocket (e.g., 'udma2', 'bonding_dev_0')");
DEFINE_string(ubsocket_dev_ip, "", "Device ip for ubsocket");
DEFINE_string(ubsocket_eid_idx, "", "Normal device eid idx for ubsocket, necessary while using ub. Obtained by querying with the 'urma_admin show' command");
DEFINE_string(ubsocket_src_eid, "", "Bonding device eid idx for ubsocket, necessary while using ub. Obtained by querying with the 'urma_admin show' command");
DEFINE_string(ubsocket_log_level, "info", "Log level for ubsocket (e.g., 'error', 'warn', 'notice', 'info', 'debug')");
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
DEFINE_string(ubsocket_async_epoll_wait, "false", "Allow do epoll wait async; default: false");
DEFINE_string(ubsocket_probe_enable, "false", "Enable ubsocket probe (e.g., 'false', 'true')");
DEFINE_string(ubsocket_probe_time_ms, "1000", "ubsocket probe interval time, the minimum value is 1, the maximum value is 360000");
DEFINE_string(ubsocket_probe_batch, "10", "ubsocket number of sock to probe per batch, the minimum value is 1, the maximum value is 500");
DEFINE_string(ubsocket_ub_epoll_enable, "false", "Whether to enable ub epoll; default: false (optional: false, true)");
DEFINE_string(ubsocket_use_brpc_zcopy, "true", "Whether to enable ub memory pool to support UB zero copy transportation; default: true (optional: false, true)");
DEFINE_string(ubsocket_prof_enable, "false", "Enable ubsocket profiling (e.g., 'false', 'true')");
DEFINE_string(ubsocket_prof_dump_interval_min, "1", "Set dump ubsocket profiling data output interval(minute), the minimum value is 1, the maximum value is 5");
DEFINE_string(ubsocket_prof_dump_path, "/tmp/ubsocket/profiling", "Set dump ubsocket profiling data output path (e.g., '/tmp/ubsocket/profiling')");

static void SetUBSocketEnv() {
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
    if (!FLAGS_ubsocket_async_epoll_wait.empty()) {
        ::setenv("UBSOCKET_ASYNC_EPOLL_WAIT", FLAGS_ubsocket_async_epoll_wait.c_str(), 1);
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
    if (!FLAGS_ubsocket_use_brpc_zcopy.empty()) {
		::setenv("UBSOCKET_USE_BRPC_ZCOPY", FLAGS_ubsocket_use_brpc_zcopy.c_str(), 1);
	}
    if (!FLAGS_ubsocket_prof_enable.empty()) {
        ::setenv("UBSOCKET_PROF_ENABLE", FLAGS_ubsocket_prof_enable.c_str(), 1);
    }
    if (!FLAGS_ubsocket_prof_dump_interval_min.empty()) {
        ::setenv("UBSOCKET_PROF_DUMP_INTERVAL_MIN", FLAGS_ubsocket_prof_dump_interval_min.c_str(), 1);
    }
    if (!FLAGS_ubsocket_prof_dump_path.empty()) {
        ::setenv("UBSOCKET_PROF_DUMP_PATH", FLAGS_ubsocket_prof_dump_path.c_str(), 1);
    }
    ::setenv("UBSOCKET_USE_UB_FORCE", "false", 1);
}

///////////////////////////////////////////////////////////////////////////////
// Register brpc bthread locks and semaphores for UBSocket.
///////////////////////////////////////////////////////////////////////////////
static u_mutex_t* brpc_external_lock_create(u_mutex_type_t type)
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
    return reinterpret_cast<u_mutex_t*>(mutex);
}

static int brpc_external_lock_destroy(u_mutex_t *m)
{
    if (m == nullptr) {
        LOG(ERROR) << "Error to execute external_lock_destroy for the pointer is nullptr";
        return -1;
    }
    delete reinterpret_cast<bthread::Mutex*>(m);
    return 0;
}

static int brpc_external_lock_lock(u_mutex_t *m)
{
    if (m == nullptr) {
        LOG(ERROR) << "Error to execute external_lock_lock for the pointer is nullptr";
        return -1;
    }
    (reinterpret_cast<bthread::Mutex*>(m))->lock();
    return 0;
}

static int brpc_external_lock_unlock(u_mutex_t *m)
{
    if (m == nullptr) {
        LOG(ERROR) << "Error to execute external_lock_unlock for the pointer is nullptr";
        return -1;
    }
    (reinterpret_cast<bthread::Mutex*>(m))->unlock();
    return 0;
}

static int brpc_external_lock_try_lock(u_mutex_t *m)
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

u_external_rw_lock_ops_t brpc_rw_lock_ops = {
    .create = brpc_rw_lock_create,
    .destroy = brpc_rw_lock_destroy,
    .lock_read = brpc_rw_lock_lock_read,
    .lock_write = brpc_rw_lock_lock_write,
    .unlock_rw = brpc_rw_lock_unlock_rw,
    .try_lock_read = brpc_rw_lock_try_lock_read,
    .try_lock_write = brpc_rw_lock_try_lock_write
};

u_external_semaphore_ops_t brpc_semaphore_ops = {
    .create = brpc_semaphore_create,
    .destroy = brpc_semaphore_destroy,
    .init = brpc_semaphore_init,
    .wait = brpc_semaphore_wait,
    .post = brpc_semaphore_post
};

///////////////////////////////////////////////////////////////////////////////
// Register brpc log for UBSocket.
///////////////////////////////////////////////////////////////////////////////
#if BRPC_WITH_GLOG
    #define UBSOCKET_LOG(level, filename, line, msg) \
        LOG_AT(level, filename, line) << (msg)
#else
    #define UBSOCKET_LOG(level, filename, line, msg) \
        LOG_AT1(level, filename, line) << (msg)
#endif

// Keep consistent with the basic log level definitions in UBSocket.
enum UBSocketLogLevel {
    UBSOCKET_LOG_DEBUG = 0,
    UBSOCKET_LOG_INFO,
    UBSOCKET_LOG_NOTICE,
    UBSOCKET_LOG_WARN,
    UBSOCKET_LOG_ERR,

    UBSOCKET_LOG_COUNT
};

void UBSocketLogger(int level, const char *msg, const char *filename, int line)
{
    switch (level) {
        case UBSOCKET_LOG_ERR:
            UBSOCKET_LOG(ERROR, filename, line, msg);
            break;
        case UBSOCKET_LOG_WARN:
            UBSOCKET_LOG(WARNING, filename, line, msg);
            break;
#ifndef BRPC_WITH_GLOG
        case UBSOCKET_LOG_NOTICE:
            UBSOCKET_LOG(NOTICE, filename, line, msg);
            break;
#endif
        default:
            UBSOCKET_LOG(INFO, filename, line, msg);
            break;
    }
}

///////////////////////////////////////////////////////////////////////////////
// Configure and initialize UBSocket.
///////////////////////////////////////////////////////////////////////////////
int InitializeUBSocket()
{
    // Prevent conflicts when UB acceleration is enabled via LD_PRELOAD.
    if (getenv("LD_PRELOAD") != nullptr) {
        LOG(ERROR) << "env LD_PRELOAD is set, InitializeUBSocket failed";
        return -1;
    }

    SetUBSocketEnv();
    ubsocket_set_logger(UBSocketLogger);

    /* initialize ubsocket */
    u_init_options_t options;
    if (ubsocket_init_options(&options) != 0) {
        LOG(ERROR) << "Inner error: init ubsocket options failed";
        return -1;
    }
    /* set options */
    options.allowed_protocol = UBS_PROTOCOL_UB_RM_RTP;
    options.lock_ops = &brpc_external_lock_ops;
    options.rw_lock_ops = &brpc_rw_lock_ops;
    options.sem_ops = &brpc_semaphore_ops;
    /* init ubsocket */
    if (ubsocket_init(&options) != 0) {
        LOG(ERROR) << "Inner error: ubsocket_init failed";
        return -1;
    }

    return 0;
}

} // namespace brpc

#endif
