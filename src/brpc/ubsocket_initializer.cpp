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
#include <atomic>
#include <cerrno>
#include <cstdint>
#include <new>
#include <sys/epoll.h>

#include <gflags/gflags.h>
#include <string>
#include "brpc/event_dispatcher.h"
#include "butil/reloadable_flags.h"
#include "brpc/log.h"
#include "bthread/rwlock.h"
#include "bthread/bthread.h"
#include "butil/logging.h"
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
DEFINE_string(ubsocket_block_type, "tiny", "Minimum fragment of the memory pool for ubsocket (e.g., 'tiny'(4K), 'default'(8k), 'small'(16k), 'medium'(32k), 'large'(64k))");
DEFINE_string(ubsocket_pool_initial_size, "200", "Total size of IO memory for ubsocket, in MB");
DEFINE_string(ubsocket_pool_max_size, "2048", "Max size of ubsocket pool, in MB");
DEFINE_string(ubsocket_buf_pool_depth, "12000", "Depth of ubsocket buffer pool");
DEFINE_bool(ubsocket_tiny_pool_enable, true, "Whether to enable the UMQ tiny pool for UBIOBuf");
DEFINE_uint32(ubsocket_tiny_pool_block_size, 1024, "UMQ tiny pool block size (e.g., 512, 1024, 2048, 4096, 8192)");
DEFINE_string(ubsocket_tiny_buf_pool_depth, "8192", "UMQ tiny pool block count");
DEFINE_string(ubsocket_schedule_policy, "affinity_priority", "Set the multi-plane load balancing policy (e.g., 'affinity_priority', 'affinity', 'rr')");
DEFINE_string(ubsocket_readv_unlimited, "true", "Whether to enable the readv reporting limit for ubsocket (e.g., 'false', 'true')");
DEFINE_string(ubsocket_use_polling, "false", "Whether to enable message processing polling for ubsocket (e.g., 'false', 'true')");
DEFINE_string(ubsocket_adpt_stats, "false", "Count statistics for ubsocket (e.g., 'false', 'true')");
DEFINE_string(ubsocket_auto_fallback_tcp, "true", "Whether to automatically downgrade TCP when the protocols do not match (e.g., 'false', 'true')");
DEFINE_string(ubsocket_enable_share_jfr, "true", "Whether to enable share jfr (e.g., 'false', 'true')");
DEFINE_string(ubsocket_share_jfr_rx_queue_depth, "1024", "Share jfr receive queue depth, the minimum value is 64. The upper limit of the setting is determined by the actual machine environment.");
DEFINE_string(ubsocket_share_jfr_rx_o3_queue_depth, "256", "Share jfr receive out of order queue depth (only available when ubsocket_ub_trans_mode is \"RM_CTP\"), the default value is 256. The upper limit of the setting is equal to ubsocket_share_jfr_rx_queue_depth.");
DEFINE_string(ubsocket_trace_enable, "true", "Enable ubsocket trace statistics (e.g., 'false', 'true')");
DEFINE_string(ubsocket_backup_link_enable, "true", "Enable ubsocket backup link (e.g., 'false', 'true')");
DEFINE_string(ubsocket_trace_time, "10", "Set monitoring ubsocket data output interval, the minimum value is 1, the maximum value is 300");
DEFINE_string(ubsocket_trace_file_path, "/tmp/ubsocket/log", "Set monitoring ubsocket data output path (e.g., '/tmp/ubsocket/log')");
DEFINE_string(ubsocket_trace_file_size, "10", "Set monitoring ubsocket data file size, the minimum value is 1, the maximum value is 300");
DEFINE_string(ubsocket_stats_cli, "true", "Enable ubsocket cli service (e.g., 'false', 'true')");
DEFINE_string(ubsocket_ub_trans_mode, "RM_CTP", "Protocol mode for ubsocket (e.g., 'RC_TP', 'RM_TP', 'RM_CTP', 'RC_CTP')");
DEFINE_string(ubsocket_initial_credit, "128", "The initial credits requested for a new send operation.");
DEFINE_string(ubsocket_max_credit_per_request, "1024", "Upper bound for credit in one request.");
DEFINE_string(ubsocket_min_reserved_credit, "100", "Minimum reserved credit, if the held credit <= min_reserved_credit, the credit will not be returned.");
DEFINE_string(ubsocket_link_priority, "-1", "Set urma flow service level priority, range from 0 to 15.");
DEFINE_string(ubsocket_degrade, "true", "Allow degradation to TCP when UB fails; default: true");
DEFINE_string(ubsocket_async_accept, "false", "Allow do accept async; default: false");
DEFINE_string(ubsocket_thread_pool_size, "1", "the number of threads in ubsocket thread pool; default: 0");
DEFINE_string(ubsocket_async_epoll_wait, "false", "Allow do epoll wait async; default: false");
DEFINE_string(ubsocket_probe_enable, "false", "Enable ubsocket probe (e.g., 'false', 'true')");
DEFINE_string(ubsocket_probe_time_ms, "1000", "ubsocket probe interval time, the minimum value is 1, the maximum value is 360000");
DEFINE_string(ubsocket_probe_batch, "10", "ubsocket number of sock to probe per batch, the minimum value is 1, the maximum value is 500");
DEFINE_string(ubsocket_ub_epoll_enable, "true", "Whether to enable ub epoll; default: false (optional: false, true)");
DEFINE_string(ubsocket_prof_enable, "false", "Enable ubsocket profiling (e.g., 'false', 'true')");
DEFINE_string(ubsocket_prof_mode, "fast", "Set ubsocket profiling mode; default: fast (optional: fast, ext)");
DEFINE_string(ubsocket_prof_dump_interval_min, "1", "Set dump ubsocket profiling data output interval(minute), the minimum value is 1, the maximum value is 5");
DEFINE_string(ubsocket_prof_dump_path, "/tmp/ubsocket/profiling", "Set dump ubsocket profiling data output path (e.g., '/tmp/ubsocket/profiling')");
DEFINE_string(ubsocket_ub_handshake_mode, "ub_sock_opt", "Handshake mode for UB connection; default: ub_sock_opt (optional: tfo, ub_sock_opt)");
DEFINE_string(ubsocket_flow_control_enable, "true", "Whether to enable flow control; default: true (optional: false, true)");
DEFINE_string(ubsocket_split_trace_enable, "false", "Enable ubsocket split trace (e.g., 'false', 'true')");
DEFINE_string(ubsocket_split_trace_buf_cap, "65535", "Set ubsocket split trace buf capacity; default: 65535, the minimum value is 16384, the maximum value is 65536");
DEFINE_string(ubsocket_split_trace_drain_interval_ms, "10", "Set ubsocket split trace buf log drain interval(ms), the minimum value is 1, the maximum value is 10000");
DEFINE_string(ubsocket_tp_type, "single", "Ubsocket jetty tranport type; default: single (optional: single, pool)");
DEFINE_string(ubsocket_tp_pool_size, "16", "Ubsocket jetty tranport pool size; the minimum value is 1, the maximum value is 1000");

static bool validate_ubsocket_tiny_pool_block_size(const char*, uint32_t value)
{
    if (value == 512 || value == 1024 || value == 2048 || value == 4096 || value == 8192) {
        return true;
    }
    LOG(ERROR) << "Invalid ubsocket_tiny_pool_block_size=" << value
               << ", expected one of 512, 1024, 2048, 4096, 8192";
    return false;
}
BUTIL_VALIDATE_GFLAG(ubsocket_tiny_pool_block_size, validate_ubsocket_tiny_pool_block_size);

namespace {

class UBSocketPollerConsumer {
public:
    UBSocketPollerConsumer(int fd, void *arg, u_poller_event_cb_t callback)
        : _fd(fd)
        , _arg(arg)
        , _callback(callback) {}

    int Start()
    {
        if (_io_event.Init(this) != 0) {
            LOG(ERROR) << "Fail to init ubsocket poller IOEvent";
            return -1;
        }
        if (_io_event.AddConsumer(_fd) != 0) {
            PLOG(ERROR) << "Fail to add ubsocket poller fd=" << _fd << " into EventDispatcher";
            _io_event.Reset();
            return -1;
        }
        _started = true;
        return 0;
    }

    void Stop()
    {
        if (!_started) {
            return;
        }
        _io_event.RemoveConsumer(_fd);
        _io_event.Reset();
        while (_running_tasks.load(std::memory_order_acquire) != 0) {
            bthread_usleep(1000);
        }
        _started = false;
    }

    static int OnInputEvent(void *user_data, uint32_t events, const bthread_attr_t &thread_attr)
    {
        auto *consumer = static_cast<UBSocketPollerConsumer *>(user_data);
        if (consumer == nullptr || consumer->_callback == nullptr) {
            return -1;
        }
        if ((events & (EPOLLIN | EPOLLERR | EPOLLHUP)) == 0) {
            return 0;
        }
        if (consumer->_pending_events.fetch_add(1, std::memory_order_acq_rel) == 0) {
            consumer->_running_tasks.fetch_add(1, std::memory_order_acq_rel);
            bthread_t tid;
            bthread_attr_t attr = thread_attr;
            attr.tag = bthread_self_tag();
            if (bthread_start_urgent(&tid, &attr, DrainTask, consumer) != 0) {
                consumer->_pending_events.fetch_sub(1, std::memory_order_acq_rel);
                LOG(ERROR) << "Fail to start ubsocket poller bthread";
                DrainInline(consumer);
                consumer->_running_tasks.fetch_sub(1, std::memory_order_acq_rel);
                return -1;
            }
        }
        return 0;
    }

    static int OnOutputEvent(void *, uint32_t, const bthread_attr_t &)
    {
        return 0;
    }

private:
    static void DrainInline(UBSocketPollerConsumer *consumer)
    {
        consumer->_callback(consumer->_arg);
    }

    static void *DrainTask(void *arg)
    {
        auto *consumer = static_cast<UBSocketPollerConsumer *>(arg);
        while (true) {
            DrainInline(consumer);
            if (consumer->_pending_events.fetch_sub(1, std::memory_order_acq_rel) == 1) {
                break;
            }
        }
        consumer->_running_tasks.fetch_sub(1, std::memory_order_acq_rel);
        return nullptr;
    }

    int _fd;
    void *_arg;
    u_poller_event_cb_t _callback;
    IOEvent<UBSocketPollerConsumer> _io_event;
    std::atomic<uint32_t> _pending_events{0};
    std::atomic<uint32_t> _running_tasks{0};
    bool _started{false};
};

} // namespace

static int brpc_poller_add_consumer(int fd, void *arg, u_poller_event_cb_t callback, void **consumer)
{
    if (consumer == nullptr || callback == nullptr || fd < 0) {
        errno = EINVAL;
        return -1;
    }
    auto *poller_consumer = new (std::nothrow) UBSocketPollerConsumer(fd, arg, callback);
    if (poller_consumer == nullptr) {
        errno = ENOMEM;
        return -1;
    }
    if (poller_consumer->Start() != 0) {
        delete poller_consumer;
        return -1;
    }
    *consumer = poller_consumer;
    return 0;
}

static void brpc_poller_remove_consumer(void *consumer, int)
{
    auto *poller_consumer = static_cast<UBSocketPollerConsumer *>(consumer);
    if (poller_consumer == nullptr) {
        return;
    }
    poller_consumer->Stop();
    delete poller_consumer;
}

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
    ::setenv("UBSOCKET_UMQ_TINY_POOL_ENABLE", FLAGS_ubsocket_tiny_pool_enable ? "true" : "false", 1);
    const std::string tiny_pool_block_size = std::to_string(FLAGS_ubsocket_tiny_pool_block_size);
    ::setenv("UBSOCKET_UMQ_TINY_POOL_BLOCK_SIZE", tiny_pool_block_size.c_str(), 1);
    if (!FLAGS_ubsocket_tiny_buf_pool_depth.empty()) {
        ::setenv("UBSOCKET_UMQ_TINY_POOL_BLOCK_COUNT", FLAGS_ubsocket_tiny_buf_pool_depth.c_str(), 1);
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
    if (!FLAGS_ubsocket_share_jfr_rx_o3_queue_depth.empty()) {
      ::setenv("UBSOCKET_SHARE_JFR_RX_O3_QUEUE_DEPTH", FLAGS_ubsocket_share_jfr_rx_o3_queue_depth.c_str(), 1);
    }
    if (!FLAGS_ubsocket_trace_enable.empty()) {
        ::setenv("UBSOCKET_TRACE_ENABLE", FLAGS_ubsocket_trace_enable.c_str(), 1);
    }
    if (!FLAGS_ubsocket_backup_link_enable.empty()) {
        ::setenv("UBSOCKET_BACKUP_LINK_ENABLE", FLAGS_ubsocket_backup_link_enable.c_str(), 1);
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
    if (!FLAGS_ubsocket_initial_credit.empty()) {
        ::setenv("UBSOCKET_INITIAL_CREDIT", FLAGS_ubsocket_initial_credit.c_str(), 1);
    }
    if (!FLAGS_ubsocket_max_credit_per_request.empty()) {
        ::setenv("UBSOCKET_MAX_CREDIT_PER_REQUEST", FLAGS_ubsocket_max_credit_per_request.c_str(), 1);
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
    if (!FLAGS_ubsocket_flow_control_enable.empty()) {
        ::setenv("UBSOCKET_FLOW_CONTROL_ENABLE", FLAGS_ubsocket_flow_control_enable.c_str(), 1);
    }
    if (!FLAGS_ubsocket_ub_handshake_mode.empty()) {
        ::setenv("UBSOCKET_UB_HANDSHAKE_MODE", FLAGS_ubsocket_ub_handshake_mode.c_str(), 1);
    }
    if (!FLAGS_ubsocket_prof_enable.empty()) {
        ::setenv("UBSOCKET_PROF_ENABLE", FLAGS_ubsocket_prof_enable.c_str(), 1);
    }
    if (!FLAGS_ubsocket_prof_mode.empty()) {
        ::setenv("UBSOCKET_PROF_MODE", FLAGS_ubsocket_prof_mode.c_str(), 1);
    }
    if (!FLAGS_ubsocket_prof_dump_interval_min.empty()) {
        ::setenv("UBSOCKET_PROF_DUMP_INTERVAL_MIN", FLAGS_ubsocket_prof_dump_interval_min.c_str(), 1);
    }
    if (!FLAGS_ubsocket_split_trace_enable.empty()) {
        ::setenv("UBSOCKET_SPLIT_TRACE_ENABLE", FLAGS_ubsocket_split_trace_enable.c_str(), 1);
    }
    if (!FLAGS_ubsocket_split_trace_buf_cap.empty()) {
        ::setenv("UBSOCKET_SPLIT_TRACE_BUF_CAPACITY", FLAGS_ubsocket_split_trace_buf_cap.c_str(), 1);
    }
    if (!FLAGS_ubsocket_split_trace_drain_interval_ms.empty()) {
        ::setenv("UBSOCKET_SPLIT_TRACE_DRAIN_INTERVAL_MS", FLAGS_ubsocket_split_trace_drain_interval_ms.c_str(), 1);
    }
    if (!FLAGS_ubsocket_prof_dump_path.empty()) {
        ::setenv("UBSOCKET_PROF_DUMP_PATH", FLAGS_ubsocket_prof_dump_path.c_str(), 1);
    }
    if (!FLAGS_ubsocket_tp_type.empty()) {
      ::setenv("UBSOCKET_TP_TYPE", FLAGS_ubsocket_tp_type.c_str(), 1);
    }
    if (!FLAGS_ubsocket_tp_pool_size.empty()) {
      ::setenv("UBSOCKET_TP_POOL_SIZE", FLAGS_ubsocket_tp_pool_size.c_str(), 1);
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

static void* brpc_get_rpc_id()
{
    return bthread_getspecific(ubsocket_trace_rpcid_key);
}

static void* brpc_get_call_timestamp()
{
    return bthread_getspecific(ubsocket_trace_call_timestamp);
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

u_external_rpc_id_ops_t brpc_rpc_id_ops = {
    .get_rpc_id = brpc_get_rpc_id,
    .get_rpc_call_timestamp = brpc_get_call_timestamp,
};

u_external_poller_ops_t brpc_poller_ops = {
    .add_consumer = brpc_poller_add_consumer,
    .remove_consumer = brpc_poller_remove_consumer
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

#define UBSOCKET_VLOG_AT(verboselevel, filename, line) \
     if (!VLOG_IS_ON(verboselevel)) ; \
     else LOG_AT(INFO, filename, line) << (msg)

// Keep consistent with the basic log level definitions in UBSocket.
enum UBSocketLogLevel {
    UBSOCKET_LOG_DEBUG = -1,
    UBSOCKET_LOG_INFO = 0,
    UBSOCKET_LOG_NOTICE,
    UBSOCKET_LOG_WARN,
    UBSOCKET_LOG_ERR,

    UBSOCKET_LOG_COUNT
};

void UBSocketLogger(int level, const char *msg, const char *filename, int line)
{
    switch (level - 1) {
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
        case UBSOCKET_LOG_DEBUG:
            UBSOCKET_LOG(DEBUG, filename, line, msg);
            break;
#else
        case UBSOCKET_LOG_DEBUG:
            UBSOCKET_VLOG_AT(1, filename, line) << msg;
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

    // 1. 获取基础日志级别
#if BRPC_WITH_GLOG
    const int min_level = FLAGS_minloglevel;
#else
    const int min_level = ::logging::GetMinLogLevel();
#endif

    // 2. 声明BRPC minloglevel与ubsocket日志级别的映射
#if BRPC_WITH_GLOG
    const int level_map[] = { UBSOCKET_LOG_INFO, UBSOCKET_LOG_WARN, UBSOCKET_LOG_ERR };
#else
    const int level_map[] = { UBSOCKET_LOG_INFO, UBSOCKET_LOG_NOTICE, UBSOCKET_LOG_WARN };
#endif

    /**
      * minloglevel配置映射关系：
      *  minloglevel |   0  |    1    |    2    |   3   |   4   |
      *  glog        | INFO | WARNING |  ERROR  | FATAL | NONE  |
      *  blog        | INFO | NOTICE  | WARNING | ERROR | FATAL |
      *  ubsocket    | INFO | NOTICE  | WARNING | ERROR | NONE  |
      *
      *  ubsocket DEBUG级别日志由--v控制
     */
    int ub_log_level = UBSOCKET_LOG_ERR; // 默认为 min_level >= 3 的情况
    if (min_level >= 0 && min_level < static_cast<int>(sizeof(level_map) / sizeof(level_map[0]))) {
        ub_log_level = level_map[min_level];
    }
    ubsocket_set_log_level(ub_log_level);
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
    options.rpc_id_ops = &brpc_rpc_id_ops;
    options.poller_ops = &brpc_poller_ops;
    /* init ubsocket */
    if (ubsocket_init(&options) != 0) {
        LOG(ERROR) << "Inner error: ubsocket_init failed";
        return -1;
    }

    return 0;
}

} // namespace brpc

#endif
