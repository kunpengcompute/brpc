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

// brpc - A framework to host and access services throughout Baidu.

// Date: Sun Jul 13 15:04:18 CST 2014

#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <limits>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <vector>
#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include "gperftools_helper.h"
#include "butil/time.h"
#include "butil/macros.h"
#include "butil/fd_utility.h"
#include "butil/fd_guard.h"
#include "butil/unix_socket.h"
#include "brpc/acceptor.h"
#include "brpc/input_messenger.h"
#include "brpc/policy/hulu_pbrpc_protocol.h"

namespace brpc {
DECLARE_bool(usercode_in_coroutine);
DECLARE_int32(input_message_batch_process_size);
}

namespace {

struct BatchRecorder {
    void Record(int value) {
        std::lock_guard<std::mutex> lock(mutex);
        values.push_back(value);
        condition.notify_all();
    }

    bool WaitForSize(size_t expected) {
        std::unique_lock<std::mutex> lock(mutex);
        return condition.wait_for(
            lock, std::chrono::seconds(5),
            [this, expected] { return values.size() >= expected; });
    }

    void RecordDestroy() {
        {
            std::lock_guard<std::mutex> lock(mutex);
            destroyed.fetch_add(1, std::memory_order_relaxed);
        }
        condition.notify_all();
    }

    bool WaitForDestroyed(int expected) {
        std::unique_lock<std::mutex> lock(mutex);
        return condition.wait_for(
            lock, std::chrono::seconds(5),
            [this, expected] {
                return destroyed.load(std::memory_order_relaxed) >= expected;
            });
    }

    std::vector<int> Snapshot() {
        std::lock_guard<std::mutex> lock(mutex);
        return values;
    }

    std::mutex mutex;
    std::condition_variable condition;
    std::vector<int> values;
    std::atomic<int> destroyed{0};
};

class ParsedInputMessage : public brpc::InputMessageBase {
public:
    ParsedInputMessage(int value, BatchRecorder* recorder)
        : value(value), recorder(recorder) {}

    int value;
    BatchRecorder* recorder;

private:
    void DestroyImpl() override {
        recorder->RecordDestroy();
        delete this;
    }
};

void RecordParsedInputMessage(brpc::InputMessageBase* msg_base) {
    brpc::DestroyingPtr<brpc::InputMessageBase> guard(msg_base);
    ParsedInputMessage* msg =
        static_cast<ParsedInputMessage*>(msg_base);
    msg->recorder->Record(msg->value);
}

brpc::ParseResult ParseBatchTestMessage(
        butil::IOBuf* source, brpc::Socket*, bool,
        const void* arg) {
    if (source->empty()) {
        return brpc::MakeParseError(brpc::PARSE_ERROR_NOT_ENOUGH_DATA);
    }
    char value = '\0';
    source->copy_to(&value, 1);
    source->pop_front(1);
    if (value == '?') {
        return brpc::MakeParseError(brpc::PARSE_ERROR_TRY_OTHERS);
    }
    if (value == '!') {
        return brpc::MakeParseError(
            brpc::PARSE_ERROR_ABSOLUTELY_WRONG, "injected parse error");
    }
    if (value == '#') {
        return brpc::MakeMessage(nullptr);
    }
    return brpc::MakeMessage(new ParsedInputMessage(
        static_cast<unsigned char>(value),
        static_cast<BatchRecorder*>(const_cast<void*>(arg))));
}

bool RejectBatchTestMessage(const brpc::InputMessageBase*) {
    return false;
}

brpc::SocketUniquePtr CreateBatchTestSocket(
        brpc::InputMessenger* messenger, brpc::SocketId* id) {
    brpc::SocketOptions options;
    options.socket_mode = brpc::SOCKET_MODE_TCP;
    EXPECT_EQ(0, messenger->Create(options, id));
    brpc::SocketUniquePtr socket;
    EXPECT_EQ(0, brpc::Socket::Address(*id, &socket));
    return socket;
}

brpc::InputMessageHandler BatchTestHandler(
        BatchRecorder* recorder,
        brpc::InputMessageHandler::Verify verify = nullptr) {
    const brpc::InputMessageHandler handler = {
        ParseBatchTestMessage,
        RecordParsedInputMessage,
        verify,
        recorder,
        "batch_test",
    };
    return handler;
}

class NumberedInputMessage : public brpc::InputMessageBase {
public:
    explicit NumberedInputMessage(int value) : value(value) {}

    int value;

private:
    void DestroyImpl() override { delete this; }
};

void RecordNumberedInputMessage(brpc::InputMessageBase* msg_base) {
    brpc::DestroyingPtr<brpc::InputMessageBase> guard(msg_base);
    NumberedInputMessage* msg =
        static_cast<NumberedInputMessage*>(msg_base);
    std::vector<int>* values =
        static_cast<std::vector<int>*>(const_cast<void*>(msg->arg()));
    values->push_back(msg->value);
}

NumberedInputMessage* NewNumberedInputMessage(
        int value, std::vector<int>* values) {
    NumberedInputMessage* msg = new NumberedInputMessage(value);
    msg->_process = RecordNumberedInputMessage;
    msg->_arg = values;
    return msg;
}

}  // namespace

void EmptyProcessHuluRequest(brpc::InputMessageBase* msg_base) {
    brpc::DestroyingPtr<brpc::InputMessageBase> a(msg_base);
}

int main(int argc, char* argv[]) {
    testing::InitGoogleTest(&argc, argv);
    brpc::Protocol dummy_protocol = 
                             { brpc::policy::ParseHuluMessage,
                               brpc::SerializeRequestDefault, 
                               brpc::policy::PackHuluRequest,
                               EmptyProcessHuluRequest, EmptyProcessHuluRequest,
                               NULL, NULL, NULL,
                               brpc::CONNECTION_TYPE_ALL, "dummy_hulu" };
    EXPECT_EQ(0,  RegisterProtocol((brpc::ProtocolType)30, dummy_protocol));
    return RUN_ALL_TESTS();
}

class MessengerTest : public ::testing::Test{
protected:
    MessengerTest(){
    };
    virtual ~MessengerTest(){};
    virtual void SetUp() {
    };
    virtual void TearDown() {
    };
};

TEST_F(MessengerTest, input_message_batch_runs_in_order_once) {
    std::vector<int> values;
    {
        brpc::InputMessageBatch batch(8);
        EXPECT_TRUE(batch.empty());
        batch.add(NewNumberedInputMessage(1, &values));
        batch.add(nullptr);
        batch.add(NewNumberedInputMessage(2, &values));
        batch.add(NewNumberedInputMessage(3, &values));
        EXPECT_EQ(3u, batch.size());

        batch.Run();
        EXPECT_TRUE(batch.empty());
        EXPECT_EQ((std::vector<int>{1, 2, 3}), values);

        batch.Run();
        EXPECT_EQ(3u, values.size());
        batch.add(NewNumberedInputMessage(4, &values));
    }
    EXPECT_EQ((std::vector<int>{1, 2, 3, 4}), values);
}

TEST_F(MessengerTest, input_message_batch_flag_validation) {
    EXPECT_FALSE(GFLAGS_NAMESPACE::SetCommandLineOption(
        "input_message_batch_process_size", "-1").empty());
    EXPECT_FALSE(GFLAGS_NAMESPACE::SetCommandLineOption(
        "input_message_batch_process_size", "0").empty());
    EXPECT_FALSE(GFLAGS_NAMESPACE::SetCommandLineOption(
        "input_message_batch_process_size", "1").empty());
    EXPECT_FALSE(GFLAGS_NAMESPACE::SetCommandLineOption(
        "input_message_batch_process_size", "8").empty());
    EXPECT_TRUE(GFLAGS_NAMESPACE::SetCommandLineOption(
        "input_message_batch_process_size", "-2").empty());
    EXPECT_FALSE(GFLAGS_NAMESPACE::SetCommandLineOption(
        "input_message_batch_process_size", "0").empty());
}

TEST_F(MessengerTest, adaptive_input_message_batch_rises_and_falls) {
    uint32_t ema_q8 = 256;
    uint32_t batch_size = 1;
    std::vector<uint32_t> levels;
    levels.push_back(batch_size);
    for (int i = 0; i < 16 && batch_size < 16; ++i) {
        const uint32_t old_batch_size = batch_size;
        batch_size = brpc::InputMessenger::UpdateAdaptiveBatchSize(
            &ema_q8, batch_size, 32);
        if (batch_size != old_batch_size) {
            levels.push_back(batch_size);
        }
    }
    EXPECT_EQ((std::vector<uint32_t>{1, 2, 4, 8, 16}), levels);
    EXPECT_LE(ema_q8, 32u * 256);

    ema_q8 = 9 * 256;
    batch_size = 8;
    batch_size = brpc::InputMessenger::UpdateAdaptiveBatchSize(
        &ema_q8, batch_size, 9);
    EXPECT_EQ(16u, batch_size);

    const uint32_t old_ema_q8 = ema_q8;
    EXPECT_EQ(batch_size,
              brpc::InputMessenger::UpdateAdaptiveBatchSize(
                  &ema_q8, batch_size, 0));
    EXPECT_EQ(old_ema_q8, ema_q8);

    std::vector<uint32_t> falling_levels;
    falling_levels.push_back(batch_size);
    for (int i = 0; i < 16 && batch_size > 1; ++i) {
        const uint32_t old_batch_size = batch_size;
        batch_size = brpc::InputMessenger::UpdateAdaptiveBatchSize(
            &ema_q8, batch_size, 1);
        if (batch_size != old_batch_size) {
            falling_levels.push_back(batch_size);
        }
    }
    EXPECT_EQ((std::vector<uint32_t>{16, 8, 4, 2, 1}),
              falling_levels);
}

TEST_F(MessengerTest, adaptive_input_message_batch_caps_sample_and_resets) {
    uint32_t ema_q8 = 0;
    uint32_t batch_size = 0;
    for (int i = 0; i < 32; ++i) {
        batch_size = brpc::InputMessenger::UpdateAdaptiveBatchSize(
            &ema_q8, batch_size, std::numeric_limits<size_t>::max());
    }
    EXPECT_EQ(16u, batch_size);
    EXPECT_LE(ema_q8, 32u * 256);

    brpc::SocketId id;
    ASSERT_EQ(0, brpc::Socket::Create(brpc::SocketOptions(), &id));
    brpc::SocketUniquePtr socket;
    ASSERT_EQ(0, brpc::Socket::Address(id, &socket));
    socket->_input_messages_per_read_ema_q8 = ema_q8;
    socket->_adaptive_input_message_batch_size = batch_size;
    ASSERT_EQ(0, socket->ResetFileDescriptor(-1));
    EXPECT_EQ(0u, socket->_input_messages_per_read_ema_q8);
    EXPECT_EQ(0u, socket->_adaptive_input_message_batch_size);
}

TEST_F(MessengerTest, input_message_closure_and_batch_entrypoints) {
    std::vector<int> values;
    brpc::InputMessageClosure closure;
    EXPECT_EQ(nullptr, closure.release());
    closure.reset(NewNumberedInputMessage(1, &values));
    closure.reset(NewNumberedInputMessage(2, &values));
    EXPECT_EQ((std::vector<int>{1}), values);
    brpc::ProcessInputMessage(closure.release());
    EXPECT_EQ((std::vector<int>{1, 2}), values);

    brpc::InputMessageBatch* batch = new brpc::InputMessageBatch(4);
    batch->add(NewNumberedInputMessage(3, &values));
    batch->add(NewNumberedInputMessage(4, &values));
    brpc::ProcessInputMessageBatch(batch);
    EXPECT_EQ((std::vector<int>{1, 2, 3, 4}), values);
}

TEST_F(MessengerTest, queue_helpers_cover_empty_full_and_coroutine_fallback) {
    const bool saved_coroutine = brpc::FLAGS_usercode_in_coroutine;
    brpc::FLAGS_usercode_in_coroutine = true;

    brpc::InputMessenger messenger;
    brpc::SocketId id;
    brpc::SocketUniquePtr socket = CreateBatchTestSocket(&messenger, &id);
    ASSERT_TRUE(socket);

    std::vector<int> values;
    int num_bthread_created = 0;
    std::unique_ptr<brpc::InputMessageBatch> batch;
    brpc::InputMessenger::QueueInputMessageBatch(
        socket.get(), &batch, &num_bthread_created, false);

    brpc::InputMessageClosure last;
    brpc::InputMessenger::QueueLastMessageOrBatch(
        socket.get(), last, &batch, &num_bthread_created, 2);
    last.reset(NewNumberedInputMessage(1, &values));
    brpc::InputMessenger::QueueLastMessageOrBatch(
        socket.get(), last, &batch, &num_bthread_created, 2);
    ASSERT_NE(nullptr, batch.get());
    EXPECT_EQ(1u, batch->size());

    last.reset(NewNumberedInputMessage(2, &values));
    brpc::InputMessenger::QueueLastMessageOrBatch(
        socket.get(), last, &batch, &num_bthread_created, 2);
    EXPECT_EQ(nullptr, batch.get());
    EXPECT_EQ((std::vector<int>{1, 2}), values);
    EXPECT_EQ(0, num_bthread_created);

    socket->SetFailed();
    socket.reset();
    brpc::FLAGS_usercode_in_coroutine = saved_coroutine;
}

TEST_F(MessengerTest, process_new_message_covers_all_batch_modes) {
    const int saved_batch_size =
        brpc::FLAGS_input_message_batch_process_size;
    const bool saved_coroutine = brpc::FLAGS_usercode_in_coroutine;
    brpc::FLAGS_usercode_in_coroutine = false;

    const int batch_sizes[] = {-1, 0, 1, 2, 8};
    for (int batch_size : batch_sizes) {
        brpc::FLAGS_input_message_batch_process_size = batch_size;
        BatchRecorder recorder;
        brpc::InputMessenger messenger(4);
        ASSERT_EQ(0, messenger.AddNonProtocolHandler(
            BatchTestHandler(&recorder)));
        brpc::SocketId id;
        brpc::SocketUniquePtr socket =
            CreateBatchTestSocket(&messenger, &id);
        ASSERT_TRUE(socket);
        socket->_read_buf.append("abcd", 4);
        {
            brpc::InputMessageClosure last;
            ASSERT_EQ(0, messenger.ProcessNewMessage(
                socket.get(), 4, false, 123, 456, last));
        }
        ASSERT_TRUE(recorder.WaitForSize(4)) << "batch_size=" << batch_size;
        socket->SetFailed();
        ASSERT_TRUE(recorder.WaitForDestroyed(4))
            << "batch_size=" << batch_size;
        std::vector<int> values = recorder.Snapshot();
        std::sort(values.begin(), values.end());
        EXPECT_EQ((std::vector<int>{'a', 'b', 'c', 'd'}), values)
            << "batch_size=" << batch_size;
        EXPECT_EQ(4, recorder.destroyed.load())
            << "batch_size=" << batch_size;
        EXPECT_EQ(1u, socket->_avg_msg_size);
        if (batch_size == -1) {
            EXPECT_EQ(2u, socket->_adaptive_input_message_batch_size);
            EXPECT_GT(socket->_input_messages_per_read_ema_q8, 256u);
        }
    }

    brpc::FLAGS_input_message_batch_process_size = saved_batch_size;
    brpc::FLAGS_usercode_in_coroutine = saved_coroutine;
}

TEST_F(MessengerTest, process_new_message_handles_skip_partial_and_mode_reset) {
    const int saved_batch_size =
        brpc::FLAGS_input_message_batch_process_size;
    const bool saved_coroutine = brpc::FLAGS_usercode_in_coroutine;
    brpc::FLAGS_usercode_in_coroutine = true;

    BatchRecorder recorder;
    brpc::InputMessenger messenger(4);
    ASSERT_EQ(0, messenger.AddNonProtocolHandler(
        BatchTestHandler(&recorder)));
    brpc::SocketId id;
    brpc::SocketUniquePtr socket =
        CreateBatchTestSocket(&messenger, &id);
    ASSERT_TRUE(socket);

    brpc::FLAGS_input_message_batch_process_size = -1;
    socket->_read_buf.append("#a", 2);
    {
        brpc::InputMessageClosure last;
        EXPECT_EQ(0, messenger.ProcessNewMessage(
            socket.get(), 2, false, 111, 222, last));
    }
    ASSERT_TRUE(recorder.WaitForSize(1));
    EXPECT_EQ((std::vector<int>{'a'}), recorder.Snapshot());
    // Coroutine mode disables adaptive batching.
    EXPECT_EQ(0u, socket->_adaptive_input_message_batch_size);

    socket->_input_messages_per_read_ema_q8 = 4096;
    socket->_adaptive_input_message_batch_size = 16;
    brpc::FLAGS_input_message_batch_process_size = 0;
    {
        brpc::InputMessageClosure last;
        EXPECT_EQ(0, messenger.ProcessNewMessage(
            socket.get(), 0, false, 333, 444, last));
    }
    EXPECT_EQ(0u, socket->_input_messages_per_read_ema_q8);
    EXPECT_EQ(0u, socket->_adaptive_input_message_batch_size);
    EXPECT_EQ(333, socket->_last_readtime_us.load());

    socket->SetFailed();
    brpc::FLAGS_input_message_batch_process_size = saved_batch_size;
    brpc::FLAGS_usercode_in_coroutine = saved_coroutine;
}

TEST_F(MessengerTest, process_new_message_handles_progressive_and_parse_errors) {
    const int saved_batch_size =
        brpc::FLAGS_input_message_batch_process_size;
    const bool saved_coroutine = brpc::FLAGS_usercode_in_coroutine;
    brpc::FLAGS_input_message_batch_process_size = 8;
    brpc::FLAGS_usercode_in_coroutine = false;

    {
        BatchRecorder recorder;
        brpc::InputMessenger messenger(4);
        ASSERT_EQ(0, messenger.AddNonProtocolHandler(
            BatchTestHandler(&recorder)));
        brpc::SocketId id;
        brpc::SocketUniquePtr socket =
            CreateBatchTestSocket(&messenger, &id);
        ASSERT_TRUE(socket);
        socket->read_will_be_progressive(brpc::CONNECTION_TYPE_SINGLE);
        socket->_read_buf.append("abc", 3);
        {
            brpc::InputMessageClosure last;
            EXPECT_EQ(0, messenger.ProcessNewMessage(
                socket.get(), 3, false, 1, 2, last));
        }
        ASSERT_TRUE(recorder.WaitForSize(3));
        socket->SetFailed();
        ASSERT_TRUE(recorder.WaitForDestroyed(3));
        EXPECT_EQ(3, recorder.destroyed.load());
    }

    const char failures[] = {'?', '!'};
    for (char failure : failures) {
        BatchRecorder recorder;
        brpc::InputMessenger messenger(4);
        ASSERT_EQ(0, messenger.AddNonProtocolHandler(
            BatchTestHandler(&recorder)));
        brpc::SocketId id;
        brpc::SocketUniquePtr socket =
            CreateBatchTestSocket(&messenger, &id);
        ASSERT_TRUE(socket);
        socket->_read_buf.append(&failure, 1);
        brpc::InputMessageClosure last;
        EXPECT_EQ(-1, messenger.ProcessNewMessage(
            socket.get(), 1, false, 1, 2, last));
        EXPECT_TRUE(socket->Failed());
    }

    brpc::FLAGS_input_message_batch_process_size = saved_batch_size;
    brpc::FLAGS_usercode_in_coroutine = saved_coroutine;
}

TEST_F(MessengerTest, process_new_message_releases_unprocessable_and_rejected) {
    const int saved_batch_size =
        brpc::FLAGS_input_message_batch_process_size;
    const bool saved_coroutine = brpc::FLAGS_usercode_in_coroutine;
    brpc::FLAGS_input_message_batch_process_size = 0;
    brpc::FLAGS_usercode_in_coroutine = true;

    {
        BatchRecorder recorder;
        brpc::InputMessenger messenger(4);
        ASSERT_EQ(0, messenger.AddNonProtocolHandler(
            BatchTestHandler(&recorder)));
        messenger._handlers[0].process = nullptr;
        brpc::SocketId id;
        brpc::SocketUniquePtr socket =
            CreateBatchTestSocket(&messenger, &id);
        socket->_read_buf.append("a", 1);
        brpc::InputMessageClosure last;
        EXPECT_EQ(0, messenger.ProcessNewMessage(
            socket.get(), 1, false, 1, 2, last));
        EXPECT_EQ(1, recorder.destroyed.load());
        EXPECT_TRUE(recorder.Snapshot().empty());
        socket->SetFailed();
    }

    {
        BatchRecorder recorder;
        brpc::InputMessenger messenger(4);
        ASSERT_EQ(0, messenger.AddNonProtocolHandler(
            BatchTestHandler(&recorder, RejectBatchTestMessage)));
        brpc::SocketId id;
        brpc::SocketUniquePtr socket =
            CreateBatchTestSocket(&messenger, &id);
        socket->_read_buf.append("a", 1);
        brpc::InputMessageClosure last;
        EXPECT_EQ(-1, messenger.ProcessNewMessage(
            socket.get(), 1, false, 1, 2, last));
        EXPECT_EQ(1, recorder.destroyed.load());
        EXPECT_TRUE(socket->Failed());
    }

    brpc::FLAGS_input_message_batch_process_size = saved_batch_size;
    brpc::FLAGS_usercode_in_coroutine = saved_coroutine;
}

#define USE_UNIX_DOMAIN_SOCKET 1

const size_t NEPOLL = 1;
const size_t NCLIENT = 6;
const size_t NMESSAGE = 1024;
const size_t MESSAGE_SIZE = 32;

inline uint32_t fmix32 ( uint32_t h ) {
    h ^= h >> 16;
    h *= 0x85ebca6b;
    h ^= h >> 13;
    h *= 0xc2b2ae35;
    h ^= h >> 16;
    return h;
}

volatile bool client_stop = false;

struct BAIDU_CACHELINE_ALIGNMENT ClientMeta {
    size_t times;
    size_t bytes;
};

butil::atomic<size_t> client_index(0);

void* client_thread(void* arg) {
    ClientMeta* m = (ClientMeta*)arg;
    size_t offset = 0;
    m->times = 0;
    m->bytes = 0;
    const size_t buf_cap = NMESSAGE * MESSAGE_SIZE;
    char* buf = (char*)malloc(buf_cap);
    for (size_t i = 0; i < NMESSAGE; ++i) {
        memcpy(buf + i * MESSAGE_SIZE, "HULU", 4);
        // HULU use host byte order directly...
        *(uint32_t*)(buf + i * MESSAGE_SIZE + 4) = MESSAGE_SIZE - 12;
        *(uint32_t*)(buf + i * MESSAGE_SIZE + 8) = 4;
    }
#ifdef USE_UNIX_DOMAIN_SOCKET
    const size_t id = client_index.fetch_add(1);
    char socket_name[64];
    snprintf(socket_name, sizeof(socket_name), "input_messenger.socket%lu",
             (id % NEPOLL));
    butil::fd_guard fd(butil::unix_socket_connect(socket_name));
    if (fd < 0) {
        PLOG(FATAL) << "Fail to connect to " << socket_name;
        return NULL;
    }
#else
    butil::EndPoint point(butil::IP_ANY, 7878);
    butil::fd_guard fd(butil::tcp_connect(point, NULL));
    if (fd < 0) {
        PLOG(FATAL) << "Fail to connect to " << point;
        return NULL;
    }
#endif

    while (!client_stop) {
        ssize_t n;
        if (offset == 0) {
            n = write(fd, buf, buf_cap);
        } else {
            iovec v[2];
            v[0].iov_base = buf + offset;
            v[0].iov_len = buf_cap - offset;
            v[1].iov_base = buf;
            v[1].iov_len = offset;
            n = writev(fd, v, 2);
        }
        if (n < 0) {
            if (errno != EINTR) {
                PLOG(FATAL) << "Fail to write fd=" << fd;
                return NULL;
            }
        } else {
            ++m->times;
            m->bytes += n;
            offset += n;
            if (offset >= buf_cap) {
                offset -= buf_cap;
            }
        }
    }
    return NULL;
}

TEST_F(MessengerTest, dispatch_tasks) {
    client_stop = false;
    
    brpc::Acceptor messenger[NEPOLL];
    pthread_t cth[NCLIENT];
    ClientMeta* cm[NCLIENT];

    const brpc::InputMessageHandler pairs[] = {
        { brpc::policy::ParseHuluMessage, 
          EmptyProcessHuluRequest, NULL, NULL, "dummy_hulu" }
    };

    for (size_t i = 0; i < NEPOLL; ++i) {        
#ifdef USE_UNIX_DOMAIN_SOCKET
        char buf[64];
        snprintf(buf, sizeof(buf), "input_messenger.socket%lu", i);
        int listening_fd = butil::unix_socket_listen(buf);
#else
        int listening_fd = tcp_listen(butil::EndPoint(butil::IP_ANY, 7878));
#endif
        ASSERT_TRUE(listening_fd > 0);
        butil::make_non_blocking(listening_fd);
        ASSERT_EQ(0, messenger[i].AddHandler(pairs[0]));
        ASSERT_EQ(0, messenger[i].StartAccept(listening_fd, -1, NULL, false));
    }
    
    for (size_t i = 0; i < NCLIENT; ++i) {
        cm[i] = new ClientMeta;
        cm[i]->times = 0;
        cm[i]->bytes = 0;
        ASSERT_EQ(0, pthread_create(&cth[i], NULL, client_thread, cm[i]));
    }

    sleep(1);


    LOG(INFO) << "Begin to profile... (5 seconds)";
    ProfilerStart("input_messenger.prof");

    size_t start_client_bytes = 0;
    for (size_t i = 0; i < NCLIENT; ++i) {
        start_client_bytes += cm[i]->bytes;
    }
    butil::Timer tm;
    tm.start();
    
    sleep(5);
    
    tm.stop();
    ProfilerStop();
    LOG(INFO) << "End profiling";

    client_stop = true;

    size_t client_bytes = 0;
    for (size_t i = 0; i < NCLIENT; ++i) {
        client_bytes += cm[i]->bytes;
    }
    LOG(INFO) << "client_tp=" << (client_bytes - start_client_bytes) / (double)tm.u_elapsed()
              << "MB/s client_msg="
              << (client_bytes - start_client_bytes) * 1000000L / (MESSAGE_SIZE * tm.u_elapsed())
              << "/s";

    for (size_t i = 0; i < NCLIENT; ++i) {
        pthread_join(cth[i], NULL);
        printf("joined client %lu\n", i);
    }
    for (size_t i = 0; i < NEPOLL; ++i) {
        messenger[i].StopAccept(0);
    }
    sleep(1);
    LOG(WARNING) << "begin to exit!!!!";
}
