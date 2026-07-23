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

#include <gtest/gtest.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <unistd.h>
#include <pthread.h>
#include <poll.h>
#include "butil/time.h"
#include "butil/fd_utility.h"
#include "bthread/bthread.h"
#include "brpc/event_dispatcher.h"

#if BRPC_WITH_IO_URING

#include <gflags/gflags.h>
#include <liburing.h>
#include <atomic>
#include <vector>

namespace brpc {
DECLARE_string(io_backend);

namespace testing {

class IoUringEventDispatcherTest : public ::testing::Test {
protected:
    IoUringEventDispatcherTest() = default;
    ~IoUringEventDispatcherTest() override = default;

    void SetUp() override {
        FLAGS_io_backend = "io_uring";
        dispatcher_ = new EventDispatcher();
    }

    void TearDown() override {
        if (dispatcher_) {
            dispatcher_->Stop();
            dispatcher_->Join();
            delete dispatcher_;
            dispatcher_ = nullptr;
        }
    }

    EventDispatcher* dispatcher_ = nullptr;
};

TEST_F(IoUringEventDispatcherTest, ConstructorAndDestructor) {
    EXPECT_TRUE(dispatcher_ != nullptr);
    EXPECT_FALSE(dispatcher_->Running());
}

TEST_F(IoUringEventDispatcherTest, StartAndStop) {
    EXPECT_EQ(0, dispatcher_->Start(nullptr));
    EXPECT_TRUE(dispatcher_->Running());

    dispatcher_->Stop();
    dispatcher_->Join();
    EXPECT_FALSE(dispatcher_->Running());
}

TEST_F(IoUringEventDispatcherTest, StartTwice) {
    EXPECT_EQ(0, dispatcher_->Start(nullptr));
    EXPECT_TRUE(dispatcher_->Running());

    EXPECT_EQ(-1, dispatcher_->Start(nullptr));

    dispatcher_->Stop();
    dispatcher_->Join();
}

TEST_F(IoUringEventDispatcherTest, AddConsumerWithInvalidFd) {
    EXPECT_EQ(-1, dispatcher_->AddConsumer(1, -1));
}

TEST_F(IoUringEventDispatcherTest, AddConsumerWithPipe) {
    int pipefd[2];
    ASSERT_EQ(0, pipe(pipefd));

    IOEventDataId event_data_id = 12345;
    EXPECT_EQ(0, dispatcher_->AddConsumer(event_data_id, pipefd[0]));

    close(pipefd[0]);
    close(pipefd[1]);
}

TEST_F(IoUringEventDispatcherTest, AddConsumerMultipleFd) {
    const size_t kNumFd = 10;
    int pipefds[kNumFd][2];

    for (size_t i = 0; i < kNumFd; ++i) {
        ASSERT_EQ(0, pipe(pipefds[i]));
        IOEventDataId event_data_id = static_cast<IOEventDataId>(i + 1);
        EXPECT_EQ(0, dispatcher_->AddConsumer(event_data_id, pipefds[i][0]));
    }

    for (size_t i = 0; i < kNumFd; ++i) {
        close(pipefds[i][0]);
        close(pipefds[i][1]);
    }
}

TEST_F(IoUringEventDispatcherTest, RemoveConsumer) {
    int pipefd[2];
    ASSERT_EQ(0, pipe(pipefd));

    IOEventDataId event_data_id = 12345;
    EXPECT_EQ(0, dispatcher_->AddConsumer(event_data_id, pipefd[0]));

    EXPECT_EQ(0, dispatcher_->RemoveConsumer(pipefd[0]));

    close(pipefd[0]);
    close(pipefd[1]);
}

TEST_F(IoUringEventDispatcherTest, RemoveConsumerWithInvalidFd) {
    EXPECT_EQ(-1, dispatcher_->RemoveConsumer(-1));
}

TEST_F(IoUringEventDispatcherTest, RemoveConsumerNotAdded) {
    int pipefd[2];
    ASSERT_EQ(0, pipe(pipefd));

    EXPECT_EQ(0, dispatcher_->RemoveConsumer(pipefd[0]));

    close(pipefd[0]);
    close(pipefd[1]);
}

TEST_F(IoUringEventDispatcherTest, RegisterEventWithPollin) {
    int pipefd[2];
    ASSERT_EQ(0, pipe(pipefd));

    IOEventDataId event_data_id = 12345;
    EXPECT_EQ(0, dispatcher_->RegisterEvent(event_data_id, pipefd[0], true));

    close(pipefd[0]);
    close(pipefd[1]);
}

TEST_F(IoUringEventDispatcherTest, RegisterEventWithoutPollin) {
    int pipefd[2];
    ASSERT_EQ(0, pipe(pipefd));

    IOEventDataId event_data_id = 12345;
    EXPECT_EQ(0, dispatcher_->RegisterEvent(event_data_id, pipefd[0], false));

    close(pipefd[0]);
    close(pipefd[1]);
}

TEST_F(IoUringEventDispatcherTest, RegisterEventWithInvalidFd) {
    IOEventDataId event_data_id = 12345;
    EXPECT_EQ(-1, dispatcher_->RegisterEvent(event_data_id, -1, true));
}

TEST_F(IoUringEventDispatcherTest, UnregisterEventWithPollin) {
    int pipefd[2];
    ASSERT_EQ(0, pipe(pipefd));

    IOEventDataId event_data_id = 12345;
    EXPECT_EQ(0, dispatcher_->AddConsumer(event_data_id, pipefd[0]));
    EXPECT_EQ(0, dispatcher_->UnregisterEvent(event_data_id, pipefd[0], true));

    close(pipefd[0]);
    close(pipefd[1]);
}

TEST_F(IoUringEventDispatcherTest, UnregisterEventWithoutPollin) {
    int pipefd[2];
    ASSERT_EQ(0, pipe(pipefd));

    IOEventDataId event_data_id = 12345;
    EXPECT_EQ(0, dispatcher_->AddConsumer(event_data_id, pipefd[0]));
    EXPECT_EQ(0, dispatcher_->UnregisterEvent(event_data_id, pipefd[0], false));

    close(pipefd[0]);
    close(pipefd[1]);
}

struct EventCallbackData {
    std::atomic<int> input_callback_count{0};
    std::atomic<int> output_callback_count{0};
    std::atomic<bool> stop_test{false};
};

static int TestInputCallback(void* user_data, uint32_t events,
                            const bthread_attr_t& thread_attr) {
    EventCallbackData* data = static_cast<EventCallbackData*>(user_data);
    data->input_callback_count.fetch_add(1, std::memory_order_relaxed);
    return 0;
}

static int TestOutputCallback(void* user_data, uint32_t events,
                             const bthread_attr_t& thread_attr) {
    EventCallbackData* data = static_cast<EventCallbackData*>(user_data);
    data->output_callback_count.fetch_add(1, std::memory_order_relaxed);
    return 0;
}

TEST_F(IoUringEventDispatcherTest, EventCallbackWithPipe) {
    int pipefd[2];
    ASSERT_EQ(0, pipe(pipefd));

    EventCallbackData cb_data;

    IOEventDataOptions options;
    options.input_cb = TestInputCallback;
    options.output_cb = TestOutputCallback;
    options.user_data = &cb_data;

    IOEventDataId event_data_id = INVALID_IO_EVENT_DATA_ID;
    ASSERT_EQ(0, IOEventData::Create(&event_data_id, options));
    ASSERT_NE(INVALID_IO_EVENT_DATA_ID, event_data_id);

    ASSERT_EQ(0, dispatcher_->Start(nullptr));

    ASSERT_EQ(0, dispatcher_->AddConsumer(event_data_id, pipefd[0]));

    usleep(100000);

    char buf[1] = {'a'};
    ASSERT_EQ(1, write(pipefd[1], buf, 1));

    usleep(100000);

    ASSERT_GE(cb_data.input_callback_count.load(), 1);

    dispatcher_->Stop();
    dispatcher_->Join();

    close(pipefd[0]);
    close(pipefd[1]);

    IOEventData::SetFailedById(event_data_id);
}

TEST_F(IoUringEventDispatcherTest, ConcurrentFdRegistration) {
    const size_t kNumFd = 20;
    int pipefds[kNumFd][2];

    for (size_t i = 0; i < kNumFd; ++i) {
        ASSERT_EQ(0, pipe(pipefds[i]));
    }

    ASSERT_EQ(0, dispatcher_->Start(nullptr));

    std::atomic<int> success_count{0};

    for (size_t i = 0; i < kNumFd; ++i) {
        IOEventDataId event_data_id = static_cast<IOEventDataId>(i + 100);
        int ret = dispatcher_->AddConsumer(event_data_id, pipefds[i][0]);
        if (ret == 0) {
            success_count.fetch_add(1, std::memory_order_relaxed);
        }
    }

    for (size_t i = 0; i < kNumFd; ++i) {
        if (success_count.load() > (int)i) {
            dispatcher_->RemoveConsumer(pipefds[i][0]);
        }
        close(pipefds[i][0]);
        close(pipefds[i][1]);
    }

    dispatcher_->Stop();
    dispatcher_->Join();

    EXPECT_EQ(kNumFd, success_count.load());
}

TEST_F(IoUringEventDispatcherTest, StressTestWithManyFd) {
    const size_t kNumFd = 50;
    int pipefds[kNumFd][2];

    for (size_t i = 0; i < kNumFd; ++i) {
        ASSERT_EQ(0, pipe(pipefds[i]));
        butil::make_non_blocking(pipefds[i][0]);
    }

    ASSERT_EQ(0, dispatcher_->Start(nullptr));

    for (size_t i = 0; i < kNumFd; ++i) {
        IOEventDataId event_data_id = static_cast<IOEventDataId>(i + 1);
        EXPECT_EQ(0, dispatcher_->AddConsumer(event_data_id, pipefds[i][0]));
    }

    usleep(50000);

    for (size_t i = 0; i < kNumFd; ++i) {
        char buf[1] = {'x'};
        ssize_t n = write(pipefds[i][1], buf, 1);
        (void)n;
    }

    usleep(50000);

    for (size_t i = 0; i < kNumFd; ++i) {
        EXPECT_EQ(0, dispatcher_->RemoveConsumer(pipefds[i][0]));
    }

    dispatcher_->Stop();
    dispatcher_->Join();

    for (size_t i = 0; i < kNumFd; ++i) {
        close(pipefds[i][0]);
        close(pipefds[i][1]);
    }
}

TEST_F(IoUringEventDispatcherTest, RunningStateCheck) {
    EXPECT_FALSE(dispatcher_->Running());

    EXPECT_EQ(0, dispatcher_->Start(nullptr));
    EXPECT_TRUE(dispatcher_->Running());

    dispatcher_->Stop();
    dispatcher_->Join();
    EXPECT_FALSE(dispatcher_->Running());
}

TEST_F(IoUringEventDispatcherTest, JoinWithoutStart) {
    dispatcher_->Join();
    EXPECT_FALSE(dispatcher_->Running());
}

TEST_F(IoUringEventDispatcherTest, StopWithoutStart) {
    dispatcher_->Stop();
    dispatcher_->Join();
    EXPECT_FALSE(dispatcher_->Running());
}

TEST_F(IoUringEventDispatcherTest, WakeupPipeFunctionality) {
    ASSERT_EQ(0, dispatcher_->Start(nullptr));
    EXPECT_TRUE(dispatcher_->Running());

    dispatcher_->Stop();
    dispatcher_->Join();
    EXPECT_FALSE(dispatcher_->Running());
}

TEST_F(IoUringEventDispatcherTest, AddRemoveConsumerSequence) {
    const size_t kNumIterations = 10;

    ASSERT_EQ(0, dispatcher_->Start(nullptr));

    for (size_t iter = 0; iter < kNumIterations; ++iter) {
        int pipefd[2];
        ASSERT_EQ(0, pipe(pipefd));

        IOEventDataId event_data_id = static_cast<IOEventDataId>(iter + 1);
        EXPECT_EQ(0, dispatcher_->AddConsumer(event_data_id, pipefd[0]));

        char buf[1] = {'b'};
        ASSERT_EQ(1, write(pipefd[1], buf, 1));

        usleep(10000);

        EXPECT_EQ(0, dispatcher_->RemoveConsumer(pipefd[0]));

        close(pipefd[0]);
        close(pipefd[1]);
    }

    dispatcher_->Stop();
    dispatcher_->Join();
}

} // namespace testing
} // namespace brpc

#else

#include <gtest/gtest.h>

TEST(IoUringTest, DISABLED_IoUringNotSupported) {
    GTEST_SKIP() << "io_uring is not enabled in this build";
}

#endif // BRPC_WITH_IO_URING
