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

#include <dirent.h>
#include <fcntl.h>
#include <linux/memfd.h>
#include <sys/eventfd.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <limits>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <gflags/gflags.h>
#include <gtest/gtest.h>

#include "brpc/memfd/shm_block_allocator.h"
#include "brpc/memfd/shm_session.h"
#include "brpc/memfd/shm_zero_copy_stream.h"

namespace brpc {

DECLARE_uint64(shm_buff_size);
DECLARE_int64(shm_block_size);
DECLARE_uint64(shm_queue_size);

namespace {

std::string UniqueName(const char* prefix) {
    static std::atomic<unsigned> sequence(0);
    return std::string(prefix) + std::to_string(getpid()) + "_" +
           std::to_string(sequence.fetch_add(1));
}

size_t MappedSize(int fd) {
    struct stat st;
    EXPECT_EQ(0, fstat(fd, &st));
    return static_cast<size_t>(st.st_size);
}

void CollectQueueElement(void* arg, const ShmQueueElement* elem) {
    static_cast<std::vector<ShmQueueElement>*>(arg)->push_back(*elem);
}

int CountOpenFds() {
    DIR* dir = opendir("/proc/self/fd");
    if (!dir) {
        return -1;
    }
    int count = 0;
    while (readdir(dir) != nullptr) {
        ++count;
    }
    closedir(dir);
    return count;
}

int SendDescriptors(int socket_fd, const std::vector<int>& fds) {
    char byte = 0;
    iovec iov = {&byte, 1};
    std::vector<char> control(CMSG_SPACE(fds.size() * sizeof(int)));
    msghdr msg = {};
    msg.msg_iov = &iov;
    msg.msg_iovlen = 1;
    msg.msg_control = control.data();
    msg.msg_controllen = control.size();
    cmsghdr* cmsg = CMSG_FIRSTHDR(&msg);
    cmsg->cmsg_level = SOL_SOCKET;
    cmsg->cmsg_type = SCM_RIGHTS;
    cmsg->cmsg_len = CMSG_LEN(fds.size() * sizeof(int));
    memcpy(CMSG_DATA(cmsg), fds.data(), fds.size() * sizeof(int));
    return static_cast<int>(sendmsg(socket_fd, &msg, 0));
}

class MemfdCoreTest : public testing::Test {
protected:
    static void SetUpTestSuite() {
        FLAGS_shm_block_size = 256;
        FLAGS_shm_buff_size = 16 * 1024;
        FLAGS_shm_queue_size = 4096;
        ASSERT_EQ(0, InitShmAllocator());
        ASSERT_NE(nullptr, GetShmAllocator());
    }
};

TEST_F(MemfdCoreTest, BlockAllocatorRejectsInvalidMappings) {
    ShmBlockAllocator allocator;
    EXPECT_EQ(-1, allocator.Init(UniqueName("zero_"), 0));
    EXPECT_EQ(-1, allocator.Init(UniqueName("small_"), sizeof(ShmBufferHeader)));
    EXPECT_EQ(-1, allocator.OpenFromFd(-1, 0));
    EXPECT_FALSE(allocator.IsValidDataRange(0, 0));
    allocator.FreeBlock(0);
    allocator.MarkWriteComplete(0);

    int fd = eventfd(0, EFD_CLOEXEC);
    ASSERT_GE(fd, 0);
    EXPECT_EQ(-1, allocator.OpenFromFd(fd, sizeof(ShmBufferHeader)));
    close(fd);

    int null_fd = open("/dev/null", O_RDWR | O_CLOEXEC);
    ASSERT_GE(null_fd, 0);
    EXPECT_EQ(-1, allocator.OpenFromFd(null_fd, 4096));
    close(null_fd);

    EXPECT_EQ(-1, allocator.Init(std::string(300, 'n'), 4096));

    const int64_t saved_block_size = FLAGS_shm_block_size;
    FLAGS_shm_block_size = sizeof(butil::IOBuf::Block);
    EXPECT_EQ(-1, allocator.Init(UniqueName("bad_block_"), 4096));
    FLAGS_shm_block_size = saved_block_size;
}

TEST_F(MemfdCoreTest, BlockAllocatorValidatesEveryHeaderInvariant) {
    ShmBlockAllocator owner;
    ASSERT_EQ(0, owner.Init(UniqueName("headers_"), 4096));
    const size_t mapped_size = MappedSize(owner.memfd());
    char saved[sizeof(ShmBufferHeader)];
    memcpy(saved, owner._header, sizeof(saved));
    const uint32_t saved_block_count = owner._header->block_count;

    auto expect_rejected = [&] {
        int fd = dup(owner.memfd());
        ASSERT_GE(fd, 0);
        ShmBlockAllocator peer;
        EXPECT_EQ(-1, peer.OpenFromFd(fd, mapped_size));
        close(fd);
        memcpy(owner._header, saved, sizeof(saved));
    };

    owner._header->magic = 0;
    expect_rejected();
    owner._header->version = 0;
    expect_rejected();
    owner._header->version = SHM_VERSION_CURRENT + 1;
    expect_rejected();
    owner._header->header_size = sizeof(ShmBufferHeader) - 1;
    expect_rejected();
    owner._header->header_size = mapped_size + 1;
    expect_rejected();
    owner._header->block_state_size = sizeof(ShmBlockState) + 1;
    expect_rejected();
    owner._header->block_size = sizeof(butil::IOBuf::Block);
    expect_rejected();
    owner._header->block_count = 0;
    expect_rejected();
    owner._header->block_count = std::numeric_limits<uint32_t>::max();
    expect_rejected();
    owner._header->free_head.store(saved_block_count);
    expect_rejected();
    owner._header->free_tail.store(saved_block_count);
    expect_rejected();
    owner._header->free_count.store(-1);
    expect_rejected();
    owner._header->free_count.store(saved_block_count + 1);
    expect_rejected();
    owner._header->initialized.store(2);
    expect_rejected();
}

TEST_F(MemfdCoreTest, BlockAllocatorAllocatesMapsAndRecycles) {
    ShmBlockAllocator owner;
    ASSERT_EQ(0, owner.Init(UniqueName("allocator_"), 4096));
    EXPECT_TRUE(owner.IsWritable());
    EXPECT_GE(owner.memfd(), 0);
    EXPECT_GE(owner.notify_fd(), 0);
    EXPECT_EQ(0, owner.WaitNotify());

    int peer_fd = dup(owner.memfd());
    ASSERT_GE(peer_fd, 0);
    ShmBlockAllocator peer;
    ASSERT_EQ(0, peer.OpenFromFd(peer_fd, MappedSize(peer_fd)));
    EXPECT_EQ(SHM_VERSION_CURRENT, peer.peer_version());
    EXPECT_EQ(0, peer.peer_features());

    butil::IOBuf::Block* block = owner.TryAllocBlock();
    ASSERT_NE(nullptr, block);
    EXPECT_TRUE(block->is_shm());
    const uint32_t index = owner.GetBlockIndex(block);
    EXPECT_NE(SHM_INVALID_BLOCK_INDEX, index);
    EXPECT_EQ(index, owner.GetBlockIndexFromOffset(owner.GetDataOffset(block->data)));
    EXPECT_EQ(block->data,
              owner.GetDataPtrFromOffset(owner.GetDataOffset(block->data)));
    EXPECT_TRUE(owner.IsValidDataRange(
        owner.GetDataOffset(block->data), block->cap));
    EXPECT_TRUE(owner.IsValidDataRange(
        owner._header->block_count * owner._header->block_size, 0));
    EXPECT_FALSE(owner.IsValidDataRange(
        owner._header->block_size - 1, 2));
    EXPECT_FALSE(owner.IsValidDataRange(
        std::numeric_limits<size_t>::max(), 1));
    EXPECT_EQ(0, owner.IncPendingCount(index));
    owner.MarkWriteComplete(index);
    EXPECT_TRUE(owner.IsWriteComplete(index));
    ShmBlockRecycler recycler(&owner, index);
    recycler(nullptr);

    butil::IOBuf::Block outside(nullptr, 0, 0);
    EXPECT_EQ(SHM_INVALID_BLOCK_INDEX, owner.GetBlockIndex(&outside));

    const uint32_t saved_head =
        owner._header->free_head.load(std::memory_order_relaxed);
    owner._header->free_head.store(
        owner._header->block_count, std::memory_order_relaxed);
    EXPECT_EQ(nullptr, owner.TryAllocBlock());
    owner._header->free_head.store(saved_head, std::memory_order_relaxed);
}

TEST_F(MemfdCoreTest, BlockAllocatorInitialFreeListIsConcurrentSafe) {
    ShmBlockAllocator allocator;
    ASSERT_EQ(0, allocator.Init(UniqueName("concurrent_init_"), 64 * 1024));
    ASSERT_EQ(1, allocator._header->initialized.load());

    std::atomic<bool> start(false);
    std::mutex blocks_mutex;
    std::vector<butil::IOBuf::Block*> blocks;
    std::vector<std::thread> threads;
    for (int i = 0; i < 8; ++i) {
        threads.emplace_back([&] {
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            while (butil::IOBuf::Block* block = allocator.TryAllocBlock()) {
                std::lock_guard<std::mutex> lock(blocks_mutex);
                blocks.push_back(block);
            }
        });
    }
    start.store(true, std::memory_order_release);
    for (std::thread& thread : threads) {
        thread.join();
    }

    ASSERT_EQ(allocator._header->block_count - 1, blocks.size());
    std::vector<uint32_t> indices;
    for (butil::IOBuf::Block* block : blocks) {
        indices.push_back(allocator.GetBlockIndex(block));
    }
    std::sort(indices.begin(), indices.end());
    EXPECT_EQ(indices.end(), std::unique(indices.begin(), indices.end()));

    for (butil::IOBuf::Block* block : blocks) {
        const uint32_t index = allocator.GetBlockIndex(block);
        block->~Block();
        allocator.FreeBlock(index);
    }
    EXPECT_EQ(static_cast<int32_t>(allocator._header->block_count),
              allocator._header->free_count.load());
}

TEST_F(MemfdCoreTest, BlockRecycleIsClaimedExactlyOnce) {
    ShmBlockAllocator allocator;
    ASSERT_EQ(0, allocator.Init(UniqueName("recycle_race_"), 4096));
    const int32_t initial_free_count = allocator._header->free_count.load();

    for (int round = 0; round < 1000; ++round) {
        butil::IOBuf::Block* block = allocator.TryAllocBlock();
        ASSERT_NE(nullptr, block);
        const uint32_t index = allocator.GetBlockIndex(block);
        ASSERT_EQ(0, allocator.IncPendingCount(index));
        block->~Block();

        std::atomic<bool> start(false);
        std::thread writer([&] {
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            allocator.MarkWriteComplete(index);
        });
        std::thread reader([&] {
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            ShmBlockRecycler recycler(&allocator, index);
            recycler(nullptr);
        });
        start.store(true, std::memory_order_release);
        writer.join();
        reader.join();

        EXPECT_EQ(1, allocator._block_states[index].recycle_claimed.load());
        ASSERT_EQ(initial_free_count, allocator._header->free_count.load());
    }
}

TEST_F(MemfdCoreTest, BlockAllocatorExhaustionAndNotification) {
    ShmBlockAllocator allocator;
    ASSERT_EQ(0, allocator.Init(UniqueName("exhaust_"), 2048));

    std::vector<butil::IOBuf::Block*> blocks;
    while (butil::IOBuf::Block* block = allocator.TryAllocBlock()) {
        blocks.push_back(block);
    }
    ASSERT_FALSE(blocks.empty());
    EXPECT_FALSE(allocator.IsWritable());
    EXPECT_EQ(nullptr, allocator.TryAllocBlock());

    const uint32_t index = allocator.GetBlockIndex(blocks.back());
    blocks.back()->~Block();
    allocator.FreeBlock(index);
    blocks.pop_back();
    EXPECT_TRUE(allocator.IsWritable());

    allocator.NotifyWaiter();
    uint64_t value = 0;
    EXPECT_EQ(static_cast<ssize_t>(sizeof(value)),
              read(allocator.notify_fd(), &value, sizeof(value)));
    EXPECT_EQ(1u, value);

    for (butil::IOBuf::Block* block : blocks) {
        const uint32_t block_index = allocator.GetBlockIndex(block);
        block->~Block();
        allocator.FreeBlock(block_index);
    }
    allocator.FreeBlock(SHM_INVALID_BLOCK_INDEX);
    allocator.MarkWriteComplete(SHM_INVALID_BLOCK_INDEX);
}

TEST_F(MemfdCoreTest, BlockAllocatorWaitsForRecycleAndTimesOut) {
    ShmBlockAllocator allocator;
    ASSERT_EQ(0, allocator.Init(UniqueName("wait_"), 2048));
    std::vector<butil::IOBuf::Block*> blocks;
    while (butil::IOBuf::Block* block = allocator.TryAllocBlock()) {
        blocks.push_back(block);
    }
    ASSERT_FALSE(blocks.empty());

    std::atomic<int> wait_result(99);
    std::thread waiter([&] {
        wait_result.store(allocator.WaitNotify(), std::memory_order_relaxed);
    });
    for (int i = 0; i < 1000 && !allocator.IsWaitNotify(); ++i) {
        std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
    ASSERT_TRUE(allocator.IsWaitNotify());
    uint32_t index = allocator.GetBlockIndex(blocks.back());
    blocks.back()->~Block();
    allocator.FreeBlock(index);
    blocks.pop_back();
    waiter.join();
    EXPECT_EQ(0, wait_result.load());

    butil::IOBuf::Block* replacement = allocator.TryAllocBlock();
    ASSERT_NE(nullptr, replacement);
    // The first waiter may observe the newly freed block before entering
    // epoll, leaving the edge notification pending. Drain that permitted
    // spurious notification so the next assertion measures the timeout path.
    uint64_t stale_notification = 0;
    while (read(allocator.notify_fd(), &stale_notification,
                sizeof(stale_notification)) ==
           static_cast<ssize_t>(sizeof(stale_notification))) {
    }
    timespec deadline;
    clock_gettime(CLOCK_REALTIME, &deadline);
    deadline.tv_nsec += 5 * 1000 * 1000;
    if (deadline.tv_nsec >= 1000 * 1000 * 1000) {
        ++deadline.tv_sec;
        deadline.tv_nsec -= 1000 * 1000 * 1000;
    }
    EXPECT_EQ(-1, allocator.WaitNotify(&deadline));
    EXPECT_EQ(ETIMEDOUT, errno);

    replacement->~Block();
    allocator.FreeBlock(allocator.GetBlockIndex(replacement));
    for (butil::IOBuf::Block* block : blocks) {
        const uint32_t block_index = allocator.GetBlockIndex(block);
        block->~Block();
        allocator.FreeBlock(block_index);
    }

    const int saved_notify_fd = allocator.notify_fd();
    allocator.set_notify_fd(-1);
    allocator.NotifyWaiter();
    allocator.set_notify_fd(saved_notify_fd);
}

TEST_F(MemfdCoreTest, QueueValidatesMetadataAndProcessesRing) {
    std::vector<char> storage(sizeof(ShmQueueHeader) +
                              3 * sizeof(ShmQueueElement));
    ShmQueue queue;
    EXPECT_EQ(-1, queue.Init(nullptr, storage.size()));
    EXPECT_EQ(-1, queue.Init(storage.data(), sizeof(ShmQueueHeader) - 1));
    EXPECT_EQ(-1, queue.Init(storage.data(), sizeof(ShmQueueHeader)));
    ASSERT_EQ(0, queue.Init(storage.data(), storage.size()));
    EXPECT_TRUE(queue.IsFree());
    EXPECT_TRUE(queue.IsNofify());
    EXPECT_FALSE(queue.IsNofify());

    ShmQueue peer;
    ASSERT_EQ(0, peer.Open(storage.data(), storage.size()));
    EXPECT_EQ(SHM_VERSION_CURRENT, peer.peer_version());
    EXPECT_EQ(0, peer.peer_features());

    ASSERT_EQ(0, queue.Enqueue(10, 11));
    ASSERT_EQ(0, queue.Enqueue(20, 21));
    ASSERT_EQ(0, queue.Enqueue(30, 31));
    EXPECT_FALSE(queue.IsFree());
    EXPECT_EQ(-1, queue.Enqueue(40, 41));

    timespec deadline;
    clock_gettime(CLOCK_REALTIME, &deadline);
    deadline.tv_nsec += 5 * 1000 * 1000;
    if (deadline.tv_nsec >= 1000 * 1000 * 1000) {
        ++deadline.tv_sec;
        deadline.tv_nsec -= 1000 * 1000 * 1000;
    }
    EXPECT_EQ(-1, queue.WaitNotify(&deadline));
    EXPECT_EQ(ETIMEDOUT, errno);

    std::vector<ShmQueueElement> elements;
    EXPECT_EQ(2, queue.DequeueAndProcessBatch(
                     CollectQueueElement, &elements, 2));
    ASSERT_EQ(2u, elements.size());
    EXPECT_EQ(10u, elements[0].offset);
    EXPECT_EQ(21u, elements[1].data_size);
    EXPECT_EQ(1u, queue.Size());

    ASSERT_EQ(0, queue.Enqueue(40, 41));
    EXPECT_EQ(2, queue.DequeueAndProcessBatch(
                     CollectQueueElement, &elements, 8));
    EXPECT_EQ(0u, queue.Size());
    EXPECT_EQ(0, queue.DequeueAndProcessBatch(
                     CollectQueueElement, &elements, 8));

    queue.SetReading(false);
    EXPECT_TRUE(queue.IsNofify());
    queue.SetReading(true);
    EXPECT_FALSE(queue.IsNofify());

    ShmQueue invalid_peer;
    std::vector<char> invalid(storage.size(), 0);
    EXPECT_EQ(-1, invalid_peer.Open(invalid.data(), invalid.size()));
}

TEST_F(MemfdCoreTest, QueueValidatesEveryHeaderInvariant) {
    std::vector<char> storage(sizeof(ShmQueueHeader) +
                              3 * sizeof(ShmQueueElement));
    ShmQueue queue;
    ASSERT_EQ(0, queue.Init(storage.data(), storage.size()));
    char saved[sizeof(ShmQueueHeader)];
    memcpy(saved, queue._header, sizeof(saved));
    const uint32_t saved_queue_size = queue._header->queue_size;

    auto expect_rejected = [&] {
        ShmQueue peer;
        EXPECT_EQ(-1, peer.Open(storage.data(), storage.size()));
        memcpy(queue._header, saved, sizeof(saved));
    };

    queue._header->magic = 0;
    expect_rejected();
    queue._header->version = 0;
    expect_rejected();
    queue._header->version = SHM_VERSION_CURRENT + 1;
    expect_rejected();
    queue._header->header_size = sizeof(ShmQueueHeader) - 1;
    expect_rejected();
    queue._header->header_size = storage.size() + 1;
    expect_rejected();
    queue._header->elem_size = sizeof(ShmQueueElement) - 1;
    expect_rejected();
    queue._header->queue_size = 0;
    expect_rejected();
    queue._header->queue_size = std::numeric_limits<uint32_t>::max();
    expect_rejected();
    queue._header->write_pos.store(0);
    queue._header->read_pos.store(1);
    expect_rejected();
    queue._header->write_pos.store(saved_queue_size + 1);
    queue._header->read_pos.store(0);
    expect_rejected();
    queue._header->reading.store(2);
    expect_rejected();
}

TEST_F(MemfdCoreTest, QueueWaitsForSpaceAndCoversInvalidOperations) {
    ShmQueue empty;
    EXPECT_EQ(0u, empty.Size());
    EXPECT_FALSE(empty.IsFree());
    empty.SetReading(true);
    EXPECT_FALSE(empty.IsNofify());
    EXPECT_EQ(-1, empty.Enqueue(1, 2));
    EXPECT_EQ(0, empty.DequeueAndProcessBatch(
                     nullptr, nullptr, 1));

    std::vector<char> storage(sizeof(ShmQueueHeader) +
                              2 * sizeof(ShmQueueElement));
    ShmQueue queue;
    ASSERT_EQ(0, queue.Init(storage.data(), storage.size()));
    ASSERT_EQ(0, queue.Enqueue(1, 2));
    ASSERT_EQ(0, queue.Enqueue(3, 4));
    ASSERT_FALSE(queue.IsFree());

    std::atomic<int> wait_result(99);
    std::thread waiter([&] {
        wait_result.store(queue.WaitNotify(nullptr), std::memory_order_relaxed);
    });
    for (int i = 0;
         i < 1000 && queue._free_notify.load() == 0; ++i) {
        std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
    ASSERT_GT(queue._free_notify.load(), 0u);
    std::vector<ShmQueueElement> elements;
    EXPECT_EQ(1, queue.DequeueAndProcessBatch(
                     CollectQueueElement, &elements, 1));
    waiter.join();
    EXPECT_EQ(0, wait_result.load());

    const int notify_fd = queue.notify_fd();
    queue.set_notify_fd(-1);
    queue.NotifyWaiter();
    queue.set_notify_fd(notify_fd);
    queue.Reset();
    EXPECT_EQ(-1, fcntl(notify_fd, F_GETFD));
    queue.Reset();
}

TEST_F(MemfdCoreTest, QueueManagerMapsSharedQueueAndClosesIdempotently) {
    ShmQueueManager sender;
    ASSERT_EQ(0, sender.CreateSendQueue(UniqueName("queue_")));
    ASSERT_GE(sender.send_queue_fd(), 0);
    ASSERT_EQ(0, sender.send_queue()->Enqueue(123, 456));

    int recv_fd = dup(sender.send_queue_fd());
    int notify_fd = dup(sender.send_queue()->notify_fd());
    ASSERT_GE(recv_fd, 0);
    ASSERT_GE(notify_fd, 0);

    ShmQueueManager receiver;
    ASSERT_EQ(0, receiver.MapRecvQueueFromFd(recv_fd, notify_fd));
    EXPECT_EQ(1u, receiver.recv_queue()->Size());
    std::vector<ShmQueueElement> elements;
    EXPECT_EQ(1, receiver.recv_queue()->DequeueAndProcessBatch(
                     CollectQueueElement, &elements, 1));
    ASSERT_EQ(1u, elements.size());
    EXPECT_EQ(123u, elements[0].offset);
    EXPECT_EQ(456u, elements[0].data_size);

    ShmQueueManager invalid;
    EXPECT_EQ(-1, invalid.MapRecvQueueFromFd(-1, -1));
    receiver.Close();
    receiver.Close();
    sender.Close();
    sender.Close();
}

TEST_F(MemfdCoreTest, QueueManagerRejectsCreationAndMappingFailures) {
    ShmQueueManager manager;
    EXPECT_EQ(-1, manager.CreateSendQueue(std::string(300, 'q')));

    const uint64_t saved_queue_size = FLAGS_shm_queue_size;
    FLAGS_shm_queue_size =
        sizeof(ShmQueueHeader) + sizeof(ShmQueueElement) - 1;
    EXPECT_EQ(-1, manager.CreateSendQueue(UniqueName("tiny_queue_")));
    FLAGS_shm_queue_size = saved_queue_size;

    EXPECT_EQ(-1, manager.MapRecvQueueFromFd(-1, -1));
    int null_fd = open("/dev/null", O_RDWR | O_CLOEXEC);
    ASSERT_GE(null_fd, 0);
    EXPECT_EQ(-1, manager.MapRecvQueueFromFd(null_fd, -1));
    close(null_fd);

    int invalid_fd = syscall(
        SYS_memfd_create, "invalid_queue", MFD_CLOEXEC);
    ASSERT_GE(invalid_fd, 0);
    ASSERT_EQ(0, ftruncate(invalid_fd, 4096));
    int notify_fd = eventfd(0, EFD_CLOEXEC);
    ASSERT_GE(notify_fd, 0);
    EXPECT_EQ(-1, manager.MapRecvQueueFromFd(invalid_fd, notify_fd));
    close(invalid_fd);
    close(notify_fd);
}

TEST_F(MemfdCoreTest, SessionTransfersDescriptorsOverUnixSocket) {
    ShmSession sender;
    ShmSession receiver;
    ASSERT_EQ(0, sender.Init(nullptr, false));
    ASSERT_EQ(0, receiver.Init(nullptr, true));

    int fds[2];
    ASSERT_EQ(0, socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, fds));
    ASSERT_EQ(0, sender.SendFdsToPeer(fds[0]));
    ASSERT_EQ(0, receiver.RecvFdsFromPeer(fds[1]));
    ASSERT_NE(nullptr, receiver.recv_allocator());
    ASSERT_NE(nullptr, receiver.queue_manager()->recv_queue());
    EXPECT_EQ(SHM_VERSION_CURRENT,
              receiver.recv_allocator()->peer_version());

    close(fds[0]);
    close(fds[1]);
    sender.Close();
    receiver.Close();
    sender.Close();
    receiver.Close();
}

TEST_F(MemfdCoreTest, SessionHandlesMissingAndInvalidControlMessages) {
    ShmSession receiver;
    ASSERT_EQ(0, receiver.Init(nullptr, false));

    int fds[2];
    ASSERT_EQ(0, socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK, 0, fds));
    EXPECT_EQ(1, receiver.RecvFdsFromPeer(fds[1]));

    char byte = 0;
    ASSERT_EQ(1, write(fds[0], &byte, 1));
    EXPECT_EQ(-1, receiver.RecvFdsFromPeer(fds[1]));

    close(fds[0]);
    close(fds[1]);
}

TEST_F(MemfdCoreTest, SessionRejectsShortFdListWithoutLeaking) {
    ShmSession receiver;
    ASSERT_EQ(0, receiver.Init(nullptr, true));
    int sockets[2];
    ASSERT_EQ(0, socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC,
                            0, sockets));
    int sent_fd = eventfd(0, EFD_CLOEXEC);
    ASSERT_GE(sent_fd, 0);
    const int before = CountOpenFds();
    ASSERT_GT(SendDescriptors(sockets[0], {sent_fd}), 0);
    EXPECT_EQ(-1, receiver.RecvFdsFromPeer(sockets[1]));
    EXPECT_EQ(before, CountOpenFds());
    close(sent_fd);
    close(sockets[0]);
    close(sockets[1]);
}

TEST_F(MemfdCoreTest, SessionCleansDescriptorsWhenQueueMappingFails) {
    ShmBlockAllocator owner;
    ASSERT_EQ(0, owner.Init(UniqueName("bad_session_"), 4096));
    ShmSession receiver;
    ASSERT_EQ(0, receiver.Init(nullptr, true));
    int sockets[2];
    ASSERT_EQ(0, socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC,
                            0, sockets));
    int bad_queue_fd = eventfd(0, EFD_CLOEXEC);
    int queue_notify_fd = eventfd(0, EFD_CLOEXEC);
    ASSERT_GE(bad_queue_fd, 0);
    ASSERT_GE(queue_notify_fd, 0);
    const int before = CountOpenFds();
    ASSERT_GT(SendDescriptors(
        sockets[0],
        {owner.memfd(), bad_queue_fd, owner.notify_fd(), queue_notify_fd}), 0);
    EXPECT_EQ(-1, receiver.RecvFdsFromPeer(sockets[1]));
    EXPECT_EQ(before, CountOpenFds());

    EXPECT_EQ(-1, receiver.SendFdsToPeer(-1));
    close(bad_queue_fd);
    close(queue_notify_fd);
    close(sockets[0]);
    close(sockets[1]);
}

TEST_F(MemfdCoreTest, PeerRegistryReusesMappingAndOwnsDescriptors) {
    ShmBlockAllocator owner;
    ASSERT_EQ(0, owner.Init(UniqueName("registry_"), 4096));

    int first_fd = dup(owner.memfd());
    int first_notify_fd = dup(owner.notify_fd());
    ASSERT_GE(first_fd, 0);
    ASSERT_GE(first_notify_fd, 0);
    ShmBlockAllocator* first = PeerShmRegistry::Instance()->GetOrCreate(
        first_fd, MappedSize(first_fd), first_notify_fd);
    ASSERT_NE(nullptr, first);

    int duplicate_fd = dup(owner.memfd());
    int duplicate_notify_fd = dup(owner.notify_fd());
    ASSERT_GE(duplicate_fd, 0);
    ASSERT_GE(duplicate_notify_fd, 0);
    ShmBlockAllocator* duplicate = PeerShmRegistry::Instance()->GetOrCreate(
        duplicate_fd, MappedSize(duplicate_fd), duplicate_notify_fd);
    EXPECT_EQ(first, duplicate);
    EXPECT_EQ(-1, fcntl(duplicate_fd, F_GETFD));
    EXPECT_EQ(EBADF, errno);
    EXPECT_EQ(-1, fcntl(duplicate_notify_fd, F_GETFD));
    EXPECT_EQ(EBADF, errno);

    PeerShmRegistry::Instance()->Release(first);
    EXPECT_NE(-1, fcntl(first_fd, F_GETFD));
    EXPECT_NE(-1, fcntl(first_notify_fd, F_GETFD));

    PeerShmRegistry::Instance()->Release(duplicate);
    EXPECT_EQ(-1, fcntl(first_fd, F_GETFD));
    EXPECT_EQ(EBADF, errno);
    EXPECT_EQ(-1, fcntl(first_notify_fd, F_GETFD));
    EXPECT_EQ(EBADF, errno);
}

TEST_F(MemfdCoreTest, PeerRegistryRejectsInvalidAndUnknownEntries) {
    EXPECT_EQ(nullptr, PeerShmRegistry::Instance()->GetOrCreate(
                           -1, 0, -1));
    int fd = eventfd(0, EFD_CLOEXEC);
    int notify_fd = eventfd(0, EFD_CLOEXEC);
    ASSERT_GE(fd, 0);
    ASSERT_GE(notify_fd, 0);
    EXPECT_EQ(nullptr, PeerShmRegistry::Instance()->GetOrCreate(
                           fd, sizeof(ShmBufferHeader), notify_fd));
    close(fd);
    close(notify_fd);
    PeerShmRegistry::Instance()->Release(nullptr);

    ShmBlockAllocator unknown;
    PeerShmRegistry::Instance()->Release(&unknown);
    EXPECT_EQ(0, InitShmAllocator());
}

TEST_F(MemfdCoreTest, TlsCacheSharesAcquiresCapsAndCleansOnThreadExit) {
    ShmBlockAllocator* allocator = GetShmAllocator();
    ASSERT_NE(nullptr, allocator);

    butil::IOBuf::Block* shared = share_shm_tls_block(allocator);
    ASSERT_NE(nullptr, shared);
    EXPECT_EQ(shared, share_shm_tls_block(allocator));
    EXPECT_EQ(shared, acquire_shm_tls_block(allocator));
    release_shm_tls_block(shared);
    release_shm_tls_block(nullptr);

    butil::IOBuf::Block* full = acquire_shm_tls_block(allocator);
    ASSERT_NE(nullptr, full);
    full->size = full->cap;
    release_shm_tls_block(full);
    butil::IOBuf::Block* replacement =
        acquire_shm_tls_block(allocator);
    ASSERT_NE(nullptr, replacement);
    EXPECT_NE(full, replacement);
    release_shm_tls_block(replacement);

    std::vector<butil::IOBuf::Block*> blocks;
    for (int i = 0; i < MAX_SHM_BLOCKS_PER_THREAD + 1; ++i) {
        butil::IOBuf::Block* block = acquire_shm_tls_block(allocator);
        ASSERT_NE(nullptr, block);
        blocks.push_back(block);
    }
    for (butil::IOBuf::Block* block : blocks) {
        release_shm_tls_block(block);
    }

    std::thread thread([allocator] {
        butil::IOBuf::Block* block = share_shm_tls_block(allocator);
        ASSERT_NE(nullptr, block);
    });
    thread.join();
}

TEST_F(MemfdCoreTest, ZeroCopyStreamWritesBacksUpAndFinishes) {
    butil::IOBuf output;
    ShmZeroCopyOutputStream stream(GetShmAllocator(), &output);
    void* data = nullptr;
    int size = 0;
    ASSERT_TRUE(stream.Next(&data, &size));
    ASSERT_GT(size, 16);
    memcpy(data, "shared-memory", 13);
    stream.BackUp(size - 13);
    EXPECT_EQ(13, stream.ByteCount());
    EXPECT_EQ(13u, output.size());
    EXPECT_EQ("shared-memory", output.to_string());
    stream.Finish();
    stream.Finish();
}

TEST_F(MemfdCoreTest, ZeroCopyStreamFallsBackWhenAllocatorIsExhausted) {
    ShmBlockAllocator allocator;
    ASSERT_EQ(0, allocator.Init(UniqueName("fallback_"), 1024));
    std::vector<butil::IOBuf::Block*> blocks;
    while (butil::IOBuf::Block* block = allocator.TryAllocBlock()) {
        blocks.push_back(block);
    }

    butil::IOBuf output;
    ShmZeroCopyOutputStream stream(&allocator, &output);
    allocator._header->free_notify.store(1);
    void* data = nullptr;
    int size = 0;
    ASSERT_TRUE(stream.Next(&data, &size));
    ASSERT_GT(size, 4);
    memcpy(data, "heap", 4);
    stream.BackUp(size - 4);
    EXPECT_EQ(4, stream.ByteCount());
    EXPECT_EQ("heap", output.to_string());
    allocator._header->free_notify.store(0);

    for (butil::IOBuf::Block* block : blocks) {
        const uint32_t index = allocator.GetBlockIndex(block);
        block->~Block();
        allocator.FreeBlock(index);
    }
}

TEST_F(MemfdCoreTest, ZeroCopyStreamCoversMultipleBlocksAndFallbackBackup) {
    butil::IOBuf output;
    ShmZeroCopyOutputStream stream(GetShmAllocator(), &output);
    void* first = nullptr;
    int first_size = 0;
    ASSERT_TRUE(stream.Next(&first, &first_size));
    ASSERT_GT(first_size, 0);
    memset(first, 'a', first_size);
    void* second = nullptr;
    int second_size = 0;
    ASSERT_TRUE(stream.Next(&second, &second_size));
    ASSERT_GT(second_size, 0);
    stream.BackUp(second_size);
    EXPECT_EQ(first_size, stream.ByteCount());
    stream.Finish();

    ShmBlockAllocator exhausted;
    ASSERT_EQ(0, exhausted.Init(UniqueName("fallback_backup_"), 1024));
    std::vector<butil::IOBuf::Block*> blocks;
    while (butil::IOBuf::Block* block = exhausted.TryAllocBlock()) {
        blocks.push_back(block);
    }
    butil::IOBuf fallback_output;
    ShmZeroCopyOutputStream fallback(&exhausted, &fallback_output);
    void* data = nullptr;
    int size = 0;
    ASSERT_TRUE(fallback.Next(&data, &size));
    ASSERT_GT(size, 0);
    memcpy(data, "f", 1);
    fallback.BackUp(size - 1);
    EXPECT_EQ(1, fallback.ByteCount());
    EXPECT_EQ("f", fallback_output.to_string());

    std::atomic<bool> used_fallback(false);
    std::thread no_tls_cache([&] {
        butil::IOBuf thread_output;
        ShmZeroCopyOutputStream thread_stream(&exhausted, &thread_output);
        void* thread_data = nullptr;
        int thread_size = 0;
        used_fallback.store(
            thread_stream.Next(&thread_data, &thread_size) &&
            thread_stream._fallback_stream != nullptr);
    });
    no_tls_cache.join();
    EXPECT_TRUE(used_fallback.load());

    for (butil::IOBuf::Block* block : blocks) {
        const uint32_t index = exhausted.GetBlockIndex(block);
        block->~Block();
        exhausted.FreeBlock(index);
    }
}

}  // namespace
}  // namespace brpc
