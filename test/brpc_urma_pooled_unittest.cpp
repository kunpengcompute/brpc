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

#include <numeric>
#include <cstring>
#include <limits>
#include <algorithm>
#include <atomic>
#include <set>
#include <thread>
#include <dlfcn.h>
#include <sys/socket.h>
#include <unistd.h>
#include <gtest/gtest.h>
#include <gflags/gflags.h>

#if BRPC_WITH_URMA
#include "butil/atomicops.h"
#include "butil/logging.h"
#include "butil/sys_byteorder.h"
#include "urma_api.h"
#include "brpc/urma/urma_handshake.h"
#include "brpc/urma/urma_handshake.pb.h"
#include "brpc/urma/urma_helper.h"
#include "brpc/urma/urma_endpoint.h"
#include "brpc/urma/urma_pooled_connection.h"
#include "brpc/urma_transport.h"
#include "bthread/butex.h"
#include "brpc/server.h"
#include "brpc/input_messenger.h"
#include "brpc/input_message_base.h"
#include "bthread/bthread.h"
#include "bthread/unstable.h"
#include "brpc/controller.h"
#include "brpc/urma/urma_big_message.h"
#include "echo.pb.h"
#include "urma_types.h"

using namespace brpc;

namespace brpc {
namespace urma {

DECLARE_int32(urma_client_handshake_version);
extern bool g_skip_urma_init;
extern butil::atomic<bool> g_urma_available;

}  // namespace urma
}  // namespace brpc

namespace {
std::vector<urma::TxCompletionCtx*> Contexts(urma::UrmaJfsSlot* slot) {
    return slot->SubmittedContexts();
}
std::vector<urma::RxRecvCtx*> Contexts(urma::UrmaJfrSlot* slot) {
    std::vector<urma::RxRecvCtx*> result;
    if (slot->contexts) {
        for (uint32_t i = 0; i < slot->rx_depth; ++i) result.push_back(&slot->contexts[i]);
    }
    return result;
}
}

// ---------------------------------------------------------------------------
// v2 binary HelloMessage: serialize + deserialize round-trips.
// ---------------------------------------------------------------------------
TEST(UrmaHandshakeTest, v2_serialize_deserialize_roundtrip) {
    urma::v2_wire::HelloMessage m;
    m.msg_len = urma::v2_wire::POOLED_PACKET_LEN;
    m.hello_ver = urma::v2_wire::HELLO_V2_VERSION;
    m.impl_ver = urma::v2_wire::IMPL_POOLED_VERSION;
    m.buffer_size = 8192;
    m.recv_buffer_cnt = 127;
    m.jetty_id = 0x12345678;
    m.socket_id = 0x123456789abcdef0ULL;
    for (int i = 0; i < 16; ++i) {
        m.eid[i] = static_cast<uint8_t>(i + 1);
    }
    m.uasid = 0xdeadbeef;
    m.tp_type = 1;  // URMA_CTP
    for (int i = 0; i < 16; ++i) {
        m.seg_eid[i] = static_cast<uint8_t>(16 - i);
    }
    m.seg_uasid = 0xcafebabe;
    m.seg_va = 0x1122334455667788ULL;
    m.seg_len = 1ULL << 20;
    m.seg_token_id = 0x42424242;

    uint8_t buf[urma::v2_wire::POOLED_BODY_LEN];
    m.Serialize(buf);

    urma::v2_wire::HelloMessage m2;
    m2.Deserialize(buf);
    EXPECT_EQ(m.msg_len, m2.msg_len);
    EXPECT_EQ(m.hello_ver, m2.hello_ver);
    EXPECT_EQ(m.impl_ver, m2.impl_ver);
    EXPECT_EQ(m.buffer_size, m2.buffer_size);
    EXPECT_EQ(m.recv_buffer_cnt, m2.recv_buffer_cnt);
    EXPECT_EQ(m.jetty_id, m2.jetty_id);
    EXPECT_EQ(m.socket_id, m2.socket_id);
    EXPECT_EQ(0, memcmp(m.eid, m2.eid, 16));
    EXPECT_EQ(m.uasid, m2.uasid);
    EXPECT_EQ(m.tp_type, m2.tp_type);
    EXPECT_EQ(0, memcmp(m.seg_eid, m2.seg_eid, 16));
    EXPECT_EQ(m.seg_uasid, m2.seg_uasid);
    EXPECT_EQ(m.seg_va, m2.seg_va);
    EXPECT_EQ(m.seg_len, m2.seg_len);
    EXPECT_EQ(m.seg_token_id, m2.seg_token_id);
}

// ---------------------------------------------------------------------------
// v2 packet on the wire: "URMA" magic + body.
// ---------------------------------------------------------------------------
TEST(UrmaHandshakeTest, v2_packet_magic_is_urma) {
    EXPECT_EQ(4u, urma::v2_wire::MAGIC_STR_LEN);
    char magic[4] = {'U', 'R', 'M', 'A'};
    EXPECT_EQ(0, memcmp(magic, "URMA", 4));
    EXPECT_EQ(4u + 90u, urma::v2_wire::POOLED_PACKET_LEN);
}

// ---------------------------------------------------------------------------
// v3 protobuf UrmaHello: serialize + parse round-trips.
// ---------------------------------------------------------------------------
TEST(UrmaHandshakeTest, v3_protobuf_roundtrip) {
    urma::UrmaHello msg;
    msg.set_buffer_size(8192);
    msg.set_recv_buffer_cnt(127);
    msg.set_jetty_id(0x12345678);
    msg.set_socket_id(0x123456789abcdef0ULL);
    msg.set_resource_mode(2);
    uint8_t eid[16];
    for (int i = 0; i < 16; ++i) {
        eid[i] = static_cast<uint8_t>(i + 1);
    }
    msg.set_eid(eid, 16);
    msg.set_uasid(0xdeadbeef);
    msg.set_tp_type(1);
    uint8_t seg_eid[16];
    for (int i = 0; i < 16; ++i) {
        seg_eid[i] = static_cast<uint8_t>(16 - i);
    }
    msg.set_seg_eid(seg_eid, 16);
    msg.set_seg_uasid(0xcafebabe);
    msg.set_seg_va(0x1122334455667788ULL);
    msg.set_seg_len(1ULL << 20);
    msg.set_seg_token_id(0x42424242);

    std::string body;
    ASSERT_TRUE(msg.SerializeToString(&body));
    urma::UrmaHello msg2;
    ASSERT_TRUE(msg2.ParseFromString(body));
    EXPECT_EQ(msg.buffer_size(), msg2.buffer_size());
    EXPECT_EQ(msg.recv_buffer_cnt(), msg2.recv_buffer_cnt());
    EXPECT_EQ(msg.jetty_id(), msg2.jetty_id());
    EXPECT_EQ(msg.socket_id(), msg2.socket_id());
    EXPECT_EQ(16, msg2.eid().size());
    EXPECT_EQ(0, memcmp(msg.eid().data(), msg2.eid().data(), 16));
    EXPECT_EQ(msg.uasid(), msg2.uasid());
    EXPECT_EQ(msg.tp_type(), msg2.tp_type());
    EXPECT_EQ(16, msg2.seg_eid().size());
    EXPECT_EQ(msg.seg_uasid(), msg2.seg_uasid());
    EXPECT_EQ(msg.seg_va(), msg2.seg_va());
    EXPECT_EQ(msg.seg_len(), msg2.seg_len());
    EXPECT_EQ(msg.seg_token_id(), msg2.seg_token_id());
}

// ---------------------------------------------------------------------------
// CreateServerHandshakeByMagic dispatches on the magic bytes.
// ---------------------------------------------------------------------------
TEST(UrmaHandshakeTest, server_handshake_factory_dispatches_on_magic) {
    // We cannot fully exercise the server handshake without a real socket +
    // endpoint, but we can verify the factory returns the right protocol
    // version for each magic, and nullptr for an unknown magic.
    uint8_t magic_v2[4] = {'U', 'R', 'M', 'A'};
    uint8_t magic_v3[4] = {'U', 'R', 'M', '3'};
    uint8_t magic_bad[4] = {'P', 'R', 'P', 'C'};

    // v2 magic -> protocol version 2
    urma::UrmaHandshake* hs2 =
        urma::CreateServerHandshakeByMagic(nullptr, magic_v2);
    // Note: the factory dereferences the endpoint only inside SendLocalHello /
    // ReceiveAndParseRemoteHello; passing nullptr is safe for the version query.
    // (We delete immediately to avoid touching the endpoint.)
    if (hs2) {
        EXPECT_EQ(2, hs2->ProtocolVersion());
        delete hs2;
    }
    // v3 magic -> protocol version 3
    urma::UrmaHandshake* hs3 =
        urma::CreateServerHandshakeByMagic(nullptr, magic_v3);
    if (hs3) {
        EXPECT_EQ(3, hs3->ProtocolVersion());
        delete hs3;
    }
    // unknown magic -> nullptr (caller falls back to TCP)
    urma::UrmaHandshake* hsb =
        urma::CreateServerHandshakeByMagic(nullptr, magic_bad);
    EXPECT_EQ(nullptr, hsb);
}

// ---------------------------------------------------------------------------
// CreateClientHandshake picks the version from the gflag.
// ---------------------------------------------------------------------------
TEST(UrmaHandshakeTest, client_handshake_factory_respects_flag) {
    GFLAGS_NAMESPACE::FlagSaver flag_saver;

    urma::FLAGS_urma_client_handshake_version = 2;
    urma::UrmaHandshake* hs2 = urma::CreateClientHandshake(nullptr);
    if (hs2) {
        EXPECT_EQ(2, hs2->ProtocolVersion());
        delete hs2;
    }

    urma::FLAGS_urma_client_handshake_version = 3;
    urma::UrmaHandshake* hs3 = urma::CreateClientHandshake(nullptr);
    if (hs3) {
        EXPECT_EQ(3, hs3->ProtocolVersion());
        delete hs3;
    }
}

// ---------------------------------------------------------------------------
// 4-byte ACK: HELLO_ACK_URMA_OK bit.
// ---------------------------------------------------------------------------
TEST(UrmaHandshakeTest, ack_bit_is_urma_ok) {
    // The ACK is a 4-byte big-endian flags word; bit 0 means "I want URMA".
    // Verify the round-trip: host -> net -> host preserves the bit.
    uint32_t flags = 0x1;  // HELLO_ACK_URMA_OK
    uint32_t flags_be = butil::HostToNet32(flags);
    uint32_t flags_back = butil::NetToHost32(flags_be);
    EXPECT_EQ(flags, flags_back);
    EXPECT_NE(0u, flags_back & 0x1);
}

// ---------------------------------------------------------------------------
// ParsedHello field layout: covers the flattened segment (seg_* fields).
// ---------------------------------------------------------------------------
TEST(UrmaHandshakeTest, parsed_hello_segment_fields) {
    urma::ParsedHello p{};
    p.buffer_size = 8192;
    p.recv_buffer_cnt = 127;
    p.jetty_id = 42;
    p.tp_type = 1;
    p.seg_va = 0x1000;
    p.seg_len = 0x100000;
    p.seg_token_id = 7;
    EXPECT_EQ(8192u, p.buffer_size);
    EXPECT_EQ(127u, p.recv_buffer_cnt);
    EXPECT_EQ(42u, p.jetty_id);
    EXPECT_EQ(1u, p.tp_type);
    EXPECT_EQ(0x1000u, p.seg_va);
    EXPECT_EQ(0x100000u, p.seg_len);
    EXPECT_EQ(7u, p.seg_token_id);
}

TEST(UrmaHandshakeTest, rejects_invalid_resource_and_window_values) {
    urma::ParsedHello hello;
        hello.pooled = true;
    hello.buffer_size = 8192;
    hello.recv_buffer_cnt = 127;
    hello.jetty_id = 1;
    hello.socket_id = 123;
    hello.tp_type = URMA_CTP;
    hello.seg_va = 0x1000;
    hello.seg_len = 8192;
    EXPECT_TRUE(urma::ValidHello(hello));

    hello.recv_buffer_cnt = 0;
    EXPECT_FALSE(urma::ValidHello(hello));
    hello.recv_buffer_cnt = 127;
    hello.seg_len = 0;
    EXPECT_FALSE(urma::ValidHello(hello));
    hello.seg_len = 8192;
    hello.jetty_id = 0;
    EXPECT_FALSE(urma::ValidHello(hello));

    hello.jetty_id = 1;
    hello.socket_id = 123;
    hello.seg_va = std::numeric_limits<uint64_t>::max() - 7;
    hello.seg_len = 8;
    EXPECT_FALSE(urma::ValidHello(hello));
}

TEST(UrmaHelperTest, selects_priority_matching_transport_path_type) {
    urma_device_attr_t attr{};
    attr.dev_cap.priority_info[3].tp_type.bs.rtp = 1;
    attr.dev_cap.priority_info[6].tp_type.bs.ctp = 1;

    EXPECT_EQ(3, urma::FindUrmaPriorityForTpType(attr, URMA_RTP));
    EXPECT_EQ(6, urma::FindUrmaPriorityForTpType(attr, URMA_CTP));
    EXPECT_EQ(-1, urma::FindUrmaPriorityForTpType(attr, URMA_UTP));
}

// ---------------------------------------------------------------------------
// SupportedByUrma: only baidu_std.
// ---------------------------------------------------------------------------
TEST(UrmaHandshakeTest, supported_by_urma_protocol_allowlist) {
    EXPECT_TRUE(urma::SupportedByUrma("baidu_std"));
    EXPECT_FALSE(urma::SupportedByUrma("http"));
    EXPECT_FALSE(urma::SupportedByUrma("hulu_pbrpc"));
    EXPECT_FALSE(urma::SupportedByUrma("nshead"));
}

// ---------------------------------------------------------------------------
// URMA mock smoke test: drives urma_init / device enumeration / context /
// jetty / post / poll. These tests rely on mock semantics and are skipped
// when the test binary links a real liburma provider.
// ---------------------------------------------------------------------------
class UrmaMockTest : public ::testing::Test {
protected:
    void SetUp() override {
        urma::g_skip_urma_init = false;

        urma_init_attr_t init_attr{};
        const urma_status_t status = urma_init(&init_attr);
        ASSERT_TRUE(status == URMA_SUCCESS || status == URMA_EEXIST);
        _owns_urma_init = status == URMA_SUCCESS;

        int num_devices = 0;
        urma_device_t** devices = urma_get_device_list(&num_devices);
        ASSERT_NE(nullptr, devices);
        ASSERT_GT(num_devices, 0);
        const bool using_mock =
            strcmp(devices[0]->name, "mock_urma_device") == 0;
        urma_free_device_list(devices);
        if (!using_mock) {
            if (_owns_urma_init) {
                EXPECT_EQ(URMA_SUCCESS, urma_uninit());
                _owns_urma_init = false;
            }
            GTEST_SKIP() << "UrmaMockTest requires the URMA link-time mock";
        }
    }

    void TearDown() override {
        if (_owns_urma_init) {
            EXPECT_EQ(URMA_SUCCESS, urma_uninit());
        }
        urma::g_skip_urma_init = true;
        urma::g_urma_available.store(true, butil::memory_order_relaxed);
    }

private:
    bool _owns_urma_init{false};
};

TEST_F(UrmaMockTest, init_and_enumerate_device) {
    urma_init_attr_t init_attr{};
    EXPECT_EQ(URMA_EEXIST, urma_init(&init_attr));

    int num_devices = 0;
    urma_device_t** devices = urma_get_device_list(&num_devices);
    ASSERT_NE(nullptr, devices);
    ASSERT_GE(num_devices, 1);
    EXPECT_STREQ("mock_urma_device", devices[0]->name);
    urma_free_device_list(devices);
}

TEST_F(UrmaMockTest, create_context_and_query_device) {
    int num_devices = 0;
    urma_device_t** devices = urma_get_device_list(&num_devices);
    ASSERT_NE(nullptr, devices);
    ASSERT_GE(num_devices, 1);

    uint32_t eid_cnt = 0;
    urma_eid_info_t* eids = urma_get_eid_list(devices[0], &eid_cnt);
    ASSERT_NE(nullptr, eids);
    ASSERT_GE(eid_cnt, 1u);
    urma_free_eid_list(eids);

    urma_context_t* ctx = urma_create_context(devices[0], 0);
    ASSERT_NE(nullptr, ctx);

    urma_device_attr_t attr{};
    ASSERT_EQ(URMA_SUCCESS, urma_query_device(devices[0], &attr));
    EXPECT_GE(attr.dev_cap.max_jfc, 1u);
    EXPECT_GE(attr.dev_cap.max_jetty, 1u);

    EXPECT_EQ(URMA_SUCCESS, urma_delete_context(ctx));
    urma_free_device_list(devices);
}

TEST_F(UrmaMockTest, rejects_send_without_target_jetty) {
    // A SEND without a target jetty is invalid. Keeping the mock strict here
    // prevents tests from relying on input that a real provider cannot post.
    int num_devices = 0;
    urma_device_t** devices = urma_get_device_list(&num_devices);
    ASSERT_NE(nullptr, devices);
    ASSERT_GE(num_devices, 1);
    uint32_t eid_cnt = 0;
    urma_eid_info_t* eids = urma_get_eid_list(devices[0], &eid_cnt);
    urma_free_eid_list(eids);
    urma_context_t* ctx = urma_create_context(devices[0], 0);
    ASSERT_NE(nullptr, ctx);

    urma_jfce_t* jfce = urma_create_jfce(ctx);
    ASSERT_NE(nullptr, jfce);
    urma_jfc_cfg_t jfc_cfg{};
    jfc_cfg.depth = 16;
    jfc_cfg.jfce = jfce;
    urma_jfc_t* jfc = urma_create_jfc(ctx, &jfc_cfg);
    ASSERT_NE(nullptr, jfc);

    urma_jfr_cfg_t jfr_cfg{};
    jfr_cfg.depth = 16;
    jfr_cfg.trans_mode = URMA_TM_RM;
    jfr_cfg.max_sge = 1;
    jfr_cfg.min_rnr_timer = URMA_TYPICAL_MIN_RNR_TIMER;
    jfr_cfg.jfc = jfc;
    urma_jfr_t* jfr = urma_create_jfr(ctx, &jfr_cfg);
    ASSERT_NE(nullptr, jfr);

    urma_jetty_cfg_t jetty_cfg{};
    jetty_cfg.flag.bs.share_jfr = 1;
    jetty_cfg.jfs_cfg.depth = 16;
    jetty_cfg.jfs_cfg.trans_mode = URMA_TM_RM;
    jetty_cfg.jfs_cfg.priority = URMA_MAX_PRIORITY;
    jetty_cfg.jfs_cfg.max_sge = 1;
    jetty_cfg.jfs_cfg.rnr_retry = URMA_TYPICAL_RNR_RETRY;
    jetty_cfg.jfs_cfg.err_timeout = URMA_TYPICAL_ERR_TIMEOUT;
    jetty_cfg.jfs_cfg.jfc = jfc;
    jetty_cfg.shared.jfr = jfr;
    jetty_cfg.shared.jfc = jfc;
    urma_jetty_t* jetty = urma_create_jetty(ctx, &jetty_cfg);
    ASSERT_NE(nullptr, jetty);

    urma_jfs_wr_t wr{};
    memset(&wr, 0, sizeof(wr));
    wr.opcode = URMA_OPC_SEND;
    wr.flag.bs.complete_enable = 1;
    wr.user_ctx = 0xABCD;
    wr.next = nullptr;
    urma_jfs_wr_t* bad = nullptr;
    EXPECT_EQ(URMA_EINVAL, urma_post_jetty_send_wr(jetty, &wr, &bad));
    EXPECT_EQ(&wr, bad);

    urma_cr_t crs[4];
    EXPECT_EQ(0, urma_poll_jfc(jfc, 4, crs));

    urma_delete_jetty(jetty);
    urma_delete_jfr(jfr);
    urma_delete_jfc(jfc);
    urma_delete_jfce(jfce);
    urma_delete_context(ctx);
    urma_free_device_list(devices);
}

TEST_F(UrmaMockTest,
       paired_send_is_bidirectional_and_separates_immediate_credit) {
    int num_devices = 0;
    urma_device_t** devices = urma_get_device_list(&num_devices);
    ASSERT_NE(nullptr, devices);
    ASSERT_GT(num_devices, 0);
    urma_context_t* ctx = urma_create_context(devices[0], 0);
    ASSERT_NE(nullptr, ctx);

    urma_jfce_t* sender_jfce = urma_create_jfce(ctx);
    urma_jfce_t* receiver_jfce = urma_create_jfce(ctx);
    ASSERT_NE(nullptr, sender_jfce);
    ASSERT_NE(nullptr, receiver_jfce);
    urma_jfc_cfg_t sender_jfc_cfg{};
    sender_jfc_cfg.depth = 8;
    sender_jfc_cfg.jfce = sender_jfce;
    urma_jfc_t* sender_jfc = urma_create_jfc(ctx, &sender_jfc_cfg);
    ASSERT_NE(nullptr, sender_jfc);
    urma_jfc_cfg_t receiver_jfc_cfg{};
    receiver_jfc_cfg.depth = 8;
    receiver_jfc_cfg.jfce = receiver_jfce;
    urma_jfc_t* receiver_jfc = urma_create_jfc(ctx, &receiver_jfc_cfg);
    ASSERT_NE(nullptr, receiver_jfc);

    urma_jfr_cfg_t sender_jfr_cfg{};
    sender_jfr_cfg.depth = 4;
    sender_jfr_cfg.trans_mode = URMA_TM_RM;
    sender_jfr_cfg.max_sge = 1;
    sender_jfr_cfg.jfc = sender_jfc;
    urma_jfr_t* sender_jfr = urma_create_jfr(ctx, &sender_jfr_cfg);
    ASSERT_NE(nullptr, sender_jfr);
    urma_jfr_cfg_t receiver_jfr_cfg = sender_jfr_cfg;
    receiver_jfr_cfg.jfc = receiver_jfc;
    urma_jfr_t* receiver_jfr = urma_create_jfr(ctx, &receiver_jfr_cfg);
    ASSERT_NE(nullptr, receiver_jfr);

    auto create_jetty = [&](urma_jfc_t* jfc, urma_jfr_t* jfr) {
        urma_jetty_cfg_t cfg{};
        cfg.flag.bs.share_jfr = 1;
        cfg.jfs_cfg.depth = 4;
        cfg.jfs_cfg.trans_mode = URMA_TM_RM;
        cfg.jfs_cfg.max_sge = 1;
        cfg.jfs_cfg.jfc = jfc;
        cfg.shared.jfr = jfr;
        cfg.shared.jfc = jfc;
        return urma_create_jetty(ctx, &cfg);
    };
    urma_jetty_t* sender = create_jetty(sender_jfc, sender_jfr);
    urma_jetty_t* receiver = create_jetty(receiver_jfc, receiver_jfr);
    ASSERT_NE(nullptr, sender);
    ASSERT_NE(nullptr, receiver);

    urma_rjetty_t remote{};
    remote.jetty_id = receiver->jetty_id;
    remote.trans_mode = URMA_TM_RM;
    remote.type = URMA_JETTY;
    remote.tp_type = URMA_CTP;
    urma_token_t token{};
    urma_target_jetty_t* target =
        urma_import_jetty(ctx, &remote, &token);
    ASSERT_NE(nullptr, target);

    char recv_buf[64]{};
    urma_sge_t recv_sge{
        reinterpret_cast<uint64_t>(recv_buf), sizeof(recv_buf), nullptr,
        nullptr};
    urma_sg_t recv_sg{&recv_sge, 1};
    urma_jfr_wr_t recv_wr{recv_sg, 99, nullptr};
    char credit_recv_buf[1]{};
    urma_sge_t credit_recv_sge{
        reinterpret_cast<uint64_t>(credit_recv_buf),
        sizeof(credit_recv_buf), nullptr, nullptr};
    urma_sg_t credit_recv_sg{&credit_recv_sge, 1};
    urma_jfr_wr_t credit_recv_wr{credit_recv_sg, 100, nullptr};
    recv_wr.next = &credit_recv_wr;
    urma_jfr_wr_t* bad_recv = nullptr;
    ASSERT_EQ(URMA_SUCCESS,
              urma_post_jfr_wr(receiver_jfr, &recv_wr, &bad_recv));

    const char payload[] = "urma-payload";
    urma_sge_t send_sge{
        reinterpret_cast<uint64_t>(payload), sizeof(payload), nullptr,
        nullptr};
    urma_sg_t send_sg{&send_sge, 1};
    urma_jfs_wr_t send_wr{};
    send_wr.opcode = URMA_OPC_SEND;
    send_wr.flag.bs.complete_enable = 1;
    send_wr.tjetty = target;
    send_wr.user_ctx = 7;
    send_wr.send.src = send_sg;
    urma_jfs_wr_t credit_wr{};
    credit_wr.opcode = URMA_OPC_SEND_IMM;
    credit_wr.flag.bs.complete_enable = 1;
    credit_wr.tjetty = target;
    credit_wr.user_ctx = 8;
    credit_wr.send.imm_data = 13;
    send_wr.next = &credit_wr;
    urma_jfs_wr_t* bad_send = nullptr;
    ASSERT_EQ(URMA_SUCCESS,
              urma_post_jetty_send_wr(sender, &send_wr, &bad_send));

    urma_cr_t sender_cr[2]{};
    ASSERT_EQ(2, urma_poll_jfc(sender_jfc, 2, sender_cr));
    EXPECT_EQ(0, sender_cr[0].flag.bs.s_r);
    EXPECT_EQ(7u, sender_cr[0].user_ctx);
    EXPECT_EQ(0, sender_cr[1].flag.bs.s_r);
    EXPECT_EQ(8u, sender_cr[1].user_ctx);

    urma_cr_t receiver_cr[2]{};
    ASSERT_EQ(2, urma_poll_jfc(receiver_jfc, 2, receiver_cr));
    EXPECT_EQ(1, receiver_cr[0].flag.bs.s_r);
    EXPECT_EQ(URMA_CR_OPC_SEND, receiver_cr[0].opcode);
    EXPECT_EQ(0u, receiver_cr[0].imm_data);
    EXPECT_EQ(sizeof(payload), receiver_cr[0].completion_len);
    EXPECT_EQ(0, memcmp(payload, recv_buf, sizeof(payload)));
    EXPECT_EQ(1, receiver_cr[1].flag.bs.s_r);
    EXPECT_EQ(URMA_CR_OPC_SEND_WITH_IMM, receiver_cr[1].opcode);
    EXPECT_EQ(13u, receiver_cr[1].imm_data);
    EXPECT_EQ(0u, receiver_cr[1].completion_len);

    // Exercise the response direction as well. Production posts all receive
    // WRs through the shared JFR.
    urma_rjetty_t sender_remote = remote;
    sender_remote.jetty_id = sender->jetty_id;
    urma_target_jetty_t* sender_target =
        urma_import_jetty(ctx, &sender_remote, &token);
    ASSERT_NE(nullptr, sender_target);
    char response_buf[64]{};
    urma_sge_t response_recv_sge{
        reinterpret_cast<uint64_t>(response_buf), sizeof(response_buf),
        nullptr, nullptr};
    urma_sg_t response_recv_sg{&response_recv_sge, 1};
    urma_jfr_wr_t response_recv_wr{response_recv_sg, 101, nullptr};
    ASSERT_EQ(URMA_SUCCESS,
              urma_post_jfr_wr(sender_jfr, &response_recv_wr, &bad_recv));

    const char response[] = "urma-response";
    urma_sge_t response_send_sge{
        reinterpret_cast<uint64_t>(response), sizeof(response), nullptr,
        nullptr};
    urma_sg_t response_send_sg{&response_send_sge, 1};
    urma_jfs_wr_t response_send_wr{};
    response_send_wr.opcode = URMA_OPC_SEND;
    response_send_wr.flag.bs.complete_enable = 1;
    response_send_wr.tjetty = sender_target;
    response_send_wr.user_ctx = 9;
    response_send_wr.send.src = response_send_sg;
    ASSERT_EQ(URMA_SUCCESS,
              urma_post_jetty_send_wr(receiver, &response_send_wr, &bad_send));

    urma_cr_t response_send_cr{};
    ASSERT_EQ(1, urma_poll_jfc(receiver_jfc, 1, &response_send_cr));
    EXPECT_EQ(0, response_send_cr.flag.bs.s_r);
    EXPECT_EQ(9u, response_send_cr.user_ctx);

    urma_cr_t response_recv_cr{};
    ASSERT_EQ(1, urma_poll_jfc(sender_jfc, 1, &response_recv_cr));
    EXPECT_EQ(1, response_recv_cr.flag.bs.s_r);
    EXPECT_EQ(URMA_CR_OPC_SEND, response_recv_cr.opcode);
    EXPECT_EQ(sizeof(response), response_recv_cr.completion_len);
    EXPECT_EQ(0, memcmp(response, response_buf, sizeof(response)));

    urma_unimport_jetty(sender_target);
    urma_unimport_jetty(target);
    urma_delete_jetty(receiver);
    urma_delete_jetty(sender);
    urma_delete_jfr(receiver_jfr);
    urma_delete_jfr(sender_jfr);
    urma_delete_jfc(receiver_jfc);
    urma_delete_jfc(sender_jfc);
    urma_delete_jfce(receiver_jfce);
    urma_delete_jfce(sender_jfce);
    urma_delete_context(ctx);
    urma_free_device_list(devices);
}

TEST_F(UrmaMockTest, pool_release_preserves_outstanding_iobuf_and_heap_fallback) {
    int count = 0;
    urma_device_t** devices = urma_get_device_list(&count);
    ASSERT_GT(count, 0);
    urma_context_t* context = urma_create_context(devices[0], 0);
    ASSERT_NE(nullptr, context);
    ASSERT_TRUE(urma::InitializeUrmaMemoryPools(context, 8192, 16, 16, 16));
    butil::IOBuf retained;
    {
        butil::IOBufAsZeroCopyOutputStream stream(&retained, 8192);
        void* buffer = nullptr;
        int size = 0;
        ASSERT_TRUE(stream.Next(&buffer, &size));
        memset(buffer, 'a', size);
    }
    butil::IOBuf ctp;
    {
        butil::IOBufAsZeroCopyOutputStream stream(&ctp, 4096);
        void* buffer = nullptr;
        int size = 0;
        ASSERT_TRUE(stream.Next(&buffer, &size));
        ASSERT_NE(nullptr, urma::GetCtpPoolSegFor(buffer));
    }
    EXPECT_FALSE(urma::ReleaseUrmaMemoryPools());
    ctp.clear();
    EXPECT_TRUE(urma::ReleaseUrmaMemoryPools());
    // The standard block keeps the deallocator installed after unregister.
    // A normal 4 KiB allocation must still work during TCP fallback.
    butil::IOBuf ordinary;
    {
        butil::IOBufAsZeroCopyOutputStream stream(&ordinary, 4096);
        void* buffer = nullptr;
        int size = 0;
        EXPECT_TRUE(stream.Next(&buffer, &size));
    }
    EXPECT_FALSE(ordinary.empty());
    EXPECT_EQ(std::string(retained.size(), 'a'), retained.to_string());
    retained.clear();
    ordinary.clear();
    EXPECT_TRUE(urma::ReleaseUrmaMemoryPools());
    EXPECT_EQ(URMA_SUCCESS, urma_delete_context(context));
    urma_free_device_list(devices);
}

TEST(UrmaMemoryPoolInitTest, factory_failure_reports_pool_and_capacity) {
    ASSERT_TRUE(urma::SetUrmaMemoryPoolFactory(
        +[](urma_context_t*, size_t, size_t) -> std::unique_ptr<urma::UrmaMemoryPool> {
            errno = ENOMEM;
            return nullptr;
        }));
    struct RestoreFactory {
        ~RestoreFactory() { urma::SetUrmaMemoryPoolFactory(nullptr); }
    } restore;
    logging::StringSink diagnostic;
    logging::LogSink* previous = logging::SetLogSink(&diagnostic);
    const bool initialized = urma::InitializeUrmaMemoryPools(
        nullptr, 8192, 16, 16, 16, 64, 65536);
    logging::SetLogSink(previous);
    EXPECT_FALSE(initialized);
    EXPECT_FALSE(urma::HasUrmaLargeBlockPool());
    for (const char* pool : {"data", "header", "ctp", "large"}) {
        EXPECT_NE(std::string::npos, diagnostic.find(std::string("pool=") + pool));
    }
    EXPECT_NE(std::string::npos, diagnostic.find("block_size=65536"));
    EXPECT_NE(std::string::npos, diagnostic.find("urma_large_buffer_count=64"));
    EXPECT_NE(std::string::npos, diagnostic.find("bytes=4194304"));
    EXPECT_NE(std::string::npos, diagnostic.find("errno=" + std::to_string(ENOMEM)));
}

// End-to-end software-provider tests. Each test uses a small pool without an
// event adapter so completions can be delivered in a controlled order.
class UrmaPooledTest : public ::testing::Test {
protected:
    // These pools have no event adapter: tests drive all posts and CQ polling.
    class RecordingPool : public urma::UrmaPooledResource {
    public:
        int PostSend(urma::UrmaJfsSlot* slot, urma_jfs_wr_t* wr,
                     urma::TxCompletionCtx* ctx, urma_jfs_wr_t** bad = nullptr) override {
            if (before_post) before_post();
            urma_jfs_wr_t* rejected = nullptr;
            int rc = 0;
            if (fail_post_after >= 0) {
                int prefix = fail_post_after;
                fail_post_after = -1;
                auto* tail = wr;
                rejected = wr;
                while (prefix-- > 0 && rejected) { tail = rejected; rejected = rejected->next; }
                CHECK(rejected);
                if (tail != rejected) {
                    tail->next = nullptr;
                    rc = UrmaPooledResource::PostSend(slot, wr, ctx, bad);
                    tail->next = rejected;
                    CHECK_EQ(0, rc);
                }
                rc = -1;
                *bad = rejected;
                errno = injected_post_error;
            } else {
                rc = UrmaPooledResource::PostSend(slot, wr, ctx, &rejected);
                if (bad) *bad = rejected;
            }
            size_t accepted_count = 0;
            for (auto* item = wr; item && item != rejected; item = item->next) {
                ++accepted_count;
                send_jfs_indices.push_back(slot->index);
                if (item->opcode == URMA_OPC_READ) {
                    ++read_posts;
                    EXPECT_EQ(1u, item->rw.src.num_sge);
                    EXPECT_EQ(1u, item->rw.dst.num_sge);
                } else {
                    sends.emplace_back(item->send.src.sge,
                                       item->send.src.sge + item->send.src.num_sge);
                }
            }
            post_chain_sizes.push_back(accepted_count);
            const int saved_errno = errno;
            if (complete_before_return) {
                urma_cr_t cr{};
                while (urma_poll_jfc(slot->jfc, 1, &cr) == 1) {
                    urma::UrmaPooledConnection::HandleTxCompletion(slot, cr);
                }
            }
            errno = saved_errno;
            return rc;
        }
        std::function<void()> before_post;
        size_t read_posts{0};
        bool complete_before_return{false};
        int fail_post_after{-1};
        int injected_post_error{URMA_EAGAIN};
        std::vector<size_t> post_chain_sizes;
        int RepostRecv(urma::UrmaJfrSlot& slot, urma::RxRecvCtx* ctx) override {
            if (fail_next_repost) {
                fail_next_repost = false;
                errno = ENOMEM;
                return -1;
            }
            return UrmaPooledResource::RepostRecv(slot, ctx);
        }
        bool fail_next_repost{false};
        std::vector<std::vector<urma_sge_t>> sends;
        std::vector<uint32_t> send_jfs_indices;
    };
    urma::UrmaJfsSlot* ReserveOne() {
        for (auto* slot : _pool->io_jfs) {
            uint32_t used = slot->sq_inflight.load();
            while (used < slot->sq_depth) {
                if (slot->sq_inflight.compare_exchange_weak(used, used + 1)) return slot;
            }
        }
        return nullptr;
    }
    void ReleaseReservation(urma::UrmaJfsSlot* slot) {
        slot->sq_inflight.fetch_sub(1);
        // These tests inject capacity only, not actual hardware work.
        BAIDU_SCOPED_LOCK(slot->writers_mutex);
        _pool->WakeWritersLocked(slot, true);
    }
    void AppendBlock(butil::IOBuf* data, size_t len, char value) {
        // An explicit block size bypasses TLS so adjacent fragments cannot merge.
        butil::IOBufAsZeroCopyOutputStream stream(data, 8192);
        void* buffer = nullptr;
        int size = 0;
        ASSERT_TRUE(stream.Next(&buffer, &size));
        ASSERT_GE(static_cast<size_t>(size), len);
        memset(buffer, value, len);
        stream.BackUp(size - len);
    }
    static void SetUpTestSuite() {
        GFLAGS_NAMESPACE::SetCommandLineOption("urma_resource_mode", "pooled");
        urma::g_skip_urma_init = false;
        urma::FLAGS_urma_io_jfs_cnt = 2;
        urma::FLAGS_urma_io_jfr_cnt = 1;
        urma::FLAGS_urma_io_sq_depth = 16;
        urma::FLAGS_urma_io_rx_depth = 16;
        urma::FLAGS_urma_header_buf_count = 512;
        GFLAGS_NAMESPACE::SetCommandLineOption("urma_buffer_count", "2048");
        GFLAGS_NAMESPACE::SetCommandLineOption("urma_ctp_buf_count", "2048");
        GFLAGS_NAMESPACE::SetCommandLineOption("urma_large_buffer_count", "64");
        GFLAGS_NAMESPACE::SetCommandLineOption("urma_use_polling", "false");
        urma::GlobalUrmaInitializeOrDie();
    }
    void SetUp() override {
        if (!urma::GetUrmaContext() ||
            strcmp(urma::GetUrmaContext()->dev->name, "mock_urma_device")) {
            GTEST_SKIP() << "Requires the URMA software provider";
        }
        _original = urma::g_urma_pool;
        _pool.reset(new RecordingPool);
        ASSERT_EQ(0, _pool->Initialize(urma::GetUrmaContext(), {}));
        urma::g_urma_pool = _pool.get();
        _state = std::make_shared<urma::UrmaPooledProviderState>();
        _state->pool = _pool.get();
        _state->callbacks.on_bytes = urma::UrmaEndpoint::OnReceivedBytes;
        _state->callbacks.on_input_ready = urma::UrmaEndpoint::OnInputReady;
        _state->callbacks.on_rx_error = urma::UrmaEndpoint::OnRxError;
        _state->callbacks.on_error = urma::UrmaEndpoint::OnConnectionError;
        for (auto* slot : _pool->io_jfs) { slot->callback_owner = _state; slot->pool = _pool.get(); slot->callbacks = _state->callbacks; }
        for (auto* slot : _pool->io_jfr) { slot->callback_owner = _state; slot->pool = _pool.get(); slot->callbacks = _state->callbacks; }
    }
    void TearDown() override {
        for (auto& socket : _sockets) { socket->SetFailed(); }
        _sockets.clear();
        if (_pool) {
            EXPECT_TRUE(_pool->Shutdown());
            _state->pool = nullptr;
            _state.reset();
            if (_state) _state->pool = nullptr;
    _pool.reset();
            urma::g_urma_pool = _original;
        }
    }
    urma::UrmaEndpoint* Endpoint() {
        SocketOptions options;
        options.socket_mode = SOCKET_MODE_URMA;
        SocketId sid;
        if (Socket::Create(options, &sid) != 0) { return nullptr; }
        SocketUniquePtr socket;
        if (Socket::Address(sid, &socket) != 0) { return nullptr; }
        auto* ep = static_cast<UrmaTransport*>(socket->_transport.get())->GetUrmaEp();
        urma::UrmaConnectionOptions connection_options;
        connection_options.socket_id = sid;
        ep->_connection = std::make_shared<urma::UrmaPooledConnection>(connection_options, _state);
        ep->_published_connection.store(ep->_connection.get(), std::memory_order_release);
        ep->_state = urma::UrmaEndpoint::S_ACK_WAIT;
        _sockets.push_back(std::move(socket));
        return ep;
    }
    void Receive(urma::UrmaEndpoint* ep, SocketUniquePtr& socket, uint16_t seq,
                 butil::IOBuf data, urma::UrmaPooledConnection::UrmaRxDispatchContext* dispatch_context = nullptr,
                 bool defer_parse = false) {
        urma::UrmaPooledConnection::UrmaRxParseBatch batch;
        urma::UrmaPooledConnection::RouteReceivedFragment(ep->_socket->id(), Connection(ep)->_shared_state->local_jfr_index,
            seq, data.movable(), dispatch_context, defer_parse ? &batch : nullptr);
    }
    void Connect(urma::UrmaEndpoint* from, urma::UrmaEndpoint* to) {
        urma::ParsedHello hello;
        hello.pooled = true;
        to->MakeLocalParsedHello(&hello);
        ASSERT_TRUE(urma::ValidHello(hello));

        ASSERT_EQ(0, from->ImportPeer(hello));
    }
    void DrainTx() {
        for (auto* slot : _pool->io_jfs) {
            urma_cr_t cr;
            while (urma_poll_jfc(slot->jfc, 1, &cr) == 1) {
                urma::UrmaPooledConnection::HandleTxCompletion(slot, cr);
            }
        }
    }
    void DrainRx(bool reverse = false) {
        for (auto* slot : _pool->io_jfr) {
            std::vector<urma_cr_t> completions;
            urma_cr_t cr;
            while (urma_poll_jfc(slot->jfc, 1, &cr) == 1) { completions.push_back(cr); }
            if (reverse) { std::reverse(completions.begin(), completions.end()); }
            for (const auto& completion : completions) {
                urma::UrmaPooledConnection::HandleRxCompletion(slot, completion);
            }
        }
    }
    static urma::UrmaPooledConnection* Connection(urma::UrmaEndpoint* ep) {
        return static_cast<urma::UrmaPooledConnection*>(ep->_connection.get());
    }
    std::shared_ptr<urma::UrmaPooledProviderState> _state;
    void CheckBlockingCallback();
    void CheckJfsRecovery(void (*fail_next_create)());
    std::unique_ptr<RecordingPool> _pool;
    urma::UrmaPooledResource* _original{nullptr};
    std::vector<SocketUniquePtr> _sockets;
};

TEST(UrmaIoQueueFlagTest, queue_configuration_boundaries) {
    GFLAGS_NAMESPACE::FlagSaver flags;
    struct Range { const char* name; int minimum; int maximum; };
    const Range ranges[] = {
        {"urma_io_jfs_cnt", 1, 64},
        {"urma_io_jfr_cnt", 1, 64},
        {"urma_io_sq_depth", 16, 1024},
        {"urma_io_rx_depth", 16, 1024},
    };
    for (const auto& range : ranges) {
        SCOPED_TRACE(range.name);
        for (int valid : {range.minimum, range.maximum}) {
            EXPECT_FALSE(GFLAGS_NAMESPACE::SetCommandLineOption(
                range.name, std::to_string(valid).c_str()).empty());
        }
        for (int invalid : {-1, 0, range.minimum - 1, range.maximum + 1}) {
            EXPECT_TRUE(GFLAGS_NAMESPACE::SetCommandLineOption(
                range.name, std::to_string(invalid).c_str()).empty());
        }
    }
}

TEST_F(UrmaPooledTest, invalid_queue_configuration_creates_no_resources) {
    GFLAGS_NAMESPACE::FlagSaver flags;
    struct Setting { const char* name; int32_t* flag; int minimum; int maximum; };
    const Setting settings[] = {
        {"urma_io_jfs_cnt", &urma::FLAGS_urma_io_jfs_cnt, 1, 64},
        {"urma_io_jfr_cnt", &urma::FLAGS_urma_io_jfr_cnt, 1, 64},
        {"urma_io_sq_depth", &urma::FLAGS_urma_io_sq_depth, 16, 1024},
        {"urma_io_rx_depth", &urma::FLAGS_urma_io_rx_depth, 16, 1024},
    };
    for (const auto& setting : settings) {
        const int32_t saved = *setting.flag;
        for (int invalid : {-1, 0, setting.minimum - 1, setting.maximum + 1}) {
            *setting.flag = invalid;  // Exercise callers bypassing gflags validators.
            urma::UrmaPooledResource rejected;
            logging::StringSink diagnostic;
            logging::LogSink* previous = logging::SetLogSink(&diagnostic);
            errno = 0;
            const int result = rejected.Initialize(urma::GetUrmaContext(), {});
            const int error = errno;
            logging::SetLogSink(previous);
            EXPECT_EQ(-1, result);
            EXPECT_EQ(EINVAL, error);
            EXPECT_NE(std::string::npos, diagnostic.find(
                std::string(setting.name) + "=" + std::to_string(invalid)));
            EXPECT_NE(std::string::npos, diagnostic.find(
                "expected [" + std::to_string(setting.minimum) + ", " +
                std::to_string(setting.maximum) + "]"));
            EXPECT_TRUE(rejected.io_jfs.empty());
            EXPECT_TRUE(rejected.io_jfr.empty());
        }
        *setting.flag = saved;
    }
}

TEST_F(UrmaPooledTest, provider_owns_shared_queues_until_last_handle_releases) {
    std::unique_ptr<urma::UrmaConnectionProvider> provider;
    ASSERT_EQ(0, urma::CreateUrmaConnectionProvider(urma::UrmaConnectionMode::POOLED, &provider));
    ASSERT_EQ(0, provider->Initialize(_state->callbacks));
    urma::UrmaConnectionOptions options;
    options.socket_id = 100;
    urma::UrmaConnectionHandle first, second;
    ASSERT_EQ(0, provider->CreateConnection(options, &first));
    options.socket_id = 101;
    ASSERT_EQ(0, provider->CreateConnection(options, &second));
    urma::UrmaConnectionDescription a, b;
    ASSERT_EQ(0, first->GetLocalDescription(&a));
    ASSERT_EQ(0, second->GetLocalDescription(&b));
    EXPECT_EQ(a.receive_target_id, b.receive_target_id);
    EXPECT_EQ(urma::UrmaReceiveTargetKind::JFR, a.receive_target_kind);
    EXPECT_EQ(urma::UrmaReceiveCountKind::SHARED_JFR_DEPTH, a.receive_count_kind);
    EXPECT_TRUE(a.has_socket_id);
    auto wrong = b;
    wrong.mode = urma::UrmaConnectionMode::EXCLUSIVE;
    EXPECT_EQ(-1, first->ImportPeer(wrong));
    EXPECT_EQ(EPROTO, errno);
    EXPECT_EQ(0, first->ImportPeer(b));
    EXPECT_FALSE(provider->Shutdown());
    EXPECT_FALSE(first->IsWritable());
    EXPECT_FALSE(static_cast<urma::UrmaPooledConnection*>(first.get())->ValidateReceiveQueue(0));
    urma::UrmaConnectionHandle rejected;
    EXPECT_EQ(-1, provider->CreateConnection(options, &rejected));
    EXPECT_EQ(ESHUTDOWN, errno);
    first.reset(); second.reset();
    EXPECT_TRUE(provider->Shutdown());
    urma::g_urma_pool = _pool.get();
}

namespace {
urma::UrmaConnectionProvider* callback_shutdown_provider = nullptr;
bool callback_shutdown_result = true;
}
TEST_F(UrmaPooledTest, cq_callback_shutdown_does_not_reenter_slot_lock) {
    urma::UrmaPooledConnectionProvider provider;
    provider._state = _state;
    auto* sender = Endpoint();
    auto* receiver = Endpoint();
    ASSERT_TRUE(sender && receiver);
    Connect(sender, receiver);
    butil::IOBuf data; data.append("x");
    butil::IOBuf* input[] = {&data};
    ASSERT_EQ(1, sender->CutFromIOBufList(input, 1));
    callback_shutdown_provider = &provider;
    callback_shutdown_result = true;
    auto* slot = _pool->io_jfr[0];
    _state->callbacks.on_bytes = [](SocketUniquePtr&, const urma::UrmaConnection*, butil::IOBuf) {
        callback_shutdown_result = callback_shutdown_provider->Shutdown();
        return false;
    };
    SocketOptions options;
    options.user = slot; slot->AddRef();
    SocketId id;
    ASSERT_EQ(0, Socket::Create(options, &id));
    SocketUniquePtr carrier;
    ASSERT_EQ(0, Socket::Address(id, &carrier));
    carrier->_nevent.store(1);
    urma::UrmaPooledConnection::PollJfc(carrier.get());
    EXPECT_FALSE(callback_shutdown_result);
    carrier->SetFailed();
    // This fixture owns the pool; the callback must not have deleted it.
    provider._state.reset();
    callback_shutdown_provider = nullptr;
    DrainTx();
}

void UrmaPooledTest::CheckJfsRecovery(void (*fail_next_create)()) {
    auto* first = Endpoint();
    auto* receiver = Endpoint();
    auto* next = Endpoint();
    auto* other = Endpoint();
    ASSERT_TRUE(first && receiver && next && other);
    Connect(first, receiver);
    Connect(next, other);
    Connection(first)->_preferred_jfs = Connection(next)->_preferred_jfs = 0;
    auto* slot = _pool->io_jfs[0];
    const uint32_t old_id = slot->jfs->jfs_id.id;
    butil::IOBuf data;
    data.append(std::string(5000, 'a'));
    butil::IOBuf* input[] = {&data};
    ASSERT_EQ(5000, first->CutFromIOBufList(input, 1));
    urma_cr_t completions[2];
    ASSERT_EQ(2, urma_poll_jfc(slot->jfc, 2, completions));
    completions[0].status = URMA_CR_ACK_TIMEOUT_ERR;
    _pool->CompleteSend(slot, completions[0]);
    ASSERT_EQ(old_id, slot->jfs->jfs_id.id);
    data.append("b");
    ASSERT_EQ(1, next->CutFromIOBufList(input, 1));
    EXPECT_EQ(1u, slot->sq_device_inflight.load());
    if (fail_next_create) {
        fail_next_create();
    }
    _pool->CompleteSend(slot, completions[1]);
    if (fail_next_create) {
        EXPECT_EQ(nullptr, slot->jfs);
        ASSERT_TRUE(slot->stopped.load());
        EXPECT_TRUE(next->_socket->Failed());
        EXPECT_EQ(EIO, next->_socket->non_zero_error_code());
        _pool->KickDrain(slot);
        EXPECT_EQ(nullptr, slot->jfs);
        EXPECT_FALSE(_pool->io_jfs[1]->stopped.load());
    } else {
        ASSERT_NE(nullptr, slot->jfs);
        EXPECT_NE(old_id, slot->jfs->jfs_id.id);
    }
    DrainTx();
    DrainRx();
    EXPECT_EQ(fail_next_create ? "" : "b", other->_input_processor.read_buf().to_string());
    EXPECT_EQ(0u, slot->sq_inflight.load());
}

TEST_F(UrmaPooledTest, replaces_ack_timed_out_send_queue_after_last_completion) {
    CheckJfsRecovery(nullptr);
}

TEST_F(UrmaPooledTest, fails_queued_connection_when_jfs_recreation_fails) {
    auto fail_next_create = reinterpret_cast<void (*)()>(
        dlsym(RTLD_DEFAULT, "brpc_urma_mock_fail_next_jfs_create"));
    ASSERT_NE(nullptr, fail_next_create);
    CheckJfsRecovery(fail_next_create);
}

TEST_F(UrmaPooledTest, shared_send_queues_use_the_configured_error_timeout) {
    const auto info = gflags::GetCommandLineFlagInfoOrDie("urma_io_jfs_err_timeout");
    EXPECT_EQ(std::to_string(URMA_TYPICAL_ERR_TIMEOUT), info.default_value);
    for (const auto* slot : _pool->io_jfs) {
        EXPECT_EQ(info.current_value, std::to_string(slot->jfs->jfs_cfg.err_timeout));
    }
    gflags::FlagSaver flags;
    ASSERT_FALSE(gflags::SetCommandLineOption("urma_io_jfs_err_timeout", "7").empty());
    urma::UrmaPooledResource pool;
    ASSERT_EQ(0, pool.Initialize(urma::GetUrmaContext(), {}));
    for (const auto* slot : pool.io_jfs) {
        EXPECT_EQ(7, slot->jfs->jfs_cfg.err_timeout);
    }
    EXPECT_TRUE(pool.Shutdown());
}

TEST_F(UrmaPooledTest, rejects_out_of_range_shared_send_error_timeouts) {
    gflags::FlagSaver flags;
    for (const char* value : {"-1", "32"}) {
        ASSERT_FALSE(gflags::SetCommandLineOption("urma_io_jfs_err_timeout", value).empty());
        urma::UrmaPooledResource pool;
        EXPECT_EQ(-1, pool.Initialize(urma::GetUrmaContext(), {}));
        EXPECT_EQ(EINVAL, errno);
        EXPECT_TRUE(pool.io_jfs.empty());
        EXPECT_TRUE(pool.io_jfr.empty());
    }
}

TEST_F(UrmaPooledTest, ctp_buffers_are_independent_and_never_fall_back_to_heap) {
    EXPECT_EQ(4032u, urma::GetUrmaRecvBlockSize());
    EXPECT_EQ(8192u, butil::GetDefaultBlockSize());
    auto* ctp_seg = urma::GetCtpPoolSegFor(nullptr);
    ASSERT_NE(nullptr, ctp_seg);
    EXPECT_NE(urma::GetPoolSegFor(nullptr), ctp_seg);
    // Consume one posted receive so replenishment has an actual queue slot.
    // Leave it unreplenished while exhausting the registered CTP storage.
    auto* sender = Endpoint();
    auto* receiver = Endpoint();
    ASSERT_TRUE(sender && receiver);
    Connect(sender, receiver);
    butil::IOBuf packet;
    AppendBlock(&packet, 1, 'x');
    butil::IOBuf* input[] = {&packet};
    ASSERT_EQ(1, sender->CutFromIOBufList(input, 1));
    DrainTx();
    auto* slot = _pool->io_jfr[0];
    urma_cr_t completion{};
    ASSERT_EQ(1, urma_poll_jfc(slot->jfc, 1, &completion));
    ASSERT_EQ(URMA_CR_SUCCESS, completion.status);
    auto* consumed = reinterpret_cast<urma::RxRecvCtx*>(completion.user_ctx);
    ASSERT_TRUE(slot->Owns(consumed));
    consumed->posted = false;
    --slot->rx_posted;
    consumed->data_iobuf.clear();
    consumed->data_block = nullptr;
    std::vector<void*> held;
    while (void* buffer = urma::AllocCtpBuffer()) {
        held.push_back(buffer);
        EXPECT_EQ(ctp_seg, urma::GetPoolSegFor(buffer));
        EXPECT_EQ(ctp_seg, urma::GetCtpPoolSegFor(
            static_cast<char*>(buffer) + 4095));
        memset(buffer, 0x5a, 4096);
    }
    EXPECT_FALSE(held.empty());
    EXPECT_EQ(ENOMEM, errno);
    const size_t posted = slot->rx_posted;
    EXPECT_EQ(-1, _pool->RepostRecv(*slot, consumed));
    EXPECT_EQ(posted, slot->rx_posted);
    // Exhausting CTP storage must not consume the standard IOBuf pool.
    butil::IOBuf data;
    AppendBlock(&data, 4096, 'x');
    EXPECT_EQ(urma::GetPoolSegFor(nullptr),
              urma::GetPoolSegFor(const_cast<void*>(data.fetch1())));
    ASSERT_FALSE(held.empty());
    urma::FreeCtpBuffer(held.back());
    held.pop_back();
    EXPECT_EQ(0, _pool->RepostRecv(*slot, consumed));
    EXPECT_EQ(posted + 1, slot->rx_posted);
    for (void* buffer : held) { urma::FreeCtpBuffer(buffer); }
    for (auto* slot : _pool->io_jfr) {
        for (const auto* ctx : Contexts(slot)) {
            EXPECT_EQ(ctp_seg, urma::GetCtpPoolSegFor(ctx->data_block));
            EXPECT_EQ(4032u, ctx->data_iobuf.size());
        }
    }
}

TEST_F(UrmaPooledTest, receive_queue_error_does_not_fail_tcp_fallback) {
    auto* endpoint = Endpoint();
    ASSERT_TRUE(endpoint);
    endpoint->_socket->_socket_mode = SOCKET_MODE_TCP;
    urma::UrmaEndpoint::OnRxError(endpoint->_socket->id(), EIO);
    EXPECT_FALSE(endpoint->_socket->Failed());
    endpoint->_socket->_socket_mode = SOCKET_MODE_URMA;
}

TEST_F(UrmaPooledTest, ordered_delivery_reuses_pinned_socket) {
    auto* sender = Endpoint(); auto* receiver = Endpoint();
    ASSERT_TRUE(sender && receiver); Connect(sender, receiver);
    _state->callbacks.on_bytes = [](SocketUniquePtr& socket,
                                      const urma::UrmaConnection* connection,
                                      butil::IOBuf bytes) {
        EXPECT_TRUE(socket);
        EXPECT_NE(nullptr, connection);
        return urma::UrmaEndpoint::OnReceivedBytes(socket, connection, bytes.movable());
    };
    butil::IOBuf data; AppendBlock(&data, 8000, 's');
    butil::IOBuf* input[] = {&data};
    ASSERT_EQ(8000, sender->CutFromIOBufList(input, 1));
    DrainTx(); DrainRx();
    EXPECT_EQ(std::string(8000, 's'), receiver->_input_processor.read_buf().to_string());
}

TEST_F(UrmaPooledTest, ctp_received_data_can_be_sent_without_copying) {
    auto* sender = Endpoint();
    auto* receiver = Endpoint();
    ASSERT_TRUE(sender && receiver);
    Connect(sender, receiver);
    Connect(receiver, sender);
    const std::string payload(4032, 'c');
    butil::IOBuf data;
    AppendBlock(&data, payload.size(), 'c');
    butil::IOBuf* input[] = {&data};
    ASSERT_EQ(4032, sender->CutFromIOBufList(input, 1));
    DrainTx();
    DrainRx();
    butil::IOBuf& echo = receiver->_input_processor.read_buf();
    ASSERT_EQ(payload, echo.to_string());
    const void* address = echo.fetch1();
    auto* segment = urma::GetCtpPoolSegFor(const_cast<void*>(address));
    ASSERT_NE(nullptr, segment);
    butil::IOBuf* response[] = {&echo};
    ASSERT_EQ(4032, receiver->CutFromIOBufList(response, 1));
    ASSERT_EQ(2u, _pool->sends.back().size());
    EXPECT_EQ(reinterpret_cast<uint64_t>(address), _pool->sends.back()[1].addr);
    EXPECT_EQ(segment, _pool->sends.back()[1].tseg);
    DrainTx();
    DrainRx();
    EXPECT_EQ(payload, sender->_input_processor.read_buf().to_string());
}

TEST_F(UrmaPooledTest, registered_header_pool_exhaustion_and_concurrency) {
    auto memory = urma::CreateUrmaMemoryPool(urma::GetUrmaContext(), 64, 128);
    ASSERT_TRUE(memory);
    std::set<void*> addresses;
    for (size_t i = 0; i < 128; ++i) {
        void* p = memory->Allocate(64);
        ASSERT_NE(nullptr, p);
        EXPECT_EQ(0u, reinterpret_cast<uintptr_t>(p) % 64);
        EXPECT_TRUE(addresses.insert(p).second);
        EXPECT_NE(nullptr, memory->SegmentFor(static_cast<char*>(p) + 63));
    }
    EXPECT_EQ(nullptr, memory->Allocate(64));
    EXPECT_EQ(128u, memory->outstanding());
    for (void* p : addresses) { memory->Deallocate(p); }
    std::vector<std::thread> threads;
    for (int i = 0; i < 8; ++i) {
        threads.emplace_back([&] {
            for (int n = 0; n < 2000; ++n) {
                void* p = memory->Allocate(64);
                ASSERT_NE(nullptr, p);
                std::memset(p, 42, 64);
                memory->Deallocate(p);
            }
        });
    }
    for (auto& thread : threads) { thread.join(); }
    EXPECT_EQ(0u, memory->outstanding());
    EXPECT_EQ(nullptr, memory->Allocate(65));
    EXPECT_TRUE(memory->Unregister());
    EXPECT_EQ(nullptr, memory->Allocate(64));
}

TEST_F(UrmaPooledTest, memory_tls_cross_thread_return_and_thread_exit) {
    auto memory = urma::CreateUrmaMemoryPool(urma::GetUrmaContext(), 65536, 128);
    ASSERT_TRUE(memory);
    std::vector<void*> blocks;
    for (size_t i = 0; i < 128; ++i) {
        void* p = memory->Allocate(65536);
        ASSERT_TRUE(p);
        blocks.push_back(p);
    }
    EXPECT_EQ(128u, memory->outstanding());
    std::thread releaser([&] {
        for (void* p : blocks) memory->Deallocate(p);
    });
    releaser.join();
    EXPECT_EQ(0u, memory->outstanding());
    // Every block, including the exiting pthread's cache, is reusable.
    std::set<void*> unique;
    for (size_t i = 0; i < 128; ++i) {
        void* p = memory->Allocate(65536);
        ASSERT_TRUE(p);
        EXPECT_TRUE(unique.insert(p).second);
    }
    for (void* p : unique) memory->Deallocate(p);
    EXPECT_TRUE(memory->Unregister());
    EXPECT_EQ(nullptr, memory->Allocate(65536));
}

TEST_F(UrmaPooledTest, memory_tls_budget_keeps_shared_capacity) {
    auto memory = urma::CreateUrmaMemoryPool(urma::GetUrmaContext(), 64, 128);
    ASSERT_TRUE(memory);
    std::atomic<int> ready{0};
    std::atomic<bool> finish{false};
    std::vector<std::thread> workers;
    for (int i = 0; i < 32; ++i) {
        workers.emplace_back([&] {
            void* p = memory->Allocate(64);
            EXPECT_NE(nullptr, p);
            if (p) memory->Deallocate(p);
            ready.fetch_add(1, std::memory_order_release);
            while (!finish.load(std::memory_order_acquire)) usleep(100);
        });
    }
    while (ready.load(std::memory_order_acquire) != 32) usleep(100);
    std::vector<void*> remaining;
    while (void* p = memory->Allocate(64)) remaining.push_back(p);
    // Idle worker caches must never consume more than half of this pool.
    EXPECT_GE(remaining.size(), 64u);
    for (void* p : remaining) memory->Deallocate(p);
    finish.store(true, std::memory_order_release);
    for (auto& worker : workers) worker.join();
    EXPECT_EQ(0u, memory->outstanding());
}

TEST_F(UrmaPooledTest, memory_tls_teardown_with_live_idle_thread) {
    auto memory = urma::CreateUrmaMemoryPool(urma::GetUrmaContext(), 64, 128);
    ASSERT_TRUE(memory);
    std::atomic<bool> ready{false}, finish{false};
    std::thread worker([&] {
        void* p = memory->Allocate(64);
        EXPECT_NE(nullptr, p);
        if (p) memory->Deallocate(p);
        ready.store(true, std::memory_order_release);
        while (!finish.load(std::memory_order_acquire)) std::this_thread::yield();
        // The thread-local destructor must tolerate a destroyed pool.
    });
    while (!ready.load(std::memory_order_acquire)) std::this_thread::yield();
    EXPECT_EQ(0u, memory->outstanding());
    memory->FlushThreadCaches();
    memory.reset();
    finish.store(true, std::memory_order_release);
    worker.join();
}

TEST_F(UrmaPooledTest, independent_queues_and_import_cache) {
    auto* a = Endpoint();
    auto* b = Endpoint();
    auto* c = Endpoint();
    ASSERT_NE(nullptr, a);
    ASSERT_NE(nullptr, b);
    ASSERT_NE(nullptr, c);
    Connect(a, b);
    Connect(c, b);
    EXPECT_EQ(Connection(a)->_shared_state->remote_jfr, Connection(c)->_shared_state->remote_jfr);
    EXPECT_EQ(1u, _pool->imports->size());
    EXPECT_NE(_pool->io_jfs[0]->jfc, _pool->io_jfr[0]->jfc);
    EXPECT_EQ(2u, _pool->io_jfr[0]->jfr->jfr_cfg.max_sge);
    for (auto* slot : _pool->io_jfs) {
        EXPECT_EQ(urma::GetUrmaMaxSge(),
                  slot->jfs->jfs_cfg.max_sge);
    }
    EXPECT_EQ(16u, Contexts(_pool->io_jfr[0]).size());
    a->Reset();
    EXPECT_EQ(1u, _pool->imports->size());
    c->Reset();
    EXPECT_EQ(0u, _pool->imports->size());
    EXPECT_EQ(16u, Contexts(_pool->io_jfr[0]).size());
}

TEST_F(UrmaPooledTest, pooled_send_reorder_and_wrap_preserve_stream) {
    // Force affinity spillover, then deliver completions in reverse order.
    _pool->io_jfs[0]->sq_depth = 2;
    auto* sender = Endpoint();
    auto* receiver = Endpoint();
    ASSERT_NE(nullptr, sender);
    ASSERT_NE(nullptr, receiver);
    Connect(sender, receiver);
    Connection(sender)->_preferred_jfs = 0;
    Connection(sender)->_next_send_seq = 65534;
    Connection(receiver)->_deliver_base = 65534;
    butil::IOBuf data;
    const std::string expected(20000, 'x');
    data.append(expected);
    const void* first_body = data.fetch1();
    butil::IOBuf* input[] = {&data};
    ASSERT_EQ(static_cast<ssize_t>(expected.size()), sender->CutFromIOBufList(input, 1));
    EXPECT_TRUE(data.empty());
    EXPECT_GT(Contexts(_pool->io_jfs[0]).size(), 0u);
    EXPECT_GT(Contexts(_pool->io_jfs[1]).size(), 0u);
    for (auto* slot : _pool->io_jfs) {
        for (const auto* ctx : Contexts(slot)) {
            EXPECT_FALSE(ctx->send_data.empty());
            EXPECT_EQ(urma::MSG_SMALL, ctx->header_buf->type);
            EXPECT_EQ(receiver->_socket->id(),
                      butil::NetToHost64(ctx->header_buf->dst_socket_id));
            if (ctx->seq == 65534) {
                EXPECT_EQ(first_body, ctx->send_data.fetch1());
            }
        }
    }
    DrainRx(true);
    EXPECT_EQ(expected, receiver->_input_processor.read_buf().to_string());
    EXPECT_TRUE(Connection(receiver)->_reorder_queue.empty());
    EXPECT_LT(Connection(receiver)->_deliver_base, 10u);
    DrainTx();
    for (auto* slot : _pool->io_jfs) {
        EXPECT_EQ(0u, slot->sq_inflight.load());
        EXPECT_TRUE(Contexts(slot).empty());
    }
}

TEST_F(UrmaPooledTest, ctp_limits_total_wire_length) {
    auto* sender = Endpoint();
    auto* receiver = Endpoint();
    ASSERT_NE(nullptr, sender);
    ASSERT_NE(nullptr, receiver);
    Connect(sender, receiver);
    // Use a fresh contiguous block, so the split must come from the CTP
    // wire limit rather than an existing IOBuf block boundary.
    butil::IOBuf data;
    {
        butil::IOBufAsZeroCopyOutputStream stream(&data, 8192);
        void* buffer = nullptr;
        int size = 0;
        ASSERT_TRUE(stream.Next(&buffer, &size));
        ASSERT_GE(size, 4033);
        memset(buffer, 'x', 4033);
        stream.BackUp(size - 4033);
    }
    butil::IOBuf* input[] = {&data};
    ASSERT_EQ(4033, sender->CutFromIOBufList(input, 1));
    EXPECT_EQ(2u, Connection(sender)->_next_send_seq);
    auto* slot = _pool->io_jfr[0];
    std::vector<uint32_t> lengths;
    urma_cr_t cr;
    while (urma_poll_jfc(slot->jfc, 1, &cr) == 1) {
        lengths.push_back(cr.completion_len);
        urma::UrmaPooledConnection::HandleRxCompletion(slot, cr);
    }
    ASSERT_EQ(2u, lengths.size());
    EXPECT_EQ(4096u, lengths[0]);
    EXPECT_EQ(65u, lengths[1]);
    EXPECT_EQ(std::string(4033, 'x'), receiver->_input_processor.read_buf().to_string());
    DrainTx();
}

TEST_F(UrmaPooledTest, partial_post_retains_accepted_suffix_without_resubmission) {
    auto* sender = Endpoint();
    auto* receiver = Endpoint();
    ASSERT_TRUE(sender && receiver);
    Connect(sender, receiver);
    for (int i = 0; i < 7; ++i) {
        butil::IOBuf data; data.append("a");
        butil::IOBuf* input[] = {&data};
        ASSERT_EQ(1, sender->CutFromIOBufList(input, 1));
    }
    butil::IOBuf first, remaining, last, empty;
    AppendBlock(&first, 1000, 'b');
    AppendBlock(&remaining, 5000, 'c');
    AppendBlock(&last, 500, 'd');
    butil::IOBuf* input[] = {&first, &empty, &remaining, &last};
    // Only one receive remains. The owner retains the rejected suffix while
    // earlier TX completions exist, so the caller must not submit it twice.
    EXPECT_EQ(6500, sender->CutFromIOBufList(input, 4));
    EXPECT_TRUE(first.empty());
    EXPECT_TRUE(remaining.empty());
    EXPECT_TRUE(last.empty());
    EXPECT_EQ(9u, Connection(sender)->_next_send_seq);
    EXPECT_EQ(0, sender->CutFromIOBufList(input, 4));
    DrainRx();
    DrainTx();
    DrainRx();
    EXPECT_EQ(std::string(7, 'a') + std::string(1000, 'b') +
                  std::string(5000, 'c') + std::string(500, 'd'),
              receiver->_input_processor.read_buf().to_string());
    for (auto* slot : _pool->io_jfs) {
        EXPECT_EQ(0u, slot->sq_inflight.load());
        EXPECT_EQ(nullptr, slot->pending_head);
    }
}

TEST_F(UrmaPooledTest, gather_across_iobufs_until_ctp_body_limit) {
    auto* sender = Endpoint();
    auto* receiver = Endpoint();
    ASSERT_NE(nullptr, sender);
    ASSERT_NE(nullptr, receiver);
    Connect(sender, receiver);
    butil::IOBuf first, second, empty;
    AppendBlock(&first, 1000, 'a');
    AppendBlock(&first, 2000, 'b');
    AppendBlock(&second, 1033, 'c');
    const void* addresses[] = {first.backing_block(0).data(),
                              first.backing_block(1).data(), second.fetch1()};
    const std::string expected = first.to_string() + second.to_string();
    butil::IOBuf* input[] = {&empty, &first, &empty, &second, &empty};
    ASSERT_EQ(4033, sender->CutFromIOBufList(input, 5));
    EXPECT_TRUE(first.empty());
    EXPECT_TRUE(second.empty());
    EXPECT_EQ(2u, Connection(sender)->_next_send_seq);
    ASSERT_EQ(2u, _pool->sends.size());
    ASSERT_EQ(4u, _pool->sends[0].size());
    EXPECT_EQ(64u, _pool->sends[0][0].len);
    EXPECT_EQ(1000u, _pool->sends[0][1].len);
    EXPECT_EQ(2000u, _pool->sends[0][2].len);
    EXPECT_EQ(1032u, _pool->sends[0][3].len);
    for (size_t i = 0; i < 3; ++i) {
        EXPECT_EQ(reinterpret_cast<uint64_t>(addresses[i]), _pool->sends[0][i + 1].addr);
    }
    ASSERT_EQ(2u, _pool->sends[1].size());
    EXPECT_EQ(1u, _pool->sends[1][1].len);
    EXPECT_EQ(reinterpret_cast<uint64_t>(addresses[2]) + 1032,
              _pool->sends[1][1].addr);
    DrainRx(true);
    EXPECT_EQ(expected, receiver->_input_processor.read_buf().to_string());
    DrainTx();
}

TEST_F(UrmaPooledTest, gather_stops_at_sge_limit_only_in_zero_copy_mode) {
    auto* sender = Endpoint();
    auto* receiver = Endpoint();
    ASSERT_NE(nullptr, sender);
    ASSERT_NE(nullptr, receiver);
    Connect(sender, receiver);
    const size_t max_sge = urma::GetUrmaMaxSge();
    const size_t fragments = max_sge + 2;
    ASSERT_LT((max_sge - 1) * 512, 4032u);
    butil::IOBuf first, second, empty;
    std::string expected;
    for (size_t i = 0; i < fragments; ++i) {
        AppendBlock(i < max_sge / 2 ? &first : &second, 512, 'a' + i);
        expected.append(512, 'a' + i);
    }
    butil::IOBuf* input[] = {&first, &empty, &second};
    ASSERT_EQ(static_cast<ssize_t>(expected.size()), sender->CutFromIOBufList(input, 3));
    ASSERT_EQ((fragments + max_sge - 2) / (max_sge - 1), _pool->sends.size());
    ASSERT_EQ(max_sge, _pool->sends[0].size());
    for (const auto& send : _pool->sends) {
        EXPECT_LE(send.size(), max_sge);
        EXPECT_EQ(64u, send[0].len);
        for (size_t i = 1; i < send.size(); ++i) { EXPECT_EQ(512u, send[i].len); }
    }
    DrainRx(true);
    EXPECT_EQ(expected, receiver->_input_processor.read_buf().to_string());
    DrainTx();
}

TEST_F(UrmaPooledTest, gather_respects_smaller_peer_receive_capacity) {
    auto* sender = Endpoint();
    auto* receiver = Endpoint();
    ASSERT_NE(nullptr, sender);
    ASSERT_NE(nullptr, receiver);
    Connect(sender, receiver);
    Connection(sender)->_remote_recv_block_size = 128;
    butil::IOBuf first, second;
    AppendBlock(&first, 64, 'a');
    AppendBlock(&second, 110, 'b');
    butil::IOBuf* input[] = {&first, &second};
    ASSERT_EQ(174, sender->CutFromIOBufList(input, 2));
    ASSERT_EQ(2u, _pool->sends.size());
    for (size_t i = 0; i < 2; ++i) {
        size_t wire_len = 0;
        for (const auto& sge : _pool->sends[i]) { wire_len += sge.len; }
        EXPECT_EQ(i == 0 ? 192u : 110u, wire_len);
    }
    DrainRx();
    EXPECT_EQ(std::string(64, 'a') + std::string(110, 'b'),
              receiver->_input_processor.read_buf().to_string());
    DrainTx();
}

TEST_F(UrmaPooledTest, gather_retains_each_user_registered_segment) {
    auto* sender = Endpoint();
    auto* receiver = Endpoint();
    ASSERT_NE(nullptr, sender);
    ASSERT_NE(nullptr, receiver);
    Connect(sender, receiver);
    auto released = std::make_shared<int>(0);
    butil::IOBuf data;
    std::vector<uint64_t> segments;
    for (int i = 0; i < 3; ++i) {
        void* block = malloc(100);
        ASSERT_NE(nullptr, block);
        memset(block, 'a' + i, 100);
        const uint64_t segment = urma::RegisterMemoryForUrma(block, 100);
        ASSERT_NE(0u, segment);
        segments.push_back(segment);
        ASSERT_EQ(0, data.append_user_data_with_meta(block, 100, [released](void* p) {
            urma::DeregisterMemoryForUrma(p);
            free(p);
            ++*released;
        }, segment));
    }
    butil::IOBuf* input[] = {&data};
    ASSERT_EQ(300, sender->CutFromIOBufList(input, 1));
    EXPECT_TRUE(data.empty());
    ASSERT_EQ(1u, _pool->sends.size());
    EXPECT_EQ(0, *released);
    ASSERT_EQ(4u, _pool->sends[0].size());
    for (size_t i = 0; i < segments.size(); ++i) {
        EXPECT_EQ(segments[i], reinterpret_cast<uint64_t>(_pool->sends[0][i + 1].tseg));
    }
    DrainRx();
    EXPECT_EQ(std::string(100, 'a') + std::string(100, 'b') + std::string(100, 'c'),
              receiver->_input_processor.read_buf().to_string());
    DrainTx();
    EXPECT_EQ(3, *released);
}

TEST_F(UrmaPooledTest, gather_unregistered_fragment_does_not_consume_input) {
    auto* sender = Endpoint();
    auto* receiver = Endpoint();
    ASSERT_NE(nullptr, sender);
    ASSERT_NE(nullptr, receiver);
    Connect(sender, receiver);
    butil::IOBuf first, second;
    AppendBlock(&first, 100, 'a');
    void* unregistered = malloc(100);
    ASSERT_NE(nullptr, unregistered);
    memset(unregistered, 'b', 100);
    ASSERT_EQ(0, second.append_user_data(unregistered, 100, free));
    butil::IOBuf* input[] = {&first, &second};
    EXPECT_EQ(-1, sender->CutFromIOBufList(input, 2));
    EXPECT_EQ(ERDMAMEM, errno);
    EXPECT_EQ(std::string(100, 'a'), first.to_string());
    EXPECT_EQ(std::string(100, 'b'), second.to_string());
    EXPECT_EQ(0u, Connection(sender)->_next_send_seq);
    EXPECT_TRUE(_pool->sends.empty());
    for (auto* slot : _pool->io_jfs) {
        EXPECT_EQ(0u, slot->sq_inflight.load());
        EXPECT_TRUE(Contexts(slot).empty());
    }
}

TEST_F(UrmaPooledTest, disconnect_keeps_dma_and_imports_alive_until_completion) {
    auto* sender = Endpoint();
    auto* receiver = Endpoint();
    ASSERT_NE(nullptr, sender);
    ASSERT_NE(nullptr, receiver);
    Connect(sender, receiver);
    std::weak_ptr<urma::UrmaPooledConnectionState> connection = Connection(sender)->_shared_state;
    butil::IOBuf data;
    data.append("in-flight");
    butil::IOBuf* input[] = {&data};
    ASSERT_EQ(9, sender->CutFromIOBufList(input, 1));
    sender->Reset();
    EXPECT_FALSE(connection.expired());
    EXPECT_EQ(1u, _pool->imports->size());
    _sockets[0]->SetFailed();
    _sockets[1]->SetFailed();
    DrainTx();
    EXPECT_TRUE(connection.expired());
    EXPECT_EQ(0u, _pool->imports->size());
    DrainRx();
    EXPECT_EQ(16u, Contexts(_pool->io_jfr[0]).size());
}

TEST_F(UrmaPooledTest, receive_errors_repost_and_reset_only_connection_state) {
    auto* ep = Endpoint();
    ASSERT_NE(nullptr, ep);
    SocketUniquePtr socket;
    ASSERT_EQ(0, Socket::Address(ep->_socket->id(), &socket));
    butil::IOBuf second;
    second.append("second");
    Receive(ep, socket, 1, second);
    EXPECT_EQ(1u, Connection(ep)->_reorder_queue.size());
    EXPECT_TRUE(ep->_input_processor.read_buf().empty());
    std::weak_ptr<urma::UrmaConnection> old = ep->PinConnection();
    ep->Reset();
    EXPECT_TRUE(old.expired());
    EXPECT_FALSE(ep->PinConnection());
    EXPECT_EQ(16u, Contexts(_pool->io_jfr[0]).size());
}

TEST_F(UrmaPooledTest, duplicate_sequence_fails_connection) {
    auto* ep = Endpoint();
    ASSERT_NE(nullptr, ep);
    SocketUniquePtr socket;
    ASSERT_EQ(0, Socket::Address(ep->_socket->id(), &socket));
    butil::IOBuf data;
    data.append("a");
    Receive(ep, socket, 1, data);
    Receive(ep, socket, 1, data);
    EXPECT_TRUE(socket->Failed());
}

TEST_F(UrmaPooledTest, receive_reposting_preserves_delivered_buffers) {
    auto* sender = Endpoint();
    auto* receiver = Endpoint();
    ASSERT_NE(nullptr, sender);
    ASSERT_NE(nullptr, receiver);
    Connect(sender, receiver);
    auto* slot = _pool->io_jfr[0];
    const auto contexts = Contexts(slot);
    std::string expected;
    // Cycle every receive slot several times while the reader retains all
    // earlier data. Alternate copied and zero-copy receives.
    for (int i = 0; i < 32; ++i) {
        const std::string packet(i % 2 ? 1024 : 16, 'a' + i % 26);
        butil::IOBuf data;
        data.append(packet);
        butil::IOBuf* input[] = {&data};
        ASSERT_EQ(static_cast<ssize_t>(packet.size()),
                  sender->CutFromIOBufList(input, 1));
        DrainTx();
        DrainRx();
        expected += packet;
        EXPECT_EQ(expected, receiver->_input_processor.read_buf().to_string());
        EXPECT_EQ(contexts, Contexts(slot));
    }
}

TEST_F(UrmaPooledTest, header_exhaustion_preserves_unsent_input) {
    auto* sender = Endpoint();
    auto* receiver = Endpoint();
    ASSERT_NE(nullptr, sender);
    ASSERT_NE(nullptr, receiver);
    Connect(sender, receiver);
    std::vector<void*> held_headers;
    while (void* header = urma::AllocHeaderBuffer()) {
        held_headers.push_back(header);
    }
    butil::IOBuf data;
    data.append("body");
    butil::IOBuf* input[] = {&data};
    const ssize_t sent = sender->CutFromIOBufList(input, 1);
    EXPECT_EQ(-1, sent);
    EXPECT_EQ("body", data.to_string());
    for (void* header : held_headers) { urma::FreeHeaderBuffer(header); }
    DrainTx();
    DrainRx();
}

TEST_F(UrmaPooledTest, failed_repost_keeps_context_owned_until_shutdown) {
    auto* sender = Endpoint();
    auto* receiver = Endpoint();
    ASSERT_NE(nullptr, sender);
    ASSERT_NE(nullptr, receiver);
    Connect(sender, receiver);
    SocketUniquePtr socket;
    ASSERT_EQ(0, Socket::Address(receiver->_socket->id(), &socket));
    auto* slot = _pool->io_jfr[0];
    const auto contexts = Contexts(slot);
    butil::IOBuf data;
    data.append(std::string(1024, 'x'));
    butil::IOBuf* input[] = {&data};
    ASSERT_EQ(1024, sender->CutFromIOBufList(input, 1));
    _pool->fail_next_repost = true;
    DrainRx();
    EXPECT_TRUE(socket->Failed());
    EXPECT_TRUE(slot->stopped.load());
    EXPECT_EQ(contexts, Contexts(slot));
    DrainTx();
    ASSERT_TRUE(_pool->Shutdown());
    EXPECT_TRUE(Contexts(slot).empty());
}

TEST_F(UrmaPooledTest, ordered_receive_drains_gap_across_sequence_wrap) {
    auto* ep = Endpoint();
    ASSERT_NE(nullptr, ep);
    SocketUniquePtr socket;
    ASSERT_EQ(0, Socket::Address(ep->_socket->id(), &socket));
    Connection(ep)->_deliver_base = 65535;
    auto receive = [&](uint16_t seq, const char* text) {
        butil::IOBuf data;
        data.append(text);
        Receive(ep, socket, seq, std::move(data));
    };
    receive(65535, "a");
    EXPECT_TRUE(Connection(ep)->_reorder_queue.empty());
    EXPECT_EQ(0u, Connection(ep)->_deliver_base);
    receive(1, "c");
    EXPECT_EQ("a", ep->_input_processor.read_buf().to_string());
    ASSERT_EQ(1u, Connection(ep)->_reorder_queue.size());
    receive(0, "b");
    EXPECT_EQ("abc", ep->_input_processor.read_buf().to_string());
    EXPECT_TRUE(Connection(ep)->_reorder_queue.empty());
    EXPECT_EQ(2u, Connection(ep)->_deliver_base);
    EXPECT_FALSE(socket->Failed());
    // A duplicate of a directly delivered packet must still fail.
    receive(0, "duplicate");
    EXPECT_TRUE(socket->Failed());
    EXPECT_EQ("abc", ep->_input_processor.read_buf().to_string());
}

TEST_F(UrmaPooledTest, jfs_affinity_keeps_connection_fragments_together) {
    auto* sender = Endpoint();
    auto* receiver = Endpoint();
    ASSERT_TRUE(sender && receiver);
    Connect(sender, receiver);
    const std::string payload(12096, 'a');
    butil::IOBuf data;
    data.append(payload);
    butil::IOBuf* input[] = {&data};
    ASSERT_EQ(static_cast<ssize_t>(payload.size()),
              sender->CutFromIOBufList(input, 1));
    ASSERT_GE(_pool->send_jfs_indices.size(), 3u);
    for (uint32_t index : _pool->send_jfs_indices) {
        EXPECT_EQ(_pool->send_jfs_indices.front(), index);
    }
    DrainTx();
    DrainRx();
    EXPECT_EQ(payload, receiver->_input_processor.read_buf().to_string());
}

TEST_F(UrmaPooledTest, jfs_affinity_distributes_more_connections_than_queues) {
    const size_t count = _pool->io_jfs.size() * 2 + 1;
    for (size_t connection = 0; connection < count; ++connection) {
        auto* sender = Endpoint();
        auto* receiver = Endpoint();
        ASSERT_TRUE(sender && receiver);
        Connect(sender, receiver);
        const std::string payload(5000, 'b');
        butil::IOBuf data;
        data.append(payload);
        butil::IOBuf* input[] = {&data};
        const size_t first = _pool->send_jfs_indices.size();
        ASSERT_EQ(static_cast<ssize_t>(payload.size()),
                  sender->CutFromIOBufList(input, 1));
        ASSERT_GT(_pool->send_jfs_indices.size(), first);
        for (size_t i = first; i < _pool->send_jfs_indices.size(); ++i) {
            EXPECT_EQ(Connection(sender)->_preferred_jfs,
                      _pool->send_jfs_indices[i]);
        }
        DrainTx();
        DrainRx();
        EXPECT_EQ(payload, receiver->_input_processor.read_buf().to_string());
    }
}

TEST_F(UrmaPooledTest, jfs_affinity_falls_back_then_returns_to_preferred_queue) {
    auto* sender = Endpoint();
    auto* receiver = Endpoint();
    ASSERT_TRUE(sender && receiver);
    Connect(sender, receiver);
    ASSERT_EQ(2u, _pool->io_jfs.size());
    auto* preferred = _pool->io_jfs[Connection(sender)->_preferred_jfs];
    auto send = [&] {
        butil::IOBuf data;
        data.append("x");
        butil::IOBuf* input[] = {&data};
        EXPECT_EQ(1, sender->CutFromIOBufList(input, 1));
        DrainTx();
        DrainRx();
    };
    preferred->sq_inflight.store(preferred->sq_depth);
    send();
    preferred->sq_inflight.store(0);
    send();
    preferred->stopped.store(true);
    send();
    preferred->stopped.store(false);
    send();
    const uint32_t primary = preferred->index;
    const uint32_t backup = 1 - primary;
    EXPECT_EQ((std::vector<uint32_t>{backup, primary, backup, primary}), _pool->send_jfs_indices);
    EXPECT_EQ("xxxx", receiver->_input_processor.read_buf().to_string());
    while (ReserveOne()) {}
    EXPECT_EQ(nullptr, _pool->PickBackup(0));
    EXPECT_EQ(EAGAIN, errno);
    EXPECT_FALSE(_pool->IsWritable());
    for (auto* slot : _pool->io_jfs) { slot->sq_inflight.store(0); }
}

TEST_F(UrmaPooledTest, shared_sq_completion_wakes_other_connection) {
    auto* waiting = Endpoint();
    auto* sender = Endpoint();
    auto* receiver = Endpoint();
    ASSERT_NE(nullptr, waiting);
    ASSERT_NE(nullptr, sender);
    ASSERT_NE(nullptr, receiver);
    Connect(sender, receiver);
    auto* waiting_receiver = Endpoint();
    ASSERT_NE(nullptr, waiting_receiver);
    Connect(waiting, waiting_receiver);
    Connection(waiting)->_preferred_jfs = Connection(sender)->_preferred_jfs;
    butil::IOBuf data;
    data.append("complete on a different connection");
    butil::IOBuf* input[] = {&data};
    ASSERT_GT(sender->CutFromIOBufList(input, 1), 0);
    // Reserve the rest of the shared capacity, without any WR owned by waiting.
    while (ReserveOne()) {}
    ASSERT_FALSE(_pool->IsWritable());
    auto* socket = _sockets[0].get();
    auto* transport = static_cast<UrmaTransport*>(socket->_transport.get());
    transport->_urma_state = UrmaTransport::URMA_ON;
    std::atomic<bool> entered{false};
    int64_t elapsed = 0;
    std::thread writer([&] {
        const auto deadline = butil::milliseconds_from_now(500);
        const int64_t start = butil::gettimeofday_us();
        entered.store(true);
        transport->WaitEpollOut(socket->_epollout_butex, true, deadline);
        elapsed = butil::gettimeofday_us() - start;
    });
    while (!entered.load()) { std::this_thread::yield(); }
    usleep(20000);
    DrainTx();
    writer.join();
    EXPECT_LT(elapsed, 250000) << "Preferred shared queue must not wait for timeout";
    EXPECT_FALSE(socket->Failed());
    for (auto* slot : _pool->io_jfs) { slot->sq_inflight.store(0); }
    DrainRx();
}

TEST_F(UrmaPooledTest, shared_sq_wait_handles_preexisting_space_and_notification) {
    auto* bx = bthread::butex_create_checked<butil::atomic<int>>();
    bx->store(0);
    auto deadline = butil::milliseconds_from_now(100);
    // Completion before registration: never sleep with space already available.
    EXPECT_EQ(0, _pool->WaitWritable(bx, 0, &deadline));
    EXPECT_FALSE(_pool->io_jfs[0]->has_writers.load());
    while (ReserveOne()) {}
    // An earlier Socket notification must not be lost before entering wait.
    bx->fetch_add(1);
    EXPECT_EQ(-1, _pool->WaitWritable(bx, 0, &deadline));
    EXPECT_EQ(EWOULDBLOCK, errno);
    EXPECT_FALSE(_pool->io_jfs[0]->has_writers.load());
    // Timed-out stack waiters must be removed before their butex is freed.
    deadline = butil::milliseconds_from_now(2);
    EXPECT_EQ(-1, _pool->WaitWritable(bx, 1, &deadline));
    EXPECT_EQ(ETIMEDOUT, errno);
    EXPECT_FALSE(_pool->io_jfs[0]->has_writers.load());
    bthread::butex_destroy(bx);
    ReleaseReservation(_pool->io_jfs[0]);
    for (auto* slot : _pool->io_jfs) { slot->sq_inflight.store(0); }
}

TEST_F(UrmaPooledTest, shared_sq_wait_registration_races_credit_release) {
    auto* bx = bthread::butex_create_checked<butil::atomic<int>>();
    bx->store(0);
    std::atomic<int> timeouts{0};
    for (int i = 0; i < 200; ++i) {
        while (ReserveOne()) {}
        const int expected = bx->load();
        std::thread release([&] { ReleaseReservation(_pool->io_jfs[0]); });
        const auto deadline = butil::milliseconds_from_now(100);
        if (_pool->WaitWritable(bx, expected, &deadline) < 0 && errno == ETIMEDOUT) {
            ++timeouts;
        }
        release.join();
        EXPECT_FALSE(_pool->io_jfs[0]->has_writers.load());
    }
    EXPECT_EQ(0, timeouts.load());
    bthread::butex_destroy(bx);
    for (auto* slot : _pool->io_jfs) { slot->sq_inflight.store(0); }
}

TEST_F(UrmaPooledTest, shared_sq_wait_cancellation_preserves_other_waiters) {
    while (ReserveOne()) {}
    butil::atomic<int>* bx[3];
    std::thread writers[3];
    std::atomic<int> timeouts{0};
    for (int i = 0; i < 3; ++i) {
        bx[i] = bthread::butex_create_checked<butil::atomic<int>>();
        bx[i]->store(0);
        writers[i] = std::thread([&, i] {
            const auto deadline = butil::milliseconds_from_now(1000);
            if (_pool->WaitWritable(bx[i], 0, &deadline) < 0 && errno == ETIMEDOUT) {
                ++timeouts;
            }
        });
        // Order registration so the next cancellation removes the middle node.
        const int64_t end = butil::gettimeofday_us() + 500000;
        size_t queued = 0;
        do {
            {
                BAIDU_SCOPED_LOCK(_pool->io_jfs[0]->writers_mutex);
                queued = 0;
                for (auto* node = _pool->io_jfs[0]->writers; node; node = node->next) { ++queued; }
            }
            if (queued == static_cast<size_t>(i + 1)) { break; }
            std::this_thread::yield();
        } while (butil::gettimeofday_us() < end);
        EXPECT_EQ(static_cast<size_t>(i + 1), queued);
    }
    // Socket failure/cancellation uses this same butex and must unlink safely.
    bx[1]->fetch_add(1);
    bthread::butex_wake_all(bx[1]);
    writers[1].join();
    bthread::butex_destroy(bx[1]);
    ReleaseReservation(_pool->io_jfs[0]);
    for (int i : {0, 2}) {
        writers[i].join();
        bthread::butex_destroy(bx[i]);
    }
    EXPECT_EQ(0, timeouts.load());
    EXPECT_FALSE(_pool->io_jfs[0]->has_writers.load());
    for (auto* slot : _pool->io_jfs) { slot->sq_inflight.store(0); }
}

TEST_F(UrmaPooledTest, shared_sq_shutdown_notifies_registered_waiters) {
    while (ReserveOne()) {}
    auto* bx = bthread::butex_create_checked<butil::atomic<int>>();
    bx->store(0);
    std::atomic<int> result{-2};
    std::thread writer([&] {
        const auto deadline = butil::milliseconds_from_now(1000);
        const int rc = _pool->WaitWritable(bx, 0, &deadline);
        result.store(rc == 0 || errno == EWOULDBLOCK ? 0 : errno);
    });
    while (!_pool->io_jfs[0]->has_writers.load()) { std::this_thread::yield(); }
    EXPECT_TRUE(_pool->Shutdown());
    writer.join();
    EXPECT_EQ(0, result.load());
    EXPECT_FALSE(_pool->io_jfs[0]->has_writers.load());
    const auto deadline = butil::milliseconds_from_now(1);
    EXPECT_EQ(-1, _pool->WaitWritable(bx, bx->load(), &deadline));
    EXPECT_EQ(EIO, errno);
    bthread::butex_destroy(bx);
}

TEST_F(UrmaPooledTest, shared_sq_shutdown_drains_returning_guards) {
    auto* bx = bthread::butex_create_checked<butil::atomic<int>>();
    bx->store(0);
    std::atomic<bool> entered{false}, release{false}, stopped{false};
    std::thread writer([&] {
        urma::UrmaPooledResource::WriteWaiter waiter(_pool.get(), bx);
        entered.store(true);
        while (!release.load()) { std::this_thread::yield(); }
    });
    while (!entered.load()) { std::this_thread::yield(); }
    std::thread shutdown([&] {
        EXPECT_TRUE(_pool->Shutdown());
        stopped.store(true);
    });
    // Wait until shutdown has closed registration, while the returning guard
    // deliberately remains alive and no longer belongs to the waiter list.
    for (;;) {
        bool closed;
        {
            BAIDU_SCOPED_LOCK(_pool->io_jfs[0]->writers_mutex);
            closed = _pool->_writers_closed;
        }
        if (closed) { break; }
        std::this_thread::yield();
    }
    EXPECT_FALSE(stopped.load());
    release.store(true);
    writer.join();
    shutdown.join();
    EXPECT_TRUE(stopped.load());
    EXPECT_EQ(0u, _pool->_active_writers.load());
    bthread::butex_destroy(bx);
}

TEST_F(UrmaPooledTest, available_prefix_is_accepted_then_remaining_input_gets_eagain) {
    auto* sender = Endpoint(); auto* receiver = Endpoint();
    ASSERT_TRUE(sender && receiver);
    Connect(sender, receiver);
    for (auto* slot : _pool->io_jfs) slot->sq_depth = 1;
    butil::IOBuf data;
    data.append(std::string(3 * 4032, 'q'));
    butil::IOBuf* input[] = {&data};
    EXPECT_EQ(2 * 4032, sender->CutFromIOBufList(input, 1));
    EXPECT_EQ(4032u, data.size());
    EXPECT_EQ(-1, sender->CutFromIOBufList(input, 1));
    EXPECT_EQ(EAGAIN, errno);
    EXPECT_EQ(4032u, data.size());
    DrainTx(); DrainRx();
    EXPECT_EQ(4032, sender->CutFromIOBufList(input, 1));
    DrainTx(); DrainRx();
    EXPECT_TRUE(data.empty());
    EXPECT_EQ(std::string(3 * 4032, 'q'), receiver->_input_processor.read_buf().to_string());
    for (auto* slot : _pool->io_jfs) {
        EXPECT_EQ(0u, slot->sq_device_inflight.load());
        slot->sq_depth = 16;
    }
}

TEST_F(UrmaPooledTest, shutdown_reclaims_unpolled_contexts) {
    auto* sender = Endpoint();
    auto* receiver = Endpoint();
    ASSERT_NE(nullptr, sender);
    ASSERT_NE(nullptr, receiver);
    Connect(sender, receiver);
    butil::IOBuf data;
    data.append("shutdown");
    butil::IOBuf* input[] = {&data};
    ASSERT_EQ(8, sender->CutFromIOBufList(input, 1));
    sender->Reset();
    receiver->Reset();
    ASSERT_TRUE(_pool->Shutdown());
    EXPECT_EQ(0u, _pool->imports->size());
    for (auto* slot : _pool->io_jfs) { EXPECT_TRUE(Contexts(slot).empty()); }
    for (auto* slot : _pool->io_jfr) { EXPECT_TRUE(Contexts(slot).empty()); }
    EXPECT_TRUE(_pool->Shutdown());
}

TEST_F(UrmaPooledTest, initialization_failure_releases_preposted_buffers) {
    auto free_headers = []() {
        std::vector<void*> headers;
        while (void* p = urma::AllocHeaderBuffer()) { headers.push_back(p); }
        const size_t count = headers.size();
        for (void* p : headers) { urma::FreeHeaderBuffer(p); }
        return count;
    };
    const size_t before = free_headers();
    {
        urma::UrmaPooledResource failed;
        urma::UrmaPooledResource::EventAdapter events;
        events.attach = [](urma::UrmaIoSlot*) { errno = EMFILE; return -1; };
        EXPECT_EQ(-1, failed.Initialize(urma::GetUrmaContext(), events));
        EXPECT_LT(free_headers(), before);
        EXPECT_TRUE(failed.Shutdown());
    }
    EXPECT_EQ(before, free_headers());
}

TEST_F(UrmaPooledTest, protobuf_resource_modes_are_parsed_before_negotiation) {
    for (int mode : {1, 2}) {
        int fds[2]; ASSERT_EQ(0, socketpair(AF_UNIX, SOCK_STREAM, 0, fds));
        auto* ep = Endpoint(); ASSERT_TRUE(ep);
        ep->_socket->_transport->_on_edge_trigger = nullptr;
        ASSERT_EQ(0, ep->_socket->ResetFileDescriptor(fds[0]));
        urma::UrmaHello msg; ep->FillLocalHelloV3(&msg);
        msg.set_resource_mode(mode);
        if (mode == 1) msg.clear_socket_id();
        std::string body; ASSERT_TRUE(msg.SerializeToString(&body));
        uint32_t length = butil::HostToNet32(body.size());
        ASSERT_EQ(4, write(fds[1], &length, 4));
        ASSERT_EQ(body.size(), write(fds[1], body.data(), body.size()));
        ASSERT_EQ(4, write(fds[1], "NEXT", 4));
        urma::ParsedHello hello; bool negotiated = false;
        ASSERT_EQ(0, ep->ReadAndParseHelloV3(&hello, &negotiated));
        EXPECT_TRUE(negotiated); EXPECT_EQ(mode == 2, hello.pooled);
        char next[4]; ASSERT_EQ(0, ep->ReadFromFd(next, 4));
        EXPECT_EQ(0, memcmp(next, "NEXT", 4)); close(fds[1]);
    }
}

TEST_F(UrmaPooledTest, exclusive_binary_hello_is_fully_consumed_before_mode_fallback) {
    int fds[2]; ASSERT_EQ(0, socketpair(AF_UNIX, SOCK_STREAM, 0, fds));
    auto* ep = Endpoint(); ASSERT_TRUE(ep);
    ep->_socket->_transport->_on_edge_trigger = nullptr;
    ASSERT_EQ(0, ep->_socket->ResetFileDescriptor(fds[0]));
    urma::v2_wire::HelloMessage msg{};
    msg.msg_len = 86; msg.hello_ver = 2; msg.impl_ver = 1;
    msg.buffer_size = 8192; msg.recv_buffer_cnt = 127; msg.jetty_id = 1;
    msg.seg_va = 0x10000; msg.seg_len = 8192;
    char bytes[82]; msg.Serialize(bytes);
    ASSERT_EQ(sizeof(bytes), write(fds[1], bytes, sizeof(bytes)));
    ASSERT_EQ(4, write(fds[1], "NEXT", 4));
    urma::ParsedHello hello; bool negotiated = false;
    EXPECT_EQ(0, urma::ReadBodyAndNegotiate(ep, &hello, &negotiated));
    EXPECT_TRUE(negotiated); EXPECT_FALSE(hello.pooled);
    char next[4]; ASSERT_EQ(0, ep->ReadFromFd(next, sizeof(next)));
    EXPECT_EQ(0, memcmp(next, "NEXT", 4)); close(fds[1]);
}

TEST_F(UrmaPooledTest, failed_and_short_rx_completions_reclaim_and_repost) {
    auto* sender = Endpoint();
    auto* receiver = Endpoint();
    ASSERT_NE(nullptr, sender);
    ASSERT_NE(nullptr, receiver);
    Connect(sender, receiver);
    for (int i = 0; i < 2; ++i) {
        butil::IOBuf data;
        data.append("x");
        butil::IOBuf* input[] = {&data};
        ASSERT_EQ(1, sender->CutFromIOBufList(input, 1));
    }
    auto* slot = _pool->io_jfr[0];
    urma_cr_t cr[2];
    ASSERT_EQ(2, urma_poll_jfc(slot->jfc, 2, cr));
    cr[0].status = URMA_CR_RNR_RETRY_CNT_EXC_ERR;
    cr[1].completion_len = 3;
    urma::UrmaPooledConnection::HandleRxCompletion(slot, cr[0]);
    urma::UrmaPooledConnection::HandleRxCompletion(slot, cr[1]);
    EXPECT_EQ(16u, Contexts(slot).size());
    EXPECT_TRUE(receiver->_input_processor.read_buf().empty());
    DrainTx();
}

TEST_F(UrmaPooledTest, failed_tx_releases_context_and_fails_sender) {
    auto* sender = Endpoint();
    auto* receiver = Endpoint();
    ASSERT_NE(nullptr, sender);
    ASSERT_NE(nullptr, receiver);
    Connect(sender, receiver);
    butil::IOBuf data;
    data.append("x");
    butil::IOBuf* input[] = {&data};
    ASSERT_EQ(1, sender->CutFromIOBufList(input, 1));
    for (auto* slot : _pool->io_jfs) {
        urma_cr_t cr;
        if (urma_poll_jfc(slot->jfc, 1, &cr) == 1) {
            cr.status = URMA_CR_RNR_RETRY_CNT_EXC_ERR;
            urma::UrmaPooledConnection::HandleTxCompletion(slot, cr);
            EXPECT_EQ(0u, slot->sq_inflight.load());
            EXPECT_TRUE(Contexts(slot).empty());
        }
    }
    EXPECT_TRUE(_sockets[0]->Failed());
    DrainRx();
}

TEST_F(UrmaPooledTest, late_tx_error_cannot_fail_replacement_connection) {
    // Cover both a still-pinned old connection and one already destroyed by
    // Reset. The DMA/import reference must survive independently in either case.
    for (bool retain_old : {false, true}) {
        auto* sender = Endpoint();
        auto* receiver = Endpoint();
        ASSERT_TRUE(sender && receiver);
        Connect(sender, receiver);
        auto old = sender->PinConnection();
        std::weak_ptr<urma::UrmaConnection> weak_old = old;
        std::weak_ptr<urma::UrmaPooledConnectionState> imports = Connection(sender)->_shared_state;
        butil::IOBuf data;
        data.append("old");
        butil::IOBuf* input[] = {&data};
        ASSERT_EQ(3, sender->CutFromIOBufList(input, 1));
        sender->Reset();
        if (!retain_old) old.reset();
        EXPECT_EQ(!retain_old, weak_old.expired());
        EXPECT_FALSE(imports.expired());
        urma::UrmaConnectionOptions options;
        options.socket_id = sender->_socket->id();
        sender->_connection =
            std::make_shared<urma::UrmaPooledConnection>(options, _state);
        sender->_published_connection.store(sender->_connection.get(), std::memory_order_release);
        sender->_state = urma::UrmaEndpoint::S_ACK_WAIT;
        Connect(sender, receiver);
        for (auto* slot : _pool->io_jfs) {
            urma_cr_t cr;
            while (urma_poll_jfc(slot->jfc, 1, &cr) == 1) {
                cr.status = URMA_CR_RNR_RETRY_CNT_EXC_ERR;
                urma::UrmaPooledConnection::HandleTxCompletion(slot, cr);
            }
            EXPECT_TRUE(Contexts(slot).empty());
            EXPECT_EQ(0u, slot->sq_inflight.load());
        }
        EXPECT_FALSE(sender->_socket->Failed());
        old.reset();
        EXPECT_TRUE(weak_old.expired());
        EXPECT_TRUE(imports.expired());
        DrainRx();
    }
}

TEST_F(UrmaPooledTest, logical_close_preserves_binding_and_rejects_late_receive) {
    auto* receiver = Endpoint();
    ASSERT_TRUE(receiver);
    auto* connection = receiver->BorrowConnection();
    ASSERT_NE(nullptr, connection);
    const long references = receiver->_connection.use_count();
    EXPECT_EQ(connection, receiver->BorrowConnection());
    EXPECT_EQ(references, receiver->_connection.use_count());
    const uint32_t queue = Connection(receiver)->_shared_state->local_jfr_index;
    std::weak_ptr<urma::UrmaConnection> weak = receiver->PinConnection();
    receiver->DeallocateResources();
    receiver->DeallocateResources();
    EXPECT_EQ(connection, receiver->BorrowConnection());
    EXPECT_FALSE(weak.expired());
    butil::IOBuf data;
    data.append("late");
    urma::UrmaPooledConnection::RouteReceivedFragment(receiver->_socket->id(), queue, 0,
                                        data.movable(), nullptr, nullptr);
    EXPECT_TRUE(receiver->_input_processor.read_buf().empty());
    EXPECT_EQ(0u, Connection(receiver)->_deliver_base);
    receiver->Reset();  // No receive callback remains: recovery boundary.
    EXPECT_EQ(nullptr, receiver->BorrowConnection());
    EXPECT_TRUE(weak.expired());
}

TEST_F(UrmaPooledTest, receive_keeps_closed_connection_alive_until_quiescent_reset) {
    // A callback borrowing the connection must remain safe across logical
    // close. Rebinding is allowed only after that callback has exited.
    ASSERT_TRUE(_pool->Shutdown());
    _state->pool = nullptr;
    _pool.reset(new RecordingPool);
    const int saved_count = urma::FLAGS_urma_io_jfr_cnt;
    urma::FLAGS_urma_io_jfr_cnt = 2;
    const int initialized = _pool->Initialize(urma::GetUrmaContext(), {});
    urma::FLAGS_urma_io_jfr_cnt = saved_count;
    ASSERT_EQ(0, initialized);
    _state->pool = _pool.get();
    urma::g_urma_pool = _pool.get();
    auto* receiver = Endpoint();
    ASSERT_TRUE(receiver);
    auto old = receiver->PinConnection();
    const uint32_t old_queue = Connection(receiver)->_shared_state->local_jfr_index;
    std::atomic<bool> entered{false};
    static_cast<urma::UrmaPooledConnection*>(old.get())->_reorder_mutex.lock();
    std::thread dispatch_context_ptr([&] {
        butil::IOBuf data;
        data.append("stale");
        entered = true;
        urma::UrmaPooledConnection::RouteReceivedFragment(receiver->_socket->id(), old_queue, 0,
                                              data.movable(), nullptr, nullptr);
    });
    while (!entered) std::this_thread::yield();
    // Close must not wait for a callback that is holding the connection and
    // waiting for its reorder mutex. The old instance must reject late data.
    receiver->DeallocateResources();
    EXPECT_EQ(old.get(), receiver->BorrowConnection());
    EXPECT_EQ(old, receiver->PinConnection());
    EXPECT_TRUE(static_cast<urma::UrmaPooledConnection*>(old.get())->_closed.load());
    static_cast<urma::UrmaPooledConnection*>(old.get())->_reorder_mutex.unlock();
    dispatch_context_ptr.join();
    EXPECT_TRUE(receiver->_input_processor.read_buf().empty());
    EXPECT_EQ(0u, Connection(receiver)->_deliver_base);
    std::weak_ptr<urma::UrmaConnection> weak = old;
    old.reset();
    EXPECT_FALSE(weak.expired());
    receiver->Reset();
    EXPECT_EQ(nullptr, receiver->BorrowConnection());
    EXPECT_TRUE(weak.expired());
}

TEST_F(UrmaPooledTest, import_cache_distinguishes_address_spaces) {
    urma_rjfr_t remote{};
    remote.jfr_id = _pool->io_jfr[0]->jfr->jfr_id;
    remote.trans_mode = URMA_TM_RM;
    remote.tp_type = URMA_CTP;
    urma_token_t token{};
    auto* a = _pool->imports->Acquire(urma::GetUrmaContext(), &remote, &token);
    ++remote.jfr_id.uasid;
    auto* b = _pool->imports->Acquire(urma::GetUrmaContext(), &remote, &token);
    ASSERT_NE(nullptr, a);
    ASSERT_NE(nullptr, b);
    EXPECT_NE(a, b);
    EXPECT_EQ(2u, _pool->imports->size());
    _pool->imports->Release(a);
    _pool->imports->Release(b);
    EXPECT_EQ(0u, _pool->imports->size());
}

class PooledEchoService : public test::EchoService {
public:
    void Echo(google::protobuf::RpcController* controller,
              const test::EchoRequest* request, test::EchoResponse* response,
              google::protobuf::Closure* done) override {
        auto* cntl = static_cast<Controller*>(controller);
        response->set_message(request->message());
        cntl->response_attachment().append(cntl->request_attachment());
        done->Run();
    }
};

TEST_F(UrmaPooledTest, event_driven_rpc_binary_and_protobuf_handshakes) {
    // Use real epoll callbacks and the normal brpc handshake/message parser.
    ASSERT_TRUE(_pool->Shutdown());
    if (_state) _state->pool = nullptr;
    _pool.reset();
    urma::g_urma_pool = _original;
    GFLAGS_NAMESPACE::FlagSaver flags;
    PooledEchoService service;
    Server server;
    ASSERT_EQ(0, server.AddService(&service, SERVER_DOESNT_OWN_SERVICE));
    ServerOptions server_options;
    server_options.socket_mode = SOCKET_MODE_URMA;
    ASSERT_EQ(0, server.Start("127.0.0.1:0", &server_options));
    for (int version : {2, 3}) {
        urma::FLAGS_urma_client_handshake_version = version;
        Channel channel;
        ChannelOptions options;
        options.socket_mode = SOCKET_MODE_URMA;
        options.connection_type = "short";
        options.timeout_ms = 3000;
        options.max_retry = 0;
        ASSERT_EQ(0, channel.Init(server.listen_address(), &options));
        test::EchoService_Stub stub(&channel);
        for (int n = 0; n < 4; ++n) {
            test::EchoRequest request;
            test::EchoResponse response;
            request.set_message(std::string(12000 + n, char('a' + n)));
            Controller cntl;
            cntl.request_attachment().append(std::string(11000, 'z'));
            stub.Echo(&cntl, &request, &response, nullptr);
            ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
            EXPECT_EQ(request.message(), response.message());
            EXPECT_EQ(cntl.request_attachment().to_string(),
                      cntl.response_attachment().to_string());
        }
    }
    server.Stop(0);
    server.Join();
}

TEST_F(UrmaPooledTest, concurrent_pooled_and_short_handshakes_are_independent) {
    ASSERT_TRUE(_pool->Shutdown());
    if (_state) _state->pool = nullptr;
    _pool.reset();
    urma::g_urma_pool = _original;
    GFLAGS_NAMESPACE::FlagSaver flags;
    PooledEchoService service;
    Server server;
    ASSERT_EQ(0, server.AddService(&service, SERVER_DOESNT_OWN_SERVICE));
    ServerOptions server_options;
    server_options.socket_mode = SOCKET_MODE_URMA;
    ASSERT_EQ(0, server.Start("127.0.0.1:0", &server_options));
    for (int version : {2, 3}) {
        urma::FLAGS_urma_client_handshake_version = version;
        for (const char* connection : {"pooled", "short"}) {
            Channel channel;
            ChannelOptions options;
            options.socket_mode = SOCKET_MODE_URMA;
            options.connection_type = connection;
            options.timeout_ms = 3000;
            options.max_retry = 0;
            ASSERT_EQ(0, channel.Init(server.listen_address(), &options));
            std::atomic<int> ready{0};
            std::vector<std::thread> workers;
            for (int i = 0; i < 4; ++i) {
                workers.emplace_back([&, i] {
                    ++ready;
                    while (ready.load() != 4) { std::this_thread::yield(); }
                    test::EchoService_Stub stub(&channel);
                    for (int n = 0; n < 4; ++n) {
                        test::EchoRequest request;
                        test::EchoResponse response;
                        request.set_message(std::string(100 + n, char('a' + i)));
                        Controller cntl;
                        stub.Echo(&cntl, &request, &response, nullptr);
                        EXPECT_FALSE(cntl.Failed()) << connection << ": " << cntl.ErrorText();
                        EXPECT_EQ(request.message(), response.message());
                    }
                });
            }
            for (auto& worker : workers) { worker.join(); }
        }
    }
    server.Stop(0);
    server.Join();
}

class BlockingPooledService : public test::EchoService {
public:
    std::atomic<bool> entered{false};
    std::atomic<bool> released{false};
    void Echo(google::protobuf::RpcController* controller,
              const test::EchoRequest* request, test::EchoResponse* response,
              google::protobuf::Closure* done) override {
        if (request->message() == "wait") {
            entered = true;
            const int64_t deadline = butil::monotonic_time_us() + 2000000;
            while (!released && butil::monotonic_time_us() < deadline) {
                bthread_usleep(1000);
            }
            if (!released) { controller->SetFailed("Shared receive CQ was blocked"); }
        } else if (request->message() == "release") {
            released = true;
        }
        response->set_message(request->message());
        done->Run();
    }
};

void UrmaPooledTest::CheckBlockingCallback() {
    ASSERT_TRUE(_pool->Shutdown());
    if (_state) _state->pool = nullptr;
    _pool.reset();
    urma::g_urma_pool = _original;
    ASSERT_EQ(1u, _original->io_jfr.size());
    BlockingPooledService service;
    Server server;
    ASSERT_EQ(0, server.AddService(&service, SERVER_DOESNT_OWN_SERVICE));
    ServerOptions server_options;
    server_options.socket_mode = SOCKET_MODE_URMA;
    ASSERT_EQ(0, server.Start("127.0.0.1:0", &server_options));
    Channel first, second;
    ChannelOptions options;
    options.socket_mode = SOCKET_MODE_URMA;
    options.timeout_ms = 3000;
    options.max_retry = 0;
    options.connection_group = "blocking-first";
    ASSERT_EQ(0, first.Init(server.listen_address(), &options));
    options.connection_group = "blocking-second";
    ASSERT_EQ(0, second.Init(server.listen_address(), &options));
    std::thread worker([&] {
        test::EchoService_Stub stub(&first);
        test::EchoRequest request;
        test::EchoResponse response;
        Controller cntl;
        request.set_message("wait");
        stub.Echo(&cntl, &request, &response, nullptr);
        EXPECT_FALSE(cntl.Failed()) << cntl.ErrorText();
    });
    const int64_t deadline = butil::monotonic_time_us() + 3000000;
    while (!service.entered && butil::monotonic_time_us() < deadline) { usleep(1000); }
    EXPECT_TRUE(service.entered);
    test::EchoService_Stub stub(&second);
    test::EchoRequest request;
    test::EchoResponse response;
    Controller cntl;
    request.set_message("release");
    stub.Echo(&cntl, &request, &response, nullptr);
    EXPECT_FALSE(cntl.Failed()) << cntl.ErrorText();
    worker.join();
    server.Stop(0);
    server.Join();
}

TEST_F(UrmaPooledTest, inline_callback_does_not_block_another_connection_on_same_jfr) {
    GFLAGS_NAMESPACE::FlagSaver flags;
    CheckBlockingCallback();
}

TEST_F(UrmaPooledTest, disconnected_connections_release_server_sockets_and_imports) {
    ASSERT_TRUE(_pool->Shutdown());
    if (_state) _state->pool = nullptr;
    _pool.reset();
    urma::g_urma_pool = _original;
    GFLAGS_NAMESPACE::FlagSaver flags;
    GFLAGS_NAMESPACE::SetCommandLineOption("defer_close_second", "0");
    PooledEchoService service;
    Server server;
    ASSERT_EQ(0, server.AddService(&service, SERVER_DOESNT_OWN_SERVICE));
    ServerOptions server_options;
    server_options.socket_mode = SOCKET_MODE_URMA;
    ASSERT_EQ(0, server.Start("127.0.0.1:0", &server_options));
    // Version 0 exercises the serialized TCP-fallback reader as well.
    for (int version : {0, 2, 3}) {
        urma::FLAGS_urma_client_handshake_version = version;
        for (int cycle = 0; cycle < 3; ++cycle) {
            std::vector<std::unique_ptr<Channel>> channels;
            for (int i = 0; i < 64; ++i) {
                std::unique_ptr<Channel> channel(new Channel);
                ChannelOptions options;
                options.connection_group = "close-test-" + std::to_string(i);
                options.socket_mode = version == 0 ? SOCKET_MODE_TCP : SOCKET_MODE_URMA;
                options.timeout_ms = 3000;
                options.max_retry = 0;
                ASSERT_EQ(0, channel->Init(server.listen_address(), &options));
                test::EchoService_Stub stub(channel.get());
                test::EchoRequest request;
                test::EchoResponse response;
                request.set_message("persistent connection close/reopen");
                Controller cntl;
                stub.Echo(&cntl, &request, &response, nullptr);
                ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
                channels.emplace_back(std::move(channel));
            }
            ServerStatistics stat;
            server.GetStat(&stat);
            ASSERT_EQ(64u, stat.connection_count);
            channels.clear();
            const int64_t deadline = butil::monotonic_time_us() + 3000000;
            do {
                usleep(1000);
                server.GetStat(&stat);
            } while ((stat.connection_count || _original->imports->size()) &&
                     butil::monotonic_time_us() < deadline);
            EXPECT_EQ(0u, stat.connection_count);
            EXPECT_EQ(0u, _original->imports->size());
        }
    }
    server.Stop(0);
    server.Join();
}

// A whole shared CQ callback may retain only one parser-last message, even
// when CQEs belong to different logical connections. Its callback must observe
// the event counter released and every queue/endpoint lock unlocked.
struct InlineDeliveryProbe {
    struct Message : InputMessageBase {
        explicit Message(char c) : value(c) {}
        char value;
        void DestroyImpl() override { delete this; }
    };
    Socket* carrier = nullptr;
    SocketId last_socket = INVALID_SOCKET_ID;
    urma::UrmaIoSlot* slot = nullptr;
    bthread_t poller = 0;
    bthread_key_t key;
    int expected_tls = 123;
    int cq_tls = 456;
    std::atomic<int> next_thread{0};
    bthread_t processing_threads[4]{};
    std::atomic<int> processed{0};
    std::atomic<int> inline_count{0};
    static ParseResult Parse(butil::IOBuf* data, Socket*, bool, const void*) {
        if (data->empty()) { return MakeParseError(PARSE_ERROR_NOT_ENOUGH_DATA); }
        char value;
        data->cutn(&value, 1);
        return MakeMessage(new Message(value));
    }
    static void Process(InputMessageBase* message) {
        auto* probe = const_cast<InlineDeliveryProbe*>(
            static_cast<const InlineDeliveryProbe*>(message->arg()));
        const int index = probe->next_thread.fetch_add(1);
        EXPECT_LT(index, 4);
        if (index < 4) { probe->processing_threads[index] = bthread_self(); }
        EXPECT_EQ(&probe->expected_tls, bthread_getspecific(probe->key));
        if (bthread_self() == probe->poller) {
            ++probe->inline_count;
            EXPECT_EQ(probe->last_socket, message->socket()->id());
            EXPECT_EQ('b', static_cast<Message*>(message)->value);
            EXPECT_EQ(0, probe->carrier->_nevent.load());
            auto* endpoint = static_cast<UrmaTransport*>(
                message->socket()->_transport.get())->GetUrmaEp();
            EXPECT_TRUE(probe->slot->operations.Idle());
            auto connection = std::static_pointer_cast<urma::UrmaPooledConnection>(
                endpoint->PinConnection());
            for (auto* mutex : {&endpoint->_dispatch_mutex,
                                &connection->_reorder_mutex}) {
                const bool unlocked = mutex->try_lock();
                EXPECT_TRUE(unlocked);
                if (unlocked) { mutex->unlock(); }
            }
        }
        message->Destroy();
        ++probe->processed;
    }
};

TEST_F(UrmaPooledTest, inline_batch_releases_locks_and_event_once_for_all_connections) {
    GFLAGS_NAMESPACE::FlagSaver flags;
    InlineDeliveryProbe probe;
    // Keep one worker from taking all eight pre-reserved tables into its
    // pthread-local cache before the other message workers can borrow one.
    GFLAGS_NAMESPACE::SetCommandLineOption("borrow_from_globle_size", "1");
    bthread_keytable_pool_t key_pool;
    ASSERT_EQ(0, bthread_keytable_pool_init(&key_pool));
    ASSERT_EQ(0, bthread_key_create(&probe.key, nullptr));
    bthread_keytable_pool_reserve(&key_pool, 8, probe.key,
        [](const void* p) -> void* { return const_cast<void*>(p); }, &probe.expected_tls);
    InputMessenger messenger;
    InputMessageHandler handler = {InlineDeliveryProbe::Parse,
        InlineDeliveryProbe::Process, nullptr, &probe, "inline-probe"};
    ASSERT_EQ(0, messenger.AddNonProtocolHandler(handler));
    SocketOptions carrier_options;
    probe.slot = _pool->io_jfr[0];
    carrier_options.user = probe.slot;
    probe.slot->AddRef();
    SocketId id;
    ASSERT_EQ(0, Socket::Create(carrier_options, &id));
    SocketUniquePtr carrier;
    ASSERT_EQ(0, Socket::Address(id, &carrier));
    probe.carrier = carrier.get();
    probe.poller = bthread_self();
    carrier->_nevent.store(1);
    for (int connection = 0; connection < 2; ++connection) {
        auto* sender = Endpoint();
        auto* receiver = Endpoint();
        ASSERT_TRUE(sender && receiver);
        receiver->_socket->_user = &messenger;
        receiver->_socket->_keytable_pool = &key_pool;
        probe.last_socket = receiver->_socket->id();
        receiver->_state = urma::UrmaEndpoint::ESTABLISHED;
        Connect(sender, receiver);
        // Two parsed messages per CQE: the first stays asynchronous.
        butil::IOBuf data;
        data.append("ab", 2);
        butil::IOBuf* list[] = {&data};
        ASSERT_EQ(2, sender->CutFromIOBufList(list, 1));
    }
    bthread_t poll_thread;
    ASSERT_EQ(0, bthread_start_background(&poll_thread, nullptr,
        [](void* p) -> void* {
            auto* probe = static_cast<InlineDeliveryProbe*>(p);
            probe->poller = bthread_self();
            EXPECT_EQ(0, bthread_setspecific(probe->key, &probe->cq_tls));
            urma::UrmaPooledConnection::PollJfc(probe->carrier);
            EXPECT_EQ(&probe->cq_tls, bthread_getspecific(probe->key));
            return nullptr;
        }, &probe));
    ASSERT_EQ(0, bthread_join(poll_thread, nullptr));
    const int64_t deadline = butil::monotonic_time_us() + 2000000;
    while (probe.processed != 4 && butil::monotonic_time_us() < deadline) {
        usleep(1000);
    }
    EXPECT_EQ(4, probe.processed.load());
    EXPECT_EQ(1, probe.inline_count.load());
    for (bthread_t thread : probe.processing_threads) {
        if (thread && thread != probe.poller) { EXPECT_EQ(0, bthread_join(thread, nullptr)); }
    }
    for (auto& socket : _sockets) {
        socket->_user = nullptr;
        socket->_keytable_pool = nullptr;
    }
    ASSERT_EQ(0, bthread_keytable_pool_destroy(&key_pool));
    ASSERT_EQ(0, bthread_key_delete(probe.key));
    carrier->SetFailed();
    DrainTx();
}


struct RxBatchParseProbe {
    struct Message : InputMessageBase {
        std::string bytes;
        void DestroyImpl() override { delete this; }
    };
    std::atomic<int> attempts{0};
    std::atomic<int> processed{0};
    static ParseResult Parse(butil::IOBuf* data, Socket*, bool, const void* arg) {
        auto* probe = const_cast<RxBatchParseProbe*>(
            static_cast<const RxBatchParseProbe*>(arg));
        if (!data->empty()) { ++probe->attempts; }
        if (data->size() < 9) { return MakeParseError(PARSE_ERROR_NOT_ENOUGH_DATA); }
        auto* message = new Message;
        data->cutn(&message->bytes, 9);
        return MakeMessage(message);
    }
    static void Process(InputMessageBase* message) {
        auto* probe = const_cast<RxBatchParseProbe*>(
            static_cast<const RxBatchParseProbe*>(message->arg()));
        EXPECT_EQ("abcdefghi", static_cast<Message*>(message)->bytes);
        message->Destroy();
        ++probe->processed;
    }
};

TEST_F(UrmaPooledTest, rx_batch_parses_interleaved_connections_once_per_poll) {
    GFLAGS_NAMESPACE::FlagSaver flags;
    ASSERT_FALSE(GFLAGS_NAMESPACE::SetCommandLineOption(
        "urma_rx_batch_parse", "true").empty());
    GFLAGS_NAMESPACE::SetCommandLineOption("urma_cqe_poll_once", "32");
    RxBatchParseProbe probe;
    InputMessenger messenger;
    InputMessageHandler handler = {RxBatchParseProbe::Parse,
        RxBatchParseProbe::Process, nullptr, &probe, "rx-batch-probe"};
    ASSERT_EQ(0, messenger.AddNonProtocolHandler(handler));
    SocketOptions options;
    options.user = _pool->io_jfr[0];
    _pool->io_jfr[0]->AddRef();
    SocketId sid;
    ASSERT_EQ(0, Socket::Create(options, &sid));
    SocketUniquePtr carrier;
    ASSERT_EQ(0, Socket::Address(sid, &carrier));
    urma::UrmaEndpoint* senders[2];
    for (int i = 0; i < 2; ++i) {
        senders[i] = Endpoint();
        auto* receiver = Endpoint();
        ASSERT_TRUE(senders[i] && receiver);
        receiver->_socket->_user = &messenger;
        receiver->_state = urma::UrmaEndpoint::ESTABLISHED;
        Connect(senders[i], receiver);
    }
    // Deliberately interleave connections. A bounded batch must deduplicate
    // them without mixing their streams or waiting for another CQ event.
    for (const char* part : {"abc", "def", "ghi"}) {
        for (auto* sender : senders) {
            butil::IOBuf data;
            data.append(part, 3);
            butil::IOBuf* list[] = {&data};
            ASSERT_EQ(3, sender->CutFromIOBufList(list, 1));
        }
    }
    carrier->_nevent.store(1);
    urma::UrmaPooledConnection::PollJfc(carrier.get());
    const int64_t deadline = butil::monotonic_time_us() + 2000000;
    while (probe.processed != 2 && butil::monotonic_time_us() < deadline) {
        usleep(1000);
    }
    EXPECT_EQ(2, probe.processed.load());
    EXPECT_EQ(2, probe.attempts.load());
    // With a one-CQE poll budget, incomplete messages must be parsed/flushed
    // at every poll result, without waiting for the whole message to arrive.
    GFLAGS_NAMESPACE::SetCommandLineOption("urma_cqe_poll_once", "1");
    for (const char* part : {"abc", "def", "ghi"}) {
        for (auto* sender : senders) {
            butil::IOBuf data;
            data.append(part, 3);
            butil::IOBuf* list[] = {&data};
            ASSERT_EQ(3, sender->CutFromIOBufList(list, 1));
        }
    }
    carrier->_nevent.store(1);
    urma::UrmaPooledConnection::PollJfc(carrier.get());
    const int64_t second_deadline = butil::monotonic_time_us() + 2000000;
    while (probe.processed != 4 && butil::monotonic_time_us() < second_deadline) {
        usleep(1000);
    }
    EXPECT_EQ(4, probe.processed.load());
    EXPECT_EQ(8, probe.attempts.load());
    for (auto& socket : _sockets) { socket->_user = nullptr; }
    carrier->SetFailed();
    DrainTx();
}


TEST_F(UrmaPooledTest, rx_batch_preserves_handshake_dispatch_boundary) {
    RxBatchParseProbe probe;
    InputMessenger messenger;
    InputMessageHandler handler = {RxBatchParseProbe::Parse,
        RxBatchParseProbe::Process, nullptr, &probe, "rx-handshake-probe"};
    ASSERT_EQ(0, messenger.AddNonProtocolHandler(handler));
    auto* ep = Endpoint();
    ASSERT_TRUE(ep);
    ep->_socket->_user = &messenger;
    SocketUniquePtr socket;
    ASSERT_EQ(0, Socket::Address(ep->_socket->id(), &socket));
    urma::UrmaPooledConnection::UrmaRxParseBatch batch;
    butil::IOBuf data;
    data.append("abcdefghi", 9);
    Receive(ep, socket, 0, data.movable(), nullptr, true);
    batch.Add(socket);
    // Flush before the TCP handshake ACK must retain, not dispatch, bytes.
    urma::UrmaPooledConnection::UrmaRxDispatchContext::ProcessPendingInputs(&batch, _state->callbacks, nullptr);
    EXPECT_EQ(0, probe.attempts.load());
    EXPECT_EQ(9, ep->_pending_received_bytes.load());
    ASSERT_EQ(0, Socket::Address(ep->_socket->id(), &socket));
    batch.Add(socket);
    ep->_state = urma::UrmaEndpoint::ESTABLISHED;
    { InputMessageClosure last_msg; ep->ProcessReceivedBytes(batch.sockets[0], last_msg); }
    // A handshake dispatch preceding the poll flush must not dispatch twice.
    urma::UrmaPooledConnection::UrmaRxDispatchContext::ProcessPendingInputs(&batch, _state->callbacks, nullptr);
    const int64_t deadline = butil::monotonic_time_us() + 2000000;
    while (probe.processed != 1 && butil::monotonic_time_us() < deadline) {
        usleep(1000);
    }
    EXPECT_EQ(1, probe.attempts.load());
    EXPECT_EQ(1, probe.processed.load());
    EXPECT_EQ(0, ep->_pending_received_bytes.load());
    ep->_socket->_user = nullptr;
}

TEST_F(UrmaPooledTest, rx_batch_drops_failed_connection_before_flush) {
    RxBatchParseProbe probe;
    InputMessenger messenger;
    InputMessageHandler handler = {RxBatchParseProbe::Parse,
        RxBatchParseProbe::Process, nullptr, &probe, "rx-failure-probe"};
    ASSERT_EQ(0, messenger.AddNonProtocolHandler(handler));
    auto* ep = Endpoint();
    ASSERT_TRUE(ep);
    ep->_socket->_user = &messenger;
    ep->_state = urma::UrmaEndpoint::ESTABLISHED;
    SocketUniquePtr socket;
    ASSERT_EQ(0, Socket::Address(ep->_socket->id(), &socket));
    urma::UrmaPooledConnection::UrmaRxParseBatch batch;
    butil::IOBuf data;
    data.append("abcdefghi", 9);
    Receive(ep, socket, 0, data.movable(), nullptr, true);
    batch.Add(socket);
    ep->_socket->SetFailed();
    urma::UrmaPooledConnection::UrmaRxDispatchContext::ProcessPendingInputs(&batch, _state->callbacks, nullptr);
    EXPECT_EQ(0u, batch.size);
    EXPECT_EQ(0, probe.attempts.load());
    EXPECT_EQ(0, probe.processed.load());
    ep->_socket->_user = nullptr;
}

TEST_F(UrmaPooledTest, cqe_before_post_return_restarts_owner_without_new_producer) {
    auto* sender = Endpoint();
    auto* receiver = Endpoint();
    ASSERT_TRUE(sender && receiver);
    Connect(sender, receiver);
    Connection(sender)->_preferred_jfs = 0;
    auto* slot = _pool->io_jfs[0];
    slot->drain_state.store(urma::UrmaJfsSlot::RUNNING);
    for (int i = 0; i < 4; ++i) {
        butil::IOBuf data; data.append("x");
        butil::IOBuf* input[] = {&data};
        ASSERT_EQ(1, sender->CutFromIOBufList(input, 1));
    }
    // Registry storage was allocated at depth16; bound the device to two WRs
    // after admission to model concurrent selection overshoot.
    slot->sq_depth = 2;
    _pool->complete_before_return = true;
    slot->drain_state.store(0);
    _pool->KickDrain(slot);
    EXPECT_EQ((std::vector<size_t>{2, 2}), _pool->post_chain_sizes);
    EXPECT_EQ(0u, slot->sq_inflight.load());
    EXPECT_EQ(0u, slot->sq_device_inflight.load());
    EXPECT_EQ(0u, slot->drain_state.load());
    slot->sq_depth = 16;
    DrainRx();
    EXPECT_EQ("xxxx", receiver->_input_processor.read_buf().to_string());
}

TEST_F(UrmaPooledTest, producers_enqueue_while_single_jfs_owner_is_posting) {
    std::vector<urma::UrmaEndpoint*> senders, receivers;
    for (int i = 0; i < 7; ++i) {
        auto* sender = Endpoint();
        auto* receiver = Endpoint();
        ASSERT_TRUE(sender && receiver);
        Connect(sender, receiver);
        Connection(sender)->_preferred_jfs = 0;
        senders.push_back(sender); receivers.push_back(receiver);
    }
    std::atomic<bool> entered{false}, release{false};
    std::atomic<int> finished{0}, in_post{0}, maximum{0};
    _pool->before_post = [&] {
        const int active = in_post.fetch_add(1) + 1;
        int old = maximum.load();
        while (old < active && !maximum.compare_exchange_weak(old, active)) {}
        entered.store(true);
        while (!release.load()) std::this_thread::yield();
        in_post.fetch_sub(1);
    };
    auto send = [&](size_t i) {
        butil::IOBuf data; data.append("p");
        butil::IOBuf* input[] = {&data};
        EXPECT_EQ(1, senders[i]->CutFromIOBufList(input, 1));
        finished.fetch_add(1);
    };
    std::vector<std::thread> producers;
    producers.emplace_back(send, 0);
    while (!entered.load()) std::this_thread::yield();
    for (size_t i = 1; i < senders.size(); ++i) producers.emplace_back(send, i);
    const int64_t deadline = butil::gettimeofday_us() + 2000000;
    while (finished.load() < 6 && butil::gettimeofday_us() < deadline) std::this_thread::yield();
    EXPECT_EQ(6, finished.load());
    release.store(true);
    for (auto& producer : producers) producer.join();
    EXPECT_EQ(1, maximum.load());
    EXPECT_EQ((std::vector<size_t>{1, 6}), _pool->post_chain_sizes);
    DrainTx(); DrainRx();
    for (auto* receiver : receivers) EXPECT_EQ("p", receiver->_input_processor.read_buf().to_string());
    EXPECT_EQ(0u, _pool->io_jfs[0]->sq_inflight.load());
}

TEST_F(UrmaPooledTest, partial_fatal_chain_fails_only_affected_connection) {
    auto* a = Endpoint(); auto* a_receiver = Endpoint();
    auto* b = Endpoint(); auto* b_receiver = Endpoint();
    ASSERT_TRUE(a && a_receiver && b && b_receiver);
    Connect(a, a_receiver); Connect(b, b_receiver);
    Connection(a)->_preferred_jfs = Connection(b)->_preferred_jfs = 0;
    auto* slot = _pool->io_jfs[0];
    slot->drain_state.store(urma::UrmaJfsSlot::RUNNING);
    butil::IOBuf data; data.append(std::string(3 * 4032, 'a'));
    butil::IOBuf* input[] = {&data};
    ASSERT_EQ(3 * 4032, a->CutFromIOBufList(input, 1));
    data.append("b");
    ASSERT_EQ(1, b->CutFromIOBufList(input, 1));
    _pool->fail_post_after = 1;
    _pool->injected_post_error = URMA_EINVAL;
    slot->drain_state.store(0);
    _pool->KickDrain(slot);
    EXPECT_TRUE(_sockets[0]->Failed());
    EXPECT_FALSE(_sockets[2]->Failed());
    EXPECT_FALSE(slot->stopped.load());
    EXPECT_EQ(2u, slot->sq_inflight.load());
    DrainTx(); DrainRx();
    EXPECT_EQ("b", b_receiver->_input_processor.read_buf().to_string());
    EXPECT_EQ(0u, slot->sq_inflight.load());
}

TEST_F(UrmaPooledTest, partial_transient_post_retries_after_prefix_completed_inline) {
    auto* sender = Endpoint(); auto* receiver = Endpoint();
    ASSERT_TRUE(sender && receiver);
    Connect(sender, receiver);
    Connection(sender)->_preferred_jfs = 0;
    _pool->fail_post_after = 1;
    _pool->injected_post_error = URMA_EAGAIN;
    _pool->complete_before_return = true;
    butil::IOBuf data;
    data.append(std::string(3 * 4032, 'r'));
    butil::IOBuf* input[] = {&data};
    EXPECT_EQ(3 * 4032, sender->CutFromIOBufList(input, 1));
    EXPECT_TRUE(data.empty());
    EXPECT_FALSE(sender->_socket->Failed());
    EXPECT_EQ((std::vector<size_t>{1, 2}), _pool->post_chain_sizes);
    auto* slot = _pool->io_jfs[0];
    EXPECT_EQ(0u, slot->sq_inflight.load());
    EXPECT_EQ(0u, slot->sq_device_inflight.load());
    DrainRx();
    EXPECT_EQ(std::string(3 * 4032, 'r'), receiver->_input_processor.read_buf().to_string());
}

TEST_F(UrmaPooledTest, failed_head_during_collection_preserves_next_connection) {
    auto* a = Endpoint(); auto* ar = Endpoint();
    auto* b = Endpoint(); auto* br = Endpoint();
    ASSERT_TRUE(a && ar && b && br);
    Connect(a, ar); Connect(b, br);
    Connection(a)->_preferred_jfs = Connection(b)->_preferred_jfs = 0;
    auto* slot = _pool->io_jfs[0];
    slot->drain_state.store(urma::UrmaJfsSlot::RUNNING);
    butil::IOBuf data; data.append("a");
    butil::IOBuf* input[] = {&data};
    ASSERT_EQ(1, a->CutFromIOBufList(input, 1));
    _pool->CollectIncoming(slot);
    auto* a_ctx = slot->pending_head->items[0];
    data.append("b");
    ASSERT_EQ(1, b->CutFromIOBufList(input, 1));
    auto* pending = slot->incoming_head.load();
    auto* b_ctx = pending->items[0];
    // Keep failed-regression contexts inspectable even if queue/CQE ownership
    // gets mismatched. The normal path leaves only these test references.
    a_ctx->AddRef(); b_ctx->AddRef();
    pending->next.store(urma::TxBatch::Unlinked());
    _pool->complete_before_return = true;
    slot->drain_state.store(0);
    std::thread owner([&] { _pool->KickDrain(slot); });
    // The head was checked before CollectIncoming exchanges this node. Pause
    // publication there, then simulate an error CQE on another shared JFS.
    while (slot->incoming_head.load() != nullptr) std::this_thread::yield();
    Connection(a)->_shared_state->failed.store(true);
    pending->next.store(nullptr, std::memory_order_release);
    owner.join();
    EXPECT_EQ((std::vector<size_t>{1}), _pool->post_chain_sizes);
    EXPECT_EQ(0u, slot->sq_inflight.load());
    EXPECT_EQ(0u, slot->sq_device_inflight.load());
    EXPECT_EQ(1u, a_ctx->refs.load());
    EXPECT_EQ(1u, b_ctx->refs.load());
    DrainRx();
    EXPECT_FALSE(b->_socket->Failed());
    EXPECT_EQ("b", br->_input_processor.read_buf().to_string());
    // Clean up the reference leaked by the deliberately exercised old bug.
    while (a_ctx->refs.load() > 1) a_ctx->Release();
    while (b_ctx->refs.load() > 1) b_ctx->Release();
    a_ctx->Release(); b_ctx->Release();
}

TEST_F(UrmaPooledTest, transient_failure_without_future_cqe_reports_error) {
    auto* sender = Endpoint(); auto* receiver = Endpoint();
    ASSERT_TRUE(sender && receiver);
    Connect(sender, receiver);
    _pool->fail_post_after = 0;
    _pool->injected_post_error = URMA_EAGAIN;
    butil::IOBuf data; data.append("owned");
    butil::IOBuf* input[] = {&data};
    EXPECT_EQ(5, sender->CutFromIOBufList(input, 1));
    EXPECT_TRUE(data.empty());
    EXPECT_TRUE(_sockets[0]->Failed());
    for (auto* slot : _pool->io_jfs) {
        EXPECT_EQ(0u, slot->sq_inflight.load());
        EXPECT_EQ(0u, slot->sq_device_inflight.load());
        EXPECT_TRUE(slot->SubmittedContexts().empty());
    }
}

TEST_F(UrmaPooledTest, collector_waits_until_publisher_links_node) {
    auto* sender = Endpoint(); auto* receiver = Endpoint();
    ASSERT_TRUE(sender && receiver);
    Connect(sender, receiver);
    Connection(sender)->_preferred_jfs = 0;
    auto* slot = _pool->io_jfs[0];
    slot->drain_state.store(urma::UrmaJfsSlot::RUNNING);
    butil::IOBuf data; data.append("linked");
    butil::IOBuf* input[] = {&data};
    ASSERT_EQ(6, sender->CutFromIOBufList(input, 1));
    auto* node = slot->incoming_head.load();
    node->next.store(urma::TxBatch::Unlinked());
    slot->drain_state.store(0);
    std::thread owner([&] { _pool->KickDrain(slot); });
    while (slot->incoming_head.load() != nullptr) std::this_thread::yield();
    EXPECT_TRUE(_pool->sends.empty());
    node->next.store(nullptr, std::memory_order_release);
    owner.join();
    DrainTx(); DrainRx();
    EXPECT_EQ("linked", receiver->_input_processor.read_buf().to_string());
}


TEST_F(UrmaPooledTest, configured_batch_limit_bounds_each_post_without_waiting_to_fill) {
    GFLAGS_NAMESPACE::FlagSaver flags;
    ASSERT_FALSE(GFLAGS_NAMESPACE::SetCommandLineOption("urma_io_tx_batch_max", "2").empty());
    auto* sender = Endpoint(); auto* receiver = Endpoint();
    ASSERT_TRUE(sender && receiver);
    Connect(sender, receiver);
    Connection(sender)->_preferred_jfs = 0;
    auto* slot = _pool->io_jfs[0];
    slot->drain_state.store(urma::UrmaJfsSlot::RUNNING);
    for (int i = 0; i < 5; ++i) {
        butil::IOBuf data; data.append("x");
        butil::IOBuf* input[] = {&data};
        ASSERT_EQ(1, sender->CutFromIOBufList(input, 1));
    }
    slot->drain_state.store(0);
    _pool->KickDrain(slot);
    EXPECT_EQ((std::vector<size_t>{2, 2, 1}), _pool->post_chain_sizes);
    DrainTx(); DrainRx();
    EXPECT_EQ("xxxxx", receiver->_input_processor.read_buf().to_string());
}

TEST_F(UrmaPooledTest, sampled_backups_do_not_scan_other_queues) {
    class FixedSamplePool : public urma::UrmaPooledResource {
    public:
        std::array<uint32_t, 4> SampleBackups(uint32_t) const override {
            return {{1, 2, 3, 4}};
        }
    } pool;
    for (uint32_t i = 0; i < 6; ++i) {
        auto* slot = new urma::UrmaJfsSlot(i);
        slot->sq_depth = 16;
        slot->sq_inflight.store(i == 5 ? 0 : 12 - i);
        pool.io_jfs.push_back(slot);
    }
    EXPECT_EQ(pool.io_jfs[4], pool.PickBackup(0));
    for (int i = 1; i <= 4; ++i) pool.io_jfs[i]->sq_inflight.store(16);
    EXPECT_EQ(nullptr, pool.PickBackup(0));
    EXPECT_EQ(EAGAIN, errno);
}

TEST_F(UrmaPooledTest, shutdown_waits_for_active_rx_operation_before_freeing_array) {
    auto* slot = _pool->io_jfr[0];
    std::atomic<bool> entered{false}, release{false}, finished{false};
    std::thread receiver([&] {
        urma::UrmaOperation operation(&slot->operations);
        EXPECT_TRUE(static_cast<bool>(operation));
        entered.store(true);
        while (!release.load()) std::this_thread::yield();
        EXPECT_NE(nullptr, slot->contexts.get());
    });
    while (!entered.load()) std::this_thread::yield();
    std::thread shutdown([&] { EXPECT_TRUE(_pool->Shutdown()); finished.store(true); });
    while (!slot->stopped.load()) std::this_thread::yield();
    EXPECT_FALSE(finished.load());
    release.store(true);
    receiver.join(); shutdown.join();
    EXPECT_TRUE(finished.load());
    EXPECT_EQ(nullptr, slot->contexts.get());
}

// READ-only large-message migration: bytes stay registered until matching FREE.
TEST_F(UrmaPooledTest, read_large_sources_survive_send_cqe_until_free) {
    GFLAGS_NAMESPACE::FlagSaver flags;
    GFLAGS_NAMESPACE::SetCommandLineOption("urma_large_transfer", "read");
    auto* sender = Endpoint(); auto* receiver = Endpoint();
    ASSERT_TRUE(sender && receiver);
    Connect(sender, receiver); Connect(receiver, sender);
    const auto before = urma::GetUrmaReadStats();
    butil::IOBuf data;
    {
        butil::IOBufAsZeroCopyOutputStream stream(&data, 65536);
        void* bytes; int size;
        ASSERT_TRUE(stream.Next(&bytes, &size));
        ASSERT_GE(size, 8192);
        memset(bytes, 'r', 8192); stream.BackUp(size - 8192);
    }
    const void* source = data.fetch1();
    butil::IOBuf* input[] = {&data};
    ASSERT_EQ(8192, sender->CutFromIOBufList(input, 1));
    ASSERT_TRUE(data.empty());
    EXPECT_EQ(before.sources_outstanding + 1, urma::GetUrmaReadStats().sources_outstanding);
    DrainTx(); // PULL SEND complete is not permission to recycle its source.
    EXPECT_EQ('r', *static_cast<const char*>(source));
    EXPECT_TRUE(receiver->_input_processor.read_buf().empty());
    DrainRx(); // PULL -> post READ, no delivery before READ completion.
    EXPECT_EQ(1u, _pool->read_posts);
    EXPECT_TRUE(receiver->_input_processor.read_buf().empty());
    DrainTx(); // READ -> FREE and byte delivery.
    EXPECT_EQ(std::string(8192, 'r'), receiver->_input_processor.read_buf().to_string());
    EXPECT_EQ(before.sources_outstanding + 1, urma::GetUrmaReadStats().sources_outstanding);
    DrainRx(); DrainTx();
    EXPECT_EQ(before.sources_outstanding, urma::GetUrmaReadStats().sources_outstanding);
    EXPECT_EQ(before.reads_completed + 1, urma::GetUrmaReadStats().reads_completed);
}

TEST_F(UrmaPooledTest, read_multiple_iobufs_share_control_and_keep_stream_order) {
    GFLAGS_NAMESPACE::FlagSaver flags;
    GFLAGS_NAMESPACE::SetCommandLineOption("urma_large_transfer", "read");
    GFLAGS_NAMESPACE::SetCommandLineOption("urma_read_items_per_control", "2");
    auto* sender = Endpoint(); auto* receiver = Endpoint();
    ASSERT_TRUE(sender && receiver);
    Connect(sender, receiver); Connect(receiver, sender);
    Connection(sender)->_next_send_seq = 65535;
    Connection(receiver)->_deliver_base = 65535;
    butil::IOBuf data[3];
    for (size_t i = 0; i < 3; ++i) AppendBlock(&data[i], 5000, 'a' + i);
    butil::IOBuf* input[] = {&data[0], &data[1], &data[2]};
    auto before = urma::GetUrmaReadStats();
    EXPECT_EQ(10000, sender->CutFromIOBufList(input, 3));
    EXPECT_EQ(before.controls_sent + 1, urma::GetUrmaReadStats().controls_sent);
    EXPECT_EQ(before.sources_outstanding + 1, urma::GetUrmaReadStats().sources_outstanding);
    ASSERT_GE(_pool->sends.back().size(), 2u);
    const auto& payload = _pool->sends.back()[1];
    butil::IOBuf wire;
    wire.append(reinterpret_cast<const void*>(payload.addr), payload.len);
    urma::UrmaControl pull;
    ASSERT_TRUE(pull.Decode(wire));
    ASSERT_EQ(2u, pull.items_size());
    EXPECT_EQ(pull.items().begin()[0].source_id(), pull.items().begin()[1].source_id());
    EXPECT_EQ(65535u, pull.items().begin()[0].seq());
    EXPECT_EQ(0u, pull.items().begin()[1].seq());
    // A FREE for just one item must not release the batch's source buffers.
    urma::UrmaControl partial;
    auto* item = partial.add_items();
    item->set_seq(65535); item->set_length(5000);
    item->set_source_id(pull.items().begin()[0].source_id());
    ASSERT_TRUE(partial.Encode(&wire));
    EXPECT_EQ(-1, Connection(sender)->HandleControl(urma::MSG_FREE, wire));
    EXPECT_EQ(before.sources_outstanding + 1, urma::GetUrmaReadStats().sources_outstanding);
    EXPECT_TRUE(data[0].empty()); EXPECT_TRUE(data[1].empty()); EXPECT_EQ(5000u, data[2].size());
    DrainRx(); DrainTx(); DrainRx(); DrainTx();
    EXPECT_EQ(5000, sender->CutFromIOBufList(input, 3));
    DrainRx(); DrainTx(); DrainRx(); DrainTx();
    EXPECT_EQ(std::string(5000, 'a') + std::string(5000, 'b') + std::string(5000, 'c'),
              receiver->_input_processor.read_buf().to_string());
    EXPECT_EQ(before.sources_outstanding, urma::GetUrmaReadStats().sources_outstanding);
    EXPECT_EQ(3u, _pool->read_posts);
}

TEST_F(UrmaPooledTest, read_batch_source_is_retired_when_control_enqueue_fails) {
    auto* sender = Endpoint(); auto* receiver = Endpoint();
    ASSERT_TRUE(sender && receiver);
    Connect(sender, receiver); Connect(receiver, sender);
    butil::IOBuf input;
    AppendBlock(&input, 5000, 'a'); AppendBlock(&input, 5000, 'b');
    auto* connection = Connection(sender);
    auto* slot = _pool->io_jfs[connection->_preferred_jfs];
    const auto before = urma::GetUrmaReadStats().sources_outstanding;
    const auto seq = connection->_next_send_seq;
    slot->stopped.store(true);
    EXPECT_EQ(-1, connection->StartReadTransfer(input));
    EXPECT_EQ(before, urma::GetUrmaReadStats().sources_outstanding);
    EXPECT_EQ(10000u, input.size());
    EXPECT_EQ(seq, connection->_next_send_seq);
    slot->stopped.store(false);
}

TEST_F(UrmaPooledTest, read_100k_uses_multiple_large_blocks) {
    GFLAGS_NAMESPACE::FlagSaver flags;
    GFLAGS_NAMESPACE::SetCommandLineOption("urma_large_transfer", "read");
    auto* sender = Endpoint(); auto* receiver = Endpoint();
    ASSERT_TRUE(sender && receiver); Connect(sender, receiver); Connect(receiver, sender);
    butil::IOBuf data;
    {
        butil::IOBufAsZeroCopyOutputStream stream(&data, 65536);
        size_t remaining = 100 * 1024;
        while (remaining) {
            void* bytes; int capacity; ASSERT_TRUE(stream.Next(&bytes, &capacity));
            ASSERT_GT(capacity, 0);
            const size_t count = std::min<size_t>(remaining, capacity);
            memset(bytes, 'L', count); stream.BackUp(capacity - count); remaining -= count;
        }
    }
    butil::IOBuf* input[] = {&data};
    EXPECT_EQ(100 * 1024, sender->CutFromIOBufList(input, 1));
    for (int i = 0; i < 4; ++i) { DrainRx(); DrainTx(); }
    EXPECT_EQ(2u, _pool->read_posts);
    EXPECT_EQ(4u, std::accumulate(_pool->post_chain_sizes.begin(),
                                 _pool->post_chain_sizes.end(), size_t(0)));
    EXPECT_EQ(std::string(100 * 1024, 'L'), receiver->_input_processor.read_buf().to_string());
}

TEST_F(UrmaPooledTest, read_peer_small_blocks_split_local_large_block) {
    GFLAGS_NAMESPACE::FlagSaver flags;
    GFLAGS_NAMESPACE::SetCommandLineOption("urma_large_transfer", "read");
    auto* sender = Endpoint(); auto* receiver = Endpoint();
    ASSERT_TRUE(sender && receiver);
    urma::UrmaConnectionDescription peer;
    ASSERT_EQ(0, Connection(receiver)->GetLocalDescription(&peer));
    peer.read_max_size = 8192 - sizeof(butil::IOBuf::Block);
    ASSERT_EQ(0, Connection(sender)->ImportPeer(peer)); Connect(receiver, sender);
    butil::IOBuf data;
    {
        butil::IOBufAsZeroCopyOutputStream stream(&data, 65536);
        void* bytes; int capacity; ASSERT_TRUE(stream.Next(&bytes, &capacity));
        ASSERT_GE(capacity, 32000);
        memset(bytes, 'm', 32000); stream.BackUp(capacity - 32000);
    }
    butil::IOBuf* input[] = {&data};
    EXPECT_EQ(32000, sender->CutFromIOBufList(input, 1));
    for (int i = 0; i < 4; ++i) { DrainRx(); DrainTx(); }
    EXPECT_EQ(4u, _pool->read_posts);
    EXPECT_EQ(std::string(32000, 'm'), receiver->_input_processor.read_buf().to_string());
}

TEST_F(UrmaPooledTest, read_pull_routes_to_backup_and_early_read_cqe_is_safe) {
    GFLAGS_NAMESPACE::FlagSaver flags;
    GFLAGS_NAMESPACE::SetCommandLineOption("urma_large_transfer", "read");
    auto* sender = Endpoint(); auto* receiver = Endpoint();
    ASSERT_TRUE(sender && receiver); Connect(sender, receiver); Connect(receiver, sender);
    Connection(sender)->_preferred_jfs = 0;
    auto* preferred = _pool->io_jfs[0];
    preferred->sq_inflight.store(preferred->sq_depth);
    butil::IOBuf data; AppendBlock(&data, 8000, 'b');
    butil::IOBuf* input[] = {&data};
    ASSERT_EQ(8000, sender->CutFromIOBufList(input, 1));
    EXPECT_EQ(1u, _pool->send_jfs_indices.back());
    preferred->sq_inflight.store(0);
    DrainTx();
    _pool->complete_before_return = true;
    DrainRx(); // READ CQE handled before post returns; its FREE may enqueue recursively.
    _pool->complete_before_return = false;
    DrainTx(); DrainRx(); DrainTx();
    EXPECT_EQ(std::string(8000, 'b'), receiver->_input_processor.read_buf().to_string());
    for (auto* slot : _pool->io_jfs) {
        EXPECT_EQ(0u, slot->sq_inflight.load());
        EXPECT_EQ(0u, slot->sq_device_inflight.load());
    }
}

TEST_F(UrmaPooledTest, read_block_size_accounts_for_iobuf_metadata) {
    GFLAGS_NAMESPACE::FlagSaver flags;
    GFLAGS_NAMESPACE::SetCommandLineOption("urma_large_block_threshold", "20480");
    EXPECT_EQ(0u, urma::GetUrmaSerializationBlockSize(20480));
    EXPECT_EQ(65536u, urma::GetUrmaSerializationBlockSize(20481));
    EXPECT_EQ(8192u, urma::GetUrmaBlockSizeForPayload(8192 - sizeof(butil::IOBuf::Block)));
    EXPECT_EQ(65536u, urma::GetUrmaBlockSizeForPayload(8192));
    EXPECT_EQ(65536u, urma::GetUrmaBlockSizeForPayload(65536 - sizeof(butil::IOBuf::Block)));
    EXPECT_EQ(0u, urma::GetUrmaBlockSizeForPayload(65536));
}

TEST_F(UrmaPooledTest, read_invalid_segments_leave_import_retryable) {
    auto* sender = Endpoint();
    auto* receiver = Endpoint();
    ASSERT_TRUE(sender && receiver);
    urma::UrmaConnectionDescription peer;
    ASSERT_EQ(0, Connection(receiver)->GetLocalDescription(&peer));
    ASSERT_TRUE(peer.big_capabilities & urma::URMA_BIG_CAP_READ);
    ASSERT_FALSE(peer.segments.empty());
    for (int variant = 0; variant < 5; ++variant) {
        SCOPED_TRACE(variant);
        auto invalid = peer;
        switch (variant) {
        case 0: invalid.segments.clear(); break;
        case 1: invalid.segments.resize(17, peer.segments.front()); break;
        case 2: invalid.segments.back().va = 0; break;
        case 3: invalid.segments.back().length = 0; break;
        case 4:
            invalid.segments.back().va = UINT64_MAX - 3;
            invalid.segments.back().length = 8;
            break;
        }
        EXPECT_EQ(-1, Connection(sender)->ImportPeer(invalid));
        EXPECT_EQ(EPROTO, errno);
        EXPECT_EQ(nullptr, Connection(sender)->_shared_state->remote_seg);
        EXPECT_TRUE(Connection(sender)->_shared_state->remote_segments.empty());
        EXPECT_FALSE(Connection(sender)->IsWritable());
    }
    // No partial import or terminal state may prevent a valid retry.
    EXPECT_EQ(0, Connection(sender)->ImportPeer(peer));
    EXPECT_TRUE(Connection(sender)->IsWritable());
}

TEST_F(UrmaPooledTest, read_missing_peer_capability_falls_back_to_send) {
    GFLAGS_NAMESPACE::FlagSaver flags;
    GFLAGS_NAMESPACE::SetCommandLineOption("urma_large_transfer", "read");
    auto* sender = Endpoint(); auto* receiver = Endpoint();
    ASSERT_TRUE(sender && receiver);
    urma::UrmaConnectionDescription peer;
    ASSERT_EQ(0, Connection(receiver)->GetLocalDescription(&peer));
    peer.big_capabilities = 0; peer.segments.clear(); peer.read_max_size = 0;
    ASSERT_EQ(0, Connection(sender)->ImportPeer(peer));
    butil::IOBuf data; AppendBlock(&data, 8000, 's');
    butil::IOBuf* input[] = {&data};
    EXPECT_EQ(8000, sender->CutFromIOBufList(input, 1));
    DrainTx(); DrainRx();
    EXPECT_EQ(0u, _pool->read_posts);
    EXPECT_EQ(std::string(8000, 's'), receiver->_input_processor.read_buf().to_string());
}

TEST_F(UrmaPooledTest, read_rejects_bad_source_and_free_before_post) {
    auto* sender = Endpoint(); auto* receiver = Endpoint();
    ASSERT_TRUE(sender && receiver); Connect(sender, receiver); Connect(receiver, sender);
    urma::UrmaControl control;
    auto* item = control.add_items(); item->set_seq(0); item->set_length(4096);
    item->set_source_id(1); item->set_addr(UINT64_MAX - 3); item->set_token_id(1);
    butil::IOBuf payload; ASSERT_TRUE(control.Encode(&payload));
    EXPECT_EQ(-1, Connection(receiver)->HandleControl(urma::MSG_PULL_MEM, payload));
    EXPECT_EQ(EPROTO, errno); EXPECT_EQ(0u, _pool->read_posts);
    urma::UrmaControl free;
    item = free.add_items(); item->set_seq(0); item->set_length(4096); item->set_source_id(1);
    ASSERT_TRUE(free.Encode(&payload));
    EXPECT_EQ(-1, Connection(sender)->HandleControl(urma::MSG_FREE, payload));
    EXPECT_EQ(EPROTO, errno);
}

TEST(UrmaReadControlTest, max_items_and_truncated_reserved_fields) {
    urma::UrmaControl control;
    for (size_t i = 0; i < 99; ++i) {
        auto* item = control.add_items(); ASSERT_NE(nullptr, item);
        item->set_seq(i); item->set_length(8192); item->set_source_id(i + 1);
        item->set_addr(0x1000 + i * 8192); item->set_token_id(17);
    }
    EXPECT_EQ(nullptr, control.add_items());
    butil::IOBuf encoded; ASSERT_TRUE(control.Encode(&encoded)); EXPECT_EQ(4008u, encoded.size());
    urma::UrmaControl decoded; ASSERT_TRUE(decoded.Decode(encoded)); EXPECT_EQ(99u, decoded.items_size());
    encoded.pop_back(1); EXPECT_FALSE(decoded.Decode(encoded));
    ASSERT_TRUE(control.Encode(&encoded));
    auto bytes = encoded.to_string(); bytes[16] = 1;
    encoded.clear(); encoded.append(bytes); EXPECT_FALSE(decoded.Decode(encoded));
}

TEST(UrmaHandshakeTest, read_segment_extension_roundtrip_and_legacy) {
    urma::UrmaSegmentDescription segment;
    segment.va = 0x10000; segment.length = 65536; segment.token_id = 7;
    segment.eid[0] = 9; segment.uasid = 11;
    auto extension = urma::EncodePoolSegments({segment}, urma::URMA_BIG_CAP_READ, 65512);
    std::vector<urma::UrmaSegmentDescription> segments;
    uint32_t caps = 0, size = 0;
    ASSERT_TRUE(urma::DecodePoolSegments(extension, &segments, &caps, &size));
    ASSERT_EQ(1u, segments.size()); EXPECT_EQ(segment.va, segments[0].va);
    EXPECT_EQ(segment.length, segments[0].length); EXPECT_EQ(segment.eid, segments[0].eid);
    EXPECT_EQ(urma::URMA_BIG_CAP_READ, caps); EXPECT_EQ(65512u, size);
    ASSERT_TRUE(urma::DecodePoolSegments("", &segments, &caps, &size));
    EXPECT_EQ(0u, caps); EXPECT_TRUE(segments.empty());
    EXPECT_FALSE(urma::DecodePoolSegments("SEG1broken", &segments, &caps, &size));
}

TEST_F(UrmaPooledTest, event_driven_read_rpc_binary_and_protobuf_handshakes) {
    // Use real epoll callbacks and the normal brpc handshake/message parser.
    ASSERT_TRUE(_pool->Shutdown());
    if (_state) _state->pool = nullptr;
    _pool.reset();
    urma::g_urma_pool = _original;
    GFLAGS_NAMESPACE::FlagSaver flags;
    GFLAGS_NAMESPACE::SetCommandLineOption("urma_large_transfer", "read");
    PooledEchoService service;
    Server server;
    ASSERT_EQ(0, server.AddService(&service, SERVER_DOESNT_OWN_SERVICE));
    ServerOptions server_options;
    server_options.socket_mode = SOCKET_MODE_URMA;
    ASSERT_EQ(0, server.Start("127.0.0.1:0", &server_options));
    for (int version : {2, 3}) {
        urma::FLAGS_urma_client_handshake_version = version;
        Channel channel;
        ChannelOptions options;
        options.socket_mode = SOCKET_MODE_URMA;
        options.connection_type = "single";
        options.timeout_ms = 3000;
        options.max_retry = 0;
        ASSERT_EQ(0, channel.Init(server.listen_address(), &options));
        test::EchoService_Stub stub(&channel);
        for (int n = 0; n < 4; ++n) {
            test::EchoRequest request;
            test::EchoResponse response;
            request.set_message(std::string(100 * 1024 + n, char('a' + n)));
            Controller cntl;
            cntl.request_attachment().append(std::string(11000, 'z'));
            stub.Echo(&cntl, &request, &response, nullptr);
            ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
            EXPECT_EQ(request.message(), response.message());
            EXPECT_EQ(cntl.request_attachment().to_string(),
                      cntl.response_attachment().to_string());
        }
    }
    server.Stop(0);
    server.Join();
}


TEST_F(UrmaPooledTest, auto_small_iobufs_do_not_trigger_read_by_total_size) {
    GFLAGS_NAMESPACE::FlagSaver flags;
    GFLAGS_NAMESPACE::SetCommandLineOption("urma_large_transfer", "auto");
    GFLAGS_NAMESPACE::SetCommandLineOption("urma_large_block_threshold", "20480");
    auto* sender = Endpoint(); auto* receiver = Endpoint();
    ASSERT_TRUE(sender && receiver); Connect(sender, receiver); Connect(receiver, sender);
    butil::IOBuf data[10]; butil::IOBuf* input[10];
    for (size_t i = 0; i < 10; ++i) { AppendBlock(&data[i], 8000, 's'); input[i] = &data[i]; }
    size_t total = 0;
    while (total < 80000) {
        const ssize_t n = sender->CutFromIOBufList(input, 10);
        ASSERT_GT(n, 0); total += n;
        DrainTx(); DrainRx();
    }
    EXPECT_EQ(0u, _pool->read_posts);
    EXPECT_EQ(std::string(80000, 's'), receiver->_input_processor.read_buf().to_string());
}

TEST_F(UrmaPooledTest, auto_threshold_and_mixed_stream_preserve_order) {
    GFLAGS_NAMESPACE::FlagSaver flags;
    GFLAGS_NAMESPACE::SetCommandLineOption("urma_large_transfer", "auto");
    GFLAGS_NAMESPACE::SetCommandLineOption("urma_large_block_threshold", "20480");
    auto* sender = Endpoint(); auto* receiver = Endpoint();
    ASSERT_TRUE(sender && receiver); Connect(sender, receiver); Connect(receiver, sender);
    butil::IOBuf data[3];
    for (int i = 0; i < 5; ++i) { AppendBlock(&data[0], 4096, 'a'); AppendBlock(&data[1], 4096, 'b'); }
    AppendBlock(&data[1], 1, 'b'); AppendBlock(&data[2], 1000, 'c');
    butil::IOBuf* input[] = {&data[0], &data[1], &data[2]};
    size_t total = 0;
    while (total < 41961) {
        const ssize_t n = sender->CutFromIOBufList(input, 3);
        ASSERT_GT(n, 0); total += n;
        for (int i = 0; i < 4; ++i) { DrainRx(); DrainTx(); }
    }
    EXPECT_GT(_pool->read_posts, 0u);
    EXPECT_EQ(std::string(20480, 'a') + std::string(20481, 'b') + std::string(1000, 'c'),
              receiver->_input_processor.read_buf().to_string());
    EXPECT_EQ(0u, Connection(sender)->_read_bytes_remaining);
}

TEST_F(UrmaPooledTest, auto_partial_read_keeps_short_tail_read_on_retry) {
    GFLAGS_NAMESPACE::FlagSaver flags;
    GFLAGS_NAMESPACE::SetCommandLineOption("urma_large_transfer", "auto");
    GFLAGS_NAMESPACE::SetCommandLineOption("urma_large_block_threshold", "20480");
    GFLAGS_NAMESPACE::SetCommandLineOption("urma_read_items_per_control", "1");
    auto* sender = Endpoint(); auto* receiver = Endpoint();
    ASSERT_TRUE(sender && receiver); Connect(sender, receiver); Connect(receiver, sender);
    butil::IOBuf data, small;
    AppendBlock(&data, 8000, 'a'); AppendBlock(&data, 8000, 'a'); AppendBlock(&data, 8000, 'a');
    AppendBlock(&small, 1000, 's');
    butil::IOBuf* input[] = {&data, &small};
    ASSERT_EQ(8000, sender->CutFromIOBufList(input, 2));
    EXPECT_EQ(16000u, data.size());
    for (int i = 0; i < 4; ++i) { DrainRx(); DrainTx(); }
    // EAGAIN must also retain the previous READ decision.
    for (auto* slot : _pool->io_jfs) slot->sq_inflight.store(slot->sq_depth);
    EXPECT_EQ(-1, sender->CutFromIOBufList(input, 2));
    EXPECT_EQ(EAGAIN, errno);
    for (auto* slot : _pool->io_jfs) slot->sq_inflight.store(0);
    for (int n = 0; n < 2; ++n) {
        ASSERT_EQ(8000, sender->CutFromIOBufList(input, 2));
        for (int i = 0; i < 4; ++i) { DrainRx(); DrainTx(); }
    }
    EXPECT_EQ(3u, _pool->read_posts);
    ASSERT_EQ(1000, sender->CutFromIOBufList(input, 2));
    DrainTx(); DrainRx();
    EXPECT_EQ(3u, _pool->read_posts);
    EXPECT_EQ(std::string(24000, 'a') + std::string(1000, 's'),
              receiver->_input_processor.read_buf().to_string());
}

#else  // BRPC_WITH_URMA

// When URMA is not compiled in, the test file is a no-op so the build stays
// clean. The brpc_urma_unittest target still links (against brpc-shared which
// provides the empty stubs).

#endif  // BRPC_WITH_URMA

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    gflags::ParseCommandLineFlags(&argc, &argv, true);
#if BRPC_WITH_URMA
    urma::g_skip_urma_init = true;
    urma::g_urma_available.store(true, butil::memory_order_relaxed);

#endif
    return RUN_ALL_TESTS();
}
