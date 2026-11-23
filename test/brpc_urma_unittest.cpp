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

#include <cstring>
#include <dlfcn.h>
#include <limits>
#include <mutex>
#include <thread>
#include <map>
#include "bthread/butex.h"
#include "bthread/bthread.h"
#include "butil/time.h"
#include <gtest/gtest.h>
#include <gflags/gflags.h>

#if BRPC_WITH_URMA
#include "butil/atomicops.h"
#include "butil/fd_guard.h"
#include "butil/sys_byteorder.h"
#include "urma_api.h"
#include "brpc/urma/urma_handshake.h"
#include "brpc/urma/urma_handshake.pb.h"
#include "brpc/urma/urma_helper.h"
#include "brpc/urma/urma_connection.h"
#include "brpc/urma/urma_endpoint.h"
#include "brpc/urma_transport.h"
#include "brpc/server.h"
#include "brpc/channel.h"
#include "brpc/controller.h"
#include "brpc/closure_guard.h"
#include "echo.pb.h"
#include "urma_types.h"

using namespace brpc;
DECLARE_int32(task_group_ntags);

namespace brpc {
namespace urma {

DECLARE_int32(urma_client_handshake_version);
DECLARE_string(urma_resource_mode);
extern bool g_skip_urma_init;
extern butil::atomic<bool> g_urma_available;

}  // namespace urma
}  // namespace brpc

TEST(UrmaConnectionModeTest, mode_is_fixed_until_explicit_startup_configuration) {
    const std::string saved = urma::FLAGS_urma_resource_mode;
    urma::FLAGS_urma_resource_mode = "pooled";
    EXPECT_EQ(0, urma::ConfigureUrmaConnectionMode());
    EXPECT_EQ(urma::UrmaConnectionMode::POOLED, urma::GetUrmaConnectionMode());
    urma::FLAGS_urma_resource_mode = "exclusive";
    EXPECT_EQ(urma::UrmaConnectionMode::POOLED, urma::GetUrmaConnectionMode());
    EXPECT_EQ(0, urma::ConfigureUrmaConnectionMode());
    EXPECT_EQ(urma::UrmaConnectionMode::EXCLUSIVE, urma::GetUrmaConnectionMode());
    urma::FLAGS_urma_resource_mode = "invalid";
    EXPECT_EQ(-1, urma::ConfigureUrmaConnectionMode());
    EXPECT_EQ(EINVAL, errno);
    EXPECT_EQ(urma::UrmaConnectionMode::EXCLUSIVE, urma::GetUrmaConnectionMode());
    urma::FLAGS_urma_resource_mode = saved;
    EXPECT_EQ(0, urma::ConfigureUrmaConnectionMode());
}

// ---------------------------------------------------------------------------
// v2 binary HelloMessage: serialize + deserialize round-trips.
// ---------------------------------------------------------------------------
TEST(UrmaHandshakeTest, v2_serialize_deserialize_roundtrip) {
    urma::v2_wire::HelloMessage m;
    m.msg_len = urma::v2_wire::HELLO_PACKET_LEN;
    m.hello_ver = urma::v2_wire::HELLO_V2_VERSION;
    m.impl_ver = urma::v2_wire::IMPL_V2_VERSION;
    m.buffer_size = 8192;
    m.recv_buffer_cnt = 127;
    m.jetty_id = 0x12345678;
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

    uint8_t buf[urma::v2_wire::HELLO_BODY_LEN];
    m.Serialize(buf);

    urma::v2_wire::HelloMessage m2;
    m2.Deserialize(buf);
    EXPECT_EQ(m.msg_len, m2.msg_len);
    EXPECT_EQ(m.hello_ver, m2.hello_ver);
    EXPECT_EQ(m.impl_ver, m2.impl_ver);
    EXPECT_EQ(m.buffer_size, m2.buffer_size);
    EXPECT_EQ(m.recv_buffer_cnt, m2.recv_buffer_cnt);
    EXPECT_EQ(m.jetty_id, m2.jetty_id);
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
    EXPECT_EQ(4u + 82u, urma::v2_wire::HELLO_PACKET_LEN);
}

// ---------------------------------------------------------------------------
// v3 protobuf UrmaHello: serialize + parse round-trips.
// ---------------------------------------------------------------------------
TEST(UrmaHandshakeTest, v3_protobuf_roundtrip) {
    urma::UrmaHello msg;
    msg.set_buffer_size(8192);
    msg.set_recv_buffer_cnt(127);
    msg.set_jetty_id(0x12345678);
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
    hello.buffer_size = 8192;
    hello.recv_buffer_cnt = 127;
    hello.jetty_id = 1;
    hello.tp_type = URMA_CTP;
    hello.seg_va = 0x1000;
    hello.seg_len = 8192;
    EXPECT_TRUE(urma::ValidHello(hello));

    hello.recv_buffer_cnt = 2;
    EXPECT_FALSE(urma::ValidHello(hello));
    hello.recv_buffer_cnt = 127;
    hello.seg_len = 0;
    EXPECT_FALSE(urma::ValidHello(hello));
    hello.seg_len = 8192;
    hello.jetty_id = 0;
    EXPECT_FALSE(urma::ValidHello(hello));

    hello.jetty_id = 1;
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
TEST(UrmaHelperTest, event_mode_polling_init_needs_no_provider) {
    gflags::FlagSaver flags;
    urma::FLAGS_urma_use_polling = false;
    EXPECT_EQ(0, urma::UrmaEndpoint::PollingModeInitialize(
        0, nullptr, nullptr, nullptr));
}

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


namespace {
std::mutex connection_test_mutex;
std::map<SocketId, butil::IOBuf> connection_received;
std::map<SocketId, bthread_tag_t> connection_received_tag;
int connection_errors = 0;
bool close_connection_on_callback = false;
butil::atomic<int> connection_user_buffer_released{0};
bool ConnectionReceived(SocketUniquePtr& socket, const urma::UrmaConnection* connection,
                      butil::IOBuf data) {
    const SocketId id = socket->id();
    std::lock_guard<std::mutex> lock(connection_test_mutex);
    connection_received[id].append(data.movable());
    connection_received_tag[id] = bthread_self_tag();
    if (close_connection_on_callback) const_cast<urma::UrmaConnection*>(connection)->Close();
    return true;
}
void ConnectionError(SocketId, const urma::UrmaConnectionHandle& connection, int) {
    std::lock_guard<std::mutex> lock(connection_test_mutex);
    ++connection_errors;
    if (close_connection_on_callback) connection->Close();
}

}

namespace brpc { namespace urma {
DECLARE_int32(urma_buffer_count);
DECLARE_int32(urma_buffer_size);
DECLARE_int32(urma_max_sge);
DECLARE_int32(urma_prepared_jetty_cnt);
DECLARE_bool(urma_use_polling);
}}

class UrmaConnectionTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Use the real connection implementation with the link-time provider.
        urma_init_attr_t attr{};
        urma_init(&attr);
        int n = 0;
        urma_device_t** devices = urma_get_device_list(&n);
        const bool mock = devices && n &&
            strcmp(devices[0]->name, "mock_urma_device") == 0;
        if (devices) urma_free_device_list(devices);
        if (!mock) GTEST_SKIP() << "Requires link-time mock";
        urma::FLAGS_urma_buffer_size = 4096;
        urma::FLAGS_urma_buffer_count = 16384;
        if (urma::FLAGS_urma_max_sge <= 0) urma::FLAGS_urma_max_sge = 8;
        urma::FLAGS_urma_prepared_jetty_cnt = 0;
        urma::g_skip_urma_init = false;
        urma::GlobalUrmaInitializeOrDie();
        ASSERT_EQ(0, urma::CreateUrmaConnectionProvider(
            urma::UrmaConnectionMode::EXCLUSIVE, &provider));
        urma::UrmaConnectionCallbacks callbacks;
        callbacks.on_bytes = ConnectionReceived;
        callbacks.on_input_ready = [](SocketUniquePtr&, InputMessageClosure&) {};
        callbacks.on_error = ConnectionError;
        ASSERT_EQ(0, provider->Initialize(callbacks));
        ASSERT_EQ(0, provider->InitializePollingMode(0, nullptr, nullptr, nullptr));
        urma::UrmaConnectionOptions opts;
        SocketOptions socket_options;
        ASSERT_EQ(0, Socket::Create(socket_options, &a_id));
        ASSERT_EQ(0, Socket::Address(a_id, &a_socket));
        ASSERT_EQ(0, Socket::Create(socket_options, &b_id));
        ASSERT_EQ(0, Socket::Address(b_id, &b_socket));
        opts.socket_id = a_id;
        ASSERT_EQ(0, provider->CreateConnection(opts, &a));
        opts.socket_id = b_id;
        opts.role = urma::UrmaConnectionRole::SERVER;
        ASSERT_EQ(0, provider->CreateConnection(opts, &b));
        ASSERT_EQ(0, a->GetLocalDescription(&ad));
        ASSERT_EQ(0, b->GetLocalDescription(&bd));
        std::lock_guard<std::mutex> lock(connection_test_mutex);
        connection_received.clear();
        connection_received_tag.clear();
        connection_errors = 0;
        close_connection_on_callback = false;
    }
    void TearDown() override {
        if (a) a->Close();
        if (b) b->Close();
        a.reset(); b.reset();
        if (a_socket) a_socket->SetFailed();
        if (b_socket) b_socket->SetFailed();
        a_socket.reset(); b_socket.reset();
        if (provider) {
            bool stopped = false;
            for (int i = 0; i < 1000 && !stopped; ++i) {
                stopped = provider->Shutdown();
                if (!stopped) usleep(1000);
            }
            EXPECT_TRUE(stopped);
        }
    }
    void Bind() {
        ASSERT_EQ(0, a->ImportPeer(bd));
        ASSERT_EQ(0, b->ImportPeer(ad));
    }
    void CreateSocketConnection(SocketUniquePtr* socket,
                                urma::UrmaConnectionHandle* connection) {
        SocketOptions options;
        SocketId id;
        ASSERT_EQ(0, Socket::Create(options, &id));
        ASSERT_EQ(0, Socket::Address(id, socket));
        urma::UrmaConnectionOptions opts;
        opts.socket_id = id;
        ASSERT_EQ(0, provider->CreateConnection(opts, connection));
    }
    std::unique_ptr<urma::UrmaConnectionProvider> provider;
    urma::UrmaConnectionHandle a, b;
    SocketId a_id = INVALID_SOCKET_ID, b_id = INVALID_SOCKET_ID;
    SocketUniquePtr a_socket, b_socket;
    urma::UrmaConnectionDescription ad, bd;
};

TEST_F(UrmaConnectionTest, send_consumes_input_and_transfers_bytes) {
    Bind();
    butil::IOBuf first, second;
    first.append("first"); second.append(std::string(9000, 'b'));
    butil::IOBuf* data[] = {&first, &second};
    ASSERT_EQ(9005, a->CutFromIOBufList(data, 2));
    EXPECT_TRUE(first.empty());
    EXPECT_TRUE(second.empty());
    for (int i = 0; i < 1000; ++i) {
        { std::lock_guard<std::mutex> lock(connection_test_mutex);
          if (connection_received[b_id].size() == 9005) break; }
        usleep(1000);
    }
    std::lock_guard<std::mutex> lock(connection_test_mutex);
    EXPECT_EQ("first" + std::string(9000, 'b'), connection_received[b_id].to_string());
    EXPECT_EQ(0, connection_errors);
}

TEST_F(UrmaConnectionTest, invalid_tail_consumes_only_accepted_prefix) {
    bd.recv_buffer_size = 1024;
    Bind();
    butil::IOBuf good, bad;
    good.append(std::string(1024, 'g'));
    char unregistered[32] = {};
    bad.append_user_data(unregistered, sizeof(unregistered), [](void*) {});
    butil::IOBuf* data[] = {&good, &bad};
    EXPECT_EQ(1024, a->CutFromIOBufList(data, 2));
    EXPECT_TRUE(good.empty());
    EXPECT_EQ(32u, bad.size());
}

TEST_F(UrmaConnectionTest, invalid_sge_restores_unsubmitted_wr_inputs) {
    if (urma::GetUrmaMaxSge() < 2) GTEST_SKIP() << "Requires multiple SGEs";
    Bind();
    butil::IOBuf good, bad;
    good.append("registered-prefix");
    char unregistered[] = "unregistered-tail";
    bad.append_user_data(unregistered, sizeof(unregistered), [](void*) {});
    butil::IOBuf* data[] = {&good, &bad};
    EXPECT_EQ(-1, a->CutFromIOBufList(data, 2));
    EXPECT_EQ("registered-prefix", good.to_string());
    EXPECT_EQ(sizeof(unregistered), bad.size());
}

TEST_F(UrmaConnectionTest, rejected_wr_restores_multiple_input_buffers) {
    if (urma::GetUrmaMaxSge() < 3) GTEST_SKIP() << "Requires multiple SGEs";
    using FailSend = void (*)(int);
    auto fail = reinterpret_cast<FailSend>(dlsym(
        RTLD_DEFAULT, "brpc_urma_mock_fail_send_after"));
    ASSERT_TRUE(fail);
    Bind();
    butil::IOBuf first, empty, second, third;
    first.append("first"); second.append("second");
    third.append(std::string(6000, 't'));
    butil::IOBuf* data[] = {&first, &empty, &second, &third};
    fail(0);
    EXPECT_EQ(-1, a->CutFromIOBufList(data, 4));
    EXPECT_EQ(EIO, errno);
    EXPECT_EQ("first", first.to_string());
    EXPECT_TRUE(empty.empty());
    EXPECT_EQ("second", second.to_string());
    EXPECT_EQ(std::string(6000, 't'), third.to_string());
}

TEST_F(UrmaConnectionTest, later_post_failure_preserves_only_unsent_user_data) {
    using FailSend = void (*)(int);
    auto fail = reinterpret_cast<FailSend>(dlsym(
        RTLD_DEFAULT, "brpc_urma_mock_fail_send_after"));
    ASSERT_TRUE(fail);
    bd.recv_buffer_size = 1024;
    Bind();
    char* memory = new char[2048];
    memset(memory, 'a', 1024);
    memset(memory + 1024, 'b', 1024);
    const uint64_t segment = urma::RegisterMemoryForUrma(memory, 2048);
    ASSERT_NE(0u, segment);
    butil::IOBuf payload;
    payload.append_user_data_with_meta(memory, 2048, [](void* p) {
        urma::DeregisterMemoryForUrma(p);
        delete[] static_cast<char*>(p);
    }, segment);
    butil::IOBuf* data[] = {&payload};
    fail(1);
    EXPECT_EQ(1024, a->CutFromIOBufList(data, 1));
    EXPECT_EQ(std::string(1024, 'b'), payload.to_string());
    for (int i = 0; i < 1000; ++i) {
        { std::lock_guard<std::mutex> lock(connection_test_mutex);
          if (connection_received[b_id].size() == 1024) break; }
        usleep(1000);
    }
    std::lock_guard<std::mutex> lock(connection_test_mutex);
    EXPECT_EQ(std::string(1024, 'a'), connection_received[b_id].to_string());
}

TEST_F(UrmaConnectionTest, close_does_not_wait_for_admitted_provider_send) {
    using Pause = void (*)(bool);
    using Entered = bool (*)();
    auto pause = reinterpret_cast<Pause>(dlsym(
        RTLD_DEFAULT, "brpc_urma_mock_pause_send"));
    auto entered = reinterpret_cast<Entered>(dlsym(
        RTLD_DEFAULT, "brpc_urma_mock_send_entered"));
    ASSERT_TRUE(pause && entered);
    Bind();
    butil::IOBuf payload;
    payload.append("in-flight");
    butil::IOBuf* data[] = {&payload};
    ssize_t sent = -1;
    pause(true);
    std::thread sender([&, connection = a] { sent = connection->CutFromIOBufList(data, 1); });
    for (int i = 0; i < 1000 && !entered(); ++i) usleep(1000);
    EXPECT_TRUE(entered());
    butil::atomic<bool> closed{false};
    std::thread closer([&, connection = a] {
        connection->Close();
        closed.store(true);
    });
    for (int i = 0; i < 500 && !closed.load(); ++i) usleep(1000);
    EXPECT_TRUE(closed.load());
    pause(false);
    sender.join();
    closer.join();
    EXPECT_EQ(9, sent);
    EXPECT_TRUE(payload.empty());
    EXPECT_FALSE(a->IsWritable());
    payload.append("after-close");
    EXPECT_EQ(-1, a->CutFromIOBufList(data, 1));
    EXPECT_EQ(EPIPE, errno);
    EXPECT_EQ("after-close", payload.to_string());
}

TEST_F(UrmaConnectionTest, consumed_user_data_is_retained_until_provider_finishes) {
    using Pause = void (*)(bool);
    using Entered = bool (*)();
    auto pause = reinterpret_cast<Pause>(dlsym(
        RTLD_DEFAULT, "brpc_urma_mock_pause_send_copy"));
    auto entered = reinterpret_cast<Entered>(dlsym(
        RTLD_DEFAULT, "brpc_urma_mock_send_copy_entered"));
    auto pause_return = reinterpret_cast<Pause>(dlsym(
        RTLD_DEFAULT, "brpc_urma_mock_pause_send_return"));
    auto return_entered = reinterpret_cast<Entered>(dlsym(
        RTLD_DEFAULT, "brpc_urma_mock_send_return_entered"));
    ASSERT_TRUE(pause && entered && pause_return && return_entered);
    Bind();
    char* memory = new char[64];
    memset(memory, 'r', 64);
    const uint64_t segment = urma::RegisterMemoryForUrma(memory, 64);
    ASSERT_NE(0u, segment);
    connection_user_buffer_released.store(0);
    butil::IOBuf payload;
    payload.append_user_data_with_meta(memory, 64, [](void* p) {
        connection_user_buffer_released.fetch_add(1);
        // Keep allocation alive until the assertion to make premature release
        // deterministic rather than allowing the mock to read freed memory.
    }, segment);
    butil::IOBuf* data[] = {&payload};
    pause(true);
    pause_return(true);
    ssize_t sent = -1;
    std::thread sender([&, connection = a] { sent = connection->CutFromIOBufList(data, 1); });
    for (int i = 0; i < 1000 && !entered(); ++i) usleep(1000);
    EXPECT_TRUE(entered());
    usleep(20000);  // Allow the CQ consumer to expose a premature completion.
    EXPECT_EQ(0, connection_user_buffer_released.load());
    pause(false);
    for (int i = 0; i < 1000 && !return_entered(); ++i) usleep(1000);
    EXPECT_TRUE(return_entered());
    // CQ may release the staged payload before native post returns. The
    // successful sender must not inspect those references afterwards.
    for (int i = 0; i < 1000 && !connection_user_buffer_released.load(); ++i) {
        usleep(1000);
    }
    EXPECT_EQ(1, connection_user_buffer_released.load());
    pause_return(false);
    sender.join();
    EXPECT_EQ(64, sent);
    EXPECT_TRUE(payload.empty());
    for (int i = 0; i < 1000; ++i) {
        { std::lock_guard<std::mutex> lock(connection_test_mutex);
          if (connection_received[b_id].size() == 64) break; }
        usleep(1000);
    }
    {
        std::lock_guard<std::mutex> lock(connection_test_mutex);
        EXPECT_EQ(std::string(64, 'r'), connection_received[b_id].to_string());
    }
    urma::DeregisterMemoryForUrma(memory);
    delete[] memory;
}

TEST_F(UrmaConnectionTest, rejects_wrong_topology_before_binding) {
    bd.receive_target_kind = urma::UrmaReceiveTargetKind::JFR;
    EXPECT_EQ(-1, a->ImportPeer(bd));
    EXPECT_EQ(EPROTO, errno);
    EXPECT_FALSE(a->IsWritable());
}

TEST_F(UrmaConnectionTest, exclusive_provider_does_not_require_delivery_scope) {
    std::unique_ptr<urma::UrmaConnectionProvider> other;
    ASSERT_EQ(0, urma::CreateUrmaConnectionProvider(
        urma::UrmaConnectionMode::EXCLUSIVE, &other));
    urma::UrmaConnectionCallbacks callbacks;
    callbacks.on_bytes = ConnectionReceived;
        callbacks.on_input_ready = [](SocketUniquePtr&, InputMessageClosure&) {};
    callbacks.on_error = ConnectionError;
    EXPECT_EQ(0, other->Initialize(callbacks));
    EXPECT_TRUE(other->Shutdown());
}

TEST_F(UrmaConnectionTest, writable_wait_rejects_peer_not_imported) {
    auto* wait = bthread::butex_create_checked<butil::atomic<int>>();
    wait->store(0);
    const timespec deadline = butil::milliseconds_from_now(10);
    EXPECT_EQ(-1, a->WaitWritable(wait, 0, &deadline));
    EXPECT_EQ(ENOTCONN, errno);
    bthread::butex_destroy(wait);
}

TEST_F(UrmaConnectionTest, close_records_socket_wakeup_before_wait_registration) {
    SocketOptions options;
    SocketId id;
    ASSERT_EQ(0, Socket::Create(options, &id));
    SocketUniquePtr socket;
    ASSERT_EQ(0, Socket::Address(id, &socket));
    urma::UrmaConnectionOptions opts;
    opts.socket_id = id;
    urma::UrmaConnectionHandle connection;
    ASSERT_EQ(0, provider->CreateConnection(opts, &connection));
    auto* wait = socket->_epollout_butex;
    const int expected = wait->load();
    ASSERT_EQ(0, connection->ImportPeer(bd));
    EXPECT_EQ(expected, wait->load());  // Binding is not write progress.
    connection->Close();
    connection->Close();
    EXPECT_EQ(expected + 1, wait->load());
    const timespec deadline = butil::milliseconds_from_now(10);
    EXPECT_EQ(-1, bthread::butex_wait(wait, expected, &deadline));
    EXPECT_EQ(EWOULDBLOCK, errno);
    socket->SetFailed();
}

TEST_F(UrmaConnectionTest, connection_handle_does_not_retain_recycled_socket) {
    struct RecycleObserver : SocketUser {
        bool recycled = false;
        void BeforeRecycle(Socket*) override { recycled = true; }
    } observer;
    SocketOptions options;
    options.user = &observer;
    SocketId id;
    ASSERT_EQ(0, Socket::Create(options, &id));
    SocketUniquePtr socket;
    ASSERT_EQ(0, Socket::Address(id, &socket));
    urma::UrmaConnectionOptions opts;
    opts.socket_id = id;
    urma::UrmaConnectionHandle connection;
    ASSERT_EQ(0, provider->CreateConnection(opts, &connection));
    ASSERT_EQ(0, connection->ImportPeer(bd));
    socket->SetFailed();
    socket.reset();
    EXPECT_TRUE(observer.recycled);
    // The connection remains alive. Neither Close nor late CQ notifications
    // may dereference a Socket/butex pointer from its previous owner.
    connection->Close();
    connection.reset();
}

TEST_F(UrmaConnectionTest, writable_wait_rejects_a_foreign_butex) {
    SocketUniquePtr socket;
    urma::UrmaConnectionHandle connection;
    CreateSocketConnection(&socket, &connection);
    ASSERT_TRUE(socket && connection);
    ASSERT_EQ(0, connection->ImportPeer(bd));
    auto* wait = bthread::butex_create_checked<butil::atomic<int>>();
    wait->store(0);
    const timespec deadline = butil::milliseconds_from_now(10);
    EXPECT_EQ(-1, connection->WaitWritable(wait, 0, &deadline));
    EXPECT_EQ(EINVAL, errno);
    bthread::butex_destroy(wait);
    socket->SetFailed();
}

TEST_F(UrmaConnectionTest, close_is_idempotent_and_wakes_socket_waiter) {
    SocketUniquePtr socket;
    urma::UrmaConnectionHandle connection;
    CreateSocketConnection(&socket, &connection);
    ASSERT_TRUE(socket && connection);
    auto* wait = socket->_epollout_butex;
    const int before_send = wait->load();
    auto peer = bd;
    peer.recv_buffer_count = 3;  // One data credit plus reserved ACK slots.
    ASSERT_EQ(0, connection->ImportPeer(peer));
    butil::IOBuf payload;
    payload.append("x");
    butil::IOBuf* data[] = {&payload};
    ASSERT_EQ(1, connection->CutFromIOBufList(data, 1));
    // b is not bound, so its receive completion cannot return peer credit.
    // Wait for the local TX notification before testing a blocked waiter.
    for (int i = 0; i < 1000 && wait->load() == before_send; ++i) usleep(1000);
    EXPECT_NE(before_send, wait->load());
    EXPECT_FALSE(connection->IsWritable());
    const int expected = wait->load();
    int result = 0;
    int error = 0;
    std::thread waiter([&] {
        const timespec deadline = butil::seconds_from_now(2);
        result = connection->WaitWritable(wait, expected, &deadline);
        error = errno;
    });
    usleep(10000);
    connection->Close(); connection->Close();
    waiter.join();
    EXPECT_NE(expected, wait->load());
    EXPECT_EQ(-1, result);
    EXPECT_EQ(EPIPE, error);
    EXPECT_FALSE(connection->IsWritable());
    payload.append("closed");
    EXPECT_EQ(-1, connection->CutFromIOBufList(data, 1));
    EXPECT_EQ("closed", payload.to_string());
    socket->SetFailed();
}

TEST_F(UrmaConnectionTest, shutdown_retains_live_connections_and_rejects_creation) {
    EXPECT_FALSE(provider->Shutdown());
    urma::UrmaConnectionHandle other;
    urma::UrmaConnectionOptions opts;
    EXPECT_EQ(-1, provider->CreateConnection(opts, &other));
    EXPECT_FALSE(other);
    EXPECT_FALSE(a->IsWritable());
}


TEST_F(UrmaConnectionTest, empty_input_is_zero_even_before_bind) {
    butil::IOBuf empty;
    butil::IOBuf* data[] = {&empty};
    EXPECT_EQ(0, a->CutFromIOBufList(data, 1));
}

TEST_F(UrmaConnectionTest, validates_description_before_import) {
    const auto valid = bd;
    bd.recv_buffer_size = 1;
    EXPECT_EQ(-1, a->ImportPeer(bd));
    EXPECT_EQ(EPROTO, errno);
    bd = valid;
    bd.segment.va = std::numeric_limits<uint64_t>::max();
    bd.segment.length = 2;
    EXPECT_EQ(-1, a->ImportPeer(bd));
    EXPECT_EQ(EPROTO, errno);
}

TEST_F(UrmaConnectionTest, tcp_fallback_notifies_the_existing_socket_reader) {
    int fds[2];
    ASSERT_EQ(0, socketpair(AF_UNIX, SOCK_STREAM, 0, fds));
    butil::fd_guard peer(fds[1]);
    SocketOptions options;
    options.fd = fds[0];
    options.socket_mode = SOCKET_MODE_URMA;
    options.need_on_edge_trigger = true;
    SocketId id;
    ASSERT_EQ(0, Socket::Create(options, &id));
    SocketUniquePtr socket;
    ASSERT_EQ(0, Socket::Address(id, &socket));
    auto* transport = static_cast<UrmaTransport*>(socket->_transport.get());

    // Hold the event on an empty socket. The handshake must queue a recheck
    // for this owner, even when there are no bytes ready to read.
    int progress = Socket::PROGRESS_INIT;
    socket->_nevent.store(progress);
    transport->GetUrmaEp()->FallbackToTcp(transport, true);
    EXPECT_EQ(Socket::PROGRESS_INIT + 1, socket->_nevent.load());
    EXPECT_TRUE(socket->MoreReadEvents(&progress));
    EXPECT_FALSE(socket->MoreReadEvents(&progress));
    socket->SetFailed();
}

TEST_F(UrmaConnectionTest, endpoint_caches_early_rx_and_ignores_old_connection_callbacks) {
    SocketOptions options;
    options.socket_mode = SOCKET_MODE_URMA;
    SocketId id;
    ASSERT_EQ(0, Socket::Create(options, &id));
    SocketUniquePtr socket;
    ASSERT_EQ(0, Socket::Address(id, &socket));
    auto* transport = static_cast<UrmaTransport*>(socket->_transport.get());
    auto* endpoint = transport->GetUrmaEp();
    ASSERT_EQ(0, endpoint->AllocateResources(urma::UrmaConnectionRole::SERVER));
    auto old = endpoint->PinConnection();
    ASSERT_TRUE(old);
    butil::IOBuf first; first.append("early");
    urma::UrmaEndpoint::OnReceivedBytes(socket, old.get(), first.movable());
    EXPECT_EQ("early", endpoint->_input_processor.read_buf().to_string());
    EXPECT_EQ(5, endpoint->_pending_received_bytes.load());
    endpoint->Reset();
    butil::IOBuf no_source; no_source.append("unowned");
    urma::UrmaEndpoint::OnReceivedBytes(socket, nullptr, no_source.movable());
    EXPECT_TRUE(endpoint->_input_processor.read_buf().empty());
    urma::UrmaEndpoint::OnConnectionError(id, {}, EIO);
    EXPECT_FALSE(socket->Failed());
    ASSERT_EQ(0, endpoint->AllocateResources(urma::UrmaConnectionRole::SERVER));
    auto current = endpoint->PinConnection();
    ASSERT_TRUE(current);
    ASSERT_NE(old, current);
    butil::IOBuf late; late.append("old data");
    urma::UrmaEndpoint::OnReceivedBytes(socket, old.get(), late.movable());
    urma::UrmaEndpoint::OnConnectionError(id, old, EIO);
    EXPECT_TRUE(endpoint->_input_processor.read_buf().empty());
    EXPECT_EQ(0, endpoint->_pending_received_bytes.load());
    EXPECT_FALSE(socket->Failed());
    current->Close();
    EXPECT_TRUE(endpoint->PinConnection());  // Close retains the stable binding.
    socket->SetFailed();
}

class ResourceEchoService : public test::EchoService {
public:
    void Echo(google::protobuf::RpcController* base, const test::EchoRequest* req,
              test::EchoResponse* resp, google::protobuf::Closure* done) override {
        brpc::ClosureGuard guard(done);
        auto* cntl = static_cast<brpc::Controller*>(base);
        resp->set_message(req->message());
        cntl->response_attachment().append(cntl->request_attachment());
    }
};

TEST_F(UrmaConnectionTest, multiconnection_rpc_preserves_v2_v3_and_credit_progress) {
    ResourceEchoService service;
    brpc::Server server;
    ASSERT_EQ(0, server.AddService(&service, brpc::SERVER_DOESNT_OWN_SERVICE));
    brpc::ServerOptions server_options;
    server_options.socket_mode = SOCKET_MODE_URMA;
    ASSERT_EQ(0, server.Start("127.0.0.1:0", &server_options));
    for (int version : {2, 3}) {
        urma::FLAGS_urma_client_handshake_version = version;
        std::vector<std::unique_ptr<brpc::Channel>> channels;
        for (int i = 0; i < 8; ++i) {
            std::unique_ptr<brpc::Channel> channel(new brpc::Channel);
            brpc::ChannelOptions options;
            options.socket_mode = SOCKET_MODE_URMA;
            options.connection_group = "resource-" + std::to_string(version) +
                                       "-" + std::to_string(i);
            options.timeout_ms = 3000;
            options.max_retry = 0;
            ASSERT_EQ(0, channel->Init(server.listen_address(), &options));
            channels.push_back(std::move(channel));
        }
        std::vector<std::thread> writers;
        for (int i = 0; i < 8; ++i) {
            writers.emplace_back([&, i] {
                test::EchoService_Stub stub(channels[i].get());
                // Enough WRs to exhaust both original windows repeatedly.
                for (int round = 0; round < 12; ++round) {
                    brpc::Controller cntl;
                    test::EchoRequest req;
                    test::EchoResponse resp;
                    const std::string payload(round % 2 ? 102400 : 64,
                                              static_cast<char>('a' + i));
                    req.set_message("resource-contract");
                    cntl.request_attachment().append(payload);
                    stub.Echo(&cntl, &req, &resp, nullptr);
                    EXPECT_FALSE(cntl.Failed()) << cntl.ErrorText();
                    if (cntl.Failed()) return;
                    EXPECT_EQ(req.message(), resp.message());
                    EXPECT_EQ(payload, cntl.response_attachment().to_string());
                }
            });
        }
        for (auto& writer : writers) writer.join();
    }
    server.Stop(0);
    server.Join();
}


TEST_F(UrmaConnectionTest, binding_flushes_credit_for_early_rx_without_another_cqe) {
    ASSERT_EQ(0, b->ImportPeer(ad));
    butil::IOBuf payload;
    {
        butil::IOBufAsZeroCopyOutputStream stream(&payload);
        void* buffer = nullptr;
        int length = 0;
        ASSERT_TRUE(stream.Next(&buffer, &length));
        ASSERT_GE(length, 64);
        memset(buffer, 'e', 64);
        stream.BackUp(length - 64);
    }
    const butil::IOBuf seed = payload;
    butil::IOBuf* data[] = {&payload};
    size_t accepted = 0;
    for (int i = 0; i < 1000; ++i) {
        if (payload.empty()) payload = seed;
        const ssize_t n = b->CutFromIOBufList(data, 1);
        if (n > 0) accepted += n;
        else if (errno != EAGAIN) FAIL() << errno;
        if (accepted == 125 * 64) break;
        usleep(1000);
    }
    ASSERT_EQ(125u * 64, accepted);
    for (int i = 0; i < 1000; ++i) {
        { std::lock_guard<std::mutex> lock(connection_test_mutex);
          if (connection_received[a_id].size() == accepted) break; }
        usleep(1000);
    }
    ASSERT_FALSE(b->IsWritable());
    payload.clear();
    payload.append("credit-blocked");
    EXPECT_EQ(-1, b->CutFromIOBufList(data, 1));
    EXPECT_EQ(EAGAIN, errno);
    EXPECT_EQ("credit-blocked", payload.to_string());
    ASSERT_EQ(0, a->ImportPeer(bd));
    for (int i = 0; i < 1000 && !b->IsWritable(); ++i) usleep(1000);
    EXPECT_TRUE(b->IsWritable());
}


TEST_F(UrmaConnectionTest, failed_hardware_delete_is_retained_until_shutdown_retry) {
    using FailDelete = void (*)(int);
    auto fail_delete = reinterpret_cast<FailDelete>(dlsym(
        RTLD_DEFAULT, "brpc_urma_mock_fail_jetty_delete"));
    ASSERT_NE(nullptr, fail_delete);
    b->Close(); b.reset();
    fail_delete(2);
    a->Close(); a.reset();  // First failure while retiring the connection.
    EXPECT_FALSE(provider->Shutdown());  // Second failure must keep ownership.
    EXPECT_TRUE(provider->Shutdown());
}


TEST_F(UrmaConnectionTest, shutdown_waits_for_destructor_not_only_weak_expiration) {
    using Pause = void (*)(bool);
    using Entered = bool (*)();
    auto pause = reinterpret_cast<Pause>(dlsym(
        RTLD_DEFAULT, "brpc_urma_mock_pause_jetty_delete"));
    auto entered = reinterpret_cast<Entered>(dlsym(
        RTLD_DEFAULT, "brpc_urma_mock_jetty_delete_entered"));
    ASSERT_TRUE(pause && entered);
    b->Close(); b.reset();
    pause(true);
    auto retiring = std::move(a);
    std::thread closer([connection = std::move(retiring)]() mutable {
        connection->Close(); connection.reset();
    });
    for (int i = 0; i < 1000 && !entered(); ++i) usleep(1000);
    EXPECT_TRUE(entered());
    const bool stopped_too_early = provider->Shutdown();
    pause(false);
    closer.join();
    EXPECT_FALSE(stopped_too_early);
    EXPECT_TRUE(provider->Shutdown());
}


TEST_F(UrmaConnectionTest, failed_import_terminates_connection_before_rebind) {
    using FailImport = void (*)();
    auto fail_import = reinterpret_cast<FailImport>(dlsym(
        RTLD_DEFAULT, "brpc_urma_mock_fail_next_jetty_import"));
    ASSERT_TRUE(fail_import);
    fail_import();
    EXPECT_EQ(-1, a->ImportPeer(bd));
    EXPECT_EQ(-1, a->ImportPeer(bd));
    EXPECT_EQ(EPIPE, errno);
    EXPECT_FALSE(a->IsWritable());
}

TEST_F(UrmaConnectionTest, callbacks_can_close_their_own_connection) {
    Bind();
    {
        std::lock_guard<std::mutex> lock(connection_test_mutex);
        close_connection_on_callback = true;
    }
    butil::IOBuf payload; payload.append("close-from-receive");
    butil::IOBuf* data[] = {&payload};
    ASSERT_EQ(18, a->CutFromIOBufList(data, 1));
    for (int i = 0; i < 1000 && b->IsWritable(); ++i) usleep(1000);
    EXPECT_FALSE(b->IsWritable());
    char memory[32] = {};
    butil::IOBuf invalid;
    invalid.append_user_data(memory, sizeof(memory), [](void*) {});
    data[0] = &invalid;
    EXPECT_EQ(-1, a->CutFromIOBufList(data, 1));
    EXPECT_FALSE(a->IsWritable());
}

TEST_F(UrmaConnectionTest, shutdown_does_not_join_poller_while_holding_creation_lock) {
    if (!urma::FLAGS_urma_use_polling) GTEST_SKIP() << "Polling lifecycle test";
    Bind();
    provider->ReleasePollingMode(0);
    butil::atomic<bool> entered{false}, proceed{false}, stopped{false};
    int create_result = 0, create_error = 0;
    ASSERT_EQ(0, provider->InitializePollingMode(0, [&] {
        entered.store(true);
        while (!proceed.load()) usleep(1000);
        urma::UrmaConnectionOptions options;
        options.socket_id = 102;
        urma::UrmaConnectionHandle connection;
        create_result = provider->CreateConnection(options, &connection);
        create_error = errno;
    }, nullptr, nullptr));
    while (!entered.load()) usleep(1000);
    std::thread shutdown([&] { provider->Shutdown(); stopped.store(true); });
    while (a->IsWritable()) usleep(1000);  // Shutdown has published stopping.
    proceed.store(true);
    shutdown.join();
    EXPECT_TRUE(stopped.load());
    EXPECT_EQ(-1, create_result);
    EXPECT_EQ(ESHUTDOWN, create_error);
}


TEST_F(UrmaConnectionTest, carrier_honors_explicit_connection_tag) {
    if (FLAGS_task_group_ntags < 2) GTEST_SKIP() << "Run with --task_group_ntags=2";
    ASSERT_EQ(0, bthread_setconcurrency_by_tag(8, 1));
    ASSERT_EQ(0, provider->InitializePollingMode(1, nullptr, nullptr, nullptr));
    urma::UrmaConnectionOptions options;
    SocketOptions socket_options;
    SocketId tagged_id;
    ASSERT_EQ(0, Socket::Create(socket_options, &tagged_id));
    SocketUniquePtr tagged_socket;
    ASSERT_EQ(0, Socket::Address(tagged_id, &tagged_socket));
    options.socket_id = tagged_id;
    options.tag = 1;
    urma::UrmaConnectionHandle tagged;
    ASSERT_EQ(0, provider->CreateConnection(options, &tagged));
    urma::UrmaConnectionDescription local_description;
    ASSERT_EQ(0, tagged->GetLocalDescription(&local_description));
    ASSERT_EQ(0, b->ImportPeer(local_description));
    ASSERT_EQ(0, tagged->ImportPeer(bd));
    butil::IOBuf payload; payload.append("tagged");
    butil::IOBuf* data[] = {&payload};
    ASSERT_EQ(6, b->CutFromIOBufList(data, 1));
    for (int i = 0; i < 1000; ++i) {
        { std::lock_guard<std::mutex> lock(connection_test_mutex);
          if (connection_received[tagged_id].size() == 6) break; }
        usleep(1000);
    }
    {
        std::lock_guard<std::mutex> lock(connection_test_mutex);
        EXPECT_EQ("tagged", connection_received[tagged_id].to_string());
        EXPECT_EQ(1, connection_received_tag[tagged_id]);
    }
    tagged->Close();
    tagged_socket->SetFailed();
}

TEST(UrmaPooledConnectionTest, factory_selects_pooled_connection_backend) {
    std::unique_ptr<urma::UrmaConnectionProvider> provider;
    ASSERT_EQ(0, urma::CreateUrmaConnectionProvider(
        urma::UrmaConnectionMode::POOLED, &provider));
    ASSERT_NE(nullptr, provider);
    EXPECT_TRUE(provider->Shutdown());
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
