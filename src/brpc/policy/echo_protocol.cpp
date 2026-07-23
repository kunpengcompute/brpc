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

// Minimal echo protocol. Wire format:
//   [4B "ECHO"][4B payload_len BE][8B correlation_id][payload]
// Server echoes back identical bytes. No protobuf RpcMeta, no Controller.
// Client uses bthread_id_lock + OnResponse for response matching.

#include "butil/logging.h"
#include "butil/raw_pack.h"
#include "bthread/bthread.h"
#include <gflags/gflags.h>
#include "brpc/controller.h"
#include "brpc/socket.h"
#include "brpc/protocol.h"          // SerializeRequestDefault
#include "brpc/details/controller_private_accessor.h"
#include "brpc/policy/echo_protocol.h"
#include "brpc/policy/most_common_message.h"

namespace brpc {
namespace policy {

DEFINE_int32(echo_fake_payload_size, 0,
             "Simulate N bytes of protobuf payload processing on server side. "
             "0 disables. Used for profiling.");
DEFINE_int32(echo_fake_payload_iterations, 1,
             "Number of memcpy passes for fake payload. Higher = more visible "
             "in flame graphs.");
DEFINE_bool(echo_verbose, false, "Print per-request size logs for debugging.");

static const size_t kEchoHeaderSize = 16; // "ECHO"(4) + len(4) + cid(8)

// Force the compiler to treat the result as observable.
static volatile uint64_t g_echo_fake_checksum = 0;

ParseResult ParseEchoMessage(butil::IOBuf* source, Socket* /*socket*/,
                             bool /*read_eof*/, const void* /*arg*/) {
    char peek8[8];
    const size_t n = source->copy_to(peek8, sizeof(peek8));
    if (n < 8) {
        if (n > 0 && memcmp(peek8, "ECHO", std::min(n, (size_t)4)) != 0) {
            return MakeParseError(PARSE_ERROR_TRY_OTHERS);
        }
        return MakeParseError(PARSE_ERROR_NOT_ENOUGH_DATA);
    }
    if (memcmp(peek8, "ECHO", 4) != 0) {
        return MakeParseError(PARSE_ERROR_TRY_OTHERS);
    }

    uint32_t payload_len;
    butil::RawUnpacker(peek8 + 4).unpack32(payload_len);

    const size_t total = kEchoHeaderSize + payload_len;
    if (source->length() < total) {
        return MakeParseError(PARSE_ERROR_NOT_ENOUGH_DATA);
    }

    MostCommonMessage* msg = MostCommonMessage::Get();
    source->cutn(&msg->meta, kEchoHeaderSize);
    source->cutn(&msg->payload, payload_len);
    return MakeMessage(msg);
}

void ProcessEchoRequest(InputMessageBase* msg_base) {
    // Server: echo entire message back. ZERO protobuf/Controller/CallMethod.
    DestroyingPtr<MostCommonMessage> msg(static_cast<MostCommonMessage*>(msg_base));
    SocketUniquePtr sock_guard(msg->ReleaseSocket());
    Socket* sock = sock_guard.get();

    // Clear any stale parsing context that might have been set by
    // other protocols (e.g. RTMP) tried before echo during CutInputMessage.
    sock->reset_parsing_context(NULL);

    if (FLAGS_echo_verbose) {
        LOG_EVERY_N(INFO, 10000) << "echo recv: meta=" << msg->meta.size()
                                 << " payload=" << msg->payload.size()
                                 << " total=" << (msg->meta.size() + msg->payload.size())
                                 << " from " << sock->remote_side();
    }

    butil::IOBuf echo_buf;
    echo_buf.append(msg->meta);
    echo_buf.append(msg->payload);

    // Simulate protobuf payload processing on server side (for profiling).
    if (FLAGS_echo_fake_payload_size > 0) {
        const int n = FLAGS_echo_fake_payload_size;
        const int iter = FLAGS_echo_fake_payload_iterations;
        char* buf = new (std::nothrow) char[n]();
        char* buf2 = new (std::nothrow) char[n];
        if (buf && buf2) {
            uint64_t sum = 0;
            for (int i = 0; i < iter; ++i) {
                memcpy(buf2, buf, n);
                // Accumulate checksum to prevent optimization.
                for (int j = 0; j < n; ++j) {
                    sum += (uint8_t)buf2[j];
                }
            }
            g_echo_fake_checksum = sum;
        }
        delete[] buf;
        delete[] buf2;
    }

    Socket::WriteOptions wopt;
    wopt.ignore_eovercrowded = true;
    sock->Write(&echo_buf, &wopt);
}

void SerializeEchoRequest(butil::IOBuf* request_buf, Controller* cntl,
                          const google::protobuf::Message* request) {
    SerializeRequestDefault(request_buf, cntl, request);
}

void PackEchoRequest(butil::IOBuf* iobuf_out,
                     SocketMessage** /*user_message_out*/,
                     uint64_t correlation_id,
                     const google::protobuf::MethodDescriptor* /*method*/,
                     Controller* controller,
                     const butil::IOBuf& request_buf,
                     const Authenticator* /*auth*/) {
    if (controller->Failed()) return;

    const size_t attached_size = controller->request_attachment().length();
    const uint32_t payload_len = request_buf.size() + attached_size;
    char header[kEchoHeaderSize];
    memcpy(header, "ECHO", 4);
    butil::RawPacker(header + 4).pack32(payload_len);
    memcpy(header + 8, &correlation_id, 8);

    iobuf_out->append(header, kEchoHeaderSize);
    iobuf_out->append(request_buf);
    if (attached_size > 0) {
        iobuf_out->append(controller->request_attachment());
    }

    if (FLAGS_echo_verbose) {
        LOG_EVERY_N(INFO, 10000) << "echo cli-send: cid=" << correlation_id
                                 << " request=" << request_buf.size()
                                 << " attachment=" << attached_size
                                 << " payload=" << payload_len
                                 << " total=" << (kEchoHeaderSize + payload_len);
    }
}

void ProcessEchoResponse(InputMessageBase* msg_base) {
    DestroyingPtr<MostCommonMessage> msg(static_cast<MostCommonMessage*>(msg_base));

    if (msg->meta.size() < 16) {
        LOG(ERROR) << "Echo response meta too short: " << msg->meta.size();
        return;
    }

    uint64_t cid_val = 0;
    msg->meta.copy_to((char*)&cid_val, 8, 8);

    if (FLAGS_echo_verbose) {
        LOG_EVERY_N(INFO, 10000) << "echo cli-recv: cid=" << cid_val
                                 << " meta=" << msg->meta.size()
                                 << " payload=" << msg->payload.size()
                                 << " total=" << (msg->meta.size() + msg->payload.size());
    }

    const bthread_id_t cid = { cid_val };
    Controller* cntl = NULL;
    const int rc = bthread_id_lock(cid, (void**)&cntl);
    if (rc != 0) {
        LOG_IF(ERROR, rc != EINVAL && rc != EPERM)
            << "Fail to lock correlation_id=" << cid_val << ": " << berror(rc);
        return;
    }

    ControllerPrivateAccessor accessor(cntl);
    // Echo protocol: server echoes back raw request bytes. No protobuf
    // parsing on either side. The response protobuf is left unmodified.
    msg.reset();  // Release resources before completing RPC
    const int saved_error = cntl->ErrorCode();
    accessor.OnResponse(cid, saved_error);
}

}  // namespace policy
}  // namespace brpc
