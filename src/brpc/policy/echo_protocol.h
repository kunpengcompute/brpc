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

// Minimal echo protocol for performance testing.
// Wire format: [4B "ECHO"][4B payload_len BE][8B correlation_id][payload]
// Server echoes back the entire received bytes verbatim.
// Zero protobuf, zero RpcMeta, zero Controller on server side.

#ifndef BRPC_POLICY_ECHO_PROTOCOL_H
#define BRPC_POLICY_ECHO_PROTOCOL_H

#include "brpc/protocol.h"

namespace brpc {
namespace policy {

ParseResult ParseEchoMessage(butil::IOBuf* source, Socket* socket,
                             bool read_eof, const void* arg);
void ProcessEchoRequest(InputMessageBase* msg);
void SerializeEchoRequest(butil::IOBuf* request_buf, Controller* cntl,
                          const google::protobuf::Message* request);
void PackEchoRequest(butil::IOBuf* iobuf_out,
                     SocketMessage** user_message_out,
                     uint64_t correlation_id,
                     const google::protobuf::MethodDescriptor* method,
                     Controller* controller,
                     const butil::IOBuf& request_buf,
                     const Authenticator* auth);
void ProcessEchoResponse(InputMessageBase* msg);

}  // namespace policy
}  // namespace brpc

#endif
