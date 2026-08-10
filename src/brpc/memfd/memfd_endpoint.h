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

#ifndef BRPC_MEMFD_MEMFD_ENDPOINT_H
#define BRPC_MEMFD_MEMFD_ENDPOINT_H

#include "butil/endpoint.h"

namespace brpc {

// Convert a local logical IPv4 endpoint into the abstract Unix domain socket
// used as MEMFD's control connection. Explicit Unix domain socket endpoints
// are kept for backward compatibility.
//
// Returns 0 on success. A non-local IP address or port 0 is rejected.
int MakeMemfdEndpoint(const butil::EndPoint& logical_endpoint,
                      butil::EndPoint* memfd_endpoint);

}  // namespace brpc

#endif  // BRPC_MEMFD_MEMFD_ENDPOINT_H
