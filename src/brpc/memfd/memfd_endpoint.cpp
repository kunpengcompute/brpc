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

#include "brpc/memfd/memfd_endpoint.h"

#include <arpa/inet.h>
#include <errno.h>
#include <ifaddrs.h>
#include <stddef.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/un.h>

namespace brpc {
namespace {

bool IsLocalIPv4(butil::ip_t ip) {
    if (ip == butil::IP_ANY ||
        (ntohl(butil::ip2int(ip)) & 0xff000000u) == 0x7f000000u) {
        return true;
    }

    ifaddrs* addresses = NULL;
    if (getifaddrs(&addresses) != 0) {
        return ip == butil::my_ip();
    }

    bool found = false;
    for (const ifaddrs* current = addresses;
         current != NULL; current = current->ifa_next) {
        if (current->ifa_addr == NULL ||
            current->ifa_addr->sa_family != AF_INET) {
            continue;
        }
        const sockaddr_in* address =
            reinterpret_cast<const sockaddr_in*>(current->ifa_addr);
        if (address->sin_addr.s_addr == ip.s_addr) {
            found = true;
            break;
        }
    }
    freeifaddrs(addresses);
    return found;
}

}  // namespace

int MakeMemfdEndpoint(const butil::EndPoint& logical_endpoint,
                      butil::EndPoint* memfd_endpoint) {
    if (memfd_endpoint == NULL) {
        errno = EINVAL;
        return -1;
    }

    const sa_family_t family = butil::get_endpoint_type(logical_endpoint);
    if (family == AF_UNIX) {
        *memfd_endpoint = logical_endpoint;
        return 0;
    }
    if (family != AF_INET || logical_endpoint.port <= 0 ||
        logical_endpoint.port > 65535) {
        errno = EINVAL;
        return -1;
    }
    if (!IsLocalIPv4(logical_endpoint.ip)) {
        errno = ENETUNREACH;
        return -1;
    }

    // Linux abstract UDS addresses disappear automatically after the last
    // socket is closed, so callers neither expose nor clean up a socket file.
    sockaddr_storage storage = {};
    sockaddr_un* address = reinterpret_cast<sockaddr_un*>(&storage);
    address->sun_family = AF_UNIX;
    const int length = snprintf(address->sun_path + 1,
                                sizeof(address->sun_path) - 1,
                                "brpc_memfd_%d", logical_endpoint.port);
    if (length <= 0 ||
        static_cast<size_t>(length) >= sizeof(address->sun_path) - 1) {
        errno = ENAMETOOLONG;
        return -1;
    }
    const socklen_t address_size =
        offsetof(sockaddr_un, sun_path) + 1 + length;
    if (butil::sockaddr2endpoint(&storage, address_size, memfd_endpoint) != 0) {
        errno = EINVAL;
        return -1;
    }
    return 0;
}

}  // namespace brpc
