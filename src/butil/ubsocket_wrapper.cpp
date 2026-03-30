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

#include "ubsocket_wrapper.h"

#include <unistd.h>

#include <gflags/gflags.h>

#ifdef BRPC_WITH_URMA
#include "brpc_socket_adapter.h"
#ifndef UB_API_WRAP
#error "You must make ubsocket exported socket APIs prefixed with ubsocket_"
#endif
#else
#define UB_API_WRAP(x) x
#endif

#define DELEGATE(f)                                         \
    ({                                                      \
        FLAGS_ubsocket_enable ? UB_API_WRAP(f) : f; \
    })

DEFINE_bool(ubsocket_enable, true, "Enable the ubsocket interposition layer. If true, socket calls are routed through ubsocket; otherwise, the original socket api is used.");

extern "C" {
int ubsocket_wrapper_socket(int domain, int type, int protocol) {
    return DELEGATE(socket)(domain, type, protocol);
}

int ubsocket_wrapper_connect(int sockfd, const struct sockaddr* addr, socklen_t addrlen) {
    return DELEGATE(connect)(sockfd, addr, addrlen);
}

int ubsocket_wrapper_close(int fd) {
    return DELEGATE(close)(fd);
}

int ubsocket_wrapper_accept(int sockfd, struct sockaddr* addr, socklen_t* addrlen) {
    return DELEGATE(accept)(sockfd, addr, addrlen);
}

int ubsocket_wrapper_epoll_create(int size) {
    return DELEGATE(epoll_create)(size);
}

int ubsocket_wrapper_epoll_ctl(int epfd, int op, int fd, struct epoll_event* event) {
    return DELEGATE(epoll_ctl)(epfd, op, fd, event);
}

int ubsocket_wrapper_epoll_wait(int epfd, struct epoll_event* events, int maxevents, int timeout) {
    return DELEGATE(epoll_wait)(epfd, events, maxevents, timeout);
}

ssize_t ubsocket_wrapper_writev(int fd, const struct iovec* iov, int iovcnt) {
    return DELEGATE(writev)(fd, iov, iovcnt);
}

ssize_t ubsocket_wrapper_readv(int fd, const struct iovec* iov, int iovcnt) {
    return DELEGATE(readv)(fd, iov, iovcnt);
}
}
