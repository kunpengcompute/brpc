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

#ifndef BUTIL_UBSOCKET_WRAPPER_H_
#define BUTIL_UBSOCKET_WRAPPER_H_

#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/uio.h>

extern "C" {
int ubsocket_wrapper_socket(int domain, int type, int protocol);

int ubsocket_wrapper_listen(int fd, int backlog);

int ubsocket_wrapper_connect(int sockfd, const struct sockaddr* addr, socklen_t addrlen);

int ubsocket_wrapper_close(int fd);

int ubsocket_wrapper_accept(int sockfd, struct sockaddr* addr, socklen_t* addrlen);

int ubsocket_wrapper_epoll_create(int size);

int ubsocket_wrapper_epoll_ctl(int epfd, int op, int fd, struct epoll_event* event);

int ubsocket_wrapper_epoll_wait(int epfd, struct epoll_event* events, int maxevents, int timeout);

ssize_t ubsocket_wrapper_writev(int fd, const struct iovec* iov, int iovcnt);

ssize_t ubsocket_wrapper_readv(int fd, const struct iovec* iov, int iovcnt);
}

#endif
