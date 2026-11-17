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

#include "brpc/urma/urma_connection.h"
#if BRPC_WITH_URMA
#include <atomic>
#include <cerrno>
#include <gflags/gflags.h>
#include "brpc/urma/urma_exclusive_connection.h"
#include "brpc/urma/urma_pooled_connection.h"
namespace brpc {
namespace urma {
int CreateUrmaConnectionProvider(UrmaConnectionMode mode,
                                 std::unique_ptr<UrmaConnectionProvider>* out) {
    if (!out) { errno = EINVAL; return -1; }
    switch (mode) {
    case UrmaConnectionMode::EXCLUSIVE:
        return CreateExclusiveUrmaConnectionProvider(out);
    case UrmaConnectionMode::POOLED:
        return CreatePooledUrmaConnectionProvider(out);
    }
    errno = ENOTSUP;
    return -1;
}

// Intentionally process-lived, like the common registered-memory service.
// No atexit cleanup: IOBuf TLS chains may outlive atexit handlers.
DEFINE_string(urma_resource_mode, "exclusive",
              "URMA resource backend: exclusive or pooled (startup only)");
namespace {
std::atomic<UrmaConnectionMode> g_connection_mode{UrmaConnectionMode::EXCLUSIVE};
}
int ConfigureUrmaConnectionMode() {
    UrmaConnectionMode mode;
    if (FLAGS_urma_resource_mode == "exclusive") mode = UrmaConnectionMode::EXCLUSIVE;
    else if (FLAGS_urma_resource_mode == "pooled") mode = UrmaConnectionMode::POOLED;
    else { errno = EINVAL; return -1; }
    g_connection_mode.store(mode, std::memory_order_relaxed);
    return 0;
}
UrmaConnectionMode GetUrmaConnectionMode() {
    return g_connection_mode.load(std::memory_order_relaxed);
}
static auto* g_provider = new std::unique_ptr<UrmaConnectionProvider>;
int InitializeUrmaConnectionProvider(const UrmaConnectionCallbacks& callbacks) {
    if (*g_provider) {
        errno = EALREADY;
        return -1;
    }
    const auto mode = GetUrmaConnectionMode();
    if (CreateUrmaConnectionProvider(mode, g_provider) !=
        0)
        return -1;
    return (*g_provider)->Initialize(callbacks);
}
UrmaConnectionProvider* GetUrmaConnectionProvider() { return g_provider->get(); }
bool ShutdownUrmaConnectionProvider() {
    if (!*g_provider)
        return true;
    if (!(*g_provider)->Shutdown())
        return false;
    g_provider->reset();
    return true;
}
}  // namespace urma
}  // namespace brpc
#endif
