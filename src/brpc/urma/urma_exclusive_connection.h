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

#ifndef BRPC_URMA_EXCLUSIVE_CONNECTION_H
#define BRPC_URMA_EXCLUSIVE_CONNECTION_H
#include "brpc/urma/urma_connection.h"
#if BRPC_WITH_URMA
namespace brpc {
namespace urma {
int CreateExclusiveUrmaConnectionProvider(std::unique_ptr<UrmaConnectionProvider>* out);
}  // namespace urma
}  // namespace brpc
#endif
#endif
