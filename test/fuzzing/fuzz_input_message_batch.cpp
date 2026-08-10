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

#include <assert.h>
#include <stddef.h>
#include <stdint.h>

#include <limits>
#include <memory>
#include <vector>

#include <fuzzer/FuzzedDataProvider.h>

#define private public
#include "brpc/input_messenger.h"
#undef private

namespace {

struct BatchState {
    std::vector<uint32_t> processed;
};

class FuzzInputMessage : public brpc::InputMessageBase {
public:
    explicit FuzzInputMessage(uint32_t id) : id(id) {}
    uint32_t id;

private:
    void DestroyImpl() override { delete this; }
};

void RecordInputMessage(brpc::InputMessageBase* msg_base) {
    brpc::DestroyingPtr<brpc::InputMessageBase> guard(msg_base);
    FuzzInputMessage* msg = static_cast<FuzzInputMessage*>(msg_base);
    BatchState* state =
        static_cast<BatchState*>(const_cast<void*>(msg->arg()));
    state->processed.push_back(msg->id);
}

FuzzInputMessage* NewInputMessage(uint32_t id, BatchState* state) {
    FuzzInputMessage* msg = new FuzzInputMessage(id);
    msg->_process = RecordInputMessage;
    msg->_arg = state;
    return msg;
}

void AssertProcessedPrefix(const BatchState& state,
                           const std::vector<uint32_t>& expected) {
    assert(state.processed.size() == expected.size());
    for (size_t i = 0; i < expected.size(); ++i) {
        assert(state.processed[i] == expected[i]);
    }
}

bool IsAdaptiveBatchLevel(uint32_t value) {
    return value == 1 || value == 2 || value == 4 ||
           value == 8 || value == 16;
}

void FuzzBatchContainer(FuzzedDataProvider* provider) {
    BatchState state;
    std::vector<uint32_t> expected;
    std::vector<uint32_t> pending;
    uint32_t next_id = 1;
    const size_t capacity =
        provider->ConsumeIntegralInRange<size_t>(0, 128);
    std::unique_ptr<brpc::InputMessageBatch> batch(
        new brpc::InputMessageBatch(capacity));
    assert(batch->_msgs.capacity() >= capacity);

    while (provider->remaining_bytes() > 0) {
        const uint8_t op = provider->ConsumeIntegral<uint8_t>() % 5;
        if (op == 0) {
            batch->add(NewInputMessage(next_id, &state));
            pending.push_back(next_id++);
        } else if (op == 1) {
            batch->add(nullptr);
        } else if (op == 2) {
            batch->Run();
            expected.insert(expected.end(), pending.begin(), pending.end());
            pending.clear();
            AssertProcessedPrefix(state, expected);
            batch->Run();
            AssertProcessedPrefix(state, expected);
        } else if (op == 3) {
            batch.reset(new brpc::InputMessageBatch(capacity));
            expected.insert(expected.end(), pending.begin(), pending.end());
            pending.clear();
            AssertProcessedPrefix(state, expected);
        } else {
            const size_t burst =
                provider->ConsumeIntegralInRange<size_t>(0, 32);
            for (size_t i = 0; i < burst; ++i) {
                batch->add(NewInputMessage(next_id, &state));
                pending.push_back(next_id++);
            }
        }
    }

    batch.reset();
    expected.insert(expected.end(), pending.begin(), pending.end());
    AssertProcessedPrefix(state, expected);
}

void FuzzAdaptiveBatch(FuzzedDataProvider* provider) {
    uint32_t ema_q8 = provider->ConsumeIntegral<uint32_t>();
    uint32_t batch_size = provider->ConsumeIntegral<uint32_t>();
    for (int i = 0; i < 64 && provider->remaining_bytes() > 0; ++i) {
        const size_t parsed_message_count =
            provider->ConsumeBool()
                ? provider->ConsumeIntegralInRange<size_t>(0, 128)
                : std::numeric_limits<size_t>::max();
        const uint32_t old_ema_q8 = ema_q8;
        const uint32_t result =
            brpc::InputMessenger::UpdateAdaptiveBatchSize(
                &ema_q8, batch_size, parsed_message_count);
        if (parsed_message_count == 0) {
            assert(result == batch_size);
            assert(ema_q8 == old_ema_q8);
        } else {
            assert(IsAdaptiveBatchLevel(result));
        }
        batch_size = result;
    }
}

}  // namespace

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    if (size == 0 || size > 4096) {
        return 0;
    }
    FuzzedDataProvider provider(data, size);
    FuzzBatchContainer(&provider);
    FuzzAdaptiveBatch(&provider);
    return 0;
}
